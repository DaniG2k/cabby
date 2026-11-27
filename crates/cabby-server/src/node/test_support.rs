//! Doubles shared by the node's pub/sub tests (D-105, D-106): a controller
//! that streams from the channel identifier, a recording connection, and a
//! pub/sub subscriber whose subscriptions become active when the test says so.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use cabby_core::broker::{Broadcaster, Broker, Config as BrokerConfig, Legacy, Memory, SystemClock as BrokerClock};
use cabby_core::common::{
    CommandResult, ConnectResult, Controller, ControllerError, HistoryRequest, Message, Outcome, RemoteCommandMessage,
    SessionEnv, Status, StreamMessage,
};
use cabby_core::pubsub::{Activations, Handler};

use super::{Config, Connection, ConnectionError, Node, NodeOption, Session};

/// How long a test waits for something it requires.
const DEADLINE: Duration = Duration::from_secs(5);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Waits (yielding) until `cond` holds; panics after [`DEADLINE`].
pub(super) fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + DEADLINE;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting: {what}");
        std::thread::yield_now();
    }
}

/// Subscribing to the identifier `<stream>` streams from `<stream>` and
/// transmits the confirmation.
struct StreamController;

impl Controller for StreamController {
    fn start(&self) -> Result<(), ControllerError> {
        Ok(())
    }
    fn shutdown(&self) -> Result<(), ControllerError> {
        Ok(())
    }
    fn authenticate(&self, _sid: &str, _env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        Outcome { result: None, error: Some(ControllerError::new("not used")) }
    }
    fn subscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, channel: &str) -> Outcome<CommandResult> {
        let confirm = serde_json::json!({"type": "confirm_subscription", "identifier": channel}).to_string();
        Outcome {
            result: Some(CommandResult {
                status: Status::Success,
                streams: vec![channel.to_owned()],
                transmissions: vec![confirm],
                ..Default::default()
            }),
            error: None,
        }
    }
    fn unsubscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, _channel: &str) -> Outcome<CommandResult> {
        Outcome { result: Some(CommandResult { status: Status::Success, ..Default::default() }), error: None }
    }
    fn perform(&self, _: &str, _: Option<&SessionEnv>, _: &str, _: &str, _: &str) -> Outcome<CommandResult> {
        Outcome { result: None, error: Some(ControllerError::new("not used")) }
    }
    fn disconnect(&self, _: &str, _: Option<&SessionEnv>, _: &str, _: &[String]) -> Result<(), ControllerError> {
        Ok(())
    }
}

/// A connection recording what the session writes.
#[derive(Default)]
pub(super) struct Rec {
    frames: Mutex<Vec<serde_json::Value>>,
    written: Condvar,
}

impl Rec {
    /// What was written, pings left out: `confirm_subscription:<id>`,
    /// `confirm_history:<id>`, `message:<message>@<offset>` (`@<offset>` only
    /// with an offset), other frames as JSON.
    pub fn frames(&self) -> Vec<String> {
        lock(&self.frames)
            .iter()
            .filter(|v| v["type"] != "ping")
            .map(|v| match (v["type"].as_str(), v.get("message")) {
                (Some(kind), None) => format!("{kind}:{}", v["identifier"].as_str().unwrap_or_default()),
                (None, Some(m)) => {
                    let text = m.as_str().map_or_else(|| m.to_string(), str::to_owned);
                    match v["offset"].as_u64() {
                        Some(offset) => format!("message:{text}@{offset}"),
                        None => format!("message:{text}"),
                    }
                }
                _ => v.to_string(),
            })
            .collect()
    }

    /// The messages written (`message:` frames).
    pub fn messages(&self) -> Vec<String> {
        self.frames().into_iter().filter(|f| f.starts_with("message:")).collect()
    }

    /// Waits until at least `n` frames (pings left out) were written.
    pub fn wait_frames(&self, n: usize) {
        let deadline = Instant::now() + DEADLINE;
        while self.frames().len() < n {
            let now = Instant::now();
            assert!(now < deadline, "timed out waiting for {n} frames, got {:?}", self.frames());
            let g = lock(&self.frames);
            let _ = self.written.wait_timeout(g, deadline - now).unwrap_or_else(PoisonError::into_inner);
        }
    }
}

struct RecConn(Arc<Rec>);

impl Connection for RecConn {
    fn write(&self, msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
        let v = serde_json::from_slice(msg).unwrap_or_else(|_| serde_json::json!(String::from_utf8_lossy(msg)));
        lock(&self.0.frames).push(v);
        self.0.written.notify_all();
        Ok(())
    }
    fn write_binary(&self, _msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
        Ok(())
    }
    fn write_ping(&self, _deadline: Instant) -> Result<(), ConnectionError> {
        Ok(())
    }
    fn read(&self) -> Result<Vec<u8>, ConnectionError> {
        Err(ConnectionError("websocket: close 1000 (normal)".into()))
    }
    fn close(&self, _code: i32, _reason: &str) {}
}

#[derive(Default)]
struct PubSubState {
    calls: HashMap<String, usize>,
    active: HashSet<String>,
}

/// A multi-node pub/sub as the node sees it: the first subscription to a
/// stream is reported pending (as the Redis subscriber does) until the test
/// activates it; a message published meanwhile does not come back.
pub(super) struct PubSub {
    node: Weak<Node>,
    activations: Option<Arc<Activations>>,
    state: Mutex<PubSubState>,
    changed: Condvar,
}

impl PubSub {
    /// Makes `stream`'s subscription active.
    pub fn activate(&self, stream: &str) {
        lock(&self.state).active.insert(stream.to_owned());
        if let Some(a) = &self.activations {
            a.settled(stream);
        }
    }

    pub fn subscribe_calls(&self, stream: &str) -> usize {
        lock(&self.state).calls.get(stream).copied().unwrap_or(0)
    }

    /// Waits until the node subscribed to `stream`.
    pub fn wait_requested(&self, stream: &str) {
        let deadline = Instant::now() + DEADLINE;
        let mut st = lock(&self.state);
        while !st.calls.contains_key(stream) {
            let now = Instant::now();
            assert!(now < deadline, "the node did not subscribe to {stream}");
            st = self.changed.wait_timeout(st, deadline - now).unwrap_or_else(PoisonError::into_inner).0;
        }
    }

    /// Publishes `text` to `stream` through the node's broker (which stores
    /// it, with a broker, and hands it to this pub/sub).
    pub fn publish(&self, stream: &str, text: &str) {
        let node = self.node.upgrade().expect("node");
        let msg = StreamMessage {
            stream: stream.to_owned(),
            data: serde_json::json!(text).to_string(),
            ..Default::default()
        };
        node.broker().handle_broadcast(&msg).expect("broadcast");
    }
}

impl Broadcaster for PubSub {
    fn broadcast(&self, msg: &StreamMessage) {
        if !lock(&self.state).active.contains(&msg.stream) {
            return;
        }
        if let Some(node) = self.node.upgrade() {
            Handler::broadcast(node.as_ref(), msg);
        }
    }
    fn broadcast_command(&self, _msg: &RemoteCommandMessage) {}
    fn subscribe(&self, stream: &str) {
        let mut st = lock(&self.state);
        *st.calls.entry(stream.to_owned()).or_default() += 1;
        if !st.active.contains(stream)
            && let Some(a) = &self.activations
        {
            a.requested(stream);
        }
        self.changed.notify_all();
    }
    fn unsubscribe(&self, stream: &str) {
        lock(&self.state).active.remove(stream);
        if let Some(a) = &self.activations {
            a.settled(stream);
        }
    }
}

pub(super) struct Fixture {
    pub node: Arc<Node>,
    pub pubsub: Arc<PubSub>,
    conns: Mutex<HashMap<String, Arc<Rec>>>,
}

impl Fixture {
    /// A node without history (legacy broker) over the pub/sub double.
    pub fn new() -> Self {
        Self::build(false)
    }

    fn build(history: bool) -> Self {
        let node = Node::new(Config::new(), vec![NodeOption::Controller(Arc::new(StreamController))]);
        let pubsub = Arc::new(PubSub {
            node: Arc::downgrade(&node),
            activations: Handler::activations(node.as_ref()),
            state: Mutex::default(),
            changed: Condvar::new(),
        });
        let broker: Arc<dyn Broker> = if history {
            Arc::new(Memory::new(Some(pubsub.clone()), None, &BrokerConfig::new(), Arc::new(BrokerClock)))
        } else {
            Arc::new(Legacy::new(pubsub.clone()))
        };
        node.set_broker(broker);
        Self { node, pubsub, conns: Mutex::default() }
    }

    pub fn activations(&self) -> Arc<Activations> {
        Handler::activations(self.node.as_ref()).expect("the node tracks activations")
    }

    /// An authenticated session `sid`.
    pub fn session(&self, sid: &str) -> Arc<Session> {
        let rec = Arc::new(Rec::default());
        lock(&self.conns).insert(sid.to_owned(), rec.clone());
        let s = Session::new(&self.node, Arc::new(RecConn(rec)), "/cable", None, sid, Vec::new());
        self.node.authenticated(&s, sid);
        s
    }

    pub fn conn(&self, sid: &str) -> Arc<Rec> {
        lock(&self.conns).get(sid).cloned().expect("a session")
    }

    /// Runs the subscribe command for `stream` (with `history`, if any) on
    /// another thread.
    pub fn spawn_subscribe(&self, s: &Arc<Session>, stream: &str, history: Option<HistoryRequest>) -> JoinHandle<()> {
        let (node, s) = (self.node.clone(), s.clone());
        let msg = Message {
            command: "subscribe".into(),
            identifier: stream.to_owned(),
            history: history.unwrap_or_default(),
            ..Default::default()
        };
        std::thread::spawn(move || {
            node.subscribe(&s, &msg).expect("subscribe");
        })
    }
}
