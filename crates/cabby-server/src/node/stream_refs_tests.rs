//! Deviation D-091: the broker's stream references follow the sessions' own
//! stream memberships.
//!
//! The broker counts references per stream: the first one subscribes the node to
//! pub/sub for the stream and the last one unsubscribes it. The recording
//! broadcaster below sees exactly those two transitions, so its log tells whether
//! the count matches what the sessions hold.

use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cabby_core::broker::{Broadcaster, Broker, BrokerError, Cacheable, Legacy, PresenceInfoOptions};
use cabby_core::common::{
    CommandResult, ConnectResult, ControllerError, Message, MessageData, Outcome, PresenceEvent, PresenceInfo,
    RemoteCommandMessage, SessionEnv, Status, StreamMessage,
};
use serde_json::Value;

use super::{CLOSE_NORMAL_CLOSURE, Config, Connection, ConnectionError, Controller, Node, NodeOption, Session};

const DEADLINE: Duration = Duration::from_secs(5);

/// Records pub/sub subscriptions: `+stream` for the first reference, `-stream`
/// for the last one.
#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<String>>,
}

impl Recorder {
    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }
}

impl Broadcaster for Recorder {
    fn broadcast(&self, _msg: &StreamMessage) {}
    fn broadcast_command(&self, _msg: &RemoteCommandMessage) {}
    fn subscribe(&self, stream: &str) {
        self.events.lock().unwrap().push(format!("+{stream}"));
    }
    fn unsubscribe(&self, stream: &str) {
        self.events.lock().unwrap().push(format!("-{stream}"));
    }
}

/// Subscribes every channel (no streams) and answers performs with the streams
/// named in their data: `{"start":[...],"stop":[...]}`.
struct Scripted;

fn strings(v: &Value) -> Vec<String> {
    v.as_array().map(|a| a.iter().filter_map(|s| s.as_str().map(str::to_owned)).collect()).unwrap_or_default()
}

impl Controller for Scripted {
    fn start(&self) -> Result<(), ControllerError> {
        Ok(())
    }
    fn shutdown(&self) -> Result<(), ControllerError> {
        Ok(())
    }
    fn authenticate(&self, _sid: &str, _env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        Outcome { result: None, error: None }
    }
    fn subscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, channel: &str) -> Outcome<CommandResult> {
        // A channel named `rejected...` rejects after starting a stream.
        let rejected = channel.starts_with("rejected");
        Outcome {
            result: Some(CommandResult {
                status: if rejected { Status::Failure } else { Status::Success },
                streams: if rejected { vec!["s".into()] } else { Vec::new() },
                stop_all_streams: rejected,
                ..Default::default()
            }),
            error: None,
        }
    }
    fn unsubscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, _ch: &str) -> Outcome<CommandResult> {
        Outcome { result: Some(CommandResult { status: Status::Success, ..Default::default() }), error: None }
    }
    fn perform(
        &self,
        _sid: &str,
        _env: Option<&SessionEnv>,
        _ids: &str,
        _channel: &str,
        data: &str,
    ) -> Outcome<CommandResult> {
        let script: Value = serde_json::from_str(data).expect("a script");
        Outcome {
            result: Some(CommandResult {
                status: Status::Success,
                streams: strings(&script["start"]),
                stopped_streams: strings(&script["stop"]),
                ..Default::default()
            }),
            error: None,
        }
    }
    fn disconnect(
        &self,
        _sid: &str,
        _env: Option<&SessionEnv>,
        _ids: &str,
        _subscriptions: &[String],
    ) -> Result<(), ControllerError> {
        Ok(())
    }
}

/// Passes the session's broadcast frames to the test (pings are left out).
struct Frames(Mutex<Sender<String>>);

impl Connection for Frames {
    fn write(&self, msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
        let text = String::from_utf8_lossy(msg).into_owned();
        if !text.contains(r#""type":"ping""#) {
            let _ = self.0.lock().unwrap().send(text);
        }
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

struct Rig {
    node: Arc<Node>,
    recorder: Arc<Recorder>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.node.hub().shutdown();
    }
}

/// A broker whose identifier for a stream differs from its name (`id:<name>`;
/// an identifier is its own identifier). Every Cabby broker returns the name.
struct Renaming(Legacy);

fn renamed(stream: &str) -> String {
    if stream.starts_with("id:") { stream.to_owned() } else { format!("id:{stream}") }
}

impl Broker for Renaming {
    fn start(&self) -> Result<(), BrokerError> {
        self.0.start()
    }
    fn shutdown(&self) -> Result<(), BrokerError> {
        self.0.shutdown()
    }
    fn announce(&self) -> String {
        self.0.announce()
    }
    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        self.0.handle_broadcast(msg)
    }
    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        self.0.handle_command(msg)
    }
    fn subscribe(&self, stream: &str) -> String {
        self.0.subscribe(&renamed(stream))
    }
    fn unsubscribe(&self, stream: &str) -> String {
        self.0.unsubscribe(&renamed(stream))
    }
    fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        self.0.history_from(stream, epoch, offset)
    }
    fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        self.0.history_since(stream, ts)
    }
    fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError> {
        self.0.commit_session(sid, session)
    }
    fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        self.0.restore_session(sid)
    }
    fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        self.0.touch_session(sid)
    }
    fn delete_session(&self, sid: &str) -> Result<(), BrokerError> {
        self.0.delete_session(sid)
    }
    fn supports_presence(&self) -> bool {
        self.0.supports_presence()
    }
    fn presence_add(
        &self,
        stream: &str,
        sid: &str,
        pid: &str,
        info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError> {
        self.0.presence_add(stream, sid, pid, info)
    }
    fn presence_remove(&self, stream: &str, sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        self.0.presence_remove(stream, sid)
    }
    fn presence_info(&self, stream: &str, opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        self.0.presence_info(stream, opts)
    }
    fn touch_presence(&self, sid: &str) -> Result<(), BrokerError> {
        self.0.touch_presence(sid)
    }
    fn peak(&self, stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        self.0.peak(stream)
    }
}

fn rig() -> Rig {
    rig_with(|recorder| Arc::new(Legacy::new(recorder)))
}

fn rig_with(broker: impl FnOnce(Arc<Recorder>) -> Arc<dyn Broker>) -> Rig {
    let mut config = Config::new();
    config.hub_gopool_size = 1;
    let node = Node::new(config, vec![NodeOption::Controller(Arc::new(Scripted))]);
    let recorder = Arc::new(Recorder::default());
    node.set_broker(broker(recorder.clone()));
    Rig { node, recorder }
}

impl Rig {
    /// A registered session and the frames it is sent.
    fn session(&self, sid: &str) -> (Arc<Session>, Receiver<String>) {
        let (tx, rx) = mpsc::channel();
        let s = Session::new(&self.node, Arc::new(Frames(Mutex::new(tx))), "/cable", None, sid, Vec::new());
        self.node.authenticated(&s, sid);
        (s, rx)
    }

    fn subscribe(&self, s: &Arc<Session>, channel: &str) {
        let msg = Message { command: "subscribe".into(), identifier: channel.into(), ..Default::default() };
        self.node.subscribe(s, &msg).expect("subscribe");
    }

    fn unsubscribe(&self, s: &Arc<Session>, channel: &str) {
        let msg = Message { command: "unsubscribe".into(), identifier: channel.into(), ..Default::default() };
        self.node.unsubscribe(s, &msg).expect("unsubscribe");
    }

    /// A perform whose reply starts and stops the given streams, in that reply's
    /// order (stops first).
    fn perform(&self, s: &Arc<Session>, channel: &str, start: &[&str], stop: &[&str]) {
        let data = serde_json::json!({"start": start, "stop": stop}).to_string();
        let msg = Message {
            command: "message".into(),
            identifier: channel.into(),
            data: Some(MessageData::Json(Value::String(data))),
            ..Default::default()
        };
        self.node.perform(s, &msg).expect("perform");
    }

    fn broadcast(&self, stream: &str, data: &str) {
        self.node.broadcast(&StreamMessage { stream: stream.into(), data: data.into(), ..Default::default() });
    }

    /// Disconnects the session and waits until the node has unregistered it.
    /// The session's writer detaches it from the node when it writes the close
    /// frame first, so the references may be released on the writer's thread.
    fn disconnect(&self, s: &Arc<Session>) {
        s.disconnect("done", CLOSE_NORMAL_CLOSURE);
        let sid = s.get_id();
        let end = Instant::now() + DEADLINE;
        while self.node.hub().sessions().iter().any(|other| other.get_id() == sid) {
            assert!(Instant::now() < end, "the session must be unregistered within the deadline");
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

fn streams(s: &Session, channel: &str) -> Vec<String> {
    let mut v = s.subscriptions().streams_for(channel).unwrap_or_default();
    v.sort();
    v
}

fn next_frame(rx: &Receiver<String>) -> String {
    rx.recv_timeout(DEADLINE).expect("a frame within the deadline")
}

/// Deviation D-091: a channel that starts the same stream twice holds one
/// reference, so a disconnect leaves none.
#[test]
fn deviation_d091_duplicate_starts_hold_one_reference() {
    let rig = rig();
    let (s, _rx) = rig.session("a");
    rig.subscribe(&s, "chat");

    rig.perform(&s, "chat", &["s", "s"], &[]);
    rig.perform(&s, "chat", &["s"], &[]);
    assert_eq!(vec!["s"], streams(&s, "chat"));
    assert_eq!(vec!["+s"], rig.recorder.events());

    rig.disconnect(&s);
    assert_eq!(vec!["+s", "-s"], rig.recorder.events());
}

/// Deviation D-091: a stop for a stream the channel does not hold leaves the
/// references of other sessions, and their delivery, intact.
#[test]
fn deviation_d091_stopping_a_stream_the_channel_does_not_hold_keeps_other_references() {
    let rig = rig();
    let (a, _rx_a) = rig.session("a");
    let (b, rx_b) = rig.session("b");
    rig.subscribe(&a, "chat");
    rig.subscribe(&b, "chat");

    rig.perform(&b, "chat", &["s"], &[]);
    rig.perform(&a, "chat", &[], &["s"]);
    rig.perform(&a, "chat", &[], &["s"]);
    assert_eq!(vec!["+s"], rig.recorder.events(), "the node must stay subscribed to pub/sub for b");

    rig.broadcast("s", "1");
    assert_eq!(r#"{"identifier":"chat","message":1}"#, next_frame(&rx_b));

    rig.disconnect(&a);
    assert_eq!(vec!["+s"], rig.recorder.events());
    rig.disconnect(&b);
    assert_eq!(vec!["+s", "-s"], rig.recorder.events());
}

/// Deviation D-091: a reply that stops and starts the same stream keeps one
/// membership and one reference (stops apply first, as in AnyCable), also after
/// a duplicate start.
#[test]
fn deviation_d091_stop_then_start_in_one_reply() {
    let rig = rig();
    let (s, rx) = rig.session("a");
    rig.subscribe(&s, "chat");

    rig.perform(&s, "chat", &["s", "s"], &[]);
    rig.perform(&s, "chat", &["s"], &["s"]);
    assert_eq!(vec!["s"], streams(&s, "chat"));
    assert_eq!(vec!["+s", "-s", "+s"], rig.recorder.events());

    rig.broadcast("s", "1");
    assert_eq!(r#"{"identifier":"chat","message":1}"#, next_frame(&rx));

    rig.disconnect(&s);
    assert_eq!(vec!["+s", "-s", "+s", "-s"], rig.recorder.events());
}

/// Deviation D-091: the same stream in two channels of one session is one
/// membership per channel; stopping it in one channel keeps the other's
/// reference and delivery.
#[test]
fn deviation_d091_same_stream_in_two_channels_of_one_session() {
    let rig = rig();
    let (s, rx) = rig.session("a");
    rig.subscribe(&s, "one");
    rig.subscribe(&s, "two");

    rig.perform(&s, "one", &["s"], &[]);
    rig.perform(&s, "two", &["s"], &[]);
    rig.perform(&s, "one", &[], &["s"]);
    // A second stop: "one" no longer holds the stream, "two" still does.
    rig.perform(&s, "one", &[], &["s"]);
    assert!(streams(&s, "one").is_empty());
    assert_eq!(vec!["s"], streams(&s, "two"));
    assert_eq!(vec!["+s"], rig.recorder.events());

    // The second broadcast orders the check: a frame for "one" would come first.
    rig.broadcast("s", "1");
    rig.broadcast("s", "2");
    assert_eq!(r#"{"identifier":"two","message":1}"#, next_frame(&rx));
    assert_eq!(r#"{"identifier":"two","message":2}"#, next_frame(&rx));

    rig.disconnect(&s);
    assert_eq!(vec!["+s", "-s"], rig.recorder.events());
}

/// Deviation D-091: stopping all streams after duplicate starts drops the
/// channel's one reference.
#[test]
fn deviation_d091_stop_all_streams_after_duplicates() {
    let rig = rig();
    let (s, _rx) = rig.session("a");
    rig.subscribe(&s, "chat");
    rig.subscribe(&s, "other");

    rig.perform(&s, "chat", &["s", "s", "t"], &[]);
    rig.perform(&s, "chat", &["s"], &[]);
    rig.perform(&s, "other", &["t"], &[]);
    rig.unsubscribe(&s, "chat");
    assert_eq!(vec!["+s", "+t", "-s"], rig.recorder.events());

    rig.disconnect(&s);
    assert_eq!(vec!["+s", "+t", "-s", "-t"], rig.recorder.events());
}

/// Deviation D-091: a reply for a channel the session does not have (here a
/// rejected subscription that started a stream) adds no reference and no hub
/// subscription.
#[test]
fn deviation_d091_streams_of_a_rejected_subscription_are_not_subscribed() {
    let rig = rig();
    let (s, rx) = rig.session("a");
    rig.subscribe(&s, "rejected");
    rig.subscribe(&s, "chat");
    assert_eq!(None, s.subscriptions().streams_for("rejected"));
    assert!(rig.recorder.events().is_empty());

    // A frame for "rejected" would come first (identifiers in subscription order).
    rig.perform(&s, "chat", &["s"], &[]);
    rig.broadcast("s", "1");
    assert_eq!(r#"{"identifier":"chat","message":1}"#, next_frame(&rx));

    rig.disconnect(&s);
    assert_eq!(vec!["+s", "-s"], rig.recorder.events());
}

/// Deviation D-091: with a broker whose stream identifiers differ from the
/// names, a duplicate start gives its extra reference back.
#[test]
fn deviation_d091_duplicate_starts_with_renamed_streams_hold_one_reference() {
    let rig = rig_with(|recorder| Arc::new(Renaming(Legacy::new(recorder))));
    let (s, rx) = rig.session("a");
    rig.subscribe(&s, "chat");

    rig.perform(&s, "chat", &["s"], &[]);
    rig.perform(&s, "chat", &["s"], &[]);
    assert_eq!(vec!["id:s"], streams(&s, "chat"));
    assert_eq!(vec!["+id:s"], rig.recorder.events());

    rig.broadcast("id:s", "1");
    rig.broadcast("id:s", "2");
    assert_eq!(r#"{"identifier":"chat","message":1}"#, next_frame(&rx));
    assert_eq!(r#"{"identifier":"chat","message":2}"#, next_frame(&rx));

    rig.disconnect(&s);
    assert_eq!(vec!["+id:s", "-id:s"], rig.recorder.events());
}
