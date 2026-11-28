//! What reaches the session cache and what is restored from it (D-083).
//!
//! A node with a recording memory broker, an application that authenticates
//! requests carrying an `x-user` header and rejects the others (as enforced JWT
//! does), and sessions on connections that report their frames.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use cabby_core::broker::{
    Broker, BrokerError, Cacheable, Config as BrokerConfig, Memory, PresenceInfoOptions, SystemClock as BrokerClock,
};
use cabby_core::common::{
    CommandResult, ConnectResult, ControllerError, Message, Outcome, PresenceEvent, PresenceInfo, RemoteCommandMessage,
    RemoteDisconnectMessage, SessionEnv, Status, StreamMessage, UNAUTHORIZED_REASON, disconnection_message,
};
use serde_json::Value;

use super::*;

const SESSIONS_TTL: Duration = Duration::from_secs(300);
const WAIT: Duration = Duration::from_secs(5);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

type Callback = Box<dyn FnOnce() + Send>;

/// Time moves only when a test advances it; due timers fire then, on the
/// advancing thread.
struct ManualClock {
    start: Instant,
    state: Mutex<ClockState>,
}

#[derive(Default)]
struct ClockState {
    offset: Duration,
    timers: Vec<(Instant, Arc<AtomicBool>, Callback)>,
}

struct ManualTimer(Arc<AtomicBool>);

impl Timer for ManualTimer {
    fn stop(&self) -> bool {
        !self.0.swap(true, Ordering::SeqCst)
    }
}

impl ManualClock {
    fn new() -> Arc<Self> {
        Arc::new(Self { start: Instant::now(), state: Mutex::new(ClockState::default()) })
    }

    fn advance(&self, d: Duration) {
        let due = {
            let mut st = lock(&self.state);
            st.offset += d;
            let now = self.start + st.offset;
            let (due, pending) = std::mem::take(&mut st.timers).into_iter().partition(|(at, _, _)| *at <= now);
            st.timers = pending;
            due
        };
        for (_, done, f) in due {
            if !done.swap(true, Ordering::SeqCst) {
                f();
            }
        }
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        self.start + lock(&self.state).offset
    }
    fn after(&self, delay: Duration, f: Callback) -> Box<dyn Timer> {
        let done = Arc::new(AtomicBool::new(false));
        let at = self.now() + delay;
        lock(&self.state).timers.push((at, done.clone(), f));
        Box::new(ManualTimer(done))
    }
    fn ticker(&self, period: Duration) -> Arc<dyn Ticker> {
        SystemClock.ticker(period)
    }
    fn unix_micros(&self) -> i64 {
        UNIX_START_MICROS + i64::try_from(lock(&self.state).offset.as_micros()).expect("a short test")
    }
}

/// The wall-clock time of a [`ManualClock`] that has not been advanced.
const UNIX_START_MICROS: i64 = 1_800_000_000_000_000;

/// The memory broker, recording every session cache write (`commit <sid>`,
/// `touch <sid>`).
struct RecordingBroker {
    memory: Memory,
    writes: Mutex<Vec<String>>,
    /// Runs once when the next entry is fetched, before it is returned.
    on_restore: Mutex<Option<Callback>>,
}

impl RecordingBroker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            memory: Memory::new(None, None, &BrokerConfig::new(), Arc::new(BrokerClock)),
            writes: Mutex::new(Vec::new()),
            on_restore: Mutex::new(None),
        })
    }

    fn writes_for(&self, sid: &str) -> Vec<String> {
        lock(&self.writes).iter().filter(|w| w.ends_with(&format!(" {sid}"))).cloned().collect()
    }

    fn commits_for(&self, sid: &str) -> usize {
        self.writes_for(sid).iter().filter(|w| w.starts_with("commit ")).count()
    }
}

impl Broker for RecordingBroker {
    fn start(&self) -> Result<(), BrokerError> {
        self.memory.start()
    }
    fn shutdown(&self) -> Result<(), BrokerError> {
        self.memory.shutdown()
    }
    fn announce(&self) -> String {
        self.memory.announce()
    }
    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        self.memory.handle_broadcast(msg)
    }
    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        self.memory.handle_command(msg)
    }
    fn subscribe(&self, stream: &str) -> String {
        self.memory.subscribe(stream)
    }
    fn unsubscribe(&self, stream: &str) -> String {
        self.memory.unsubscribe(stream)
    }
    fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        self.memory.history_from(stream, epoch, offset)
    }
    fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        self.memory.history_since(stream, ts)
    }
    fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError> {
        lock(&self.writes).push(format!("commit {sid}"));
        self.memory.commit_session(sid, session)
    }
    fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        let entry = self.memory.restore_session(sid);
        if let Some(f) = lock(&self.on_restore).take() {
            f();
        }
        entry
    }
    fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        lock(&self.writes).push(format!("touch {sid}"));
        self.memory.touch_session(sid)
    }
    fn delete_session(&self, sid: &str) -> Result<(), BrokerError> {
        lock(&self.writes).push(format!("delete {sid}"));
        self.memory.delete_session(sid)
    }
    fn supports_presence(&self) -> bool {
        self.memory.supports_presence()
    }
    fn presence_add(
        &self,
        stream: &str,
        sid: &str,
        pid: &str,
        info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError> {
        self.memory.presence_add(stream, sid, pid, info)
    }
    fn presence_remove(&self, stream: &str, sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        self.memory.presence_remove(stream, sid)
    }
    fn presence_info(&self, stream: &str, opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        self.memory.presence_info(stream, opts)
    }
    fn touch_presence(&self, sid: &str) -> Result<(), BrokerError> {
        self.memory.touch_presence(sid)
    }
    fn peak(&self, stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        self.memory.peak(stream)
    }
}

/// Authenticates requests with an `x-user` header as `{"user":"<name>"}` and
/// rejects the others; confirms every subscription with the stream
/// `stream:<channel>`.
#[derive(Default)]
struct App {
    authentications: AtomicUsize,
}

impl App {
    fn authentications(&self) -> usize {
        self.authentications.load(Ordering::SeqCst)
    }
}

impl Controller for App {
    fn start(&self) -> Result<(), ControllerError> {
        Ok(())
    }
    fn shutdown(&self) -> Result<(), ControllerError> {
        Ok(())
    }
    fn authenticate(&self, sid: &str, env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        self.authentications.fetch_add(1, Ordering::SeqCst);
        let user = env.and_then(|e| e.headers.as_ref()).and_then(|h| h.get("x-user")).cloned();
        let result = match user {
            Some(user) => ConnectResult {
                status: Status::Success,
                identifier: format!(r#"{{"user":"{user}"}}"#),
                transmissions: vec![format!(r#"{{"type":"welcome","sid":"{sid}"}}"#)],
                ..Default::default()
            },
            None => ConnectResult {
                status: Status::Failure,
                transmissions: vec![disconnection_message(UNAUTHORIZED_REASON, false)],
                disconnect_interest: -1,
                ..Default::default()
            },
        };
        Outcome { result: Some(result), error: None }
    }
    fn subscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, channel: &str) -> Outcome<CommandResult> {
        Outcome {
            result: Some(CommandResult {
                status: Status::Success,
                streams: vec![format!("stream:{channel}")],
                transmissions: vec![format!(r#"{{"identifier":"{channel}","type":"confirm_subscription"}}"#)],
                ..Default::default()
            }),
            error: None,
        }
    }
    fn unsubscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, _channel: &str) -> Outcome<CommandResult> {
        Outcome { result: Some(CommandResult { status: Status::Success, ..Default::default() }), error: None }
    }
    fn perform(
        &self,
        _sid: &str,
        _env: Option<&SessionEnv>,
        _ids: &str,
        _channel: &str,
        _data: &str,
    ) -> Outcome<CommandResult> {
        Outcome { result: Some(CommandResult { status: Status::Success, ..Default::default() }), error: None }
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

/// Reports written frames and the close.
struct Wire {
    frames: Mutex<Sender<String>>,
}

impl Connection for Wire {
    fn write(&self, msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
        let _ = lock(&self.frames).send(String::from_utf8_lossy(msg).into_owned());
        Ok(())
    }
    fn write_binary(&self, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError> {
        self.write(msg, deadline)
    }
    fn write_ping(&self, _deadline: Instant) -> Result<(), ConnectionError> {
        Ok(())
    }
    fn read(&self) -> Result<Vec<u8>, ConnectionError> {
        Err(ConnectionError("websocket: close 1000 (normal)".into()))
    }
    fn close(&self, code: i32, _reason: &str) {
        let _ = lock(&self.frames).send(format!("close {code}"));
    }
}

/// Tells when the node hands a session to the disconnect queue: the end of
/// its teardown (after the cache touch).
struct Teardowns(Mutex<Sender<String>>);

impl Disconnector for Teardowns {
    fn run(&self) -> Result<(), NodeError> {
        Ok(())
    }
    fn shutdown(&self, _ctx: &crate::rpc::Context) -> Result<(), NodeError> {
        Ok(())
    }
    fn enqueue(&self, session: Arc<Session>) -> Result<(), NodeError> {
        let _ = lock(&self.0).send(session.get_id());
        Ok(())
    }
    fn size(&self) -> usize {
        0
    }
}

struct Rig {
    node: Arc<Node>,
    broker: Arc<RecordingBroker>,
    app: Arc<App>,
    clock: Arc<ManualClock>,
    teardowns: Receiver<String>,
}

struct Client {
    session: Arc<Session>,
    frames: Receiver<String>,
}

impl Client {
    /// The next frame (fails after [`WAIT`]).
    fn next(&self) -> String {
        self.frames.recv_timeout(WAIT).expect("a frame")
    }

    /// Frames until the connection closes.
    fn until_close(&self) -> Vec<String> {
        let mut frames = Vec::new();
        loop {
            let frame = self.next();
            if frame.starts_with("close ") {
                return frames;
            }
            frames.push(frame);
        }
    }
}

fn rig() -> Rig {
    let clock = ManualClock::new();
    let app = Arc::new(App::default());
    // No pings: the clients' frames are the node's replies only.
    let config = Config { ping_interval: 0, ..Config::new() };
    let node = Node::new(
        config,
        vec![NodeOption::Controller(app.clone()), NodeOption::Clock(clock.clone() as Arc<dyn Clock>)],
    );
    let broker = RecordingBroker::new();
    node.set_broker(broker.clone());
    node.set_sessions_ttl(SESSIONS_TTL);
    let (tx, teardowns) = channel();
    node.set_disconnector(Arc::new(Teardowns(Mutex::new(tx))));
    Rig { node, broker, app, clock, teardowns }
}

impl Rig {
    /// A session of the extended protocol (it uses the cache); `user` sends
    /// credentials, `prev_sid` asks to restore that session.
    fn client(&self, sid: &str, user: Option<&str>, prev_sid: Option<&str>) -> Client {
        let (tx, frames) = channel();
        let mut headers = HashMap::new();
        if let Some(user) = user {
            headers.insert("x-user".to_owned(), user.to_owned());
        }
        let mut opts =
            vec![SessionOption::KeepaliveIntervals { sessions_ttl: SESSIONS_TTL / 2, presence_ttl: Duration::ZERO }];
        if let Some(prev) = prev_sid {
            opts.push(SessionOption::PrevSid(prev.to_owned()));
        }
        let session =
            Session::new(&self.node, Arc::new(Wire { frames: Mutex::new(tx) }), "/cable", Some(headers), sid, opts);
        Client { session, frames }
    }

    /// Connects and authenticates (or restores) a client; returns it with its first frame.
    fn connect(&self, sid: &str, user: Option<&str>, prev_sid: Option<&str>) -> (Client, String) {
        let client = self.client(sid, user, prev_sid);
        self.node.authenticate(&client.session, &[]).expect("authenticate");
        let first = client.next();
        (client, first)
    }

    fn subscribe(&self, client: &Client, channel: &str) {
        let msg = Message { command: "subscribe".into(), identifier: channel.into(), ..Default::default() };
        self.node.subscribe(&client.session, &msg).expect("subscribe");
    }

    /// The client goes away (a network drop): the node tears the session down.
    fn drop_connection(&self, client: &Client) {
        client.session.disconnect("", CLOSE_ABNORMAL_CLOSURE);
        self.await_teardown(client);
    }

    fn await_teardown(&self, client: &Client) {
        assert_eq!(client.session.get_id(), self.teardowns.recv_timeout(WAIT).expect("a teardown"));
    }
}

fn is_restored(welcome: &str) -> bool {
    serde_json::from_str::<Value>(welcome).ok().and_then(|v| v["restored"].as_bool()).unwrap_or(false)
}

/// A rejected connection writes nothing to the session cache, as in AnyCable
/// 1.6.22; a later connection is authenticated by the application.
#[test]
fn rejected_sessions_are_not_cached() {
    let rig = rig();

    let (rejected, first) = rig.connect("client-chosen-id", None, None);
    assert_eq!(disconnection_message(UNAUTHORIZED_REASON, false), first);
    rejected.until_close();
    assert_eq!(Vec::<String>::new(), rig.broker.writes_for("client-chosen-id"));

    let (again, first) = rig.connect("next", None, Some("client-chosen-id"));
    assert_eq!(2, rig.app.authentications(), "the application authenticates the reconnection");
    assert!(!is_restored(&first), "{first}");
    assert_eq!(disconnection_message(UNAUTHORIZED_REASON, false), first);
    again.until_close();
    assert!(again.session.get_identifiers().is_empty());
}

/// Pins D-083: a rejected session that stays open
/// (`DisconnectOnFailure(false)`) writes nothing to the session cache either,
/// whatever it does before it goes.
#[test]
fn deviation_d083_rejected_sessions_left_open_are_not_cached() {
    let rig = rig();

    let open = rig.client("kept-open", None, None);
    let res = rig.node.authenticate(&open.session, &[AuthOption::DisconnectOnFailure(false)]).expect("authenticate");
    assert_eq!(Status::Failure, res.expect("a result").status);
    assert_eq!(disconnection_message(UNAUTHORIZED_REASON, false), open.next());
    assert!(!open.session.is_closed());

    rig.clock.advance(SESSIONS_TTL);
    rig.subscribe(&open, "chat");
    let unsubscribe = Message { command: "unsubscribe".into(), identifier: "chat".into(), ..Default::default() };
    rig.node.unsubscribe(&open.session, &unsubscribe).expect("unsubscribe");
    rig.node.disconnect(&open.session).expect("disconnect");
    assert_eq!(Vec::<String>::new(), rig.broker.writes_for("kept-open"));

    let (_, first) = rig.connect("next", None, Some("kept-open"));
    assert!(!is_restored(&first), "{first}");
    assert_eq!(2, rig.app.authentications());
}

/// Authenticated sessions commit and are restored without the application.
#[test]
fn authenticated_sessions_are_cached_and_restored() {
    let rig = rig();

    let (alice, first) = rig.connect("alice-1", Some("alice"), None);
    assert_eq!(r#"{"type":"welcome","sid":"alice-1"}"#, first);
    rig.subscribe(&alice, "chat");
    assert!(rig.broker.commits_for("alice-1") >= 1);
    rig.clock.advance(SESSIONS_TTL);
    assert!(rig.broker.writes_for("alice-1").contains(&"touch alice-1".to_owned()), "kept alive while connected");
    rig.drop_connection(&alice);

    let (restored, first) = rig.connect("alice-2", None, Some("alice-1"));
    assert_eq!(r#"{"type":"welcome","sid":"alice-2","restored":true,"restored_ids":["chat"]}"#, first);
    assert_eq!(1, rig.app.authentications(), "restored without the application");
    assert_eq!(r#"{"user":"alice"}"#, restored.session.get_identifiers());
    assert!(rig.broker.commits_for("alice-2") >= 1, "the restored session is cached under its new id");
}

const ALICE: &str = r#"{"user":"alice"}"#;

fn remote_disconnect(rig: &Rig, identifier: &str) {
    rig.node.remote_disconnect(&RemoteDisconnectMessage { identifier: identifier.into(), reconnect: true });
}

/// Alice connects, subscribes to `chat` and is cached under `sid`.
fn alice_subscribed(rig: &Rig, sid: &str) -> Client {
    let (alice, _) = rig.connect(sid, Some("alice"), None);
    rig.subscribe(&alice, "chat");
    alice
}

/// Deviation D-084: after a remote disconnect, the disconnected session is
/// authenticated by the application instead of restored. Its cache entry is
/// rewritten marked unrecoverable, then deleted when it disconnects, as in
/// AnyCable 1.6.22; it is not touched.
#[test]
fn deviation_d084_a_remotely_disconnected_session_is_not_restored() {
    let rig = rig();
    let alice = alice_subscribed(&rig, "alice-1");
    let mut writes = rig.broker.writes_for("alice-1");

    remote_disconnect(&rig, ALICE);
    let frames = alice.until_close();
    assert!(frames.contains(&r#"{"type":"disconnect","reason":"remote","reconnect":true}"#.to_owned()), "{frames:?}");
    rig.await_teardown(&alice);
    writes.extend(["commit alice-1".to_owned(), "delete alice-1".to_owned()]);
    assert_eq!(writes, rig.broker.writes_for("alice-1"), "an unrecoverable commit and a delete, no touch");
    assert_eq!(None, rig.broker.restore_session("alice-1").unwrap(), "the entry is deleted");

    let (again, first) = rig.connect("alice-2", None, Some("alice-1"));
    assert!(!is_restored(&first), "{first}");
    assert_eq!(disconnection_message(UNAUTHORIZED_REASON, false), first);
    assert_eq!(2, rig.app.authentications(), "the application authenticates the reconnection");
    again.until_close();
    assert!(again.session.get_identifiers().is_empty());
    assert!(again.session.subscriptions().channels().is_empty());
}

/// Deviation D-084: a refused restore is an ordinary authentication: with
/// credentials, the client is welcome again, without the old session's
/// subscriptions.
#[test]
fn deviation_d084_a_refused_restore_falls_back_to_authentication() {
    let rig = rig();
    let alice = alice_subscribed(&rig, "alice-1");
    remote_disconnect(&rig, ALICE);
    alice.until_close();
    rig.await_teardown(&alice);

    let (again, first) = rig.connect("alice-2", Some("alice"), Some("alice-1"));
    assert_eq!(r#"{"type":"welcome","sid":"alice-2"}"#, first);
    assert_eq!(2, rig.app.authentications());
    assert_eq!(ALICE, again.session.get_identifiers());
    assert!(again.session.subscriptions().channels().is_empty(), "nothing restored");
}

/// Deviation D-084: a session that was already offline when the remote
/// disconnect arrived is not restored either.
#[test]
fn deviation_d084_an_offline_session_is_not_restored_after_a_remote_disconnect() {
    let rig = rig();
    let alice = alice_subscribed(&rig, "alice-1");
    rig.drop_connection(&alice);

    remote_disconnect(&rig, ALICE);

    let (_, first) = rig.connect("alice-2", None, Some("alice-1"));
    assert!(!is_restored(&first), "{first}");
    assert_eq!(2, rig.app.authentications());
}

/// Deviation D-084 concerns the disconnected identifiers only: after a
/// network drop, and after a remote disconnect of someone else, sessions are
/// restored as before.
#[test]
fn deviation_d084_other_sessions_are_restored() {
    let rig = rig();
    let alice = alice_subscribed(&rig, "alice-1");
    rig.drop_connection(&alice);

    remote_disconnect(&rig, r#"{"user":"bob"}"#);

    let (restored, first) = rig.connect("alice-2", None, Some("alice-1"));
    assert_eq!(r#"{"type":"welcome","sid":"alice-2","restored":true,"restored_ids":["chat"]}"#, first);
    assert_eq!(1, rig.app.authentications());
    assert_eq!(ALICE, restored.session.get_identifiers());
}

/// Deviation D-084: a session of the same identifiers authenticated after the
/// remote disconnect is cached and restored as usual; the earlier one still
/// is not.
#[test]
fn deviation_d084_a_session_authenticated_afterwards_is_restored() {
    let rig = rig();
    let alice = alice_subscribed(&rig, "alice-1");
    rig.drop_connection(&alice);
    remote_disconnect(&rig, ALICE);

    rig.clock.advance(Duration::from_secs(2));
    let later = alice_subscribed(&rig, "alice-3");
    rig.drop_connection(&later);

    let (restored, first) = rig.connect("alice-4", None, Some("alice-3"));
    assert_eq!(r#"{"type":"welcome","sid":"alice-4","restored":true,"restored_ids":["chat"]}"#, first);
    assert_eq!(ALICE, restored.session.get_identifiers());
    assert_eq!(2, rig.app.authentications());

    // The restored session keeps its authentication time: it is cached and
    // restored again.
    rig.drop_connection(&restored);
    let (_, first) = rig.connect("alice-5", None, Some("alice-4"));
    assert!(is_restored(&first), "{first}");

    let (_, first) = rig.connect("alice-6", None, Some("alice-1"));
    assert!(!is_restored(&first), "{first}");
    assert_eq!(3, rig.app.authentications());
}

/// Deviation D-084: an entry that a broker still returns just after its TTL
/// (the memory broker until its next sweep) is refused too: the record lasts
/// longer than the sessions TTL.
#[test]
fn deviation_d084_an_entry_read_just_after_its_ttl_is_refused() {
    let rig = rig();
    let alice = alice_subscribed(&rig, "alice-1");
    rig.drop_connection(&alice);
    remote_disconnect(&rig, ALICE);

    // The memory broker here never sweeps (it is not started): the entry stays
    // readable past its TTL.
    rig.clock.advance(SESSIONS_TTL + Duration::from_secs(1));
    let (_, first) = rig.connect("alice-2", None, Some("alice-1"));
    assert!(!is_restored(&first), "{first}");
    assert_eq!(2, rig.app.authentications());
}

/// Deviation D-084: the record of a remote disconnect expires the sessions TTL
/// plus a margin after it, when every entry written before it is gone (the
/// memory broker here keeps them, as it is not started).
#[test]
fn deviation_d084_the_record_expires_after_the_sessions_ttl_and_a_margin() {
    let rig = rig();
    let alice = alice_subscribed(&rig, "alice-1");
    rig.drop_connection(&alice);
    remote_disconnect(&rig, ALICE);

    let kept = SESSIONS_TTL + super::revocations::EXPIRY_MARGIN;
    rig.clock.advance(kept - Duration::from_secs(1));
    let (_, first) = rig.connect("alice-2", None, Some("alice-1"));
    assert!(!is_restored(&first), "{first}");

    rig.clock.advance(Duration::from_secs(1));
    let (_, first) = rig.connect("alice-3", None, Some("alice-1"));
    assert!(is_restored(&first), "{first}");
}

/// Deviation D-084: a remote disconnect that arrives while a restore is
/// fetching the entry refuses that restore too.
#[test]
fn deviation_d084_a_remote_disconnect_during_a_restore_refuses_it() {
    let rig = rig();
    let alice = alice_subscribed(&rig, "alice-1");
    rig.drop_connection(&alice);

    let node = Arc::downgrade(&rig.node);
    *lock(&rig.broker.on_restore) = Some(Box::new(move || {
        let node = node.upgrade().expect("the node");
        node.remote_disconnect(&RemoteDisconnectMessage { identifier: ALICE.into(), reconnect: true });
    }));

    let (again, first) = rig.connect("alice-2", None, Some("alice-1"));
    assert!(!is_restored(&first), "{first}");
    assert_eq!(2, rig.app.authentications());
    again.until_close();
    assert_eq!(0, rig.node.size(), "nothing left in the hub");
}

/// Deviation D-084: once its identifiers are disconnected remotely, an
/// earlier session writes nothing to the cache, however late the write comes
/// (a command that finished after the disconnect, a keepalive); a session
/// authenticated afterwards writes as usual.
#[test]
fn deviation_d084_earlier_sessions_leave_their_entries_as_they_are() {
    let rig = rig();
    let alice = alice_subscribed(&rig, "alice-1");
    remote_disconnect(&rig, ALICE);
    alice.until_close();
    rig.await_teardown(&alice);

    rig.clock.advance(Duration::from_secs(2));
    rig.node.write_session_cache(&alice.session, || panic!("a late write of the disconnected session"));

    let later = alice_subscribed(&rig, "alice-2");
    let written = AtomicBool::new(false);
    rig.node.write_session_cache(&later.session, || written.store(true, Ordering::SeqCst));
    assert!(written.load(Ordering::SeqCst));
}
