//! upstream file: node/broker_integration_test.go
//!
//! A node with a real broker: the in-memory broker, or the NATS broker against
//! a real NATS server with JetStream (`cabby_testkit::nats::NatsServer`, an
//! external process on its own port and storage directory; upstream starts the
//! embedded `enats` service on fixed ports 4532/4533). A missing NATS server
//! fails the test; it is never skipped.
//!
//! Time:
//! * Memory variants run the node (session keepalive timers) and the broker
//!   (TTLs) on one [`ManualTimers`] clock. Each upstream `time.Sleep(d)` is an
//!   advance of `d` in one-second steps, each followed by `expire()`: upstream's
//!   memory broker expires entries from a background loop every second.
//! * NATS variants keep wall-clock time: the NATS server enforces the TTLs on
//!   its own clock, so the node uses the system clock and each `time.Sleep(d)`
//!   is a real wait of `d` (it lets TTLs elapse; it never orders events). NATS
//!   cases run one at a time.
//!
//! Upstream subtests share the setup performed before them (authentication,
//! subscriptions, broadcasts, the disconnect and the restore); every Rust test
//! runs that setup, then its subtest.
//!
//! Against Go these cases are not observable: they set unexported session
//! fields (`pingInterval`), call unexported `startTimers`, read `session.conn`
//! and script testify controller callbacks.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use cabby_core::broker::{Broker, Memory, Nats};
use cabby_core::common::{
    CommandResult, ConnectResult, HistoryPosition, HistoryRequest, Message, Outcome, PRESENCE_STREAM_STATE,
    PresenceEvent, Status,
};
use cabby_server::node::NoopDisconnectQueue;
use cabby_testkit::nats::NatsServer;
use cabby_testkit::reference::broker as broker_facade;
use cabby_testkit::reference::node::*;
use serde_json::{Value, json};

use super::support::*;

const SECOND: Duration = Duration::from_secs(1);

fn ok<T>(r: T) -> Outcome<T> {
    Outcome { result: Some(r), error: None }
}

/// Held for the whole test: one NATS case at a time (their waits are real time).
fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// The broker under the node and how time passes for it.
enum Rig<'a> {
    Memory { timers: &'a cabby_testkit::timers::ManualTimers, broker: Arc<Memory> },
    Nats,
}

impl Rig<'_> {
    /// Upstream `time.Sleep(d)` (see the module documentation).
    fn sleep(&self, d: Duration) {
        match self {
            Rig::Memory { timers, broker } => {
                let mut left = d;
                while left >= SECOND {
                    timers.advance(SECOND);
                    broker.expire();
                    left -= SECOND;
                }
                if !left.is_zero() {
                    timers.advance(left);
                }
            }
            Rig::Nats => std::thread::sleep(d),
        }
    }

    /// Go `time.Now().Unix()` on the rig's clock.
    fn unix_now(&self) -> i64 {
        match self {
            Rig::Memory { timers, .. } => timers.unix(),
            Rig::Nats => {
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("after epoch").as_secs()
                    as i64
            }
        }
    }
}

/// Upstream `setupIntegrationNode()`: `HubGopoolSize = 2`, disconnect mode
/// "never", a testify controller expecting `Shutdown`, metrics, no-op disconnector.
fn setup_integration_node(system_clock: bool) -> (TestNode, Arc<MockNodeController>) {
    let mut config = new_config();
    config.hub_gopool_size = 2;
    config.disconnect_mode = DISCONNECT_MODE_NEVER.into();

    let controller = MockNodeController::new();
    controller.on("Shutdown", vec![], |_| Ok::<(), cabby_core::common::ControllerError>(()));

    let node = if system_clock {
        new_node_with_controller_on_system_clock(config, controller.clone())
    } else {
        new_node_with_controller(config, controller.clone())
    };
    set_disconnector(&node, Arc::new(NoopDisconnectQueue));

    (node, controller)
}

/// `defer node.Shutdown(context.Background())`.
struct ShutdownNode<'a>(&'a TestNode);

impl Drop for ShutdownNode<'_> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ = self.0.rust().shutdown(&background());
        }
    }
}

/// Upstream `startNATSServer`.
fn start_nats_server() -> NatsServer {
    NatsServer::start()
}

fn nats_config(server: &NatsServer) -> cabby_core::broker::NatsConfig {
    let mut nconfig = broker_facade::new_nats_config();
    nconfig.servers = server.url();
    nconfig
}

/// Memory setup shared by the restore and history cases: `bconf` → broker with
/// epoch "2022" → `br.Start` → `go node.Start()`.
fn with_memory_node(
    epoch: Option<&str>,
    tune: impl FnOnce(&mut cabby_core::broker::Config),
    run: impl FnOnce(&TestNode, &MockNodeController, &Rig<'_>),
) {
    let (node, controller) = setup_integration_node(false);

    let mut bconf = broker_facade::new_config();
    tune(&mut bconf);

    let br = new_memory_broker_for(&node, &bconf);
    if let Some(e) = epoch {
        br.set_epoch(e);
    }
    set_broker(&node, br.clone());

    br.start().expect("broker start");

    let _ = node.rust().start(); // upstream: go node.Start()
    let _shutdown = ShutdownNode(&node);

    let rig = Rig::Memory { timers: node.timers(), broker: br };
    run(&node, &controller, &rig);
}

/// NATS setup shared by the restore and history cases.
fn with_nats_node(
    tune: impl FnOnce(&mut cabby_core::broker::Config),
    run: impl FnOnce(&TestNode, &MockNodeController, &Rig<'_>),
) {
    let _serial = serial();
    let server = start_nats_server();

    let (node, controller) = setup_integration_node(true);

    let mut bconf = broker_facade::new_config();
    tune(&mut bconf);

    let broker: Arc<Nats> = new_nats_broker_for(&node, &bconf, &nats_config(&server));
    set_broker(&node, broker.clone());

    node.rust().start().expect("node start");
    broker.start().expect("broker start");
    let _shutdown = ShutdownNode(&node);

    broker.reset().expect("reset");
    broker.set_epoch("2022").expect("set epoch");

    run(&node, &controller, &Rig::Nats);
}

/// Upstream `requireAuthenticatedSession(t, node, sid, opts...)`.
fn require_authenticated_session(
    node: &TestNode,
    controller: &MockNodeController,
    sid: &str,
    opts: Vec<SessionOpt>,
) -> TestSession {
    let session = new_mock_session_with_env(sid, node, "ws://test.anycable.io/cable", None, opts);

    // Upstream matches `session.env` (the session's own pointer, so any env of this call).
    let id = sid.to_owned();
    controller.on("Authenticate", vec![eq(sid), any()], move |_| {
        ok(ConnectResult {
            identifier: id.clone(),
            status: Status::Success,
            transmissions: vec![r#"{"type":"welcome"}"#.into()],
            ..Default::default()
        })
    });

    node.authenticate(&session).expect("authenticate");

    require_receive(&session, r#"{"type":"welcome"}"#);

    session
}

// ------------------------------------------------------------------- restore

#[derive(Clone, Copy, PartialEq, Eq)]
enum RestoreCase {
    HubSubscriptions,
    States,
    Expired,
}

/// Upstream `sharedIntegrationRestore` up to the restored welcome, then one subtest.
///
/// SETUP: a session subscribes to channels/streams, receives broadcasts, then
/// disconnects. EXECUTE: a new session restores from its sid. TEST 1: hub
/// subscriptions; TEST 2: connection/channel state; TEST 3: an expired cache
/// is not restored (`controller.Authenticate` runs instead).
fn shared_integration_restore(node: &TestNode, controller: &MockNodeController, rig: &Rig<'_>, case: RestoreCase) {
    let sid = "s18";
    let ids = "user:jack";

    let prev_session = new_mock_session_with_env(
        sid,
        node,
        "ws://test.anycable.io/cable",
        None,
        vec![SessionOpt::KeepaliveIntervals(Duration::from_millis(1500), Duration::ZERO)],
    );
    // do not send pings
    prev_session.rust().set_ping_interval(Duration::ZERO);
    prev_session.rust().start_timers();

    controller.on("Authenticate", vec![eq(sid), any()], move |_| {
        ok(ConnectResult {
            identifier: ids.into(),
            status: Status::Success,
            transmissions: vec![r#"{"type":"welcome"}"#.into()],
            cstate: Some(map(&[("city", "Napoli")])),
            ..Default::default()
        })
    });

    node.authenticate(&prev_session).expect("authenticate");

    require_receive(&prev_session, r#"{"type":"welcome"}"#);

    // Subscribe the channels
    controller.on("Subscribe", vec![eq(sid), any(), eq(ids), eq("chat_1")], |_| {
        ok(CommandResult {
            status: Status::Success,
            transmissions: vec![r#"{"type":"confirm","identifier":"chat_1"}"#.into()],
            streams: vec!["presence_1".into(), "messages_1".into()],
            ..Default::default()
        })
    });
    controller.on("Subscribe", vec![eq(sid), any(), eq(ids), eq("user_jack")], |_| {
        ok(CommandResult {
            status: Status::Success,
            transmissions: vec![r#"{"type":"confirm","identifier":"user_jack"}"#.into()],
            streams: vec!["u_jack".into()],
            istate: Some(map(&[("locale", "it")])),
            ..Default::default()
        })
    });

    node.rust().subscribe(prev_session.rust(), &cmd("subscribe", "chat_1")).expect("subscribe");

    require_receive(&prev_session, r#"{"type":"confirm","identifier":"chat_1"}"#);

    node.rust().subscribe(prev_session.rust(), &cmd("subscribe", "user_jack")).expect("subscribe");

    require_receive(&prev_session, r#"{"type":"confirm","identifier":"user_jack"}"#);

    node.rust().handle_broadcast(br#"{"stream": "messages_1", "data": "Alice: Hey!"}"#).expect("broadcast");
    require_receive(
        &prev_session,
        r#"{"identifier":"chat_1","message":"Alice: Hey!","stream_id":"messages_1","epoch":"2022","offset":1}"#,
    );

    node.rust().handle_broadcast(br#"{"stream": "u_jack", "data": "New message from Alice"}"#).expect("broadcast");
    require_receive(
        &prev_session,
        r#"{"identifier":"user_jack","message":"New message from Alice","stream_id":"u_jack","epoch":"2022","offset":1}"#,
    );

    // wait before disconnecting to ensure that the session's cache is not expired
    // while the session is still connected
    rig.sleep(Duration::from_secs(3));

    prev_session.rust().disconnect("normal", CLOSE_NORMAL_CLOSURE);

    let session = new_mock_session_with_env(
        "s21",
        node,
        &format!("ws://test.anycable.io/cable?sid={sid}"),
        None,
        vec![
            SessionOpt::KeepaliveIntervals(Duration::from_millis(500), Duration::ZERO),
            SessionOpt::PrevSid(sid.into()),
        ],
    );

    node.authenticate(&session).expect("authenticate");

    let welcome_msg = session.read().expect("welcome");

    let welcome: Value = serde_json::from_slice(&welcome_msg).expect("welcome JSON");

    assert_eq!(json!("welcome"), welcome["type"]);
    assert_eq!(json!("s21"), welcome["sid"]);
    assert_eq!(json!(true), welcome["restored"]);
    let restored_ids = welcome["restored_ids"].as_array().cloned().unwrap_or_default();
    assert!(restored_ids.contains(&json!("chat_1")), "{welcome}");
    assert!(restored_ids.contains(&json!("user_jack")), "{welcome}");

    match case {
        RestoreCase::HubSubscriptions => {
            node.rust().handle_broadcast(br#"{"stream": "messages_1", "data": "Lorenzo: Ciao"}"#).expect("broadcast");
            require_receive(
                &session,
                r#"{"identifier":"chat_1","message":"Lorenzo: Ciao","stream_id":"messages_1","epoch":"2022","offset":2}"#,
            );

            let _ = node.rust().handle_broadcast(br#"{"stream": "presence_1", "data": "@lorenzo:join"}"#);
            require_receive(
                &session,
                r#"{"identifier":"chat_1","message":"@lorenzo:join","stream_id":"presence_1","epoch":"2022","offset":1}"#,
            );

            let _ = node.rust().handle_broadcast(br#"{"stream": "u_jack", "data": "1:1"}"#);
            require_receive(
                &session,
                r#"{"identifier":"user_jack","message":"1:1","stream_id":"u_jack","epoch":"2022","offset":2}"#,
            );
        }
        RestoreCase::States => {
            controller.on("Perform", vec![eq("s21"), any(), eq(ids), eq("user_jack"), eq("echo")], |a| {
                let env = &a[1];
                ok(CommandResult {
                    status: Status::Success,
                    transmissions: vec![
                        format!("city:{}", env_arg_connection_state(env, "city")),
                        format!("locale:{}", env_arg_channel_state(env, "user_jack", "locale")),
                    ],
                    ..Default::default()
                })
            });

            let mut m = cmd("message", "user_jack");
            m.data = Some(cabby_core::common::MessageData::Json(json!("echo")));
            node.rust().perform(session.rust(), &m).expect("perform");

            require_receive(&session, "city:Napoli");
            require_receive(&session, "locale:it");
        }
        RestoreCase::Expired => {
            controller.on("Authenticate", vec![eq("s42"), any()], move |_| {
                ok(ConnectResult {
                    identifier: ids.into(),
                    status: Status::Success,
                    transmissions: vec![r#"{"type":"welcome","restored":false}"#.into()],
                    ..Default::default()
                })
            });

            let new_session = new_mock_session_with_env(
                "s42",
                node,
                &format!("ws://test.anycable.io/cable?sid={sid}"),
                None,
                vec![
                    SessionOpt::KeepaliveIntervals(Duration::from_millis(500), Duration::ZERO),
                    SessionOpt::PrevSid(sid.into()),
                ],
            );

            rig.sleep(Duration::from_secs(4));

            node.authenticate(&new_session).expect("authenticate");

            require_receive(&new_session, r#"{"type":"welcome","restored":false}"#);
        }
    }
}

fn restore_memory(case: RestoreCase) {
    with_memory_node(
        Some("2022"),
        |c| c.sessions_ttl = 2,
        |node, controller, rig| shared_integration_restore(node, controller, rig, case),
    );
}

fn restore_nats(case: RestoreCase) {
    with_nats_node(
        |c| c.sessions_ttl = 2,
        |node, controller, rig| shared_integration_restore(node, controller, rig, case),
    );
}

mod test_integration_restore_memory {
    use super::*;

    /// upstream: node/broker_integration_test.go#TestIntegrationRestore_Memory/Restore_hub_subscriptions
    /// upstream: node/broker_integration_test.go#TestIntegrationRestore_Memory
    #[test]
    fn restore_hub_subscriptions() {
        restore_memory(RestoreCase::HubSubscriptions);
    }

    /// upstream: node/broker_integration_test.go#TestIntegrationRestore_Memory/Restore_session_connection_and_channels_state
    /// upstream: node/broker_integration_test.go#TestIntegrationRestore_Memory
    #[test]
    fn restore_session_connection_and_channels_state() {
        restore_memory(RestoreCase::States);
    }

    /// upstream: node/broker_integration_test.go#TestIntegrationRestore_Memory/Not_restored_when_cache_expired
    /// upstream: node/broker_integration_test.go#TestIntegrationRestore_Memory
    #[test]
    fn not_restored_when_cache_expired() {
        restore_memory(RestoreCase::Expired);
    }
}

mod test_integration_restore_nats {
    use super::*;

    /// upstream: node/broker_integration_test.go#TestIntegrationRestore_NATS/Restore_hub_subscriptions
    /// upstream: node/broker_integration_test.go#TestIntegrationRestore_NATS
    #[test]
    fn restore_hub_subscriptions() {
        restore_nats(RestoreCase::HubSubscriptions);
    }

    /// upstream: node/broker_integration_test.go#TestIntegrationRestore_NATS/Restore_session_connection_and_channels_state
    /// upstream: node/broker_integration_test.go#TestIntegrationRestore_NATS
    #[test]
    fn restore_session_connection_and_channels_state() {
        restore_nats(RestoreCase::States);
    }

    /// upstream: node/broker_integration_test.go#TestIntegrationRestore_NATS/Not_restored_when_cache_expired
    /// upstream: node/broker_integration_test.go#TestIntegrationRestore_NATS
    #[test]
    fn not_restored_when_cache_expired() {
        restore_nats(RestoreCase::Expired);
    }
}

// ------------------------------------------------------------------- history

#[derive(Clone, Copy, PartialEq, Eq)]
enum HistoryCase {
    SubscribeWithHistory,
    SubscribeThenHistory,
}

fn history_msg(command: &str, identifier: &str, history: HistoryRequest) -> Message {
    let mut m = cmd(command, identifier);
    m.history = history;
    m
}

/// Upstream `sharedIntegrationHistory`: a backlog is published, then one subtest.
///
/// TEST 1: subscribe with a history request (Since): confirmation, then the
/// backlog, then `confirm_history`. TEST 2: subscribe, then a history request
/// with stream offsets: the messages after the offset.
fn shared_integration_history(node: &TestNode, controller: &MockNodeController, rig: &Rig<'_>, case: HistoryCase) {
    let _ = node.rust().handle_broadcast(br#"{"stream": "messages_1","data":"Lorenzo: Ciao"}"#);

    // Use sleep to make sure Since option works (and we don't want
    // to hack broker internals to update stream messages timestamps)
    rig.sleep(Duration::from_secs(2));
    let ts = rig.unix_now();

    let _ = node.rust().handle_broadcast(br#"{"stream": "messages_1","data":"Flavia: buona sera"}"#);
    // Transient messages must not be stored in the history
    let _ =
        node.rust().handle_broadcast(br#"{"stream": "messages_1","data":"Who's there?","meta":{"transient":true}}"#);
    let _ = node.rust().handle_broadcast(br#"{"stream": "messages_1","data":"Mario: ta-dam!"}"#);

    let _ = node.rust().handle_broadcast(br#"{"stream": "presence_1","data":"1 new notification"}"#);
    let _ = node.rust().handle_broadcast(br#"{"stream": "presence_1","data":"2 new notifications"}"#);
    let _ = node.rust().handle_broadcast(br#"{"stream": "presence_1","data":"3 new notifications"}"#);
    let _ = node.rust().handle_broadcast(br#"{"stream": "presence_1","data":"4 new notifications"}"#);
    let _ = node.rust().handle_broadcast(br#"{"stream": "presence_1","data":"100+ new notifications"}"#);

    match case {
        HistoryCase::SubscribeWithHistory => {
            let session = require_authenticated_session(node, controller, "alice", vec![]);

            controller.on("Subscribe", vec![eq("alice"), any(), eq("alice"), eq("chat_1")], |_| {
                ok(CommandResult {
                    status: Status::Success,
                    streams: vec!["messages_1".into()],
                    transmissions: vec![r#"{"type":"confirm","identifier":"chat_1"}"#.into()],
                    ..Default::default()
                })
            });

            node.rust()
                .subscribe(
                    session.rust(),
                    &history_msg("subscribe", "chat_1", HistoryRequest { since: ts, ..Default::default() }),
                )
                .expect("subscribe");

            assert_receive(&session, r#"{"type":"confirm","identifier":"chat_1"}"#);
            assert_receive(
                &session,
                r#"{"identifier":"chat_1","message":"Flavia: buona sera","stream_id":"messages_1","epoch":"2022","offset":2}"#,
            );
            assert_receive(
                &session,
                r#"{"identifier":"chat_1","message":"Mario: ta-dam!","stream_id":"messages_1","epoch":"2022","offset":3}"#,
            );
            assert_receive(&session, r#"{"type":"confirm_history","identifier":"chat_1"}"#);
        }
        HistoryCase::SubscribeThenHistory => {
            let session = require_authenticated_session(node, controller, "bob", vec![]);

            controller.on("Subscribe", vec![eq("bob"), any(), eq("bob"), eq("chat_1")], |_| {
                ok(CommandResult {
                    status: Status::Success,
                    streams: vec!["messages_1".into(), "presence_1".into()],
                    transmissions: vec![r#"{"type":"confirm","identifier":"chat_1"}"#.into()],
                    ..Default::default()
                })
            });

            node.rust().subscribe(session.rust(), &cmd("subscribe", "chat_1")).expect("subscribe");

            require_receive(&session, r#"{"type":"confirm","identifier":"chat_1"}"#);

            let history = HistoryRequest {
                streams: [("presence_1".to_owned(), HistoryPosition { epoch: "2022".into(), offset: 2 })].into(),
                ..Default::default()
            };
            node.rust().history(session.rust(), &history_msg("history", "chat_1", history)).expect("history");

            assert_receive(
                &session,
                r#"{"identifier":"chat_1","message":"3 new notifications","stream_id":"presence_1","epoch":"2022","offset":3}"#,
            );
            assert_receive(
                &session,
                r#"{"identifier":"chat_1","message":"4 new notifications","stream_id":"presence_1","epoch":"2022","offset":4}"#,
            );
            assert_receive(
                &session,
                r#"{"identifier":"chat_1","message":"100+ new notifications","stream_id":"presence_1","epoch":"2022","offset":5}"#,
            );
            assert_receive(&session, r#"{"type":"confirm_history","identifier":"chat_1"}"#);
        }
    }
}

mod test_integration_history_memory {
    use super::*;

    fn run(case: HistoryCase) {
        with_memory_node(
            Some("2022"),
            |_| {},
            |node, controller, rig| shared_integration_history(node, controller, rig, case),
        );
    }

    /// upstream: node/broker_integration_test.go#TestIntegrationHistory_Memory/Subscribe_with_history
    /// upstream: node/broker_integration_test.go#TestIntegrationHistory_Memory
    #[test]
    fn subscribe_with_history() {
        run(HistoryCase::SubscribeWithHistory);
    }

    /// upstream: node/broker_integration_test.go#TestIntegrationHistory_Memory/Subscribe_+_History
    /// upstream: node/broker_integration_test.go#TestIntegrationHistory_Memory
    #[test]
    fn subscribe_and_history() {
        run(HistoryCase::SubscribeThenHistory);
    }
}

mod test_integration_history_nats {
    use super::*;

    fn run(case: HistoryCase) {
        with_nats_node(|_| {}, |node, controller, rig| shared_integration_history(node, controller, rig, case));
    }

    /// upstream: node/broker_integration_test.go#TestIntegrationHistory_NATS/Subscribe_with_history
    /// upstream: node/broker_integration_test.go#TestIntegrationHistory_NATS
    #[test]
    fn subscribe_with_history() {
        run(HistoryCase::SubscribeWithHistory);
    }

    /// upstream: node/broker_integration_test.go#TestIntegrationHistory_NATS/Subscribe_+_History
    /// upstream: node/broker_integration_test.go#TestIntegrationHistory_NATS
    #[test]
    fn subscribe_and_history() {
        run(HistoryCase::SubscribeThenHistory);
    }
}

// ------------------------------------------------------------------ presence

#[derive(Clone, Copy, PartialEq, Eq)]
enum PresenceCase {
    JoinAndLeave,
    Expiration,
}

fn presence_join(id: &str, name: &str) -> PresenceEvent {
    PresenceEvent { id: id.into(), info: Some(json!({"name": name})), ..Default::default() }
}

fn with_presence(identifier: &str, presence: PresenceEvent) -> Message {
    let mut m = msg(identifier);
    m.presence = Some(presence);
    m
}

/// Upstream `sharedIntegrationPresence`.
///
/// SETUP: two sessions authenticate and subscribe with a presence stream.
/// TEST 1: join/leave events and presence info. TEST 2: a disconnected
/// session's presence expires and the other session receives the leave event.
fn shared_integration_presence(node: &TestNode, controller: &MockNodeController, rig: &Rig<'_>, case: PresenceCase) {
    for sid in ["sasha", "mia"] {
        controller.on("Subscribe", vec![eq(sid), any(), eq(sid), eq("chat_1")], |_| {
            ok(CommandResult {
                status: Status::Success,
                transmissions: vec![r#"{"type":"confirm","identifier":"chat_1"}"#.into()],
                streams: vec!["presence_1".into(), "messages_1".into()],
                istate: Some(map(&[(PRESENCE_STREAM_STATE, "presence_1")])),
                ..Default::default()
            })
        });
    }
    controller.on("Unsubscribe", vec![any(), any(), any(), any()], |_| {
        ok(CommandResult { status: Status::Success, ..Default::default() })
    });

    let setup_session = |sid: &str| {
        let session = require_authenticated_session(
            node,
            controller,
            sid,
            vec![SessionOpt::KeepaliveIntervals(Duration::ZERO, Duration::from_millis(500))],
        );
        session.rust().start_timers();

        node.rust().subscribe(session.rust(), &cmd("subscribe", "chat_1")).expect("subscribe");
        assert_receive(&session, r#"{"type":"confirm","identifier":"chat_1"}"#);
        session
    };

    let sasha = setup_session("sasha");
    let mia = setup_session("mia");

    let cleanup = |sasha: &TestSession, mia: &TestSession| {
        // Unsubscribe to ensure sessions are removed from the presence set
        let _ = node.rust().unsubscribe(sasha.rust(), &cmd("unsubscribe", "chat_1"));
        let _ = node.rust().unsubscribe(mia.rust(), &cmd("unsubscribe", "chat_1"));

        // Disconnect to ensure timers are stopped and no conflicts between examples
        sasha.rust().disconnect_now("normal", 1);
        mia.rust().disconnect_now("normal", 1);
    };

    let presence = |s: &TestSession| node.rust().presence(s.rust(), &cmd("presence", "chat_1")).expect("presence");

    match case {
        PresenceCase::JoinAndLeave => {
            node.rust()
                .presence_join(sasha.rust(), &with_presence("chat_1", presence_join("42", "Sasha")))
                .expect("join");

            assert_receive(
                &mia,
                r#"{"type":"presence","identifier":"chat_1","message":{"id":"42","info":{"name":"Sasha"},"type":"join"}}"#,
            );
            assert_receive(
                &sasha,
                r#"{"type":"presence","identifier":"chat_1","message":{"id":"42","info":{"name":"Sasha"},"type":"join"}}"#,
            );

            presence(&mia);

            assert_receive(
                &mia,
                r#"{"type":"presence","identifier":"chat_1","message":{"type":"info","total":1,"records":[{"id":"42","info":{"name":"Sasha"}}]}}"#,
            );

            node.rust().presence_join(mia.rust(), &with_presence("chat_1", presence_join("13", "Mia"))).expect("join");

            assert_receive(
                &mia,
                r#"{"type":"presence","identifier":"chat_1","message":{"id":"13","info":{"name":"Mia"},"type":"join"}}"#,
            );
            assert_receive(
                &sasha,
                r#"{"type":"presence","identifier":"chat_1","message":{"id":"13","info":{"name":"Mia"},"type":"join"}}"#,
            );

            presence(&sasha);

            let msg = assert_receive_msg(&sasha);
            assert_eq!(json!("presence"), msg["type"]);
            assert_eq!(json!("chat_1"), msg["identifier"]);
            let payload = msg["message"].as_object().expect("presence payload");
            assert_eq!(Some(&json!("info")), payload.get("type"));
            assert_eq!(Some(2.0), payload.get("total").and_then(Value::as_f64));
            let records = payload["records"].as_array().expect("records");
            assert_eq!(2, records.len());
            assert!(records.contains(&json!({"id": "42", "info": {"name": "Sasha"}})));
            assert!(records.contains(&json!({"id": "13", "info": {"name": "Mia"}})));

            node.rust().presence_leave(sasha.rust(), &msg_ident("chat_1")).expect("leave");

            assert_receive(&mia, r#"{"type":"presence","identifier":"chat_1","message":{"id":"42","type":"leave"}}"#);
            assert_receive(&sasha, r#"{"type":"presence","identifier":"chat_1","message":{"id":"42","type":"leave"}}"#);

            presence(&mia);

            assert_receive(
                &mia,
                r#"{"type":"presence","identifier":"chat_1","message":{"type":"info","total":1,"records":[{"id":"13","info":{"name":"Mia"}}]}}"#,
            );
        }
        PresenceCase::Expiration => {
            node.rust()
                .presence_join(sasha.rust(), &with_presence("chat_1", presence_join("142", "Rickie")))
                .expect("join");

            assert_receive(
                &mia,
                r#"{"type":"presence","identifier":"chat_1","message":{"id":"142","info":{"name":"Rickie"},"type":"join"}}"#,
            );
            assert_receive(
                &sasha,
                r#"{"type":"presence","identifier":"chat_1","message":{"id":"142","info":{"name":"Rickie"},"type":"join"}}"#,
            );

            presence(&mia);

            assert_receive(
                &mia,
                r#"{"type":"presence","identifier":"chat_1","message":{"type":"info","total":1,"records":[{"id":"142","info":{"name":"Rickie"}}]}}"#,
            );

            // Make sure session doesn't expire while it's still connected
            rig.sleep(Duration::from_secs(3));

            presence(&mia);

            assert_receive(
                &mia,
                r#"{"type":"presence","identifier":"chat_1","message":{"type":"info","total":1,"records":[{"id":"142","info":{"name":"Rickie"}}]}}"#,
            );

            sasha.rust().disconnect_now("normal", 1);

            // Wait for expiration to happen
            rig.sleep(Duration::from_secs(4));

            assert_receive(&mia, r#"{"type":"presence","identifier":"chat_1","message":{"id":"142","type":"leave"}}"#);

            presence(&mia);

            assert_receive(&mia, r#"{"type":"presence","identifier":"chat_1","message":{"type":"info","total":0}}"#);
        }
    }

    cleanup(&sasha, &mia);
}

fn msg_ident(identifier: &str) -> Message {
    msg(identifier)
}

mod test_integration_presence_memory {
    use super::*;

    fn run(case: PresenceCase) {
        with_memory_node(
            None,
            |c| c.presence_ttl = 2,
            |node, controller, rig| shared_integration_presence(node, controller, rig, case),
        );
    }

    /// upstream: node/broker_integration_test.go#TestIntegrationPresence_Memory/Join_and_leave
    /// upstream: node/broker_integration_test.go#TestIntegrationPresence_Memory
    #[test]
    fn join_and_leave() {
        run(PresenceCase::JoinAndLeave);
    }

    /// upstream: node/broker_integration_test.go#TestIntegrationPresence_Memory/Presence_expiration
    /// upstream: node/broker_integration_test.go#TestIntegrationPresence_Memory
    #[test]
    fn presence_expiration() {
        run(PresenceCase::Expiration);
    }
}
