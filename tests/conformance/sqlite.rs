//! Deviation D-065: the SQLite broker (`--broker=sqlite`), a Cabby addition.
//! A graceful restart keeps the epoch, the stream offsets, the history and the
//! sessions. AnyCable has no such broker, so the Go arm is not observable.

use cabby_core::protos;
use cabby_testkit::fake_rpc::{FakeRpc, Method, Reply};
use cabby_testkit::sqlite_broker::{self as sq, DataDir};
use serde_json::json;

/// An application that authenticates every connection and welcomes it with
/// its session ID, as anycable-rails does.
fn app() -> FakeRpc {
    let fake = FakeRpc::start();
    fake.on(Method::Connect).reply(Reply::with(|call| {
        let sid = call.meta("sid").or_else(|| call.meta("x-anycable-meta-sid")).unwrap_or_default().to_owned();
        protos::ConnectionResponse {
            status: protos::Status::Success as i32,
            identifiers: r#"{"user":"u1"}"#.into(),
            transmissions: vec![json!({"type": "welcome", "sid": sid}).to_string()],
            ..Default::default()
        }
        .into()
    }));
    fake
}

/// regression-for: node/broker_integration_test.go#TestIntegrationRestore_Memory/Restore_session_connection_and_channels_state
/// regression-for: node/broker_integration_test.go#TestIntegrationHistory_Memory/Subscribe_+_History
/// note: Deviation D-065: with --broker=sqlite, a graceful restart (SIGTERM, then a new process on the same file) keeps the epoch and the stream offsets: a client resuming from its offset gets the messages it missed (m2, m3) with the old epoch, and the next broadcast continues the offsets.
/// note: The session restores by X-ANYCABLE-RESTORE-SID after the restart without an RPC Connect call (restored: true, its $pubsub subscription live again); the startup line reports 3 messages, 1 stream and 2 sessions after a clean shutdown.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn deviation_d065_sqlite_broker_resumes_history_and_sessions_after_a_graceful_restart() {
    sq::rust_only();
    let fake = app();
    let dir = DataDir::new("d065");
    let db = dir.db();
    let rpc = format!("--rpc_host={}", fake.http_url());
    let extra = [rpc.as_str(), "--public_streams", "--disconnect_mode=never"];

    // First run: a session subscribes and sees m1; m2 and m3 arrive after it left.
    let mut sut = sq::start(&db, &extra);
    let (mut client, welcome) = sq::connect(&sut, None);
    let sid = welcome["sid"].as_str().expect("sid in the welcome").to_owned();
    let id = sq::subscribe(&mut client, "d065");
    sq::broadcast(&sut, "d065", "m1");
    let m1 = sq::receive_message(&mut client, "m1");
    let epoch = m1["epoch"].as_str().expect("epoch").to_owned();
    let offset = m1["offset"].as_u64().expect("offset");
    let stream_id = m1["stream_id"].as_str().expect("stream_id").to_owned();
    let (mut witness, _) = sq::connect(&sut, None);
    sq::subscribe(&mut witness, "d065");
    client.close(true);
    sq::broadcast(&sut, "d065", "m2");
    sq::broadcast(&sut, "d065", "m3");
    // Observed by a witness, so both reached the broker before the stop.
    sq::receive_message(&mut witness, "m3");
    drop(witness);
    let connects = fake.call_count(Method::Connect);
    sq::stop_gracefully(&mut sut);

    // Second run on the same file.
    let mut sut = sq::start(&db, &extra);
    let (mut restored, welcome) = sq::connect(&sut, Some(&sid));
    assert_eq!(Some(true), welcome["restored"].as_bool(), "the session is restored: {welcome}");
    assert_eq!(connects, fake.call_count(Method::Connect), "restoring the session called the application");

    let replies = sq::history_after(&mut restored, &id, &stream_id, offset, &epoch);
    assert_eq!(
        vec![("m2".to_owned(), offset + 1, epoch.clone()), ("m3".to_owned(), offset + 2, epoch.clone())],
        sq::messages(&replies),
        "the missed messages, with the old epoch and offsets"
    );
    assert_eq!(Some("confirm_history"), replies.last().and_then(|r| r["type"].as_str()));

    sq::broadcast(&sut, "d065", "m4");
    let m4 = sq::receive_message(&mut restored, "m4");
    assert_eq!((Some(offset + 3), Some(epoch.as_str())), (m4["offset"].as_u64(), m4["epoch"].as_str()));

    let output = sq::stop_gracefully(&mut sut);
    let line = format!("restored 3 messages across 1 streams and 2 sessions (epoch {epoch}, clean shutdown)");
    assert!(output.contains(&line), "no startup line {line:?} in:\n{output}");
}
