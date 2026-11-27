//! Deviations D-070 and D-071: two Cabby processes sharing one PostgreSQL
//! database (`--pubsub=postgres --broadcast_adapter=postgres
//! --broker=postgres`), no Redis or NATS. A broadcast row inserted into the
//! outbox reaches clients on both nodes with one offset; a session that
//! disconnects from node 1 resumes on node 2 and catches up on what it
//! missed. AnyCable has no Postgres adapters, so the Go arm is not observable.

use cabby_core::protos;
use cabby_testkit::fake_rpc::{FakeRpc, Method, Reply};
use cabby_testkit::postgres::{PgScope, rust_only};
use cabby_testkit::sqlite_broker as sq;
use cabby_testkit::sut::Sut;
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

fn node(scope: &PgScope, rpc: &str) -> Sut {
    let config = scope.config();
    let args = [
        "--pubsub=postgres".to_owned(),
        "--broadcast_adapter=postgres".to_owned(),
        "--broker=postgres".to_owned(),
        format!("--postgres_url={}", config.url),
        format!("--postgres_schema={}", config.schema),
        format!("--postgres_channel={}", config.channel),
        format!("--rpc_host={rpc}"),
        "--public_streams".to_owned(),
        "--disconnect_mode=never".to_owned(),
    ];
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    Sut::try_start(&args, &[]).unwrap_or_else(|e| panic!("{e:#}"))
}

/// Inserts the broadcast of text `text` to `stream` into the outbox.
fn insert(scope: &PgScope, stream: &str, text: &str) {
    scope.insert_broadcast(&json!({"stream": stream, "data": json!(text).to_string()}).to_string());
}

/// regression-for: node/broker_integration_test.go#TestIntegrationRestore_Memory/Restore_session_connection_and_channels_state
/// regression-for: node/broker_integration_test.go#TestIntegrationHistory_Memory/Subscribe_+_History
/// note: Deviations D-070, D-071: two processes on one database with --pubsub=postgres --broadcast_adapter=postgres --broker=postgres. A row inserted into <prefix>broadcasts reaches a client on node 1 and a witness on node 2 with the same offset and epoch (claimed once, fanned out over LISTEN/NOTIFY).
/// note: The client disconnects from node 1, misses m2 and m3, reconnects to node 2 with X-ANYCABLE-RESTORE-SID: the session is restored from the shared broker without an RPC Connect call, history from its offset returns m2 and m3, and m4 arrives live on node 2 with the next offset.
/// note: not_observable on Go: AnyCable rejects --pubsub=postgres, --broadcast_adapter=postgres and --broker=postgres.
#[test]
fn deviation_d070_d071_postgres_nodes_share_broadcasts_history_and_sessions() {
    rust_only();
    let scope = PgScope::new("conf");
    let fake = app();
    let rpc = fake.http_url();
    let mut node1 = node(&scope, &rpc);
    let mut node2 = node(&scope, &rpc);

    let (mut client, welcome) = sq::connect(&node1, None);
    let sid = welcome["sid"].as_str().expect("sid in the welcome").to_owned();
    let id = sq::subscribe(&mut client, "pg_conf");
    let (mut witness, _) = sq::connect(&node2, None);
    sq::subscribe(&mut witness, "pg_conf");

    insert(&scope, "pg_conf", "m1");
    let m1 = sq::receive_message(&mut client, "m1");
    let epoch = m1["epoch"].as_str().expect("epoch").to_owned();
    let offset = m1["offset"].as_u64().expect("offset");
    let stream_id = m1["stream_id"].as_str().expect("stream_id").to_owned();
    let w1 = sq::receive_message(&mut witness, "m1");
    assert_eq!(
        (Some(offset), Some(epoch.as_str())),
        (w1["offset"].as_u64(), w1["epoch"].as_str()),
        "one offset on both nodes"
    );

    client.close(true);
    insert(&scope, "pg_conf", "m2");
    insert(&scope, "pg_conf", "m3");
    // Observed on node 2, so both are in the shared history.
    sq::receive_message(&mut witness, "m3");
    let connects = fake.call_count(Method::Connect);

    let (mut restored, welcome) = sq::connect(&node2, Some(&sid));
    assert_eq!(Some(true), welcome["restored"].as_bool(), "the session is restored on node 2: {welcome}");
    assert_eq!(connects, fake.call_count(Method::Connect), "restoring the session called the application");

    let replies = sq::history_after(&mut restored, &id, &stream_id, offset, &epoch);
    assert_eq!(
        vec![("m2".to_owned(), offset + 1, epoch.clone()), ("m3".to_owned(), offset + 2, epoch.clone())],
        sq::messages(&replies),
        "the missed messages, from the shared history"
    );
    assert_eq!(Some("confirm_history"), replies.last().and_then(|r| r["type"].as_str()));

    insert(&scope, "pg_conf", "m4");
    let m4 = sq::receive_message(&mut restored, "m4");
    assert_eq!((Some(offset + 3), Some(epoch.as_str())), (m4["offset"].as_u64(), m4["epoch"].as_str()));

    drop((restored, witness));
    sq::stop_gracefully(&mut node1);
    let output = sq::stop_gracefully(&mut node2);
    for line in ["Using Postgres broker: ", "Starting Postgres pub/sub: ", "Starting Postgres broadcaster: "] {
        assert!(output.contains(line), "no {line:?} in:\n{output}");
    }
    assert!(!output.contains("cabby:cabby@"), "credentials in the log:\n{output}");
}

/// regression-for: node/broker_integration_test.go#TestIntegrationHistory_Memory/Subscribe_+_History
/// note: Deviations D-070, D-071: outbox rows the Postgres broker can never store (a NUL character in the data, which a text column rejects, and a stream name over the 2704-byte limit of a btree index entry) are deleted with an error log naming the row; the rows after them are delivered, in order. A failure that may pass (a lock, a timeout, an outage) is still retried.
/// note: not_observable on Go: AnyCable rejects --pubsub=postgres, --broadcast_adapter=postgres and --broker=postgres.
#[test]
fn deviation_d070_d071_postgres_rows_postgres_cannot_store_do_not_block_the_outbox() {
    rust_only();
    let scope = PgScope::new("conf");
    let fake = app();
    let mut node1 = node(&scope, &fake.http_url());
    let (mut client, _) = sq::connect(&node1, None);
    sq::subscribe(&mut client, "pg_rows");

    insert(&scope, "pg_rows", "before");
    sq::receive_message(&mut client, "before");
    // `data` is the text the client gets; a NUL in it can't be stored in a text column.
    scope.insert_broadcast(&json!({"stream": "pg_rows", "data": "nul\u{0}"}).to_string());
    // md5 digests don't compress, so the key stays over the index entry limit.
    let long: String = (0..100).map(|i| format!("{:x}", md5_like(i))).collect();
    insert(&scope, &long, "long stream");
    insert(&scope, "pg_rows", "after");
    sq::receive_message(&mut client, "after");
    // The rows are deleted when the batch commits, right after "after" is fanned out.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let outbox = || scope.scalar(&format!("SELECT count(*) FROM {}", scope.table("broadcasts")));
    while outbox().as_deref() != Some("0") {
        assert!(std::time::Instant::now() < deadline, "{:?} rows left in the outbox", outbox());
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    drop(client);
    let output = sq::stop_gracefully(&mut node1);
    let dropped: Vec<&str> =
        output.lines().filter(|l| l.contains("dropping a broadcast Postgres can't store")).collect();
    assert_eq!(2, dropped.len(), "one error line per row:\n{output}");
    assert!(dropped[0].contains("invalid byte sequence"), "{}", dropped[0]);
    assert!(dropped[1].contains("index row"), "{}", dropped[1]);
}

/// 32 hex digits that differ for every `i` (not compressible as a run).
fn md5_like(i: u64) -> u128 {
    let mut x = u128::from(i).wrapping_add(0x9e37_79b9_7f4a_7c15_f39c_c060_5ced_c834);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd_c4ce_b9fe_1a85_ec53);
    x ^= x >> 29;
    x
}
