//! Deviation D-069: the Redis broker (`--broker=redis`), a Cabby addition.
//! Two nodes on one Redis share the epoch, the stream offsets, the history
//! and the sessions; without `--pubsub`, `--broker=redis` selects Redis
//! pub/sub, so broadcasts reach both nodes. AnyCable rejects
//! `--broker=redis`, so the Go arm is not observable.

use cabby_testkit::fake_rpc::Method;
use cabby_testkit::redis_broker as rb;
use cabby_testkit::services;
use cabby_testkit::sqlite_broker::{self as ext, DEADLINE};

/// regression-for: node/broker_integration_test.go#TestIntegrationRestore_Memory/Restore_session_connection_and_channels_state
/// regression-for: node/broker_integration_test.go#TestIntegrationHistory_Memory/Subscribe_+_History
/// note: Deviation D-069: two cabby processes with --broker=redis (pub/sub not set, so Redis pub/sub) on one Redis: a client that saw m1 on node A and left catches up on node B from its offset (m2, m3, broadcast through B, with A's epoch), and its session restores on B by X-ANYCABLE-RESTORE-SID without an RPC Connect call.
/// note: The next broadcast through A reaches the client on B over Redis pub/sub with the next offset; A starts with a new epoch and B logs the same one as existing.
/// note: not_observable on Go: AnyCable rejects --broker=redis (Unsupported broker adapter: redis).
#[test]
fn deviation_d069_two_nodes_share_history_and_sessions_through_redis() {
    rb::rust_only();
    let scope = services::redis().scope("d069");
    let fake = ext::session_app();
    let rpc = format!("--rpc_host={}", fake.http_url());
    let extra = [rpc.as_str(), "--public_streams", "--disconnect_mode=never"];
    let mut a = rb::start(&scope, &extra);
    let mut b = rb::start(&scope, &extra);
    let stream = scope.name("chat");

    // On node A: a session subscribes and sees m1, then leaves.
    let (mut client, welcome) = ext::connect(&a, None);
    let sid = welcome["sid"].as_str().expect("sid in the welcome").to_owned();
    let id = ext::subscribe(&mut client, &stream);
    ext::broadcast(&a, &stream, "m1");
    let m1 = ext::receive_message(&mut client, "m1");
    let epoch = m1["epoch"].as_str().expect("epoch").to_owned();
    let offset = m1["offset"].as_u64().expect("offset");
    let stream_id = m1["stream_id"].as_str().expect("stream_id").to_owned();
    let (mut witness, _) = ext::connect(&a, None);
    ext::subscribe(&mut witness, &stream);
    client.close(true);

    // Broadcast through node B: stored in Redis by B, seen on A over pub/sub.
    ext::broadcast(&b, &stream, "m2");
    ext::broadcast(&b, &stream, "m3");
    ext::receive_message(&mut witness, "m3");
    let session_key = format!("{}:sess:{sid}", scope.prefix);
    services::wait_until("node A caches the session in Redis", DEADLINE, || {
        (scope.command(&["EXISTS", &session_key]).ok()?.as_int() == Some(1)).then_some(())
    })
    .unwrap_or_else(|e| panic!("{e:#}"));
    let connects = fake.call_count(Method::Connect);

    // On node B: the session restores and catches up from its offset.
    let (mut restored, welcome) = ext::connect(&b, Some(&sid));
    assert_eq!(Some(true), welcome["restored"].as_bool(), "the session is restored on B: {welcome}");
    assert_eq!(connects, fake.call_count(Method::Connect), "restoring the session called the application");

    let replies = ext::history_after(&mut restored, &id, &stream_id, offset, &epoch);
    assert_eq!(
        vec![("m2".to_owned(), offset + 1, epoch.clone()), ("m3".to_owned(), offset + 2, epoch.clone())],
        ext::messages(&replies),
        "the missed messages, with A's epoch and the shared offsets"
    );
    assert_eq!(Some("confirm_history"), replies.last().and_then(|r| r["type"].as_str()));

    // A's witness and B's restored session both listen on the stream's channel.
    scope.wait_numsub(&stream, 2, DEADLINE).unwrap_or_else(|e| panic!("{e:#}"));
    ext::broadcast(&a, &stream, "m4");
    let m4 = ext::receive_message(&mut restored, "m4");
    assert_eq!((Some(offset + 3), Some(epoch.as_str())), (m4["offset"].as_u64(), m4["epoch"].as_str()));

    drop((restored, witness));
    let out_b = ext::stop_gracefully(&mut b);
    let out_a = ext::stop_gracefully(&mut a);
    let line = |how: &str| format!("Redis broker is ready (epoch {epoch}, {how})");
    assert!(out_a.contains(&line("new")), "no {:?} in A's output:\n{out_a}", line("new"));
    assert!(out_b.contains(&line("existing")), "no {:?} in B's output:\n{out_b}", line("existing"));
    assert!(out_a.contains("context=broker") && out_a.contains("provider=redis"), "{out_a}");
}
