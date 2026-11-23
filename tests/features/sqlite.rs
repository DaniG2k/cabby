//! The SQLite broker through the binary (deviation D-065, Cabby only): crash
//! recovery, the lock, the shutdown order, a failed final write, the WAL file
//! and startup validation. Every case is not observable on Go.

use std::time::Duration;

use cabby_testkit::sqlite_broker::{self as sq, DataDir};
use cabby_testkit::sut::Sut;

/// regression-for: broker/memory_test.go#TestMemory_FromOffset/With_unknown_epoch
/// note: D-065: after kill -9 the restart uses a new epoch (startup line "new epoch after an unclean shutdown"); a client resuming with the old epoch gets reject_history, then resyncs by time and gets the flushed messages with the new epoch; offsets continue.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn a_crash_restart_uses_a_new_epoch_and_clients_resync() {
    sq::rust_only();
    let dir = DataDir::new("crash");
    let db = dir.db();
    let mut sut = sq::start(&db, &["--public"]);
    let (mut client, _) = sq::connect(&sut, None);
    sq::subscribe(&mut client, "crash");
    sq::broadcast(&sut, "crash", "c1");
    sq::broadcast(&sut, "crash", "c2");
    let c2 = sq::receive_message(&mut client, "c2");
    let (epoch, offset) = (c2["epoch"].as_str().unwrap().to_owned(), c2["offset"].as_u64().unwrap());
    let stream_id = c2["stream_id"].as_str().unwrap().to_owned();
    sq::await_history_rows(&db, 2);
    sq::kill_hard(&mut sut);
    drop(client);

    let mut sut = sq::start(&db, &["--public"]);
    assert!(sut.process.output().contains("new epoch after an unclean shutdown"), "{}", sut.process.output());
    let (mut client, _) = sq::connect(&sut, None);
    let id = sq::subscribe(&mut client, "crash");

    let replies = sq::history_after(&mut client, &id, &stream_id, offset, &epoch);
    assert_eq!(Some("reject_history"), replies.last().and_then(|r| r["type"].as_str()), "{replies:?}");

    let replies = sq::history_since(&mut client, &id, sq::unix_now() - 60);
    assert_eq!(Some("confirm_history"), replies.last().and_then(|r| r["type"].as_str()), "{replies:?}");
    let resynced = sq::messages(&replies);
    assert_eq!(vec![("c1", 1), ("c2", 2)], resynced.iter().map(|(m, o, _)| (m.as_str(), *o)).collect::<Vec<_>>());
    let new_epoch = resynced[0].2.clone();
    assert_ne!(epoch, new_epoch);

    sq::broadcast(&sut, "crash", "c3");
    let c3 = sq::receive_message(&mut client, "c3");
    assert_eq!((Some(3), Some(new_epoch.as_str())), (c3["offset"].as_u64(), c3["epoch"].as_str()));
    sq::stop_gracefully(&mut sut);
}

/// regression-for: node/broker_integration_test.go#TestIntegrationRestore_Memory/Restore_session_connection_and_channels_state
/// note: D-065: a second Cabby on the same database waits 10 s for the lock file (<path>.lock), then exits with "in use by another Cabby process"; the first keeps serving.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn a_second_server_on_the_same_database_exits_with_the_lock_error() {
    sq::rust_only();
    let dir = DataDir::new("lock");
    let db = dir.db();
    let mut first = sq::start(&db, &["--public"]);

    let err = sq::try_start(&db, &["--public"]).err().expect("the second server must not start");
    let text = format!("{err:#}");
    assert!(text.contains("failed to initialize broker, cause: SQLite database"), "{text}");
    assert!(text.contains(": in use by another Cabby process (lock file "), "{text}");

    let (mut client, _) = sq::connect(&first, None);
    sq::subscribe(&mut client, "lock");
    sq::stop_gracefully(&mut first);
}

/// regression-for: cli/cli_test.go#TestRunnerDrainsBroadcastsBeforeBrokerShutdown
/// note: D-065, shutdown order: the session touch made when node.shutdown disconnects a connected extended-protocol client reaches the file (its expiry is at least SIGTERM time + sessions_ttl), so the broker is shut down after the node.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn a_session_touched_while_clients_disconnect_at_shutdown_is_on_disk() {
    sq::rust_only();
    let dir = DataDir::new("order");
    let db = dir.db();
    let mut sut = sq::start(&db, &["--public"]);
    let (mut client, welcome) = sq::connect(&sut, None);
    let sid = welcome["sid"].as_str().expect("sid").to_owned();
    sq::subscribe(&mut client, "order");
    // Not synchronization: the wall clock must move past the subscribe-time
    // commit, so the shutdown's touch sets a later expiry than the commit did.
    std::thread::sleep(Duration::from_millis(50));
    let stopping_at = sq::unix_now_ms();

    sq::stop_gracefully(&mut sut);
    drop(client);

    let expiry = sq::session_expiry_ms(&db, &sid).expect("the session is on disk");
    let sessions_ttl_ms = 300 * 1000;
    assert!(
        expiry >= stopping_at + sessions_ttl_ms,
        "expiry {expiry} predates the shutdown touch (>= {})",
        stopping_at + sessions_ttl_ms
    );
}

/// regression-for: broker/memory_test.go#TestMemory_FromOffset/With_unknown_epoch
/// note: D-065: when every write fails at shutdown (here another connection holds SQLite's write lock, so writes end with SQLITE_BUSY; a read-only or full disk behaves the same), clean_shutdown stays '0' and the next start rotates the epoch.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn a_failed_final_write_leaves_an_unclean_shutdown_and_a_new_epoch() {
    sq::rust_only();
    let dir = DataDir::new("flush");
    let db = dir.db();
    let mut sut = sq::start(&db, &["--public", "--shutdown_timeout=2"]);
    let (mut client, _) = sq::connect(&sut, None);
    sq::subscribe(&mut client, "flush");
    sq::broadcast(&sut, "flush", "f1");
    let epoch = sq::receive_message(&mut client, "f1")["epoch"].as_str().unwrap().to_owned();
    sq::await_history_rows(&db, 1);

    let blocker = sq::open_db(&db);
    blocker.execute_batch("BEGIN EXCLUSIVE").expect("SQLite's write lock");
    sq::stop_gracefully(&mut sut);
    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    drop(client);

    assert_eq!(Some("0".to_owned()), sq::meta(&db, "clean_shutdown"));
    let mut sut = sq::start(&db, &["--public"]);
    assert!(sut.process.output().contains("new epoch after an unclean shutdown"), "{}", sut.process.output());
    let (mut client, _) = sq::connect(&sut, None);
    sq::subscribe(&mut client, "flush");
    sq::broadcast(&sut, "flush", "f2");
    let f2 = sq::receive_message(&mut client, "f2");
    assert_ne!(Some(epoch.as_str()), f2["epoch"].as_str());
    sq::stop_gracefully(&mut sut);
}

/// regression-for: cli/cli_test.go#TestRunnerDrainsBroadcastsBeforeBrokerShutdown
/// note: D-065: after a graceful shutdown the WAL is checkpointed and removed, so the .db file alone is a complete copy (backups, volume snapshots); clean_shutdown is '1'.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn no_wal_file_is_left_after_a_graceful_shutdown() {
    sq::rust_only();
    let dir = DataDir::new("wal");
    let db = dir.db();
    let mut sut = sq::start(&db, &["--public"]);
    let (mut client, _) = sq::connect(&sut, None);
    sq::subscribe(&mut client, "wal");
    sq::broadcast(&sut, "wal", "w1");
    sq::receive_message(&mut client, "w1");

    sq::stop_gracefully(&mut sut);
    drop(client);

    assert!(db.exists());
    assert!(!sq::sibling(&db, "-wal").exists(), "a -wal file is left next to {}", db.display());
    assert_eq!(1, sq::history_rows(&db));
    assert_eq!(Some("1".to_owned()), sq::meta(&db, "clean_shutdown"));
}

/// regression-for: config/presets_test.go#TestBroker
/// note: D-065: --broker=sqlite without --sqlite_path (or CABBY_SQLITE_PATH, or [sqlite] path) stops the server at startup with a message naming the three settings.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn the_sqlite_broker_needs_a_database_path() {
    sq::rust_only();
    let err = Sut::try_start(&["--public", "--broadcast_adapter=http", "--broker=sqlite"], &[])
        .err()
        .expect("the server must not start");
    let text = format!("{err:#}");
    assert!(
        text.contains("failed to initialize broker, cause: --broker=sqlite needs a database path: set --sqlite_path, CABBY_SQLITE_PATH or [sqlite] path"),
        "{text}"
    );
}
