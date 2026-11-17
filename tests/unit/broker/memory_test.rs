//! upstream file: broker/memory_test.go
//!
//! Upstream runs the expiry tests inside `synctest.Test` (a fake-time bubble)
//! and sleeps to move time. Here the broker reads a [`ManualClock`] and each
//! `time.Sleep(d)` is a `clock.advance(d)` (original durations kept). The Go
//! reference cannot run on that clock and its `expire()` is unexported, so
//! those cases are not observable against Go.

use std::sync::Arc;
use std::time::Duration;

use cabby_core::common::{PRESENCE_JOIN_TYPE, PRESENCE_LEAVE_TYPE, PresenceEvent};
use cabby_testkit::clock::ManualClock;
use cabby_testkit::reference::broker::*;
use serde_json::json;

fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new())
}

/// upstream: broker/memory_test.go#TestMemory_Expire
#[test]
fn test_memory_expire() {
    let clock = clock();
    let mut config = new_config();
    config.history_ttl = 1;

    let broker = new_memory_broker(None, &config, clock.clone());

    let start = clock.unix() - 10;

    broker.add("test", "a");
    broker.add("test", "b");

    clock.advance(Duration::from_secs(2)); // upstream: time.Sleep(2 * time.Second)

    broker.add("test", "c");
    broker.add("test", "d");

    broker.expire();

    let history = broker.history_since("test", start).expect("history").unwrap_or_default();

    assert_eq!(2, history.len());
    assert_eq!(3, history[0].offset);
    assert_eq!("c", history[0].data);
    assert_eq!(4, history[1].offset);
    assert_eq!("d", history[1].data);

    clock.advance(Duration::from_secs(2)); // upstream: time.Sleep(2 * time.Second)

    broker.expire();

    let history = broker.history_since("test", start).expect("history");
    assert!(history.unwrap_or_default().is_empty());
}

/// upstream: broker/memory_test.go#TestMemory_Limit
#[test]
fn test_memory_limit() {
    let clock = clock();
    let mut config = new_config();
    config.history_limit = 2;

    let broker = new_memory_broker(None, &config, clock.clone());

    let start = clock.unix() - 10;

    broker.add("test", "a");
    broker.add("test", "b");
    broker.add("test", "c");

    let history = broker.history_since("test", start).expect("history").unwrap_or_default();

    assert_eq!(2, history.len());
    assert_eq!(3, history[1].offset);
    assert_eq!("c", history[1].data);
}

mod test_memory_from_offset {
    use super::*;

    fn broker() -> MemoryBroker {
        let config = new_config();

        let broker = new_memory_broker(None, &config, clock());

        for data in ["a", "b", "c", "d", "e"] {
            broker.add("test", data);
        }
        broker
    }

    /// upstream: broker/memory_test.go#TestMemory_FromOffset/With_current_epoch
    #[test]
    fn with_current_epoch() {
        let broker = broker();
        let history = broker.history_from("test", &broker.epoch(), 2).expect("history");

        assert_eq!(3, history.len());
        assert_eq!(3, history[0].offset);
        assert_eq!("c", history[0].data);
        assert_eq!(4, history[1].offset);
        assert_eq!("d", history[1].data);
        assert_eq!(5, history[2].offset);
        assert_eq!("e", history[2].data);
    }

    /// upstream: broker/memory_test.go#TestMemory_FromOffset/With_unknown_epoch
    #[test]
    fn with_unknown_epoch() {
        let broker = broker();
        // Upstream also requires the history to be nil; an `Err` carries none.
        let history = broker.history_from("test", "unknown", 2);
        assert!(history.is_err(), "{history:?}");
    }
}

/// upstream: broker/memory_test.go#TestMemory_Store
#[test]
fn test_memory_store() {
    let clock = clock();
    let config = new_config();

    let broker = new_memory_broker(None, &config, clock.clone());
    broker.set_epoch("2023");

    let ts = cabby_core::broker::Clock::now(&*clock);

    let offset = broker.store("test", b"a", 10, Some(ts)).expect("store");
    assert_eq!(10, offset);

    broker.store("test", b"b", 11, Some(ts)).expect("store");

    broker.store("tes2", b"c", 1, Some(ts)).expect("store");

    let history = broker.history_from("test", &broker.epoch(), 10).expect("history");
    assert_eq!(1, history.len());
    assert_eq!(11, history[0].offset);

    assert!(broker.store("test", b"c", 3, Some(ts)).is_err());
}

/// upstream: broker/memory_test.go#TestMemory_Peak
#[test]
fn test_memory_peak() {
    // Upstream's test body is empty; kept as written. The linked regression
    // `test_memory_peak_reports_latest_position` exercises Peak.
}

/// regression-for: broker/memory_test.go#TestMemory_Peak
/// Peak is the latest stored offset with the broker's epoch, and nothing for an
/// unknown stream.
#[test]
fn test_memory_peak_reports_latest_position() {
    let broker = new_memory_broker(None, &new_config(), clock());

    assert_eq!(Ok(None), broker.peak("test"));

    broker.add("test", "a");
    broker.add("test", "b");

    let peak = broker.peak("test").expect("peak").expect("position");
    assert_eq!(2, peak.offset);
    assert_eq!(broker.epoch(), peak.epoch);

    assert_eq!(Ok(None), broker.peak("unknown"));
}

/// upstream: broker/memory_test.go#TestMemstream_filterByOffset
#[test]
fn test_memstream_filter_by_offset() {
    let clock = clock();
    let ms = new_memstream(1, 5, clock.clone());

    ms.add("test1");
    ms.add("test2");

    // Should return error if offset is out of range
    assert!(ms.filter_by_offset(10, &mut |_: &MemstreamEntry| {}).is_err());

    let res = ms.filter_by_offset(1, &mut |e: &MemstreamEntry| assert_eq!("test2", e.data));
    assert!(res.is_ok(), "{res:?}");

    clock.advance(Duration::from_secs(2)); // upstream: time.Sleep(2 * time.Second)

    ms.expire();

    let res = ms.filter_by_offset(1, &mut |e: &MemstreamEntry| panic!("entry should be expired: {e:?}"));
    assert!(res.is_err());
}

/// upstream: broker/memory_test.go#TestMemory_RestoreSession
#[test]
fn test_memory_restore_session() {
    let clock = clock();
    let mut config = new_config();
    config.sessions_ttl = 1;

    let broker = new_memory_broker(None, &config, clock.clone());

    broker.commit_session("123", &TestCacheable("cache-me".into())).expect("commit");

    let data = broker.restore_session("123").expect("restore");
    assert_eq!(Some(b"cache-me".to_vec()), data);

    clock.advance(Duration::from_millis(500)); // upstream: time.Sleep(500 * time.Millisecond)
    broker.expire();

    broker.touch_session("123").expect("touch");
    clock.advance(Duration::from_millis(500)); // upstream: time.Sleep(500 * time.Millisecond)
    broker.expire();

    let data = broker.restore_session("123").expect("restore");
    assert_eq!(Some(b"cache-me".to_vec()), data);

    clock.advance(Duration::from_secs(1)); // upstream: time.Sleep(1 * time.Second)
    broker.expire();

    let data = broker.restore_session("123").expect("restore");
    assert_eq!(None, data);
}

fn event(kind: &str, id: &str, info: Option<serde_json::Value>) -> PresenceEvent {
    PresenceEvent { kind: kind.into(), id: id.into(), info }
}

/// upstream: broker/memory_test.go#TestMemory_Presence
#[test]
fn test_memory_presence() {
    let config = new_config();

    let presenter = MockPresenter::new();

    let broker = new_memory_broker(Some(&presenter), &config, clock());

    let ev = broker.presence_add("a", "s1", "user_1", json!({"name": "John"})).expect("presence add");

    presenter.assert_called(
        PresenterMethod::HandleJoin,
        "a",
        &event(PRESENCE_JOIN_TYPE, "user_1", Some(json!({"name": "John"}))),
    );

    let ev = ev.expect("join event");
    assert_eq!("user_1", ev.id);
    assert_eq!("join", ev.kind);
    assert_eq!(Some(json!({"name": "John"})), ev.info);

    // Adding presence for the same session with different ID is illegal
    // (upstream also requires a nil event; an `Err` carries none)
    let res = broker.presence_add("a", "s1", "user_2", json!({"name": "Boo"}));
    assert!(res.is_err(), "{res:?}");

    let ev = broker.presence_add("a", "s2", "user_1", json!({"name": "Jack"})).expect("presence add");
    assert_eq!(None, ev);

    // No new presence event has been triggered
    presenter.assert_number_of_calls(PresenterMethod::HandleJoin, 1);

    let ev = broker.presence_add("a", "s3", "user_2", json!({"name": "Alice"})).expect("presence add");
    assert_eq!("user_2", ev.expect("join event").id);

    presenter.assert_number_of_calls(PresenterMethod::HandleJoin, 2);

    let ev = broker.presence_add("b", "s3", "user_2", json!({"name": "Alice"})).expect("presence add");
    assert_eq!("user_2", ev.expect("join event").id);

    // Different stream -> new event
    presenter.assert_number_of_calls(PresenterMethod::HandleJoin, 3);

    presenter.assert_called(
        PresenterMethod::HandleJoin,
        "b",
        &event(PRESENCE_JOIN_TYPE, "user_2", Some(json!({"name": "Alice"}))),
    );

    let info = broker.presence_info("a").expect("presence info");

    assert_eq!(2, info.total);
    assert_eq!(2, info.records.len());

    // Make sure the latest info is returned
    assert!(
        info.records.iter().any(|r| r.id == "user_1" && r.info.as_ref().map(|i| &i["name"]) == Some(&json!("Jack"))),
        "presence user with user_id and name:Jack not found: {:?}",
        info.records
    );

    // Now let's check that leave works
    let ev = broker.presence_remove("a", "s1").expect("presence remove");
    assert_eq!(None, ev);

    presenter.assert_number_of_calls(PresenterMethod::HandleLeave, 0);

    let info = broker.presence_info("a").expect("presence info");

    assert_eq!(2, info.total);

    let ev = broker.presence_remove("a", "s2").expect("presence remove");
    assert_eq!("user_1", ev.expect("leave event").id);

    presenter.assert_number_of_calls(PresenterMethod::HandleLeave, 1);
    presenter.assert_called(PresenterMethod::HandleLeave, "a", &event(PRESENCE_LEAVE_TYPE, "user_1", None));

    let info = broker.presence_info("a").expect("presence info");

    assert_eq!(1, info.total);
}

/// upstream: broker/memory_test.go#TestMemory_expirePresence
#[test]
fn test_memory_expire_presence() {
    let clock = clock();
    let mut config = new_config();
    config.presence_ttl = 1;

    let broker = new_memory_broker(None, &config, clock.clone());

    let _ = broker.presence_add("a", "s1", "user_1", json!("john"));
    let _ = broker.presence_add("a", "s2", "user_2", json!("kate"));

    let info = broker.presence_info("a").expect("presence info");

    assert_eq!(2, info.total);

    clock.advance(Duration::from_millis(500)); // upstream: time.Sleep(500 * time.Millisecond)
    let _ = broker.touch_presence("s1");

    clock.advance(Duration::from_millis(500)); // upstream: time.Sleep(500 * time.Millisecond)
    let _ = broker.touch_presence("s1");

    clock.advance(Duration::from_millis(500)); // upstream: time.Sleep(500 * time.Millisecond)
    let _ = broker.touch_presence("s1");

    clock.advance(Duration::from_millis(500)); // upstream: time.Sleep(500 * time.Millisecond)
    broker.expire();

    let info = broker.presence_info("a").expect("presence info");

    assert_eq!(1, info.total);
    assert_eq!("user_1", info.records[0].id);

    clock.advance(Duration::from_millis(1000)); // upstream: time.Sleep(1000 * time.Millisecond)

    let _ = broker.presence_add("a", "s3", "user_1", json!("jack"));

    broker.expire();

    let info = broker.presence_info("a").expect("presence info");

    assert_eq!(1, info.total);
    assert_eq!("user_1", info.records[0].id);
    assert_eq!(Some(json!("jack")), info.records[0].info);
}
