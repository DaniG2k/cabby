//! upstream file: broker/broker_test.go

use cabby_testkit::reference::broker::*;

fn noop() {}

mod test_streams_tracker_add_remove {
    //! Upstream subtests share one tracker and run in order; each Rust test
    //! replays the earlier steps (without their assertions) before its own.

    use super::*;

    const STEPS: [(&str, &str); 7] = [
        ("add", "stream1"),
        ("add", "stream1"),
        ("add", "stream1"),
        ("remove", "stream1"),
        ("remove", "stream1"),
        ("remove", "stream1"),
        ("remove", "nonexistent"),
    ];

    fn tracker_after(steps: usize) -> Tracker {
        let tracker = new_streams_tracker();
        for (op, name) in &STEPS[..steps] {
            match *op {
                "add" => tracker.add(name, &mut noop),
                _ => tracker.remove(name, &mut noop),
            };
        }
        tracker
    }

    /// upstream: broker/broker_test.go#TestStreamsTracker_AddRemove/Add_first_subscription
    #[test]
    fn add_first_subscription() {
        let tracker = tracker_after(0);
        let is_new = tracker.add("stream1", &mut noop);
        assert!(is_new, "First subscription should return true");
        assert!(tracker.has("stream1"));
    }

    /// upstream: broker/broker_test.go#TestStreamsTracker_AddRemove/Add_second_subscription_to_same_stream
    #[test]
    fn add_second_subscription_to_same_stream() {
        let tracker = tracker_after(1);
        let is_new = tracker.add("stream1", &mut noop);
        assert!(!is_new, "Second subscription should return false");
        assert!(tracker.has("stream1"));
    }

    /// upstream: broker/broker_test.go#TestStreamsTracker_AddRemove/Add_third_subscription_to_same_stream
    #[test]
    fn add_third_subscription_to_same_stream() {
        let tracker = tracker_after(2);
        let is_new = tracker.add("stream1", &mut noop);
        assert!(!is_new, "Third subscription should return false");
        assert!(tracker.has("stream1"));
    }

    /// upstream: broker/broker_test.go#TestStreamsTracker_AddRemove/Remove_first_subscription_(count:_3_->_2)
    #[test]
    fn remove_first_subscription_count_3_to_2() {
        let tracker = tracker_after(3);
        let is_last = tracker.remove("stream1", &mut noop);
        assert!(!is_last, "Should not be last when count > 1");
        assert!(tracker.has("stream1"), "Stream should still exist");
    }

    /// upstream: broker/broker_test.go#TestStreamsTracker_AddRemove/Remove_second_subscription_(count:_2_->_1)
    #[test]
    fn remove_second_subscription_count_2_to_1() {
        let tracker = tracker_after(4);
        let is_last = tracker.remove("stream1", &mut noop);
        assert!(!is_last, "Should not be last when count = 1 (will be deleted on next remove)");
        assert!(tracker.has("stream1"), "Stream should still exist");
    }

    /// upstream: broker/broker_test.go#TestStreamsTracker_AddRemove/Remove_third_subscription_(count:_1_->_0)
    #[test]
    fn remove_third_subscription_count_1_to_0() {
        let tracker = tracker_after(5);
        let is_last = tracker.remove("stream1", &mut noop);
        assert!(is_last, "Should be last when count = 1");
        assert!(!tracker.has("stream1"), "Stream should be deleted");
    }

    /// upstream: broker/broker_test.go#TestStreamsTracker_AddRemove/Remove_non-existent_stream
    #[test]
    fn remove_non_existent_stream() {
        let tracker = tracker_after(6);
        let is_last = tracker.remove("nonexistent", &mut noop);
        assert!(!is_last, "Should return false for non-existent stream");
    }
}

/// upstream: broker/broker_test.go#TestStreamsTracker_MultipleStreams
#[test]
fn test_streams_tracker_multiple_streams() {
    let tracker = new_streams_tracker();

    assert!(tracker.add("stream1", &mut noop));
    assert!(tracker.add("stream2", &mut noop));
    assert!(!tracker.add("stream1", &mut noop));
    assert!(!tracker.add("stream2", &mut noop));

    assert!(tracker.has("stream1"));
    assert!(tracker.has("stream2"));

    assert!(!tracker.remove("stream1", &mut noop));
    assert!(tracker.has("stream1"), "stream1 should still exist after first remove");

    assert!(tracker.remove("stream1", &mut noop));
    assert!(!tracker.has("stream1"), "stream1 should be deleted");

    assert!(tracker.has("stream2"));

    assert!(!tracker.remove("stream2", &mut noop));
    assert!(tracker.remove("stream2", &mut noop));
    assert!(!tracker.has("stream2"));
}

/// upstream: broker/broker_test.go#TestStreamsTracker_ReferenceCount
#[test]
fn test_streams_tracker_reference_count() {
    let tracker = new_streams_tracker();

    let mut subscribed = 0;
    let mut unsubscribed = 0;

    for i in 0..5 {
        let is_new = tracker.add("popular_stream", &mut || subscribed += 1);
        if i == 0 {
            assert!(is_new, "First subscription should be new");
        } else {
            assert!(!is_new, "Subsequent subscriptions should not be new");
        }
    }

    for _ in 0..4 {
        let is_last = tracker.remove("popular_stream", &mut || unsubscribed += 1);
        assert!(!is_last, "Should not be last until count reaches 1");
        assert!(tracker.has("popular_stream"), "Stream should still exist");
    }

    let is_last = tracker.remove("popular_stream", &mut || unsubscribed += 1);
    assert!(is_last, "Should be last when removing final subscription");
    assert!(!tracker.has("popular_stream"), "Stream should be deleted");

    assert_eq!(1, subscribed);
    assert_eq!(1, unsubscribed);
}
