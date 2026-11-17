//! upstream file: utils/priority_queue_test.go
//!
//! Upstream compares item pointers with `assert.Same`; items are compared by
//! identity (`ItemId`) here.

use cabby_testkit::reference::utils::PriorityQueue;
use serde_json::json;

/// upstream: utils/priority_queue_test.go#TestPriorityQueuePushPopItem
#[test]
fn test_priority_queue_push_pop_item() {
    let mut pq = PriorityQueue::new();
    let b = pq.push_item(json!("b"), 2);
    let a = pq.push_item(json!("a"), 1);
    let d = pq.push_item(json!("d"), 4);
    let c = pq.push_item(json!("c"), 3);

    assert_eq!(4, pq.len());

    assert_eq!(Some(a), pq.pop_item().map(|i| i.id));
    assert_eq!(Some(b), pq.pop_item().map(|i| i.id));
    assert_eq!(Some(c), pq.pop_item().map(|i| i.id));
    assert_eq!(Some(d), pq.pop_item().map(|i| i.id));
}

/// upstream: utils/priority_queue_test.go#TestPriorityQueuePeek
#[test]
fn test_priority_queue_peek() {
    let mut pq = PriorityQueue::new();

    pq.push_item(json!(2), 2);
    pq.push_item(json!(1), 1);
    pq.push_item(json!(3), 3);

    let item = pq.peek().expect("an item");
    // Upstream: assert.Equal(&PriorityQueueItem{value: 1, priority: 1}, item); the
    // struct's heap index is 0 for the head item, so value and priority decide.
    assert_eq!((json!(1), 1), (item.value, item.priority));
}

/// upstream: utils/priority_queue_test.go#TestPriorityQueueUpdate
#[test]
fn test_priority_queue_update() {
    let mut pq = PriorityQueue::new();

    let item = pq.push_item(json!(1), 1);
    pq.push_item(json!(3), 3);
    pq.push_item(json!(5), 5);

    pq.update(item, 4);

    let one = pq.pop_item().expect("an item");
    assert_eq!(json!(3), one.value);

    let two = pq.pop_item().expect("an item");
    assert_eq!(json!(1), two.value);

    let three = pq.pop_item().expect("an item");
    assert_eq!(json!(5), three.value);
}

/// upstream: utils/priority_queue_test.go#TestPriorityQueueRemove
#[test]
fn test_priority_queue_remove() {
    let mut pq = PriorityQueue::new();

    pq.push_item(json!(2), 2);
    let item = pq.push_item(json!(1), 1);

    pq.remove(item);

    assert_eq!(1, pq.len());
    assert_eq!(json!(2), pq.peek().expect("an item").value);
}
