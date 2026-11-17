//! upstream file: utils/queue_test.go

use cabby_core::utils::{Item, PushResult};
use cabby_testkit::reference::utils::{ByteQueue, TrackedQueue};

fn test_byte_queue_item(data: &[u8]) -> Item<Vec<u8>> {
    Item { data: data.to_vec(), size: data.len() as u64 }
}

const INITIAL_CAPACITY: usize = 2;

/// upstream: utils/queue_test.go#TestByteQueueResize
#[test]
fn test_byte_queue_resize() {
    let q = ByteQueue::new(INITIAL_CAPACITY);
    assert_eq!(0, q.len());
    assert!(!q.closed());

    for i in 0..INITIAL_CAPACITY {
        q.add(test_byte_queue_item(i.to_string().as_bytes()));
    }
    q.add(test_byte_queue_item(b"resize here"));
    assert_eq!(INITIAL_CAPACITY * 2, q.cap());
    q.remove();

    q.add(test_byte_queue_item(b"new resize here"));
    assert_eq!(INITIAL_CAPACITY * 2, q.cap());
    q.add(test_byte_queue_item(b"one more item, no resize must happen"));
    assert_eq!(INITIAL_CAPACITY * 2, q.cap());

    assert_eq!(INITIAL_CAPACITY + 2, q.len());
}

/// upstream: utils/queue_test.go#TestByteQueueSize
#[test]
fn test_byte_queue_size() {
    let q = ByteQueue::new(INITIAL_CAPACITY);
    assert_eq!(0, q.size());
    q.add(test_byte_queue_item(b"1"));
    q.add(test_byte_queue_item(b"2"));
    assert_eq!(2, q.size());
    q.remove();
    assert_eq!(1, q.size());
}

/// upstream: utils/queue_test.go#TestByteQueueWait
#[test]
fn test_byte_queue_wait() {
    let q = ByteQueue::new(INITIAL_CAPACITY);
    q.add(test_byte_queue_item(b"12"));
    q.add(test_byte_queue_item(b"23"));

    assert!(q.wait());
    let s = q.remove();
    assert!(s.is_some());
    assert_eq!(b"12".to_vec(), s.unwrap().data);
    assert_eq!(2, q.size());

    assert!(q.wait());
    let s = q.remove();
    assert!(s.is_some());
    assert_eq!(b"23".to_vec(), s.unwrap().data);
    assert_eq!(0, q.size());

    // go func() { q.Add(...) }(): an unsynchronized concurrent add; Wait must return
    // whether the add happens before or while it blocks.
    q.add_concurrently(test_byte_queue_item(b"3"));

    assert!(q.wait());
    assert_eq!(1, q.size());
    let s = q.remove();
    assert!(s.is_some());
    assert_eq!(b"3".to_vec(), s.unwrap().data);
    assert_eq!(0, q.size());
}

/// upstream: utils/queue_test.go#TestByteQueueClose
#[test]
fn test_byte_queue_close() {
    let q = ByteQueue::new(INITIAL_CAPACITY);

    // test removing from empty queue
    assert!(q.remove().is_none());

    q.add(test_byte_queue_item(b"1"));
    q.add(test_byte_queue_item(b"2"));
    q.close();

    assert!(!q.add(test_byte_queue_item(b"3")));

    assert!(!q.wait());

    assert!(q.remove().is_none());

    assert!(q.closed());
}

mod test_byte_queue_push {
    use super::*;

    /// upstream: utils/queue_test.go#TestByteQueuePush/returns_PushOK_and_enqueues_when_open_and_under_cap
    #[test]
    fn returns_push_ok_and_enqueues_when_open_and_under_cap() {
        let q = ByteQueue::new(INITIAL_CAPACITY);
        assert_eq!(PushResult::Ok, q.push(test_byte_queue_item(b"a"), 0));
        assert_eq!(PushResult::Ok, q.push(test_byte_queue_item(b"b"), 100));
        assert_eq!(2, q.len());
        assert_eq!(2, q.size());
    }

    /// upstream: utils/queue_test.go#TestByteQueuePush/returns_PushClosed_without_enqueueing
    #[test]
    fn returns_push_closed_without_enqueueing() {
        let q = ByteQueue::new(INITIAL_CAPACITY);
        q.close();
        assert_eq!(PushResult::Closed, q.push(test_byte_queue_item(b"a"), 0));
    }

    /// upstream: utils/queue_test.go#TestByteQueuePush/returns_PushOverflow_and_clears_when_size_>=_maxSize
    #[test]
    fn returns_push_overflow_and_clears_when_size_ge_max_size() {
        let q = ByteQueue::new(INITIAL_CAPACITY);
        q.add(test_byte_queue_item(b"12345"));
        assert_eq!(5, q.size());
        assert_eq!(PushResult::Overflow, q.push(test_byte_queue_item(b"6"), 5));
        assert_eq!(0, q.len());
        assert_eq!(0, q.size());
        assert!(!q.closed());
    }
}

/// upstream: utils/queue_test.go#TestByteQueueClear
/// note: not_observable on Go: head/tail are unexported ring-buffer fields with no accessor; the assertions before the first head() run against Go.
#[test]
fn test_byte_queue_clear() {
    let q = ByteQueue::new(INITIAL_CAPACITY);

    q.add(test_byte_queue_item(b"1"));
    q.add(test_byte_queue_item(b"2"));
    q.clear();

    assert_eq!(0, q.len());
    assert_eq!(INITIAL_CAPACITY, q.cap());
    assert_eq!(0, q.size());
    assert_eq!(0, q.head());
    assert_eq!(0, q.tail());
    assert!(!q.closed());

    q.add(test_byte_queue_item(b"3"));

    assert_eq!(1, q.len());
    assert_eq!(INITIAL_CAPACITY, q.cap());
    assert_eq!(1, q.size());
    assert_eq!(0, q.head());
    assert_eq!(1, q.tail());
    assert!(!q.closed());
}

/// upstream: utils/queue_test.go#TestQueue_vs_GC
/// note: Go arm keeps a utils.Queue[*map[string]string] in the bridge; payloads get the upstream finalizers, runtime.GC() and require.Eventually(2s, 100ms) are bridge ops. Rust equivalent of "finalized": the payload was dropped.
/// note: upstream orders the two producer goroutines with a 100ms sleep; the Rust test joins the first producer before starting the second.
#[test]
fn test_queue_vs_gc() {
    let q = TrackedQueue::new(INITIAL_CAPACITY);

    let qa = q.clone();
    std::thread::spawn(move || {
        qa.add_tracked("a");
    })
    .join()
    .unwrap();
    let qb = q.clone();
    std::thread::spawn(move || {
        qb.add_tracked("b");
    })
    .join()
    .unwrap();

    assert_eq!(2, q.len());

    q.remove();

    q.gc();

    assert!(q.released("a", true), "require.Eventually: a finalized");

    assert!(!q.released("b", false));

    q.remove();

    q.gc();

    assert!(q.released("b", true), "require.Eventually: b finalized");

    assert!(q.released("a", false));
    assert!(q.released("b", false));
}
