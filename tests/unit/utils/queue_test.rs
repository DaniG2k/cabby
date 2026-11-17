//! upstream file: utils/queue_test.go

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cabby_core::utils::{Item, PushResult};
use cabby_testkit::reference::utils::{ByteQueue, rust_queue};

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
#[test]
fn test_byte_queue_clear() {
    // head/tail are unexported ring-buffer fields upstream: against Go this case stops
    // (not observable) at the first head() after verifying the preceding assertions.
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

/// Records when its payload is dropped (upstream sets a GC finalizer).
struct Tracked {
    _payload: std::collections::HashMap<String, String>,
    dropped: Arc<AtomicBool>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

/// upstream: utils/queue_test.go#TestQueue_vs_GC
#[test]
fn test_queue_vs_gc() {
    // Upstream checks with runtime finalizers that a removed item is no longer
    // referenced by the queue (collectable after runtime.GC()). The Rust equivalent:
    // a removed item is dropped as soon as the caller drops it, while items still
    // queued stay alive.
    let q = Arc::new(rust_queue::<Tracked>(INITIAL_CAPACITY, "Go GC finalizers on queued pointers"));

    let a = Arc::new(AtomicBool::new(false));
    let b = Arc::new(AtomicBool::new(false));

    // Upstream orders the two producer goroutines with a 100ms sleep; join instead.
    let (qa, fa) = (Arc::clone(&q), Arc::clone(&a));
    std::thread::spawn(move || {
        let payload = [("foo".to_owned(), "a".to_owned())].into();
        qa.add(Item { size: 1, data: Tracked { _payload: payload, dropped: fa } });
    })
    .join()
    .unwrap();
    let (qb, fb) = (Arc::clone(&q), Arc::clone(&b));
    std::thread::spawn(move || {
        let payload = [("foo".to_owned(), "b".to_owned())].into();
        qb.add(Item { size: 1, data: Tracked { _payload: payload, dropped: fb } });
    })
    .join()
    .unwrap();

    assert_eq!(2, q.len());

    drop(q.remove());

    assert!(a.load(Ordering::SeqCst));

    assert!(!b.load(Ordering::SeqCst));

    drop(q.remove());

    assert!(b.load(Ordering::SeqCst));

    assert!(a.load(Ordering::SeqCst));
    assert!(b.load(Ordering::SeqCst));
}
