//! Growable FIFO with blocking wait and size accounting (upstream generic `utils.Queue`).
//!
//! Upstream is a ring buffer guarded by an RW mutex and a condition variable. Cabby keeps
//! the ring layout and growth policy (double when full, halve when at most half full but
//! never below the initial capacity; the translated tests pin `cap`, `head` and `tail`),
//! and improves the synchronization:
//!
//! * A plain mutex: every accessor is a few loads, so reader/writer locking costs more
//!   than it saves.
//! * Producers signal the condition variable only when a consumer is actually waiting,
//!   so the common case (a writer loop that is busy sending) costs no wake-up syscall.
//! * [`Queue::wait`] tolerates spurious wake-ups and reports `false` when it is woken by
//!   [`Queue::close`] (upstream returns `true` and lets the following `Remove` fail; the
//!   callers observe the same thing either way).
//! * Slots are `Option`s: a removed item is moved out and its slot cleared, so payloads
//!   are released as soon as they leave the queue.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard};

/// A queued element with its accounted size (upstream `utils.Item`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Item<T> {
    pub size: u64,
    pub data: T,
}

/// Outcome of [`Queue::push`] (upstream `PushResult`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushResult {
    /// The item was enqueued.
    Ok,
    /// The queue is closed; the item was dropped.
    Closed,
    /// The size limit was reached; the queue was cleared and the item dropped.
    Overflow,
}

#[derive(Debug)]
struct Ring<T> {
    nodes: Vec<Option<Item<T>>>,
    head: usize,
    tail: usize,
    cnt: usize,
    size: u64,
    closed: bool,
    waiters: usize,
}

impl<T> Ring<T> {
    fn slots(n: usize) -> Vec<Option<Item<T>>> {
        let mut nodes = Vec::with_capacity(n);
        nodes.resize_with(n, || None);
        nodes
    }

    fn resize(&mut self, n: usize) {
        let mut nodes = Self::slots(n);
        let len = self.nodes.len();
        for (i, slot) in nodes.iter_mut().enumerate().take(self.cnt) {
            *slot = self.nodes[(self.head + i) % len].take();
        }
        self.tail = self.cnt % n;
        self.head = 0;
        self.nodes = nodes;
    }

    /// Appends; the caller checked that the queue is open.
    fn append(&mut self, item: Item<T>) {
        if self.cnt == self.nodes.len() {
            // Upstream doubles; an empty buffer (initial capacity 0) grows to one slot.
            self.resize((self.cnt * 2).max(1));
        }
        self.size += item.size;
        self.nodes[self.tail] = Some(item);
        self.tail = (self.tail + 1) % self.nodes.len();
        self.cnt += 1;
    }

    fn clear(&mut self, init_cap: usize) {
        self.closed = false;
        self.cnt = 0;
        self.nodes = Self::slots(init_cap);
        self.size = 0;
        self.head = 0;
        self.tail = 0;
    }
}

/// Unbounded FIFO with blocking wait and size accounting, shared between threads
/// (upstream generic `utils.Queue`, a growable ring buffer).
#[derive(Debug)]
pub struct Queue<T> {
    ring: Mutex<Ring<T>>,
    cond: Condvar,
    init_cap: usize,
    /// Mirrors `ring.cnt` (updated under `ring`) for lock-free empty checks.
    cnt: AtomicUsize,
    /// Mirrors `ring.closed` (updated under `ring`) for lock-free closed checks.
    closed: AtomicBool,
}

impl<T: Send> Queue<T> {
    pub fn new(initial_capacity: usize) -> Self {
        Queue {
            ring: Mutex::new(Ring {
                nodes: Ring::slots(initial_capacity),
                head: 0,
                tail: 0,
                cnt: 0,
                size: 0,
                closed: false,
                waiters: 0,
            }),
            cond: Condvar::new(),
            init_cap: initial_capacity,
            cnt: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Ring<T>> {
        // No user code runs under the lock; a poisoned lock still holds a valid ring.
        self.ring.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn signal(&self, ring: &Ring<T>) {
        if ring.waiters > 0 {
            self.cond.notify_one();
        }
    }

    /// Appends an item; `false` (item dropped) if the queue is closed.
    pub fn add(&self, item: Item<T>) -> bool {
        let mut ring = self.lock();
        if ring.closed {
            return false;
        }
        ring.append(item);
        self.cnt.store(ring.cnt, Ordering::SeqCst);
        self.signal(&ring);
        true
    }

    /// Appends an item unless closed or `max_size > 0` and the current size reached it.
    pub fn push(&self, item: Item<T>, max_size: u64) -> PushResult {
        let mut ring = self.lock();
        if ring.closed {
            return PushResult::Closed;
        }
        if max_size > 0 && ring.size >= max_size {
            ring.clear(self.init_cap);
            self.cnt.store(0, Ordering::SeqCst);
            self.closed.store(false, Ordering::SeqCst);
            return PushResult::Overflow;
        }
        ring.append(item);
        self.cnt.store(ring.cnt, Ordering::SeqCst);
        self.signal(&ring);
        PushResult::Ok
    }

    /// Closes the queue, discarding its contents and waking waiters.
    pub fn close(&self) {
        let mut ring = self.lock();
        ring.closed = true;
        ring.cnt = 0;
        ring.nodes = Vec::new();
        ring.size = 0;
        self.cnt.store(0, Ordering::SeqCst);
        self.closed.store(true, Ordering::SeqCst);
        if ring.waiters > 0 {
            self.cond.notify_all();
        }
    }

    pub fn closed(&self) -> bool {
        self.lock().closed
    }

    /// Lock-free check of whether [`Queue::close`] has been called.
    #[inline]
    pub fn is_closed_atomic(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Lock-free check of whether the queue currently has no queued items.
    #[inline]
    pub fn is_empty_atomic(&self) -> bool {
        self.cnt.load(Ordering::SeqCst) == 0
    }

    /// Removes all items without closing (upstream `Clear`; like upstream, this also
    /// reopens a closed queue).
    pub fn clear(&self) {
        let mut ring = self.lock();
        ring.clear(self.init_cap);
        self.cnt.store(0, Ordering::SeqCst);
        self.closed.store(false, Ordering::SeqCst);
    }

    /// Blocks until an item is available (`true`) or the queue is closed (`false`).
    pub fn wait(&self) -> bool {
        let mut ring = self.lock();
        while !ring.closed && ring.cnt == 0 {
            ring.waiters += 1;
            ring = self.cond.wait(ring).unwrap_or_else(|e| e.into_inner());
            ring.waiters -= 1;
        }
        !ring.closed
    }

    /// Removes the front item, if any and the queue is open.
    pub fn remove(&self) -> Option<Item<T>> {
        let mut ring = self.lock();
        if ring.cnt == 0 {
            return None;
        }
        let head = ring.head;
        let item = ring.nodes[head].take().expect("occupied ring slot");
        ring.head = (head + 1) % ring.nodes.len();
        ring.cnt -= 1;
        ring.size -= item.size;
        self.cnt.store(ring.cnt, Ordering::SeqCst);

        let n = ring.nodes.len() / 2;
        if n >= self.init_cap && ring.cnt <= n && n > 0 {
            ring.resize(n);
        }
        Some(item)
    }

    /// Current buffer capacity.
    pub fn cap(&self) -> usize {
        self.lock().nodes.len()
    }

    pub fn len(&self) -> usize {
        self.lock().cnt
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Sum of the queued items' sizes.
    pub fn size(&self) -> u64 {
        self.lock().size
    }

    /// Ring-buffer read position (upstream unexported `head`; exposed for the
    /// translated layout assertions).
    pub fn head(&self) -> usize {
        self.lock().head
    }

    /// Ring-buffer write position (upstream unexported `tail`).
    pub fn tail(&self) -> usize {
        self.lock().tail
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn item(n: u64) -> Item<u64> {
        Item { size: 1, data: n }
    }

    #[test]
    fn keeps_fifo_order_across_wraparound_and_resizes() {
        let q = Queue::new(2);
        let mut next_out = 0;
        let mut next_in = 0;
        for round in 0..50 {
            for _ in 0..(round % 7) + 1 {
                q.add(item(next_in));
                next_in += 1;
            }
            for _ in 0..(round % 5) {
                if let Some(i) = q.remove() {
                    assert_eq!(next_out, i.data);
                    next_out += 1;
                }
            }
        }
        while let Some(i) = q.remove() {
            assert_eq!(next_out, i.data);
            next_out += 1;
        }
        assert_eq!(next_in, next_out);
        assert_eq!(2, q.cap());
    }

    #[test]
    fn zero_initial_capacity_grows() {
        let q = Queue::new(0);
        assert!(q.add(item(1)));
        assert!(q.add(item(2)));
        assert_eq!(Some(1), q.remove().map(|i| i.data));
        assert_eq!(Some(2), q.remove().map(|i| i.data));
        assert!(q.remove().is_none());
    }

    #[test]
    fn close_wakes_waiters() {
        let q = Arc::new(Queue::<u64>::new(2));
        let waiter = {
            let q = q.clone();
            std::thread::spawn(move || q.wait())
        };
        while q.lock().waiters == 0 {
            std::thread::yield_now();
        }
        q.close();
        assert!(!waiter.join().unwrap());
    }
}
