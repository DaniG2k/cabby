//! Utilities (upstream package `utils`): worker pool, message verifier,
//! priority queue and the session send queue.

use std::marker::PhantomData;

use crate::pending;

// --- GoPool -------------------------------------------------------------------

/// Tasks a worker runs before it is replaced by a fresh one (upstream
/// `workerRespawnThreshold`).
pub const WORKER_RESPAWN_THRESHOLD: usize = 1 << 16;

/// Identity of a pool worker (upstream tests compare goroutine IDs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerId(pub u64);

pub type Task = Box<dyn FnOnce() + Send + 'static>;

/// A bounded pool of reusable workers (upstream `utils.GoPool`).
pub struct GoPool {
    _name: String,
    _size: usize,
}

impl GoPool {
    pub fn new(_name: &str, _size: usize) -> Self {
        pending!("utils::new_go_pool")
    }

    pub fn name(&self) -> String {
        pending!("utils::go_pool_name")
    }

    pub fn size(&self) -> usize {
        pending!("utils::go_pool_size")
    }

    /// Schedules a task on the pool's workers, blocking while the pool is saturated.
    pub fn schedule(&self, _task: Task) {
        pending!("utils::go_pool_schedule")
    }

    /// The worker executing the calling task (`None` outside pool workers). This is
    /// Cabby's equivalent of the goroutine ID upstream tests observe.
    pub fn current_worker_id() -> Option<WorkerId> {
        pending!("utils::go_pool_current_worker_id")
    }
}

// --- MessageVerifier ----------------------------------------------------------

/// A verification failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct VerifierError(pub String);

/// Rails-compatible `MessageVerifier` (Base64 JSON payload + `--` + HMAC-SHA256 hex digest).
#[derive(Debug, Clone)]
pub struct MessageVerifier {
    _key: Vec<u8>,
}

impl MessageVerifier {
    pub fn new(key: &str) -> Self {
        MessageVerifier { _key: key.as_bytes().to_vec() }
    }

    /// Signs a JSON-encodable payload.
    pub fn generate(&self, _payload: &serde_json::Value) -> Result<String, VerifierError> {
        pending!("utils::message_verifier_generate")
    }

    /// Verifies and decodes a signed message.
    pub fn verified(&self, _msg: &str) -> Result<serde_json::Value, VerifierError> {
        pending!("utils::message_verifier_verified")
    }

    /// Checks the signature only.
    pub fn validate(&self, _msg: &str) -> Result<(), VerifierError> {
        pending!("utils::message_verifier_validate")
    }
}

// --- PriorityQueue ------------------------------------------------------------

/// Handle of an item pushed into a [`PriorityQueue`]. Upstream returns item
/// pointers and tests compare them by identity; handles compare the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ItemId(pub u64);

/// A queued item (upstream `PriorityQueueItem`).
#[derive(Debug, Clone, PartialEq)]
pub struct PriorityQueueItem<T, P> {
    pub id: ItemId,
    pub value: T,
    pub priority: P,
}

/// Min-priority queue (upstream generic `utils.PriorityQueue`).
#[derive(Debug)]
pub struct PriorityQueue<T, P> {
    _marker: PhantomData<(T, P)>,
}

impl<T: Clone, P: PartialOrd + Clone> PriorityQueue<T, P> {
    pub fn new() -> Self {
        pending!("utils::new_priority_queue")
    }

    pub fn push_item(&mut self, _value: T, _priority: P) -> ItemId {
        pending!("utils::priority_queue_push_item")
    }

    /// Removes and returns the lowest-priority item.
    pub fn pop_item(&mut self) -> Option<PriorityQueueItem<T, P>> {
        pending!("utils::priority_queue_pop_item")
    }

    /// Changes the priority of an item still in the queue.
    pub fn update(&mut self, _item: ItemId, _priority: P) {
        pending!("utils::priority_queue_update")
    }

    /// The lowest-priority item, if any.
    pub fn peek(&self) -> Option<PriorityQueueItem<T, P>> {
        pending!("utils::priority_queue_peek")
    }

    pub fn remove(&mut self, _item: ItemId) {
        pending!("utils::priority_queue_remove")
    }

    pub fn len(&self) -> usize {
        pending!("utils::priority_queue_len")
    }

    pub fn is_empty(&self) -> bool {
        pending!("utils::priority_queue_len")
    }
}

impl<T: Clone, P: PartialOrd + Clone> Default for PriorityQueue<T, P> {
    fn default() -> Self {
        Self::new()
    }
}

// --- Queue --------------------------------------------------------------------

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

/// Unbounded FIFO with blocking wait and size accounting, shared between threads
/// (upstream generic `utils.Queue`, a growable ring buffer).
#[derive(Debug)]
pub struct Queue<T> {
    _marker: PhantomData<T>,
}

impl<T: Send> Queue<T> {
    pub fn new(_initial_capacity: usize) -> Self {
        pending!("utils::new_queue")
    }

    /// Appends an item; `false` (item dropped) if the queue is closed.
    pub fn add(&self, _item: Item<T>) -> bool {
        pending!("utils::queue_add")
    }

    /// Appends an item unless closed or `max_size > 0` and the current size reached it.
    pub fn push(&self, _item: Item<T>, _max_size: u64) -> PushResult {
        pending!("utils::queue_push")
    }

    /// Closes the queue, discarding its contents and waking waiters.
    pub fn close(&self) {
        pending!("utils::queue_close")
    }

    pub fn closed(&self) -> bool {
        pending!("utils::queue_closed")
    }

    /// Removes all items without closing.
    pub fn clear(&self) {
        pending!("utils::queue_clear")
    }

    /// Blocks until an item is available (`true`) or the queue is closed (`false`).
    pub fn wait(&self) -> bool {
        pending!("utils::queue_wait")
    }

    /// Removes the front item, if any and the queue is open.
    pub fn remove(&self) -> Option<Item<T>> {
        pending!("utils::queue_remove")
    }

    /// Current buffer capacity.
    pub fn cap(&self) -> usize {
        pending!("utils::queue_cap")
    }

    pub fn len(&self) -> usize {
        pending!("utils::queue_len")
    }

    pub fn is_empty(&self) -> bool {
        pending!("utils::queue_len")
    }

    /// Sum of the queued items' sizes.
    pub fn size(&self) -> u64 {
        pending!("utils::queue_size")
    }

    /// Ring-buffer read position (upstream unexported `head`; exposed for the
    /// translated layout assertions).
    pub fn head(&self) -> usize {
        pending!("utils::queue_head")
    }

    /// Ring-buffer write position (upstream unexported `tail`).
    pub fn tail(&self) -> usize {
        pending!("utils::queue_tail")
    }
}
