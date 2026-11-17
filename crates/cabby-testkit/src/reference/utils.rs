//! Facade for upstream package `utils`.
//!
//! Stateful Go objects live in the bridge behind integer handles.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cabby_core::utils::{self as rs, Item, ItemId, MessageVerifier, PushResult, VerifierError};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::reference::common::GoParsed;
use crate::{Backend, backend, bridge};

// --- MessageVerifier ------------------------------------------------------------

pub fn verifier_generate(key: &str, payload: &Value) -> Result<String, VerifierError> {
    match backend() {
        Backend::Rust => MessageVerifier::new(key).generate(payload),
        Backend::Go => {
            let p: GoParsed<String> =
                bridge::call("utils.MessageVerifier.Generate", json!({"Key": key, "Payload": payload}));
            p.into_result(VerifierError).map(Option::unwrap_or_default)
        }
    }
}

pub fn verifier_verified(key: &str, msg: &str) -> Result<Value, VerifierError> {
    match backend() {
        Backend::Rust => MessageVerifier::new(key).verified(msg),
        Backend::Go => {
            let p: GoParsed<Value> = bridge::call("utils.MessageVerifier.Verified", json!({"Key": key, "Msg": msg}));
            p.into_result(VerifierError).map(|v| v.unwrap_or(Value::Null))
        }
    }
}

pub fn verifier_validate(key: &str, msg: &str) -> Result<(), VerifierError> {
    match backend() {
        Backend::Rust => MessageVerifier::new(key).validate(msg),
        Backend::Go => {
            let p: GoParsed<Value> = bridge::call("utils.MessageVerifier.Validate", json!({"Key": key, "Msg": msg}));
            p.into_result(VerifierError).map(|_| ())
        }
    }
}

// --- PriorityQueue ----------------------------------------------------------------

/// An item as returned by push/pop/peek: identity plus value and priority.
#[derive(Debug, Clone, PartialEq)]
pub struct PqItem {
    pub id: ItemId,
    pub value: Value,
    pub priority: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoPqItem {
    item: u64,
    value: Value,
    priority: i64,
}

impl From<GoPqItem> for PqItem {
    fn from(g: GoPqItem) -> Self {
        PqItem { id: ItemId(g.item), value: g.value, priority: g.priority }
    }
}

/// `utils.PriorityQueue[T, int]` with JSON values for `T`.
pub enum PriorityQueue {
    Rust(rs::PriorityQueue<Value, i64>),
    Go(u64),
}

impl PriorityQueue {
    pub fn new() -> Self {
        match backend() {
            Backend::Rust => PriorityQueue::Rust(rs::PriorityQueue::new()),
            Backend::Go => PriorityQueue::Go(bridge::call("utils.NewPriorityQueue", json!({}))),
        }
    }

    pub fn push_item(&mut self, value: Value, priority: i64) -> ItemId {
        match self {
            PriorityQueue::Rust(q) => q.push_item(value, priority),
            PriorityQueue::Go(h) => {
                let item: GoPqItem =
                    bridge::call("utils.PriorityQueue.PushItem", json!({"Q": h, "Value": value, "Priority": priority}));
                ItemId(item.item)
            }
        }
    }

    pub fn pop_item(&mut self) -> Option<PqItem> {
        match self {
            PriorityQueue::Rust(q) => q.pop_item().map(|i| PqItem { id: i.id, value: i.value, priority: i.priority }),
            PriorityQueue::Go(h) => {
                bridge::call::<_, Option<GoPqItem>>("utils.PriorityQueue.PopItem", json!({"Q": h})).map(Into::into)
            }
        }
    }

    pub fn peek(&self) -> Option<PqItem> {
        match self {
            PriorityQueue::Rust(q) => q.peek().map(|i| PqItem { id: i.id, value: i.value, priority: i.priority }),
            PriorityQueue::Go(h) => {
                bridge::call::<_, Option<GoPqItem>>("utils.PriorityQueue.Peek", json!({"Q": h})).map(Into::into)
            }
        }
    }

    pub fn update(&mut self, item: ItemId, priority: i64) {
        match self {
            PriorityQueue::Rust(q) => q.update(item, priority),
            PriorityQueue::Go(h) => {
                bridge::call::<_, Value>(
                    "utils.PriorityQueue.Update",
                    json!({"Q": h, "Item": item.0, "Priority": priority}),
                );
            }
        }
    }

    pub fn remove(&mut self, item: ItemId) {
        match self {
            PriorityQueue::Rust(q) => q.remove(item),
            PriorityQueue::Go(h) => {
                bridge::call::<_, Value>("utils.PriorityQueue.Remove", json!({"Q": h, "Item": item.0}));
            }
        }
    }

    /// `len(*pq)`.
    pub fn len(&self) -> usize {
        match self {
            PriorityQueue::Rust(q) => q.len(),
            PriorityQueue::Go(h) => bridge::call("utils.PriorityQueue.Len", json!({"Q": h})),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for PriorityQueue {
    fn default() -> Self {
        Self::new()
    }
}

// --- Queue ------------------------------------------------------------------------

/// `utils.Queue[[]byte]`, shareable between threads.
#[derive(Clone)]
pub enum ByteQueue {
    Rust(Arc<rs::Queue<Vec<u8>>>),
    Go(u64),
}

impl ByteQueue {
    pub fn new(initial_capacity: usize) -> Self {
        match backend() {
            Backend::Rust => ByteQueue::Rust(Arc::new(rs::Queue::new(initial_capacity))),
            Backend::Go => ByteQueue::Go(bridge::call("utils.NewQueue", json!({"InitialCapacity": initial_capacity}))),
        }
    }

    fn item_args(h: u64, item: &Item<Vec<u8>>) -> Value {
        let data = std::str::from_utf8(&item.data).expect("UTF-8 test data");
        json!({"Q": h, "Data": data, "Size": item.size})
    }

    fn go<R: serde::de::DeserializeOwned>(h: u64, op: &str) -> R {
        bridge::call(op, json!({"Q": h}))
    }

    pub fn add(&self, item: Item<Vec<u8>>) -> bool {
        match self {
            ByteQueue::Rust(q) => q.add(item),
            ByteQueue::Go(h) => bridge::call("utils.Queue.Add", Self::item_args(*h, &item)),
        }
    }

    /// Adds the item from another thread (goroutine) without waiting for it, as the
    /// upstream test does while the caller blocks in `wait`.
    pub fn add_concurrently(&self, item: Item<Vec<u8>>) {
        match self {
            ByteQueue::Rust(q) => {
                let q = Arc::clone(q);
                std::thread::spawn(move || {
                    q.add(item);
                });
            }
            ByteQueue::Go(h) => {
                bridge::call::<_, Value>("utils.Queue.AddAsync", Self::item_args(*h, &item));
            }
        }
    }

    pub fn push(&self, item: Item<Vec<u8>>, max_size: u64) -> PushResult {
        match self {
            ByteQueue::Rust(q) => q.push(item, max_size),
            ByteQueue::Go(h) => {
                let mut args = Self::item_args(*h, &item);
                args["MaxSize"] = json!(max_size);
                match bridge::call::<_, i64>("utils.Queue.Push", args) {
                    0 => PushResult::Ok,
                    1 => PushResult::Closed,
                    2 => PushResult::Overflow,
                    other => panic!("unknown Go PushResult {other}"),
                }
            }
        }
    }

    pub fn close(&self) {
        match self {
            ByteQueue::Rust(q) => q.close(),
            ByteQueue::Go(h) => Self::go::<Value>(*h, "utils.Queue.Close").as_null().expect("no result"),
        }
    }

    pub fn closed(&self) -> bool {
        match self {
            ByteQueue::Rust(q) => q.closed(),
            ByteQueue::Go(h) => Self::go(*h, "utils.Queue.Closed"),
        }
    }

    pub fn clear(&self) {
        match self {
            ByteQueue::Rust(q) => q.clear(),
            ByteQueue::Go(h) => Self::go::<Value>(*h, "utils.Queue.Clear").as_null().expect("no result"),
        }
    }

    pub fn wait(&self) -> bool {
        match self {
            ByteQueue::Rust(q) => q.wait(),
            ByteQueue::Go(h) => Self::go(*h, "utils.Queue.Wait"),
        }
    }

    pub fn remove(&self) -> Option<Item<Vec<u8>>> {
        match self {
            ByteQueue::Rust(q) => q.remove(),
            ByteQueue::Go(h) => {
                #[derive(Deserialize)]
                #[serde(rename_all = "PascalCase")]
                struct Removed {
                    data: String,
                    size: u64,
                    ok: bool,
                }
                let r: Removed = Self::go(*h, "utils.Queue.Remove");
                r.ok.then(|| Item { size: r.size, data: r.data.into_bytes() })
            }
        }
    }

    pub fn cap(&self) -> usize {
        match self {
            ByteQueue::Rust(q) => q.cap(),
            ByteQueue::Go(h) => Self::go(*h, "utils.Queue.Cap"),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ByteQueue::Rust(q) => q.len(),
            ByteQueue::Go(h) => Self::go(*h, "utils.Queue.Len"),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn size(&self) -> u64 {
        match self {
            ByteQueue::Rust(q) => q.size(),
            ByteQueue::Go(h) => Self::go(*h, "utils.Queue.Size"),
        }
    }

    /// Ring-buffer read position (upstream unexported field `head`).
    pub fn head(&self) -> usize {
        match self {
            ByteQueue::Rust(q) => q.head(),
            ByteQueue::Go(_) => unimplemented!("not_observable: utils.Queue.head is an unexported field"),
        }
    }

    /// Ring-buffer write position (upstream unexported field `tail`).
    pub fn tail(&self) -> usize {
        match self {
            ByteQueue::Rust(q) => q.tail(),
            ByteQueue::Go(_) => unimplemented!("not_observable: utils.Queue.tail is an unexported field"),
        }
    }
}

// --- GoPool -----------------------------------------------------------------------

/// Observations of the upstream `TestWorkerRespawn` schedule: the worker identity
/// seen by the first and the last task, and how many data tasks had run when the
/// last task ran (`len(dataChan)`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct WorkerRespawn {
    /// Go: goroutine ID; Rust: `GoPool::current_worker_id()`.
    pub initial: Option<u64>,
    pub current: Option<u64>,
    pub data_len: usize,
}

/// Runs the worker-respawn schedule on a pool of size 1. The Rust arm is `rust`
/// (the test's schedule on [`rs::GoPool`]); the Go arm runs the same schedule in the
/// bridge on `NewGoPool("test", 1)` with the upstream `getGID` probe.
pub fn worker_respawn(rust: impl FnOnce() -> WorkerRespawn) -> WorkerRespawn {
    match backend() {
        Backend::Rust => rust(),
        Backend::Go => bridge::call("utils.GoPool.WorkerRespawn", json!({})),
    }
}

// --- Queue of tracked pointers (TestQueue_vs_GC) ------------------------------------

/// A payload that records when it is released (Go: a finalizer on the map pointer;
/// Rust: `Drop`).
pub struct Tracked {
    _payload: HashMap<String, String>,
    dropped: Arc<AtomicBool>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

/// `utils.Queue[*map[string]string]` holding tracked payloads.
#[derive(Clone)]
pub enum TrackedQueue {
    Rust { queue: Arc<rs::Queue<Tracked>>, released: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>> },
    Go(u64),
}

/// upstream `require.Eventually(..., 2*time.Second, 100*time.Millisecond)` bounds.
const EVENTUALLY: Duration = Duration::from_secs(2);
const TICK: Duration = Duration::from_millis(100);

impl TrackedQueue {
    pub fn new(initial_capacity: usize) -> Self {
        match backend() {
            Backend::Rust => {
                TrackedQueue::Rust { queue: Arc::new(rs::Queue::new(initial_capacity)), released: Arc::default() }
            }
            Backend::Go => {
                TrackedQueue::Go(bridge::call("utils.NewPtrQueue", json!({"InitialCapacity": initial_capacity})))
            }
        }
    }

    /// Adds `Item{Size: 1, Data: &map[string]string{"foo": id}}` with release tracking.
    pub fn add_tracked(&self, id: &str) -> bool {
        match self {
            TrackedQueue::Rust { queue, released } => {
                let dropped = Arc::new(AtomicBool::new(false));
                released.lock().unwrap().insert(id.to_owned(), dropped.clone());
                let payload = [("foo".to_owned(), id.to_owned())].into();
                queue.add(Item { size: 1, data: Tracked { _payload: payload, dropped } })
            }
            TrackedQueue::Go(h) => bridge::call("utils.PtrQueue.AddTracked", json!({"Q": h, "ID": id})),
        }
    }

    /// `q.Remove()`, discarding the item.
    pub fn remove(&self) -> bool {
        match self {
            TrackedQueue::Rust { queue, .. } => queue.remove().is_some(),
            TrackedQueue::Go(h) => bridge::call("utils.PtrQueue.Remove", json!({"Q": h})),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            TrackedQueue::Rust { queue, .. } => queue.len(),
            TrackedQueue::Go(h) => bridge::call("utils.PtrQueue.Len", json!({"Q": h})),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `runtime.GC()` (nothing to do in Rust: release happens on drop).
    pub fn gc(&self) {
        match self {
            TrackedQueue::Rust { .. } => {}
            TrackedQueue::Go(_) => {
                bridge::call::<_, Value>("utils.RuntimeGC", json!({}));
            }
        }
    }

    /// Whether the payload added as `id` was released; with `eventually`, polls within
    /// upstream's `require.Eventually` bounds.
    pub fn released(&self, id: &str, eventually: bool) -> bool {
        match self {
            TrackedQueue::Rust { released, .. } => {
                let flag = released.lock().unwrap().get(id).cloned().expect("tracked id");
                let deadline = Instant::now() + EVENTUALLY;
                loop {
                    if flag.load(Ordering::SeqCst) {
                        return true;
                    }
                    if !eventually || Instant::now() >= deadline {
                        return false;
                    }
                    std::thread::sleep(TICK);
                }
            }
            TrackedQueue::Go(h) => {
                bridge::call("utils.PtrQueue.Finalized", json!({"Q": h, "ID": id, "Eventually": eventually}))
            }
        }
    }
}

/// The Rust worker pool, for the Rust arm of [`worker_respawn`] only.
pub fn rust_go_pool(name: &str, size: usize) -> rs::GoPool {
    match backend() {
        Backend::Rust => rs::GoPool::new(name, size),
        Backend::Go => unreachable!("the Go arm of worker_respawn runs in the bridge"),
    }
}
