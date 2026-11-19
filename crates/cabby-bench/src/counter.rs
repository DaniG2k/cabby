//! Per-client receive counts (upstream `lib/counter.go`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

/// Concurrent per-client receive counter with a separate running total that the
/// throughput sampler can read without taking the map lock.
///
/// Upstream guards one map with a mutex. Here each client has its own atomic cell:
/// pool drain tasks look theirs up once ([`Accumulator::handle`]) and then count
/// without any lock, so the counter is not a point of contention for the load
/// generator at high message rates.
pub struct Accumulator {
    counts: RwLock<HashMap<String, Arc<AtomicI64>>>,
    total: AtomicI64,
}

impl Accumulator {
    pub fn new() -> Self {
        Self { counts: RwLock::new(HashMap::new()), total: AtomicI64::new(0) }
    }

    /// Increments `client_id`'s count by one.
    pub fn bump(&self, client_id: &str) {
        let cell = self.handle(client_id);
        self.bump_handle(&cell);
    }

    /// The counter cell of `client_id` (created on first use).
    pub(crate) fn handle(&self, client_id: &str) -> Arc<AtomicI64> {
        if let Some(c) = self.counts.read().unwrap_or_else(|e| e.into_inner()).get(client_id) {
            return Arc::clone(c);
        }
        let mut w = self.counts.write().unwrap_or_else(|e| e.into_inner());
        Arc::clone(w.entry(client_id.to_owned()).or_default())
    }

    /// Increments the client whose cell is `cell` (from [`Accumulator::handle`]).
    pub(crate) fn bump_handle(&self, cell: &AtomicI64) {
        cell.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    /// Running total of [`Accumulator::bump`] calls.
    pub fn total_received(&self) -> i64 {
        self.total.load(Ordering::Relaxed)
    }

    /// A copy of the counts; mutating it never affects later snapshots.
    pub fn snapshot(&self) -> HashMap<String, i64> {
        self.counts
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect()
    }
}

impl Default for Accumulator {
    fn default() -> Self {
        Self::new()
    }
}
