//! Per-client receive counts (upstream `lib/counter.go`).

use std::collections::HashMap;

use cabby_core::pending;

/// Concurrent per-client receive counter with a separate running total that the
/// throughput sampler can read without taking the map lock.
pub struct Accumulator {
    _counts: (),
}

impl Accumulator {
    pub fn new() -> Self {
        pending!("benchi::new_accumulator")
    }

    /// Increments `client_id`'s count by one.
    pub fn bump(&self, _client_id: &str) {
        pending!("benchi::accumulator_bump")
    }

    /// Running total of [`Accumulator::bump`] calls.
    pub fn total_received(&self) -> i64 {
        pending!("benchi::accumulator_total_received")
    }

    /// A copy of the counts; mutating it never affects later snapshots.
    pub fn snapshot(&self) -> HashMap<String, i64> {
        pending!("benchi::accumulator_snapshot")
    }
}

impl Default for Accumulator {
    fn default() -> Self {
        Self::new()
    }
}
