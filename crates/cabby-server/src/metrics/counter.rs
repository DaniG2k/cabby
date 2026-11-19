//! Monotonic counter with an interval delta (upstream `metrics/counter.go`).

use std::sync::atomic::{AtomicU64, Ordering};

/// A counter. Increments are single relaxed atomic adds; the interval fields
/// are only written by rotation.
#[derive(Debug)]
pub struct Counter {
    name: String,
    desc: String,
    value: AtomicU64,
    last_interval_value: AtomicU64,
    last_interval_delta: AtomicU64,
}

impl Counter {
    pub fn new(name: &str, desc: &str) -> Self {
        Self {
            name: name.to_owned(),
            desc: desc.to_owned(),
            value: AtomicU64::new(0),
            last_interval_value: AtomicU64::new(0),
            last_interval_delta: AtomicU64::new(0),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn desc(&self) -> &str {
        &self.desc
    }

    pub fn value(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// The delta of the last completed interval (the raw value before the first rotation).
    pub fn interval_value(&self) -> u64 {
        if self.last_interval_value.load(Ordering::Relaxed) == 0 {
            return self.value();
        }
        self.last_interval_delta.load(Ordering::Relaxed)
    }

    /// Adds one; returns the new value.
    #[inline]
    pub fn inc(&self) -> u64 {
        self.add(1)
    }

    /// Adds `n`; returns the new value.
    #[inline]
    pub fn add(&self, n: u64) -> u64 {
        self.value.fetch_add(n, Ordering::Relaxed).wrapping_add(n)
    }

    /// Closes the current interval.
    pub fn update_delta(&self) {
        let now = self.value.load(Ordering::Relaxed);
        let last = self.last_interval_value.load(Ordering::Relaxed);
        self.last_interval_delta.store(now.wrapping_sub(last), Ordering::Relaxed);
        self.last_interval_value.store(now, Ordering::Relaxed);
    }
}
