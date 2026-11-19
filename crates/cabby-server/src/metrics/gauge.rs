//! Unsigned gauge (upstream `metrics/gauge.go`; decrementing below zero wraps, as upstream).

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct Gauge {
    name: String,
    desc: String,
    value: AtomicU64,
}

impl Gauge {
    pub fn new(name: &str, desc: &str) -> Self {
        Self { name: name.to_owned(), desc: desc.to_owned(), value: AtomicU64::new(0) }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn desc(&self) -> &str {
        &self.desc
    }

    /// Upstream `Set(int)` (negative values wrap, as Go's `uint64(value)`).
    pub fn set(&self, value: i64) {
        self.value.store(value as u64, Ordering::Relaxed);
    }

    /// Upstream `Set64(uint64)`.
    pub fn set64(&self, value: u64) {
        self.value.store(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc(&self) -> u64 {
        self.value.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
    }

    #[inline]
    pub fn dec(&self) -> u64 {
        self.value.fetch_sub(1, Ordering::Relaxed).wrapping_sub(1)
    }

    pub fn value(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}
