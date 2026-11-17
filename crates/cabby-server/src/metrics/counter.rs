//! Monotonic counter with an interval delta (upstream `metrics/counter.go`).

use cabby_core::pending;

#[derive(Debug)]
pub struct Counter {
    _name: String,
    _desc: String,
}

impl Counter {
    pub fn new(name: &str, desc: &str) -> Self {
        Self { _name: name.to_owned(), _desc: desc.to_owned() }
    }

    pub fn name(&self) -> &str {
        pending!("metrics::counter_name")
    }

    pub fn desc(&self) -> &str {
        pending!("metrics::counter_desc")
    }

    pub fn value(&self) -> u64 {
        pending!("metrics::counter_value")
    }

    /// The delta of the last completed interval (the raw value before the first rotation).
    pub fn interval_value(&self) -> u64 {
        pending!("metrics::counter_interval_value")
    }

    /// Adds one; returns the new value.
    pub fn inc(&self) -> u64 {
        pending!("metrics::counter_inc")
    }

    /// Adds `n`; returns the new value.
    pub fn add(&self, _n: u64) -> u64 {
        pending!("metrics::counter_add")
    }

    /// Closes the current interval.
    pub fn update_delta(&self) {
        pending!("metrics::counter_update_delta")
    }
}
