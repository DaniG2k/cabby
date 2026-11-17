//! Unsigned gauge (upstream `metrics/gauge.go`; decrementing below zero wraps, as upstream).

use cabby_core::pending;

#[derive(Debug)]
pub struct Gauge {
    _name: String,
    _desc: String,
}

impl Gauge {
    pub fn new(name: &str, desc: &str) -> Self {
        Self { _name: name.to_owned(), _desc: desc.to_owned() }
    }

    pub fn name(&self) -> &str {
        pending!("metrics::gauge_name")
    }

    pub fn desc(&self) -> &str {
        pending!("metrics::gauge_desc")
    }

    /// Upstream `Set(int)`.
    pub fn set(&self, _value: i64) {
        pending!("metrics::gauge_set")
    }

    /// Upstream `Set64(uint64)`.
    pub fn set64(&self, _value: u64) {
        pending!("metrics::gauge_set64")
    }

    pub fn inc(&self) -> u64 {
        pending!("metrics::gauge_inc")
    }

    pub fn dec(&self) -> u64 {
        pending!("metrics::gauge_dec")
    }

    pub fn value(&self) -> u64 {
        pending!("metrics::gauge_value")
    }
}
