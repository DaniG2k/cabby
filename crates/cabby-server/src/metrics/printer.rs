//! Structured-log interval writer (upstream `metrics/printer.go`).

use cabby_core::pending;

use super::{IntervalWriter, Metrics, MetricsError};

/// Logs the interval snapshot with sorted keys, optionally filtered.
#[derive(Debug)]
pub struct BasePrinter {
    _filter: Option<Vec<String>>,
}

impl BasePrinter {
    pub fn new(filter: Option<Vec<String>>) -> Self {
        Self { _filter: filter }
    }
}

impl IntervalWriter for BasePrinter {
    fn run(&self, _interval: u64) -> Result<(), MetricsError> {
        pending!("metrics::printer_run")
    }
    fn stop(&self) {
        pending!("metrics::printer_stop")
    }
    fn write(&self, _metrics: &Metrics) -> Result<(), MetricsError> {
        pending!("metrics::printer_write")
    }
}
