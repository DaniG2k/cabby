//! StatsD interval writer (upstream `metrics/statsd_writer.go`).

use cabby_core::common::StringMap;
use cabby_core::pending;

use super::{IntervalWriter, Metrics, MetricsError, StatsdConfig};

/// Sends counter interval values (`|c`) and gauge values (`|g`) over UDP with the
/// configured prefix and default tags in the configured tag format.
#[derive(Debug)]
pub struct StatsdWriter {
    _config: StatsdConfig,
    _tags: Option<StringMap>,
}

impl StatsdWriter {
    pub fn new(config: StatsdConfig, tags: Option<StringMap>) -> Self {
        Self { _config: config, _tags: tags }
    }
}

impl IntervalWriter for StatsdWriter {
    fn run(&self, _interval: u64) -> Result<(), MetricsError> {
        pending!("metrics::statsd_run")
    }
    fn stop(&self) {
        pending!("metrics::statsd_stop")
    }
    fn write(&self, _metrics: &Metrics) -> Result<(), MetricsError> {
        pending!("metrics::statsd_write")
    }
}
