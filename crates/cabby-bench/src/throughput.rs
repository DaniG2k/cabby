//! Rate timelines and sliding-window statistics (upstream `lib/throughput.go`).

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use cabby_core::pending;

/// A monotonic counter the sampler polls (received messages, completed POSTs).
pub type CounterSource = Arc<dyn Fn() -> i64 + Send + Sync>;

/// One (timestamp, count) pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveSample {
    pub t: SystemTime,
    pub n: i64,
}

/// Polls a counter every `interval`. The first sample is taken synchronously in
/// [`ReceiveSampler::start`] and a final one in [`ReceiveSampler::stop`].
pub struct ReceiveSampler {
    _interval: Duration,
    _source: CounterSource,
}

impl ReceiveSampler {
    pub fn new(_interval: Duration, _source: CounterSource) -> Self {
        pending!("benchi::new_receive_sampler")
    }

    pub fn start(&self) {
        pending!("benchi::receive_sampler_start")
    }

    /// Stops sampling and waits for the sampler thread. Safe to call more than once.
    pub fn stop(&self) {
        pending!("benchi::receive_sampler_stop")
    }

    /// A copy of the samples collected so far.
    pub fn samples(&self) -> Vec<ReceiveSample> {
        pending!("benchi::receive_sampler_samples")
    }
}

/// Rates (per second) over sliding windows of a fixed size. `valid` is false when no
/// full window could be evaluated.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct WindowStats {
    pub max: f64,
    pub min: f64,
    pub p50: f64,
    pub p95: f64,
    pub valid: bool,
}

/// `(last.n - first.n) / (last.t - first.t)`; zero with fewer than two samples or a
/// non-positive duration.
pub fn overall(_samples: &[ReceiveSample]) -> f64 {
    pending!("benchi::overall")
}

/// Max/min/p50/p95 of the rates over every window of size `window` ending at a sample
/// (percentiles interpolate linearly, like numpy's default).
pub fn compute_window_stats(_samples: &[ReceiveSample], _window: Duration) -> WindowStats {
    pending!("benchi::compute_window_stats")
}
