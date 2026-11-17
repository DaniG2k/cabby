//! Bounded-memory latency histogram (upstream `lib/histogram.go`).

use std::time::Duration;

use cabby_core::pending;

/// Smallest bucket bound (upstream `histogramMin`).
pub const HISTOGRAM_MIN: Duration = Duration::from_micros(100);
/// Bucket growth factor, ~5% relative precision (upstream `histogramGrowth`).
pub const HISTOGRAM_GROWTH: f64 = 1.05;
/// Number of log-scale buckets: 100µs × 1.05^360 ≈ 4.2h (upstream `histogramBuckets`).
pub const HISTOGRAM_BUCKETS: usize = 360;

/// Concurrency-safe histogram with log-scale buckets from [`HISTOGRAM_MIN`]; never
/// stores raw samples.
pub struct Histogram {
    _buckets: (),
}

impl Histogram {
    pub fn new() -> Self {
        pending!("benchi::histogram_new")
    }

    pub fn record(&self, _d: Duration) {
        pending!("benchi::histogram_record")
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        pending!("benchi::histogram_snapshot")
    }

    /// Returns the current state and resets the histogram.
    pub fn swap(&self) -> HistogramSnapshot {
        pending!("benchi::histogram_swap")
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable copy of a [`Histogram`].
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramSnapshot {
    pub count: i64,
    pub max: Duration,
    pub buckets: Vec<i64>,
}

impl HistogramSnapshot {
    /// The q-quantile (0 < q <= 1): the upper bound of the containing bucket, capped
    /// at `max`; zero for an empty snapshot.
    pub fn quantile(&self, _q: f64) -> Duration {
        pending!("benchi::histogram_quantile")
    }

    /// Samples recorded between `prev` (an earlier snapshot) and `self`.
    pub fn sub(&self, _prev: &HistogramSnapshot) -> HistogramSnapshot {
        pending!("benchi::histogram_sub")
    }
}
