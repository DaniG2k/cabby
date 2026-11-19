//! Bounded-memory latency histogram (upstream `lib/histogram.go`).

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

/// Smallest bucket bound (upstream `histogramMin`).
pub const HISTOGRAM_MIN: Duration = Duration::from_micros(100);
/// Bucket growth factor, ~5% relative precision (upstream `histogramGrowth`).
pub const HISTOGRAM_GROWTH: f64 = 1.05;
/// Number of log-scale buckets: 100µs × 1.05^360 ≈ 4.2h (upstream `histogramBuckets`).
pub const HISTOGRAM_BUCKETS: usize = 360;

fn bucket_for(d: Duration) -> usize {
    if d <= HISTOGRAM_MIN {
        return 0;
    }
    let ratio = d.as_nanos() as f64 / HISTOGRAM_MIN.as_nanos() as f64;
    let i = (ratio.ln() / HISTOGRAM_GROWTH.ln()) as usize + 1;
    i.min(HISTOGRAM_BUCKETS - 1)
}

/// Upper bound of bucket `i` (the value [`HistogramSnapshot::quantile`] reports).
fn bucket_upper(i: usize) -> Duration {
    Duration::from_nanos((HISTOGRAM_MIN.as_nanos() as f64 * HISTOGRAM_GROWTH.powi(i as i32)) as u64)
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// Concurrency-safe histogram with log-scale buckets from [`HISTOGRAM_MIN`]; never
/// stores raw samples.
///
/// Lock-free: recording is one atomic increment (upstream takes a mutex), so many
/// receiver tasks can record delivery latencies without contending. A snapshot's
/// count is the sum of its buckets, so it is always self-consistent.
pub struct Histogram {
    buckets: Box<[AtomicI64]>,
    max: AtomicU64,
}

impl Histogram {
    pub fn new() -> Self {
        Self { buckets: (0..HISTOGRAM_BUCKETS).map(|_| AtomicI64::new(0)).collect(), max: AtomicU64::new(0) }
    }

    pub fn record(&self, d: Duration) {
        self.buckets[bucket_for(d)].fetch_add(1, Ordering::Relaxed);
        self.max.fetch_max(nanos(d), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        let buckets: Vec<i64> = self.buckets.iter().map(|b| b.load(Ordering::Relaxed)).collect();
        HistogramSnapshot {
            count: buckets.iter().sum(),
            max: Duration::from_nanos(self.max.load(Ordering::Relaxed)),
            buckets,
        }
    }

    /// Returns the current state and resets the histogram.
    pub fn swap(&self) -> HistogramSnapshot {
        let buckets: Vec<i64> = self.buckets.iter().map(|b| b.swap(0, Ordering::Relaxed)).collect();
        HistogramSnapshot {
            count: buckets.iter().sum(),
            max: Duration::from_nanos(self.max.swap(0, Ordering::Relaxed)),
            buckets,
        }
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

impl Default for HistogramSnapshot {
    fn default() -> Self {
        Self { count: 0, max: Duration::ZERO, buckets: vec![0; HISTOGRAM_BUCKETS] }
    }
}

impl HistogramSnapshot {
    /// The q-quantile (0 < q <= 1): the upper bound of the containing bucket, capped
    /// at `max`; zero for an empty snapshot.
    pub fn quantile(&self, q: f64) -> Duration {
        if self.count == 0 {
            return Duration::ZERO;
        }
        let rank = ((q * self.count as f64).ceil() as i64).max(1);
        let mut seen = 0;
        for (i, n) in self.buckets.iter().enumerate() {
            seen += n;
            if seen >= rank {
                return bucket_upper(i).min(self.max);
            }
        }
        self.max
    }

    /// Samples recorded between `prev` (an earlier snapshot) and `self`. Max is
    /// approximated by the upper bound of the highest non-empty bucket.
    pub fn sub(&self, prev: &HistogramSnapshot) -> HistogramSnapshot {
        let mut out = HistogramSnapshot { count: self.count - prev.count, ..Default::default() };
        for (i, b) in self.buckets.iter().enumerate() {
            let d = b - prev.buckets.get(i).copied().unwrap_or(0);
            out.buckets[i] = d;
            if d > 0 {
                out.max = bucket_upper(i);
            }
        }
        out.max = out.max.min(self.max);
        out
    }
}
