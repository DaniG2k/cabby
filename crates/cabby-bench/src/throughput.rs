//! Rate timelines and sliding-window statistics (upstream `lib/throughput.go`).

use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

/// A monotonic counter the sampler polls (received messages, completed POSTs).
pub type CounterSource = Arc<dyn Fn() -> i64 + Send + Sync>;

/// One (timestamp, count) pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveSample {
    pub t: SystemTime,
    pub n: i64,
}

struct State {
    samples: Vec<ReceiveSample>,
    stopped: bool,
}

struct Shared {
    state: Mutex<State>,
    cv: Condvar,
}

/// Polls a counter every `interval`. The first sample is taken synchronously in
/// [`ReceiveSampler::start`] and a final one in [`ReceiveSampler::stop`].
///
/// Sample times are wall-clock values derived from a monotonic clock (the wall
/// time at start plus the monotonic time elapsed since), so rates never jump
/// with clock adjustments.
pub struct ReceiveSampler {
    interval: Duration,
    source: CounterSource,
    shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl ReceiveSampler {
    pub fn new(interval: Duration, source: CounterSource) -> Self {
        Self {
            interval,
            source,
            shared: Arc::new(Shared {
                state: Mutex::new(State { samples: Vec::new(), stopped: false }),
                cv: Condvar::new(),
            }),
            thread: Mutex::new(None),
        }
    }

    pub fn start(&self) {
        let wall = SystemTime::now();
        let mono = Instant::now();
        let at = move |i: Instant| wall + i.duration_since(mono);
        {
            let mut st = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
            st.samples.push(ReceiveSample { t: wall, n: (self.source)() });
        }
        let shared = Arc::clone(&self.shared);
        let source = Arc::clone(&self.source);
        let interval = self.interval.max(Duration::from_nanos(1));
        let handle = std::thread::Builder::new()
            .name("bench-sampler".into())
            .spawn(move || {
                // Ticks on the grid start + k·interval; missed ticks are skipped
                // (Go ticker semantics).
                let mut next = mono + interval;
                let mut st = shared.state.lock().unwrap_or_else(|e| e.into_inner());
                loop {
                    let now = Instant::now();
                    if st.stopped {
                        st.samples.push(ReceiveSample { t: at(now), n: source() });
                        return;
                    }
                    if now >= next {
                        st.samples.push(ReceiveSample { t: at(now), n: source() });
                        while next <= now {
                            next += interval;
                        }
                        continue;
                    }
                    st = shared.cv.wait_timeout(st, next - now).unwrap_or_else(|e| e.into_inner()).0;
                }
            })
            .expect("spawning the sampler thread");
        *self.thread.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
    }

    /// Stops sampling and waits for the sampler thread. Safe to call more than once.
    pub fn stop(&self) {
        self.shared.state.lock().unwrap_or_else(|e| e.into_inner()).stopped = true;
        self.shared.cv.notify_all();
        let handle = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }

    /// A copy of the samples collected so far.
    pub fn samples(&self) -> Vec<ReceiveSample> {
        self.shared.state.lock().unwrap_or_else(|e| e.into_inner()).samples.clone()
    }
}

impl Drop for ReceiveSampler {
    fn drop(&mut self) {
        self.stop();
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

/// `b - a` in nanoseconds (negative when `b` is earlier).
fn delta_ns(a: SystemTime, b: SystemTime) -> i128 {
    match b.duration_since(a) {
        Ok(d) => d.as_nanos() as i128,
        Err(e) => -(e.duration().as_nanos() as i128),
    }
}

/// `(last.n - first.n) / (last.t - first.t)`; zero with fewer than two samples or a
/// non-positive duration.
pub fn overall(samples: &[ReceiveSample]) -> f64 {
    let (Some(first), Some(last)) = (samples.first(), samples.last()) else { return 0.0 };
    if samples.len() < 2 {
        return 0.0;
    }
    let dt = delta_ns(first.t, last.t) as f64 / 1e9;
    if dt <= 0.0 {
        return 0.0;
    }
    (last.n - first.n) as f64 / dt
}

/// Max/min/p50/p95 of the rates over every window of size `window` ending at a sample
/// (percentiles interpolate linearly, like numpy's default).
pub fn compute_window_stats(samples: &[ReceiveSample], window: Duration) -> WindowStats {
    if samples.len() < 2 || window.is_zero() {
        return WindowStats::default();
    }
    let window = window.as_nanos() as i128;
    let mut rates = Vec::new();
    let mut j = 0;
    for i in 1..samples.len() {
        while j + 1 < i && delta_ns(samples[j + 1].t, samples[i].t) >= window {
            j += 1;
        }
        let span = delta_ns(samples[j].t, samples[i].t);
        if span < window || span <= 0 {
            continue;
        }
        rates.push((samples[i].n - samples[j].n) as f64 / (span as f64 / 1e9));
    }
    if rates.is_empty() {
        return WindowStats::default();
    }
    let min = rates.iter().copied().fold(f64::INFINITY, f64::min);
    let max = rates.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    rates.sort_by(f64::total_cmp);
    WindowStats { max, min, p50: percentile(&rates, 0.50), p95: percentile(&rates, 0.95), valid: true }
}

/// The p-th percentile (p in [0, 1]) of a sorted slice, interpolating linearly.
pub(crate) fn percentile(sorted: &[f64], p: f64) -> f64 {
    match sorted.len() {
        0 => 0.0,
        1 => sorted[0],
        n => {
            let rank = p * (n - 1) as f64;
            let lo = rank as usize;
            if lo >= n - 1 {
                return sorted[n - 1];
            }
            let frac = rank - lo as f64;
            sorted[lo] + frac * (sorted[lo + 1] - sorted[lo])
        }
    }
}
