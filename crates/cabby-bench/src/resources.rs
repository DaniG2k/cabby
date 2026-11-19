//! Process resource sampling: the Rust equivalent of upstream's Go runtime
//! measurements.
//!
//! Upstream reports `runtime.NumGoroutine()` and `MemStats.HeapInuse` of the one
//! process that holds both the clients and the embedded server. With an external
//! server those numbers mean nothing for the server under test, so the harness
//! samples the server process instead:
//!
//! | upstream (Go runtime)     | here (`/proc/<pid>`)                         |
//! |---------------------------|----------------------------------------------|
//! | `peak_goroutines`         | `peak_server_threads` (`status: Threads`)    |
//! | `peak_heap_mb` (HeapInuse)| `peak_server_rss_mb` (`status: VmRSS`)       |
//! | goroutine leak checks     | residual threads/fds/sockets back to baseline|
//!
//! The same sampling of the harness's own process (`/proc/self`) tells whether the
//! load generator had CPU to spare ([`ResourceMonitor`], [`cpu_time`]).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::BenchError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProcessResources {
    /// Resident set size (`VmRSS`).
    pub rss_bytes: u64,
    /// Peak resident set size (`VmHWM`).
    pub peak_rss_bytes: u64,
    pub threads: u64,
    pub open_fds: u64,
    /// Open TCP sockets (fds that are sockets).
    pub sockets: u64,
    /// CPU time consumed so far (user + system).
    pub cpu_time: Duration,
}

fn status_kb(status: &str, key: &str) -> u64 {
    status
        .lines()
        .find_map(|l| l.strip_prefix(key)?.strip_prefix(':'))
        .and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok())
        .unwrap_or(0)
}

/// CPU time of every thread of `pid` (`"self"` for the harness), exited threads
/// included: `utime + stime` from `/proc/<pid>/stat`, in clock ticks of the
/// Linux `USER_HZ` (100 Hz on every supported architecture), so 10 ms resolution.
pub fn cpu_time(pid: &str) -> Option<Duration> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Fields after the parenthesized command name; utime and stime are fields 14
    // and 15 (indexes 11 and 12 after the name).
    let rest = &stat[stat.rfind(')')? + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    let ticks = f.get(11)?.parse::<u64>().ok()? + f.get(12)?.parse::<u64>().ok()?;
    Some(Duration::from_millis(ticks * 10))
}

/// Samples `pid` from `/proc`.
pub fn sample(pid: u32) -> Result<ProcessResources, BenchError> {
    sample_path(&pid.to_string(), true)
}

/// Samples the harness's own process.
pub fn sample_self() -> Result<ProcessResources, BenchError> {
    sample_path("self", true)
}

/// `with_fds: false` skips the fd scan (one `readlink` per descriptor, costly for a
/// server holding many connections); `open_fds` and `sockets` are then 0.
fn sample_path(pid: &str, with_fds: bool) -> Result<ProcessResources, BenchError> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .map_err(|e| BenchError(format!("reading /proc/{pid}/status: {e}")))?;
    let mut open_fds = 0;
    let mut sockets = 0;
    if with_fds && let Ok(dir) = std::fs::read_dir(format!("/proc/{pid}/fd")) {
        for entry in dir.flatten() {
            open_fds += 1;
            if std::fs::read_link(entry.path()).is_ok_and(|t| t.to_string_lossy().starts_with("socket:")) {
                sockets += 1;
            }
        }
    }
    Ok(ProcessResources {
        rss_bytes: status_kb(&status, "VmRSS") * 1024,
        peak_rss_bytes: status_kb(&status, "VmHWM") * 1024,
        threads: status_kb(&status, "Threads"),
        open_fds,
        sockets,
        cpu_time: cpu_time(pid).unwrap_or_default(),
    })
}

/// Residual resources after a scenario compared with a baseline sampled before it:
/// threads, fds and sockets must return to the baseline (within `slack` each).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Residual {
    pub baseline: ProcessResources,
    pub after: ProcessResources,
}

impl Residual {
    pub fn returned_to_baseline(&self, slack: u64) -> bool {
        self.after.threads <= self.baseline.threads + slack
            && self.after.open_fds <= self.baseline.open_fds + slack
            && self.after.sockets <= self.baseline.sockets + slack
    }
}

/// Peaks observed by a [`ResourceMonitor`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourcePeaks {
    pub rss_bytes: u64,
    pub threads: u64,
    pub open_fds: u64,
    pub sockets: u64,
    pub samples: u64,
}

/// Samples a process every `interval` on a background thread and keeps the peaks.
/// Descriptors and sockets are scanned on every tenth sample only, so the
/// monitor stays cheap next to a server with many connections.
pub struct ResourceMonitor {
    peaks: Arc<Mutex<ResourcePeaks>>,
    stop: Arc<AtomicBool>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl ResourceMonitor {
    pub fn start(pid: u32, interval: Duration) -> Self {
        let peaks = Arc::new(Mutex::new(ResourcePeaks::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let (p, s) = (Arc::clone(&peaks), Arc::clone(&stop));
        let thread = std::thread::Builder::new()
            .name("bench-resources".into())
            .spawn(move || {
                let mut next = Instant::now();
                let pid = pid.to_string();
                let mut n: u64 = 0;
                while !s.load(Ordering::SeqCst) {
                    let with_fds = n.is_multiple_of(10);
                    n += 1;
                    if let Ok(r) = sample_path(&pid, with_fds) {
                        let mut pk = p.lock().unwrap_or_else(|e| e.into_inner());
                        pk.rss_bytes = pk.rss_bytes.max(r.rss_bytes);
                        pk.threads = pk.threads.max(r.threads);
                        pk.open_fds = pk.open_fds.max(r.open_fds);
                        pk.sockets = pk.sockets.max(r.sockets);
                        pk.samples += 1;
                    }
                    next = (next + interval).max(Instant::now());
                    // Short naps so `stop` returns promptly.
                    while !s.load(Ordering::SeqCst) {
                        let now = Instant::now();
                        if now >= next {
                            break;
                        }
                        std::thread::sleep((next - now).min(Duration::from_millis(20)));
                    }
                }
            })
            .expect("spawning the resource monitor");
        Self { peaks, stop, thread: Mutex::new(Some(thread)) }
    }

    pub fn peaks(&self) -> ResourcePeaks {
        *self.peaks.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Stops sampling and returns the peaks.
    pub fn stop(&self) -> ResourcePeaks {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = t.join();
        }
        self.peaks()
    }
}

impl Drop for ResourceMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}
