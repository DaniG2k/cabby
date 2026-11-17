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

use cabby_core::pending;

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
}

/// Samples `pid` from `/proc`.
pub fn sample(_pid: u32) -> Result<ProcessResources, BenchError> {
    pending!("benchi::resources_sample")
}

/// Residual resources after a scenario compared with a baseline sampled before it:
/// threads, fds and sockets must return to the baseline (within `slack` each).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Residual {
    pub baseline: ProcessResources,
    pub after: ProcessResources,
}

impl Residual {
    pub fn returned_to_baseline(&self, _slack: u64) -> bool {
        pending!("benchi::residual_returned_to_baseline")
    }
}
