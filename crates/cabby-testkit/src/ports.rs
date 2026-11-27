//! Reserved loopback ports for servers that are told their port before they
//! start (see [`cabby_bench::ports`], which the benchmark harness uses too).
//!
//! Hold the [`Port`] for as long as anything uses the port; [`Port::keep`]
//! reserves it until the process exits.

pub use cabby_bench::ports::{BLOCK, Port, RANGE, lock_dir, reserve, reserve_n};
