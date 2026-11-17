//! Shared helpers for the rpc translations (no upstream counterpart).

use std::time::{Duration, Instant};

use cabby_core::common::StringMap;

/// Deadline for every wait in this suite.
pub const DEADLINE: Duration = Duration::from_secs(10);

pub fn map(pairs: &[(&str, &str)]) -> StringMap {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
}

pub fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

/// Waits for a condition on reference state that offers no change notification
/// (Go metrics read through the bridge): a bounded readiness probe, like
/// testify's `Eventually`. Panics after `deadline`.
pub fn eventually(what: &str, deadline: Duration, mut cond: impl FnMut() -> bool) {
    let end = Instant::now() + deadline;
    while !cond() {
        assert!(Instant::now() < end, "{what}: not reached within {deadline:?}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Lets wall-clock time pass. Used only where elapsed time *is* the behavior
/// under test (a call must still be waiting after its wait timeout would have
/// expired), never to order events.
pub fn elapse(d: Duration) {
    std::thread::sleep(d);
}
