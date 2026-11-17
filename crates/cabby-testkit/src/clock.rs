//! A controlled clock for unit tests (SPEC.md §6): time only moves when the test
//! advances it. It starts at the current wall-clock time so timestamps computed
//! by tests (e.g. `now - 10s`) also make sense to a Go reference using real time.

use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cabby_core::broker::Clock;

#[derive(Debug)]
pub struct ManualClock {
    now: Mutex<SystemTime>,
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl ManualClock {
    pub fn new() -> Self {
        Self { now: Mutex::new(SystemTime::now()) }
    }

    /// Moves time forward (replaces an upstream `time.Sleep(d)`).
    pub fn advance(&self, d: Duration) {
        *self.now.lock().unwrap() += d;
    }

    /// Unix seconds (Go `time.Now().Unix()`).
    pub fn unix(&self) -> i64 {
        self.now.lock().unwrap().duration_since(UNIX_EPOCH).expect("after epoch").as_secs() as i64
    }
}

impl Clock for ManualClock {
    fn now(&self) -> SystemTime {
        *self.now.lock().unwrap()
    }
}
