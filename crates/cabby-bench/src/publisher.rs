//! Bounded asynchronous HTTP broadcast publisher (upstream `lib/publisher.go`).
//!
//! `publish` never blocks on HTTP: tasks go into a queue of `max_inflight` served by
//! a worker pool. A full queue (or a closed publisher) counts the task as dropped.
//! Workers coalesce queued tasks into one JSON-array POST of up to `batch_size`.

use std::collections::HashMap;

use cabby_core::pending;

/// Upstream defaults: 64 workers (sized for an embedded server), batches of 64.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublisherOptions {
    pub workers: usize,
    pub batch_size: usize,
}

impl Default for PublisherOptions {
    fn default() -> Self {
        Self { workers: 64, batch_size: 64 }
    }
}

impl PublisherOptions {
    /// Upstream `WithWorkers`; non-positive values are ignored.
    pub fn with_workers(self, _n: i64) -> Self {
        pending!("benchi::with_workers")
    }

    /// Upstream `WithBatchSize`; `1` disables batching, non-positive values are ignored.
    pub fn with_batch_size(self, _n: i64) -> Self {
        pending!("benchi::with_batch_size")
    }
}

pub struct Publisher {
    _queue: (),
}

impl Publisher {
    /// `max_inflight < 1` is raised to 1.
    pub fn new(_url: &str, _max_inflight: i64, _opts: PublisherOptions) -> Self {
        pending!("benchi::new_publisher")
    }

    pub fn publish(&self, _stream: &str, _data: &str) {
        pending!("benchi::publisher_publish")
    }

    /// Tasks successfully enqueued.
    pub fn issued_count(&self) -> i64 {
        pending!("benchi::publisher_issued_count")
    }

    /// Tasks rejected because the queue was full or the publisher closed.
    pub fn dropped_count(&self) -> i64 {
        pending!("benchi::publisher_dropped_count")
    }

    /// Broadcasts whose POST returned 2xx.
    pub fn completed_count(&self) -> i64 {
        pending!("benchi::publisher_completed_count")
    }

    /// Issued tasks per stream.
    pub fn target_counts(&self) -> HashMap<String, i64> {
        pending!("benchi::publisher_target_counts")
    }

    /// Stops accepting tasks, flushes the queue and joins the workers. Idempotent.
    pub fn close(&self) {
        pending!("benchi::publisher_close")
    }
}
