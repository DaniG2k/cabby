//! Delivery reconciliation.
//!
//! Upstream reconciles counts only: a client is complete when it received at least
//! the number of broadcasts issued to its streams ([`reconcile_counts`], the logic of
//! stress_publications' `buildSummary`). SPEC.md §10 also asks for message-id
//! reconciliation, so the external harness can publish sequence-numbered payloads
//! (`<stream>:<seq>`) and check ordering and duplicates per client and stream
//! ([`DeliveryLedger`]).

use std::collections::HashMap;

use cabby_core::pending;

/// Count reconciliation (upstream stress_publications summary).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CountReconciliation {
    /// Clients whose received count met or exceeded their expected count.
    pub clients_complete: usize,
    pub clients_short: usize,
    /// Sum of `expected - received` over short clients.
    pub messages_missing: i64,
}

/// Expected messages per client: the sum of issued broadcasts over the client's
/// distinct streams (upstream `computeExpected`; client ids are pool indexes).
pub fn expected_per_client(_subsets: &[Vec<String>], _targets: &HashMap<String, i64>) -> HashMap<String, i64> {
    pending!("benchi::compute_expected")
}

/// Whether every client reached its expected count (upstream `drained`).
pub fn drained(_received: &HashMap<String, i64>, _expected: &HashMap<String, i64>) -> bool {
    pending!("benchi::drained")
}

pub fn reconcile_counts(_received: &HashMap<String, i64>, _expected: &HashMap<String, i64>) -> CountReconciliation {
    pending!("benchi::reconcile_counts")
}

/// Sequence-number reconciliation per (client, stream).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SequenceReport {
    pub duplicates: u64,
    /// Messages that arrived with a lower sequence than one already seen.
    pub ordering_violations: u64,
    pub missing: u64,
}

pub struct DeliveryLedger {
    _seen: (),
}

impl DeliveryLedger {
    pub fn new() -> Self {
        pending!("benchi::ledger_new")
    }

    /// Payload for the `seq`-th broadcast on `stream`.
    pub fn payload(_stream: &str, _seq: u64) -> String {
        pending!("benchi::ledger_payload")
    }

    /// Records that `client_id` received `data` on `stream`.
    pub fn record(&self, _client_id: &str, _stream: &str, _data: &str) {
        pending!("benchi::ledger_record")
    }

    /// Compares what was recorded with `issued` (last sequence per stream) for each
    /// client's streams.
    pub fn report(&self, _subsets: &[Vec<String>], _issued: &HashMap<String, u64>) -> SequenceReport {
        pending!("benchi::ledger_report")
    }
}

impl Default for DeliveryLedger {
    fn default() -> Self {
        Self::new()
    }
}
