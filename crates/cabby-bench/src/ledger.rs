//! Delivery reconciliation.
//!
//! Upstream reconciles counts only: a client is complete when it received at least
//! the number of broadcasts issued to its streams ([`reconcile_counts`], the logic of
//! stress_publications' `buildSummary`). SPEC.md §10 also asks for message-id
//! reconciliation, so the external harness publishes sequence-numbered payloads
//! (`<stream>:<seq>`, see [`DeliveryLedger::payload`]) and checks them per client
//! and stream ([`DeliveryLedger`]):
//!
//! * **missing**: sequence numbers issued to a client's streams that it never
//!   received (distinct ids, so duplicates cannot hide losses);
//! * **duplicates**: a sequence number received more than once;
//! * **ordering violations**: a lower sequence number received after a higher one
//!   *from the same publisher worker*. Each worker sends its POSTs one after another
//!   and takes tasks from the queue in order, so the server saw that worker's
//!   broadcasts of a stream in sequence order and must deliver them in that order.
//!   Broadcasts from different workers race on the way to the server, so their
//!   relative order is not a server property and is not checked;
//! * **misrouted**: a payload naming another stream than the one it arrived on.
//!
//! When the publisher stamps payloads ([`crate::publisher::PublisherOptions::stamp`]),
//! each one also carries its worker and its dispatch time on the harness's
//! monotonic clock (`<stream>:<seq>|<worker>|<ns>`), from which the ledger records
//! the end-to-end delivery latency (POST sent → message received).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::histogram::{Histogram, HistogramSnapshot};
use crate::runtime::now_ns;

/// Count reconciliation (upstream stress_publications summary).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CountReconciliation {
    /// Clients whose received count met or exceeded their expected count.
    pub clients_complete: usize,
    pub clients_short: usize,
    /// Sum of `expected - received` over short clients.
    pub messages_missing: i64,
}

fn distinct(streams: &[String]) -> impl Iterator<Item = &String> {
    let mut seen = HashSet::new();
    streams.iter().filter(move |s| seen.insert(s.as_str()))
}

/// Expected messages per client: the sum of issued broadcasts over the client's
/// distinct streams (upstream `computeExpected`; client ids are pool indexes).
pub fn expected_per_client(subsets: &[Vec<String>], targets: &HashMap<String, i64>) -> HashMap<String, i64> {
    subsets
        .iter()
        .enumerate()
        .map(|(i, streams)| (i.to_string(), distinct(streams).map(|s| targets.get(s).copied().unwrap_or(0)).sum()))
        .collect()
}

/// Whether every client reached its expected count (upstream `drained`).
pub fn drained(received: &HashMap<String, i64>, expected: &HashMap<String, i64>) -> bool {
    expected.iter().all(|(id, exp)| received.get(id).copied().unwrap_or(0) >= *exp)
}

pub fn reconcile_counts(received: &HashMap<String, i64>, expected: &HashMap<String, i64>) -> CountReconciliation {
    let mut r = CountReconciliation::default();
    for (id, exp) in expected {
        let got = received.get(id).copied().unwrap_or(0);
        if got >= *exp {
            r.clients_complete += 1;
        } else {
            r.clients_short += 1;
            r.messages_missing += exp - got;
        }
    }
    r
}

/// Sequence-number reconciliation per (client, stream).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SequenceReport {
    pub duplicates: u64,
    /// Messages that arrived with a lower sequence than one already seen from the
    /// same publisher worker.
    pub ordering_violations: u64,
    pub missing: u64,
    /// Payloads that named another stream than the one they arrived on.
    pub misrouted: u64,
    /// Messages whose payload was not a ledger payload.
    pub unparsable: u64,
}

/// What one client received on one stream.
#[derive(Default)]
struct StreamSeqs {
    /// Every sequence number below this was received (sequences start at 1).
    watermark: u64,
    /// Received sequence numbers above the watermark.
    ahead: BTreeSet<u64>,
    distinct: u64,
    duplicates: u64,
    ordering_violations: u64,
    /// Highest sequence received from each publisher worker.
    last_by_worker: Vec<u64>,
}

impl StreamSeqs {
    fn record(&mut self, seq: u64, worker: usize) {
        if self.watermark == 0 {
            self.watermark = 1;
        }
        if seq < self.watermark || self.ahead.contains(&seq) {
            self.duplicates += 1;
            return;
        }
        self.distinct += 1;
        if seq == self.watermark {
            self.watermark += 1;
            while self.ahead.remove(&self.watermark) {
                self.watermark += 1;
            }
        } else {
            self.ahead.insert(seq);
        }
        if self.last_by_worker.len() <= worker {
            self.last_by_worker.resize(worker + 1, 0);
        }
        let last = &mut self.last_by_worker[worker];
        if seq < *last {
            self.ordering_violations += 1;
        } else {
            *last = seq;
        }
    }
}

/// One client's records; its receiver task locks it (uncontended) per message.
#[derive(Default)]
pub(crate) struct ClientLedger {
    streams: Mutex<HashMap<String, StreamSeqs>>,
}

/// A parsed ledger payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Payload<'a> {
    stream: &'a str,
    seq: u64,
    worker: usize,
    sent_ns: Option<u64>,
}

fn parse_payload(data: &str) -> Option<Payload<'_>> {
    let (base, stamp) = match data.split_once('|') {
        Some((b, s)) => (b, Some(s)),
        None => (data, None),
    };
    let (stream, seq) = base.rsplit_once(':')?;
    let seq = seq.parse().ok()?;
    let (worker, sent_ns) = match stamp {
        Some(s) => {
            let (w, ns) = s.split_once('|')?;
            (w.parse().ok()?, Some(ns.parse().ok()?))
        }
        None => (0, None),
    };
    Some(Payload { stream, seq, worker, sent_ns })
}

pub struct DeliveryLedger {
    clients: Mutex<HashMap<String, Arc<ClientLedger>>>,
    latency: Histogram,
    misrouted: AtomicU64,
    unparsable: AtomicU64,
}

impl DeliveryLedger {
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            latency: Histogram::new(),
            misrouted: AtomicU64::new(0),
            unparsable: AtomicU64::new(0),
        }
    }

    /// Payload for the `seq`-th broadcast on `stream`.
    pub fn payload(stream: &str, seq: u64) -> String {
        format!("{stream}:{seq}")
    }

    /// The records of `client_id` (created on first use).
    pub(crate) fn client(&self, client_id: &str) -> Arc<ClientLedger> {
        let mut clients = self.clients.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(clients.entry(client_id.to_owned()).or_default())
    }

    /// Records that `client_id` received `data` on `stream`.
    pub fn record(&self, client_id: &str, stream: &str, data: &str) {
        let client = self.client(client_id);
        self.record_for(&client, stream, data);
    }

    /// [`DeliveryLedger::record`] with the client's records already looked up.
    pub(crate) fn record_for(&self, client: &ClientLedger, stream: &str, data: &str) {
        let received_ns = now_ns();
        let Some(p) = parse_payload(data) else {
            self.unparsable.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if let Some(sent) = p.sent_ns {
            self.latency.record(Duration::from_nanos(received_ns.saturating_sub(sent)));
        }
        if p.stream != stream {
            // Not a delivery on the stream it was published to.
            self.misrouted.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut streams = client.streams.lock().unwrap_or_else(|e| e.into_inner());
        match streams.get_mut(p.stream) {
            Some(s) => s.record(p.seq, p.worker),
            None => streams.entry(p.stream.to_owned()).or_default().record(p.seq, p.worker),
        }
    }

    /// Delivery latencies of stamped payloads (POST sent → message received).
    pub fn latency(&self) -> HistogramSnapshot {
        self.latency.snapshot()
    }

    /// Compares what was recorded with `issued` (last sequence per stream) for each
    /// client's streams.
    pub fn report(&self, subsets: &[Vec<String>], issued: &HashMap<String, u64>) -> SequenceReport {
        let mut r = SequenceReport {
            misrouted: self.misrouted.load(Ordering::Relaxed),
            unparsable: self.unparsable.load(Ordering::Relaxed),
            ..Default::default()
        };
        let clients = self.clients.lock().unwrap_or_else(|e| e.into_inner());
        for (i, subset) in subsets.iter().enumerate() {
            let client = clients.get(&i.to_string());
            let streams = client.map(|c| c.streams.lock().unwrap_or_else(|e| e.into_inner()));
            for stream in distinct(subset) {
                let expected = issued.get(stream).copied().unwrap_or(0);
                let got = streams.as_ref().and_then(|s| s.get(stream));
                let distinct_in_range = got.map_or(0, |g| {
                    let below = g.watermark.saturating_sub(1).min(expected);
                    below + g.ahead.range(..=expected).count() as u64
                });
                r.missing += expected.saturating_sub(distinct_in_range);
            }
        }
        for client in clients.values() {
            for s in client.streams.lock().unwrap_or_else(|e| e.into_inner()).values() {
                r.duplicates += s.duplicates;
                r.ordering_violations += s.ordering_violations;
            }
        }
        r
    }
}

impl Default for DeliveryLedger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_and_stamped_payloads() {
        assert_eq!(Some(Payload { stream: "7", seq: 3, worker: 0, sent_ns: None }), parse_payload("7:3"));
        assert_eq!(Some(Payload { stream: "a:b", seq: 9, worker: 4, sent_ns: Some(55) }), parse_payload("a:b:9|4|55"));
        assert_eq!(None, parse_payload("x"));
    }

    #[test]
    fn reports_missing_duplicates_and_per_worker_order() {
        let l = DeliveryLedger::new();
        // Client 0 on stream "1": 1, 2, 2 (dup), 4 from worker 0, then 3 from worker
        // 1 (a different worker: not an ordering violation).
        for d in ["1:1|0|0", "1:2|0|0", "1:2|0|0", "1:4|0|0", "1:3|1|0"] {
            l.record("0", "1", d);
        }
        // Client 1: 2 then 1 from the same worker: one violation; 3 and 4 missing;
        // a stream "1" payload arriving on stream "2" is misrouted.
        for d in ["1:2|0|0", "1:1|0|0"] {
            l.record("1", "1", d);
        }
        l.record("1", "2", "1:1|0|0");
        let subsets = vec![vec!["1".to_owned()], vec!["1".to_owned()]];
        let issued: HashMap<String, u64> = [("1".to_owned(), 4)].into();
        let r = l.report(&subsets, &issued);
        assert_eq!(1, r.duplicates);
        assert_eq!(1, r.ordering_violations);
        assert_eq!(2, r.missing);
        assert_eq!(1, r.misrouted);
    }
}
