//! Bounded asynchronous HTTP broadcast publisher (upstream `lib/publisher.go`).
//!
//! `publish` never blocks on HTTP: tasks go into a queue of `max_inflight` served by
//! a worker pool. A full queue (or a closed publisher) counts the task as dropped.
//! Workers coalesce queued tasks into one JSON-array POST of up to `batch_size`.
//!
//! Workers are tasks on the harness runtime, each with its own keep-alive HTTP/1.1
//! connection, sending one POST at a time. The publisher also reports the
//! generator-side signals a benchmark needs to tell a slow server from a saturated
//! generator: the peak queue depth and the POST round-trip histogram.
//!
//! [`Publisher::with_protocol`] with [`Protocol::Pusher`] posts to the Pusher HTTP API
//! instead: one event per `POST .../events`, or `POST .../batch_events` for a batch (at
//! most [`crate::pusher::MAX_BATCH`]), each request signed ([`crate::pusher::signed_path`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::gojson;
use crate::histogram::{Histogram, HistogramSnapshot};
use crate::net::{self, Target};
use crate::pusher::{self, Protocol, PusherApp};
use crate::runtime::{block_on, now_ns, rt};

/// Per-request deadline (upstream `http.Client.Timeout`).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Upstream defaults: 64 workers (sized for an embedded server), batches of 64.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublisherOptions {
    pub workers: usize,
    pub batch_size: usize,
    /// Appends `|<worker>|<dispatch time>` to every payload when it is sent, for
    /// per-worker ordering checks and delivery latency (see [`crate::ledger`]).
    /// Off by default (upstream payloads are sent as given).
    pub stamp: bool,
}

impl Default for PublisherOptions {
    fn default() -> Self {
        Self { workers: 64, batch_size: 64, stamp: false }
    }
}

impl PublisherOptions {
    /// Upstream `WithWorkers`; non-positive values are ignored.
    pub fn with_workers(self, n: i64) -> Self {
        match usize::try_from(n) {
            Ok(n) if n > 0 => Self { workers: n, ..self },
            _ => self,
        }
    }

    /// Upstream `WithBatchSize`; `1` disables batching, non-positive values are ignored.
    pub fn with_batch_size(self, n: i64) -> Self {
        match usize::try_from(n) {
            Ok(n) if n > 0 => Self { batch_size: n, ..self },
            _ => self,
        }
    }

    /// Enables payload stamping ([`PublisherOptions::stamp`]).
    pub fn with_stamping(self, on: bool) -> Self {
        Self { stamp: on, ..self }
    }
}

struct Task {
    stream: String,
    data: String,
}

/// Where a POST goes and what its body looks like.
#[derive(Clone)]
enum Format {
    /// AnyCable's broadcast endpoint: a JSON array of `{"stream","data"}`.
    AnyCable,
    /// The Pusher HTTP API, signed for `app`.
    Pusher { app: Arc<PusherApp>, events: Arc<str>, batch: Arc<str> },
}

struct Counters {
    issued: AtomicI64,
    dropped: AtomicI64,
    completed: AtomicI64,
    /// POSTs that failed (transport error, timeout or non-2xx).
    failed_posts: AtomicI64,
    posts: AtomicI64,
    round_trip: Histogram,
}

pub struct Publisher {
    tx: RwLock<Option<mpsc::Sender<Task>>>,
    counters: Arc<Counters>,
    targets: Mutex<HashMap<String, i64>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    max_inflight: usize,
    queue_peak: AtomicUsize,
}

impl Publisher {
    /// `max_inflight < 1` is raised to 1.
    pub fn new(url: &str, max_inflight: i64, opts: PublisherOptions) -> Self {
        Self::with_protocol(url, max_inflight, opts, &Protocol::ActionCable)
    }

    /// [`Publisher::new`] for `protocol`. In Pusher mode `url` is the events endpoint
    /// (`.../apps/<id>/events`); batches go to the `batch_events` endpoint next to it and
    /// are capped at Pusher's limit.
    pub fn with_protocol(url: &str, max_inflight: i64, opts: PublisherOptions, protocol: &Protocol) -> Self {
        let target = net::parse_url(url);
        let (format, batch_size) = match protocol {
            Protocol::ActionCable => (Format::AnyCable, opts.batch_size.max(1)),
            Protocol::Pusher(app) => {
                let events = target.as_ref().map_or_else(|_| app.events_path(), |t| t.path.clone());
                let batch = match events.strip_suffix("/events") {
                    Some(prefix) => format!("{prefix}/batch_events"),
                    None => app.batch_events_path(),
                };
                let format = Format::Pusher { app: Arc::new(app.clone()), events: events.into(), batch: batch.into() };
                (format, opts.batch_size.clamp(1, pusher::MAX_BATCH))
            }
        };
        let max_inflight = usize::try_from(max_inflight).unwrap_or(0).max(1);
        let (tx, rx) = mpsc::channel(max_inflight);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let counters = Arc::new(Counters {
            issued: AtomicI64::new(0),
            dropped: AtomicI64::new(0),
            completed: AtomicI64::new(0),
            failed_posts: AtomicI64::new(0),
            posts: AtomicI64::new(0),
            round_trip: Histogram::new(),
        });
        let workers = (0..opts.workers.max(1))
            .map(|i| {
                let w = Worker {
                    index: i,
                    target: target.clone(),
                    rx: Arc::clone(&rx),
                    counters: Arc::clone(&counters),
                    batch_size,
                    stamp: opts.stamp,
                    format: format.clone(),
                    conn: None,
                };
                rt().spawn(w.run())
            })
            .collect();
        Self {
            tx: RwLock::new(Some(tx)),
            counters,
            targets: Mutex::new(HashMap::new()),
            workers: Mutex::new(workers),
            max_inflight,
            queue_peak: AtomicUsize::new(0),
        }
    }

    pub fn publish(&self, stream: &str, data: &str) {
        self.try_publish(stream, data);
    }

    /// [`Publisher::publish`] that reports whether the task was enqueued.
    pub fn try_publish(&self, stream: &str, data: &str) -> bool {
        let tx = self.tx.read().unwrap_or_else(|e| e.into_inner());
        let Some(tx) = tx.as_ref() else {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        match tx.try_send(Task { stream: stream.to_owned(), data: data.to_owned() }) {
            Ok(()) => {
                self.counters.issued.fetch_add(1, Ordering::Relaxed);
                *self.targets.lock().unwrap_or_else(|e| e.into_inner()).entry(stream.to_owned()).or_default() += 1;
                let depth = self.max_inflight - tx.capacity();
                self.queue_peak.fetch_max(depth, Ordering::Relaxed);
                true
            }
            Err(_) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// Tasks successfully enqueued.
    pub fn issued_count(&self) -> i64 {
        self.counters.issued.load(Ordering::Relaxed)
    }

    /// Tasks rejected because the queue was full or the publisher closed.
    pub fn dropped_count(&self) -> i64 {
        self.counters.dropped.load(Ordering::Relaxed)
    }

    /// Broadcasts whose POST returned 2xx.
    pub fn completed_count(&self) -> i64 {
        self.counters.completed.load(Ordering::Relaxed)
    }

    /// POSTs that failed (transport error, timeout or non-2xx status).
    pub fn failed_posts(&self) -> i64 {
        self.counters.failed_posts.load(Ordering::Relaxed)
    }

    /// POSTs sent (one per batch).
    pub fn posts(&self) -> i64 {
        self.counters.posts.load(Ordering::Relaxed)
    }

    /// Highest number of tasks waiting in the queue at once (out of `max_inflight`).
    pub fn queue_peak(&self) -> usize {
        self.queue_peak.load(Ordering::Relaxed)
    }

    pub fn max_inflight(&self) -> usize {
        self.max_inflight
    }

    /// POST round-trip times (request sent → response read).
    pub fn round_trip(&self) -> HistogramSnapshot {
        self.counters.round_trip.snapshot()
    }

    /// Issued tasks per stream.
    pub fn target_counts(&self) -> HashMap<String, i64> {
        self.targets.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Stops accepting tasks, flushes the queue and joins the workers. Idempotent.
    pub fn close(&self) {
        // Dropping the sender closes the queue once the workers have drained it.
        self.tx.write().unwrap_or_else(|e| e.into_inner()).take();
        let workers = std::mem::take(&mut *self.workers.lock().unwrap_or_else(|e| e.into_inner()));
        if workers.is_empty() {
            return;
        }
        block_on(async move {
            for w in workers {
                let _ = w.await;
            }
        });
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.close();
    }
}

struct Worker {
    index: usize,
    target: Result<Target, String>,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Task>>>,
    counters: Arc<Counters>,
    batch_size: usize,
    stamp: bool,
    format: Format,
    conn: Option<SendRequest<Full<Bytes>>>,
}

impl Worker {
    async fn run(mut self) {
        let mut batch = Vec::with_capacity(self.batch_size);
        loop {
            {
                let mut rx = self.rx.lock().await;
                let Some(first) = rx.recv().await else { return };
                batch.push(first);
                while batch.len() < self.batch_size {
                    match rx.try_recv() {
                        Ok(t) => batch.push(t),
                        Err(_) => break,
                    }
                }
            }
            self.dispatch(&batch).await;
            batch.clear();
        }
    }

    /// The JSON array body (`[{"stream":..,"data":..}]`); the broadcast endpoint
    /// accepts an object or an array, and an array serves every batch size.
    fn body(&self, batch: &[Task]) -> String {
        let stamp = self.stamp.then(|| format!("|{}|{}", self.index, now_ns()));
        if let Format::Pusher { .. } = self.format {
            return Self::pusher_body(batch, stamp.as_deref());
        }
        let mut out = String::with_capacity(batch.len() * 48);
        out.push('[');
        for (i, t) in batch.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str("{\"stream\":");
            gojson::push_str(&mut out, &t.stream);
            out.push_str(",\"data\":");
            match &stamp {
                Some(s) => gojson::push_str(&mut out, &format!("{}{s}", t.data)),
                None => gojson::push_str(&mut out, &t.data),
            }
            out.push('}');
        }
        out.push(']');
        out
    }

    /// One `{"name","channel","data"}` event, or `{"batch":[..]}` for several.
    fn pusher_body(batch: &[Task], stamp: Option<&str>) -> String {
        let mut out = String::with_capacity(batch.len() * 64 + 12);
        let single = batch.len() == 1;
        if !single {
            out.push_str("{\"batch\":[");
        }
        for (i, t) in batch.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            match stamp {
                Some(s) => pusher::push_event(&mut out, &t.stream, &format!("{}{s}", t.data)),
                None => pusher::push_event(&mut out, &t.stream, &t.data),
            }
        }
        if !single {
            out.push_str("]}");
        }
        out
    }

    /// The request path (and query) of a POST of `body` carrying `n` broadcasts.
    fn request_path(&self, target: &Target, n: usize, body: &str) -> String {
        match &self.format {
            Format::AnyCable => target.path.clone(),
            Format::Pusher { app, events, batch } => {
                pusher::signed_path(app, if n == 1 { events } else { batch }, body.as_bytes())
            }
        }
    }

    async fn dispatch(&mut self, batch: &[Task]) {
        let Ok(target) = self.target.clone() else {
            self.counters.failed_posts.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let body = self.body(batch);
        let path = self.request_path(&target, batch.len(), &body);
        self.counters.posts.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let ok = matches!(tokio::time::timeout(REQUEST_TIMEOUT, self.post(&target, &path, body)).await, Ok(Ok(true)));
        self.counters.round_trip.record(started.elapsed());
        if ok {
            self.counters.completed.fetch_add(batch.len() as i64, Ordering::Relaxed);
        } else {
            self.counters.failed_posts.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A ready keep-alive connection, reconnecting when the previous one closed.
    async fn ready(&mut self, target: &Target) -> Result<&mut SendRequest<Full<Bytes>>, String> {
        let reusable = match self.conn.as_mut() {
            Some(c) => !c.is_closed() && c.ready().await.is_ok(),
            None => false,
        };
        if !reusable {
            let stream = net::connect_tcp(target).await.map_err(|e| e.to_string())?;
            let (sender, conn) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.map_err(|e| e.to_string())?;
            rt().spawn(async move {
                let _ = conn.await;
            });
            self.conn = Some(sender);
        }
        self.conn.as_mut().ok_or_else(|| "no connection".to_owned())
    }

    /// POSTs `body` to `path`; whether the server answered 2xx.
    async fn post(&mut self, target: &Target, path: &str, body: String) -> Result<bool, String> {
        let host = format!("{}:{}", target.host, target.port);
        let req = http::Request::post(path)
            .header(http::header::HOST, host)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| e.to_string())?;
        let sender = self.ready(target).await?;
        let resp = match sender.send_request(req).await {
            Ok(r) => r,
            Err(e) => {
                self.conn = None;
                return Err(e.to_string());
            }
        };
        let status = resp.status();
        if resp.into_body().collect().await.is_err() {
            self.conn = None;
        }
        Ok(status.is_success())
    }
}
