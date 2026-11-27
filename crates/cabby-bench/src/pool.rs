//! A pool of connected, subscribed clients (upstream `lib/pool.go`).

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};

use tokio::sync::Semaphore;

use crate::BenchError;
use crate::client::Client;
use crate::counter::Accumulator;
use crate::ledger::{ClientLedger, DeliveryLedger};
use crate::pusher::Wire;
use crate::rng::Rng;
use crate::runtime::{block_on, rt};

/// Called once per client after a setup phase; must be safe to call concurrently.
pub type PoolHook = Arc<dyn Fn() + Send + Sync>;

/// Shape of a pool build. `c` clients each subscribe to a random `s`-sized subset of
/// streams `"1"..="big_s"`; up to `tolerance` setup failures are accepted.
#[derive(Clone, Default)]
pub struct PoolConfig {
    pub server_url: String,
    pub c: usize,
    pub s: usize,
    pub big_s: usize,
    pub tolerance: usize,
    pub seed: u64,
    /// Receives `bump(client_id)` for every message a pool client receives.
    pub accumulator: Option<Arc<Accumulator>>,
    /// Receives every message a pool client receives, for sequence reconciliation
    /// (Cabby addition, see `docs/benchmarks.md`, "Method").
    pub ledger: Option<Arc<DeliveryLedger>>,
    /// Overrides subset generation; its length must equal `c`.
    pub streams: Option<Vec<Vec<String>>>,
    pub on_connect: Option<PoolHook>,
    pub on_subscribe: Option<PoolHook>,
    /// Wire format of the clients (Action Cable `$pubsub` streams, or public Pusher
    /// channels).
    pub wire: Wire,
}

struct Entry {
    client: Arc<Client>,
    streams: Vec<String>,
}

/// Connected clients. Client ids are the decimal indexes `"0"..` in pool order.
///
/// Upstream runs a drain goroutine per client that reads its merged queue and
/// bumps the accumulator. Here each client's connection reader records its
/// messages directly ([`Recorder`]): no queue and no task hand-off per message,
/// which keeps the generator's cost per delivered message low. As upstream,
/// closing a client stops its counting.
pub struct ClientPool {
    entries: Vec<Entry>,
    closed: AtomicBool,
}

/// Counts and reconciles one pool client's messages, called by its reader.
pub(crate) struct Recorder {
    id: String,
    acc: Option<Arc<Accumulator>>,
    cell: OnceLock<Arc<AtomicI64>>,
    ledger: Option<(Arc<DeliveryLedger>, Arc<ClientLedger>)>,
}

impl Recorder {
    fn new(id: String, acc: Option<Arc<Accumulator>>, ledger: Option<Arc<DeliveryLedger>>) -> Self {
        let ledger = ledger.map(|l| {
            let records = l.client(&id);
            (l, records)
        });
        Self { id, acc, cell: OnceLock::new(), ledger }
    }

    pub(crate) fn record(&self, stream: &str, data: &str) {
        if let Some(a) = &self.acc {
            // The cell is created on the first message, so clients that received
            // nothing are absent from snapshots, as upstream.
            a.bump_handle(self.cell.get_or_init(|| a.handle(&self.id)));
        }
        if let Some((l, records)) = &self.ledger {
            l.record_for(records, stream, data);
        }
    }
}

/// Setup parallelism: bounded like upstream (`NumCPU × 8`) so the server's accept
/// loop is not flooded with dials, which would produce false timeouts.
fn setup_concurrency(c: usize) -> usize {
    let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
    (cpus * 8).clamp(1, c.max(1))
}

impl ClientPool {
    /// Builds the pool with bounded setup parallelism. Over-tolerance failures close
    /// every client and return `pool setup failed: N failure(s) exceed tolerance T:
    /// [client <i>: <phase>: <err> ...]` (phase: build, connect, subscribe).
    pub fn build(cfg: PoolConfig) -> Result<ClientPool, BenchError> {
        block_on(Self::build_async(cfg))
    }

    async fn build_async(cfg: PoolConfig) -> Result<ClientPool, BenchError> {
        if cfg.c == 0 {
            return Err(BenchError("PoolConfig.C must be positive".into()));
        }
        if let Some(s) = &cfg.streams
            && s.len() != cfg.c
        {
            return Err(BenchError(format!("PoolConfig.Streams has length {}, want {}", s.len(), cfg.c)));
        }
        let subsets = cfg.streams.clone().unwrap_or_else(|| build_stream_subsets(cfg.seed, cfg.c, cfg.s, cfg.big_s));
        let sem = Arc::new(Semaphore::new(setup_concurrency(cfg.c)));
        let mut tasks = Vec::with_capacity(cfg.c);
        for (idx, streams) in subsets.into_iter().enumerate() {
            let permit = Arc::clone(&sem).acquire_owned().await.expect("semaphore open");
            let (url, wire) = (cfg.server_url.clone(), cfg.wire);
            let (on_connect, on_subscribe) = (cfg.on_connect.clone(), cfg.on_subscribe.clone());
            let recorder = Arc::new(Recorder::new(idx.to_string(), cfg.accumulator.clone(), cfg.ledger.clone()));
            tasks.push(rt().spawn(async move {
                let _permit = permit;
                let client = Client::build_with(&url, wire).map_err(|e| format!("client {idx}: build: {e}"))?;
                client.record_into(recorder);
                if let Err(e) = client.connect_async(None).await {
                    client.close_async().await;
                    return Err(format!("client {idx}: connect: {e}"));
                }
                if let Some(h) = on_connect {
                    h();
                }
                if let Err(e) = client.subscribe_async(&streams).await {
                    client.close_async().await;
                    return Err(format!("client {idx}: subscribe: {e}"));
                }
                if let Some(h) = on_subscribe {
                    h();
                }
                Ok(Entry { client: Arc::new(client), streams })
            }));
        }
        let mut entries = Vec::with_capacity(cfg.c);
        let mut failures = Vec::new();
        for t in tasks {
            match t.await {
                Ok(Ok(e)) => entries.push(e),
                Ok(Err(f)) => failures.push(f),
                Err(e) => failures.push(format!("setup task: {e}")),
            }
        }
        if failures.len() > cfg.tolerance {
            for e in &entries {
                e.client.close_async().await;
            }
            return Err(BenchError(format!(
                "pool setup failed: {} failure(s) exceed tolerance {}: [{}]",
                failures.len(),
                cfg.tolerance,
                failures.join(" ")
            )));
        }
        Ok(ClientPool { entries, closed: AtomicBool::new(false) })
    }

    /// Runs `f` for every client, in pool order.
    pub fn each(&self, f: &mut dyn FnMut(&Client)) {
        for e in &self.entries {
            f(&e.client);
        }
    }

    pub fn size(&self) -> usize {
        self.entries.len()
    }

    /// Each client's stream subset, in pool order.
    pub fn streams(&self) -> Vec<Vec<String>> {
        self.entries.iter().map(|e| e.streams.clone()).collect()
    }

    /// Closes every client (waiting for their readers). Idempotent.
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let clients: Vec<Arc<Client>> = self.entries.iter().map(|e| Arc::clone(&e.client)).collect();
        block_on(async move {
            let closing: Vec<_> =
                clients.into_iter().map(|c| rt().spawn(async move { c.close_async().await })).collect();
            for c in closing {
                let _ = c.await;
            }
        });
    }
}

impl Drop for ClientPool {
    fn drop(&mut self) {
        self.close();
    }
}

/// `c` seeded, uniformly random `s`-subsets (without replacement) of `"1"..="big_s"`.
/// The generator is Cabby's own: subsets match Go's only in distribution.
pub fn build_stream_subsets(seed: u64, c: usize, s: usize, big_s: usize) -> Vec<Vec<String>> {
    let mut rng = Rng::new(seed ^ 0x9E37_79B9_7F4A_7C15);
    (0..c)
        .map(|_| {
            if big_s == 0 || s == 0 {
                return Vec::new();
            }
            let s = s.min(big_s);
            let mut universe: Vec<usize> = (1..=big_s).collect();
            // Partial Fisher-Yates: only the first `s` positions are needed.
            for i in 0..s {
                let j = i + rng.below((big_s - i) as u64) as usize;
                universe.swap(i, j);
            }
            universe[..s].iter().map(usize::to_string).collect()
        })
        .collect()
}
