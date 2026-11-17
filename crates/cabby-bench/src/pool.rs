//! A pool of connected, subscribed clients (upstream `lib/pool.go`).

use std::sync::Arc;

use cabby_core::pending;

use crate::BenchError;
use crate::client::Client;
use crate::counter::Accumulator;

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
    /// Overrides subset generation; its length must equal `c`.
    pub streams: Option<Vec<Vec<String>>>,
    pub on_connect: Option<PoolHook>,
    pub on_subscribe: Option<PoolHook>,
}

/// Connected clients plus their drain threads. Client ids are the decimal indexes
/// `"0"..` in pool order.
pub struct ClientPool {
    _entries: (),
}

impl ClientPool {
    /// Builds the pool with bounded setup parallelism. Over-tolerance failures close
    /// every client and return `pool setup failed: N failure(s) exceed tolerance T:
    /// [client <i>: <phase>: <err> ...]` (phase: build, connect, subscribe).
    pub fn build(_cfg: PoolConfig) -> Result<ClientPool, BenchError> {
        pending!("benchi::build_pool")
    }

    /// Runs `f` for every client, in pool order.
    pub fn each(&self, _f: &mut dyn FnMut(&Client)) {
        pending!("benchi::pool_each")
    }

    pub fn size(&self) -> usize {
        pending!("benchi::pool_size")
    }

    /// Each client's stream subset, in pool order.
    pub fn streams(&self) -> Vec<Vec<String>> {
        pending!("benchi::pool_streams")
    }

    /// Closes every client and joins the drain threads. Idempotent.
    pub fn close(&self) {
        pending!("benchi::pool_close")
    }
}

/// `c` seeded, uniformly random `s`-subsets (without replacement) of `"1"..="big_s"`.
/// The generator is Cabby's own: subsets match Go's only in distribution.
pub fn build_stream_subsets(_seed: u64, _c: usize, _s: usize, _big_s: usize) -> Vec<Vec<String>> {
    pending!("benchi::build_stream_subsets")
}
