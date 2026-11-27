//! Cabby's port of Benchi (`etc/benchi` upstream): a load harness for
//! AnyCable-compatible servers.
//!
//! # Architecture: an external harness
//!
//! Upstream Benchi embeds the Go server (`cli.Embedded`) in the scenario process, so
//! clients, publisher and server share one Go scheduler and heap. That is not a fair
//! basis for comparing two servers (`docs/benchmarks.md`, "Method"). This port keeps Benchi's workloads
//! and correctness checks but moves the server out of the measured process:
//!
//! * [`server::Server`] launches **any** server binary (Cabby or the pinned
//!   `anycable-go`) as a child process with the same AnyCable CLI flags, isolated
//!   ports and a captured log, and talks to it only over the network: WebSocket,
//!   the HTTP broadcast endpoint and the Prometheus metrics endpoint.
//! * The load generators ([`client::Client`], [`pool::ClientPool`],
//!   [`publisher::Publisher`], [`reconnecting_client::ReconnectingClient`]) and the
//!   fake HTTP RPC application ([`rpc_server::RpcServer`]) run in the harness
//!   process. RPC latency is injected harness-side, identically for every server.
//! * Correctness checks are network-observable: per-client message counts reconciled
//!   against what the publisher issued ([`counter`], [`ledger`]), plus the stronger
//!   checks `docs/benchmarks.md` ("Method") asks for (sequence-numbered payloads for ordering and
//!   duplicates), reconnect success ([`scenarios::reconnect_floods`]) and residual
//!   resources after a run ([`resources`]).
//! * Go runtime measurements (goroutines, Go heap) become process measurements of the
//!   server under test, sampled from `/proc/<pid>` ([`resources`]). Internal gauges the
//!   embedded server exposed in-process (disconnect queue size, RPC counters) are read
//!   from the server's metrics endpoint.
//!
//! * The generator is asynchronous: every client, publisher worker and RPC handler
//!   is a task on one multi-threaded Tokio runtime ([`runtime`]), so thousands of
//!   connections cost no threads, and the hot paths (receive counters, latency
//!   histograms, per-client ledgers) are lock-free or use uncontended per-client
//!   locks.
//!
//! # Accuracy safeguards
//!
//! * **Monotonic clock** for every timing: schedules, latencies, sampling
//!   (throughput samples carry wall-clock values derived from it).
//! * **Delivery reconciled by id**, not by accepted publications: each broadcast is
//!   `<stream>:<seq>`, and [`ledger::DeliveryLedger`] reports missing ids,
//!   duplicates, per-publisher-worker ordering violations and misrouted messages,
//!   next to upstream's count reconciliation.
//! * **End-to-end latency**: the publisher stamps each POST body with its dispatch
//!   time; receivers record dispatch → delivery.
//! * **Generator saturation is reported** (`generator_saturated`, with the evidence:
//!   late schedule ticks, runtime timer lag, harness CPU use), so a run that measured
//!   the generator instead of the server is visible in its output.
//! * **Server-side truth** from the server process: peak RSS/threads/sockets, CPU
//!   time over the measured window, residual sockets/fds after the clients leave,
//!   and gauges from its metrics endpoint (a stale gauge never ends a wait early,
//!   see [`scenarios::reconnect_floods::wave_drained`]).
//!
//! # Comparing servers
//!
//! Build both servers in release mode, then run the same scenario against each
//! (`cabby-bench <scenario> --help` lists the flags):
//!
//! ```text
//! cargo build --release -p cabby-server -p cabby-bench
//! cargo xtask reference-build   # target/reference-bin/anycable-go
//! for sut in target/release/cabby target/reference-bin/anycable-go; do
//!   target/release/cabby-bench stress_publications --sut "$sut" \
//!     -c 2000 -r 2000 -d 30s -S 100 -s 5 --non-interactive
//! done
//! ```
//!
//! Rails' Action Cable runs the same way through `bench/actioncable/sut`, which takes the
//! AnyCable flags and execs Puma. It has no metrics endpoint and no RPC; see
//! [`server`] and [`scenarios::reconnect_floods`] for how the scenarios handle that, and
//! `compat/scripts/bench-compare.sh` for the three-way comparison.
//!
//! `--protocol=pusher` runs both scenarios over the Pusher protocol instead ([`pusher`]):
//! Cabby and `anycable-go` in Pusher mode, Soketi and Laravel Reverb through
//! `bench/soketi/sut` and `bench/reverb/sut`; `compat/scripts/bench-compare-pusher.sh` is
//! that four-way comparison (`bench/pusher/README.md`).
//!
//! `cabby-bench soak` is a memory-stability run (AnyCable issue 265, deviation D-005):
//! RPC-mode connection and subscription churn with few broadcasts for hours, sampling the
//! server's RSS, threads, fds and sockets ([`scenarios::soak`]);
//! `compat/scripts/soak-compare.sh` runs it against AnyCable and Cabby side by side.
//!
//! `--server-arg=<flag>` passes extra flags to the server (`--server-arg=--broker=memory`).
//! `CABBY_BENCH_THREADS` sets the generator's worker threads; on one host, pin the
//! generator and the server to separate cores (`taskset`) and keep runs whose
//! summary says `generator_saturated=true` out of the comparison.
//!
//! Module map (upstream file in parentheses): [`cable`] (`client/client.go`),
//! [`client`] (`lib/client.go`), [`counter`] (`lib/counter.go`), [`histogram`]
//! (`lib/histogram.go`), [`pool`] (`lib/pool.go`), [`publisher`] (`lib/publisher.go`),
//! [`reconnecting_client`] (`lib/reconnecting_client.go`), [`rpc_server`]
//! (`lib/rpc_server.go`), [`server`] (`lib/server.go`), [`throughput`]
//! (`lib/throughput.go`), [`scenarios`] (`scenarios/*/main.go`).

pub mod cable;
pub mod cancel;
pub mod client;
pub mod counter;
mod gojson;
pub mod histogram;
pub mod ledger;
mod net;
pub mod pool;
pub mod publisher;
pub mod pusher;
pub mod reconnecting_client;
pub mod resources;
mod rng;
pub mod rpc_server;
pub mod runtime;
pub mod scenarios;
pub mod server;
pub mod server_env;
pub mod throughput;

/// An error reported by the harness. Upstream returns `error` values whose text the
/// tests inspect (`client 2`, `subscribe`, the offending stream name), so the message
/// is the contract.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct BenchError(pub String);
