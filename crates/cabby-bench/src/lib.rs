//! Cabby's port of Benchi (`etc/benchi` upstream): a load harness for
//! AnyCable-compatible servers.
//!
//! Status: interfaces only (SPEC.md §1 steps 1–3). Every behavior is a
//! [`cabby_core::pending!`] marker until its translated tests are promoted.
//!
//! # Architecture: an external harness
//!
//! Upstream Benchi embeds the Go server (`cli.Embedded`) in the scenario process, so
//! clients, publisher and server share one Go scheduler and heap. That is not a fair
//! basis for comparing two servers (SPEC.md §10). This port keeps Benchi's workloads
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
//!   checks SPEC.md §10 asks for (sequence-numbered payloads for ordering and
//!   duplicates), reconnect success ([`scenarios::reconnect_floods`]) and residual
//!   resources after a run ([`resources`]).
//! * Go runtime measurements (goroutines, Go heap) become process measurements of the
//!   server under test, sampled from `/proc/<pid>` ([`resources`]). Internal gauges the
//!   embedded server exposed in-process (disconnect queue size, RPC counters) are read
//!   from the server's metrics endpoint.
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
pub mod histogram;
pub mod ledger;
pub mod pool;
pub mod publisher;
pub mod reconnecting_client;
pub mod resources;
pub mod rpc_server;
pub mod scenarios;
pub mod server;
pub mod throughput;

/// An error reported by the harness. Upstream returns `error` values whose text the
/// tests inspect (`client 2`, `subscribe`, the offending stream name), so the message
/// is the contract.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct BenchError(pub String);
