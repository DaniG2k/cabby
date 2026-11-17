//! Cabby server: transports, RPC, adapters, configuration.
//!
//! Status: interfaces only (SPEC.md §1 steps 1–3); behavior is `pending!`.

pub mod metrics;
pub mod rpc;
pub mod server;
pub mod telemetry;
pub mod ws;
