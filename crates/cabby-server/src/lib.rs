//! Cabby server: transports, RPC, adapters, configuration.
//!
//! Status: interfaces only (SPEC.md §1 steps 1–3); behavior is `pending!`.

pub mod api;
pub mod cli;
pub mod config;
pub mod metrics;
pub mod mrb;
pub mod pusher;
pub mod pusher_sse_deps;
pub mod rpc;
pub mod server;
pub mod sse;
pub mod telemetry;
pub mod ws;
