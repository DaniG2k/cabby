//! Cabby server: transports, RPC, adapters, configuration.
//!
//! Status: interfaces only (SPEC.md §1 steps 1–3); behavior is `pending!`.

pub mod api;
pub mod broadcast;
pub mod broker;
pub mod cli;
mod client_tls;
pub mod config;
pub mod ds;
pub mod enats;
pub mod healthcheck;
mod http_client;
pub mod metrics;
pub mod mrb;
pub mod nats;
pub mod node;
pub mod pubsub;
pub mod pusher;
pub mod redis;
pub mod rpc;
pub mod server;
pub mod sse;
pub mod version;
pub mod ws;
