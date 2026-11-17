//! Facades over the implementation under test, one module per upstream package.
//!
//! Each function runs the Rust implementation or calls the Go reference through
//! the bridge, converting Go's JSON shapes into Cabby types. Facades only marshal;
//! they must not contain behavior of their own (SPEC.md §5).

pub mod api;
pub mod benchi;
pub mod broadcast;
pub mod broker;
pub mod cli;
pub mod common;
pub mod config;
pub mod enats;
pub mod encoders;
mod go_types;
pub mod hub;
pub mod identity;
pub mod logger;
pub mod metrics;
pub mod mrb;
pub mod mruby;
mod msg_shapes;
pub mod nats;
pub mod protocol;
pub mod pubsub;
pub mod pusher;
pub mod redis;
pub mod router;
pub mod rpc;
pub mod server;
pub mod sse;
pub mod streams;
pub mod telemetry;
pub mod utils;
pub mod ws;
