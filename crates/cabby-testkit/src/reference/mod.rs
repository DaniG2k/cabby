//! Facades over the implementation under test, one module per upstream package.
//!
//! Each function runs the Rust implementation or calls the Go reference through
//! the bridge, converting Go's JSON shapes into Cabby types. Facades only marshal;
//! they must not contain behavior of their own (SPEC.md §5).

pub mod api;
pub mod broker;
pub mod cli;
pub mod common;
pub mod config;
pub mod encoders;
mod go_types;
pub mod identity;
pub mod logger;
pub mod metrics;
pub mod protocol;
pub mod router;
pub mod rpc;
pub mod server;
pub mod streams;
pub mod telemetry;
pub mod utils;
pub mod ws;
