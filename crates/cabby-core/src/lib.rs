//! Core of Cabby, an AnyCable-compatible server.
//!
//! Status: interfaces and generated bindings only (SPEC.md §1 steps 1–3).
//! Every behavior is a [`pending!`] marker until its translated tests are promoted.

pub mod broadcast;
pub mod broker;
pub mod common;
pub mod encoders;
pub mod gojson;
pub mod hub;
pub mod identity;
pub mod logger;
pub mod nats;
pub mod pending;
pub mod protocol;
pub mod protos;
pub mod pubsub;
pub mod router;
pub mod streams;
pub mod toml_config;
pub mod utils;
