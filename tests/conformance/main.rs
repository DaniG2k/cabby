//! External-behavior scenarios run against a server binary (`SUT_BIN`, or chosen
//! by `CABBY_BACKEND`). They complement translated unit tests by checking the
//! same contracts through the network interface.
//!
//! Scenarios derived from AnyCable (https://github.com/anycable/anycable) at commit
//! 26a1a703a54b870e42f5660c3d0785db2c602764. Copyright 2016-2026 Vladimir
//! Dementyev. MIT License; see compat/licenses/anycable-MIT-LICENSE.
//!
//! The `anyt_*` modules port the AnyT conformance scenarios of anyt-core 1.4.0
//! (https://github.com/anycable/anyt), Copyright 2017-2026 Vladimir Dementyev,
//! MIT License; see compat/licenses/anyt-core-MIT-LICENSE.

/// One `#[test]` per scenario for a conformance variant:
/// `/// upstream: <id>` / `#[test]` / `fn name() = path::to::scenario;`.
macro_rules! anyt_tests {
    ($variant:expr; $( $(#[$attr:meta])* fn $name:ident() = $body:path; )*) => {
        $( $(#[$attr])* fn $name() { $body($variant) } )*
    };
}

mod api;
mod brokers;
mod client_messages;
mod health;
mod issues;
mod origin;
mod pusher;
mod rpc_sessions;
mod sse;
mod threads;

mod anyt_core;
mod anyt_ext;
mod anyt_support;

mod anyt_broker_http;
mod anyt_broker_nats;
mod anyt_broker_redis;
mod anyt_default;
mod anyt_embedded;
mod anyt_http;
mod anyt_nats;
mod anyt_nats_embedded;
mod anyt_ssl;
