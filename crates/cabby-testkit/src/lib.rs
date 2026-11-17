//! Test support for Cabby's translated upstream tests.
//!
//! * [`backend`]: which implementation a test exercises (`CABBY_BACKEND=rust|go`).
//! * [`bridge`]: client for the test-only Go reference bridge (JSON lines).
//! * [`benchi_bridge`]: client for the Benchi Go bridge (handles, concurrent calls).
//! * [`http_stub`]: HTTP stub server, POST client and a stalled WebSocket endpoint.
//! * [`mruby_bridge`]: client for the go-mruby reference bridge (cgo, callbacks).
//! * [`cable_client`]: synchronous Action Cable WebSocket client (AnyT semantics).
//! * [`reference`]: per-upstream-package facades that dispatch to Rust or Go. Each
//!   translated test calls these, so one test body verifies both implementations.
//! * [`process`]: supervised external processes (SUT binaries, fake services).
//! * [`ports`]: isolated local ports.
//! * [`services`]: required external Redis/NATS services with per-test scopes.
//! * [`sut`]: the server under test as an external process.
//! * [`fake_rpc`]: scriptable fake AnyCable RPC application (gRPC and HTTP RPC).
//! * [`fake_http`], [`udp`]: recording HTTP/UDP endpoints (telemetry, StatsD).
//! * [`rpc_doubles`]: client/service/helper doubles for the Rust RPC controller.
//! * [`runtime`], [`sync`]: shared Tokio runtime, observable state and gates.

pub mod backend;
pub mod benchi_bridge;
pub mod bridge;
pub mod cable_client;
pub mod fake_http;
pub mod fake_rpc;
pub mod fault_proxy;
pub mod http_stub;
pub mod mruby_bridge;
pub mod ports;
pub mod process;
pub mod reference;
pub mod rpc_doubles;
pub mod runtime;
pub mod services;
pub mod sut;
pub mod sync;
pub mod timers;
pub mod udp;

pub use backend::{Backend, backend};

/// Workspace root (two levels above this crate).
pub fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().expect("workspace root")
}
pub mod clock;
pub mod jwt;
pub mod nats;

// added for pusher/sse
pub mod pusher_auth;
pub mod raw_http;
pub mod text_ws;

// added for the AnyT / feature scenario ports
pub mod anyt_app;
pub mod publish;
pub mod wsdirector;

// added for ds (durable streams)
pub mod ds_client;
