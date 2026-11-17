//! Test support for Cabby's translated upstream tests.
//!
//! * [`backend`]: which implementation a test exercises (`CABBY_BACKEND=rust|go`).
//! * [`bridge`]: client for the test-only Go reference bridge (JSON lines).
//! * [`reference`]: per-upstream-package facades that dispatch to Rust or Go. Each
//!   translated test calls these, so one test body verifies both implementations.
//! * [`process`]: supervised external processes (SUT binaries, fake services).
//! * [`ports`]: isolated local ports.
//! * [`sut`]: the server under test as an external process.
//! * [`fake_rpc`]: scriptable fake AnyCable RPC application (gRPC and HTTP RPC).
//! * [`fake_http`], [`udp`]: recording HTTP/UDP endpoints (telemetry, StatsD).
//! * [`rpc_doubles`]: client/service/helper doubles for the Rust RPC controller.
//! * [`runtime`], [`sync`]: shared Tokio runtime, observable state and gates.

pub mod backend;
pub mod bridge;
pub mod fake_http;
pub mod fake_rpc;
pub mod ports;
pub mod process;
pub mod reference;
pub mod rpc_doubles;
pub mod runtime;
pub mod sut;
pub mod sync;
pub mod udp;

pub use backend::{Backend, backend};

/// Workspace root (two levels above this crate).
pub fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().expect("workspace root")
}
