//! Translated tests for upstream Benchi `etc/benchi/lib` (package `lib` of module `github.com/anycable/anycable-go/etc/benchi`).
//!
//! Translated from AnyCable (https://github.com/anycable/anycable) at commit
//! ef995afbfa177e8e0b789cc651c7d11a550ccc70.
//! Copyright 2016-2026 Vladimir Dementyev. Licensed under the MIT License;
//! see compat/licenses/anycable-MIT-LICENSE.
//!
//! Each test names its upstream case with `/// upstream: <file>#<go test path>`
//! (Go's runtime subtest names). The Rust backend drives the Benchi port
//! (`cabby-bench`) against the server under test as an external process;
//! `CABBY_BACKEND=go` runs upstream Benchi through
//! `compat/reference-adapters/benchi-bridge` (see `cabby_testkit::reference::benchi`).

mod client_test;
mod counter_test;
mod lib_test;
mod pool_test;
mod publisher_test;
mod reconnecting_client_test;
mod rpc_server_test;
mod server_test;
mod throughput_test;
