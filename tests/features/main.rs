//! Feature scenarios: ports of the upstream `features/*.testfile` scenarios
//! (run by `features/runner.rb` with wsdirector) and the correctness checks of
//! the `features/*.benchfile` workloads. Each `#[test]` follows one file; the
//! Ruby originals stay runnable through `compat/scripts/reference-conformance.sh`.
//!
//! Scenarios derived from AnyCable (https://github.com/anycable/anycable) at commit
//! ef995afbfa177e8e0b789cc651c7d11a550ccc70. Copyright 2016-2026 Vladimir
//! Dementyev. MIT License; see compat/licenses/anycable-MIT-LICENSE.

mod support;

mod bench;
mod enats;
mod file_config;
mod pusher;
mod rpc_less;
mod sqlite;
mod sse;
mod with_rpc;
