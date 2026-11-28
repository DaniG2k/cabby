//! Translated tests for upstream Go package `node`.
//!
//! Translated from AnyCable (https://github.com/anycable/anycable) at commit
//! ef995afbfa177e8e0b789cc651c7d11a550ccc70.
//! Copyright 2016-2026 Vladimir Dementyev. Licensed under the MIT License;
//! see compat/licenses/anycable-MIT-LICENSE.
//!
//! Each test names its upstream case with `/// upstream: <file>#<go test path>`
//! (Go's runtime subtest names). Run against Go with `CABBY_BACKEND=go`.
//!
//! Upstream subtests share the state their parent test sets up and run in order.
//! A Rust test per subtest re-runs the parent's setup and the earlier subtests'
//! steps it depends on (`*_steps` helpers), so every case is reported on its own
//! and the parent case maps to all of its subtests.
//!
//! Most cases reach into unexported node state; against Go they run up to the
//! first such step and are classified `not_observable` (see
//! `cabby_testkit::reference::node`).

mod broker_integration_test;
mod config_test;
mod disconnect_queue_test;
mod node_test;
mod session_test;
mod subscription_state_test;
mod support;
