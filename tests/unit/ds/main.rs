//! Translated tests for upstream Go package `ds` (durable streams).
//!
//! Translated from AnyCable (https://github.com/anycable/anycable) at commit
//! 26a1a703a54b870e42f5660c3d0785db2c602764.
//! Copyright 2016-2026 Vladimir Dementyev. Licensed under the MIT License;
//! see compat/licenses/anycable-MIT-LICENSE.
//!
//! Each test names its upstream case with `/// upstream: <file>#<go test path>`
//! (Go's runtime subtest names). Run against Go with `CABBY_BACKEND=go`.

mod auth_test;
mod config_test;
mod ds_test;
mod encoder_test;
mod handler_test;
mod integration_test;
mod poll_test;
mod sse_test;
