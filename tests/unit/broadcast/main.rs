//! Translated tests for upstream Go package `broadcast`.
//!
//! Translated from AnyCable (https://github.com/anycable/anycable) at commit
//! 26a1a703a54b870e42f5660c3d0785db2c602764.
//! Copyright 2016-2026 Vladimir Dementyev. Licensed under the MIT License;
//! see compat/licenses/anycable-MIT-LICENSE.
//!
//! Each test names its upstream case with `/// upstream: <file>#<go test path>`
//! (Go's runtime subtest names). Run against Go with `CABBY_BACKEND=go`.

mod http_test;
mod legacy_nats_test;
mod legacy_redis_test;
mod postgres_test;
mod redis_test;
