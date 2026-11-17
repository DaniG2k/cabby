//! Translated tests for upstream Go package `hub`.
//!
//! Translated from AnyCable (https://github.com/anycable/anycable) at commit
//! 26a1a703a54b870e42f5660c3d0785db2c602764.
//! Copyright 2016-2026 Vladimir Dementyev. Licensed under the MIT License;
//! see compat/licenses/anycable-MIT-LICENSE.
//!
//! Each test names its upstream case with `/// upstream: <file>#<go test path>`
//! (Go's runtime subtest names). Run against Go with `CABBY_BACKEND=go`.

mod gate_test;
mod hub_test;

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// On the Go backend all tests of this binary share one bridge process with
/// serialized calls. The benchmark configurations hold it for seconds and load
/// its Go runtime, which would distort the 100 ms read windows of the other
/// tests, so they run exclusively (write lock); every other test takes the read
/// lock.
static BRIDGE_LOAD: RwLock<()> = RwLock::new(());

pub(crate) fn shared_load() -> RwLockReadGuard<'static, ()> {
    BRIDGE_LOAD.read().unwrap_or_else(|e| e.into_inner())
}

pub(crate) fn exclusive_load() -> RwLockWriteGuard<'static, ()> {
    BRIDGE_LOAD.write().unwrap_or_else(|e| e.into_inner())
}
