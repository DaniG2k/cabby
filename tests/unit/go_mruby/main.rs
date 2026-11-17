//! Translated tests for the vendored Go module `github.com/mitchellh/go-mruby`
//! (package `mruby`, at `vendorlib/go-mruby`).
//!
//! Translated from AnyCable (https://github.com/anycable/anycable) at commit
//! 26a1a703a54b870e42f5660c3d0785db2c602764.
//! Copyright 2016-2026 Vladimir Dementyev. Licensed under the MIT License;
//! see compat/licenses/anycable-MIT-LICENSE.
//!
//! The vendored go-mruby tests are Copyright (c) 2017 Mitchell Hashimoto,
//! licensed under the MIT License; see compat/licenses/go-mruby-LICENSE.
//!
//! Each test names its upstream case with `/// upstream: <file>#<go test path>`
//! (Go's runtime test names). Run against Go with `CABBY_BACKEND=go`, which uses
//! the cgo mruby bridge (`compat/reference-adapters/mruby-bridge`).
//!
//! Upstream closes every VM with `defer mrb.Close()`; the translations call
//! `close()` at the end, so a failing test leaves its VM open (harmless: the
//! test process or the bridge exits). Assertions that depend on Go mechanics carry a `Runtime adaptation:` comment
//! naming the Rust invariant that replaces them (see docs/mruby.md).

mod array_test;
mod class_test;
mod context_test;
mod decode_test;
mod examples_test;
mod func_test;
mod gc_test;
mod hash_test;
mod mruby_test;
mod parser_test;
mod value_test;

/// Joins a thread, re-raising its panic message on the test thread so pending
/// markers and assertion messages from worker threads reach the test output.
pub(crate) fn join<T>(handle: std::thread::JoinHandle<T>) -> T {
    match handle.join() {
        Ok(v) => v,
        Err(payload) => {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                .unwrap_or_else(|| "worker thread panicked".to_owned());
            panic!("{msg}")
        }
    }
}
