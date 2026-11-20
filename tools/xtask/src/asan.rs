//! `cargo xtask asan`: the mruby binding's tests under AddressSanitizer
//! (docs/mruby.md, "Checking memory safety").
//!
//! The safe API of `cabby-mruby-sys` must not let safe Rust cause undefined
//! behavior in the vendored C. Its regression tests (`tests/probes.rs`) assert
//! what they can, but a use-after-free often reads plausible data; under ASAN
//! it fails the run. The C code (mruby and the shim) is compiled with
//! `-fsanitize=address` and the Rust code with `-Zsanitizer=address`, which on
//! the pinned stable toolchain needs `RUSTC_BOOTSTRAP=1` (the sanitizer runtime
//! ships with the x86_64 Linux standard library). The build goes to
//! `target/asan` with an explicit `--target`, so build scripts are not
//! instrumented.

use std::process::Command;

use anyhow::{Result, bail};

use crate::reference::sh;
use crate::root;

const TARGET: &str = "x86_64-unknown-linux-gnu";

pub fn run() -> Result<()> {
    if !(cfg!(target_os = "linux") && cfg!(target_arch = "x86_64")) {
        bail!("`cargo xtask asan` runs on x86_64 Linux only ({TARGET})");
    }
    let target_dir = root().join("target/asan");
    // Leak checking stays off: mruby 1.2 does not free everything at exit,
    // and leaks are not what these tests are about.
    sh(Command::new("cargo")
        .args(["test", "-p", "cabby-mruby-sys", "--target", TARGET, "--target-dir"])
        .arg(&target_dir)
        .env("RUSTC_BOOTSTRAP", "1")
        .env("RUSTFLAGS", "-Zsanitizer=address")
        .env("CFLAGS", "-fsanitize=address -fno-omit-frame-pointer -g")
        .env("ASAN_OPTIONS", "detect_leaks=0:abort_on_error=1")
        .current_dir(root()))
}
