//! Vendored mruby 1.2.0 and the memory-safety boundary of Cabby's mruby
//! binding.
//!
//! This crate is the only place in the workspace with `unsafe` code. It has two
//! layers:
//!
//! - [`ffi`]: raw declarations of the C shim (`shim/cabby_mruby.c`). The shim is
//!   the only code that calls mruby. It wraps every call that can raise in a
//!   jump buffer of its own, so an mruby exception (a `longjmp`) never crosses a
//!   Rust frame, and it checks value types before mruby dereferences them.
//! - the safe API ([`Vm`], [`Val`], [`Parser`], [`Context`]): handles that keep
//!   the shim's preconditions. A value is only passed to C after checking that
//!   it belongs to the same open VM and is a live object of its recorded type
//!   (values can be collected by mruby's GC, as in go-mruby); a closed VM is
//!   never touched; Rust methods called from Ruby run under `catch_unwind`, and
//!   a panic is re-raised in Rust once control is back in Rust.
//!
//! `cabby_server::mrb` builds the go-mruby-compatible API on top of the safe
//! layer without any `unsafe`. Scripts are trusted operator input: mruby 1.2.0
//! has known memory-safety bugs in the interpreter itself, which no binding can
//! fix (docs/mruby.md).

mod safe;

pub mod ffi;

pub use safe::{Context, Method, Parser, ParserMessage, Raised, Val, Vm};

/// Whether mruby is compiled in (cargo feature `vendored` on Linux or macOS).
pub const fn available() -> bool {
    cfg!(cabby_mruby)
}

/// `MRUBY_DESCRIPTION`, e.g. `mruby 1.2.0 (2015-11-17) `; `None` when mruby is
/// not compiled in.
pub fn description() -> Option<&'static str> {
    if !available() {
        return None;
    }
    // SAFETY: the shim returns a pointer to a static NUL-terminated literal.
    let ptr = unsafe { ffi::cabby_description() };
    // SAFETY: `ptr` is a valid, NUL-terminated, 'static C string (see above).
    let text = unsafe { std::ffi::CStr::from_ptr(ptr) };
    text.to_str().ok()
}
