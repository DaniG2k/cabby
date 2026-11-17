//! Embedded mruby scripting (upstream package `mrb` and the vendored go-mruby
//! binding `vendorlib/go-mruby`, Go package `mruby`).
//!
//! Upstream links mruby 1.2.0 (`etc/build_config.rb`) through cgo and uses it for
//! custom metrics printers (`metrics/custom_printer.go`). This module is the Rust
//! interface Cabby needs for the same capabilities; see `docs/mruby.md` for the
//! embedding strategy. Status: interfaces only (SPEC.md §1 steps 1–3); every body
//! is `pending!`.
//!
//! Mapping from go-mruby:
//!
//! | go-mruby | Rust |
//! |---|---|
//! | `*Mrb` | [`Mrb`] (a cloneable handle to one VM; `close` frees it) |
//! | `*MrbValue` | [`MrbValue`] |
//! | `Value` interface (`Int`, `String`, `Nil`, `*MrbValue`, `*Class`) | [`Value`] |
//! | `*Class` | [`Class`] |
//! | `Func` returning `(Value, Value)` | closure returning `Result<Option<Value>, Value>` |
//! | `*Exception` (an `error`) | [`Exception`] |
//! | `*Array`, `*Hash` | [`Array`], [`Hash`] |
//! | `*Parser`, `*CompileContext`, `ParserError` | [`Parser`], [`CompileContext`], [`ParserError`] |
//! | `Decode(out interface{}, v)` (reflection) | [`decode`] (serde) and [`decode_value`] |
//! | `mrb.Engine` | [`Engine`] |
//!
//! None of these types is `Send`: an mruby VM and everything that points into it
//! belong to one thread. Independent VMs on different threads are independent.

mod class;
mod decode;
mod engine;
mod parser;
mod value;
mod vm;

pub use class::{ArgSpec, Class, Func};
pub use decode::{DecodeError, Decoded, decode, decode_value};
pub use engine::{Engine, MrbError, default_engine, supported, version};
pub use parser::{CompileContext, Parser, ParserError, ParserMessage};
pub use value::{Array, Exception, Hash, MrbValue, Value, ValueType};
pub use vm::{ArenaIndex, Mrb};

/// Marker that keeps VM-bound handles `!Send` and `!Sync`.
type NotSend = std::marker::PhantomData<*const ()>;
