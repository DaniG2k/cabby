//! Embedded mruby scripting (upstream package `mrb` and the vendored go-mruby
//! binding `vendorlib/go-mruby`, Go package `mruby`).
//!
//! Upstream links mruby 1.2.0 (`etc/build_config.rb`) through cgo and uses it for
//! custom metrics printers (`metrics/custom_printer.go`). Cabby links the same
//! mruby, vendored and compiled by `cabby-mruby-sys`, whose safe layer is the
//! only `unsafe` code involved; this module rebuilds go-mruby's API and
//! behavior on top of it without `unsafe`. See `docs/mruby.md`.
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
//!
//! Where go-mruby reads freed or unchecked memory (a collected value, a closed
//! VM, a value of the wrong type), Cabby panics or returns an error instead;
//! those cases are noted on the methods.

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

/// The result of an mruby call that go-mruby does not expect to raise (it
/// would abort or crash); here a raise panics with the exception message.
fn expect_ok<T>(r: Result<T, cabby_mruby_sys::Raised>, what: &str) -> T {
    r.unwrap_or_else(|e| panic!("mruby {what} raised: {}", e.message))
}

#[cfg(test)]
mod tests {
    //! Behavior where go-mruby reads freed or unchecked memory, aborts or
    //! crashes (deviation D-032 in compat/deviations.md).

    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;

    fn panics(f: impl FnOnce()) -> bool {
        catch_unwind(AssertUnwindSafe(f)).is_err()
    }

    #[test]
    fn deviation_d032_values_of_a_closed_vm_are_dead_and_unusable() {
        let mrb = Mrb::new();
        let v = mrb.load_string("'text'").expect("runs");
        mrb.clone().close();
        assert!(v.is_dead());
        assert!(panics(|| {
            let _ = v.call("upcase", &[]);
        }));
        assert!(panics(|| {
            let _ = mrb.load_string("1");
        }));
    }

    #[test]
    fn deviation_d032_collected_values_are_not_passed_to_mruby() {
        let mrb = Mrb::new();
        let ai = mrb.arena_save();
        let v = mrb.string_value("foo");
        mrb.arena_restore(ai);
        mrb.full_gc();
        assert!(v.is_dead());
        assert!(panics(|| {
            let _ = v.call("upcase", &[]);
        }));
        mrb.close();
    }

    #[test]
    fn deviation_d032_unchecked_go_mruby_calls_fail_cleanly() {
        let mrb = Mrb::new();
        // A Rust method may only raise exception objects.
        let class = mrb.define_class("Hello", None);
        class.define_class_method("bad", |_m: &Mrb, _s: &MrbValue| Err(Value::String("no".into())), ArgSpec::none());
        let e = mrb.load_string("Hello.bad").expect_err("raises");
        assert_eq!(e.message, "exception object expected");
        // Array and Hash operations check the type.
        let n = mrb.fixnum_value(1);
        assert_eq!(n.array().len(), 0);
        assert!(n.array().get(0).is_err());
        assert!(n.hash().get(&Value::Int(1)).is_err());
        // Run, Yield and CallBlock need procs.
        assert!(mrb.run(&Value::Int(1), None).is_err());
        assert!(mrb.yield_block(&Value::Nil, &[]).is_err());
        assert!(mrb.string_value("x").call_block("each_char", &[], &Value::Int(1)).is_err());
        // Missing classes and modules.
        assert!(mrb.class("Missing", None).is_none());
        assert!(mrb.module("Object").is_none());
        // Arguments outside a Rust method; an arena index above the current one.
        assert!(mrb.get_args().is_empty());
        mrb.arena_restore(ArenaIndex(i32::MAX));
        assert_eq!(mrb.load_string("1 + 1").expect("VM still usable").fixnum(), 2);
        mrb.close();
    }

    #[test]
    fn deviation_d032_no_code_from_a_failed_parse() {
        let mrb = Mrb::new();
        let mut p = Parser::new(&mrb);
        assert!(p.parse("def foo", None).is_err());
        assert_eq!(p.generate_code().value_type(), ValueType::Nil);
        p.close();
        mrb.close();
    }

    #[test]
    fn version_is_mruby_1_2_0() {
        assert!(supported());
        assert_eq!(version().expect("version"), "mruby 1.2.0 (2015-11-17)");
    }

    #[test]
    fn load_file_errors_read_like_go() {
        let e = Engine::new().load_file(std::path::Path::new("/nonexistent/cabby.rb")).expect_err("missing file");
        assert_eq!(e.to_string(), "open /nonexistent/cabby.rb: no such file or directory");
    }
}
