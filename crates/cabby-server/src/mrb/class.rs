//! Classes, modules and Rust-defined methods (go-mruby `class.go`, `func.go`,
//! `args.go`).

use std::ops::BitOr;

use cabby_core::pending;

use super::{Exception, Mrb, MrbValue, NotSend, Value};

/// A class or module (`*Class`).
#[derive(Debug, Clone)]
pub struct Class {
    _c: NotSend,
}

/// Whether both handles denote the same class (Go compares `*Class` with
/// `reflect.DeepEqual`).
impl PartialEq for Class {
    fn eq(&self, _other: &Self) -> bool {
        pending!("mrb::Class::eq")
    }
}

/// Accepted arguments of a method (`ArgSpec`, mruby's `mrb_aspec`). Specs combine
/// with `|`, as in Go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArgSpec(u32);

impl ArgSpec {
    /// `ArgsAny`.
    pub fn any() -> ArgSpec {
        pending!("mrb::ArgSpec::any")
    }

    /// `ArgsNone`.
    pub fn none() -> ArgSpec {
        pending!("mrb::ArgSpec::none")
    }

    /// `ArgsBlock`.
    pub fn block() -> ArgSpec {
        pending!("mrb::ArgSpec::block")
    }

    /// `ArgsReq(n)`.
    pub fn req(_n: u32) -> ArgSpec {
        pending!("mrb::ArgSpec::req")
    }

    /// `ArgsOpt(n)`.
    pub fn opt(_n: u32) -> ArgSpec {
        pending!("mrb::ArgSpec::opt")
    }

    /// `ArgsArg(r, o)`.
    pub fn arg(_required: u32, _optional: u32) -> ArgSpec {
        pending!("mrb::ArgSpec::arg")
    }
}

impl BitOr for ArgSpec {
    type Output = ArgSpec;

    fn bitor(self, _rhs: ArgSpec) -> ArgSpec {
        pending!("mrb::ArgSpec::bitor")
    }
}

/// A method implemented in Rust (go-mruby `Func`). It receives the VM and `self`.
/// Go returns `(result, exception)`; here `Ok(result)` (`None` is nil) or
/// `Err(exception)`, which is raised in Ruby.
pub trait Func: Fn(&Mrb, &MrbValue) -> Result<Option<Value>, Value> + 'static {}

impl<F: Fn(&Mrb, &MrbValue) -> Result<Option<Value>, Value> + 'static> Func for F {}

impl Class {
    /// Defines a class method (`DefineClassMethod`).
    pub fn define_class_method(&self, _name: &str, _f: impl Func, _spec: ArgSpec) {
        pending!("mrb::Class::define_class_method")
    }

    /// Defines an instance method (`DefineMethod`).
    pub fn define_method(&self, _name: &str, _f: impl Func, _spec: ArgSpec) {
        pending!("mrb::Class::define_method")
    }

    pub fn define_const(&self, _name: &str, _value: &Value) {
        pending!("mrb::Class::define_const")
    }

    /// The class as a value (`MrbValue`).
    pub fn mrb_value(&self, _m: &Mrb) -> MrbValue {
        pending!("mrb::Class::mrb_value")
    }

    /// Instantiates the class (`New`); an exception from `initialize` is
    /// returned and cleared.
    pub fn new_instance(&self, _args: &[Value]) -> Result<MrbValue, Exception> {
        pending!("mrb::Class::new_instance")
    }
}
