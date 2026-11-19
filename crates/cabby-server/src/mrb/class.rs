//! Classes, modules and Rust-defined methods (go-mruby `class.go`, `func.go`,
//! `args.go`).

use std::ops::BitOr;
use std::rc::Rc;

use cabby_mruby_sys as sys;

use super::{Exception, Mrb, MrbValue, Value, expect_ok};

/// A class or module (`*Class`).
#[derive(Debug, Clone)]
pub struct Class {
    pub(super) v: sys::Val,
}

/// Whether both handles denote the same class (Go compares `*Class` with
/// `reflect.DeepEqual`).
impl PartialEq for Class {
    fn eq(&self, other: &Self) -> bool {
        self.v.same(&other.v)
    }
}

/// Accepted arguments of a method (`ArgSpec`, mruby's `mrb_aspec`). Specs combine
/// with `|`, as in Go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArgSpec(u32);

impl ArgSpec {
    /// `ArgsAny` (`MRB_ARGS_REST()`).
    pub fn any() -> ArgSpec {
        ArgSpec(1 << 12)
    }

    /// `ArgsNone`.
    pub fn none() -> ArgSpec {
        ArgSpec(0)
    }

    /// `ArgsBlock`.
    pub fn block() -> ArgSpec {
        ArgSpec(1)
    }

    /// `ArgsReq(n)`.
    pub fn req(n: u32) -> ArgSpec {
        ArgSpec((n & 0x1f) << 18)
    }

    /// `ArgsOpt(n)`.
    pub fn opt(n: u32) -> ArgSpec {
        ArgSpec((n & 0x1f) << 13)
    }

    /// `ArgsArg(r, o)`.
    pub fn arg(required: u32, optional: u32) -> ArgSpec {
        ArgSpec::req(required) | ArgSpec::opt(optional)
    }
}

impl BitOr for ArgSpec {
    type Output = ArgSpec;

    fn bitor(self, rhs: ArgSpec) -> ArgSpec {
        ArgSpec(self.0 | rhs.0)
    }
}

/// A method implemented in Rust (go-mruby `Func`). It receives the VM and `self`.
/// Go returns `(result, exception)`; here `Ok(result)` (`None` is nil) or
/// `Err(exception)`, which is raised in Ruby.
pub trait Func: Fn(&Mrb, &MrbValue) -> Result<Option<Value>, Value> + 'static {}

impl<F: Fn(&Mrb, &MrbValue) -> Result<Option<Value>, Value> + 'static> Func for F {}

/// Adapts a [`Func`] to the binding's method type (go-mruby `goMRBFuncCall`):
/// nil for `None`, the exception value converted in the calling VM.
fn method(f: impl Func) -> sys::Method {
    Rc::new(move |vm: &sys::Vm, this: &sys::Val| {
        let m = Mrb { vm: vm.clone() };
        let this = MrbValue { v: this.clone() };
        match f(&m, &this) {
            Ok(None) => Ok(None),
            Ok(Some(v)) => Ok(Some(v.mrb_value(&m).v)),
            Err(e) => Err(e.mrb_value(&m).v),
        }
    })
}

impl Class {
    fn vm(&self) -> sys::Vm {
        self.v.vm()
    }

    /// Defines a class method (`DefineClassMethod`).
    pub fn define_class_method(&self, name: &str, f: impl Func, spec: ArgSpec) {
        expect_ok(self.vm().define_method(&self.v, name, spec.0, true, method(f)), "DefineClassMethod");
    }

    /// Defines an instance method (`DefineMethod`).
    pub fn define_method(&self, name: &str, f: impl Func, spec: ArgSpec) {
        expect_ok(self.vm().define_method(&self.v, name, spec.0, false, method(f)), "DefineMethod");
    }

    pub fn define_const(&self, name: &str, value: &Value) {
        let vm = self.vm();
        let value = value.mrb_value(&Mrb { vm: vm.clone() });
        expect_ok(vm.define_const(&self.v, name, &value.v), "DefineConst");
    }

    /// The class as a value (`MrbValue`).
    pub fn mrb_value(&self, _m: &Mrb) -> MrbValue {
        MrbValue { v: self.v.clone() }
    }

    /// Instantiates the class (`New`); an exception from `initialize` is
    /// returned and cleared.
    pub fn new_instance(&self, args: &[Value]) -> Result<MrbValue, Exception> {
        let m = Mrb { vm: self.vm() };
        let args: Vec<_> = args.iter().map(|a| a.mrb_value(&m).v).collect();
        self.v.obj_new(&args).map(|v| MrbValue { v }).map_err(Exception::from_raised)
    }
}
