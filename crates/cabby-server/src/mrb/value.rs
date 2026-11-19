//! mruby values (go-mruby `value.go`, `value_type.go`, `array.go`, `hash.go`).

use std::fmt;

use cabby_mruby_sys as sys;

use super::{Class, Mrb, expect_ok};

/// The mruby 1.2.0 `enum mrb_vtype` as go-mruby numbers it (`ValueType`);
/// `Nil` is go-mruby's own `0xffffffff` (mruby reports nil as `MRB_TT_FALSE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ValueType {
    False = 0,
    Free,
    True,
    Fixnum,
    Symbol,
    Undef,
    Float,
    Cptr,
    Object,
    Class,
    Module,
    IClass,
    SClass,
    Proc,
    Array,
    Hash,
    String,
    Range,
    Exception,
    File,
    Env,
    Data,
    Fiber,
    MaxDefine,
    Nil = 0xffff_ffff,
}

impl ValueType {
    fn from_tag(tag: u32) -> ValueType {
        const TYPES: [ValueType; 24] = [
            ValueType::False,
            ValueType::Free,
            ValueType::True,
            ValueType::Fixnum,
            ValueType::Symbol,
            ValueType::Undef,
            ValueType::Float,
            ValueType::Cptr,
            ValueType::Object,
            ValueType::Class,
            ValueType::Module,
            ValueType::IClass,
            ValueType::SClass,
            ValueType::Proc,
            ValueType::Array,
            ValueType::Hash,
            ValueType::String,
            ValueType::Range,
            ValueType::Exception,
            ValueType::File,
            ValueType::Env,
            ValueType::Data,
            ValueType::Fiber,
            ValueType::MaxDefine,
        ];
        usize::try_from(tag).ok().and_then(|i| TYPES.get(i).copied()).unwrap_or(ValueType::Nil)
    }

    /// The number go-mruby prints for the type (`%v` of a `ValueType`).
    pub(super) fn number(self) -> u32 {
        self as u32
    }
}

/// A value inside one VM (`*MrbValue`).
#[derive(Debug, Clone)]
pub struct MrbValue {
    pub(super) v: sys::Val,
}

/// Anything that converts to an mruby value (go-mruby's `Value` interface).
#[derive(Debug, Clone)]
pub enum Value {
    /// `mruby.Int`.
    Int(i64),
    /// `mruby.String`.
    String(String),
    /// `mruby.Nil`.
    Nil,
    /// `*MrbValue` (also `*Array`, `*Hash`, `*Exception` through their value).
    Mrb(MrbValue),
    /// `*Class`.
    Class(Class),
}

impl Value {
    /// The value inside `m` (`Value.MrbValue(*Mrb)`).
    pub fn mrb_value(&self, m: &Mrb) -> MrbValue {
        match self {
            Value::Int(n) => m.fixnum_value(*n),
            Value::String(s) => m.string_value(s),
            Value::Nil => m.nil_value(),
            Value::Mrb(v) => v.clone(),
            Value::Class(c) => c.mrb_value(m),
        }
    }
}

impl From<MrbValue> for Value {
    fn from(v: MrbValue) -> Self {
        Value::Mrb(v)
    }
}

impl From<&MrbValue> for Value {
    fn from(v: &MrbValue) -> Self {
        Value::Mrb(v.clone())
    }
}

impl From<Class> for Value {
    fn from(c: Class) -> Self {
        Value::Class(c)
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Int(n)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_owned())
    }
}

impl MrbValue {
    fn values(&self, args: &[Value]) -> Vec<sys::Val> {
        let m = self.mrb();
        args.iter().map(|a| a.mrb_value(&m).v).collect()
    }

    /// `Type`.
    pub fn value_type(&self) -> ValueType {
        ValueType::from_tag(self.v.type_tag())
    }

    /// The integer of a `Fixnum` (`Fixnum`).
    pub fn fixnum(&self) -> i64 {
        self.v.fixnum()
    }

    /// The number of a `Float` (`Float`). go-mruby reads it through a C `float`,
    /// so `1.2` comes back as `1.2000000476837158`; Cabby keeps that narrowing.
    pub fn float(&self) -> f64 {
        self.v.float()
    }

    pub fn array(&self) -> Array {
        Array { v: self.clone() }
    }

    pub fn hash(&self) -> Hash {
        Hash { v: self.clone() }
    }

    pub fn class(&self) -> Class {
        Class { v: expect_ok(self.v.class_of(), "Class") }
    }

    /// The singleton class. Panics for values that have none (Fixnum, Symbol,
    /// Float), where mruby raises and go-mruby crashes.
    pub fn singleton_class(&self) -> Class {
        Class { v: expect_ok(self.v.singleton_class(), "SingletonClass") }
    }

    /// Calls `method` with `args` (`Call`).
    pub fn call(&self, method: &str, args: &[Value]) -> Result<MrbValue, Exception> {
        let args = self.values(args);
        self.v.call(method, &args, None).map(|v| MrbValue { v }).map_err(Exception::from_raised)
    }

    /// Calls `method` with `args` and `block` (`CallBlock`). Go takes the block
    /// as the last variadic argument and errors when there is none; here it is a
    /// separate parameter.
    pub fn call_block(&self, method: &str, args: &[Value], block: &Value) -> Result<MrbValue, Exception> {
        let args = self.values(args);
        let block = block.mrb_value(&self.mrb());
        self.v.call(method, &args, Some(&block.v)).map(|v| MrbValue { v }).map_err(Exception::from_raised)
    }

    pub fn get_instance_variable(&self, name: &str) -> MrbValue {
        MrbValue { v: expect_ok(self.v.iv_get(name), "GetInstanceVariable") }
    }

    /// Sets an instance variable. Panics for values without instance
    /// variables, where mruby raises and go-mruby crashes.
    pub fn set_instance_variable(&self, name: &str, value: &MrbValue) {
        expect_ok(self.v.iv_set(name, &value.v), "SetInstanceVariable");
    }

    /// Whether the GC has collected the object, or its VM was closed (`IsDead`).
    pub fn is_dead(&self) -> bool {
        self.v.is_dead()
    }

    /// The VM of this value (`Mrb`).
    pub fn mrb(&self) -> Mrb {
        Mrb { vm: self.v.vm() }
    }

    pub fn gc_protect(&self) {
        self.v.gc_protect();
    }

    pub fn set_proc_target_class(&self, class: &Class) {
        expect_ok(self.v.set_proc_target_class(&class.v), "SetProcTargetClass");
    }

    /// `(*MrbValue).MrbValue(*Mrb)`: the value itself.
    pub fn mrb_value(&self, _m: &Mrb) -> MrbValue {
        self.clone()
    }

    /// Whether both handles denote the same value (Go: pointer identity).
    pub fn same(&self, other: &MrbValue) -> bool {
        self.v.same(&other.v)
    }
}

/// `String()`: the result of `to_s`. Empty when `to_s` raises (go-mruby
/// crashes there).
impl fmt::Display for MrbValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.v.to_s().unwrap_or_default())
    }
}

/// A Ruby exception raised out of a VM call (`*Exception`). Its fields are copied
/// out when it is raised, so they stay readable after the VM is closed.
#[derive(Debug, Clone)]
pub struct Exception {
    /// The exception object.
    pub value: MrbValue,
    /// File of the first backtrace line (`Unknown` without a backtrace).
    pub file: String,
    /// Line of the first backtrace line (0 without a backtrace).
    pub line: i64,
    pub message: String,
    pub backtrace: Vec<String>,
}

impl Exception {
    /// go-mruby `newExceptionValue`: file and line come from the first
    /// backtrace line split at `:` (line 0 when it is not a number).
    pub(super) fn from_raised(r: sys::Raised) -> Exception {
        let mut file = "Unknown".to_owned();
        let mut line = 0;
        if let Some(first) = r.backtrace.first() {
            let parts: Vec<&str> = first.split(':').collect();
            if parts.len() >= 2 {
                file = parts[0].to_owned();
                line = parts[1].parse().unwrap_or(0);
            }
        }
        Exception { value: MrbValue { v: r.value }, file, line, message: r.message, backtrace: r.backtrace }
    }
}

/// `Error()`: the message.
impl fmt::Display for Exception {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Exception {}

/// A Ruby array (`*Array`).
#[derive(Debug, Clone)]
pub struct Array {
    v: MrbValue,
}

impl Array {
    pub fn value(&self) -> MrbValue {
        self.v.clone()
    }

    /// The length; 0 when the value is not an array.
    pub fn len(&self) -> usize {
        self.v.v.ary_len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The element at `idx`; `None` for a nil element (`Get`).
    pub fn get(&self, idx: i64) -> Result<Option<MrbValue>, Exception> {
        let v = self.v.v.ary_entry(idx).map_err(Exception::from_raised)?;
        Ok((!v.is_nil()).then_some(MrbValue { v }))
    }
}

/// A Ruby hash (`*Hash`).
#[derive(Debug, Clone)]
pub struct Hash {
    v: MrbValue,
}

impl Hash {
    fn key(&self, key: &Value) -> sys::Val {
        key.mrb_value(&self.v.mrb()).v
    }

    pub fn value(&self) -> MrbValue {
        self.v.clone()
    }

    pub fn get(&self, key: &Value) -> Result<MrbValue, Exception> {
        self.v.v.hash_get(&self.key(key)).map(|v| MrbValue { v }).map_err(Exception::from_raised)
    }

    pub fn set(&self, key: &Value, value: &Value) -> Result<(), Exception> {
        let value = value.mrb_value(&self.v.mrb()).v;
        self.v.v.hash_set(&self.key(key), &value).map_err(Exception::from_raised)
    }

    /// Removes `key`, returning its value; `None` when absent or nil (`Delete`).
    pub fn delete(&self, key: &Value) -> Result<Option<MrbValue>, Exception> {
        let v = self.v.v.hash_delete(&self.key(key)).map_err(Exception::from_raised)?;
        Ok((!v.is_nil()).then_some(MrbValue { v }))
    }

    /// The keys as a Ruby array (`Keys`).
    pub fn keys(&self) -> Result<MrbValue, Exception> {
        self.v.v.hash_keys().map(|v| MrbValue { v }).map_err(Exception::from_raised)
    }
}
