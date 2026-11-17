//! mruby values (go-mruby `value.go`, `value_type.go`, `array.go`, `hash.go`).

use std::fmt;

use cabby_core::pending;

use super::{Class, Mrb, NotSend};

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

/// A value inside one VM (`*MrbValue`).
#[derive(Debug, Clone)]
pub struct MrbValue {
    _v: NotSend,
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
    pub fn mrb_value(&self, _m: &Mrb) -> MrbValue {
        pending!("mrb::Value::mrb_value")
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
    /// `Type`.
    pub fn value_type(&self) -> ValueType {
        pending!("mrb::MrbValue::value_type")
    }

    /// The integer of a `Fixnum` (`Fixnum`).
    pub fn fixnum(&self) -> i64 {
        pending!("mrb::MrbValue::fixnum")
    }

    /// The number of a `Float` (`Float`). go-mruby reads it through a C `float`,
    /// so `1.2` comes back as `1.2000000476837158`; Cabby keeps that narrowing.
    pub fn float(&self) -> f64 {
        pending!("mrb::MrbValue::float")
    }

    pub fn array(&self) -> Array {
        pending!("mrb::MrbValue::array")
    }

    pub fn hash(&self) -> Hash {
        pending!("mrb::MrbValue::hash")
    }

    pub fn class(&self) -> Class {
        pending!("mrb::MrbValue::class")
    }

    pub fn singleton_class(&self) -> Class {
        pending!("mrb::MrbValue::singleton_class")
    }

    /// Calls `method` with `args` (`Call`).
    pub fn call(&self, _method: &str, _args: &[Value]) -> Result<MrbValue, Exception> {
        pending!("mrb::MrbValue::call")
    }

    /// Calls `method` with `args` and `block` (`CallBlock`). Go takes the block
    /// as the last variadic argument and errors when there is none; here it is a
    /// separate parameter.
    pub fn call_block(&self, _method: &str, _args: &[Value], _block: &Value) -> Result<MrbValue, Exception> {
        pending!("mrb::MrbValue::call_block")
    }

    pub fn get_instance_variable(&self, _name: &str) -> MrbValue {
        pending!("mrb::MrbValue::get_instance_variable")
    }

    pub fn set_instance_variable(&self, _name: &str, _value: &MrbValue) {
        pending!("mrb::MrbValue::set_instance_variable")
    }

    /// Whether the GC has collected the object, or its VM was closed (`IsDead`).
    pub fn is_dead(&self) -> bool {
        pending!("mrb::MrbValue::is_dead")
    }

    /// The VM of this value (`Mrb`).
    pub fn mrb(&self) -> Mrb {
        pending!("mrb::MrbValue::mrb")
    }

    pub fn gc_protect(&self) {
        pending!("mrb::MrbValue::gc_protect")
    }

    pub fn set_proc_target_class(&self, _class: &Class) {
        pending!("mrb::MrbValue::set_proc_target_class")
    }

    /// `(*MrbValue).MrbValue(*Mrb)`: the value itself.
    pub fn mrb_value(&self, _m: &Mrb) -> MrbValue {
        pending!("mrb::MrbValue::mrb_value")
    }

    /// Whether both handles denote the same value (Go: pointer identity).
    pub fn same(&self, _other: &MrbValue) -> bool {
        pending!("mrb::MrbValue::same")
    }
}

/// `String()`: the result of `to_s`.
impl fmt::Display for MrbValue {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        pending!("mrb::MrbValue::to_s")
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

/// `Error()`: the message.
impl fmt::Display for Exception {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        pending!("mrb::Exception::fmt")
    }
}

impl std::error::Error for Exception {}

/// A Ruby array (`*Array`).
#[derive(Debug, Clone)]
pub struct Array {
    _v: NotSend,
}

impl Array {
    pub fn value(&self) -> MrbValue {
        pending!("mrb::Array::value")
    }

    pub fn len(&self) -> usize {
        pending!("mrb::Array::len")
    }

    pub fn is_empty(&self) -> bool {
        pending!("mrb::Array::is_empty")
    }

    /// The element at `idx`; `None` for a nil element (`Get`).
    pub fn get(&self, _idx: i64) -> Result<Option<MrbValue>, Exception> {
        pending!("mrb::Array::get")
    }
}

/// A Ruby hash (`*Hash`).
#[derive(Debug, Clone)]
pub struct Hash {
    _v: NotSend,
}

impl Hash {
    pub fn value(&self) -> MrbValue {
        pending!("mrb::Hash::value")
    }

    pub fn get(&self, _key: &Value) -> Result<MrbValue, Exception> {
        pending!("mrb::Hash::get")
    }

    pub fn set(&self, _key: &Value, _value: &Value) -> Result<(), Exception> {
        pending!("mrb::Hash::set")
    }

    /// Removes `key`, returning its value; `None` when absent or nil (`Delete`).
    pub fn delete(&self, _key: &Value) -> Result<Option<MrbValue>, Exception> {
        pending!("mrb::Hash::delete")
    }

    /// The keys as a Ruby array (`Keys`).
    pub fn keys(&self) -> Result<MrbValue, Exception> {
        pending!("mrb::Hash::keys")
    }
}
