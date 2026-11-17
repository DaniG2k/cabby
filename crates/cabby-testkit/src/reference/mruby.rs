//! Facade for the vendored go-mruby binding (Go module
//! `github.com/mitchellh/go-mruby`, package `mruby`).
//!
//! Every handle type is an enum over the backend: `Rust` wraps the Cabby type
//! from `cabby_server::mrb`; `Go` is a handle to the Go object living in the
//! mruby bridge (`compat/reference-adapters/mruby-bridge`). Handles are interned
//! by Go pointer, so equal Go handles mean the same Go pointer. Ruby methods
//! defined from a test run as bridge callbacks on the defining thread.
//!
//! Facades only convert shapes; they contain no behavior.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::ops::BitOr;
use std::rc::Rc;

use cabby_server::mrb as rs;
pub use cabby_server::mrb::{ArenaIndex, Decoded, ParserMessage, ValueType};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value as Json, json};

use crate::mruby_bridge as bridge;
use crate::{Backend, backend};

type H = u64;

fn mixed() -> ! {
    panic!("handle from the other backend")
}

/// A VM (`*Mrb`).
#[derive(Debug, Clone)]
pub enum Mrb {
    Rust(rs::Mrb),
    Go(H),
}

/// A value (`*MrbValue`).
#[derive(Debug, Clone)]
pub enum MrbValue {
    Rust(rs::MrbValue),
    Go(H),
}

/// A class or module (`*Class`).
#[derive(Debug, Clone)]
pub enum Class {
    Rust(rs::Class),
    Go(H),
}

/// go-mruby's `Value` interface: `Int`, `String`, `Nil`, `*MrbValue`, `*Class`.
#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    String(String),
    Nil,
    Mrb(MrbValue),
    Class(Class),
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

impl Value {
    fn to_rs(&self) -> rs::Value {
        match self {
            Value::Int(n) => rs::Value::Int(*n),
            Value::String(s) => rs::Value::String(s.clone()),
            Value::Nil => rs::Value::Nil,
            Value::Mrb(v) => rs::Value::Mrb(v.rs().clone()),
            Value::Class(c) => rs::Value::Class(c.rs().clone()),
        }
    }

    fn to_go(&self) -> Json {
        match self {
            Value::Int(n) => json!({"int": n}),
            Value::String(s) => json!({"str": s}),
            Value::Nil => json!({"nil": true}),
            Value::Mrb(v) => json!({"h": v.go()}),
            Value::Class(c) => json!({"h": c.go()}),
        }
    }

    /// `Value.MrbValue(*Mrb)`.
    pub fn mrb_value(&self, m: &Mrb) -> MrbValue {
        match m {
            Mrb::Rust(m) => MrbValue::Rust(self.to_rs().mrb_value(m)),
            Mrb::Go(h) => MrbValue::Go(bridge::call("mruby.ToMrbValue", json!({"value": self.to_go(), "mrb": h}))),
        }
    }
}

fn rs_values(args: &[Value]) -> Vec<rs::Value> {
    args.iter().map(Value::to_rs).collect()
}

fn go_values(args: &[Value]) -> Vec<Json> {
    args.iter().map(Value::to_go).collect()
}

// ---------------------------------------------------------------- results

/// A Ruby exception (`*Exception`).
#[derive(Debug, Clone)]
pub struct Exception {
    pub value: MrbValue,
    pub file: String,
    pub line: i64,
    pub message: String,
    pub backtrace: Vec<String>,
    error: Box<ErrorText>,
}

#[derive(Debug, Clone)]
enum ErrorText {
    Rust(rs::Exception),
    Go(String),
}

impl Exception {
    /// `err.Error()`.
    pub fn error(&self) -> String {
        match &*self.error {
            ErrorText::Rust(e) => e.to_string(),
            ErrorText::Go(s) => s.clone(),
        }
    }
}

impl From<rs::Exception> for Exception {
    fn from(e: rs::Exception) -> Self {
        Exception {
            value: MrbValue::Rust(e.value.clone()),
            file: e.file.clone(),
            line: e.line,
            message: e.message.clone(),
            backtrace: e.backtrace.clone(),
            error: Box::new(ErrorText::Rust(e)),
        }
    }
}

#[derive(Deserialize)]
struct GoExc {
    h: H,
    file: String,
    line: i64,
    message: String,
    backtrace: Option<Vec<String>>,
    error: String,
}

impl From<GoExc> for Exception {
    fn from(e: GoExc) -> Self {
        Exception {
            value: MrbValue::Go(e.h),
            file: e.file,
            line: e.line,
            message: e.message,
            backtrace: e.backtrace.unwrap_or_default(),
            error: Box::new(ErrorText::Go(e.error)),
        }
    }
}

/// Converts an exception as the bridge encodes it (shared with [`super::mrb`]).
pub(crate) fn go_exception(j: Json) -> Exception {
    serde_json::from_value::<GoExc>(j.clone()).unwrap_or_else(|e| panic!("decoding Go exception {j}: {e}")).into()
}

/// A Go `(*MrbValue, error)` pair.
#[derive(Deserialize)]
struct GoFallible {
    value: Option<H>,
    exc: Option<GoExc>,
    error: Option<String>,
}

impl GoFallible {
    /// The value (`None` for a nil `*MrbValue`) or the raised exception. A
    /// non-exception error is a harness error for the ops that use this.
    fn result(self) -> Result<Option<MrbValue>, Exception> {
        if let Some(e) = self.exc {
            // Go returns a nil value with every exception (TestMrbStackedException:
            // "result was not cleared"); `Err` drops the value, so check it here.
            assert!(self.value.is_none(), "go-mruby returned a value together with an exception");
            return Err(e.into());
        }
        if let Some(e) = self.error {
            panic!("go-mruby returned a non-exception error: {e}");
        }
        Ok(self.value.map(MrbValue::Go))
    }

    fn value(self) -> Result<MrbValue, Exception> {
        self.result().map(|v| v.expect("go-mruby returned a nil *MrbValue without error"))
    }
}

fn go_value(op: &str, args: Json) -> Result<MrbValue, Exception> {
    bridge::call::<_, GoFallible>(op, args).value()
}

// ---------------------------------------------------------------- Mrb

impl Mrb {
    /// `NewMrb`.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Mrb {
        match backend() {
            Backend::Rust => Mrb::Rust(rs::Mrb::new()),
            Backend::Go => Mrb::Go(bridge::call("mruby.NewMrb", json!({}))),
        }
    }

    fn go_unit(&self, op: &str) {
        let Mrb::Go(h) = self else { mixed() };
        bridge::call::<_, Json>(op, json!({"h": h}));
    }

    fn go_get<R: DeserializeOwned>(&self, op: &str, mut args: Json) -> R {
        let Mrb::Go(h) = self else { mixed() };
        args["h"] = json!(h);
        bridge::call(op, args)
    }

    pub fn close(self) {
        match self {
            Mrb::Rust(m) => m.close(),
            Mrb::Go(_) => self.go_unit("mruby.Mrb.Close"),
        }
    }

    pub fn arena_save(&self) -> ArenaIndex {
        match self {
            Mrb::Rust(m) => m.arena_save(),
            Mrb::Go(_) => ArenaIndex(self.go_get("mruby.Mrb.ArenaSave", json!({}))),
        }
    }

    pub fn arena_restore(&self, idx: ArenaIndex) {
        match self {
            Mrb::Rust(m) => m.arena_restore(idx),
            Mrb::Go(_) => {
                self.go_get::<Json>("mruby.Mrb.ArenaRestore", json!({"idx": idx.0}));
            }
        }
    }

    pub fn enable_gc(&self) {
        match self {
            Mrb::Rust(m) => m.enable_gc(),
            Mrb::Go(_) => self.go_unit("mruby.Mrb.EnableGC"),
        }
    }

    pub fn disable_gc(&self) {
        match self {
            Mrb::Rust(m) => m.disable_gc(),
            Mrb::Go(_) => self.go_unit("mruby.Mrb.DisableGC"),
        }
    }

    pub fn full_gc(&self) {
        match self {
            Mrb::Rust(m) => m.full_gc(),
            Mrb::Go(_) => self.go_unit("mruby.Mrb.FullGC"),
        }
    }

    pub fn incremental_gc(&self) {
        match self {
            Mrb::Rust(m) => m.incremental_gc(),
            Mrb::Go(_) => self.go_unit("mruby.Mrb.IncrementalGC"),
        }
    }

    pub fn live_object_count(&self) -> usize {
        match self {
            Mrb::Rust(m) => m.live_object_count(),
            Mrb::Go(_) => self.go_get("mruby.Mrb.LiveObjectCount", json!({})),
        }
    }

    /// `Class(name, super)`: Go never returns nil (it aborts instead).
    pub fn class(&self, name: &str, outer: Option<&Class>) -> Option<Class> {
        match self {
            Mrb::Rust(m) => m.class(name, outer.map(Class::rs)).map(Class::Rust),
            Mrb::Go(_) => self
                .go_get::<Option<H>>("mruby.Mrb.Class", json!({"name": name, "super": outer.map(Class::go)}))
                .map(Class::Go),
        }
    }

    pub fn module(&self, name: &str) -> Option<Class> {
        match self {
            Mrb::Rust(m) => m.module(name).map(Class::Rust),
            Mrb::Go(_) => self.go_get::<Option<H>>("mruby.Mrb.Module", json!({"name": name})).map(Class::Go),
        }
    }

    pub fn const_defined(&self, name: &str, scope: &Value) -> bool {
        match self {
            Mrb::Rust(m) => m.const_defined(name, &scope.to_rs()),
            Mrb::Go(_) => self.go_get("mruby.Mrb.ConstDefined", json!({"name": name, "scope": scope.to_go()})),
        }
    }

    pub fn define_class(&self, name: &str, super_class: Option<&Class>) -> Class {
        match self {
            Mrb::Rust(m) => Class::Rust(m.define_class(name, super_class.map(Class::rs))),
            Mrb::Go(_) => Class::Go(
                self.go_get("mruby.Mrb.DefineClass", json!({"name": name, "super": super_class.map(Class::go)})),
            ),
        }
    }

    pub fn define_class_under(&self, name: &str, super_class: Option<&Class>, outer: Option<&Class>) -> Class {
        match self {
            Mrb::Rust(m) => Class::Rust(m.define_class_under(name, super_class.map(Class::rs), outer.map(Class::rs))),
            Mrb::Go(_) => Class::Go(self.go_get(
                "mruby.Mrb.DefineClassUnder",
                json!({"name": name, "super": super_class.map(Class::go), "outer": outer.map(Class::go)}),
            )),
        }
    }

    pub fn define_module(&self, name: &str) -> Class {
        match self {
            Mrb::Rust(m) => Class::Rust(m.define_module(name)),
            Mrb::Go(_) => Class::Go(self.go_get("mruby.Mrb.DefineModule", json!({"name": name}))),
        }
    }

    pub fn define_module_under(&self, name: &str, outer: Option<&Class>) -> Class {
        match self {
            Mrb::Rust(m) => Class::Rust(m.define_module_under(name, outer.map(Class::rs))),
            Mrb::Go(_) => Class::Go(
                self.go_get("mruby.Mrb.DefineModuleUnder", json!({"name": name, "outer": outer.map(Class::go)})),
            ),
        }
    }

    pub fn object_class(&self) -> Class {
        match self {
            Mrb::Rust(m) => Class::Rust(m.object_class()),
            Mrb::Go(_) => Class::Go(self.go_get("mruby.Mrb.ObjectClass", json!({}))),
        }
    }

    pub fn kernel_module(&self) -> Class {
        match self {
            Mrb::Rust(m) => Class::Rust(m.kernel_module()),
            Mrb::Go(_) => Class::Go(self.go_get("mruby.Mrb.KernelModule", json!({}))),
        }
    }

    pub fn top_self(&self) -> MrbValue {
        match self {
            Mrb::Rust(m) => MrbValue::Rust(m.top_self()),
            Mrb::Go(_) => MrbValue::Go(self.go_get("mruby.Mrb.TopSelf", json!({}))),
        }
    }

    pub fn false_value(&self) -> MrbValue {
        match self {
            Mrb::Rust(m) => MrbValue::Rust(m.false_value()),
            Mrb::Go(_) => MrbValue::Go(self.go_get("mruby.Mrb.FalseValue", json!({}))),
        }
    }

    pub fn nil_value(&self) -> MrbValue {
        match self {
            Mrb::Rust(m) => MrbValue::Rust(m.nil_value()),
            Mrb::Go(_) => MrbValue::Go(self.go_get("mruby.Mrb.NilValue", json!({}))),
        }
    }

    pub fn true_value(&self) -> MrbValue {
        match self {
            Mrb::Rust(m) => MrbValue::Rust(m.true_value()),
            Mrb::Go(_) => MrbValue::Go(self.go_get("mruby.Mrb.TrueValue", json!({}))),
        }
    }

    pub fn fixnum_value(&self, n: i64) -> MrbValue {
        match self {
            Mrb::Rust(m) => MrbValue::Rust(m.fixnum_value(n)),
            Mrb::Go(_) => MrbValue::Go(self.go_get("mruby.Mrb.FixnumValue", json!({"n": n}))),
        }
    }

    pub fn string_value(&self, s: &str) -> MrbValue {
        match self {
            Mrb::Rust(m) => MrbValue::Rust(m.string_value(s)),
            Mrb::Go(_) => MrbValue::Go(self.go_get("mruby.Mrb.StringValue", json!({"s": s}))),
        }
    }

    pub fn get_global_variable(&self, name: &str) -> MrbValue {
        match self {
            Mrb::Rust(m) => MrbValue::Rust(m.get_global_variable(name)),
            Mrb::Go(_) => MrbValue::Go(self.go_get("mruby.Mrb.GetGlobalVariable", json!({"name": name}))),
        }
    }

    pub fn set_global_variable(&self, name: &str, value: &Value) {
        match self {
            Mrb::Rust(m) => m.set_global_variable(name, &value.to_rs()),
            Mrb::Go(_) => {
                self.go_get::<Json>("mruby.Mrb.SetGlobalVariable", json!({"name": name, "value": value.to_go()}));
            }
        }
    }

    pub fn get_args(&self) -> Vec<MrbValue> {
        match self {
            Mrb::Rust(m) => m.get_args().into_iter().map(MrbValue::Rust).collect(),
            Mrb::Go(_) => self.go_get::<Vec<H>>("mruby.Mrb.GetArgs", json!({})).into_iter().map(MrbValue::Go).collect(),
        }
    }

    pub fn load_string(&self, code: &str) -> Result<MrbValue, Exception> {
        match self {
            Mrb::Rust(m) => m.load_string(code).map(MrbValue::Rust).map_err(Exception::from),
            Mrb::Go(h) => go_value("mruby.Mrb.LoadString", json!({"h": h, "code": code})),
        }
    }

    /// `Run(proc, self)`; `None` self is Go's nil (top-level self).
    pub fn run(&self, proc: &Value, self_value: Option<&Value>) -> Result<MrbValue, Exception> {
        match self {
            Mrb::Rust(m) => {
                m.run(&proc.to_rs(), self_value.map(Value::to_rs).as_ref()).map(MrbValue::Rust).map_err(Exception::from)
            }
            Mrb::Go(h) => {
                go_value("mruby.Mrb.Run", json!({"h": h, "proc": proc.to_go(), "self": self_value.map(Value::to_go)}))
            }
        }
    }

    pub fn run_with_context(
        &self,
        proc: &Value,
        self_value: Option<&Value>,
        stack_keep: usize,
    ) -> Result<(usize, MrbValue), Exception> {
        match self {
            Mrb::Rust(m) => m
                .run_with_context(&proc.to_rs(), self_value.map(Value::to_rs).as_ref(), stack_keep)
                .map(|(k, v)| (k, MrbValue::Rust(v)))
                .map_err(Exception::from),
            Mrb::Go(h) => {
                #[derive(Deserialize)]
                struct R {
                    stack_keep: usize,
                    result: GoFallible,
                }
                let r: R = bridge::call(
                    "mruby.Mrb.RunWithContext",
                    json!({"h": h, "proc": proc.to_go(), "self": self_value.map(Value::to_go), "stack_keep": stack_keep}),
                );
                r.result.value().map(|v| (r.stack_keep, v))
            }
        }
    }

    /// `Yield(block, args...)`.
    pub fn yield_block(&self, block: &Value, args: &[Value]) -> Result<MrbValue, Exception> {
        match self {
            Mrb::Rust(m) => {
                m.yield_block(&block.to_rs(), &rs_values(args)).map(MrbValue::Rust).map_err(Exception::from)
            }
            Mrb::Go(h) => go_value("mruby.Mrb.Yield", json!({"h": h, "block": block.to_go(), "args": go_values(args)})),
        }
    }
}

/// Runs `f` in a bridge process of its own on Go (no other VM in that process);
/// directly on Rust.
pub fn isolated<R>(f: impl FnOnce() -> R) -> R {
    match backend() {
        Backend::Rust => f(),
        Backend::Go => bridge::isolated(f),
    }
}

// ---------------------------------------------------------------- MrbValue

impl MrbValue {
    fn rs(&self) -> &rs::MrbValue {
        match self {
            MrbValue::Rust(v) => v,
            MrbValue::Go(_) => mixed(),
        }
    }

    fn go(&self) -> H {
        match self {
            MrbValue::Go(h) => *h,
            MrbValue::Rust(_) => mixed(),
        }
    }

    fn go_get<R: DeserializeOwned>(&self, op: &str, mut args: Json) -> R {
        args["h"] = json!(self.go());
        bridge::call(op, args)
    }

    /// `Type()`.
    pub fn value_type(&self) -> ValueType {
        match self {
            MrbValue::Rust(v) => v.value_type(),
            MrbValue::Go(_) => go_value_type(self.go_get("mruby.Value.Type", json!({}))),
        }
    }

    /// `String()`.
    pub fn string(&self) -> String {
        match self {
            MrbValue::Rust(v) => v.to_string(),
            MrbValue::Go(_) => self.go_get("mruby.Value.String", json!({})),
        }
    }

    pub fn fixnum(&self) -> i64 {
        match self {
            MrbValue::Rust(v) => v.fixnum(),
            MrbValue::Go(_) => self.go_get("mruby.Value.Fixnum", json!({})),
        }
    }

    pub fn float(&self) -> f64 {
        match self {
            MrbValue::Rust(v) => v.float(),
            MrbValue::Go(_) => self.go_get("mruby.Value.Float", json!({})),
        }
    }

    pub fn is_dead(&self) -> bool {
        match self {
            MrbValue::Rust(v) => v.is_dead(),
            MrbValue::Go(_) => self.go_get("mruby.Value.IsDead", json!({})),
        }
    }

    pub fn mrb(&self) -> Mrb {
        match self {
            MrbValue::Rust(v) => Mrb::Rust(v.mrb()),
            MrbValue::Go(_) => Mrb::Go(self.go_get("mruby.Value.Mrb", json!({}))),
        }
    }

    pub fn array(&self) -> Array {
        match self {
            MrbValue::Rust(v) => Array::Rust(v.array()),
            MrbValue::Go(_) => Array::Go(self.go_get("mruby.Value.Array", json!({}))),
        }
    }

    pub fn hash(&self) -> Hash {
        match self {
            MrbValue::Rust(v) => Hash::Rust(v.hash()),
            MrbValue::Go(_) => Hash::Go(self.go_get("mruby.Value.Hash", json!({}))),
        }
    }

    pub fn class(&self) -> Class {
        match self {
            MrbValue::Rust(v) => Class::Rust(v.class()),
            MrbValue::Go(_) => Class::Go(self.go_get("mruby.Value.Class", json!({}))),
        }
    }

    pub fn singleton_class(&self) -> Class {
        match self {
            MrbValue::Rust(v) => Class::Rust(v.singleton_class()),
            MrbValue::Go(_) => Class::Go(self.go_get("mruby.Value.SingletonClass", json!({}))),
        }
    }

    pub fn get_instance_variable(&self, name: &str) -> MrbValue {
        match self {
            MrbValue::Rust(v) => MrbValue::Rust(v.get_instance_variable(name)),
            MrbValue::Go(_) => MrbValue::Go(self.go_get("mruby.Value.GetInstanceVariable", json!({"name": name}))),
        }
    }

    pub fn set_instance_variable(&self, name: &str, value: &MrbValue) {
        match self {
            MrbValue::Rust(v) => v.set_instance_variable(name, value.rs()),
            MrbValue::Go(_) => {
                self.go_get::<Json>("mruby.Value.SetInstanceVariable", json!({"name": name, "value": value.go()}));
            }
        }
    }

    /// `Call(method, args...)`.
    pub fn call(&self, method: &str, args: &[Value]) -> Result<MrbValue, Exception> {
        match self {
            MrbValue::Rust(v) => v.call(method, &rs_values(args)).map(MrbValue::Rust).map_err(Exception::from),
            MrbValue::Go(h) => go_value("mruby.Value.Call", json!({"h": h, "method": method, "args": go_values(args)})),
        }
    }

    /// `CallBlock(method, args..., block)`.
    pub fn call_block(&self, method: &str, args: &[Value], block: &Value) -> Result<MrbValue, Exception> {
        match self {
            MrbValue::Rust(v) => {
                v.call_block(method, &rs_values(args), &block.to_rs()).map(MrbValue::Rust).map_err(Exception::from)
            }
            MrbValue::Go(h) => {
                let mut all = go_values(args);
                all.push(block.to_go());
                go_value("mruby.Value.CallBlock", json!({"h": h, "method": method, "args": all}))
            }
        }
    }

    /// `(*MrbValue).MrbValue(*Mrb)`.
    pub fn mrb_value(&self, m: &Mrb) -> MrbValue {
        match (self, m) {
            (MrbValue::Rust(v), Mrb::Rust(m)) => MrbValue::Rust(v.mrb_value(m)),
            _ => Value::Mrb(self.clone()).mrb_value(m),
        }
    }

    /// Identity (Go: `*MrbValue` pointer equality; handles are interned by pointer).
    pub fn same(&self, other: &MrbValue) -> bool {
        match (self, other) {
            (MrbValue::Rust(a), MrbValue::Rust(b)) => a.same(b),
            (MrbValue::Go(a), MrbValue::Go(b)) => a == b,
            _ => mixed(),
        }
    }
}

impl fmt::Display for MrbValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.string())
    }
}

/// go-mruby's `ValueType` numbering (`value_type.go`).
fn go_value_type(n: u32) -> ValueType {
    use ValueType::*;
    const ORDER: [ValueType; 24] = [
        False, Free, True, Fixnum, Symbol, Undef, Float, Cptr, Object, Class, Module, IClass, SClass, Proc, Array,
        Hash, String, Range, Exception, File, Env, Data, Fiber, MaxDefine,
    ];
    match n {
        0xffff_ffff => Nil,
        n => *ORDER.get(n as usize).unwrap_or_else(|| panic!("unknown go-mruby ValueType {n}")),
    }
}

// ---------------------------------------------------------------- Class, ArgSpec

/// `ArgSpec`; specs combine with `|` as in Go.
#[derive(Debug, Clone, Copy)]
pub enum ArgSpec {
    Rust(rs::ArgSpec),
    Go(u32),
}

impl ArgSpec {
    fn go_spec(kind: &str, n: u32, o: u32) -> ArgSpec {
        ArgSpec::Go(bridge::call("mruby.ArgSpec", json!({"kind": kind, "n": n, "o": o})))
    }

    /// `ArgsAny()`.
    pub fn any() -> ArgSpec {
        match backend() {
            Backend::Rust => ArgSpec::Rust(rs::ArgSpec::any()),
            Backend::Go => Self::go_spec("any", 0, 0),
        }
    }

    /// `ArgsNone()`.
    pub fn none() -> ArgSpec {
        match backend() {
            Backend::Rust => ArgSpec::Rust(rs::ArgSpec::none()),
            Backend::Go => Self::go_spec("none", 0, 0),
        }
    }

    /// `ArgsBlock()`.
    pub fn block() -> ArgSpec {
        match backend() {
            Backend::Rust => ArgSpec::Rust(rs::ArgSpec::block()),
            Backend::Go => Self::go_spec("block", 0, 0),
        }
    }

    /// `ArgsReq(n)`.
    pub fn req(n: u32) -> ArgSpec {
        match backend() {
            Backend::Rust => ArgSpec::Rust(rs::ArgSpec::req(n)),
            Backend::Go => Self::go_spec("req", n, 0),
        }
    }

    /// `ArgsOpt(n)`.
    pub fn opt(n: u32) -> ArgSpec {
        match backend() {
            Backend::Rust => ArgSpec::Rust(rs::ArgSpec::opt(n)),
            Backend::Go => Self::go_spec("opt", n, 0),
        }
    }

    /// `ArgsArg(r, o)`.
    pub fn arg(required: u32, optional: u32) -> ArgSpec {
        match backend() {
            Backend::Rust => ArgSpec::Rust(rs::ArgSpec::arg(required, optional)),
            Backend::Go => Self::go_spec("arg", required, optional),
        }
    }

    fn rs(self) -> rs::ArgSpec {
        match self {
            ArgSpec::Rust(s) => s,
            ArgSpec::Go(_) => mixed(),
        }
    }

    fn go(self) -> u32 {
        match self {
            ArgSpec::Go(s) => s,
            ArgSpec::Rust(_) => mixed(),
        }
    }
}

/// Go's `ArgSpec | ArgSpec` is the integer `|` of the mrb_aspec values.
impl BitOr for ArgSpec {
    type Output = ArgSpec;

    fn bitor(self, rhs: ArgSpec) -> ArgSpec {
        match (self, rhs) {
            (ArgSpec::Rust(a), ArgSpec::Rust(b)) => ArgSpec::Rust(a | b),
            (ArgSpec::Go(a), ArgSpec::Go(b)) => ArgSpec::Go(a | b),
            _ => mixed(),
        }
    }
}

/// A method body defined from a test (go-mruby `Func`): `Ok(result)` (`None`
/// is nil) or `Err(exception)`, as Go's `(Value, Value)` pair.
pub trait Func: Fn(&Mrb, &MrbValue) -> Result<Option<Value>, Value> + 'static {}

impl<F: Fn(&Mrb, &MrbValue) -> Result<Option<Value>, Value> + 'static> Func for F {}

fn rs_func(f: impl Func) -> impl rs::Func {
    move |m: &rs::Mrb, s: &rs::MrbValue| {
        f(&Mrb::Rust(m.clone()), &MrbValue::Rust(s.clone())).map(|r| r.map(|v| v.to_rs())).map_err(|e| e.to_rs())
    }
}

fn go_callback(f: impl Func) -> u64 {
    bridge::register_callback(Rc::new(move |args: Json| {
        let h = |k: &str| args[k].as_u64().unwrap_or_else(|| panic!("callback arg {k}: {args}"));
        match f(&Mrb::Go(h("mrb")), &MrbValue::Go(h("self"))) {
            Ok(ret) => json!({"ret": ret.map(|v| v.to_go()), "exc": null}),
            Err(exc) => json!({"ret": null, "exc": exc.to_go()}),
        }
    }))
}

impl Class {
    fn rs(&self) -> &rs::Class {
        match self {
            Class::Rust(c) => c,
            Class::Go(_) => mixed(),
        }
    }

    fn go(&self) -> H {
        match self {
            Class::Go(h) => *h,
            Class::Rust(_) => mixed(),
        }
    }

    /// `DefineClassMethod(name, cb, spec)`.
    pub fn define_class_method(&self, name: &str, f: impl Func, spec: ArgSpec) {
        match self {
            Class::Rust(c) => c.define_class_method(name, rs_func(f), spec.rs()),
            Class::Go(h) => {
                let cb = go_callback(f);
                bridge::call::<_, Json>(
                    "mruby.Class.DefineClassMethod",
                    json!({"h": h, "name": name, "cb": cb, "spec": spec.go()}),
                );
            }
        }
    }

    /// `DefineMethod(name, cb, spec)`.
    pub fn define_method(&self, name: &str, f: impl Func, spec: ArgSpec) {
        match self {
            Class::Rust(c) => c.define_method(name, rs_func(f), spec.rs()),
            Class::Go(h) => {
                let cb = go_callback(f);
                bridge::call::<_, Json>(
                    "mruby.Class.DefineMethod",
                    json!({"h": h, "name": name, "cb": cb, "spec": spec.go()}),
                );
            }
        }
    }

    pub fn define_const(&self, name: &str, value: &Value) {
        match self {
            Class::Rust(c) => c.define_const(name, &value.to_rs()),
            Class::Go(h) => {
                bridge::call::<_, Json>(
                    "mruby.Class.DefineConst",
                    json!({"h": h, "name": name, "value": value.to_go()}),
                );
            }
        }
    }

    /// `MrbValue(*Mrb)`.
    pub fn mrb_value(&self, m: &Mrb) -> MrbValue {
        match (self, m) {
            (Class::Rust(c), Mrb::Rust(m)) => MrbValue::Rust(c.mrb_value(m)),
            _ => Value::Class(self.clone()).mrb_value(m),
        }
    }

    /// `New(args...)`.
    pub fn new_instance(&self, args: &[Value]) -> Result<MrbValue, Exception> {
        match self {
            Class::Rust(c) => c.new_instance(&rs_values(args)).map(MrbValue::Rust).map_err(Exception::from),
            Class::Go(h) => go_value("mruby.Class.New", json!({"h": h, "args": go_values(args)})),
        }
    }
}

/// Go: `reflect.DeepEqual` of two `*Class`.
impl PartialEq for Class {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Class::Rust(a), Class::Rust(b)) => a == b,
            (Class::Go(a), Class::Go(b)) => bridge::call("mruby.Class.DeepEqual", json!({"a": a, "b": b})),
            _ => mixed(),
        }
    }
}

// ---------------------------------------------------------------- Array, Hash

/// `*Array`.
#[derive(Debug, Clone)]
pub enum Array {
    Rust(rs::Array),
    Go(H),
}

impl Array {
    pub fn len(&self) -> usize {
        match self {
            Array::Rust(a) => a.len(),
            Array::Go(h) => bridge::call("mruby.Array.Len", json!({"h": h})),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `Get(idx)`: `None` for a nil element.
    pub fn get(&self, idx: i64) -> Result<Option<MrbValue>, Exception> {
        match self {
            Array::Rust(a) => a.get(idx).map(|v| v.map(MrbValue::Rust)).map_err(Exception::from),
            Array::Go(h) => bridge::call::<_, GoFallible>("mruby.Array.Get", json!({"h": h, "idx": idx})).result(),
        }
    }
}

/// `*Hash`.
#[derive(Debug, Clone)]
pub enum Hash {
    Rust(rs::Hash),
    Go(H),
}

impl Hash {
    fn go_call(h: H, op: &str, key: &Value, value: Option<&Value>) -> Result<Option<MrbValue>, Exception> {
        bridge::call::<_, GoFallible>(op, json!({"h": h, "key": key.to_go(), "value": value.map(Value::to_go)}))
            .result()
    }

    pub fn get(&self, key: &Value) -> Result<MrbValue, Exception> {
        match self {
            Hash::Rust(x) => x.get(&key.to_rs()).map(MrbValue::Rust).map_err(Exception::from),
            Hash::Go(h) => {
                Self::go_call(*h, "mruby.Hash.Get", key, None).map(|v| v.expect("Hash.Get returned a nil *MrbValue"))
            }
        }
    }

    pub fn set(&self, key: &Value, value: &Value) -> Result<(), Exception> {
        match self {
            Hash::Rust(x) => x.set(&key.to_rs(), &value.to_rs()).map_err(Exception::from),
            Hash::Go(h) => Self::go_call(*h, "mruby.Hash.Set", key, Some(value)).map(|_| ()),
        }
    }

    /// `Delete(key)`: `None` when Go returns a nil `*MrbValue`.
    pub fn delete(&self, key: &Value) -> Result<Option<MrbValue>, Exception> {
        match self {
            Hash::Rust(x) => x.delete(&key.to_rs()).map(|v| v.map(MrbValue::Rust)).map_err(Exception::from),
            Hash::Go(h) => Self::go_call(*h, "mruby.Hash.Delete", key, None),
        }
    }

    pub fn keys(&self) -> Result<MrbValue, Exception> {
        match self {
            Hash::Rust(x) => x.keys().map(MrbValue::Rust).map_err(Exception::from),
            Hash::Go(h) => bridge::call::<_, GoFallible>("mruby.Hash.Keys", json!({"h": h})).value(),
        }
    }
}

// ---------------------------------------------------------------- Parser, CompileContext

/// `*Parser`.
#[derive(Debug)]
pub enum Parser {
    Rust(rs::Parser),
    Go(H),
}

/// `ParserError` (`*ParserError` as an `error`).
#[derive(Debug, Clone)]
pub struct ParserError {
    pub errors: Vec<ParserMessage>,
    text: ParserErrorText,
}

#[derive(Debug, Clone)]
enum ParserErrorText {
    Rust(rs::ParserError),
    Go(String),
}

impl ParserError {
    /// `err.Error()`.
    pub fn error(&self) -> String {
        match &self.text {
            ParserErrorText::Rust(e) => e.to_string(),
            ParserErrorText::Go(s) => s.clone(),
        }
    }
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.error())
    }
}

impl std::error::Error for ParserError {}

/// The zero `ParserError` (`new(ParserError)`).
pub fn parser_error_zero() -> ParserError {
    match backend() {
        Backend::Rust => ParserError { errors: vec![], text: ParserErrorText::Rust(rs::ParserError::default()) },
        Backend::Go => {
            ParserError { errors: vec![], text: ParserErrorText::Go(bridge::call("mruby.ParserError.Zero", json!({}))) }
        }
    }
}

#[derive(Deserialize)]
struct GoParserMessage {
    col: i64,
    line: i64,
    message: String,
}

fn go_messages(ms: Option<Vec<GoParserMessage>>) -> Vec<ParserMessage> {
    ms.unwrap_or_default().into_iter().map(|m| ParserMessage { col: m.col, line: m.line, message: m.message }).collect()
}

impl Parser {
    /// `NewParser(mrb)`.
    pub fn new(m: &Mrb) -> Parser {
        match m {
            Mrb::Rust(m) => Parser::Rust(rs::Parser::new(m)),
            Mrb::Go(h) => Parser::Go(bridge::call("mruby.NewParser", json!({"h": h}))),
        }
    }

    pub fn close(self) {
        match self {
            Parser::Rust(p) => p.close(),
            Parser::Go(h) => {
                bridge::call::<_, Json>("mruby.Parser.Close", json!({"h": h}));
            }
        }
    }

    /// `Parse(code, ctx)`: the warnings (empty for Go's nil) or the parse error.
    pub fn parse(&mut self, code: &str, ctx: Option<&CompileContext>) -> Result<Vec<ParserMessage>, ParserError> {
        match self {
            Parser::Rust(p) => p
                .parse(code, ctx.map(CompileContext::rs))
                .map_err(|e| ParserError { errors: e.errors.clone(), text: ParserErrorText::Rust(e) }),
            Parser::Go(h) => {
                #[derive(Deserialize)]
                struct GoErr {
                    errors: Option<Vec<GoParserMessage>>,
                    message: String,
                }
                #[derive(Deserialize)]
                struct R {
                    warnings: Option<Vec<GoParserMessage>>,
                    error: Option<GoErr>,
                }
                let r: R = bridge::call(
                    "mruby.Parser.Parse",
                    json!({"h": h, "code": code, "ctx": ctx.map(CompileContext::go)}),
                );
                match r.error {
                    Some(e) => Err(ParserError { errors: go_messages(e.errors), text: ParserErrorText::Go(e.message) }),
                    None => Ok(go_messages(r.warnings)),
                }
            }
        }
    }

    /// `GenerateCode()`.
    pub fn generate_code(&mut self) -> MrbValue {
        match self {
            Parser::Rust(p) => MrbValue::Rust(p.generate_code()),
            Parser::Go(h) => MrbValue::Go(bridge::call("mruby.Parser.GenerateCode", json!({"h": h}))),
        }
    }
}

/// `*CompileContext`.
#[derive(Debug)]
pub enum CompileContext {
    Rust(rs::CompileContext),
    Go(H),
}

impl CompileContext {
    fn rs(&self) -> &rs::CompileContext {
        match self {
            CompileContext::Rust(c) => c,
            CompileContext::Go(_) => mixed(),
        }
    }

    fn go(&self) -> H {
        match self {
            CompileContext::Go(h) => *h,
            CompileContext::Rust(_) => mixed(),
        }
    }

    /// `NewCompileContext(mrb)`.
    pub fn new(m: &Mrb) -> CompileContext {
        match m {
            Mrb::Rust(m) => CompileContext::Rust(rs::CompileContext::new(m)),
            Mrb::Go(h) => CompileContext::Go(bridge::call("mruby.NewCompileContext", json!({"h": h}))),
        }
    }

    pub fn close(self) {
        match self {
            CompileContext::Rust(c) => c.close(),
            CompileContext::Go(h) => {
                bridge::call::<_, Json>("mruby.CompileContext.Close", json!({"h": h}));
            }
        }
    }

    pub fn filename(&self) -> String {
        match self {
            CompileContext::Rust(c) => c.filename(),
            CompileContext::Go(h) => bridge::call("mruby.CompileContext.Filename", json!({"h": h})),
        }
    }

    pub fn set_filename(&mut self, name: &str) {
        match self {
            CompileContext::Rust(c) => c.set_filename(name),
            CompileContext::Go(h) => {
                bridge::call::<_, Json>("mruby.CompileContext.SetFilename", json!({"h": h, "name": name}));
            }
        }
    }

    pub fn capture_errors(&mut self, yes: bool) {
        match self {
            CompileContext::Rust(c) => c.capture_errors(yes),
            CompileContext::Go(h) => {
                bridge::call::<_, Json>("mruby.CompileContext.CaptureErrors", json!({"h": h, "yes": yes}));
            }
        }
    }
}

// ---------------------------------------------------------------- Decode

/// A Rust decode target and the Go type the bridge decodes into for it.
pub trait DecodeTarget: DeserializeOwned {
    const GO_TYPE: &'static str;
}

impl DecodeTarget for bool {
    const GO_TYPE: &'static str = "bool";
}

impl DecodeTarget for f64 {
    const GO_TYPE: &'static str = "float64";
}

impl DecodeTarget for i64 {
    const GO_TYPE: &'static str = "int";
}

impl DecodeTarget for String {
    const GO_TYPE: &'static str = "string";
}

impl DecodeTarget for HashMap<String, String> {
    const GO_TYPE: &'static str = "map[string]string";
}

impl DecodeTarget for Vec<String> {
    const GO_TYPE: &'static str = "[]string";
}

impl DecodeTarget for Box<i64> {
    const GO_TYPE: &'static str = "*int";
}

#[derive(Deserialize)]
struct GoDecoded {
    result: Json,
    error: Option<String>,
}

/// `Decode(&out, v)` into `T`; the error is Go's `err.Error()` text.
pub fn decode<T: DecodeTarget>(v: &MrbValue) -> Result<T, String> {
    match v {
        MrbValue::Rust(v) => rs::decode::<T>(v).map_err(|e| e.to_string()),
        MrbValue::Go(h) => {
            let r: GoDecoded = bridge::call("mruby.Decode", json!({"h": h, "target": T::GO_TYPE}));
            match r.error {
                Some(e) => Err(e),
                None => Ok(serde_json::from_value(r.result.clone())
                    .unwrap_or_else(|e| panic!("decoding Go {} result {}: {e}", T::GO_TYPE, r.result))),
            }
        }
    }
}

/// `Decode(&iface, v)` with `var iface interface{}`.
pub fn decode_value(v: &MrbValue) -> Result<Decoded, String> {
    match v {
        MrbValue::Rust(v) => rs::decode_value(v).map_err(|e| e.to_string()),
        MrbValue::Go(h) => {
            let r: GoDecoded = bridge::call("mruby.Decode", json!({"h": h, "target": "interface{}"}));
            match r.error {
                Some(e) => Err(e),
                None => Ok(from_tagged(&r.result)),
            }
        }
    }
}

/// Go's dynamic types, tagged by the bridge.
fn from_tagged(j: &Json) -> Decoded {
    let v = &j["v"];
    let bad = || -> ! { panic!("unexpected Go interface value {j}") };
    match j["t"].as_str() {
        Some("bool") => Decoded::Bool(v.as_bool().unwrap_or_else(|| bad())),
        Some("int") => Decoded::Int(v.as_i64().unwrap_or_else(|| bad())),
        Some("float") => Decoded::Float(v.as_f64().unwrap_or_else(|| bad())),
        Some("string") => Decoded::String(v.as_str().unwrap_or_else(|| bad()).to_owned()),
        Some("map") => Decoded::Map(
            v.as_object()
                .unwrap_or_else(|| bad())
                .iter()
                .map(|(k, x)| (k.clone(), from_tagged(x)))
                .collect::<BTreeMap<_, _>>(),
        ),
        Some("slice") => Decoded::Slice(v.as_array().unwrap_or_else(|| bad()).iter().map(from_tagged).collect()),
        _ => bad(),
    }
}
