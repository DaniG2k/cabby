//! One mruby VM (go-mruby `Mrb`, `mruby.go`).

use cabby_core::pending;

use super::{Class, Exception, MrbValue, NotSend, Value};

/// A handle to one mruby VM (`mrb_state`). Clones refer to the same VM.
///
/// Go requires exactly one `Close`; here [`Mrb::close`] frees the VM, after which
/// every value of that VM reports [`MrbValue::is_dead`] and other operations on
/// its handles panic instead of touching freed memory.
#[derive(Debug, Clone)]
pub struct Mrb {
    _vm: NotSend,
}

/// Index into the GC arena (`mrb_gc_arena_save`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArenaIndex(pub i32);

impl Default for Mrb {
    fn default() -> Self {
        Self::new()
    }
}

impl Mrb {
    /// Opens a new VM (`NewMrb`, `mrb_open`).
    pub fn new() -> Mrb {
        pending!("mrb::Mrb::new")
    }

    /// Frees the VM (`Close`).
    pub fn close(self) {
        pending!("mrb::Mrb::close")
    }

    pub fn arena_save(&self) -> ArenaIndex {
        pending!("mrb::Mrb::arena_save")
    }

    pub fn arena_restore(&self, _idx: ArenaIndex) {
        pending!("mrb::Mrb::arena_restore")
    }

    pub fn enable_gc(&self) {
        pending!("mrb::Mrb::enable_gc")
    }

    pub fn disable_gc(&self) {
        pending!("mrb::Mrb::disable_gc")
    }

    pub fn full_gc(&self) {
        pending!("mrb::Mrb::full_gc")
    }

    pub fn incremental_gc(&self) {
        pending!("mrb::Mrb::incremental_gc")
    }

    /// Number of live objects in the GC heap (`LiveObjectCount`, `gc->live`).
    pub fn live_object_count(&self) -> usize {
        pending!("mrb::Mrb::live_object_count")
    }

    /// The class `name`, under `outer` or top-level (`Class`). Go aborts the
    /// process when the class does not exist; Rust returns `None`.
    pub fn class(&self, _name: &str, _outer: Option<&Class>) -> Option<Class> {
        pending!("mrb::Mrb::class")
    }

    /// The top-level module `name` (`Module`); `None` where Go aborts.
    pub fn module(&self, _name: &str) -> Option<Class> {
        pending!("mrb::Mrb::module")
    }

    pub fn const_defined(&self, _name: &str, _scope: &Value) -> bool {
        pending!("mrb::Mrb::const_defined")
    }

    /// Defines a top-level class; `None` super means `Object` (`DefineClass`).
    pub fn define_class(&self, _name: &str, _super_class: Option<&Class>) -> Class {
        pending!("mrb::Mrb::define_class")
    }

    /// Defines a class under `outer` (`Object` when `None`) (`DefineClassUnder`).
    pub fn define_class_under(&self, _name: &str, _super_class: Option<&Class>, _outer: Option<&Class>) -> Class {
        pending!("mrb::Mrb::define_class_under")
    }

    pub fn define_module(&self, _name: &str) -> Class {
        pending!("mrb::Mrb::define_module")
    }

    pub fn define_module_under(&self, _name: &str, _outer: Option<&Class>) -> Class {
        pending!("mrb::Mrb::define_module_under")
    }

    pub fn object_class(&self) -> Class {
        pending!("mrb::Mrb::object_class")
    }

    pub fn kernel_module(&self) -> Class {
        pending!("mrb::Mrb::kernel_module")
    }

    pub fn top_self(&self) -> MrbValue {
        pending!("mrb::Mrb::top_self")
    }

    pub fn false_value(&self) -> MrbValue {
        pending!("mrb::Mrb::false_value")
    }

    pub fn nil_value(&self) -> MrbValue {
        pending!("mrb::Mrb::nil_value")
    }

    pub fn true_value(&self) -> MrbValue {
        pending!("mrb::Mrb::true_value")
    }

    pub fn fixnum_value(&self, _n: i64) -> MrbValue {
        pending!("mrb::Mrb::fixnum_value")
    }

    pub fn string_value(&self, _s: &str) -> MrbValue {
        pending!("mrb::Mrb::string_value")
    }

    pub fn get_global_variable(&self, _name: &str) -> MrbValue {
        pending!("mrb::Mrb::get_global_variable")
    }

    pub fn set_global_variable(&self, _name: &str, _value: &Value) {
        pending!("mrb::Mrb::set_global_variable")
    }

    /// Arguments of the Rust function currently called from Ruby (`GetArgs`).
    pub fn get_args(&self) -> Vec<MrbValue> {
        pending!("mrb::Mrb::get_args")
    }

    /// Parses and runs `code`; a raised exception is returned and cleared, so the
    /// VM stays usable (`LoadString`).
    pub fn load_string(&self, _code: &str) -> Result<MrbValue, Exception> {
        pending!("mrb::Mrb::load_string")
    }

    /// Runs a proc with `self_value` (top-level self when `None`) (`Run`).
    pub fn run(&self, _proc: &Value, _self_value: Option<&Value>) -> Result<MrbValue, Exception> {
        pending!("mrb::Mrb::run")
    }

    /// Runs a proc keeping `stack_keep` stack slots, returning the new stack
    /// size so local variables carry over (`RunWithContext`). Go also returns the
    /// unchanged `stack_keep` on error; the caller already has it.
    pub fn run_with_context(
        &self,
        _proc: &Value,
        _self_value: Option<&Value>,
        _stack_keep: usize,
    ) -> Result<(usize, MrbValue), Exception> {
        pending!("mrb::Mrb::run_with_context")
    }

    /// Yields `args` to `block` from inside a Rust function (`Yield`).
    pub fn yield_block(&self, _block: &Value, _args: &[Value]) -> Result<MrbValue, Exception> {
        pending!("mrb::Mrb::yield_block")
    }
}
