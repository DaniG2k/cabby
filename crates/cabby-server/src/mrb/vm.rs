//! One mruby VM (go-mruby `Mrb`, `mruby.go`).

use cabby_mruby_sys as sys;

use super::{Class, Exception, MrbValue, Value, expect_ok};

/// A handle to one mruby VM (`mrb_state`). Clones refer to the same VM.
///
/// Go requires exactly one `Close`; here [`Mrb::close`] frees the VM, after which
/// every value of that VM reports [`MrbValue::is_dead`] and other operations on
/// its handles panic instead of touching freed memory.
#[derive(Debug, Clone)]
pub struct Mrb {
    pub(super) vm: sys::Vm,
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
        Mrb { vm: sys::Vm::open() }
    }

    pub(super) fn value(&self, v: sys::Val) -> MrbValue {
        MrbValue { v }
    }

    /// Frees the VM (`Close`).
    pub fn close(self) {
        self.vm.close();
    }

    pub fn arena_save(&self) -> ArenaIndex {
        ArenaIndex(self.vm.arena_save())
    }

    pub fn arena_restore(&self, idx: ArenaIndex) {
        self.vm.arena_restore(idx.0);
    }

    pub fn enable_gc(&self) {
        self.vm.set_gc_disabled(false);
    }

    pub fn disable_gc(&self) {
        self.vm.set_gc_disabled(true);
    }

    pub fn full_gc(&self) {
        self.vm.full_gc();
    }

    pub fn incremental_gc(&self) {
        self.vm.incremental_gc();
    }

    /// Number of live objects in the GC heap (`LiveObjectCount`, `gc->live`).
    pub fn live_object_count(&self) -> usize {
        self.vm.live_objects()
    }

    /// The class `name`, under `outer` or top-level (`Class`). Go aborts the
    /// process when the class does not exist; Rust returns `None`.
    pub fn class(&self, name: &str, outer: Option<&Class>) -> Option<Class> {
        self.vm.class_get(outer.map(|c| &c.v), name).ok().map(|v| Class { v })
    }

    /// The top-level module `name` (`Module`); `None` where Go aborts.
    pub fn module(&self, name: &str) -> Option<Class> {
        self.vm.module_get(name).ok().map(|v| Class { v })
    }

    pub fn const_defined(&self, name: &str, scope: &Value) -> bool {
        self.vm.const_defined(&scope.mrb_value(self).v, name).unwrap_or(false)
    }

    /// Defines a top-level class; `None` super means `Object` (`DefineClass`).
    pub fn define_class(&self, name: &str, super_class: Option<&Class>) -> Class {
        self.define_class_under(name, super_class, None)
    }

    /// Defines a class under `outer` (`Object` when `None`) (`DefineClassUnder`).
    /// Panics where mruby raises (e.g. a superclass mismatch), which go-mruby
    /// does not handle either.
    pub fn define_class_under(&self, name: &str, super_class: Option<&Class>, outer: Option<&Class>) -> Class {
        let object = self.vm.object_class();
        let super_class = super_class.map_or(&object, |c| &c.v);
        let outer = outer.map_or(&object, |c| &c.v);
        Class { v: expect_ok(self.vm.define_class(outer, name, super_class), "DefineClass") }
    }

    pub fn define_module(&self, name: &str) -> Class {
        self.define_module_under(name, None)
    }

    pub fn define_module_under(&self, name: &str, outer: Option<&Class>) -> Class {
        let object = self.vm.object_class();
        let outer = outer.map_or(&object, |c| &c.v);
        Class { v: expect_ok(self.vm.define_module(outer, name), "DefineModule") }
    }

    pub fn object_class(&self) -> Class {
        Class { v: self.vm.object_class() }
    }

    pub fn kernel_module(&self) -> Class {
        Class { v: self.vm.kernel_module() }
    }

    pub fn top_self(&self) -> MrbValue {
        self.value(self.vm.top_self())
    }

    pub fn false_value(&self) -> MrbValue {
        self.value(self.vm.false_value())
    }

    pub fn nil_value(&self) -> MrbValue {
        self.value(self.vm.nil())
    }

    pub fn true_value(&self) -> MrbValue {
        self.value(self.vm.true_value())
    }

    /// A Fixnum. As go-mruby, the number goes through mruby's 32-bit `mrb_int`.
    pub fn fixnum_value(&self, n: i64) -> MrbValue {
        self.value(self.vm.fixnum(n))
    }

    pub fn string_value(&self, s: &str) -> MrbValue {
        self.value(self.vm.string(s))
    }

    pub fn get_global_variable(&self, name: &str) -> MrbValue {
        self.value(expect_ok(self.vm.gv_get(name), "GetGlobalVariable"))
    }

    pub fn set_global_variable(&self, name: &str, value: &Value) {
        expect_ok(self.vm.gv_set(name, &value.mrb_value(self).v), "SetGlobalVariable");
    }

    /// Arguments of the Rust function currently called from Ruby (`GetArgs`).
    pub fn get_args(&self) -> Vec<MrbValue> {
        self.vm.get_args().into_iter().map(|v| MrbValue { v }).collect()
    }

    /// Parses and runs `code`; a raised exception is returned and cleared, so the
    /// VM stays usable (`LoadString`).
    pub fn load_string(&self, code: &str) -> Result<MrbValue, Exception> {
        self.vm.load_string(code).map(|v| MrbValue { v }).map_err(Exception::from_raised)
    }

    /// Runs a proc with `self_value` (top-level self when `None`) (`Run`).
    pub fn run(&self, proc: &Value, self_value: Option<&Value>) -> Result<MrbValue, Exception> {
        let proc = proc.mrb_value(self);
        let self_value = self_value.map_or_else(|| self.top_self(), |s| s.mrb_value(self));
        self.vm.run(&proc.v, &self_value.v).map(|v| MrbValue { v }).map_err(Exception::from_raised)
    }

    /// Runs a proc keeping `stack_keep` stack slots, returning the new stack
    /// size so local variables carry over (`RunWithContext`). Go also returns the
    /// unchanged `stack_keep` on error; the caller already has it.
    pub fn run_with_context(
        &self,
        proc: &Value,
        self_value: Option<&Value>,
        stack_keep: usize,
    ) -> Result<(usize, MrbValue), Exception> {
        let proc = proc.mrb_value(self);
        let self_value = self_value.map_or_else(|| self.top_self(), |s| s.mrb_value(self));
        let keep = u32::try_from(stack_keep).unwrap_or(u32::MAX);
        let (nlocals, v) = self.vm.context_run(&proc.v, &self_value.v, keep).map_err(Exception::from_raised)?;
        Ok((usize::try_from(nlocals).unwrap_or(usize::MAX), MrbValue { v }))
    }

    /// Yields `args` to `block` from inside a Rust function (`Yield`).
    pub fn yield_block(&self, block: &Value, args: &[Value]) -> Result<MrbValue, Exception> {
        let block = block.mrb_value(self);
        let args: Vec<_> = args.iter().map(|a| a.mrb_value(self).v).collect();
        self.vm.yield_block(&block.v, &args).map(|v| MrbValue { v }).map_err(Exception::from_raised)
    }
}
