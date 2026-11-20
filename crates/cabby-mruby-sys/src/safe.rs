//! Safe handles over the C shim. Every `unsafe` block here calls one shim
//! function; the invariants that make those calls sound are:
//!
//! - **open VM**: `Vm::state` returns the `mrb_state` only while the VM is
//!   open (it panics after `close`), so no call reaches a freed state;
//! - **valid values**: `Val::checked` passes a value to C only if it belongs to
//!   the VM of the call and is an immediate or a live heap object of its
//!   recorded type (`cabby_value_live`, which never dereferences a pointer
//!   outside the VM's live heap pages);
//! - **valid pointers**: out-parameters are local `mrb_value`s, strings are
//!   `CString`s that outlive the call, argument arrays are slices of checked
//!   values that outlive the call;
//! - **no unwinding into C**: Rust methods run under `catch_unwind` in
//!   [`dispatch`]; C never unwinds into Rust because the shim catches raises.
//!
//! The shim adds what a liveness check before the call cannot give (see the
//! "protected calls" comment in `shim/cabby_mruby.c`): values stay rooted
//! while the call runs, a raise pops the call frames it leaves behind, procs
//! without an environment never reach Ruby code, backtraces read only call
//! frames whose procs are still alive, `run` inside a Rust method gets a call
//! frame of its own, stack keeps cover rooted slots only, and `Fiber#resume`
//! called from C is refused. Here, `arena_restore` inside a Rust method stops
//! at the arena index the method was entered with.

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::rc::Rc;
use std::sync::Once;

use crate::ffi::{self, mrb_parser_state, mrb_state, mrb_value, mrbc_context};

/// A method implemented in Rust: receives the VM and `self`; returns the
/// result (`None` is nil) or an exception value to raise.
pub type Method = Rc<dyn Fn(&Vm, &Val) -> Result<Option<Val>, Val>>;

/// `mrb_value` type tag of nil as go-mruby reports it.
const NIL_TYPE: u32 = 0xffff_ffff;

struct VmInner {
    /// Null once closed.
    state: Cell<*mut mrb_state>,
    /// Rust methods by (holder class identity, method symbol), as go-mruby's
    /// per-state method table.
    methods: RefCell<HashMap<(u64, u32), Method>>,
    /// A panic of a Rust method, re-raised when control returns to Rust.
    panic: RefCell<Option<Box<dyn Any + Send>>>,
    /// Number of Rust methods running on this VM.
    depth: Cell<u32>,
    /// The arena index when the innermost running Rust method was entered:
    /// `arena_restore` never goes below it, because the C code that called
    /// the method may keep unrooted objects in the arena below that point.
    arena_floor: Cell<i32>,
}

impl Drop for VmInner {
    fn drop(&mut self) {
        let state = self.state.replace(std::ptr::null_mut());
        if !state.is_null() {
            // SAFETY: `state` came from `cabby_open` and was not closed yet
            // (closing nulls it); no handle can reach it after this point.
            unsafe { ffi::cabby_close(state) };
        }
    }
}

/// A handle to one mruby VM. Clones share the VM. Not `Send`: an mruby VM
/// belongs to one thread.
#[derive(Clone)]
pub struct Vm {
    inner: Rc<VmInner>,
}

impl std::fmt::Debug for Vm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vm").field("open", &!self.is_closed()).finish()
    }
}

/// A value of one VM.
#[derive(Clone)]
pub struct Val {
    vm: Vm,
    raw: mrb_value,
}

impl std::fmt::Debug for Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Val").field("type", &self.type_tag()).finish()
    }
}

/// An exception raised by a VM call, already cleared from the VM (go-mruby
/// `checkException` + `newExceptionValue`): its object, `to_s` and backtrace.
#[derive(Debug, Clone)]
pub struct Raised {
    /// The exception object.
    pub value: Val,
    /// Its `to_s`.
    pub message: String,
    /// Its backtrace lines.
    pub backtrace: Vec<String>,
}

/// A parser warning or error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserMessage {
    /// Line number.
    pub line: i64,
    /// Column.
    pub col: i64,
    /// The message text.
    pub message: String,
}

/// A C string as go-mruby's `C.CString` + `strlen` sees it: cut at the first
/// NUL byte.
fn cstring(s: &str) -> CString {
    let bytes = s.as_bytes();
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    CString::new(&bytes[..end]).expect("no NUL before the cut")
}

/// Copies C bytes up to the first NUL (go-mruby `C.GoString`).
///
/// # Safety
/// `ptr` must be valid for reads of `len` bytes (or null with `len == 0`).
unsafe fn copy_c_bytes(ptr: *const c_char, len: usize) -> String {
    if ptr.is_null() || len == 0 {
        return String::new();
    }
    // SAFETY: guaranteed by the caller.
    let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// Reads a NUL-terminated C string owned by the VM.
///
/// # Safety
/// `ptr` must be null or a valid NUL-terminated string.
unsafe fn copy_c_str(ptr: *const c_char) -> String {
    if ptr.is_null() {
        return String::new();
    }
    // SAFETY: guaranteed by the caller.
    unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned()
}

fn install_dispatch() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // SAFETY: stores a function pointer in a C static before any VM runs a
        // Rust method; `dispatch` matches `ffi::DispatchFn`.
        unsafe { ffi::cabby_set_dispatch(dispatch) };
    });
}

impl Vm {
    /// Opens a VM (`mrb_open`). Panics when mruby is not compiled in or the
    /// VM cannot be allocated.
    pub fn open() -> Vm {
        assert!(crate::available(), "mruby is not compiled in (cargo feature `vendored` on Linux/macOS)");
        install_dispatch();
        // SAFETY: plain constructor; returns null on failure.
        let state = unsafe { ffi::cabby_open() };
        assert!(!state.is_null(), "mrb_open failed");
        let inner = Rc::new(VmInner {
            state: Cell::new(state),
            methods: RefCell::default(),
            panic: RefCell::default(),
            depth: Cell::new(0),
            arena_floor: Cell::new(0),
        });
        // SAFETY: `state` is open; `ud` stores a pointer to the VmInner that
        // owns it, which outlives the state (Drop closes the state first).
        unsafe { ffi::cabby_set_ud(state, Rc::as_ptr(&inner).cast_mut().cast::<c_void>()) };
        Vm { inner }
    }

    /// The open state; panics once closed.
    fn state(&self) -> *mut mrb_state {
        let state = self.inner.state.get();
        assert!(!state.is_null(), "the mruby VM is closed");
        state
    }

    /// Whether the VM was closed.
    pub fn is_closed(&self) -> bool {
        self.inner.state.get().is_null()
    }

    /// Whether both handles denote the same VM.
    pub fn same(&self, other: &Vm) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }

    /// Frees the VM (`mrb_close`). Later operations on its handles panic and
    /// its values report dead. Closing twice is a no-op. Panics when called
    /// from a Rust method running on this VM.
    pub fn close(&self) {
        assert!(self.inner.depth.get() == 0, "cannot close an mruby VM from inside one of its methods");
        let state = self.inner.state.replace(std::ptr::null_mut());
        if state.is_null() {
            return;
        }
        let methods = std::mem::take(&mut *self.inner.methods.borrow_mut());
        // SAFETY: `state` was open and is no longer reachable through any handle.
        unsafe { ffi::cabby_close(state) };
        drop(methods);
    }

    fn wrap(&self, raw: mrb_value) -> Val {
        Val { vm: self.clone(), raw }
    }

    /// Re-raises a pending panic of a Rust method, then collects a pending
    /// exception. Called after every shim call that may run Ruby or raise.
    fn finish(&self, ok: c_int) -> Result<(), Raised> {
        let raised = self.take_raised();
        if let Some(payload) = self.inner.panic.borrow_mut().take() {
            resume_unwind(payload);
        }
        match raised {
            Some(r) => Err(r),
            None if ok == 0 => {
                Err(Raised { value: self.nil(), message: "unknown mruby error".into(), backtrace: vec![] })
            }
            None => Ok(()),
        }
    }

    /// go-mruby `newExceptionValue`: takes `mrb->exc` (clearing it) and reads
    /// its backtrace and message inside a saved arena.
    fn take_raised(&self) -> Option<Raised> {
        let state = self.state();
        let ai = self.arena_save();
        let mut exc = mrb_value::zeroed();
        // SAFETY: open state, local out-parameter.
        if unsafe { ffi::cabby_take_exc(state, &mut exc) } == 0 {
            return None;
        }
        let value = self.wrap(exc);
        let mut backtrace = Vec::new();
        let mut bt = mrb_value::zeroed();
        // SAFETY: open state; `exc` is the live exception just taken (it is in
        // the arena); local out-parameter.
        if unsafe { ffi::cabby_exc_backtrace(state, &exc, &mut bt) } != 0 {
            let bt = self.wrap(bt);
            for i in 0..bt.ary_len() {
                let entry = self.quiet(bt.ary_entry_raw(i64::try_from(i).unwrap_or(i64::MAX)));
                backtrace.push(entry.and_then(|e| self.quiet(e.to_s_raw())).unwrap_or_default());
            }
        } else {
            self.clear_exc();
        }
        let message = self.quiet(value.to_s_raw()).unwrap_or_default();
        self.arena_restore(ai);
        Some(Raised { value, message, backtrace })
    }

    /// Drops a pending exception without inspecting it.
    fn clear_exc(&self) {
        let mut exc = mrb_value::zeroed();
        // SAFETY: open state, local out-parameter.
        unsafe { ffi::cabby_take_exc(self.state(), &mut exc) };
    }

    /// The value of an internal call, discarding (not reporting) a raise.
    fn quiet<T>(&self, r: (c_int, T)) -> Option<T> {
        if r.0 == 0 {
            self.clear_exc();
            None
        } else {
            Some(r.1)
        }
    }

    // ------------------------------------------------------------ GC

    /// `mrb_gc_arena_save`.
    pub fn arena_save(&self) -> i32 {
        // SAFETY: open state.
        unsafe { ffi::cabby_arena_save(self.state()) }
    }

    /// `mrb_gc_arena_restore`; an index above the current one is ignored.
    /// Inside a Rust method, an index below the arena index at the method's
    /// entry restores to that index instead: the C caller of the method may
    /// rely on objects it left in the arena.
    pub fn arena_restore(&self, idx: i32) {
        let idx = idx.max(self.inner.arena_floor.get());
        // SAFETY: open state; the shim ignores indexes above the current one.
        let ok = unsafe { ffi::cabby_arena_restore(self.state(), idx) };
        self.quiet((ok, ()));
    }

    /// Disables or enables the collector (`gc->disabled`).
    pub fn set_gc_disabled(&self, disabled: bool) {
        // SAFETY: open state.
        unsafe { ffi::cabby_gc_set_disabled(self.state(), c_int::from(disabled)) };
    }

    /// `mrb_full_gc`.
    pub fn full_gc(&self) {
        // SAFETY: open state.
        let ok = unsafe { ffi::cabby_full_gc(self.state()) };
        self.quiet((ok, ()));
    }

    /// `mrb_incremental_gc`.
    pub fn incremental_gc(&self) {
        // SAFETY: open state.
        let ok = unsafe { ffi::cabby_incremental_gc(self.state()) };
        self.quiet((ok, ()));
    }

    /// `gc->live`.
    pub fn live_objects(&self) -> usize {
        // SAFETY: open state.
        unsafe { ffi::cabby_gc_live(self.state()) }
    }

    // ------------------------------------------------------------ values

    /// `nil`.
    pub fn nil(&self) -> Val {
        let mut out = mrb_value::zeroed();
        // SAFETY: writes the local out-parameter only.
        unsafe { ffi::cabby_nil(&mut out) };
        self.wrap(out)
    }

    /// `true`.
    pub fn true_value(&self) -> Val {
        let mut out = mrb_value::zeroed();
        // SAFETY: writes the local out-parameter only.
        unsafe { ffi::cabby_true(&mut out) };
        self.wrap(out)
    }

    /// `false`.
    pub fn false_value(&self) -> Val {
        let mut out = mrb_value::zeroed();
        // SAFETY: writes the local out-parameter only.
        unsafe { ffi::cabby_false(&mut out) };
        self.wrap(out)
    }

    /// A Fixnum (converted through mruby's 32-bit `mrb_int`, as go-mruby).
    pub fn fixnum(&self, n: i64) -> Val {
        let mut out = mrb_value::zeroed();
        // SAFETY: writes the local out-parameter only.
        unsafe { ffi::cabby_fixnum(n, &mut out) };
        self.wrap(out)
    }

    /// A String (cut at the first NUL, as go-mruby's `C.CString`).
    pub fn string(&self, s: &str) -> Val {
        let c = cstring(s);
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; `c` is valid for `len` bytes during the call.
        let ok = unsafe { ffi::cabby_str_new(self.state(), c.as_ptr(), c.as_bytes().len(), &mut out) };
        match self.finish(ok) {
            Ok(()) => self.wrap(out),
            Err(e) => panic!("creating an mruby string: {}", e.message),
        }
    }

    /// `Object`.
    pub fn object_class(&self) -> Val {
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; reads a permanent object of the state.
        unsafe { ffi::cabby_object_class(self.state(), &mut out) };
        self.wrap(out)
    }

    /// `Kernel`.
    pub fn kernel_module(&self) -> Val {
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; reads a permanent object of the state.
        unsafe { ffi::cabby_kernel_module(self.state(), &mut out) };
        self.wrap(out)
    }

    /// The top-level `self`.
    pub fn top_self(&self) -> Val {
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; reads a permanent object of the state.
        unsafe { ffi::cabby_top_self(self.state(), &mut out) };
        self.wrap(out)
    }

    // ------------------------------------------------------------ classes

    /// `mrb_class_get` / `mrb_class_get_under(outer)`.
    pub fn class_get(&self, outer: Option<&Val>, name: &str) -> Result<Val, Raised> {
        let c = cstring(name);
        let outer = outer.map(|o| o.checked(self));
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; `outer` is null or a checked value of this VM;
        // `c` outlives the call; local out-parameter.
        let ok = unsafe { ffi::cabby_class_get(self.state(), outer.unwrap_or(std::ptr::null()), c.as_ptr(), &mut out) };
        self.finish(ok).map(|()| self.wrap(out))
    }

    /// `mrb_module_get`.
    pub fn module_get(&self, name: &str) -> Result<Val, Raised> {
        let c = cstring(name);
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; `c` outlives the call; local out-parameter.
        let ok = unsafe { ffi::cabby_module_get(self.state(), c.as_ptr(), &mut out) };
        self.finish(ok).map(|()| self.wrap(out))
    }

    /// `mrb_const_defined`; false when `scope` is not a class or module.
    pub fn const_defined(&self, scope: &Val, name: &str) -> Result<bool, Raised> {
        let c = cstring(name);
        let mut out: c_int = 0;
        // SAFETY: open state; `scope` checked; `c` outlives the call.
        let ok = unsafe { ffi::cabby_const_defined(self.state(), scope.checked(self), c.as_ptr(), &mut out) };
        self.finish(ok).map(|()| out != 0)
    }

    /// `mrb_define_class_under(outer, name, super)`.
    pub fn define_class(&self, outer: &Val, name: &str, superclass: &Val) -> Result<Val, Raised> {
        let c = cstring(name);
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; both values checked; `c` outlives the call.
        let ok = unsafe {
            ffi::cabby_define_class(self.state(), outer.checked(self), c.as_ptr(), superclass.checked(self), &mut out)
        };
        self.finish(ok).map(|()| self.wrap(out))
    }

    /// `mrb_define_module_under(outer, name)`.
    pub fn define_module(&self, outer: &Val, name: &str) -> Result<Val, Raised> {
        let c = cstring(name);
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; `outer` checked; `c` outlives the call.
        let ok = unsafe { ffi::cabby_define_module(self.state(), outer.checked(self), c.as_ptr(), &mut out) };
        self.finish(ok).map(|()| self.wrap(out))
    }

    /// `mrb_define_const`.
    pub fn define_const(&self, cls: &Val, name: &str, value: &Val) -> Result<(), Raised> {
        let c = cstring(name);
        // SAFETY: open state; both values checked; `c` outlives the call.
        let ok = unsafe { ffi::cabby_define_const(self.state(), cls.checked(self), c.as_ptr(), value.checked(self)) };
        self.finish(ok)
    }

    /// Defines a method (a class method on `cls`'s singleton class when
    /// `class_method`) implemented by `f`.
    pub fn define_method(
        &self,
        cls: &Val,
        name: &str,
        aspec: u32,
        class_method: bool,
        f: Method,
    ) -> Result<(), Raised> {
        let c = cstring(name);
        let mut holder = mrb_value::zeroed();
        let mut sym: u32 = 0;
        // SAFETY: open state; `cls` checked; `c` outlives the call; locals.
        let ok = unsafe {
            ffi::cabby_define_method(
                self.state(),
                cls.checked(self),
                c.as_ptr(),
                aspec,
                c_int::from(class_method),
                &mut holder,
                &mut sym,
            )
        };
        self.finish(ok)?;
        // SAFETY: reads the tag and pointer bits of a local value.
        let holder = unsafe { ffi::cabby_identity(&holder) };
        self.inner.methods.borrow_mut().insert((holder, sym), f);
        Ok(())
    }

    // ------------------------------------------------------------ variables

    /// `mrb_gv_get`.
    pub fn gv_get(&self, name: &str) -> Result<Val, Raised> {
        let c = cstring(name);
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; `c` outlives the call; local out-parameter.
        let ok = unsafe { ffi::cabby_gv_get(self.state(), c.as_ptr(), &mut out) };
        self.finish(ok).map(|()| self.wrap(out))
    }

    /// `mrb_gv_set`.
    pub fn gv_set(&self, name: &str, value: &Val) -> Result<(), Raised> {
        let c = cstring(name);
        // SAFETY: open state; `value` checked; `c` outlives the call.
        let ok = unsafe { ffi::cabby_gv_set(self.state(), c.as_ptr(), value.checked(self)) };
        self.finish(ok)
    }

    // ------------------------------------------------------------ running code

    /// Arguments of the running Rust method (go-mruby `GetArgs`:
    /// `mrb_get_args(mrb, "*&?")`, the block last when given). Empty when no
    /// Rust method of this VM is running.
    pub fn get_args(&self) -> Vec<Val> {
        if self.inner.depth.get() == 0 {
            return Vec::new();
        }
        let mut argv: *const mrb_value = std::ptr::null();
        let mut argc: i64 = 0;
        let mut block = mrb_value::zeroed();
        let mut has_block: c_int = 0;
        // SAFETY: open state; a Rust method is running, so the current call
        // frame is a method call; locals as out-parameters.
        let ok = unsafe { ffi::cabby_get_args(self.state(), &mut argv, &mut argc, &mut block, &mut has_block) };
        if self.finish(ok).is_err() {
            return Vec::new();
        }
        let argc = usize::try_from(argc).unwrap_or(0);
        let mut out = Vec::with_capacity(argc + 1);
        if argc > 0 && !argv.is_null() {
            // SAFETY: mrb_get_args("*") returned `argc` values at `argv` (VM
            // stack or array storage); nothing ran since, and they are copied
            // before any other call.
            let args = unsafe { std::slice::from_raw_parts(argv, argc) };
            out.extend(args.iter().map(|v| self.wrap(*v)));
        }
        if has_block != 0 {
            out.push(self.wrap(block));
        }
        out
    }

    /// `mrb_load_string` (result kept in the arena, as go-mruby).
    pub fn load_string(&self, code: &str) -> Result<Val, Raised> {
        let c = cstring(code);
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; `c` outlives the call; local out-parameter.
        let ok = unsafe { ffi::cabby_load_string(self.state(), c.as_ptr(), &mut out) };
        self.finish(ok).map(|()| self.wrap(out))
    }

    /// `mrb_run(proc, self)`. Inside a Rust method the proc runs on a call
    /// frame of its own (`mrb_run` would reuse the method's frame).
    pub fn run(&self, proc_: &Val, self_: &Val) -> Result<Val, Raised> {
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; both values checked (the shim checks that `proc_`
        // is a Ruby proc); local out-parameter.
        let ok = unsafe { ffi::cabby_run(self.state(), proc_.checked(self), self_.checked(self), &mut out) };
        self.finish(ok).map(|()| self.wrap(out))
    }

    /// `mrb_context_run(proc, self, keep)`, returning the proc's local count
    /// (the keep for the next run) with the result.
    ///
    /// Only stack slots that the collector still roots may be kept: the shim
    /// limits `keep` to the root frame's register count (slots above it may
    /// hold collected objects). Inside a Rust method the proc runs on a call
    /// frame of its own and `keep` is ignored.
    pub fn context_run(&self, proc_: &Val, self_: &Val, keep: u32) -> Result<(u32, Val), Raised> {
        let mut nlocals: u32 = 0;
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; both values checked; the shim limits `keep` to
        // rooted stack slots; locals as out-parameters.
        let ok = unsafe {
            ffi::cabby_context_run(self.state(), proc_.checked(self), self_.checked(self), keep, &mut nlocals, &mut out)
        };
        self.finish(ok).map(|()| (nlocals, self.wrap(out)))
    }

    /// `mrb_yield_argv(block, args)` (result kept in the arena, as go-mruby).
    /// The block must be a proc with an environment (a block or `proc {}` from
    /// Ruby code); anything else raises (`ArgumentError`/`TypeError`).
    pub fn yield_block(&self, block: &Val, args: &[Val]) -> Result<Val, Raised> {
        let argv: Vec<mrb_value> = args.iter().map(|a| a.checked_copy(self)).collect();
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; `block` and every argument checked; `argv`
        // outlives the call; local out-parameter.
        let ok = unsafe { ffi::cabby_yield(self.state(), block.checked(self), argc(&argv), argv_ptr(&argv), &mut out) };
        self.finish(ok).map(|()| self.wrap(out))
    }
}

fn argc(argv: &[mrb_value]) -> i64 {
    i64::try_from(argv.len()).expect("argument count fits i64")
}

fn argv_ptr(argv: &[mrb_value]) -> *const mrb_value {
    if argv.is_empty() { std::ptr::null() } else { argv.as_ptr() }
}

impl Val {
    /// The value as a pointer for C, after checking that it belongs to `vm`,
    /// which is open, and that it is an immediate or a live object of its
    /// recorded type. Panics otherwise.
    fn checked(&self, vm: &Vm) -> *const mrb_value {
        assert!(self.vm.same(vm), "mruby value used with another VM");
        assert!(self.live(), "mruby value was garbage collected");
        &self.raw
    }

    fn checked_copy(&self, vm: &Vm) -> mrb_value {
        self.checked(vm);
        self.raw
    }

    fn live(&self) -> bool {
        // SAFETY: open state (`state` panics otherwise); the shim only
        // compares the pointer with live heap pages before reading it.
        unsafe { ffi::cabby_value_live(self.vm.state(), &self.raw) != 0 }
    }

    /// The VM of this value.
    pub fn vm(&self) -> Vm {
        self.vm.clone()
    }

    /// The mruby type tag (`enum mrb_vtype`), with nil as `0xffffffff`.
    pub fn type_tag(&self) -> u32 {
        // SAFETY: reads the tag (and, for false, the payload) of this value's
        // own bytes; no pointer is followed.
        unsafe { ffi::cabby_type(&self.raw) }
    }

    /// Whether this is nil.
    pub fn is_nil(&self) -> bool {
        self.type_tag() == NIL_TYPE
    }

    /// The integer payload through a C `int` (go-mruby `Fixnum`).
    pub fn fixnum(&self) -> i64 {
        // SAFETY: reads this value's own payload bytes.
        unsafe { ffi::cabby_fixnum_of(&self.raw) }
    }

    /// The float payload through a C `float` (go-mruby `Float`).
    pub fn float(&self) -> f64 {
        // SAFETY: reads this value's own payload bytes.
        unsafe { ffi::cabby_float_of(&self.raw) }
    }

    /// Whether the object was collected, or its VM closed (go-mruby `IsDead`).
    pub fn is_dead(&self) -> bool {
        self.vm.is_closed() || !self.live()
    }

    /// Whether both handles denote the same value of the same VM.
    pub fn same(&self, other: &Val) -> bool {
        // SAFETY: compares the two values' own bytes; no pointer is followed.
        self.vm.same(&other.vm) && unsafe { ffi::cabby_same(&self.raw, &other.raw) != 0 }
    }

    /// Identity of a heap object (0 for immediates).
    pub fn identity(&self) -> u64 {
        // SAFETY: reads this value's own bytes.
        unsafe { ffi::cabby_identity(&self.raw) }
    }

    fn to_s_raw(&self) -> (c_int, String) {
        let vm = &self.vm;
        let mut s = mrb_value::zeroed();
        let mut ptr: *const c_char = std::ptr::null();
        let mut len: usize = 0;
        // SAFETY: open state; `self` checked; locals as out-parameters.
        let ok = unsafe { ffi::cabby_to_s(vm.state(), self.checked(vm), &mut s, &mut ptr, &mut len) };
        if ok == 0 {
            return (0, String::new());
        }
        // SAFETY: on success `ptr` points to `len` bytes of the string `s`,
        // which nothing has touched since.
        (1, unsafe { copy_c_bytes(ptr, len) })
    }

    /// `to_s` (go-mruby `String()`, cut at the first NUL).
    pub fn to_s(&self) -> Result<String, Raised> {
        let (ok, s) = self.to_s_raw();
        self.vm.finish(ok).map(|()| s)
    }

    /// Calls `name` with `args` and an optional block (a proc with an
    /// environment, or nil). A proc without an environment (from
    /// [`Parser::generate_code`]) as receiver, argument or block raises
    /// `TypeError`: it can only be run.
    pub fn call(&self, name: &str, args: &[Val], block: Option<&Val>) -> Result<Val, Raised> {
        let vm = &self.vm;
        let c = cstring(name);
        let argv: Vec<mrb_value> = args.iter().map(|a| a.checked_copy(vm)).collect();
        let block = block.map(|b| b.checked(vm));
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; receiver, arguments and block checked; `c` and
        // `argv` outlive the call; local out-parameter.
        let ok = unsafe {
            ffi::cabby_funcall(
                vm.state(),
                self.checked(vm),
                c.as_ptr(),
                argc(&argv),
                argv_ptr(&argv),
                block.unwrap_or(std::ptr::null()),
                &mut out,
            )
        };
        vm.finish(ok).map(|()| vm.wrap(out))
    }

    /// `mrb_iv_get` (nil for values without instance variables).
    pub fn iv_get(&self, name: &str) -> Result<Val, Raised> {
        let vm = &self.vm;
        let c = cstring(name);
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; `self` checked; `c` outlives the call.
        let ok = unsafe { ffi::cabby_iv_get(vm.state(), self.checked(vm), c.as_ptr(), &mut out) };
        vm.finish(ok).map(|()| vm.wrap(out))
    }

    /// `mrb_iv_set`.
    pub fn iv_set(&self, name: &str, value: &Val) -> Result<(), Raised> {
        let vm = &self.vm;
        let c = cstring(name);
        // SAFETY: open state; both values checked; `c` outlives the call.
        let ok = unsafe { ffi::cabby_iv_set(vm.state(), self.checked(vm), c.as_ptr(), value.checked(vm)) };
        vm.finish(ok)
    }

    /// `mrb_class` (the class, skipping singleton classes).
    pub fn class_of(&self) -> Result<Val, Raised> {
        let vm = &self.vm;
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; `self` checked; local out-parameter.
        let ok = unsafe { ffi::cabby_class_of(vm.state(), self.checked(vm), &mut out) };
        vm.finish(ok).map(|()| vm.wrap(out))
    }

    /// `mrb_singleton_class`.
    pub fn singleton_class(&self) -> Result<Val, Raised> {
        let vm = &self.vm;
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; `self` checked; local out-parameter.
        let ok = unsafe { ffi::cabby_singleton_class(vm.state(), self.checked(vm), &mut out) };
        vm.finish(ok).map(|()| vm.wrap(out))
    }

    /// `mrb_obj_new(self, args)` for a class.
    pub fn obj_new(&self, args: &[Val]) -> Result<Val, Raised> {
        let vm = &self.vm;
        let argv: Vec<mrb_value> = args.iter().map(|a| a.checked_copy(vm)).collect();
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; class and arguments checked; `argv` outlives the
        // call; local out-parameter.
        let ok = unsafe { ffi::cabby_obj_new(vm.state(), self.checked(vm), argc(&argv), argv_ptr(&argv), &mut out) };
        vm.finish(ok).map(|()| vm.wrap(out))
    }

    /// `mrb_gc_protect`.
    pub fn gc_protect(&self) {
        let vm = &self.vm;
        // SAFETY: open state; `self` checked.
        let ok = unsafe { ffi::cabby_gc_protect(vm.state(), self.checked(vm)) };
        vm.quiet((ok, ()));
    }

    /// Sets a proc's target class.
    pub fn set_proc_target_class(&self, cls: &Val) -> Result<(), Raised> {
        let vm = &self.vm;
        // SAFETY: open state; both values checked (the shim checks their types).
        let ok = unsafe { ffi::cabby_set_proc_target_class(vm.state(), self.checked(vm), cls.checked(vm)) };
        vm.finish(ok)
    }

    /// Length of an array (0 for other values).
    pub fn ary_len(&self) -> usize {
        let vm = &self.vm;
        // SAFETY: `self` checked (live, so an array tag means a live RArray).
        let n = unsafe { ffi::cabby_ary_len(self.checked(vm)) };
        usize::try_from(n).unwrap_or(0)
    }

    fn ary_entry_raw(&self, idx: i64) -> (c_int, Val) {
        let vm = &self.vm;
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; `self` checked; local out-parameter.
        let ok = unsafe { ffi::cabby_ary_entry(vm.state(), self.checked(vm), idx, &mut out) };
        (ok, vm.wrap(out))
    }

    /// `mrb_ary_entry` (nil out of range).
    pub fn ary_entry(&self, idx: i64) -> Result<Val, Raised> {
        let (ok, v) = self.ary_entry_raw(idx);
        self.vm.finish(ok).map(|()| v)
    }

    /// `mrb_hash_get`.
    pub fn hash_get(&self, key: &Val) -> Result<Val, Raised> {
        let vm = &self.vm;
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; hash and key checked; local out-parameter.
        let ok = unsafe { ffi::cabby_hash_get(vm.state(), self.checked(vm), key.checked(vm), &mut out) };
        vm.finish(ok).map(|()| vm.wrap(out))
    }

    /// `mrb_hash_set`.
    pub fn hash_set(&self, key: &Val, value: &Val) -> Result<(), Raised> {
        let vm = &self.vm;
        // SAFETY: open state; hash, key and value checked.
        let ok = unsafe { ffi::cabby_hash_set(vm.state(), self.checked(vm), key.checked(vm), value.checked(vm)) };
        vm.finish(ok)
    }

    /// `mrb_hash_delete_key`.
    pub fn hash_delete(&self, key: &Val) -> Result<Val, Raised> {
        let vm = &self.vm;
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; hash and key checked; local out-parameter.
        let ok = unsafe { ffi::cabby_hash_delete(vm.state(), self.checked(vm), key.checked(vm), &mut out) };
        vm.finish(ok).map(|()| vm.wrap(out))
    }

    /// `mrb_hash_keys`.
    pub fn hash_keys(&self) -> Result<Val, Raised> {
        let vm = &self.vm;
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; `self` checked; local out-parameter.
        let ok = unsafe { ffi::cabby_hash_keys(vm.state(), self.checked(vm), &mut out) };
        vm.finish(ok).map(|()| vm.wrap(out))
    }
}

/// Runs the Rust method of the current call (the trampoline's callback).
///
/// # Safety
/// Called by the shim's trampoline only, with the running state and valid
/// pointers to `self` and two out-values.
unsafe extern "C" fn dispatch(
    mrb: *mut mrb_state,
    self_: *const mrb_value,
    result: *mut mrb_value,
    exc: *mut mrb_value,
) -> c_int {
    // SAFETY: `mrb` is the running state.
    let ud = unsafe { ffi::cabby_get_ud(mrb) }.cast_const().cast::<VmInner>();
    if ud.is_null() {
        return 2;
    }
    // SAFETY: `ud` was set by `Vm::open` from an `Rc<VmInner>`. The VM is
    // running, which only happens inside a call made through a `Vm` or `Val`
    // handle, so that Rc is alive; taking another strong reference is sound.
    let inner = unsafe {
        Rc::increment_strong_count(ud);
        Rc::from_raw(ud)
    };
    let vm = Vm { inner };
    let mut cls = mrb_value::zeroed();
    let mut mid: u32 = 0;
    // SAFETY: running state; locals as out-parameters.
    if unsafe { ffi::cabby_current_method(mrb, &mut cls, &mut mid) } == 0 {
        return 2;
    }
    // SAFETY: reads the bits of a local value.
    let key = (unsafe { ffi::cabby_identity(&cls) }, mid);
    let Some(f) = vm.inner.methods.try_borrow().ok().and_then(|m| m.get(&key).cloned()) else {
        return 2;
    };
    // SAFETY: `self_` points to the receiver, a valid value of this VM.
    let this = vm.wrap(unsafe { *self_ });

    // SAFETY: running state.
    let floor = vm.inner.arena_floor.replace(unsafe { ffi::cabby_arena_save(mrb) });
    vm.inner.depth.set(vm.inner.depth.get() + 1);
    let outcome = catch_unwind(AssertUnwindSafe(|| f(&vm, &this)));
    vm.inner.depth.set(vm.inner.depth.get() - 1);
    vm.inner.arena_floor.set(floor);

    let usable = |v: &Val| v.vm.same(&vm) && v.live();
    match outcome {
        Ok(Ok(Some(v))) if usable(&v) => {
            // SAFETY: `result` is a valid out-pointer (see the contract).
            unsafe { *result = v.raw };
            0
        }
        Ok(Ok(None)) => {
            // SAFETY: `result` is a valid out-pointer.
            unsafe { ffi::cabby_nil(result) };
            0
        }
        Ok(Err(e)) if usable(&e) => {
            // SAFETY: `exc` is a valid out-pointer.
            unsafe { *exc = e.raw };
            1
        }
        Ok(_) => 2,
        Err(payload) => {
            *vm.inner.panic.borrow_mut() = Some(payload);
            2
        }
    }
}

/// A parser (`mrb_parser_state`) that captures errors.
pub struct Parser {
    vm: Vm,
    p: *mut mrb_parser_state,
    /// The source of the last parse (the parser points into it while parsing).
    code: Option<CString>,
}

impl std::fmt::Debug for Parser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Parser").finish_non_exhaustive()
    }
}

impl Parser {
    /// `mrb_parser_new` with `capture_errors`.
    pub fn new(vm: &Vm) -> Parser {
        // SAFETY: open state.
        let p = unsafe { ffi::cabby_parser_new(vm.state()) };
        assert!(!p.is_null(), "mrb_parser_new failed");
        Parser { vm: vm.clone(), p, code: None }
    }

    /// Parses `code` (cut at the first NUL) with an optional compile context
    /// of the same VM. Returns the errors and warnings.
    pub fn parse(&mut self, code: &str, ctx: Option<&Context>) -> (Vec<ParserMessage>, Vec<ParserMessage>) {
        let ctx = ctx.map_or(std::ptr::null_mut(), |c| {
            assert!(c.vm.same(&self.vm), "compile context of another VM");
            c.ctx
        });
        let c = cstring(code);
        let len = c.as_bytes().len();
        let code = self.code.insert(c);
        // SAFETY: open state; `self.p` is this VM's live parser; `code` is
        // owned by the parser handle and valid for `len` bytes; `ctx` is null
        // or a live context of the same VM.
        let ok = unsafe { ffi::cabby_parser_parse(self.vm.state(), self.p, code.as_ptr(), len, ctx) };
        self.vm.quiet((ok, ()));
        (self.messages(false), self.messages(true))
    }

    fn messages(&self, warn: bool) -> Vec<ParserMessage> {
        // SAFETY: `self.p` is a live parser.
        let n = unsafe { if warn { ffi::cabby_parser_nwarn(self.p) } else { ffi::cabby_parser_nerr(self.p) } };
        (0..n)
            .map(|i| {
                let (mut line, mut col): (c_int, c_int) = (0, 0);
                // SAFETY: `i` is below the stored message count.
                let msg = unsafe { ffi::cabby_parser_message(self.p, c_int::from(warn), i, &mut line, &mut col) };
                // SAFETY: stored messages are NUL-terminated strings in the
                // parser's pool (or null).
                let message = unsafe { copy_c_str(msg) };
                ParserMessage { line: i64::from(line), col: i64::from(col), message }
            })
            .collect()
    }

    /// `mrb_generate_code`: the proc of the last parse, nil when there is none
    /// (no parse, a parse with errors, or a code generation failure). The proc
    /// has no environment, so it can only be run ([`Vm::run`],
    /// [`Vm::context_run`]); other uses raise `TypeError`.
    pub fn generate_code(&mut self) -> Val {
        let mut out = mrb_value::zeroed();
        // SAFETY: open state; live parser of this VM; local out-parameter.
        let ok = unsafe { ffi::cabby_generate_code(self.vm.state(), self.p, &mut out) };
        match self.vm.quiet((ok, ())) {
            Some(()) => self.vm.wrap(out),
            None => self.vm.nil(),
        }
    }
}

impl Drop for Parser {
    fn drop(&mut self) {
        // The parser's memory belongs to the VM's allocator; once the VM is
        // closed it is gone with it (leaked, never freed twice).
        if !self.vm.is_closed() {
            // SAFETY: live parser of an open VM, freed exactly once.
            unsafe { ffi::cabby_parser_free(self.p) };
        }
    }
}

/// Compile options (`mrbc_context`).
pub struct Context {
    vm: Vm,
    ctx: *mut mrbc_context,
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context").finish_non_exhaustive()
    }
}

impl Context {
    /// `mrbc_context_new`.
    pub fn new(vm: &Vm) -> Context {
        // SAFETY: open state.
        let ctx = unsafe { ffi::cabby_ctx_new(vm.state()) };
        assert!(!ctx.is_null(), "mrbc_context_new failed");
        Context { vm: vm.clone(), ctx }
    }

    /// The file name; empty when unset.
    pub fn filename(&self) -> String {
        self.vm.state();
        // SAFETY: live context of an open VM; the name is null or a
        // NUL-terminated string in VM memory.
        unsafe { copy_c_str(ffi::cabby_ctx_filename(self.ctx)) }
    }

    /// Sets the file name used in backtraces.
    pub fn set_filename(&mut self, name: &str) {
        let c = cstring(name);
        // SAFETY: open state; live context; `c` outlives the call (mruby
        // copies it).
        let ok = unsafe { ffi::cabby_ctx_set_filename(self.vm.state(), self.ctx, c.as_ptr()) };
        self.vm.quiet((ok, ()));
    }

    /// Sets `capture_errors`.
    pub fn capture_errors(&mut self, yes: bool) {
        self.vm.state();
        // SAFETY: live context of an open VM.
        unsafe { ffi::cabby_ctx_capture_errors(self.ctx, c_int::from(yes)) };
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        if !self.vm.is_closed() {
            // SAFETY: live context of an open VM, freed exactly once.
            unsafe { ffi::cabby_ctx_free(self.vm.state(), self.ctx) };
        }
    }
}

#[cfg(all(test, cabby_mruby))]
mod tests {
    use super::*;

    #[test]
    fn loads_and_raises() {
        let vm = Vm::open();
        assert_eq!(vm.load_string("1 + 41").expect("runs").fixnum(), 42);
        let e = vm.load_string("raise 'boom'").expect_err("raises");
        assert_eq!(e.message, "boom");
        assert_eq!(vm.load_string("'ok'").expect("cleared").to_s().expect("to_s"), "ok");
        vm.close();
    }

    #[test]
    fn closed_vm_values_are_dead() {
        let vm = Vm::open();
        let v = vm.load_string("[]").expect("runs");
        assert!(!v.is_dead());
        vm.close();
        assert!(v.is_dead());
    }

    #[test]
    fn collected_values_are_not_passed_to_c() {
        let vm = Vm::open();
        let ai = vm.arena_save();
        let s = vm.string("foo");
        vm.arena_restore(ai);
        vm.full_gc();
        assert!(s.is_dead());
        let r = catch_unwind(AssertUnwindSafe(|| s.to_s()));
        assert!(r.is_err(), "a collected value must not reach mruby");
        vm.close();
    }

    #[test]
    fn panics_in_methods_propagate() {
        let vm = Vm::open();
        let cls = vm.object_class();
        let f: Method = Rc::new(|_, _| panic!("from Rust"));
        vm.define_method(&cls, "boom", 0, false, f).expect("defined");
        let r = catch_unwind(AssertUnwindSafe(|| vm.load_string("boom")));
        let payload = r.expect_err("panic propagates");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"from Rust"));
        assert_eq!(vm.load_string("2").expect("VM still usable").fixnum(), 2);
        vm.close();
    }

    #[test]
    fn description_is_mruby_1_2_0() {
        assert_eq!(crate::description(), Some("mruby 1.2.0 (2015-11-17) "));
    }
}
