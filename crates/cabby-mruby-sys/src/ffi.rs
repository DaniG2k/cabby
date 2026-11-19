//! Raw declarations of the C shim (`shim/cabby_mruby.c`). Rust calls only these
//! functions, never mruby directly; the shim catches every raise.
//!
//! Without the vendored C library (feature off or unsupported target) the same
//! names exist as stubs that are never reached, because [`crate::available`]
//! is false and no VM can be opened.

#![allow(missing_docs)]

use std::ffi::{c_char, c_int, c_void};
use std::mem::MaybeUninit;

/// `mrb_state`, opaque.
#[repr(C)]
pub struct mrb_state {
    _opaque: [u8; 0],
}

/// `struct mrb_parser_state`, opaque.
#[repr(C)]
pub struct mrb_parser_state {
    _opaque: [u8; 0],
}

/// `mrbc_context`, opaque.
#[repr(C)]
pub struct mrbc_context {
    _opaque: [u8; 0],
}

/// `mrb_value` (unboxed: a 64-bit union and a type tag; the shim asserts
/// size 16 and alignment 8). Rust never reads its bytes; the shim does.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct mrb_value {
    _bytes: [MaybeUninit<u8>; 16],
}

impl mrb_value {
    /// Storage for an out-parameter.
    pub const fn zeroed() -> mrb_value {
        mrb_value { _bytes: [MaybeUninit::new(0); 16] }
    }
}

/// The callback the trampoline uses to run Rust methods. Returns 0 with
/// `result` set, 1 with `exc` set, or 2 when the method failed (panicked).
pub type DispatchFn = unsafe extern "C" fn(
    mrb: *mut mrb_state,
    self_: *const mrb_value,
    result: *mut mrb_value,
    exc: *mut mrb_value,
) -> c_int;

macro_rules! shim {
    ($( pub fn $name:ident ( $($arg:ident : $ty:ty),* $(,)? ) $(-> $ret:ty)? ; )*) => {
        #[cfg(cabby_mruby)]
        unsafe extern "C" {
            $( pub fn $name($($arg: $ty),*) $(-> $ret)?; )*
        }

        #[cfg(not(cabby_mruby))]
        mod stubs {
            use super::*;
            $(
                #[allow(unused_variables, clippy::missing_safety_doc)]
                pub unsafe fn $name($($arg: $ty),*) $(-> $ret)? {
                    unreachable!("mruby is not compiled in")
                }
            )*
        }

        #[cfg(not(cabby_mruby))]
        pub use stubs::*;
    };
}

shim! {
    pub fn cabby_set_dispatch(f: DispatchFn);
    pub fn cabby_open() -> *mut mrb_state;
    pub fn cabby_close(mrb: *mut mrb_state);
    pub fn cabby_set_ud(mrb: *mut mrb_state, ud: *mut c_void);
    pub fn cabby_get_ud(mrb: *mut mrb_state) -> *mut c_void;
    pub fn cabby_description() -> *const c_char;

    pub fn cabby_value_live(mrb: *mut mrb_state, v: *const mrb_value) -> c_int;
    pub fn cabby_type(v: *const mrb_value) -> u32;
    pub fn cabby_fixnum_of(v: *const mrb_value) -> i64;
    pub fn cabby_float_of(v: *const mrb_value) -> f64;
    pub fn cabby_identity(v: *const mrb_value) -> u64;
    pub fn cabby_same(a: *const mrb_value, b: *const mrb_value) -> c_int;
    pub fn cabby_nil(out: *mut mrb_value);
    pub fn cabby_true(out: *mut mrb_value);
    pub fn cabby_false(out: *mut mrb_value);
    pub fn cabby_fixnum(n: i64, out: *mut mrb_value);
    pub fn cabby_object_class(mrb: *mut mrb_state, out: *mut mrb_value);
    pub fn cabby_kernel_module(mrb: *mut mrb_state, out: *mut mrb_value);
    pub fn cabby_top_self(mrb: *mut mrb_state, out: *mut mrb_value);

    pub fn cabby_take_exc(mrb: *mut mrb_state, out: *mut mrb_value) -> c_int;
    pub fn cabby_exc_backtrace(mrb: *mut mrb_state, exc: *const mrb_value, out: *mut mrb_value) -> c_int;
    pub fn cabby_arena_save(mrb: *mut mrb_state) -> c_int;
    pub fn cabby_arena_restore(mrb: *mut mrb_state, idx: c_int) -> c_int;
    pub fn cabby_gc_protect(mrb: *mut mrb_state, v: *const mrb_value) -> c_int;
    pub fn cabby_gc_set_disabled(mrb: *mut mrb_state, disabled: c_int);
    pub fn cabby_full_gc(mrb: *mut mrb_state) -> c_int;
    pub fn cabby_incremental_gc(mrb: *mut mrb_state) -> c_int;
    pub fn cabby_gc_live(mrb: *mut mrb_state) -> usize;

    pub fn cabby_str_new(mrb: *mut mrb_state, s: *const c_char, len: usize, out: *mut mrb_value) -> c_int;
    pub fn cabby_to_s(
        mrb: *mut mrb_state,
        v: *const mrb_value,
        str_out: *mut mrb_value,
        ptr: *mut *const c_char,
        len: *mut usize,
    ) -> c_int;

    pub fn cabby_gv_get(mrb: *mut mrb_state, name: *const c_char, out: *mut mrb_value) -> c_int;
    pub fn cabby_gv_set(mrb: *mut mrb_state, name: *const c_char, v: *const mrb_value) -> c_int;
    pub fn cabby_iv_get(mrb: *mut mrb_state, obj: *const mrb_value, name: *const c_char, out: *mut mrb_value) -> c_int;
    pub fn cabby_iv_set(mrb: *mut mrb_state, obj: *const mrb_value, name: *const c_char, v: *const mrb_value) -> c_int;

    pub fn cabby_class_get(mrb: *mut mrb_state, outer: *const mrb_value, name: *const c_char, out: *mut mrb_value) -> c_int;
    pub fn cabby_module_get(mrb: *mut mrb_state, name: *const c_char, out: *mut mrb_value) -> c_int;
    pub fn cabby_const_defined(mrb: *mut mrb_state, scope: *const mrb_value, name: *const c_char, out: *mut c_int) -> c_int;
    pub fn cabby_define_class(
        mrb: *mut mrb_state,
        outer: *const mrb_value,
        name: *const c_char,
        superclass: *const mrb_value,
        out: *mut mrb_value,
    ) -> c_int;
    pub fn cabby_define_module(mrb: *mut mrb_state, outer: *const mrb_value, name: *const c_char, out: *mut mrb_value) -> c_int;
    pub fn cabby_define_const(mrb: *mut mrb_state, cls: *const mrb_value, name: *const c_char, v: *const mrb_value) -> c_int;
    pub fn cabby_define_method(
        mrb: *mut mrb_state,
        cls: *const mrb_value,
        name: *const c_char,
        aspec: u32,
        class_method: c_int,
        holder: *mut mrb_value,
        sym: *mut u32,
    ) -> c_int;
    pub fn cabby_current_method(mrb: *mut mrb_state, target_class: *mut mrb_value, mid: *mut u32) -> c_int;
    pub fn cabby_class_of(mrb: *mut mrb_state, v: *const mrb_value, out: *mut mrb_value) -> c_int;
    pub fn cabby_singleton_class(mrb: *mut mrb_state, v: *const mrb_value, out: *mut mrb_value) -> c_int;
    pub fn cabby_obj_new(mrb: *mut mrb_state, cls: *const mrb_value, argc: i64, argv: *const mrb_value, out: *mut mrb_value) -> c_int;
    pub fn cabby_set_proc_target_class(mrb: *mut mrb_state, proc_: *const mrb_value, cls: *const mrb_value) -> c_int;

    pub fn cabby_load_string(mrb: *mut mrb_state, code: *const c_char, out: *mut mrb_value) -> c_int;
    pub fn cabby_funcall(
        mrb: *mut mrb_state,
        recv: *const mrb_value,
        name: *const c_char,
        argc: i64,
        argv: *const mrb_value,
        block: *const mrb_value,
        out: *mut mrb_value,
    ) -> c_int;
    pub fn cabby_yield(mrb: *mut mrb_state, block: *const mrb_value, argc: i64, argv: *const mrb_value, out: *mut mrb_value) -> c_int;
    pub fn cabby_run(mrb: *mut mrb_state, proc_: *const mrb_value, self_: *const mrb_value, out: *mut mrb_value) -> c_int;
    pub fn cabby_context_run(
        mrb: *mut mrb_state,
        proc_: *const mrb_value,
        self_: *const mrb_value,
        keep: u32,
        nlocals: *mut u32,
        out: *mut mrb_value,
    ) -> c_int;
    pub fn cabby_get_args(
        mrb: *mut mrb_state,
        argv: *mut *const mrb_value,
        argc: *mut i64,
        block: *mut mrb_value,
        has_block: *mut c_int,
    ) -> c_int;

    pub fn cabby_ary_len(ary: *const mrb_value) -> i64;
    pub fn cabby_ary_entry(mrb: *mut mrb_state, ary: *const mrb_value, idx: i64, out: *mut mrb_value) -> c_int;
    pub fn cabby_hash_get(mrb: *mut mrb_state, h: *const mrb_value, key: *const mrb_value, out: *mut mrb_value) -> c_int;
    pub fn cabby_hash_set(mrb: *mut mrb_state, h: *const mrb_value, key: *const mrb_value, v: *const mrb_value) -> c_int;
    pub fn cabby_hash_delete(mrb: *mut mrb_state, h: *const mrb_value, key: *const mrb_value, out: *mut mrb_value) -> c_int;
    pub fn cabby_hash_keys(mrb: *mut mrb_state, h: *const mrb_value, out: *mut mrb_value) -> c_int;

    pub fn cabby_parser_new(mrb: *mut mrb_state) -> *mut mrb_parser_state;
    pub fn cabby_parser_free(p: *mut mrb_parser_state);
    pub fn cabby_parser_parse(
        mrb: *mut mrb_state,
        p: *mut mrb_parser_state,
        code: *const c_char,
        len: usize,
        ctx: *mut mrbc_context,
    ) -> c_int;
    pub fn cabby_parser_nerr(p: *const mrb_parser_state) -> usize;
    pub fn cabby_parser_nwarn(p: *const mrb_parser_state) -> usize;
    pub fn cabby_parser_message(
        p: *const mrb_parser_state,
        warn: c_int,
        i: usize,
        line: *mut c_int,
        col: *mut c_int,
    ) -> *const c_char;
    pub fn cabby_generate_code(mrb: *mut mrb_state, p: *mut mrb_parser_state, out: *mut mrb_value) -> c_int;
    pub fn cabby_ctx_new(mrb: *mut mrb_state) -> *mut mrbc_context;
    pub fn cabby_ctx_free(mrb: *mut mrb_state, c: *mut mrbc_context);
    pub fn cabby_ctx_filename(c: *const mrbc_context) -> *const c_char;
    pub fn cabby_ctx_set_filename(mrb: *mut mrb_state, c: *mut mrbc_context, name: *const c_char) -> c_int;
    pub fn cabby_ctx_capture_errors(c: *mut mrbc_context, yes: c_int);
}
