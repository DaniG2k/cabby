/*
 * C side of Cabby's mruby binding (the role of go-mruby's gomruby.h).
 *
 * Rust never calls an mruby function that can raise directly: a raise is a
 * longjmp, and a longjmp across Rust frames is undefined behavior. Every such
 * call goes through a function here that sets its own jump buffer
 * (CABBY_CALL), so a raise ends in this file and leaves the exception in
 * mrb->exc, where Rust collects it (cabby_take_exc). The C functions only
 * marshal; type and liveness checks that keep the calls memory-safe happen
 * here too, before mruby dereferences anything.
 *
 * Values cross the boundary by pointer (const mrb_value *), so the Rust side
 * only needs mrb_value's size and alignment, which are checked below.
 */

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <mruby.h>
#include <mruby/array.h>
#include <mruby/class.h>
#include <mruby/compile.h>
#include <mruby/error.h>
#include <mruby/gc.h>
#include <mruby/hash.h>
#include <mruby/irep.h>
#include <mruby/debug.h> /* after irep.h, which it needs */
#include <mruby/numeric.h>
#include <mruby/proc.h>
#include <mruby/string.h>
#include <mruby/throw.h>
#include <mruby/value.h>
#include <mruby/variable.h>
#include <mruby/version.h>

_Static_assert(sizeof(mrb_value) == 16, "Rust mirrors mrb_value as 16 bytes");
_Static_assert(_Alignof(mrb_value) == 8, "Rust mirrors mrb_value with 8-byte alignment");

/* Implemented in gc_live.c, which compiles mruby's gc.c with this accessor. */
int cabby_heap_object_live(mrb_state *mrb, const struct RBasic *p, enum mrb_vtype tt);

/* Implemented in vm_live.c, which compiles mruby's vm.c with these functions. */
void cabby_unwind_ci(mrb_state *mrb, ptrdiff_t nth_ci);
mrb_value cabby_run_nested(mrb_state *mrb, struct RProc *p, mrb_value self);

/* ------------------------------------------------------------ protected calls
 *
 * A protected call (CABBY_CALL) keeps four promises:
 *
 * - a raise ends in this file: the call sets a jump buffer of its own and
 *   leaves the exception in mrb->exc;
 * - a raise leaves the VM's call stack as the call found it: the context and
 *   the call frames pushed during the call are restored and popped
 *   (mrb_funcall_with_block does the same when no jump buffer is set; under
 *   ours it leaves its frames behind, and their stale procs and stack slots
 *   are read later);
 * - the values handed to the call stay alive during it: they are pushed on the
 *   GC arena before anything allocates, and removed again afterwards without
 *   disturbing what the call itself left in the arena. A value that is live
 *   but no longer rooted (the caller restored the arena) would otherwise be
 *   collected by the first allocation inside the call;
 * - a proc without an environment (what mrb_generate_code returns) never
 *   becomes a Ruby-visible value: mruby reads proc->env without checking when
 *   such a proc is yielded to, called or used as a block. It can only be run.
 *
 * Cleanup after a raise runs with no jump buffer, so a raise there (out of
 * memory only) aborts the process instead of jumping past the shim.
 */

struct cabby_guard {
  struct mrb_jmpbuf *prev_jmp;
  struct mrb_context *c;
  ptrdiff_t nth_ci;
  int arena_base;
};

static void guard_enter(mrb_state *mrb, struct cabby_guard *g) {
  g->prev_jmp = mrb->jmp;
  g->c = mrb->c;
  g->nth_ci = (mrb->c->ci != NULL && mrb->c->cibase != NULL) ? mrb->c->ci - mrb->c->cibase : 0;
  g->arena_base = mrb->gc.arena_idx;
}

static void guard_unwind(mrb_state *mrb, const struct cabby_guard *g) {
  mrb->jmp = NULL;
  mrb->c = g->c;
  cabby_unwind_ci(mrb, g->nth_ci);
  mrb->jmp = g->prev_jmp;
}

/* Removes the `nroots` argument roots at the guard's arena base, moving down
 * whatever the call pushed above them. `nroots` is -1 when the call raised
 * while rooting its arguments (nothing else ran: the arena is cut back; the
 * exception is rooted as mrb->exc). */
static void guard_unroot(mrb_state *mrb, const struct cabby_guard *g, int nroots) {
  mrb_gc *gc = &mrb->gc;
  int base = g->arena_base;

  if (nroots < 0 || gc->arena_idx < base + nroots) {
    if (gc->arena_idx > base) {
      gc->arena_idx = base;
    }
    return;
  }
  if (nroots > 0) {
    memmove(&gc->arena[base], &gc->arena[base + nroots],
            sizeof(struct RBasic *) * (size_t)(gc->arena_idx - base - nroots));
    gc->arena_idx -= nroots;
  }
}

/* Runs the statements after `roots` with a jump buffer of their own, once
 * `roots` (ROOT/VALUE_ARG statements) has rooted the call's values; ok_ is 1
 * when they returned normally and 0 when they raised (the exception is then in
 * mrb->exc). */
#define CABBY_CALL(mrb, roots, ...)                \
  volatile int ok_ = 1;                            \
  volatile int nroots_ = -1;                       \
  struct cabby_guard g_;                           \
  struct mrb_jmpbuf c_jmp_;                        \
  guard_enter(mrb, &g_);                           \
  MRB_TRY(&c_jmp_) {                               \
    (mrb)->jmp = &c_jmp_;                          \
    roots;                                         \
    nroots_ = (mrb)->gc.arena_idx - g_.arena_base; \
    __VA_ARGS__;                                   \
    (mrb)->jmp = g_.prev_jmp;                      \
  }                                                \
  MRB_CATCH(&c_jmp_) {                             \
    (mrb)->jmp = g_.prev_jmp;                      \
    ok_ = 0;                                       \
    guard_unwind(mrb, &g_);                        \
  }                                                \
  MRB_END_EXC(&c_jmp_);                            \
  guard_unroot(mrb, &g_, nroots_)

#define PROTECTED(mrb, roots, ...)     \
  CABBY_CALL(mrb, roots, __VA_ARGS__); \
  return ok_

#define NO_ROOTS ((void)0)

/* Runs `stmt` with no jump buffer: an mruby raise in it (out of memory only,
 * for the calls used this way) aborts instead of jumping into Rust frames. */
#define RAISE_ABORTS(mrb, stmt)                 \
  do {                                          \
    struct mrb_jmpbuf *saved_jmp_ = (mrb)->jmp; \
    (mrb)->jmp = NULL;                          \
    stmt;                                       \
    (mrb)->jmp = saved_jmp_;                    \
  } while (0)

/* Keeps `v` (may be NULL) alive for the rest of the protected call. */
static void root_value(mrb_state *mrb, const mrb_value *v) {
  if (v != NULL && !mrb_immediate_p(*v)) {
    mrb_gc_protect(mrb, *v);
  }
}

/* A value Ruby code may see: refuses a proc without an environment, then
 * roots it. */
static void value_arg(mrb_state *mrb, const mrb_value *v) {
  if (v != NULL && mrb_type(*v) == MRB_TT_PROC && mrb_proc_ptr(*v)->env == NULL) {
    mrb_raise(mrb, E_TYPE_ERROR, "a proc without an environment can only be run");
  }
  root_value(mrb, v);
}

static void value_args(mrb_state *mrb, int64_t argc, const mrb_value *argv) {
  int64_t i;
  for (i = 0; argv != NULL && i < argc; i++) {
    value_arg(mrb, &argv[i]);
  }
}

#define ROOT(v) root_value(mrb, (v))
#define VALUE_ARG(v) value_arg(mrb, (v))
#define VALUE_ARGS(argc, argv) value_args(mrb, (argc), (argv))

/* ------------------------------------------------------------ lifecycle */

typedef int (*cabby_dispatch_fn)(mrb_state *mrb, const mrb_value *self, mrb_value *result, mrb_value *exc);
static cabby_dispatch_fn cabby_dispatch;

void cabby_set_dispatch(cabby_dispatch_fn f) { cabby_dispatch = f; }

/* Fiber#resume and Fiber#transfer called directly from C (mrb_funcall, or a
 * block defined in C) switch mruby's context while the C caller is still on
 * the native stack; the caller then pops call frames of the wrong context
 * (mruby 1.2 checks only the target fiber's frames for C boundaries). Called
 * from Ruby code, the VM carries on in the new context, which is supported.
 * The wrappers refuse the C case with mruby's own error. The original
 * functions are the same in every VM. */
static mrb_func_t fiber_resume_orig;
static mrb_func_t fiber_transfer_orig;

static mrb_value fiber_switch_guard(mrb_state *mrb, mrb_value self, mrb_func_t *orig) {
  mrb_func_t f = __atomic_load_n(orig, __ATOMIC_ACQUIRE);
  if (f == NULL || mrb->c->ci->acc < 0) {
    mrb_raise(mrb, E_FIBER_ERROR, "can't cross C function boundary");
  }
  return f(mrb, self);
}

static mrb_value cabby_fiber_resume(mrb_state *mrb, mrb_value self) {
  return fiber_switch_guard(mrb, self, &fiber_resume_orig);
}

static mrb_value cabby_fiber_transfer(mrb_state *mrb, mrb_value self) {
  return fiber_switch_guard(mrb, self, &fiber_transfer_orig);
}

static void wrap_method(mrb_state *mrb, struct RClass *cls, const char *name, mrb_func_t *orig, mrb_func_t repl) {
  struct RClass *c = cls;
  struct RProc *p = mrb_method_search_vm(mrb, &c, mrb_intern_cstr(mrb, name));
  if (p == NULL || !MRB_PROC_CFUNC_P(p)) {
    return;
  }
  if (p->body.func != repl) {
    __atomic_store_n(orig, p->body.func, __ATOMIC_RELEASE);
  }
  mrb_define_method(mrb, cls, name, repl, MRB_ARGS_ANY());
}

static void guard_fiber_switches(mrb_state *mrb) {
  struct RClass *fiber;
  if (!mrb_class_defined(mrb, "Fiber")) {
    return;
  }
  fiber = mrb_class_get(mrb, "Fiber");
  wrap_method(mrb, fiber, "resume", &fiber_resume_orig, cabby_fiber_resume);
  wrap_method(mrb, fiber, "transfer", &fiber_transfer_orig, cabby_fiber_transfer);
}

mrb_state *cabby_open(void) {
  mrb_state *mrb = mrb_open();
  int ai;
  if (mrb == NULL) {
    return NULL;
  }
  ai = mrb_gc_arena_save(mrb);
  {
    CABBY_CALL(mrb, NO_ROOTS, guard_fiber_switches(mrb));
    if (!ok_) {
      mrb_close(mrb);
      return NULL;
    }
  }
  mrb_gc_arena_restore(mrb, ai);
  return mrb;
}

void cabby_close(mrb_state *mrb) { mrb_close(mrb); }

void cabby_set_ud(mrb_state *mrb, void *ud) { mrb->ud = ud; }

void *cabby_get_ud(mrb_state *mrb) { return mrb->ud; }

const char *cabby_description(void) { return MRUBY_DESCRIPTION; }

/* ------------------------------------------------------------ values */

static int is_class_like(const mrb_value *v) {
  enum mrb_vtype t = mrb_type(*v);
  return t == MRB_TT_CLASS || t == MRB_TT_MODULE || t == MRB_TT_SCLASS;
}

/* Whether `v` may be handed to mruby: an immediate, or a heap object of this
 * VM that is alive and still has the type recorded in `v`. A collected slot
 * reports dead (no read of freed memory: only live heap pages are searched). */
int cabby_value_live(mrb_state *mrb, const mrb_value *v) {
  if (mrb_immediate_p(*v)) {
    return 1;
  }
  return cabby_heap_object_live(mrb, (const struct RBasic *)mrb_ptr(*v), mrb_type(*v));
}

/* go-mruby's Type(): nil is reported as 0xffffffff. */
uint32_t cabby_type(const mrb_value *v) {
  if (mrb_nil_p(*v)) {
    return 0xffffffffu;
  }
  return (uint32_t)mrb_type(*v);
}

/* go-mruby's Fixnum() goes through a C int. */
int64_t cabby_fixnum_of(const mrb_value *v) { return (int64_t)(int)mrb_fixnum(*v); }

/* go-mruby's Float() returns a C float, so the double is narrowed. */
double cabby_float_of(const mrb_value *v) { return (double)(float)mrb_float(*v); }

/* Identity of a heap object (its address); 0 for immediates. */
uint64_t cabby_identity(const mrb_value *v) {
  return mrb_immediate_p(*v) ? 0 : (uint64_t)(uintptr_t)mrb_ptr(*v);
}

int cabby_same(const mrb_value *a, const mrb_value *b) {
  if (mrb_type(*a) != mrb_type(*b)) {
    return 0;
  }
  switch (mrb_type(*a)) {
  case MRB_TT_TRUE:
  case MRB_TT_FALSE:
  case MRB_TT_FIXNUM:
  case MRB_TT_UNDEF:
    return mrb_fixnum(*a) == mrb_fixnum(*b);
  case MRB_TT_SYMBOL:
    return mrb_symbol(*a) == mrb_symbol(*b);
  case MRB_TT_FLOAT:
    return memcmp(&mrb_float(*a), &mrb_float(*b), sizeof(mrb_float)) == 0;
  default:
    return mrb_ptr(*a) == mrb_ptr(*b);
  }
}

void cabby_nil(mrb_value *out) { *out = mrb_nil_value(); }
void cabby_true(mrb_value *out) { *out = mrb_true_value(); }
void cabby_false(mrb_value *out) { *out = mrb_false_value(); }

/* go-mruby's FixnumValue converts through mrb_int (32 bits here). */
void cabby_fixnum(int64_t n, mrb_value *out) { *out = mrb_fixnum_value((mrb_int)n); }

void cabby_object_class(mrb_state *mrb, mrb_value *out) { *out = mrb_obj_value(mrb->object_class); }
void cabby_kernel_module(mrb_state *mrb, mrb_value *out) { *out = mrb_obj_value(mrb->kernel_module); }
void cabby_top_self(mrb_state *mrb, mrb_value *out) { *out = mrb_obj_value(mrb->top_self); }

/* ------------------------------------------------------------ exceptions and GC */

/* Moves the pending exception (if any) out of the VM, leaving it in the GC
 * arena so it survives until the caller restores the arena. */
int cabby_take_exc(mrb_state *mrb, mrb_value *out) {
  if (mrb->exc == NULL) {
    return 0;
  }
  *out = mrb_obj_value(mrb->exc);
  RAISE_ABORTS(mrb, mrb_gc_protect(mrb, *out));
  mrb->exc = NULL;
  return 1;
}

static void backtrace_push(mrb_state *mrb, mrb_value ary, const char *filename, int32_t lineno, const char *method,
                           const char *class_name, const char *sep) {
  int ai = mrb_gc_arena_save(mrb);
  mrb_value str = mrb_str_new_cstr(mrb, filename);

  mrb_str_cat_lit(mrb, str, ":");
  mrb_str_concat(mrb, str, mrb_fixnum_to_str(mrb, mrb_fixnum_value(lineno), 10));
  if (method) {
    mrb_str_cat_lit(mrb, str, ":in ");
    if (class_name) {
      mrb_str_cat_cstr(mrb, str, class_name);
      mrb_str_cat_cstr(mrb, str, sep);
    }
    mrb_str_cat_cstr(mrb, str, method);
  }
  mrb_ary_push(mrb, ary, str);
  mrb_gc_arena_restore(mrb, ai);
}

/* mrb_exc_backtrace (backtrace.c output_backtrace + get_backtrace_i), with the
 * same output wherever mruby's own is well defined. mruby rebuilds the
 * backtrace from the call frames still in memory at index `ciidx` and below,
 * and from the pc saved at the raise; by the time the exception is read those
 * frames have been popped, and their procs (and so their ireps) may have been
 * collected. A frame is used only when its proc is a live Proc of this VM
 * (then its irep is alive too: the proc holds a reference); the debug lookups
 * reject a pc outside the irep. */
static void backtrace_of(mrb_state *mrb, struct RObject *exc, mrb_value ary) {
  struct mrb_context *c = mrb->c;
  mrb_value v;
  mrb_int ciidx, frames;
  mrb_code *pc0 = NULL;
  mrb_int i;

  if (c->cibase == NULL) {
    return;
  }
  v = mrb_obj_iv_get(mrb, exc, mrb_intern_lit(mrb, "lastpc"));
  if (mrb_cptr_p(v)) {
    pc0 = (mrb_code *)mrb_cptr(v);
  }
  v = mrb_obj_iv_get(mrb, exc, mrb_intern_lit(mrb, "ciidx"));
  if (!mrb_fixnum_p(v)) {
    return;
  }
  ciidx = mrb_fixnum(v);
  frames = c->ciend - c->cibase;
  if (ciidx >= frames) {
    ciidx = 10; /* as mruby: "ciidx is broken..." */
  }
  if (ciidx >= frames) {
    ciidx = frames - 1;
  }
  for (i = ciidx; i >= 0; i--) {
    mrb_callinfo *ci = &c->cibase[i];
    struct RProc *proc = ci->proc;
    mrb_irep *irep;
    mrb_code *pc;
    uint32_t offset;
    const char *filename;
    int32_t lineno;

    if (proc == NULL || !cabby_heap_object_live(mrb, (struct RBasic *)proc, MRB_TT_PROC)) {
      continue;
    }
    if (MRB_PROC_CFUNC_P(proc)) {
      continue;
    }
    irep = proc->body.irep;
    if (irep == NULL) {
      continue;
    }
    if (ci->err) {
      pc = ci->err;
    } else if (i + 1 <= ciidx) {
      pc = c->cibase[i + 1].pc;
      if (pc != NULL) {
        pc--;
      }
    } else {
      pc = pc0;
    }
    /* mruby's lookups below report line -1 for a pc outside the irep; the
     * offset is computed on integers so an unrelated pc stays well defined. */
    if (pc == NULL || irep->iseq == NULL) {
      continue;
    }
    offset = (uint32_t)(((uintptr_t)pc - (uintptr_t)irep->iseq) / sizeof(mrb_code));
    if ((uintptr_t)pc < (uintptr_t)irep->iseq) {
      continue;
    }
    filename = mrb_debug_get_filename(irep, offset);
    lineno = mrb_debug_get_line(irep, offset);
    if (lineno == -1) {
      continue;
    }
    backtrace_push(mrb, ary, filename ? filename : "(unknown)", lineno, mrb_sym2name(mrb, ci->mid),
                   proc->target_class ? mrb_class_name(mrb, proc->target_class) : NULL,
                   ci->target_class == proc->target_class ? "." : "#");
  }
}

int cabby_exc_backtrace(mrb_state *mrb, const mrb_value *exc, mrb_value *out) {
  PROTECTED(mrb, ROOT(exc), {
    *out = mrb_ary_new(mrb);
    if (mrb_obj_is_kind_of(mrb, *exc, mrb->eException_class)) {
      backtrace_of(mrb, mrb_obj_ptr(*exc), *out);
    }
  });
}

int cabby_arena_save(mrb_state *mrb) { return mrb_gc_arena_save(mrb); }

/* Only an index at or below the current one is meaningful; a larger one would
 * expose unused arena slots to the collector. */
int cabby_arena_restore(mrb_state *mrb, int idx) {
  PROTECTED(mrb, NO_ROOTS, {
    if (idx >= 0 && idx <= mrb->gc.arena_idx) {
      mrb_gc_arena_restore(mrb, idx);
    }
  });
}

int cabby_gc_protect(mrb_state *mrb, const mrb_value *v) { PROTECTED(mrb, NO_ROOTS, mrb_gc_protect(mrb, *v)); }

void cabby_gc_set_disabled(mrb_state *mrb, int disabled) { mrb->gc.disabled = disabled ? 1 : 0; }

int cabby_full_gc(mrb_state *mrb) { PROTECTED(mrb, NO_ROOTS, mrb_full_gc(mrb)); }

int cabby_incremental_gc(mrb_state *mrb) { PROTECTED(mrb, NO_ROOTS, mrb_incremental_gc(mrb)); }

size_t cabby_gc_live(mrb_state *mrb) { return mrb->gc.live; }

/* ------------------------------------------------------------ strings */

int cabby_str_new(mrb_state *mrb, const char *s, size_t len, mrb_value *out) {
  PROTECTED(mrb, NO_ROOTS, *out = mrb_str_new(mrb, s, len));
}

/* `to_s` of a value (go-mruby String()). The bytes stay valid while `str_out`
 * is alive; the caller copies them at once. */
int cabby_to_s(mrb_state *mrb, const mrb_value *v, mrb_value *str_out, const char **ptr, size_t *len) {
  PROTECTED(mrb, VALUE_ARG(v), {
    *str_out = mrb_obj_as_string(mrb, *v);
    *ptr = RSTRING_PTR(*str_out);
    *len = (size_t)RSTRING_LEN(*str_out);
  });
}

/* ------------------------------------------------------------ variables */

int cabby_gv_get(mrb_state *mrb, const char *name, mrb_value *out) {
  PROTECTED(mrb, NO_ROOTS, *out = mrb_gv_get(mrb, mrb_intern_cstr(mrb, name)));
}

int cabby_gv_set(mrb_state *mrb, const char *name, const mrb_value *v) {
  PROTECTED(mrb, VALUE_ARG(v), mrb_gv_set(mrb, mrb_intern_cstr(mrb, name), *v));
}

int cabby_iv_get(mrb_state *mrb, const mrb_value *obj, const char *name, mrb_value *out) {
  PROTECTED(mrb, VALUE_ARG(obj), *out = mrb_iv_get(mrb, *obj, mrb_intern_cstr(mrb, name)));
}

int cabby_iv_set(mrb_state *mrb, const mrb_value *obj, const char *name, const mrb_value *v) {
  PROTECTED(mrb, { VALUE_ARG(obj); VALUE_ARG(v); }, mrb_iv_set(mrb, *obj, mrb_intern_cstr(mrb, name), *v));
}

/* ------------------------------------------------------------ classes */

/* mrb_class_get(_under) / mrb_module_get raise when the constant is missing
 * or has the wrong type; go-mruby lets that abort the process. */
int cabby_class_get(mrb_state *mrb, const mrb_value *outer, const char *name, mrb_value *out) {
  PROTECTED(mrb, VALUE_ARG(outer), {
    struct RClass *c;
    if (outer == NULL) {
      c = mrb_class_get(mrb, name);
    } else if (is_class_like(outer)) {
      c = mrb_class_get_under(mrb, mrb_class_ptr(*outer), name);
    } else {
      mrb_raise(mrb, E_TYPE_ERROR, "outer is not a class/module");
    }
    *out = mrb_obj_value(c);
  });
}

int cabby_module_get(mrb_state *mrb, const char *name, mrb_value *out) {
  PROTECTED(mrb, NO_ROOTS, *out = mrb_obj_value(mrb_module_get(mrb, name)));
}

int cabby_const_defined(mrb_state *mrb, const mrb_value *scope, const char *name, int *out) {
  PROTECTED(mrb, VALUE_ARG(scope), {
    *out = 0;
    if (is_class_like(scope)) {
      *out = mrb_const_defined(mrb, *scope, mrb_intern_cstr(mrb, name)) ? 1 : 0;
    }
  });
}

int cabby_define_class(mrb_state *mrb, const mrb_value *outer, const char *name, const mrb_value *super,
                       mrb_value *out) {
  PROTECTED(mrb, { VALUE_ARG(outer); VALUE_ARG(super); }, {
    if (!is_class_like(outer) || mrb_type(*super) != MRB_TT_CLASS) {
      mrb_raise(mrb, E_TYPE_ERROR, "outer must be a class/module and super a class");
    }
    *out = mrb_obj_value(mrb_define_class_under(mrb, mrb_class_ptr(*outer), name, mrb_class_ptr(*super)));
  });
}

int cabby_define_module(mrb_state *mrb, const mrb_value *outer, const char *name, mrb_value *out) {
  PROTECTED(mrb, VALUE_ARG(outer), {
    if (!is_class_like(outer)) {
      mrb_raise(mrb, E_TYPE_ERROR, "outer must be a class/module");
    }
    *out = mrb_obj_value(mrb_define_module_under(mrb, mrb_class_ptr(*outer), name));
  });
}

int cabby_define_const(mrb_state *mrb, const mrb_value *cls, const char *name, const mrb_value *v) {
  PROTECTED(mrb, { VALUE_ARG(cls); VALUE_ARG(v); }, {
    if (!is_class_like(cls)) {
      mrb_raise(mrb, E_TYPE_ERROR, "not a class/module");
    }
    mrb_define_const(mrb, mrb_class_ptr(*cls), name, *v);
  });
}

/* Every Rust-defined method is this C function; it asks Rust which closure
 * belongs to the running method (go-mruby goMRBFuncCall). Rust has returned
 * before anything here raises, so no longjmp crosses Rust frames. */
static mrb_value cabby_trampoline(mrb_state *mrb, mrb_value self) {
  mrb_value result = mrb_nil_value();
  mrb_value exc = mrb_nil_value();
  int status = cabby_dispatch ? cabby_dispatch(mrb, &self, &result, &exc) : 2;

  switch (status) {
  case 0:
    return result;
  case 1:
    /* go-mruby sets mrb->exc and returns nil; the VM raises it. Only an
     * exception object may go there. */
    if (mrb_immediate_p(exc) || !mrb_obj_is_kind_of(mrb, exc, mrb->eException_class)) {
      exc = mrb_exc_new_str(mrb, E_TYPE_ERROR, mrb_str_new_lit(mrb, "exception object expected"));
    }
    mrb->exc = mrb_obj_ptr(exc);
    return mrb_nil_value();
  default:
    /* A Rust panic (re-raised in Rust once the VM call returns) or a method
     * without a closure. */
    mrb_raise(mrb, E_RUNTIME_ERROR, "Rust method failed");
    return mrb_nil_value();
  }
}

/* Defines a method backed by cabby_trampoline. `holder` receives the class
 * whose method table holds it (the singleton class for class methods) and
 * `sym` its name, the key Rust files the closure under. */
int cabby_define_method(mrb_state *mrb, const mrb_value *cls, const char *name, uint32_t aspec, int class_method,
                        mrb_value *holder, uint32_t *sym) {
  PROTECTED(mrb, VALUE_ARG(cls), {
    struct RClass *c;
    if (!is_class_like(cls)) {
      mrb_raise(mrb, E_TYPE_ERROR, "not a class/module");
    }
    c = mrb_class_ptr(*cls);
    if (class_method) {
      mrb_define_class_method(mrb, c, name, cabby_trampoline, (mrb_aspec)aspec);
      c = c->c;
    } else {
      mrb_define_method(mrb, c, name, cabby_trampoline, (mrb_aspec)aspec);
    }
    *holder = mrb_obj_value(c);
    *sym = (uint32_t)mrb_intern_cstr(mrb, name);
  });
}

/* Whether the current call frame runs a Rust method. */
static int in_rust_method(mrb_state *mrb) {
  mrb_callinfo *ci = mrb->c ? mrb->c->ci : NULL;
  return ci != NULL && ci->proc != NULL && MRB_PROC_CFUNC_P(ci->proc) && ci->proc->body.func == cabby_trampoline;
}

/* The running method's target class and name (go-mruby: ci->proc->target_class
 * and ci->mid). Returns 0 when no Rust method is running. */
int cabby_current_method(mrb_state *mrb, mrb_value *target_class, uint32_t *mid) {
  mrb_callinfo *ci = mrb->c ? mrb->c->ci : NULL;
  if (!in_rust_method(mrb) || ci->proc->target_class == NULL) {
    return 0;
  }
  *target_class = mrb_obj_value(ci->proc->target_class);
  *mid = (uint32_t)ci->mid;
  return 1;
}

int cabby_class_of(mrb_state *mrb, const mrb_value *v, mrb_value *out) {
  PROTECTED(mrb, VALUE_ARG(v), *out = mrb_obj_value(mrb_class(mrb, *v)));
}

int cabby_singleton_class(mrb_state *mrb, const mrb_value *v, mrb_value *out) {
  PROTECTED(mrb, VALUE_ARG(v), *out = mrb_singleton_class(mrb, *v));
}

int cabby_obj_new(mrb_state *mrb, const mrb_value *cls, int64_t argc, const mrb_value *argv, mrb_value *out) {
  PROTECTED(mrb, { VALUE_ARG(cls); VALUE_ARGS(argc, argv); }, {
    if (mrb_type(*cls) != MRB_TT_CLASS) {
      mrb_raise(mrb, E_TYPE_ERROR, "can't instantiate a non-class");
    }
    *out = mrb_obj_new(mrb, mrb_class_ptr(*cls), (mrb_int)argc, argv);
  });
}

/* The proc keeps no environment requirement here: changing its target class
 * does not run it. */
int cabby_set_proc_target_class(mrb_state *mrb, const mrb_value *proc, const mrb_value *cls) {
  PROTECTED(mrb, { ROOT(proc); VALUE_ARG(cls); }, {
    struct RProc *p;
    if (mrb_type(*proc) != MRB_TT_PROC || !is_class_like(cls)) {
      mrb_raise(mrb, E_TYPE_ERROR, "expected a proc and a class");
    }
    p = mrb_proc_ptr(*proc);
    p->target_class = mrb_class_ptr(*cls);
    mrb_field_write_barrier(mrb, (struct RBasic *)p, (struct RBasic *)p->target_class);
  });
}

/* ------------------------------------------------------------ running code */

/* go-mruby _go_mrb_load_string, including the arena protection of the result. */
int cabby_load_string(mrb_state *mrb, const char *code, mrb_value *out) {
  *out = mrb_nil_value();
  {
    CABBY_CALL(mrb, NO_ROOTS, *out = mrb_load_string(mrb, code));
    if (!ok_) {
      *out = mrb_nil_value();
    }
    RAISE_ABORTS(mrb, mrb_gc_protect(mrb, *out));
    return ok_;
  }
}

/* go-mruby _go_mrb_call; `block` may be NULL. A block must be a proc with an
 * environment, or nil: mruby casts it to a proc without checking. */
int cabby_funcall(mrb_state *mrb, const mrb_value *recv, const char *name, int64_t argc, const mrb_value *argv,
                  const mrb_value *block, mrb_value *out) {
  *out = mrb_nil_value();
  {
    CABBY_CALL(mrb, {
      VALUE_ARG(recv);
      VALUE_ARGS(argc, argv);
      if (block != NULL && !mrb_nil_p(*block) && mrb_type(*block) != MRB_TT_PROC) {
        mrb_raise(mrb, E_TYPE_ERROR, "block must be a Proc");
      }
      VALUE_ARG(block);
    }, {
      mrb_sym mid = mrb_intern_cstr(mrb, name);
      if (block != NULL) {
        *out = mrb_funcall_with_block(mrb, *recv, mid, (mrb_int)argc, argv, *block);
      } else {
        *out = mrb_funcall_argv(mrb, *recv, mid, (mrb_int)argc, argv);
      }
    });
    if (!ok_) {
      *out = mrb_nil_value();
    }
    RAISE_ABORTS(mrb, mrb_gc_protect(mrb, *out));
    return ok_;
  }
}

/* go-mruby _go_mrb_yield_argv. mrb_yield_argv takes `self` from the block's
 * environment, which value_arg requires. */
int cabby_yield(mrb_state *mrb, const mrb_value *block, int64_t argc, const mrb_value *argv, mrb_value *out) {
  *out = mrb_nil_value();
  {
    CABBY_CALL(mrb, {
      if (mrb_type(*block) != MRB_TT_PROC) {
        mrb_raise(mrb, E_ARGUMENT_ERROR, "no block given");
      }
      VALUE_ARG(block);
      VALUE_ARGS(argc, argv);
    }, *out = mrb_yield_argv(mrb, *block, (mrb_int)argc, argv));
    if (!ok_) {
      *out = mrb_nil_value();
    }
    RAISE_ABORTS(mrb, mrb_gc_protect(mrb, *out));
    return ok_;
  }
}

static void check_runnable(mrb_state *mrb, const mrb_value *proc) {
  if (mrb_type(*proc) != MRB_TT_PROC || MRB_PROC_CFUNC_P(mrb_proc_ptr(*proc))) {
    mrb_raise(mrb, E_TYPE_ERROR, "expected a Ruby proc");
  }
}

/* Runs a Ruby proc with `self`; `keep` < 0 is mrb_run's keep.
 *
 * With no Ruby frame running it runs on the root frame, as mrb_run and
 * mrb_context_run do, keeping stack slots only below the frame's register
 * count: only those slots are GC roots, so a slot above it may hold a
 * collected object.
 *
 * Inside a running method (a Rust method calling Run) it runs on a frame of
 * its own (cabby_run_nested): mrb_run would reuse the method's frame and
 * overwrite its arguments, and a fiber switch from inside it would not see
 * the C boundary. */
static mrb_value run_proc(mrb_state *mrb, struct RProc *p, mrb_value self, int64_t keep) {
  mrb_callinfo *ci = mrb->c->ci;
  if (mrb->c->cibase == NULL || ci == mrb->c->cibase) {
    int64_t k = keep;
    if (ci == NULL) {
      k = 0;
    } else {
      if (k < 0) {
        k = (int64_t)ci->argc + 2;
      }
      if (k > ci->nregs) {
        k = ci->nregs;
      }
      if (k < 0) {
        k = 0;
      }
    }
    return mrb_context_run(mrb, p, self, (unsigned int)k);
  }
  return cabby_run_nested(mrb, p, self);
}

/* go-mruby Run: mrb_run (not arena-protected, as upstream). */
int cabby_run(mrb_state *mrb, const mrb_value *proc, const mrb_value *self, mrb_value *out) {
  PROTECTED(mrb, { ROOT(proc); VALUE_ARG(self); }, {
    check_runnable(mrb, proc);
    *out = run_proc(mrb, mrb_proc_ptr(*proc), *self, -1);
  });
}

/* go-mruby _go_mrb_context_run: `nlocals` receives the proc's local count,
 * the stack keep for the next run. */
int cabby_context_run(mrb_state *mrb, const mrb_value *proc, const mrb_value *self, uint32_t keep,
                      uint32_t *nlocals, mrb_value *out) {
  PROTECTED(mrb, { ROOT(proc); VALUE_ARG(self); }, {
    struct RProc *p;
    check_runnable(mrb, proc);
    p = mrb_proc_ptr(*proc);
    *nlocals = p->body.irep->nlocals;
    *out = run_proc(mrb, p, *self, (int64_t)keep);
  });
}

/* go-mruby _go_mrb_get_args_all: mrb_get_args(mrb, "*&?"). `argv` points into
 * the VM stack; the caller copies it at once. Empty unless the current frame
 * is a Rust method's. */
int cabby_get_args(mrb_state *mrb, const mrb_value **argv, int64_t *argc, mrb_value *block, int *has_block) {
  *argv = NULL;
  *argc = 0;
  *has_block = 0;
  if (!in_rust_method(mrb)) {
    return 1;
  }
  PROTECTED(mrb, NO_ROOTS, {
    mrb_value *av = NULL;
    mrb_int ac = 0;
    mrb_bool given = 0;
    mrb_get_args(mrb, "*&?", &av, &ac, block, &given);
    *argv = av;
    *argc = (int64_t)ac;
    *has_block = !(given == FALSE || mrb_type(*block) == MRB_TT_FALSE);
  });
}

/* ------------------------------------------------------------ arrays and hashes */

int64_t cabby_ary_len(const mrb_value *ary) {
  return mrb_type(*ary) == MRB_TT_ARRAY ? (int64_t)RARRAY_LEN(*ary) : 0;
}

int cabby_ary_entry(mrb_state *mrb, const mrb_value *ary, int64_t idx, mrb_value *out) {
  PROTECTED(mrb, VALUE_ARG(ary), {
    if (mrb_type(*ary) != MRB_TT_ARRAY) {
      mrb_raise(mrb, E_TYPE_ERROR, "not an array");
    }
    *out = mrb_ary_entry(*ary, (mrb_int)idx);
  });
}

static void check_hash(mrb_state *mrb, const mrb_value *h) {
  if (mrb_type(*h) != MRB_TT_HASH) {
    mrb_raise(mrb, E_TYPE_ERROR, "not a hash");
  }
}

int cabby_hash_get(mrb_state *mrb, const mrb_value *h, const mrb_value *key, mrb_value *out) {
  PROTECTED(mrb, { VALUE_ARG(h); VALUE_ARG(key); }, {
    check_hash(mrb, h);
    *out = mrb_hash_get(mrb, *h, *key);
  });
}

int cabby_hash_set(mrb_state *mrb, const mrb_value *h, const mrb_value *key, const mrb_value *v) {
  PROTECTED(mrb, { VALUE_ARG(h); VALUE_ARG(key); VALUE_ARG(v); }, {
    check_hash(mrb, h);
    mrb_hash_set(mrb, *h, *key, *v);
  });
}

int cabby_hash_delete(mrb_state *mrb, const mrb_value *h, const mrb_value *key, mrb_value *out) {
  PROTECTED(mrb, { VALUE_ARG(h); VALUE_ARG(key); }, {
    check_hash(mrb, h);
    *out = mrb_hash_delete_key(mrb, *h, *key);
  });
}

int cabby_hash_keys(mrb_state *mrb, const mrb_value *h, mrb_value *out) {
  PROTECTED(mrb, VALUE_ARG(h), {
    check_hash(mrb, h);
    *out = mrb_hash_keys(mrb, *h);
  });
}

/* ------------------------------------------------------------ parser */

/* go-mruby NewParser: a parser that captures errors. NULL when out of memory. */
struct mrb_parser_state *cabby_parser_new(mrb_state *mrb) {
  struct mrb_parser_state *volatile p = NULL;
  {
    CABBY_CALL(mrb, NO_ROOTS, p = mrb_parser_new(mrb));
    if (!ok_) {
      mrb->exc = NULL;
      return NULL;
    }
  }
  if (p != NULL) {
    p->capture_errors = 1;
  }
  return p;
}

void cabby_parser_free(struct mrb_parser_state *p) { mrb_parser_free(p); }

/* Parses code[0..len) with an optional compile context. */
int cabby_parser_parse(mrb_state *mrb, struct mrb_parser_state *p, const char *code, size_t len, mrbc_context *ctx) {
  PROTECTED(mrb, NO_ROOTS, {
    p->s = code;
    p->send = code + len;
    mrb_parser_parse(p, ctx);
  });
}

/* Message counts, limited to what the parser stored (it keeps 10 of each). */
size_t cabby_parser_nerr(const struct mrb_parser_state *p) {
  size_t cap = sizeof(p->error_buffer) / sizeof(p->error_buffer[0]);
  return p->nerr < cap ? p->nerr : cap;
}

size_t cabby_parser_nwarn(const struct mrb_parser_state *p) {
  size_t cap = sizeof(p->warn_buffer) / sizeof(p->warn_buffer[0]);
  return p->nwarn < cap ? p->nwarn : cap;
}

/* Message `i` (below the count above) of the errors (warn = 0) or warnings. */
const char *cabby_parser_message(const struct mrb_parser_state *p, int warn, size_t i, int *line, int *col) {
  const struct mrb_parser_message *m = warn ? &p->warn_buffer[i] : &p->error_buffer[i];
  *line = m->lineno;
  *col = m->column;
  return m->message;
}

/* go-mruby GenerateCode; `out` is nil when there is nothing to run: no parse
 * yet (no tree; mrb_generate_code would dereference it), a parse with errors,
 * or a code generation failure. */
int cabby_generate_code(mrb_state *mrb, struct mrb_parser_state *p, mrb_value *out) {
  PROTECTED(mrb, NO_ROOTS, {
    struct RProc *proc = NULL;
    *out = mrb_nil_value();
    if (p->nerr == 0 && p->tree != NULL) {
      proc = mrb_generate_code(mrb, p);
    }
    if (proc != NULL) {
      *out = mrb_obj_value(proc);
    }
  });
}

mrbc_context *cabby_ctx_new(mrb_state *mrb) {
  mrbc_context *volatile c = NULL;
  {
    CABBY_CALL(mrb, NO_ROOTS, c = mrbc_context_new(mrb));
    if (!ok_) {
      mrb->exc = NULL;
      return NULL;
    }
  }
  return c;
}

void cabby_ctx_free(mrb_state *mrb, mrbc_context *c) { mrbc_context_free(mrb, c); }

const char *cabby_ctx_filename(const mrbc_context *c) { return c->filename; }

/* mrbc_filename copies the name into VM-owned memory (freed by mrb_close). */
int cabby_ctx_set_filename(mrb_state *mrb, mrbc_context *c, const char *name) {
  PROTECTED(mrb, NO_ROOTS, mrbc_filename(mrb, c, name));
}

void cabby_ctx_capture_errors(mrbc_context *c, int yes) { c->capture_errors = yes ? 1 : 0; }
