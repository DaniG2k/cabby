/*
 * C side of Cabby's mruby binding (the role of go-mruby's gomruby.h).
 *
 * Rust never calls an mruby function that can raise directly: a raise is a
 * longjmp, and a longjmp across Rust frames is undefined behavior. Every such
 * call goes through a function here that sets its own jump buffer
 * (CABBY_PROTECT), so a raise ends in this file and leaves the exception in
 * mrb->exc, where Rust collects it (cabby_take_exc). The C functions only
 * marshal; type and liveness checks that keep the calls memory-safe happen
 * here too, before mruby dereferences anything.
 *
 * Values cross the boundary by pointer (const mrb_value *), so the Rust side
 * only needs mrb_value's size and alignment, which are checked below.
 */

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

/* Runs `body` with a jump buffer of its own; evaluates to 1 when it returned
 * normally and 0 when it raised (the exception is then in mrb->exc). */
#define CABBY_PROTECT(mrb, body)                     \
  do {                                               \
    struct mrb_jmpbuf *prev_jmp_ = (mrb)->jmp;       \
    struct mrb_jmpbuf c_jmp_;                        \
    MRB_TRY(&c_jmp_) {                               \
      (mrb)->jmp = &c_jmp_;                          \
      body;                                          \
      (mrb)->jmp = prev_jmp_;                        \
    }                                                \
    MRB_CATCH(&c_jmp_) {                             \
      (mrb)->jmp = prev_jmp_;                        \
      ok_ = 0;                                       \
    }                                                \
    MRB_END_EXC(&c_jmp_);                            \
  } while (0)

#define PROTECTED(mrb, body)  \
  volatile int ok_ = 1;       \
  CABBY_PROTECT(mrb, body);   \
  return ok_

/* ------------------------------------------------------------ lifecycle */

typedef int (*cabby_dispatch_fn)(mrb_state *mrb, const mrb_value *self, mrb_value *result, mrb_value *exc);
static cabby_dispatch_fn cabby_dispatch;

void cabby_set_dispatch(cabby_dispatch_fn f) { cabby_dispatch = f; }

mrb_state *cabby_open(void) { return mrb_open(); }

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
  mrb->exc = NULL;
  mrb_gc_protect(mrb, *out);
  return 1;
}

int cabby_exc_backtrace(mrb_state *mrb, const mrb_value *exc, mrb_value *out) {
  PROTECTED(mrb, {
    if (mrb_obj_is_kind_of(mrb, *exc, mrb->eException_class)) {
      *out = mrb_exc_backtrace(mrb, *exc);
    } else {
      *out = mrb_ary_new(mrb);
    }
  });
}

int cabby_arena_save(mrb_state *mrb) { return mrb_gc_arena_save(mrb); }

/* Only an index at or below the current one is meaningful; a larger one would
 * expose unused arena slots to the collector. */
int cabby_arena_restore(mrb_state *mrb, int idx) {
  PROTECTED(mrb, {
    if (idx >= 0 && idx <= mrb->gc.arena_idx) {
      mrb_gc_arena_restore(mrb, idx);
    }
  });
}

int cabby_gc_protect(mrb_state *mrb, const mrb_value *v) { PROTECTED(mrb, mrb_gc_protect(mrb, *v)); }

void cabby_gc_set_disabled(mrb_state *mrb, int disabled) { mrb->gc.disabled = disabled ? 1 : 0; }

int cabby_full_gc(mrb_state *mrb) { PROTECTED(mrb, mrb_full_gc(mrb)); }

int cabby_incremental_gc(mrb_state *mrb) { PROTECTED(mrb, mrb_incremental_gc(mrb)); }

size_t cabby_gc_live(mrb_state *mrb) { return mrb->gc.live; }

/* ------------------------------------------------------------ strings */

int cabby_str_new(mrb_state *mrb, const char *s, size_t len, mrb_value *out) {
  PROTECTED(mrb, *out = mrb_str_new(mrb, s, len));
}

/* `to_s` of a value (go-mruby String()). The bytes stay valid while `str_out`
 * is alive; the caller copies them at once. */
int cabby_to_s(mrb_state *mrb, const mrb_value *v, mrb_value *str_out, const char **ptr, size_t *len) {
  PROTECTED(mrb, {
    *str_out = mrb_obj_as_string(mrb, *v);
    *ptr = RSTRING_PTR(*str_out);
    *len = (size_t)RSTRING_LEN(*str_out);
  });
}

/* ------------------------------------------------------------ variables */

int cabby_gv_get(mrb_state *mrb, const char *name, mrb_value *out) {
  PROTECTED(mrb, *out = mrb_gv_get(mrb, mrb_intern_cstr(mrb, name)));
}

int cabby_gv_set(mrb_state *mrb, const char *name, const mrb_value *v) {
  PROTECTED(mrb, mrb_gv_set(mrb, mrb_intern_cstr(mrb, name), *v));
}

int cabby_iv_get(mrb_state *mrb, const mrb_value *obj, const char *name, mrb_value *out) {
  PROTECTED(mrb, *out = mrb_iv_get(mrb, *obj, mrb_intern_cstr(mrb, name)));
}

int cabby_iv_set(mrb_state *mrb, const mrb_value *obj, const char *name, const mrb_value *v) {
  PROTECTED(mrb, mrb_iv_set(mrb, *obj, mrb_intern_cstr(mrb, name), *v));
}

/* ------------------------------------------------------------ classes */

/* mrb_class_get(_under) / mrb_module_get raise when the constant is missing
 * or has the wrong type; go-mruby lets that abort the process. */
int cabby_class_get(mrb_state *mrb, const mrb_value *outer, const char *name, mrb_value *out) {
  PROTECTED(mrb, {
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
  PROTECTED(mrb, *out = mrb_obj_value(mrb_module_get(mrb, name)));
}

int cabby_const_defined(mrb_state *mrb, const mrb_value *scope, const char *name, int *out) {
  PROTECTED(mrb, {
    *out = 0;
    if (is_class_like(scope)) {
      *out = mrb_const_defined(mrb, *scope, mrb_intern_cstr(mrb, name)) ? 1 : 0;
    }
  });
}

int cabby_define_class(mrb_state *mrb, const mrb_value *outer, const char *name, const mrb_value *super,
                       mrb_value *out) {
  PROTECTED(mrb, {
    if (!is_class_like(outer) || mrb_type(*super) != MRB_TT_CLASS) {
      mrb_raise(mrb, E_TYPE_ERROR, "outer must be a class/module and super a class");
    }
    *out = mrb_obj_value(mrb_define_class_under(mrb, mrb_class_ptr(*outer), name, mrb_class_ptr(*super)));
  });
}

int cabby_define_module(mrb_state *mrb, const mrb_value *outer, const char *name, mrb_value *out) {
  PROTECTED(mrb, {
    if (!is_class_like(outer)) {
      mrb_raise(mrb, E_TYPE_ERROR, "outer must be a class/module");
    }
    *out = mrb_obj_value(mrb_define_module_under(mrb, mrb_class_ptr(*outer), name));
  });
}

int cabby_define_const(mrb_state *mrb, const mrb_value *cls, const char *name, const mrb_value *v) {
  PROTECTED(mrb, {
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
  PROTECTED(mrb, {
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

/* The running method's target class and name (go-mruby: ci->proc->target_class
 * and ci->mid). Returns 0 when no Rust method is running. */
int cabby_current_method(mrb_state *mrb, mrb_value *target_class, uint32_t *mid) {
  mrb_callinfo *ci = mrb->c ? mrb->c->ci : NULL;
  if (ci == NULL || ci->proc == NULL || !MRB_PROC_CFUNC_P(ci->proc) || ci->proc->body.func != cabby_trampoline ||
      ci->proc->target_class == NULL) {
    return 0;
  }
  *target_class = mrb_obj_value(ci->proc->target_class);
  *mid = (uint32_t)ci->mid;
  return 1;
}

int cabby_class_of(mrb_state *mrb, const mrb_value *v, mrb_value *out) {
  PROTECTED(mrb, *out = mrb_obj_value(mrb_class(mrb, *v)));
}

int cabby_singleton_class(mrb_state *mrb, const mrb_value *v, mrb_value *out) {
  PROTECTED(mrb, *out = mrb_singleton_class(mrb, *v));
}

int cabby_obj_new(mrb_state *mrb, const mrb_value *cls, int64_t argc, const mrb_value *argv, mrb_value *out) {
  PROTECTED(mrb, {
    if (mrb_type(*cls) != MRB_TT_CLASS) {
      mrb_raise(mrb, E_TYPE_ERROR, "can't instantiate a non-class");
    }
    *out = mrb_obj_new(mrb, mrb_class_ptr(*cls), (mrb_int)argc, argv);
  });
}

int cabby_set_proc_target_class(mrb_state *mrb, const mrb_value *proc, const mrb_value *cls) {
  PROTECTED(mrb, {
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
  volatile int ok_ = 1;
  *out = mrb_nil_value();
  CABBY_PROTECT(mrb, *out = mrb_load_string(mrb, code));
  if (!ok_) {
    *out = mrb_nil_value();
  }
  mrb_gc_protect(mrb, *out);
  return ok_;
}

/* go-mruby _go_mrb_call; `block` may be NULL. A block must be a proc or nil:
 * mruby casts it to a proc without checking. */
int cabby_funcall(mrb_state *mrb, const mrb_value *recv, const char *name, int64_t argc, const mrb_value *argv,
                  const mrb_value *block, mrb_value *out) {
  volatile int ok_ = 1;
  *out = mrb_nil_value();
  CABBY_PROTECT(mrb, {
    mrb_sym mid = mrb_intern_cstr(mrb, name);
    if (block != NULL) {
      if (!mrb_nil_p(*block) && mrb_type(*block) != MRB_TT_PROC) {
        mrb_raise(mrb, E_TYPE_ERROR, "block must be a Proc");
      }
      *out = mrb_funcall_with_block(mrb, *recv, mid, (mrb_int)argc, argv, *block);
    } else {
      *out = mrb_funcall_argv(mrb, *recv, mid, (mrb_int)argc, argv);
    }
  });
  if (!ok_) {
    *out = mrb_nil_value();
  }
  mrb_gc_protect(mrb, *out);
  return ok_;
}

/* go-mruby _go_mrb_yield_argv. */
int cabby_yield(mrb_state *mrb, const mrb_value *block, int64_t argc, const mrb_value *argv, mrb_value *out) {
  volatile int ok_ = 1;
  *out = mrb_nil_value();
  CABBY_PROTECT(mrb, {
    if (mrb_type(*block) != MRB_TT_PROC) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "no block given");
    }
    *out = mrb_yield_argv(mrb, *block, (mrb_int)argc, argv);
  });
  if (!ok_) {
    *out = mrb_nil_value();
  }
  mrb_gc_protect(mrb, *out);
  return ok_;
}

static void check_runnable(mrb_state *mrb, const mrb_value *proc) {
  if (mrb_type(*proc) != MRB_TT_PROC || MRB_PROC_CFUNC_P(mrb_proc_ptr(*proc))) {
    mrb_raise(mrb, E_TYPE_ERROR, "expected a Ruby proc");
  }
}

/* go-mruby Run: mrb_run (not arena-protected, as upstream). */
int cabby_run(mrb_state *mrb, const mrb_value *proc, const mrb_value *self, mrb_value *out) {
  PROTECTED(mrb, {
    check_runnable(mrb, proc);
    *out = mrb_run(mrb, mrb_proc_ptr(*proc), *self);
  });
}

/* go-mruby _go_mrb_context_run: `nlocals` receives the proc's local count,
 * the stack keep for the next run. */
int cabby_context_run(mrb_state *mrb, const mrb_value *proc, const mrb_value *self, uint32_t keep,
                      uint32_t *nlocals, mrb_value *out) {
  PROTECTED(mrb, {
    struct RProc *p;
    check_runnable(mrb, proc);
    p = mrb_proc_ptr(*proc);
    *nlocals = p->body.irep->nlocals;
    *out = mrb_context_run(mrb, p, *self, keep);
  });
}

/* go-mruby _go_mrb_get_args_all: mrb_get_args(mrb, "*&?"). `argv` points into
 * the VM stack; the caller copies it at once. */
int cabby_get_args(mrb_state *mrb, const mrb_value **argv, int64_t *argc, mrb_value *block, int *has_block) {
  PROTECTED(mrb, {
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
  PROTECTED(mrb, {
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
  PROTECTED(mrb, {
    check_hash(mrb, h);
    *out = mrb_hash_get(mrb, *h, *key);
  });
}

int cabby_hash_set(mrb_state *mrb, const mrb_value *h, const mrb_value *key, const mrb_value *v) {
  PROTECTED(mrb, {
    check_hash(mrb, h);
    mrb_hash_set(mrb, *h, *key, *v);
  });
}

int cabby_hash_delete(mrb_state *mrb, const mrb_value *h, const mrb_value *key, mrb_value *out) {
  PROTECTED(mrb, {
    check_hash(mrb, h);
    *out = mrb_hash_delete_key(mrb, *h, *key);
  });
}

int cabby_hash_keys(mrb_state *mrb, const mrb_value *h, mrb_value *out) {
  PROTECTED(mrb, {
    check_hash(mrb, h);
    *out = mrb_hash_keys(mrb, *h);
  });
}

/* ------------------------------------------------------------ parser */

/* go-mruby NewParser: a parser that captures errors. NULL when out of memory. */
struct mrb_parser_state *cabby_parser_new(mrb_state *mrb) {
  struct mrb_parser_state *volatile p = NULL;
  volatile int ok_ = 1;
  CABBY_PROTECT(mrb, p = mrb_parser_new(mrb));
  if (!ok_) {
    mrb->exc = NULL;
    return NULL;
  }
  if (p != NULL) {
    p->capture_errors = 1;
  }
  return p;
}

void cabby_parser_free(struct mrb_parser_state *p) { mrb_parser_free(p); }

/* Parses code[0..len) with an optional compile context. */
int cabby_parser_parse(mrb_state *mrb, struct mrb_parser_state *p, const char *code, size_t len, mrbc_context *ctx) {
  PROTECTED(mrb, {
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

/* go-mruby GenerateCode; `out` is nil when there is nothing to run (code
 * generation failed or the parse had errors). */
int cabby_generate_code(mrb_state *mrb, struct mrb_parser_state *p, mrb_value *out) {
  PROTECTED(mrb, {
    struct RProc *proc = NULL;
    *out = mrb_nil_value();
    if (p->nerr == 0) {
      proc = mrb_generate_code(mrb, p);
    }
    if (proc != NULL) {
      *out = mrb_obj_value(proc);
    }
  });
}

mrbc_context *cabby_ctx_new(mrb_state *mrb) {
  mrbc_context *volatile c = NULL;
  volatile int ok_ = 1;
  CABBY_PROTECT(mrb, c = mrbc_context_new(mrb));
  if (!ok_) {
    mrb->exc = NULL;
    return NULL;
  }
  return c;
}

void cabby_ctx_free(mrb_state *mrb, mrbc_context *c) { mrbc_context_free(mrb, c); }

const char *cabby_ctx_filename(const mrbc_context *c) { return c->filename; }

/* mrbc_filename copies the name into VM-owned memory (freed by mrb_close). */
int cabby_ctx_set_filename(mrb_state *mrb, mrbc_context *c, const char *name) {
  PROTECTED(mrb, mrbc_filename(mrb, c, name));
}

void cabby_ctx_capture_errors(mrbc_context *c, int yes) { c->capture_errors = yes ? 1 : 0; }
