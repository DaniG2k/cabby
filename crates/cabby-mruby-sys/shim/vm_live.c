/*
 * mruby's vm.c plus two functions that need its private helpers (cipush,
 * cipop).
 * The build compiles this file in place of mruby/src/vm.c, so the call-frame
 * handling below is exactly the VM's own.
 */

#include "../mruby/src/vm.c"

/* Pops the call frames of the current context above index `nth_ci`, as
 * mrb_funcall_with_block does when a call it protects raises. cipop moves the
 * stack of a frame's closure environment to the heap, so no environment keeps
 * pointing into stack slots that later calls reuse. */
void cabby_unwind_ci(mrb_state *mrb, ptrdiff_t nth_ci);

void cabby_unwind_ci(mrb_state *mrb, ptrdiff_t nth_ci) {
  if (mrb->c->cibase == NULL) {
    return;
  }
  while (nth_ci < (mrb->c->ci - mrb->c->cibase)) {
    mrb->c->stack = mrb->c->ci->stackent;
    cipop(mrb);
  }
}

/* Runs a Ruby proc with `self` on a call frame of its own above the current
 * one, as mrb_toplevel_run_keep does for code loaded inside a method: the
 * frame is marked CI_ACC_SKIP, so the VM returns to this caller (and a raise
 * jumps to the caller's jump buffer) instead of continuing in the frame below,
 * and a fiber switch sees the C boundary. Top-level code ends with OP_STOP,
 * which leaves the frame in place, while a block's OP_RETURN pops it: the
 * frame is popped here when it is still there. */
mrb_value cabby_run_nested(mrb_state *mrb, struct RProc *p, mrb_value self);

mrb_value cabby_run_nested(mrb_state *mrb, struct RProc *p, mrb_value self) {
  ptrdiff_t nth_ci = mrb->c->ci - mrb->c->cibase;
  int n = mrb->c->ci->nregs;
  mrb_callinfo *ci = cipush(mrb);
  mrb_value v;

  ci->mid = ci[-1].mid;
  ci->proc = p;
  ci->stackent = mrb->c->stack;
  ci->argc = 0;
  ci->target_class = p->target_class;
  ci->acc = CI_ACC_SKIP;
  mrb->c->stack = mrb->c->stack + n;
  v = mrb_context_run(mrb, p, self, 0);
  cabby_unwind_ci(mrb, nth_ci);
  return v;
}
