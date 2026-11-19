/*
 * mruby's gc.c plus one accessor that needs its private definitions (RVALUE,
 * the heap page size, the dead-object test). The build compiles this file in
 * place of mruby/src/gc.c, so the object layout used below is exactly the
 * collector's own.
 */

#include "../mruby/src/gc.c"

/* Whether `p` is a live object of type `tt` in one of this VM's heap pages.
 * Only pages still linked into the heap are searched, so a pointer to a freed
 * page or a swept slot reports dead without being dereferenced. */
int cabby_heap_object_live(mrb_state *mrb, const struct RBasic *p, enum mrb_vtype tt);

int cabby_heap_object_live(mrb_state *mrb, const struct RBasic *p, enum mrb_vtype tt) {
  mrb_heap_page *page;
  uintptr_t addr = (uintptr_t)p;

  for (page = mrb->gc.heaps; page != NULL; page = page->next) {
    uintptr_t base = (uintptr_t)objects(page);
    uintptr_t end = base + sizeof(RVALUE) * MRB_HEAP_PAGE_SIZE;
    if (addr < base || addr >= end) {
      continue;
    }
    if ((addr - base) % sizeof(RVALUE) != 0) {
      return 0;
    }
    if (p->tt != tt) {
      return 0;
    }
    return !is_dead(&mrb->gc, p);
  }
  return 0;
}
