//! upstream file: vendorlib/go-mruby/gc_test.go

use cabby_testkit::reference::mruby::*;

/// upstream: vendorlib/go-mruby/gc_test.go#TestEnableDisableGC
#[test]
fn test_enable_disable_gc() {
    let mrb = Mrb::new();

    mrb.full_gc();
    mrb.disable_gc();

    if let Err(e) = mrb.load_string("b = []; a = []; a = []") {
        panic!("{}", e.error());
    }

    let orig = mrb.live_object_count();
    mrb.full_gc();

    assert_eq!(
        orig,
        mrb.live_object_count(),
        "Object count was not what was expected after full GC: {orig} {}",
        mrb.live_object_count()
    );

    mrb.enable_gc();
    mrb.full_gc();

    assert_eq!(
        orig - 2,
        mrb.live_object_count(),
        "Object count was not what was expected after full GC: {} {}",
        orig - 2,
        mrb.live_object_count()
    );

    mrb.close();
}

/// upstream: vendorlib/go-mruby/gc_test.go#TestIsDead
/// note: runtime adaptation (dangling C pointer -> closed-VM invariant): go-mruby IsDead after Close reads the freed mrb_state (mrb_object_dead_p on a closed VM). Rust invariant: a value outliving its VM reports is_dead() without touching freed memory. On Go the case runs in its own bridge process (isolated) so no other VM can reuse the freed memory, matching upstream sequential tests; before isolation 1 of about 95 parallel runs failed.
///
/// Runtime adaptation (dangling C pointer -> closed-VM invariant): go-mruby's
/// `IsDead` after `Close` reads the freed `mrb_state` (`mrb_object_dead_p` on a
/// closed VM), which happens to report dead. The Rust invariant is that a value
/// outliving its VM reports `is_dead() == true` without touching freed memory
/// (values hold a weak reference to the VM). The Go run exercises the original
/// path, so its pass depends on freed memory not being reused in between; it
/// runs in a bridge process of its own so no other VM allocates meanwhile.
#[test]
fn test_is_dead() {
    isolated(|| {
        let mrb = Mrb::new();

        let val = mrb.load_string("$a = []").unwrap_or_else(|e| panic!("{}", e.error()));

        assert!(!val.is_dead(), "Value is already dead and should not be");

        mrb.close();

        assert!(val.is_dead(), "Value should be dead and is not");
    });
}
