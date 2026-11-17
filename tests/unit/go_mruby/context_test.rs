//! upstream file: vendorlib/go-mruby/context_test.go

use cabby_testkit::reference::mruby::*;

/// upstream: vendorlib/go-mruby/context_test.go#TestCompileContextFilename
#[test]
fn test_compile_context_filename() {
    let mrb = Mrb::new();

    let mut ctx = CompileContext::new(&mrb);

    assert_eq!(ctx.filename(), "", "bad filename");

    ctx.set_filename("foo");

    assert_eq!(ctx.filename(), "foo", "bad filename");

    ctx.close();
    mrb.close();
}
