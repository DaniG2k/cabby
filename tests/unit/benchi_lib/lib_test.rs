//! upstream file: etc/benchi/lib/lib_test.go

use cabby_testkit::reference::benchi::module_links;

// Upstream anchors `*cli.Embedded` (the embedded server) and `*anycable.Client` (the
// forked client) at compile time. The Rust harness has no embedded server: its
// server-side anchor is the external-process launcher.
const _: fn() = || {
    let _: Option<&cabby_bench::server::Server> = None;
    let _: Option<&cabby_bench::cable::Client> = None;
};

/// upstream: etc/benchi/lib/lib_test.go#TestModuleLinks
///
/// Runtime-specific adaptation: upstream proves the Benchi module links against the
/// root module through its `replace` directive and against the forked client; the
/// assertion itself is trivial. The external harness's link to the server is the
/// server binary, so the Rust equivalent invariant is that the harness can execute it
/// (Go backend: the bridge reports that it links `cli.Embedded` and the client).
#[test]
fn test_module_links() {
    assert_eq!(1, 1);
    assert!(module_links(), "the harness must reach the server under test");
}
