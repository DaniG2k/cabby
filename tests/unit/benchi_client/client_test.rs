//! upstream file: etc/benchi/client/client_test.go

use cabby_testkit::reference::benchi::new_cable_client;

/// upstream: etc/benchi/client/client_test.go#TestClientPackageCompiles
///
/// Upstream is compile-only: `var _ *Client = nil` etc. prove the forked package builds
/// and its exported symbols resolve. Runtime-specific adaptation: the type references
/// are kept (they are checked by `rustc`), and the runtime counterpart of the linkage,
/// constructing an unconnected client, is exercised; no client state is asserted.
#[test]
fn test_client_package_compiles() {
    let _: Option<&cabby_bench::cable::Client> = None;
    let _: Option<&cabby_bench::cable::Subscription> = None;
    let _ = cabby_bench::cable::Event::default();
    let _ = cabby_bench::cable::Command::default();

    let _client = new_cable_client("ws://127.0.0.1:1/cable");
}
