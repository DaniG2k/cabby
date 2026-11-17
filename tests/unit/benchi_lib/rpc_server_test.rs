//! upstream file: etc/benchi/lib/rpc_server_test.go

use std::sync::Arc;

use cabby_testkit::http_stub;
use cabby_testkit::reference::benchi::*;

/// upstream: etc/benchi/lib/rpc_server_test.go#TestRPCServer_AfterClose
#[test]
fn test_rpc_server_after_close() {
    let rpc = RpcServer::new(RpcServerConfig {
        session_closed: Some(Arc::new(|sid: &str| sid == "closed")),
        ..Default::default()
    });

    let post = |path: &str, body: &str| {
        let (status, _) = http_stub::post(&format!("{}{path}", rpc.url()), "application/json", body.as_bytes())
            .unwrap_or_else(|e| panic!("POST {path}: {e:#}"));
        assert_eq!(200, status, "POST {path}");
    };

    post("/connect", r#"{"env":{"url":"ws://localhost/cable?bench_sid=closed"}}"#);
    post("/connect", r#"{"env":{"url":"ws://localhost/cable?bench_sid=open"}}"#);
    post("/command", r#"{"command":"subscribe","identifier":"x","connection_identifiers":"{\"id\":\"closed\"}"}"#);
    post("/command", r#"{"command":"subscribe","identifier":"x","connection_identifiers":"{\"id\":\"open\"}"}"#);
    post("/disconnect", r#"{"identifiers":"{\"id\":\"closed\"}"}"#);

    assert_eq!(1, rpc.stats(RpcMethod::Connect).after_close);
    assert_eq!(1, rpc.stats(RpcMethod::Command).after_close);
    // Disconnect calls always come after the close; they are not counted.
    assert_eq!(0, rpc.stats(RpcMethod::Disconnect).after_close);

    rpc.close();
}
