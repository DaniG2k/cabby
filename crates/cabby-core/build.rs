// Generates protobuf/gRPC bindings from the pinned upstream `etc/rpc.proto`
// (copied verbatim to proto/rpc.proto; `cargo xtask check-proto` verifies the hash).
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/rpc.proto");
    tonic_prost_build::configure()
        // Field-level JSON shape matches Go's encoding/json for the generated structs
        // (snake_case names, enums as integers). HTTP RPC wire compatibility is tested
        // separately against Go; this derive alone is not evidence of it.
        .type_attribute(".anycable", "#[derive(serde::Serialize, serde::Deserialize)]")
        .message_attribute(".anycable", "#[serde(default)]")
        // The service has an RPC named `Connect`, which would clash with tonic's
        // generated `RpcClient::connect` constructor; build clients with `RpcClient::new`.
        .build_transport(false)
        .compile_protos(&["proto/rpc.proto"], &["proto"])?;
    Ok(())
}
