// Generates protobuf/gRPC bindings from the pinned upstream `etc/rpc.proto`
// (copied verbatim to proto/rpc.proto; `cargo xtask check-proto` verifies the hash).

/// Every message of `proto/rpc.proto`.
const MESSAGES: &[&str] = &[
    "Env",
    "EnvResponse",
    "PresenceResponse",
    "ConnectionRequest",
    "ConnectionResponse",
    "CommandMessage",
    "CommandResponse",
    "DisconnectRequest",
    "DisconnectResponse",
];

/// Every `map<string,string>` field of `proto/rpc.proto`.
const MAP_FIELDS: &[&str] = &["Env.headers", "Env.cstate", "Env.istate", "EnvResponse.cstate", "EnvResponse.istate"];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/rpc.proto");
    let mut config = tonic_prost_build::configure()
        // Field-level JSON shape matches Go's encoding/json for the generated structs
        // (snake_case names, enums as integers). HTTP RPC wire compatibility is tested
        // separately against Go; this derive alone is not evidence of it.
        .type_attribute(".anycable", "#[derive(serde::Serialize, serde::Deserialize)]")
        .message_attribute(".anycable", "#[serde(default)]");
    // Go's generated structs tag every field `omitempty`: zero values (empty strings,
    // lists and maps, 0, false, nil messages) are left out of the JSON. Listed per
    // message, because a package-wide path would also match the enum variants.
    for message in MESSAGES {
        config = config.field_attribute(
            format!(".anycable.{message}"),
            "#[serde(skip_serializing_if = \"crate::gojson::is_zero\")]",
        );
    }
    // Go's encoding/json writes map keys sorted.
    for field in MAP_FIELDS {
        config = config.field_attribute(
            format!(".anycable.{field}"),
            "#[serde(serialize_with = \"crate::gojson::sorted_string_map\")]",
        );
    }
    config
        // The service has an RPC named `Connect`, which would clash with tonic's
        // generated `RpcClient::connect` constructor; build clients with `RpcClient::new`.
        .build_transport(false)
        .compile_protos(&["proto/rpc.proto"], &["proto"])?;
    Ok(())
}
