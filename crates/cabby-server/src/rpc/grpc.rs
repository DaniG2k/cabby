//! gRPC transport for the RPC controller (upstream `defaultDialer`,
//! `grpcClientHelper`, and the `grpc-list://` multi-host resolver).

use cabby_core::pending;

use super::{Config, Dialed, RpcError};

/// URL scheme listing several hosts: `grpc-list://host1:port,host2:port`.
pub const MULTI_HOST_SCHEME: &str = "grpc-list";

/// Round-robin over all resolved addresses, keepalive pings every 10s, TLS per
/// config, message size limits per config.
pub fn default_dialer(_config: &Config) -> Result<Dialed, RpcError> {
    pending!("rpc::grpc_default_dialer")
}
