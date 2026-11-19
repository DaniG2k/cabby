//! In-process RPC service dialer (upstream `rpc/inprocess.go`).

use std::sync::Arc;

use cabby_core::protos;

use super::{BoxFuture, ClientError, ClientHelper, Context, Dialer, RpcClient, RpcService};

/// Calls a service directly. The controller's outgoing metadata (the context's
/// metadata) is what the service sees as incoming metadata, as with grpchan's
/// in-process channel; errors pass through unchanged.
struct InprocessClient {
    service: Arc<dyn RpcService>,
}

impl RpcClient for InprocessClient {
    fn connect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::ConnectionRequest,
    ) -> BoxFuture<'a, Result<protos::ConnectionResponse, ClientError>> {
        self.service.connect(ctx, request)
    }
    fn command<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::CommandMessage,
    ) -> BoxFuture<'a, Result<protos::CommandResponse, ClientError>> {
        self.service.command(ctx, request)
    }
    fn disconnect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::DisconnectRequest,
    ) -> BoxFuture<'a, Result<protos::DisconnectResponse, ClientError>> {
        self.service.disconnect(ctx, request)
    }
}

/// A dialer whose client calls `service` directly, turning the controller's
/// outgoing metadata into the service's incoming metadata.
pub fn new_inprocess_service_dialer(service: Arc<dyn RpcService>, helper: Arc<dyn ClientHelper>) -> Dialer {
    let client: Arc<dyn RpcClient> = Arc::new(InprocessClient { service });
    Arc::new(move |_config| Ok((Arc::clone(&client), Arc::clone(&helper))))
}
