//! Test doubles for the Rust RPC controller's injection points, mirroring what
//! upstream tests inject into the Go controller:
//!
//! * [`TonicRpcClient`]: a plain gRPC client ([`RpcClient`]) built on the generated
//!   tonic bindings; stands in for upstream's mock `pb.RPCClient`, but talks to a
//!   real (fake) service over a socket.
//! * [`ForwardingService`]: an [`RpcService`] that forwards to a gRPC endpoint,
//!   copying incoming metadata (for in-process dialer tests).
//! * [`AlwaysReady`]: a [`ClientHelper`] that is always ready (upstream `MockState`).
//! * [`CancelOnCall`]: a client that passes each call to a gRPC endpoint (where
//!   the fake records it), then cancels the session context and returns the bare
//!   context error, like upstream's mock in `TestAuthenticate/Canceled_during_RPC`.

use std::sync::Arc;
use std::time::Duration;

use cabby_core::protos;
use cabby_core::protos::rpc_client::RpcClient as GrpcClient;
use cabby_server::rpc::{
    BoxFuture, ClientError, ClientHelper, Context, Dialer, RpcClient, RpcError, RpcService, RpcStatus,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

/// Always ready, no active connection support (upstream `MockState{ready: true}`).
#[derive(Debug, Default, Clone, Copy)]
pub struct AlwaysReady;

impl ClientHelper for AlwaysReady {
    fn ready(&self) -> Result<(), RpcError> {
        Ok(())
    }
    fn supports_active_conns(&self) -> bool {
        false
    }
    fn active_conns(&self) -> usize {
        0
    }
    fn close(&self) {}
}

/// gRPC client for `127.0.0.1:<port>` targets. Applies the context's metadata,
/// deadline and cancellation to each call.
#[derive(Debug, Clone)]
pub struct TonicRpcClient {
    channel: Channel,
}

impl TonicRpcClient {
    /// Lazily connecting client for `target` (`host:port`).
    pub fn new(target: &str) -> Self {
        let _guard = crate::runtime::runtime().enter();
        let channel = Endpoint::from_shared(format!("http://{target}")).expect("valid gRPC target").connect_lazy();
        Self { channel }
    }

    /// A dialer returning this client with [`AlwaysReady`].
    pub fn dialer(target: &str) -> Dialer {
        let client: Arc<dyn RpcClient> = Arc::new(Self::new(target));
        Arc::new(move |_config| Ok((Arc::clone(&client), Arc::new(AlwaysReady) as Arc<dyn ClientHelper>)))
    }
}

fn request<T>(ctx: &Context, message: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(message);
    for (k, v) in &ctx.metadata {
        if let (Ok(k), Ok(v)) = (
            k.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>(),
            v.parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>(),
        ) {
            req.metadata_mut().append(k, v);
        }
    }
    if let Some(deadline) = ctx.deadline {
        req.set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()).max(Duration::from_millis(1)));
    }
    req
}

fn status(s: tonic::Status) -> ClientError {
    ClientError::Status(RpcStatus { code: s.code(), message: s.message().to_owned() })
}

async fn cancellable<T>(
    ctx: &Context,
    call: impl Future<Output = Result<tonic::Response<T>, tonic::Status>>,
) -> Result<T, ClientError> {
    tokio::select! {
        r = call => r.map(tonic::Response::into_inner).map_err(status),
        // Like grpc-go, a cancelled call surfaces as a CANCELED status, not as the
        // bare context error.
        () = ctx.cancel.cancelled() => Err(ClientError::Status(RpcStatus {
            code: tonic::Code::Cancelled,
            message: "context canceled".into(),
        })),
    }
}

impl RpcClient for TonicRpcClient {
    fn connect<'a>(
        &'a self,
        ctx: &'a Context,
        message: protos::ConnectionRequest,
    ) -> BoxFuture<'a, Result<protos::ConnectionResponse, ClientError>> {
        Box::pin(
            async move { cancellable(ctx, GrpcClient::new(self.channel.clone()).connect(request(ctx, message))).await },
        )
    }
    fn command<'a>(
        &'a self,
        ctx: &'a Context,
        message: protos::CommandMessage,
    ) -> BoxFuture<'a, Result<protos::CommandResponse, ClientError>> {
        Box::pin(
            async move { cancellable(ctx, GrpcClient::new(self.channel.clone()).command(request(ctx, message))).await },
        )
    }
    fn disconnect<'a>(
        &'a self,
        ctx: &'a Context,
        message: protos::DisconnectRequest,
    ) -> BoxFuture<'a, Result<protos::DisconnectResponse, ClientError>> {
        Box::pin(async move {
            cancellable(ctx, GrpcClient::new(self.channel.clone()).disconnect(request(ctx, message))).await
        })
    }
}

/// Serves calls by forwarding them to a gRPC endpoint with the incoming metadata.
#[derive(Debug, Clone)]
pub struct ForwardingService {
    client: TonicRpcClient,
}

impl ForwardingService {
    pub fn new(target: &str) -> Self {
        Self { client: TonicRpcClient::new(target) }
    }
}

impl RpcService for ForwardingService {
    fn connect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::ConnectionRequest,
    ) -> BoxFuture<'a, Result<protos::ConnectionResponse, ClientError>> {
        self.client.connect(ctx, request)
    }
    fn command<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::CommandMessage,
    ) -> BoxFuture<'a, Result<protos::CommandResponse, ClientError>> {
        self.client.command(ctx, request)
    }
    fn disconnect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::DisconnectRequest,
    ) -> BoxFuture<'a, Result<protos::DisconnectResponse, ClientError>> {
        self.client.disconnect(ctx, request)
    }
}

/// Records each call at `target` (forwarding it), then cancels `session` and
/// returns [`ClientError::Canceled`]: the upstream mock's
/// `func(...) error { cancel(); return canceledCtx.Err() }`.
#[derive(Debug, Clone)]
pub struct CancelOnCall {
    inner: TonicRpcClient,
    session: CancellationToken,
}

impl CancelOnCall {
    pub fn new(target: &str, session: CancellationToken) -> Self {
        Self { inner: TonicRpcClient::new(target), session }
    }

    /// A dialer returning this client with [`AlwaysReady`].
    pub fn dialer(target: &str, session: CancellationToken) -> Dialer {
        let client: Arc<dyn RpcClient> = Arc::new(Self::new(target, session));
        Arc::new(move |_config| Ok((Arc::clone(&client), Arc::new(AlwaysReady) as Arc<dyn ClientHelper>)))
    }

    async fn cancel<T>(&self, forwarded: impl Future<Output = Result<T, ClientError>>) -> Result<T, ClientError> {
        // The forwarded reply only serves to record the call; it is discarded.
        let _ = forwarded.await;
        self.session.cancel();
        Err(ClientError::Canceled)
    }
}

impl RpcClient for CancelOnCall {
    fn connect<'a>(
        &'a self,
        ctx: &'a Context,
        message: protos::ConnectionRequest,
    ) -> BoxFuture<'a, Result<protos::ConnectionResponse, ClientError>> {
        Box::pin(self.cancel(self.inner.connect(ctx, message)))
    }
    fn command<'a>(
        &'a self,
        ctx: &'a Context,
        message: protos::CommandMessage,
    ) -> BoxFuture<'a, Result<protos::CommandResponse, ClientError>> {
        Box::pin(self.cancel(self.inner.command(ctx, message)))
    }
    fn disconnect<'a>(
        &'a self,
        ctx: &'a Context,
        message: protos::DisconnectRequest,
    ) -> BoxFuture<'a, Result<protos::DisconnectResponse, ClientError>> {
        Box::pin(self.cancel(self.inner.disconnect(ctx, message)))
    }
}
