//! HTTP RPC transport (upstream `rpc/http.go`).
//!
//! Wire format (pinned by `tests/unit/rpc/http_test.rs` against the Go client):
//! `POST <host>/<connect|command|disconnect>`, `Content-Type: application/json`,
//! body = Go `encoding/json` of the protobuf struct (snake_case keys, empty values
//! omitted, enums as integers), `Authorization: Bearer <secret>` when configured,
//! and every metadata pair as an `X-Anycable-Meta-<key>` header. Response
//! statuses map to gRPC codes: 401 Unauthenticated, 400/422 InvalidArgument,
//! other non-200 Unknown; transport errors are DeadlineExceeded when the context
//! is done, Unauthenticated for TLS failures, Unavailable otherwise.

use std::sync::Arc;

use cabby_core::pending;
use cabby_core::protos;

use super::{BoxFuture, ClientHelper, Config, Context, RpcError, RpcService, RpcStatus};

/// Calls the application over HTTP (upstream `HTTPService`). Keeps up to
/// `concurrency` idle keep-alive connections and guards calls with a circuit
/// breaker (opens after >= 10 requests in a 10s window with >= 80% failures).
#[derive(Debug)]
pub struct HttpService {
    _private: (),
}

impl HttpService {
    pub fn new(_config: &Config) -> Result<Self, RpcError> {
        pending!("rpc::new_http_service")
    }
}

impl RpcService for HttpService {
    fn connect<'a>(
        &'a self,
        _ctx: &'a Context,
        _request: protos::ConnectionRequest,
    ) -> BoxFuture<'a, Result<protos::ConnectionResponse, RpcStatus>> {
        pending!("rpc::http_connect")
    }
    fn command<'a>(
        &'a self,
        _ctx: &'a Context,
        _request: protos::CommandMessage,
    ) -> BoxFuture<'a, Result<protos::CommandResponse, RpcStatus>> {
        pending!("rpc::http_command")
    }
    fn disconnect<'a>(
        &'a self,
        _ctx: &'a Context,
        _request: protos::DisconnectRequest,
    ) -> BoxFuture<'a, Result<protos::DisconnectResponse, RpcStatus>> {
        pending!("rpc::http_disconnect")
    }
}

/// Readiness of an [`HttpService`] (upstream `httpClientHelper`): not ready while
/// the circuit breaker is open.
#[derive(Debug)]
pub struct HttpClientHelper {
    _service: Arc<HttpService>,
}

impl HttpClientHelper {
    pub fn new(service: Arc<HttpService>) -> Self {
        Self { _service: service }
    }
}

impl ClientHelper for HttpClientHelper {
    fn ready(&self) -> Result<(), RpcError> {
        pending!("rpc::http_client_helper_ready")
    }
    fn supports_active_conns(&self) -> bool {
        pending!("rpc::http_client_helper_supports_active_conns")
    }
    fn active_conns(&self) -> usize {
        pending!("rpc::http_client_helper_active_conns")
    }
    fn close(&self) {
        pending!("rpc::http_client_helper_close")
    }
}
