//! In-process RPC service dialer (upstream `rpc/inprocess.go`).

use std::sync::Arc;

use cabby_core::pending;

use super::{ClientHelper, Dialer, RpcService};

/// A dialer whose client calls `service` directly, turning the controller's
/// outgoing metadata into the service's incoming metadata.
pub fn new_inprocess_service_dialer(_service: Arc<dyn RpcService>, _helper: Arc<dyn ClientHelper>) -> Dialer {
    pending!("rpc::new_inprocess_service_dialer")
}
