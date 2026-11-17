//! WebSocket transport (upstream package `ws`).

use cabby_core::pending;

/// Origin policy built from the comma-separated `allowed_origins` setting.
#[derive(Debug, Clone)]
pub struct OriginCheck {
    _origins: String,
}

/// Builds the origin policy (upstream `ws.CheckOrigin`).
pub fn check_origin(origins: &str) -> OriginCheck {
    OriginCheck { _origins: origins.to_owned() }
}

impl OriginCheck {
    /// Decides a handshake given its `Origin` header (`None` when absent).
    pub fn allows(&self, _origin: Option<&str>) -> bool {
        pending!("ws::check_origin")
    }
}
