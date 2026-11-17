//! Facade for upstream package `ws`.

use serde_json::json;

use crate::{Backend, backend, bridge};

/// `ws.CheckOrigin(origins)(request)` where the request carries `origin` as its
/// `Origin` header (`None`: header absent).
pub fn check_origin(origins: &str, origin: Option<&str>) -> bool {
    match backend() {
        Backend::Rust => cabby_server::ws::check_origin(origins).allows(origin),
        Backend::Go => bridge::call("ws.CheckOrigin", json!({"Origins": origins, "Origin": origin})),
    }
}
