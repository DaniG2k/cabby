//! HTTP server helpers (upstream package `server`).

use cabby_core::pending;

/// Whether an `Origin` header value matches one of the allowed host patterns
/// (exact host, `*` for any, `*.example.com` or `*example.com` wildcards).
pub fn origin_allowed(_origin: &str, _hosts: &[&str]) -> bool {
    pending!("server::origin_allowed")
}
