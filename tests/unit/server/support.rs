//! Request fixtures mirroring Go's `httptest.NewRequest`.

use cabby_server::server::HttpRequest;

/// `httptest.NewRequest(method, target, nil)`: host `example.com` (or the absolute
/// URL's host) and peer address `192.0.2.1:1234`.
pub fn new_request(method: &str, target: &str) -> HttpRequest {
    let host = target
        .split_once("://")
        .map(|(_, rest)| rest.split(['/', '?']).next().unwrap_or("").to_owned())
        .unwrap_or_else(|| "example.com".to_owned());
    HttpRequest {
        method: method.into(),
        target: target.into(),
        host,
        headers: vec![],
        remote_addr: "192.0.2.1:1234".into(),
        tls: false,
    }
}

/// `req.Header.Set(name, value)`: replaces any existing value of the header.
pub fn set_header(req: &mut HttpRequest, name: &str, value: &str) {
    req.headers.retain(|(n, _)| !n.eq_ignore_ascii_case(name));
    req.headers.push((name.into(), value.into()));
}
