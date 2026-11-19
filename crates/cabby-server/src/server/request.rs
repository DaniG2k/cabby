//! Request inspection: health, headers extraction, `RequestInfo`, `FetchUID` and
//! CORS (upstream `server/{health_handler,headers_extractor,request_info,cors}.go`).

use std::collections::HashMap;

use super::{ConnInfo, origin_allowed};

/// The parts of an incoming HTTP request the server inspects.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpRequest {
    pub method: String,
    /// The request target as received: a path (`/cable?x=1`) or an absolute URL.
    pub target: String,
    /// The `Host` the request was addressed to.
    pub host: String,
    /// Header fields in arrival order; names are case-insensitive.
    pub headers: Vec<(String, String)>,
    /// Peer address, `ip:port`.
    pub remote_addr: String,
    /// Whether the request arrived over TLS.
    pub tls: bool,
}

impl HttpRequest {
    /// The request as the server received it (header values that are not valid
    /// UTF-8 are converted lossily).
    pub fn from_parts(parts: &http::request::Parts, conn: &ConnInfo) -> Self {
        let target = if parts.uri.scheme().is_some() {
            parts.uri.to_string()
        } else {
            parts.uri.path_and_query().map_or_else(|| parts.uri.to_string(), |pq| pq.as_str().to_owned())
        };
        let host = parts
            .headers
            .get(http::header::HOST)
            .map(|h| String::from_utf8_lossy(h.as_bytes()).into_owned())
            .or_else(|| parts.uri.authority().map(|a| a.as_str().to_owned()))
            .unwrap_or_default();
        let headers = parts
            .headers
            .iter()
            .map(|(k, v)| (k.as_str().to_owned(), String::from_utf8_lossy(v.as_bytes()).into_owned()))
            .collect();
        HttpRequest {
            method: parts.method.as_str().to_owned(),
            target,
            host,
            headers,
            remote_addr: conn.remote_addr.map(|a| a.to_string()).unwrap_or_default(),
            tls: conn.tls,
        }
    }

    /// The first value of a header (Go `Header.Get`; `""` when missing).
    pub fn header(&self, name: &str) -> &str {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map_or("", |(_, v)| v.as_str())
    }
}

/// A response produced by a plain HTTP handler.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// The health check reply (upstream `healthMsg`).
pub const HEALTH_MESSAGE: &[u8] = b"Ah, ha, ha, ha, stayin' alive, stayin' alive.";

/// Upstream `HealthHandler`: always 200.
pub fn health_handler(_req: &HttpRequest) -> HttpResponse {
    HttpResponse { status: 200, body: HEALTH_MESSAGE.to_vec() }
}

/// Extracts the headers forwarded to the RPC application (upstream `HeadersExtractor`;
/// `extract_headers` is upstream `FromRequest`).
pub trait HeadersExtractor {
    fn extract_headers(&self, req: &HttpRequest) -> HashMap<String, String>;
}

/// Upstream `remoteAddrHeader`.
pub const REMOTE_ADDR_HEADER: &str = "REMOTE_ADDR";

/// Upstream `DefaultHeadersExtractor`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DefaultHeadersExtractor {
    pub auth_header: String,
    pub headers: Vec<String>,
    pub cookies: Vec<String>,
}

impl HeadersExtractor for DefaultHeadersExtractor {
    fn extract_headers(&self, req: &HttpRequest) -> HashMap<String, String> {
        let mut res = HashMap::with_capacity(self.headers.len() + 2);

        for header in &self.headers {
            let value = req.header(header);
            let value = if header.eq_ignore_ascii_case("cookie") {
                parse_cookies(value, &self.cookies)
            } else {
                value.to_owned()
            };
            if !value.is_empty() {
                res.insert(header.clone(), value);
            }
        }

        if !self.auth_header.is_empty() {
            // A token passed as a WebSocket subprotocol (`anycable-token.<token>`).
            let protocols = req.header("sec-websocket-protocol");
            if protocols.contains("anycable-token.") {
                let token =
                    protocols.split(',').map(str::trim).find_map(|p| p.strip_prefix("anycable-token.")).unwrap_or("");
                if !token.is_empty() {
                    res.insert(self.auth_header.clone(), token.to_owned());
                }
            }

            let value = req.header(&self.auth_header);
            if !value.is_empty() {
                res.insert(self.auth_header.clone(), value.to_owned());
            }
        }

        res.insert(REMOTE_ADDR_HEADER.to_owned(), split_host_port(&req.remote_addr).unwrap_or("").to_owned());
        res
    }
}

/// Keeps only the cookies named in `filter` (upstream unexported `parseCookies`).
fn parse_cookies(value: &str, filter: &[String]) -> String {
    if filter.is_empty() {
        return value.to_owned();
    }
    let mut result = String::new();
    for cookie in value.split(';') {
        let mut parts = cookie.split('=');
        let (Some(name), Some(_), None) = (parts.next(), parts.next(), parts.next()) else {
            continue;
        };
        if filter.iter().any(|f| f == name) {
            result.push_str(cookie);
            result.push(';');
        }
    }
    result
}

/// Go `net.SplitHostPort`, host part only (`None` on error).
fn split_host_port(addr: &str) -> Option<&str> {
    if let Some(rest) = addr.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        let port = after.strip_prefix(':')?;
        return (!port.contains([':', '[', ']']) && !host.contains('[')).then_some(host);
    }
    let (host, port) = addr.rsplit_once(':')?;
    (!host.contains([':', '[', ']']) && !port.contains(['[', ']'])).then_some(host)
}

/// A request-handling failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct RequestError(pub String);

/// Connection request data (upstream `server.RequestInfo`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestInfo {
    pub uid: String,
    pub url: String,
    pub headers: Option<HashMap<String, String>>,
    anycable_headers: Option<HashMap<String, String>>,
    params: Option<HashMap<String, String>>,
}

impl RequestInfo {
    /// Upstream `NewRequestInfo` (`extractor: None` collects no headers).
    pub fn new(req: &HttpRequest, extractor: Option<&dyn HeadersExtractor>) -> Result<RequestInfo, RequestError> {
        let headers = extractor.map(|e| e.extract_headers(req)).unwrap_or_default();

        // `X-AnyCable-*` headers; the last value of a repeated header wins.
        let mut anycable_headers = HashMap::new();
        for (name, value) in &req.headers {
            if name.get(..11).is_some_and(|p| p.eq_ignore_ascii_case("x-anycable-")) {
                anycable_headers.insert(name.to_ascii_lowercase(), value.clone());
            }
        }

        let uid = fetch_uid(req).map_err(|_| RequestError("failed to retrieve connection uid".to_owned()))?;

        let url = if url_scheme(&req.target).is_some() {
            req.target.clone()
        } else {
            let scheme = if req.tls { "https://" } else { "http://" };
            format!("{scheme}{}{}", req.host, req.target)
        };

        Ok(RequestInfo {
            uid,
            url,
            headers: Some(headers),
            anycable_headers: Some(anycable_headers),
            params: Some(parse_query(&req.target)),
        })
    }

    /// The last value of a URL query parameter (`""` when missing).
    pub fn param(&self, key: &str) -> String {
        self.params.as_ref().and_then(|p| p.get(key)).cloned().unwrap_or_default()
    }

    /// The value of an `X-AnyCable-*` header, case-insensitively (`""` when missing).
    pub fn anycable_header(&self, key: &str) -> String {
        self.anycable_headers.as_ref().and_then(|h| h.get(&key.to_ascii_lowercase())).cloned().unwrap_or_default()
    }
}

/// The scheme of an absolute URL (Go `url.URL.IsAbs`).
fn url_scheme(raw: &str) -> Option<&str> {
    let scheme = &raw[..raw.find(':')?];
    let mut chars = scheme.chars();
    (chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')))
    .then_some(scheme)
}

/// Go `url.URL.Query()` keeping the last value of each key: pairs separated by
/// `&`, `+` decodes as a space, percent-decoding; pairs with `;` or a bad escape
/// are skipped.
fn parse_query(target: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    let without_fragment = target.split('#').next().unwrap_or("");
    let Some((_, query)) = without_fragment.split_once('?') else {
        return params;
    };
    for pair in query.split('&') {
        if pair.is_empty() || pair.contains(';') {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if let (Some(key), Some(value)) = (query_unescape(key), query_unescape(value)) {
            params.insert(key, value);
        }
    }
    params
}

/// The first value of a query parameter of a request target, `""` when missing
/// (Go `r.URL.Query().Get(key)`; decoding as in [`RequestInfo::param`]).
pub fn query_param(target: &str, key: &str) -> String {
    let without_fragment = target.split('#').next().unwrap_or("");
    let Some((_, query)) = without_fragment.split_once('?') else {
        return String::new();
    };
    for pair in query.split('&') {
        if pair.is_empty() || pair.contains(';') {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if let (Some(k), Some(v)) = (query_unescape(k), query_unescape(v))
            && k == key
        {
            return v;
        }
    }
    String::new()
}

/// Go `url.QueryUnescape` (`None` on an invalid escape).
fn query_unescape(s: &str) -> Option<String> {
    if !s.contains(['%', '+']) {
        return Some(s.to_owned());
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    Some(String::from_utf8_lossy(&out).into_owned())
}

/// Upstream `maxRequestIDLength`.
const MAX_REQUEST_ID_LENGTH: usize = 128;

/// The connection UID: a valid `X-Request-ID`, or a generated one (upstream `FetchUID`).
pub fn fetch_uid(req: &HttpRequest) -> Result<String, RequestError> {
    let request_id = req.header("X-Request-ID");
    if valid_request_id(request_id) { Ok(request_id.to_owned()) } else { Ok(nanoid(21)) }
}

/// Only printable non-space ASCII, at most [`MAX_REQUEST_ID_LENGTH`] bytes.
fn valid_request_id(val: &str) -> bool {
    !val.is_empty() && val.len() <= MAX_REQUEST_ID_LENGTH && val.bytes().all(|b| b > b' ' && b <= b'~')
}

/// A random URL-friendly ID (go-nanoid `Nanoid(size)`, at most 64 characters).
fn nanoid(size: usize) -> String {
    const ALPHABET: &[u8; 64] = b"_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut buf = [0u8; 64];
    let bytes = &mut buf[..size.min(64)];
    // The OS generator does not fail in practice; upstream ignores the error too.
    let _ = getrandom::fill(bytes);
    bytes.iter().map(|b| ALPHABET[usize::from(b & 63)] as char).collect()
}

/// Upstream `WriteCORSHeaders`: no `origins` allows any origin.
pub fn write_cors_headers(headers: &mut http::HeaderMap, origin: &str, origins: &[String]) {
    use http::HeaderValue;
    use http::header::{
        ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
        ACCESS_CONTROL_ALLOW_ORIGIN, VARY,
    };

    if origins.is_empty() {
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    } else {
        headers.append(VARY, HeaderValue::from_static("Origin"));
        let hosts: Vec<&str> = origins.iter().map(String::as_str).collect();
        if origin_allowed(origin, &hosts)
            && let Ok(value) = HeaderValue::from_str(&origin.to_lowercase())
        {
            headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, value);
        }
    }
    headers.insert(ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("POST, OPTIONS"));
    headers.insert(ACCESS_CONTROL_ALLOW_CREDENTIALS, HeaderValue::from_static("true"));
    headers.insert(
        ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static(
            "Origin, X-Requested-With, X-Request-ID, Content-Type, Accept, X-CSRF-Token, Authorization",
        ),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_host_port_like_go() {
        assert_eq!(Some("192.0.2.1"), split_host_port("192.0.2.1:1234"));
        assert_eq!(Some("::1"), split_host_port("[::1]:80"));
        assert_eq!(None, split_host_port("::1:80"));
        assert_eq!(None, split_host_port(""));
        assert_eq!(None, split_host_port("host"));
    }

    #[test]
    fn parses_queries_like_go() {
        let q = parse_query("/cable?a=1&b=x+y&c=%41&bad=%zz&semi=1;2&a=2#frag");
        assert_eq!(Some("2"), q.get("a").map(String::as_str));
        assert_eq!(Some("x y"), q.get("b").map(String::as_str));
        assert_eq!(Some("A"), q.get("c").map(String::as_str));
        assert!(!q.contains_key("bad"));
        assert!(!q.contains_key("semi"));
    }

    #[test]
    fn query_param_takes_the_first_value() {
        assert_eq!("1", query_param("/x?a=1&a=2", "a"));
        assert_eq!("x y", query_param("/x?b=x+y#a=3", "b"));
        assert_eq!("", query_param("/x?bad=%zz", "bad"));
        assert_eq!("", query_param("/x", "a"));
    }

    #[test]
    fn generates_21_character_uids() {
        let uid = fetch_uid(&HttpRequest::default()).unwrap();
        assert_eq!(21, uid.len());
    }
}
