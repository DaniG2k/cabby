//! `cabby-healthcheck`: a health probe for container images that have no shell
//! or curl. It sends `GET <health path>` to `127.0.0.1:<port>` and succeeds on a
//! 200 response; when the server serves TLS (`ssl_cert` and `ssl_key` set), it
//! only checks that the port accepts connections. The port, the path and the TLS
//! settings come from the variables the server's flags read (`CABBY_*`, then
//! `ANYCABLE_*`); the probe can't see command-line flags.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

/// How long the probe waits to connect, and then for each read and write.
pub const TIMEOUT: Duration = Duration::from_secs(2);

/// The server port: the first of `CABBY_PORT`, `ANYCABLE_PORT` and `PORT` that
/// is set, else 8080, parsed as the server parses it (Go `strconv.ParseInt` with
/// base prefixes and underscores). An empty value keeps 8080, as it does for the
/// server.
pub fn port(get: impl Fn(&str) -> Option<String>) -> Result<u16, String> {
    match ["CABBY_PORT", "ANYCABLE_PORT", "PORT"].iter().find_map(|k| get(k).map(|v| (k, v))) {
        None => Ok(8080),
        Some((_, v)) if v.is_empty() => Ok(8080),
        Some((k, v)) => crate::cli::parse_go_int(&v)
            .ok()
            .and_then(|n| u16::try_from(n).ok())
            .ok_or_else(|| format!("{k}={v:?} is not a port number")),
    }
}

/// The first of `CABBY_<name>` and `ANYCABLE_<name>` that is set (an empty
/// value counts as set, as for the server), else empty.
fn setting(get: &impl Fn(&str) -> Option<String>, name: &str) -> String {
    get(&format!("CABBY_{name}")).or_else(|| get(&format!("ANYCABLE_{name}"))).unwrap_or_default()
}

/// Whether the server serves TLS: both `ssl_cert` and `ssl_key` are set.
pub fn tls(get: impl Fn(&str) -> Option<String>) -> bool {
    !setting(&get, "SSL_CERT").is_empty() && !setting(&get, "SSL_KEY").is_empty()
}

/// Connects to `addr`; `Ok(())` when the port accepts the connection.
pub fn connect(addr: SocketAddr) -> Result<(), String> {
    TcpStream::connect_timeout(&addr, TIMEOUT).map(drop).map_err(|e| format!("connecting to {addr}: {e}"))
}

/// The health path: `CABBY_HEALTH_PATH`, then `ANYCABLE_HEALTH_PATH`, else `/health`.
pub fn health_path(get: impl Fn(&str) -> Option<String>) -> String {
    ["CABBY_HEALTH_PATH", "ANYCABLE_HEALTH_PATH"]
        .iter()
        .find_map(|k| get(k))
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| "/health".to_owned())
}

/// Sends `GET path` to `addr`; `Ok(())` when the response status is 200.
pub fn probe(addr: SocketAddr, path: &str) -> Result<(), String> {
    let mut stream = TcpStream::connect_timeout(&addr, TIMEOUT).map_err(|e| format!("connecting to {addr}: {e}"))?;
    stream.set_read_timeout(Some(TIMEOUT)).map_err(|e| e.to_string())?;
    stream.set_write_timeout(Some(TIMEOUT)).map_err(|e| e.to_string())?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).map_err(|e| format!("sending the request: {e}"))?;
    let mut head = Vec::new();
    let mut buf = [0u8; 128];
    while !head.windows(2).any(|w| w == b"\r\n") && head.len() < 512 {
        let n = stream.read(&mut buf).map_err(|e| format!("reading the response: {e}"))?;
        if n == 0 {
            break;
        }
        head.extend_from_slice(&buf[..n]);
    }
    let text = String::from_utf8_lossy(&head);
    let status_line = text.lines().next().unwrap_or_default();
    if status_line.split_whitespace().nth(1) == Some("200") {
        Ok(())
    } else {
        Err(format!("GET {path} answered {status_line:?}"))
    }
}

/// The whole probe, with `get` reading the environment.
pub fn run(get: impl Fn(&str) -> Option<String>) -> Result<(), String> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port(&get)?));
    if tls(&get) { connect(addr) } else { probe(addr, &health_path(&get)) }
}

#[cfg(test)]
mod tests {
    use std::net::{SocketAddr, TcpListener};

    use super::*;

    fn env(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |k| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| (*v).to_owned())
    }

    #[test]
    fn port_and_path_follow_the_server_variables() {
        assert_eq!(port(env(&[])), Ok(8080));
        assert_eq!(port(env(&[("PORT", "5000")])), Ok(5000));
        assert_eq!(port(env(&[("ANYCABLE_PORT", "8081"), ("PORT", "5000")])), Ok(8081));
        assert_eq!(port(env(&[("CABBY_PORT", "9000"), ("ANYCABLE_PORT", "8081")])), Ok(9000));
        assert_eq!(port(env(&[("CABBY_PORT", "")])), Ok(8080));
        assert_eq!(port(env(&[("CABBY_PORT", "x")])), Err("CABBY_PORT=\"x\" is not a port number".to_owned()));
        // The server's integer syntax (Go `strconv.ParseInt(s, 0, 64)`).
        assert_eq!(port(env(&[("CABBY_PORT", "0x1F90")])), Ok(8080));
        assert_eq!(port(env(&[("CABBY_PORT", "+8_080")])), Ok(8080));
        assert_eq!(port(env(&[("CABBY_PORT", "70000")])), Err("CABBY_PORT=\"70000\" is not a port number".to_owned()));
        assert_eq!(health_path(env(&[])), "/health");
        assert_eq!(health_path(env(&[("ANYCABLE_HEALTH_PATH", "/up")])), "/up");
        assert_eq!(health_path(env(&[("CABBY_HEALTH_PATH", "/ok"), ("ANYCABLE_HEALTH_PATH", "/up")])), "/ok");
    }

    #[test]
    fn tls_is_configured_like_the_server() {
        assert!(!tls(env(&[])));
        assert!(!tls(env(&[("CABBY_SSL_CERT", "/c.pem")])));
        assert!(tls(env(&[("CABBY_SSL_CERT", "/c.pem"), ("CABBY_SSL_KEY", "/k.pem")])));
        assert!(tls(env(&[("ANYCABLE_SSL_CERT", "/c.pem"), ("ANYCABLE_SSL_KEY", "/k.pem")])));
        // An empty CABBY_* value counts as set, as for the server.
        assert!(!tls(env(&[("CABBY_SSL_CERT", ""), ("ANYCABLE_SSL_CERT", "/c.pem"), ("ANYCABLE_SSL_KEY", "/k.pem")])));
    }

    /// With TLS the probe only connects: it can't speak HTTPS without a TLS stack.
    #[test]
    fn with_tls_a_connection_is_healthy() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        assert_eq!(connect(addr), Ok(()));
        drop(listener);
        assert!(connect(addr).expect_err("nothing listens").starts_with("connecting to "));
    }

    /// A one-shot server answering with `status_line`; the handle yields the
    /// request it received.
    fn serve_once(status_line: &'static str) -> (SocketAddr, std::thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handle = std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().expect("accept");
            let mut request = Vec::new();
            let mut buf = [0u8; 512];
            while !request.ends_with(b"\r\n\r\n") {
                let n = conn.read(&mut buf).expect("read");
                assert!(n > 0, "the probe closed before finishing its request");
                request.extend_from_slice(&buf[..n]);
            }
            conn.write_all(format!("{status_line}\r\nContent-Length: 0\r\n\r\n").as_bytes()).expect("write");
            String::from_utf8_lossy(&request).into_owned()
        });
        (addr, handle)
    }

    #[test]
    fn a_200_is_healthy_and_anything_else_is_not() {
        let (addr, server) = serve_once("HTTP/1.1 200 OK");
        assert_eq!(probe(addr, "/health"), Ok(()));
        assert!(server.join().expect("server").starts_with("GET /health HTTP/1.1\r\n"));

        let (addr, server) = serve_once("HTTP/1.1 503 Service Unavailable");
        assert_eq!(probe(addr, "/health"), Err("GET /health answered \"HTTP/1.1 503 Service Unavailable\"".to_owned()));
        server.join().expect("server");

        let closed = TcpListener::bind("127.0.0.1:0").expect("bind").local_addr().expect("addr");
        assert!(probe(closed, "/health").expect_err("nothing listens").starts_with("connecting to "));
    }
}
