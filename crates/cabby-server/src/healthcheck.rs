//! `cabby-healthcheck`: a health probe for container images that have no shell
//! or curl. It sends `GET <health path>` to `<host>:<port>` and succeeds on a
//! 200 response; when the server serves TLS (`ssl_cert` and `ssl_key` set), it
//! only checks that the port accepts connections. The host, the port, the path
//! and the TLS settings come from the variables the server's flags read
//! (`CABBY_*`, then `ANYCABLE_*`); the probe can't see command-line flags.
//!
//! The host (`CABBY_HOST`, then `ANYCABLE_HOST`) gives the addresses to try, in
//! order, until one is healthy: `127.0.0.1` then `::1` when it is empty,
//! `localhost` or an unspecified address (`0.0.0.0`, `::`); the address itself
//! for an IP literal; every resolved address, IPv4 first, for a host name.

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream, ToSocketAddrs};
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

/// The addresses to probe for the host setting (`CABBY_HOST`, then
/// `ANYCABLE_HOST`), in order; see the module documentation.
pub fn hosts(get: impl Fn(&str) -> Option<String>) -> Result<Vec<IpAddr>, String> {
    candidates(&setting(&get, "HOST"), |name| {
        (name, 0).to_socket_addrs().map(|addrs| addrs.map(|a| a.ip()).collect()).map_err(|e| e.to_string())
    })
}

/// The addresses to probe for the host `raw`, with `resolve` looking up host names.
fn candidates(
    raw: &str,
    resolve: impl Fn(&str) -> Result<Vec<IpAddr>, String>,
) -> Result<Vec<IpAddr>, String> {
    const LOOPBACK: [IpAddr; 2] = [IpAddr::V4(Ipv4Addr::LOCALHOST), IpAddr::V6(Ipv6Addr::LOCALHOST)];
    let host = raw.trim();
    let host = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')).unwrap_or(host);
    if host.is_empty() || host.eq_ignore_ascii_case("localhost") {
        return Ok(LOOPBACK.to_vec());
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(if ip.is_unspecified() { LOOPBACK.to_vec() } else { vec![ip] });
    }
    let mut addrs = resolve(host).map_err(|e| format!("resolving {host:?}: {e}"))?;
    if addrs.is_empty() {
        return Err(format!("resolving {host:?}: no addresses"));
    }
    // Stable: keeps the resolver's order within each family.
    addrs.sort_by_key(IpAddr::is_ipv6);
    Ok(addrs)
}

/// Checks each of `addrs` in turn (a connection with `tls`, else `GET path`)
/// and succeeds on the first healthy one; otherwise reports every failure.
pub fn check_any(addrs: &[SocketAddr], tls: bool, path: &str) -> Result<(), String> {
    let mut failures = Vec::with_capacity(addrs.len());
    for &addr in addrs {
        match if tls { connect(addr) } else { probe(addr, path) } {
            Ok(()) => return Ok(()),
            Err(e) => failures.push(e),
        }
    }
    Err(failures.join("; "))
}

/// The whole probe, with `get` reading the environment.
pub fn run(get: impl Fn(&str) -> Option<String>) -> Result<(), String> {
    let port = port(&get)?;
    let addrs: Vec<SocketAddr> = hosts(&get)?.into_iter().map(|ip| SocketAddr::new(ip, port)).collect();
    check_any(&addrs, tls(&get), &health_path(&get))
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

    const V4_LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
    const V6_LOOPBACK: IpAddr = IpAddr::V6(Ipv6Addr::LOCALHOST);

    fn no_lookups(name: &str) -> Result<Vec<IpAddr>, String> {
        panic!("unexpected lookup of {name:?}")
    }

    #[test]
    fn unspecified_empty_and_localhost_hosts_try_both_loopbacks() {
        for raw in ["", " ", "0.0.0.0", "::", "[::]", "0:0:0:0:0:0:0:0", "localhost", "LocalHost", "[localhost]"] {
            assert_eq!(candidates(raw, no_lookups), Ok(vec![V4_LOOPBACK, V6_LOOPBACK]), "{raw:?}");
        }
    }

    #[test]
    fn an_ip_literal_is_probed_as_given() {
        assert_eq!(candidates("127.0.0.2", no_lookups), Ok(vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2))]));
        assert_eq!(candidates("10.1.2.3", no_lookups), Ok(vec![IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))]));
        assert_eq!(candidates("::1", no_lookups), Ok(vec![V6_LOOPBACK]));
        assert_eq!(candidates("[::1]", no_lookups), Ok(vec![V6_LOOPBACK]));
        assert_eq!(candidates("fd00::5", no_lookups), Ok(vec!["fd00::5".parse::<IpAddr>().unwrap()]));
    }

    #[test]
    fn a_host_name_tries_every_resolved_address_ipv4_first() {
        let v6a: IpAddr = "fd00::1".parse().unwrap();
        let v6b: IpAddr = "fd00::2".parse().unwrap();
        let v4a = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let v4b = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let resolve = |name: &str| {
            assert_eq!("cabby.internal", name);
            Ok(vec![v6a, v4a, v6b, v4b])
        };
        assert_eq!(candidates("cabby.internal", resolve), Ok(vec![v4a, v4b, v6a, v6b]));
        assert_eq!(
            candidates("nowhere.invalid", |_| Err("not found".to_owned())),
            Err("resolving \"nowhere.invalid\": not found".to_owned())
        );
        assert_eq!(
            candidates("empty.invalid", |_| Ok(Vec::new())),
            Err("resolving \"empty.invalid\": no addresses".to_owned())
        );
    }

    #[test]
    fn hosts_reads_the_server_variables() {
        assert_eq!(hosts(env(&[])), Ok(vec![V4_LOOPBACK, V6_LOOPBACK]));
        assert_eq!(hosts(env(&[("ANYCABLE_HOST", "127.0.0.2")])), Ok(vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2))]));
        assert_eq!(hosts(env(&[("CABBY_HOST", "::1"), ("ANYCABLE_HOST", "127.0.0.2")])), Ok(vec![V6_LOOPBACK]));
    }

    #[test]
    fn the_first_healthy_candidate_succeeds() {
        let (_reserved, refusing) = refusing_addr();
        let (unhealthy, first) = serve_once("HTTP/1.1 503 Service Unavailable");
        let (healthy, second) = serve_once("HTTP/1.1 200 OK");
        assert_eq!(check_any(&[refusing, unhealthy, healthy], false, "/health"), Ok(()));
        first.join().expect("server");
        second.join().expect("server");

        let (one, first) = serve_once("HTTP/1.1 503 Service Unavailable");
        let (two, second) = serve_once("HTTP/1.1 500 Internal Server Error");
        let err = check_any(&[one, two], false, "/health").expect_err("no healthy candidate");
        assert_eq!(
            "GET /health answered \"HTTP/1.1 503 Service Unavailable\"; \
             GET /health answered \"HTTP/1.1 500 Internal Server Error\"",
            err
        );
        first.join().expect("server");
        second.join().expect("server");
    }

    /// A server listening on `::1` only is found from an unspecified host
    /// after `127.0.0.1` (skipped where IPv6 loopback is unavailable).
    #[test]
    fn an_unspecified_host_reaches_a_server_on_ipv6_loopback() {
        let Ok(listener) = TcpListener::bind("[::1]:0") else { return };
        let port = listener.local_addr().expect("addr").port();
        let server = std::thread::spawn(move || {
            // The probe may first reach whatever owns this port on 127.0.0.1.
            let (mut conn, _) = listener.accept().expect("accept");
            let mut request = Vec::new();
            let mut buf = [0u8; 512];
            while !request.ends_with(b"\r\n\r\n") {
                let n = conn.read(&mut buf).expect("read");
                assert!(n > 0, "the probe closed before finishing its request");
                request.extend_from_slice(&buf[..n]);
            }
            conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").expect("write");
        });
        let port = port.to_string();
        let get = move |k: &str| match k {
            "CABBY_HOST" => Some("::".to_owned()),
            "CABBY_PORT" => Some(port.clone()),
            _ => None,
        };
        assert_eq!(run(get), Ok(()));
        server.join().expect("server");
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

    /// A loopback address whose port is bound but not listening, so connecting
    /// fails (refused, or on macOS timed out); the port stays reserved while the
    /// socket lives.
    fn refusing_addr() -> (tokio::net::TcpSocket, SocketAddr) {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket.bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("bind");
        let addr = socket.local_addr().expect("addr");
        (socket, addr)
    }

    /// With TLS the probe only connects: it can't speak HTTPS without a TLS stack.
    #[test]
    fn with_tls_a_connection_is_healthy() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        assert_eq!(connect(addr), Ok(()));
        assert_eq!(check_any(&[addr], true, "/health"), Ok(()));
        let (_reserved, refusing) = refusing_addr();
        assert_eq!(check_any(&[refusing, addr], true, "/health"), Ok(()));
        assert!(connect(refusing).expect_err("nothing listens").starts_with("connecting to "));
        drop(listener);
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

        let (_reserved, refusing) = refusing_addr();
        assert!(probe(refusing, "/health").expect_err("nothing listens").starts_with("connecting to "));
    }
}
