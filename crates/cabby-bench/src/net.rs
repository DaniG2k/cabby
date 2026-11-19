//! Network helpers shared by the clients, the publisher and the server launcher.

use std::net::{Shutdown, SocketAddr};

use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;

pub(crate) type Ws = WebSocketStream<TcpStream>;

/// The parts of an `http://` or `ws://` URL the harness needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Target {
    pub host: String,
    pub port: u16,
    /// Path and query, at least `/`.
    pub path: String,
}

pub(crate) fn parse_url(url: &str) -> Result<Target, String> {
    let uri: http::Uri = url.parse().map_err(|e| format!("parse {url:?}: {e}"))?;
    let default_port = match uri.scheme_str() {
        Some("ws" | "http") => 80,
        Some(other) => return Err(format!("unsupported URL scheme {other:?} in {url:?}")),
        None => return Err(format!("missing URL scheme in {url:?}")),
    };
    let host = uri.host().ok_or_else(|| format!("missing host in {url:?}"))?;
    let host = host.trim_start_matches('[').trim_end_matches(']').to_owned();
    let port = uri.port_u16().unwrap_or(default_port);
    let path = uri.path_and_query().map_or("/", |p| p.as_str()).to_owned();
    let path = if path.is_empty() { "/".to_owned() } else { path };
    Ok(Target { host, port, path })
}

/// A dialed WebSocket plus a duplicate handle of its socket, which closes the
/// connection abruptly (no close frame) from outside the task that owns `ws`.
pub(crate) struct Dialed {
    pub ws: Ws,
    pub raw: std::net::TcpStream,
}

/// Opens a TCP connection to `target`.
pub(crate) async fn connect_tcp(target: &Target) -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect((target.host.as_str(), target.port)).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

/// Dials `url` and performs the WebSocket opening handshake.
pub(crate) async fn dial(url: &str) -> Result<Dialed, String> {
    let target = parse_url(url)?;
    let stream = connect_tcp(&target).await.map_err(|e| format!("dial tcp {}:{}: {e}", target.host, target.port))?;
    let std_stream = stream.into_std().map_err(|e| e.to_string())?;
    let raw = std_stream.try_clone().map_err(|e| e.to_string())?;
    let stream = TcpStream::from_std(std_stream).map_err(|e| e.to_string())?;
    let (ws, _response) = tokio_tungstenite::client_async(url, stream).await.map_err(|e| e.to_string())?;
    Ok(Dialed { ws, raw })
}

/// Closes a socket without a WebSocket close frame (upstream:
/// `UnderlyingConn().Close()`).
pub(crate) fn abort(raw: &std::net::TcpStream) {
    let _ = raw.shutdown(Shutdown::Both);
}

/// Loopback address with `port`.
pub(crate) fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}
