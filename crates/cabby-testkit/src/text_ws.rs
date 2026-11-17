//! Blocking WebSocket client for text-frame protocols (e.g. Pusher), with bounded
//! reads.

use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tungstenite::client::IntoClientRequest;
use tungstenite::{Message, WebSocket};

pub struct TextWs {
    socket: WebSocket<TcpStream>,
}

impl TextWs {
    /// Connects to `ws://<addr><path>` with extra request headers.
    pub fn connect(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Result<Self> {
        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).context("connecting")?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let mut req = format!("ws://{addr}{path}").into_client_request()?;
        for (k, v) in headers {
            req.headers_mut().insert(
                tungstenite::http::HeaderName::from_bytes(k.as_bytes())?,
                tungstenite::http::HeaderValue::from_str(v)?,
            );
        }
        let (socket, _resp) = tungstenite::client(req, stream).map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
        Ok(Self { socket })
    }

    pub fn send_text(&mut self, text: &str) -> Result<()> {
        self.socket.send(Message::text(text)).context("sending")
    }

    /// The next text frame; fails if none arrives before `timeout` or the server closes.
    pub fn recv_text(&mut self, timeout: Duration) -> Result<String> {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                bail!("no text frame within {timeout:?}");
            }
            self.socket.get_ref().set_read_timeout(Some(left))?;
            match self.socket.read() {
                Ok(Message::Text(t)) => return Ok(t.as_str().to_owned()),
                Ok(Message::Close(f)) => bail!("server closed the connection: {f:?}"),
                Ok(_) => continue,
                Err(tungstenite::Error::Io(e))
                    if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) =>
                {
                    bail!("no text frame within {timeout:?}")
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}
