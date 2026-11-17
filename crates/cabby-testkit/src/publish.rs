//! Minimal publishers for AnyCable broadcast adapters, as an application (the
//! `anycable` Ruby gem) uses them: HTTP `POST /_broadcast`, Redis `PUBLISH`
//! (legacy `redis` adapter), Redis `XADD` (`redisx`) and NATS `PUB`. Each call
//! opens one connection and waits for the server's acknowledgement (HTTP status,
//! Redis reply, NATS `PONG` after the publish), so a returned `Ok` means the
//! message was accepted. Every read and write has a deadline.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::raw_http;

const IO_DEADLINE: Duration = Duration::from_secs(5);

/// Where an application publishes broadcasts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// AnyCable HTTP broadcast endpoint (`http_broadcast_url`), with an optional
    /// `Authorization: Bearer <key>`.
    Http { addr: SocketAddr, path: String, key: Option<String> },
    /// Legacy Redis adapter: `PUBLISH <channel> <payload>`.
    RedisPublish { addr: SocketAddr, channel: String },
    /// `redisx` adapter: `XADD <stream> * payload <payload>`.
    RedisStream { addr: SocketAddr, stream: String },
    /// NATS adapter: `PUB <subject> <payload>`.
    Nats { addr: SocketAddr, subject: String },
}

impl Target {
    /// Publishes one raw broadcast payload (a JSON document).
    pub fn publish(&self, payload: &str) -> Result<()> {
        match self {
            Target::Http { addr, path, key } => {
                let auth = key.as_ref().map(|k| format!("Bearer {k}"));
                let mut headers = vec![("Content-Type", "application/json")];
                if let Some(a) = &auth {
                    headers.push(("Authorization", a.as_str()));
                }
                let res = raw_http::request(*addr, "POST", path, &headers, Some(payload.as_bytes()))
                    .with_context(|| format!("broadcasting to http://{addr}{path}"))?;
                // features/runner.rb#broadcast: anything but 201 is a failure.
                if res.status != 201 {
                    bail!("Broadcast returned unexpected status: {} ({})", res.status, res.text());
                }
                Ok(())
            }
            Target::RedisPublish { addr, channel } => redis(*addr, &["PUBLISH", channel, payload]).map(drop),
            Target::RedisStream { addr, stream } => redis(*addr, &["XADD", stream, "*", "payload", payload]).map(drop),
            Target::Nats { addr, subject } => nats_publish(*addr, subject, payload),
        }
    }
}

fn connect(addr: SocketAddr) -> Result<TcpStream> {
    let s = TcpStream::connect_timeout(&addr, IO_DEADLINE).with_context(|| format!("connecting to {addr}"))?;
    s.set_read_timeout(Some(IO_DEADLINE))?;
    s.set_write_timeout(Some(IO_DEADLINE))?;
    Ok(s)
}

/// Sends one RESP command and returns the first reply line (without CRLF).
pub fn redis(addr: SocketAddr, args: &[&str]) -> Result<String> {
    let mut s = connect(addr)?;
    let mut cmd = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        cmd.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        cmd.extend_from_slice(a.as_bytes());
        cmd.extend_from_slice(b"\r\n");
    }
    s.write_all(&cmd)?;
    let mut reader = BufReader::new(s);
    let mut line = String::new();
    reader.read_line(&mut line).context("reading the Redis reply")?;
    let line = line.trim_end().to_owned();
    if line.starts_with('-') || line.is_empty() {
        bail!("Redis {} failed: {line:?}", args[0]);
    }
    if let Some(len) = line.strip_prefix('$').and_then(|l| l.parse::<usize>().ok()) {
        let mut value = String::new();
        reader.read_line(&mut value)?;
        return Ok(value.trim_end().chars().take(len).collect());
    }
    Ok(line)
}

/// `PUBSUB NUMSUB <channel>`: how many clients subscribe to `channel` (0 when
/// Redis cannot be reached).
pub fn redis_subscribers(addr: SocketAddr, channel: &str) -> i64 {
    let Ok(mut s) = connect(addr) else { return 0 };
    let cmd = format!("*3\r\n$6\r\nPUBSUB\r\n$6\r\nNUMSUB\r\n${}\r\n{channel}\r\n", channel.len());
    if s.write_all(cmd.as_bytes()).is_err() {
        return 0;
    }
    // *2 / $<len> / <channel> / :<count>
    let mut reader = BufReader::new(s);
    let mut lines = Vec::new();
    for _ in 0..4 {
        let mut l = String::new();
        if reader.read_line(&mut l).unwrap_or(0) == 0 {
            break;
        }
        lines.push(l.trim_end().to_owned());
    }
    lines.get(3).and_then(|l| l.strip_prefix(':')).and_then(|n| n.parse().ok()).unwrap_or(0)
}

/// `CONNECT`, `PUB`, then `PING`; succeeds on the `PONG` (the server processed
/// the publish before answering the ping).
pub fn nats_publish(addr: SocketAddr, subject: &str, payload: &str) -> Result<()> {
    let mut s = connect(addr)?;
    let mut reader = BufReader::new(s.try_clone()?);
    let mut info = String::new();
    reader.read_line(&mut info).context("reading NATS INFO")?;
    if !info.starts_with("INFO") {
        bail!("unexpected NATS greeting {info:?}");
    }
    let msg = format!(
        "CONNECT {{\"verbose\":false,\"pedantic\":false,\"lang\":\"cabby-testkit\",\"version\":\"0\",\"protocol\":1}}\r\n\
         PUB {subject} {}\r\n{payload}\r\nPING\r\n",
        payload.len()
    );
    s.write_all(msg.as_bytes())?;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).context("waiting for NATS PONG")? == 0 {
            bail!("NATS closed the connection before PONG");
        }
        match line.trim_end() {
            "PONG" => return Ok(()),
            "PING" => s.write_all(b"PONG\r\n")?,
            l if l.starts_with("-ERR") => bail!("NATS publish failed: {l}"),
            _ => {}
        }
    }
}
