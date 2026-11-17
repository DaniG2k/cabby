//! The server under test as an external process.
//!
//! The binary is `SUT_BIN`, or by backend: `target/debug/cabby` (Rust) and
//! `target/reference-bin/anycable-go` (Go, built by `cargo xtask reference-build`).
//! Both receive the same AnyCable CLI flags. Every listener gets an isolated port
//! and the environment is cleared of `ANYCABLE_*` variables.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::ports::free_port;
use crate::process::Supervised;
use crate::{Backend, backend, workspace_root};

const START_DEADLINE: Duration = Duration::from_secs(20);
const IO_DEADLINE: Duration = Duration::from_secs(5);

pub fn sut_binary() -> PathBuf {
    if let Ok(p) = std::env::var("SUT_BIN") {
        return PathBuf::from(p);
    }
    let root = workspace_root();
    let path = match backend() {
        Backend::Rust => root.join("target/debug/cabby"),
        Backend::Go => root.join("target/reference-bin/anycable-go"),
    };
    assert!(path.exists(), "SUT binary {} missing: run via `cargo xtask test` or set SUT_BIN", path.display());
    path
}

pub struct Sut {
    pub process: Supervised,
    pub port: u16,
    pub broadcast_port: u16,
}

impl Sut {
    /// Starts the server with extra CLI `args` and waits until it accepts connections.
    pub fn start(args: &[&str]) -> Self {
        Self::try_start(args, &[]).unwrap_or_else(|e| panic!("{e:#}"))
    }

    /// Starts the server, retrying with fresh ports if a listener could not bind
    /// because another process took its port in the meantime.
    pub fn try_start(args: &[&str], env: &[(&str, &str)]) -> Result<Self> {
        const ATTEMPTS: usize = 3;
        let mut last = None;
        for _ in 0..ATTEMPTS {
            match Self::start_once(args, env) {
                Ok(sut) => return Ok(sut),
                Err(e) if format!("{e:#}").contains("address already in use") => last = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last.expect("at least one attempt")).context(format!("{ATTEMPTS} attempts hit port collisions"))
    }

    fn start_once(args: &[&str], env: &[(&str, &str)]) -> Result<Self> {
        let port = free_port();
        let broadcast_port = free_port();
        let mut cmd = Command::new(sut_binary());
        for (k, _) in std::env::vars() {
            if k.starts_with("ANYCABLE_") {
                cmd.env_remove(k);
            }
        }
        cmd.env("ANYCABLE_DISABLE_TELEMETRY", "true");
        cmd.envs(env.iter().copied());
        cmd.arg("--host=127.0.0.1")
            .arg(format!("--port={port}"))
            .arg(format!("--http_broadcast_port={broadcast_port}"))
            .args(args);
        let mut process = Supervised::spawn("sut", cmd)?;
        process.wait_tcp(SocketAddr::from(([127, 0, 0, 1], port)), START_DEADLINE)?;
        Ok(Self { process, port, broadcast_port })
    }

    pub fn addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.port))
    }

    /// Performs a WebSocket opening handshake on `path` and returns the HTTP status.
    pub fn handshake_status(&self, path: &str, headers: &[(&str, &str)]) -> u16 {
        handshake_status(self.addr(), path, headers).unwrap_or_else(|e| {
            self.process.propagate_pending();
            panic!("handshake failed: {e:#}\nSUT output:\n{}", self.process.output())
        })
    }
}

impl Drop for Sut {
    fn drop(&mut self) {
        let _ = self.process.stop(Duration::from_secs(5));
    }
}

/// Raw RFC 6455 opening handshake; returns the response status code.
pub fn handshake_status(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Result<u16> {
    let mut stream = TcpStream::connect_timeout(&addr, IO_DEADLINE).context("connecting")?;
    stream.set_read_timeout(Some(IO_DEADLINE))?;
    stream.set_write_timeout(Some(IO_DEADLINE))?;
    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Protocol: actioncable-v1-json\r\n"
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes())?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).context("reading status line")?;
    let code = line.split_whitespace().nth(1).and_then(|c| c.parse().ok());
    match code {
        Some(c) => Ok(c),
        None => bail!("malformed status line {line:?}"),
    }
}
