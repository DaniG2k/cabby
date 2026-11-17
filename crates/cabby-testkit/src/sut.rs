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
    /// The HTTP broadcast port: a separate isolated port, or `port` when
    /// [`SutOptions::separate_broadcast_port`] is off.
    pub broadcast_port: u16,
}

/// How [`Sut::try_start_with`] passes the isolated ports.
#[derive(Debug, Clone, Copy)]
pub struct SutOptions {
    /// Pass `--http_broadcast_port=<free port>` (the default). Off, the server
    /// keeps its own default, which is the main port when a broadcast key is
    /// configured (upstream `test-conformance-http`/`-embedded`); without a key
    /// it is a fixed port, so turn this off only together with a key.
    pub separate_broadcast_port: bool,
    /// Pass host and ports as `ANYCABLE_HOST`/`ANYCABLE_PORT`/
    /// `ANYCABLE_HTTP_BROADCAST_PORT` instead of flags (settings entirely from
    /// the environment, like the upstream embedded target).
    pub via_env: bool,
}

impl Default for SutOptions {
    fn default() -> Self {
        Self { separate_broadcast_port: true, via_env: false }
    }
}

impl Sut {
    /// Starts the server with extra CLI `args` and waits until it accepts connections.
    pub fn start(args: &[&str]) -> Self {
        Self::try_start(args, &[]).unwrap_or_else(|e| panic!("{e:#}"))
    }

    /// Starts the server, retrying with fresh ports if a listener could not bind
    /// because another process took its port in the meantime.
    pub fn try_start(args: &[&str], env: &[(&str, &str)]) -> Result<Self> {
        Self::try_start_with(args, env, SutOptions::default())
    }

    /// [`Sut::try_start`] with explicit [`SutOptions`].
    pub fn try_start_with(args: &[&str], env: &[(&str, &str)], options: SutOptions) -> Result<Self> {
        const ATTEMPTS: usize = 3;
        let mut last = None;
        for _ in 0..ATTEMPTS {
            match Self::start_once(args, env, options) {
                Ok(sut) => return Ok(sut),
                Err(e) if format!("{e:#}").contains("address already in use") => last = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last.expect("at least one attempt")).context(format!("{ATTEMPTS} attempts hit port collisions"))
    }

    fn start_once(args: &[&str], env: &[(&str, &str)], options: SutOptions) -> Result<Self> {
        let port = free_port();
        let broadcast_port = if options.separate_broadcast_port { free_port() } else { port };
        let mut cmd = Command::new(sut_binary());
        for (k, _) in std::env::vars() {
            if k.starts_with("ANYCABLE_") {
                cmd.env_remove(k);
            }
        }
        cmd.env("ANYCABLE_DISABLE_TELEMETRY", "true");
        cmd.envs(env.iter().copied());
        if options.via_env {
            cmd.env("ANYCABLE_HOST", "127.0.0.1").env("ANYCABLE_PORT", port.to_string());
            if options.separate_broadcast_port {
                cmd.env("ANYCABLE_HTTP_BROADCAST_PORT", broadcast_port.to_string());
            }
        } else {
            cmd.arg("--host=127.0.0.1").arg(format!("--port={port}"));
            if options.separate_broadcast_port {
                cmd.arg(format!("--http_broadcast_port={broadcast_port}"));
            }
        }
        cmd.args(args);
        let mut process = Supervised::spawn("sut", cmd)?;
        process.wait_listening(SocketAddr::from(([127, 0, 0, 1], port)), START_DEADLINE)?;
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
