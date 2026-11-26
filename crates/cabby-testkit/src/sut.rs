//! The server under test as an external process.
//!
//! The binary is `SUT_BIN`, or by backend: `target/debug/cabby` (Rust) and
//! `target/reference-bin/anycable-go` (Go, built by `cargo xtask reference-build`).
//! Both receive the same AnyCable CLI flags. Every listener gets an isolated port,
//! and the server inherits no `CABBY_*`, `ANYCABLE_*`, `PORT` or `REDIS_URL`
//! variable apart from [`KEPT_VARS`] and `CABBY_TEST_*` ([`scrub_env`], D-064).

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

pub use cabby_bench::server_env::{KEPT_VARS, scrub_env, scrubbed};

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
    /// Pass host and ports as `<env_prefix>HOST`/`<env_prefix>PORT`/
    /// `<env_prefix>HTTP_BROADCAST_PORT` instead of flags (settings entirely from
    /// the environment, like the upstream embedded target).
    pub via_env: bool,
    /// The prefix of the variables `via_env` sets: `ANYCABLE_` (the default,
    /// which both servers read) or `CABBY_` (Cabby only, D-061).
    pub env_prefix: &'static str,
}

impl Default for SutOptions {
    fn default() -> Self {
        Self { separate_broadcast_port: true, via_env: false, env_prefix: "ANYCABLE_" }
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
        Self::try_start_with_ports(args, env, options, |[]| Listeners::default()).map(|(sut, [])| sut)
    }

    /// [`Sut::try_start_with`] for a server with `N` more listeners of its own,
    /// such as `--api_port` or `--pusher_api_port`: `listeners` turns `N` free
    /// ports into arguments and variables. Every attempt gets new ports, so a
    /// retry after a collision does not reuse a taken port, and waits until the
    /// server listens on each of them, so a listener that fails after the main
    /// one is retried too. The ports must be the server process's own listeners
    /// (not a child's, such as Cabby's embedded `nats-server`). Returns the
    /// server and the ports it listens on.
    pub fn try_start_with_ports<const N: usize>(
        args: &[&str],
        env: &[(&str, &str)],
        options: SutOptions,
        listeners: impl Fn([u16; N]) -> Listeners,
    ) -> Result<(Self, [u16; N])> {
        retry_with_fresh_ports(|ports| {
            let extra = listeners(ports);
            let mut all_args = args.to_vec();
            all_args.extend(extra.args.iter().map(String::as_str));
            let mut all_env = env.to_vec();
            all_env.extend(extra.env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
            Self::start_once(&all_args, &all_env, options, &ports)
        })
    }

    fn start_once(args: &[&str], env: &[(&str, &str)], options: SutOptions, extra_ports: &[u16]) -> Result<Self> {
        let port = free_port();
        let broadcast_port = if options.separate_broadcast_port { free_port() } else { port };
        let mut cmd = Command::new(sut_binary());
        scrub_env(&mut cmd);
        cmd.env("ANYCABLE_DISABLE_TELEMETRY", "true");
        cmd.envs(env.iter().copied());
        if options.via_env {
            let prefix = options.env_prefix;
            cmd.env(format!("{prefix}HOST"), "127.0.0.1").env(format!("{prefix}PORT"), port.to_string());
            if options.separate_broadcast_port {
                cmd.env(format!("{prefix}HTTP_BROADCAST_PORT"), broadcast_port.to_string());
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
        for &p in extra_ports {
            process.wait_listening(SocketAddr::from(([127, 0, 0, 1], p)), START_DEADLINE)?;
        }
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

/// Arguments and environment variables for the extra listeners of
/// [`Sut::try_start_with_ports`].
#[derive(Debug, Default)]
pub struct Listeners {
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
}

const START_ATTEMPTS: usize = 3;

/// Runs `attempt` with `N` free ports, again with new ones (up to
/// [`START_ATTEMPTS`] times) while it fails because a port was taken.
fn retry_with_fresh_ports<const N: usize, T>(mut attempt: impl FnMut([u16; N]) -> Result<T>) -> Result<(T, [u16; N])> {
    let mut last = None;
    for _ in 0..START_ATTEMPTS {
        let ports = std::array::from_fn(|_| free_port());
        match attempt(ports) {
            Ok(t) => return Ok((t, ports)),
            Err(e) if port_taken(&e) => last = Some(e),
            Err(e) => return Err(e),
        }
    }
    Err(last.expect("at least one attempt")).context(format!("{START_ATTEMPTS} attempts hit port collisions"))
}

/// Whether a start failed because a listener's port was taken: Go prints
/// `listen tcp <addr>: bind: address already in use`, Cabby `listen tcp <addr>:
/// Address already in use (os error 48)` (98 on Linux).
fn port_taken(e: &anyhow::Error) -> bool {
    format!("{e:#}").to_ascii_lowercase().contains("address already in use")
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins D-064: servers under test inherit no server configuration.
    #[test]
    fn server_config_variables_are_scrubbed() {
        let vars = [
            "ANYCABLE_PORT",
            "CABBY_PORT",
            "CABBY_REDIS_URL",
            "CABBY_BACKEND",
            "CABBY_NATS_SERVER",
            "CABBY_NATS_SERVER_BIN",
            "CABBY_TEST_REDIS_URL",
            "PATH",
            "PORT",
        ];
        assert_eq!(scrubbed(vars), vec!["ANYCABLE_PORT", "CABBY_PORT", "CABBY_REDIS_URL", "CABBY_BACKEND", "PORT"]);
        assert_eq!(scrubbed(["REDIS_URL", "HOME"]), vec!["REDIS_URL"]);
    }

    /// What each server prints when a listener's port is taken (captured from
    /// the binaries), as [`Supervised::wait_listening`] reports it.
    fn exited_with(line: &str) -> anyhow::Error {
        anyhow::anyhow!("sut exited (exit status: 1) before listening on 127.0.0.1:50695. Output:\n{line}")
    }

    #[test]
    fn port_collisions_are_recognized_in_both_servers_texts() {
        for line in [
            "WebSocket server at http://127.0.0.1:50695 stopped: listen tcp 127.0.0.1:50695: Address already in use (os error 48)",
            "API HTTP server at http://127.0.0.1:50695 stopped: listen tcp 127.0.0.1:50695: Address already in use (os error 98)",
            "listen tcp 127.0.0.1:50695: bind: address already in use",
        ] {
            assert!(port_taken(&exited_with(line)), "{line}");
        }
        assert!(!port_taken(&exited_with("dial tcp 127.0.0.1:6379: connect: connection refused")));
    }

    /// A collision is retried with new ports; another error is not retried.
    #[test]
    fn a_port_collision_is_retried_with_fresh_ports() {
        let mut seen = Vec::new();
        let taken = "listen tcp 127.0.0.1:50695: Address already in use (os error 48)";
        let (n, ports) = retry_with_fresh_ports(|ports: [u16; 2]| {
            seen.push(ports);
            if seen.len() == 1 { Err(exited_with(taken)) } else { Ok(seen.len()) }
        })
        .expect("the second attempt starts");
        assert_eq!((2, seen[1]), (n, ports));
        assert!(seen[1].iter().all(|p| !seen[0].contains(p)), "new ports: {seen:?}");

        let mut calls = 0;
        let e = retry_with_fresh_ports(|[]| -> Result<()> {
            calls += 1;
            Err(exited_with("no such file"))
        })
        .expect_err("not a collision");
        assert_eq!(1, calls, "{e:#}");

        let e = retry_with_fresh_ports(|[]| -> Result<()> { Err(exited_with(taken)) }).expect_err("always taken");
        assert!(format!("{e:#}").starts_with("3 attempts hit port collisions"), "{e:#}");
    }
}
