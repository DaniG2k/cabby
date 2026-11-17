//! Supervised external processes with bounded waits.
//!
//! Output is drained continuously by reader threads, so a child can never block
//! on a full pipe while the test waits for it (the upstream feature runner had
//! that deadlock, see compat/baseline/README.md).

use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use cabby_core::pending::{MARKER, pending_id};

/// Interval between readiness probes. Readiness is observed (TCP accept), and every
/// wait has a deadline; this is not a substitute for synchronization.
const PROBE_INTERVAL: Duration = Duration::from_millis(20);

pub struct Supervised {
    name: String,
    child: Child,
    output: Arc<Mutex<Vec<u8>>>,
    readers: Vec<JoinHandle<()>>,
    status: Option<ExitStatus>,
}

impl Supervised {
    /// Spawns `cmd` with stdout and stderr captured into one buffer.
    pub fn spawn(name: impl Into<String>, mut cmd: Command) -> Result<Self> {
        let name = name.into();
        let mut child = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("spawning {name}: {cmd:?}"))?;
        let output = Arc::new(Mutex::new(Vec::new()));
        let mut readers = Vec::new();
        let streams: [Box<dyn Read + Send>; 2] =
            [Box::new(child.stdout.take().expect("stdout")), Box::new(child.stderr.take().expect("stderr"))];
        for mut stream in streams {
            let sink = Arc::clone(&output);
            readers.push(std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                while let Ok(n @ 1..) = stream.read(&mut buf) {
                    sink.lock().unwrap_or_else(|e| e.into_inner()).extend_from_slice(&buf[..n]);
                }
            }));
        }
        Ok(Self { name, child, output, readers, status: None })
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Everything the process wrote so far (stdout and stderr interleaved).
    pub fn output(&self) -> String {
        String::from_utf8_lossy(&self.output.lock().unwrap_or_else(|e| e.into_inner())).into_owned()
    }

    /// If the process reported a pending marker, re-raises it in the test thread so
    /// the case is classified as pending.
    pub fn propagate_pending(&self) {
        let out = self.output();
        if let Some(id) = pending_id(&out) {
            panic!("{MARKER}{id} (reported by {})", self.name);
        }
    }

    fn poll_exit(&mut self) -> Result<Option<ExitStatus>> {
        if self.status.is_none() {
            self.status = self.child.try_wait()?;
        }
        Ok(self.status)
    }

    /// Waits until `addr` accepts TCP connections. Fails early if the process exits.
    pub fn wait_tcp(&mut self, addr: SocketAddr, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if TcpStream::connect_timeout(&addr, PROBE_INTERVAL).is_ok() {
                return Ok(());
            }
            if let Some(status) = self.poll_exit()? {
                self.join_readers();
                self.propagate_pending();
                bail!("{} exited ({status}) before listening on {addr}. Output:\n{}", self.name, self.output());
            }
            if Instant::now() >= deadline {
                bail!("{} did not listen on {addr} within {timeout:?}. Output:\n{}", self.name, self.output());
            }
            std::thread::sleep(PROBE_INTERVAL);
        }
    }

    /// Waits for the process to exit on its own.
    pub fn wait_exit(&mut self, timeout: Duration) -> Result<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.poll_exit()? {
                self.join_readers();
                return Ok(status);
            }
            if Instant::now() >= deadline {
                bail!("{} still running after {timeout:?}. Output:\n{}", self.name, self.output());
            }
            std::thread::sleep(PROBE_INTERVAL);
        }
    }

    /// Sends SIGTERM, then SIGKILL after `grace`.
    pub fn stop(&mut self, grace: Duration) -> Result<ExitStatus> {
        if let Some(status) = self.poll_exit()? {
            self.join_readers();
            return Ok(status);
        }
        let _ = Command::new("kill").args(["-TERM", &self.child.id().to_string()]).status();
        match self.wait_exit(grace) {
            Ok(status) => Ok(status),
            Err(_) => {
                self.child.kill()?;
                let status = self.child.wait()?;
                self.status = Some(status);
                self.join_readers();
                Ok(status)
            }
        }
    }

    fn join_readers(&mut self) {
        for r in self.readers.drain(..) {
            let _ = r.join();
        }
    }
}

impl Drop for Supervised {
    fn drop(&mut self) {
        if self.status.is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
