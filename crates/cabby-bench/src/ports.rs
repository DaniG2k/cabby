//! Loopback ports for servers that are told their port before they start: a
//! server under test, a `nats-server`, a URL in another process's
//! configuration.
//!
//! Binding `127.0.0.1:0`, reading the port and closing the socket leaves the
//! port free until the server binds it, and in between any other process can
//! take it: the OS hands the same ephemeral port to another `bind(0)` or to an
//! outgoing connection. A reserved port ([`reserve`]) comes from [`RANGE`]
//! instead, below the ephemeral ranges the OS picks from on its own (Linux
//! 32768–60999, macOS 49152–65535 by default), and is never handed to two
//! processes that use this module at the same time:
//!
//! * Among processes, a block of [`BLOCK`] ports belongs to the process that
//!   holds the lock (`flock`) on the block's file in [`lock_dir`]. The OS
//!   releases the lock when the process exits, crashed or not.
//! * Within a process, a port has one holder at a time ([`Port`]); a released
//!   port goes to the back of the queue, so it is handed out again only after
//!   every other free port of the process's blocks.
//! * A port that something else listens on (a process that does not use this
//!   module) fails a bind probe and is dropped from the queue.
//!
//! A server can still find its port taken (by such a process, after the
//! probe). Supervisors check that the listener they wait for is the server's
//! own ([`listening`]) and restart a server that failed to bind on new ports,
//! a bounded number of times, logging each retry.

use std::collections::VecDeque;
use std::fs::{File, TryLockError};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

/// The ports handed out, below the OS's ephemeral ranges.
pub const RANGE: std::ops::Range<u16> = 20000..30000;

/// Ports per lock file.
pub const BLOCK: u16 = 50;

const BLOCKS: usize = ((RANGE.end - RANGE.start) / BLOCK) as usize;

/// Where the block locks live: one directory for every process of the user
/// (`$TMPDIR/cabby-test-ports`), so separate test binaries, runs and
/// checkouts share them.
pub fn lock_dir() -> PathBuf {
    std::env::temp_dir().join("cabby-test-ports")
}

#[derive(Default)]
struct Pool {
    /// Locked block files, held until the process exits.
    blocks: Vec<File>,
    free: VecDeque<u16>,
    /// The next block to try.
    next: Option<usize>,
}

impl Pool {
    /// A free port that nothing listens on now, claiming a new block when
    /// none is left.
    fn take(&mut self) -> u16 {
        loop {
            match self.free.pop_front() {
                Some(port) if TcpListener::bind(("127.0.0.1", port)).is_ok() => return port,
                // Something that does not reserve ports listens there: never hand it out.
                Some(_) => {}
                None => self.claim_block(),
            }
        }
    }

    fn give_back(&mut self, port: u16) {
        self.free.push_back(port);
    }

    /// Locks the next block no other holder has locked; panics when every
    /// block is held.
    fn claim_block(&mut self) {
        let dir = lock_dir();
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("creating {}: {e}", dir.display()));
        // Processes start at different blocks, so they rarely try the same one.
        let mut next = self.next.unwrap_or(std::process::id() as usize % BLOCKS);
        for _ in 0..BLOCKS {
            let index = next;
            next = (next + 1) % BLOCKS;
            let first = RANGE.start + index as u16 * BLOCK;
            let path = dir.join(format!("{first}.lock"));
            let file = File::options()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .unwrap_or_else(|e| panic!("opening {}: {e}", path.display()));
            match file.try_lock() {
                Ok(()) => {
                    self.free.extend(first..first + BLOCK);
                    self.blocks.push(file);
                    self.next = Some(next);
                    return;
                }
                // Another process holds it (or this one, through another pool).
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Error(e)) => panic!("locking {}: {e}", path.display()),
            }
        }
        panic!(
            "no free block of loopback ports: all {BLOCKS} blocks of {}..{} in {} are held by other processes",
            RANGE.start,
            RANGE.end,
            dir.display()
        );
    }
}

fn pool() -> MutexGuard<'static, Pool> {
    static POOL: OnceLock<Mutex<Pool>> = OnceLock::new();
    POOL.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner())
}

/// A reserved loopback port, held until dropped. Keep it as long as anything
/// uses the port (a server, or a configuration that names it).
#[derive(Debug, PartialEq, Eq)]
pub struct Port(u16);

impl Port {
    pub fn get(&self) -> u16 {
        self.0
    }

    /// Keeps the port reserved until the process exits and returns it.
    pub fn keep(self) -> u16 {
        let port = self.0;
        std::mem::forget(self);
        port
    }

    /// Takes the port out of use for this process: a server could not bind it.
    pub fn retire(self) {
        std::mem::forget(self);
    }
}

impl Drop for Port {
    fn drop(&mut self) {
        pool().give_back(self.0);
    }
}

impl std::fmt::Display for Port {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A reserved loopback port that nothing listens on now.
pub fn reserve() -> Port {
    Port(pool().take())
}

/// Whether process `pid` listens on loopback `port` itself, so that a
/// supervisor that waits for its server is not satisfied by another process
/// listening there (the server then fails with "address already in use",
/// which the supervisor can retry). Linux: a LISTEN socket on `port` among
/// the process's descriptors (procfs). Elsewhere: the port accepts
/// connections and `lsof` lists the socket as the process's; without a
/// working `lsof`, accepting connections counts.
pub fn listening(pid: u32, port: u16) -> bool {
    if std::path::Path::new("/proc/net/tcp").exists() {
        return listener_in_procfs(pid, port);
    }
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    TcpStream::connect_timeout(&addr, Duration::from_millis(20)).is_ok() && listener_per_lsof(pid, port)
}

fn listener_per_lsof(pid: u32, port: u16) -> bool {
    let out = Command::new("lsof")
        .args(["-a", "-p", &pid.to_string(), &format!("-iTCP:{port}"), "-sTCP:LISTEN", "-nP", "-t"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output();
    match out {
        // `lsof -t` prints the PID of a match; it exits with 1 when nothing matches.
        Ok(out) => !out.stdout.is_empty() || !(out.status.success() || out.status.code() == Some(1)),
        Err(_) => true,
    }
}

/// Whether process `pid` holds a TCP socket in LISTEN state on `port` (Linux procfs).
fn listener_in_procfs(pid: u32, port: u16) -> bool {
    const LISTEN: &str = "0A";
    let mut inodes = Vec::new();
    for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(text) = std::fs::read_to_string(table) else { continue };
        for line in text.lines().skip(1) {
            let f: Vec<&str> = line.split_whitespace().collect();
            let (Some(local), Some(state), Some(inode)) = (f.get(1), f.get(3), f.get(9)) else { continue };
            let local_port = local.rsplit(':').next().and_then(|p| u16::from_str_radix(p, 16).ok());
            if *state == LISTEN && local_port == Some(port) {
                inodes.push(format!("socket:[{inode}]"));
            }
        }
    }
    if inodes.is_empty() {
        return false;
    }
    let Ok(fds) = std::fs::read_dir(format!("/proc/{pid}/fd")) else { return false };
    fds.flatten().filter_map(|e| std::fs::read_link(e.path()).ok()).any(|t| {
        let t = t.to_string_lossy();
        inodes.iter().any(|i| *i == t)
    })
}

/// [`reserve`] `N` ports.
pub fn reserve_n<const N: usize>() -> [Port; N] {
    std::array::from_fn(|_| reserve())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pool behind [`reserve`] is shared by the tests running in parallel,
    /// so the queue is checked on a pool of the test's own; it locks blocks of
    /// its own (a second lock in one process conflicts as another process's
    /// would).
    #[test]
    fn ports_are_in_the_range_and_handed_out_once() {
        let mut pool = Pool::default();
        let mut ports: Vec<u16> = (0..2 * BLOCK).map(|_| pool.take()).collect();
        assert!(ports.iter().all(|p| RANGE.contains(p)), "{ports:?}");
        ports.sort_unstable();
        ports.dedup();
        assert_eq!(usize::from(2 * BLOCK), ports.len(), "a port is handed out once");
        assert_eq!(2, pool.blocks.len());
    }

    #[test]
    fn a_released_port_comes_back_after_the_other_free_ones() {
        let mut pool = Pool::default();
        let first = pool.take();
        pool.give_back(first);
        let others: Vec<u16> = (0..BLOCK - 1).map(|_| pool.take()).collect();
        assert!(!others.contains(&first), "{first} handed out again before the other free ports");
        assert_eq!(first, pool.take());
    }

    #[test]
    fn a_port_something_else_listens_on_is_skipped() {
        let mut pool = Pool::default();
        let port = pool.take();
        let listener = TcpListener::bind(("127.0.0.1", port)).expect("binding the reserved port");
        pool.give_back(port);
        let others: Vec<u16> = (0..2 * BLOCK).map(|_| pool.take()).collect();
        assert!(!others.contains(&port), "{port} handed out while something listens on it");
        drop(listener);
    }

    #[test]
    fn a_block_has_one_holder() {
        let mut pool = Pool::default();
        let port = pool.take();
        let first = RANGE.start + (port - RANGE.start) / BLOCK * BLOCK;
        let file = File::options().write(true).open(lock_dir().join(format!("{first}.lock"))).expect("lock file");
        assert!(matches!(file.try_lock(), Err(TryLockError::WouldBlock)), "block {first} is locked once");
        let mut other = Pool::default();
        assert!(!(first..first + BLOCK).contains(&other.take()), "another holder gets another block");
    }

    #[test]
    fn a_listener_of_another_process_is_not_the_servers() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        assert!(listening(std::process::id(), port), "this process listens on {port}");
        let mut other = Command::new("sleep").arg("5").spawn().expect("spawning another process");
        assert!(!listening(other.id(), port), "another process does not listen on {port}");
        let _ = other.kill();
        let _ = other.wait();
    }

    #[test]
    fn kept_and_retired_ports_do_not_come_back() {
        let (kept, retired) = (reserve(), reserve());
        let (kept, retired) = (kept.keep(), {
            let n = retired.get();
            retired.retire();
            n
        });
        assert!(!pool().free.contains(&kept) && !pool().free.contains(&retired));
    }
}
