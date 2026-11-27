//! Sentinel configurations: which connections use TLS (D-115), on the
//! resolved configuration and against a fake Sentinel and a fake master behind
//! TLS terminators.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use super::RedisConfig;
use super::connect::{self, TlsSpec, Topology};

const SERVER_FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
const CLIENT_FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/unit/redis/fixtures");

fn config(url: &str, sentinels: &str) -> RedisConfig {
    RedisConfig { url: url.to_owned(), sentinels: sentinels.to_owned(), ..RedisConfig::new() }
}

/// The TLS mode of a connection kind: `None` (plain), `Some(true)` (TLS without
/// verification) or `Some(false)` (TLS, verified).
fn mode(tls: &Option<TlsSpec>) -> Option<bool> {
    tls.as_ref().map(|t| t.insecure)
}

/// (master, Sentinel nodes) TLS modes of a configuration.
fn modes(c: &RedisConfig) -> (Option<bool>, Option<bool>) {
    let spec = c.connect_spec().expect("connect spec");
    match &spec.topology {
        Topology::Sentinel { tls, .. } => (mode(&spec.tls), mode(tls)),
        other => panic!("not a Sentinel topology: {other:?}"),
    }
}

/// Pins D-115: the Sentinels' scheme decides TLS for the master only, as in
/// AnyCable; `sentinel_tls` turns it on for the Sentinel nodes, with the same
/// verification setting.
#[test]
fn deviation_d115_tls_mode_of_each_connection() {
    const PLAIN: Option<bool> = None;
    const INSECURE: Option<bool> = Some(true);
    const VERIFIED: Option<bool> = Some(false);
    // (sentinel URL, sentinel_tls, tls_verify) -> (master, Sentinel nodes)
    for (sentinels, sentinel_tls, verify, expected) in [
        ("redis://s1:26379,redis://s2:26379", false, false, (PLAIN, PLAIN)),
        ("redis://s1:26379,redis://s2:26379", false, true, (PLAIN, PLAIN)),
        ("redis://s1:26379,redis://s2:26379", true, false, (PLAIN, INSECURE)),
        ("redis://s1:26379,redis://s2:26379", true, true, (PLAIN, VERIFIED)),
        ("rediss://s1:26379,rediss://s2:26379", false, false, (INSECURE, PLAIN)),
        ("rediss://s1:26379,rediss://s2:26379", false, true, (VERIFIED, PLAIN)),
        ("rediss://s1:26379,rediss://s2:26379", true, false, (INSECURE, INSECURE)),
        ("rediss://s1:26379,rediss://s2:26379", true, true, (VERIFIED, VERIFIED)),
        // Without a scheme, as `redis://`; the first URL decides.
        ("s1:26379", false, false, (PLAIN, PLAIN)),
        ("s1:26379", true, false, (PLAIN, INSECURE)),
        ("valkeys://s1:26379,redis://s2:26379", false, false, (INSECURE, PLAIN)),
        ("redis://s1:26379,rediss://s2:26379", false, false, (PLAIN, PLAIN)),
        ("redis://s1:26379,rediss://s2:26379", true, true, (PLAIN, VERIFIED)),
    ] {
        let mut c = config("redis://mymaster", sentinels);
        c.sentinel_tls = sentinel_tls;
        c.tls_verify = verify;
        assert_eq!(expected, modes(&c), "{sentinels} sentinel_tls={sentinel_tls} tls_verify={verify}");

        // The upstream-shaped options are AnyCable's: no TLS setting for the
        // Sentinel nodes, the master's from the first Sentinel URL's scheme.
        let options = c.to_client_options().expect("options");
        assert_eq!(expected.0, options.tls.as_ref().map(|t| t.insecure_skip_verify), "{sentinels}");
    }

    // The scheme of the master URL is not read for TLS, as upstream.
    let mut c = config("rediss://mymaster", "redis://s1:26379");
    c.sentinel_tls = true;
    assert_eq!((None, Some(true)), modes(&c));

    // The implicit form (`master_set` in the Sentinel's URL): the same rule.
    for (url, sentinel_tls, expected) in [
        ("redis://s1:26379?master_set=mymaster", false, (None, None)),
        ("redis://s1:26379?master_set=mymaster", true, (None, Some(true))),
        ("rediss://s1:26379?master_set=mymaster", false, (Some(true), None)),
        ("rediss://s1:26379?master_set=mymaster", true, (Some(true), Some(true))),
    ] {
        let mut c = config(url, "");
        c.sentinel_tls = sentinel_tls;
        assert_eq!(expected, modes(&c), "{url} sentinel_tls={sentinel_tls}");
    }

    // Without Sentinels the setting changes nothing.
    for url in ["redis://h:6379", "rediss://h:6379", "redis://a:6379,redis://b:6379"] {
        let mut c = config(url, "");
        let before = c.connect_spec().unwrap();
        c.sentinel_tls = true;
        assert_eq!(before, c.connect_spec().unwrap(), "{url}");
    }
}

/// Pins D-115: with `sentinel_tls`, the Sentinel nodes get the master's CA and
/// client certificate; the files are read only when some connection uses TLS.
#[test]
fn deviation_d115_sentinel_tls_uses_the_tls_settings() {
    let fixture = |name: &str| format!("{CLIENT_FIXTURES}/{name}");
    let with_files = |sentinels: &str, sentinel_tls: bool| {
        let mut c = config("redis://mymaster", sentinels);
        c.sentinel_tls = sentinel_tls;
        c.tls_verify = true;
        c.tls_ca_cert_path = fixture("ca.crt");
        c.tls_client_cert_path = fixture("client.crt");
        c.tls_client_key_path = fixture("client.key");
        c
    };
    let ca = std::fs::read(fixture("ca.crt")).unwrap();
    let pair = (std::fs::read(fixture("client.crt")).unwrap(), std::fs::read(fixture("client.key")).unwrap());
    let expected = TlsSpec { insecure: false, root_pem: Some(ca), client: Some(pair) };

    let spec = with_files("rediss://s1:26379", true).connect_spec().unwrap();
    assert_eq!(Some(&expected), spec.tls.as_ref());
    assert!(matches!(&spec.topology, Topology::Sentinel { tls: Some(t), .. } if *t == expected));

    let spec = with_files("redis://s1:26379", true).connect_spec().unwrap();
    assert_eq!(None, spec.tls);
    assert!(matches!(&spec.topology, Topology::Sentinel { tls: Some(t), .. } if *t == expected));

    // The upstream-shaped options of a plain master stay without TLS.
    let options = with_files("redis://s1:26379", true).to_client_options().unwrap();
    assert_eq!(None, options.tls);

    // Missing files: an error when a connection uses TLS, nothing otherwise
    // (as upstream, which reads them only for a TLS URL).
    let missing = |sentinel_tls: bool| {
        let mut c = config("redis://mymaster", "redis://s1:26379");
        c.sentinel_tls = sentinel_tls;
        c.tls_ca_cert_path = "/nonexistent/ca.pem".to_owned();
        c.to_client_options().map(|_| ()).map_err(|e| e.0)
    };
    assert_eq!(Ok(()), missing(false));
    assert_eq!(
        Err("failed to read Redis CA certificate: open /nonexistent/ca.pem: no such file or directory".to_owned()),
        missing(true)
    );
}

/// Pins D-115 (with D-081): TLS to the Sentinel nodes without verification is
/// reported as unverified TLS; a master over TLS with plain Sentinel nodes is
/// recognized for the startup line.
#[test]
fn deviation_d115_tls_checks_follow_the_connections() {
    for (url, sentinels, sentinel_tls, skipped, master_only) in [
        ("redis://mymaster", "redis://s1:26379", false, false, false),
        ("redis://mymaster", "redis://s1:26379", true, true, false),
        ("redis://mymaster", "rediss://s1:26379", false, true, true),
        ("redis://mymaster", "rediss://s1:26379", true, true, false),
        ("redis://mymaster", "s1:26379,rediss://s2:26379", false, false, false),
        ("rediss://s1:26379?master_set=mymaster", "", false, true, true),
        ("rediss://s1:26379?master_set=mymaster", "", true, true, false),
        ("redis://s1:26379?master_set=mymaster", "", false, false, false),
        ("rediss://h:6379", "", false, true, false),
        ("rediss://h:6379", "", true, true, false),
    ] {
        let mut c = config(url, sentinels);
        c.sentinel_tls = sentinel_tls;
        let what = format!("{url} {sentinels} sentinel_tls={sentinel_tls}");
        assert_eq!(skipped, c.tls_verification_skipped(), "{what}");
        assert_eq!(master_only, c.sentinel_tls_off_for_tls_master(), "{what}");
        c.tls_verify = true;
        assert!(!c.tls_verification_skipped(), "{what}");
        assert_eq!(master_only, c.sentinel_tls_off_for_tls_master(), "{what}");
    }
}

// --- End to end: a fake Sentinel and a fake master, behind TLS terminators ---

/// The master set the fake Sentinel monitors.
const MASTER_SET: &str = "mymaster";

fn bulk(s: &str) -> String {
    format!("${}\r\n{s}\r\n", s.len())
}

/// Reads one command (a RESP array of bulk strings); `None` at the end of the
/// stream or for anything else (a TLS handshake, for example).
fn read_command(r: &mut impl BufRead) -> Option<Vec<String>> {
    if r.fill_buf().ok()?.first() != Some(&b'*') {
        return None;
    }
    let mut line = String::new();
    r.read_line(&mut line).ok()?;
    let n: usize = line.trim().strip_prefix('*')?.parse().ok()?;
    (0..n)
        .map(|_| {
            let mut head = String::new();
            r.read_line(&mut head).ok()?;
            let len: usize = head.trim().strip_prefix('$')?.parse().ok()?;
            let mut arg = vec![0; len + 2];
            r.read_exact(&mut arg).ok()?;
            arg.truncate(len);
            String::from_utf8(arg).ok()
        })
        .collect()
}

/// A plain RESP server answering what the `redis` crate sends a Sentinel
/// (`master: Some(addr)`, naming `addr` as the master of [`MASTER_SET`]) or a
/// master (`None`). Counts the commands it answered.
struct Fake {
    addr: SocketAddr,
    commands: Arc<AtomicUsize>,
}

impl Fake {
    fn start(master: Option<SocketAddr>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let commands = Arc::new(AtomicUsize::new(0));
        let counter = commands.clone();
        std::thread::spawn(move || {
            for conn in listener.incoming().flatten() {
                let counter = counter.clone();
                std::thread::spawn(move || serve(conn, master, &counter));
            }
        });
        Fake { addr, commands }
    }

    fn commands(&self) -> usize {
        self.commands.load(Ordering::SeqCst)
    }
}

fn serve(conn: TcpStream, master: Option<SocketAddr>, commands: &AtomicUsize) -> Option<()> {
    let mut writer = conn.try_clone().ok()?;
    let mut reader = BufReader::new(conn);
    loop {
        let args = read_command(&mut reader)?;
        commands.fetch_add(1, Ordering::SeqCst);
        let reply = match (args.first()?.to_uppercase().as_str(), master) {
            ("ROLE", Some(_)) => format!("*2\r\n{}*1\r\n{}", bulk("sentinel"), bulk(MASTER_SET)),
            ("ROLE", None) => format!("*3\r\n{}:0\r\n*0\r\n", bulk("master")),
            ("SENTINEL", Some(m)) => {
                let (ip, port) = (m.ip().to_string(), m.port().to_string());
                let fields = ["name", MASTER_SET, "ip", &ip, "port", &port, "flags", "master"];
                format!("*1\r\n*{}\r\n{}", fields.len(), fields.map(bulk).concat())
            }
            ("PING", _) => "+PONG\r\n".to_owned(),
            _ => "+OK\r\n".to_owned(),
        };
        writer.write_all(reply.as_bytes()).ok()?;
    }
}

/// A TLS terminator in front of `backend` (with `server.crt`), counting the
/// handshakes that succeeded. With `client_ca`, clients must present a
/// certificate it signed.
struct Terminator {
    addr: SocketAddr,
    handshakes: Arc<AtomicUsize>,
}

impl Terminator {
    fn start(backend: SocketAddr, client_ca: Option<&str>) -> Self {
        let certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_file_iter(format!("{SERVER_FIXTURES}/server.crt"))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        let key = PrivateKeyDer::from_pem_file(format!("{SERVER_FIXTURES}/server.key")).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap();
        let builder = match client_ca {
            None => builder.with_no_client_auth(),
            Some(path) => {
                let mut roots = rustls::RootCertStore::empty();
                for cert in CertificateDer::pem_file_iter(path).unwrap() {
                    roots.add(cert.unwrap()).unwrap();
                }
                let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                    .build()
                    .unwrap();
                builder.with_client_cert_verifier(verifier)
            }
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(builder.with_single_cert(certs, key).unwrap()));

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let handshakes = Arc::new(AtomicUsize::new(0));
        let counter = handshakes.clone();
        connect::runtime().spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            while let Ok((tcp, _)) = listener.accept().await {
                let (acceptor, counter) = (acceptor.clone(), counter.clone());
                tokio::spawn(async move {
                    let Ok(mut tls) = acceptor.accept(tcp).await else { return };
                    counter.fetch_add(1, Ordering::SeqCst);
                    if let Ok(mut plain) = tokio::net::TcpStream::connect(backend).await {
                        let _ = tokio::io::copy_bidirectional(&mut tls, &mut plain).await;
                    }
                });
            }
        });
        Terminator { addr, handshakes }
    }

    fn handshakes(&self) -> usize {
        self.handshakes.load(Ordering::SeqCst)
    }
}

/// Opens a commands connection for `c` and sends `PING`.
fn ping(c: &RedisConfig) -> Result<String, String> {
    let spec = c.connect_spec().map_err(|e| e.0)?;
    connect::wait_on(Duration::from_secs(10), async move {
        let mut conn = spec.commands(Some(Duration::from_secs(2))).await.map_err(|e| e.to_string())?;
        redis::cmd("PING").query_async::<String>(&mut conn).await.map_err(|e| e.to_string())
    })
    .unwrap_or_else(|| Err("timed out".to_owned()))
}

fn sentinel_config(scheme: &str, sentinel: SocketAddr, sentinel_tls: bool) -> RedisConfig {
    let mut c = config(&format!("redis://{MASTER_SET}"), &format!("{scheme}://{sentinel}"));
    c.sentinel_tls = sentinel_tls;
    c
}

/// Pins D-115 end to end: AnyCable's configuration (plain Sentinels, the
/// master over TLS from `rediss://` in the Sentinel list) connects; with
/// `sentinel_tls` the Sentinel nodes are reached over TLS, verified with the CA
/// when `tls_verify` is on, and with the client certificate.
#[test]
fn deviation_d115_connections_to_sentinels_and_master() {
    let master = Fake::start(None);
    let tls_master = Terminator::start(master.addr, None);

    // Plain Sentinel, TLS master: connects without `sentinel_tls` only.
    let sentinel = Fake::start(Some(tls_master.addr));
    assert_eq!(Ok("PONG".to_owned()), ping(&sentinel_config("rediss", sentinel.addr, false)));
    assert!(sentinel.commands() > 0 && tls_master.handshakes() > 0);
    let err = ping(&sentinel_config("rediss", sentinel.addr, true)).unwrap_err();
    assert!(!err.is_empty());

    // TLS Sentinel, TLS master.
    let tls_sentinel = Terminator::start(Fake::start(Some(tls_master.addr)).addr, None);
    assert_eq!(Ok("PONG".to_owned()), ping(&sentinel_config("rediss", tls_sentinel.addr, true)));
    assert!(tls_sentinel.handshakes() > 0);
    assert!(ping(&sentinel_config("rediss", tls_sentinel.addr, false)).is_err());

    // TLS Sentinel, plain master: only the Sentinel nodes use TLS.
    let tls_sentinel = Terminator::start(Fake::start(Some(master.addr)).addr, None);
    let before = tls_master.handshakes();
    assert_eq!(Ok("PONG".to_owned()), ping(&sentinel_config("redis", tls_sentinel.addr, true)));
    assert!(tls_sentinel.handshakes() > 0);
    assert_eq!(before, tls_master.handshakes());

    // Verification of the Sentinel nodes follows `tls_verify` and the CA.
    let mut c = sentinel_config("redis", tls_sentinel.addr, true);
    c.tls_verify = true;
    assert!(ping(&c).is_err(), "server.crt is not trusted without the CA");
    c.tls_ca_cert_path = format!("{SERVER_FIXTURES}/server.crt");
    assert_eq!(Ok("PONG".to_owned()), ping(&c));

    // The client certificate is presented to the Sentinel nodes.
    let client_ca = format!("{CLIENT_FIXTURES}/client.crt");
    let mtls_sentinel = Terminator::start(Fake::start(Some(master.addr)).addr, Some(&client_ca));
    let mut c = sentinel_config("redis", mtls_sentinel.addr, true);
    assert!(ping(&c).is_err(), "a client certificate is required");
    c.tls_client_cert_path = format!("{CLIENT_FIXTURES}/client.crt");
    c.tls_client_key_path = format!("{CLIENT_FIXTURES}/client.key");
    assert_eq!(Ok("PONG".to_owned()), ping(&c));
}
