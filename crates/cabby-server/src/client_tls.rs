//! Client-side TLS for outgoing connections (RPC over gRPC and HTTP, telemetry).
//!
//! Mirrors upstream `rpc.Config.TLSConfig`: a custom root CA (a file path or PEM
//! contents) or the system roots, optional verification, TLS 1.2 at least.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

/// What a TLS client configuration is built from.
#[derive(Debug, Clone, Default)]
pub(crate) struct TlsOptions {
    /// CA root certificate path or PEM contents (empty: system roots).
    pub root_ca: String,
    pub verify: bool,
    /// ALPN protocols to offer (`h2` for gRPC).
    pub alpn: Vec<Vec<u8>>,
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Builds the client configuration (upstream `TLSConfig`; error texts as upstream).
pub(crate) fn client_config(options: &TlsOptions) -> Result<Arc<ClientConfig>, String> {
    let mut roots = RootCertStore::empty();
    if options.root_ca.is_empty() {
        // Use the system CA certificates. Unreadable entries are skipped, as Go does.
        for cert in rustls_native_certs::load_native_certs().certs {
            let _ = roots.add(cert);
        }
    } else {
        let path = std::path::Path::new(&options.root_ca);
        let pem = if path.is_file() {
            std::fs::read(path).map_err(|e| format!("failed to read RPC root CA certificate: {e}"))?
        } else {
            options.root_ca.clone().into_bytes()
        };
        let mut added = 0;
        for cert in CertificateDer::pem_slice_iter(&pem).flatten() {
            if roots.add(cert).is_ok() {
                added += 1;
            }
        }
        if added == 0 {
            return Err("failed to parse RPC root CA certificate".to_owned());
        }
    }

    let builder = ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS12, &rustls::version::TLS13])
        .map_err(|e| e.to_string())?;
    let mut config = if options.verify {
        builder.with_root_certificates(roots).with_no_client_auth()
    } else {
        builder.dangerous().with_custom_certificate_verifier(Arc::new(NoVerification)).with_no_client_auth()
    };
    config.alpn_protocols.clone_from(&options.alpn);
    Ok(Arc::new(config))
}

/// Performs the TLS handshake over `stream` for `host`.
pub(crate) async fn connect(
    config: &Arc<ClientConfig>,
    host: &str,
    stream: TcpStream,
) -> io::Result<TlsStream<TcpStream>> {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let name = ServerName::try_from(host.to_owned())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("tls: {e}")))?;
    // Error texts carry the `tls:` marker upstream uses to detect TLS failures.
    TlsConnector::from(Arc::clone(config))
        .connect(name, stream)
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("tls: {e}")))
}

/// A client connection, plain or TLS.
#[derive(Debug)]
pub(crate) enum ClientStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl ClientStream {
    /// Connects to `host:port`, with TLS when `tls` is set.
    pub(crate) async fn connect(host: &str, port: u16, tls: Option<&Arc<ClientConfig>>) -> io::Result<Self> {
        let addr_host = host.trim_start_matches('[').trim_end_matches(']');
        let tcp = TcpStream::connect((addr_host, port)).await?;
        let _ = tcp.set_nodelay(true);
        match tls {
            None => Ok(ClientStream::Plain(tcp)),
            Some(config) => Ok(ClientStream::Tls(Box::new(connect(config, host, tcp).await?))),
        }
    }
}

macro_rules! delegate {
    ($self:ident, $s:ident => $e:expr) => {
        match $self.get_mut() {
            ClientStream::Plain($s) => {
                let $s = Pin::new($s);
                $e
            }
            ClientStream::Tls($s) => {
                let $s = Pin::new($s.as_mut());
                $e
            }
        }
    };
}

impl AsyncRead for ClientStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        delegate!(self, s => s.poll_read(cx, buf))
    }
}

impl AsyncWrite for ClientStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        delegate!(self, s => s.poll_write(cx, buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        delegate!(self, s => s.poll_flush(cx))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        delegate!(self, s => s.poll_shutdown(cx))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        delegate!(self, s => s.poll_write_vectored(cx, bufs))
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            ClientStream::Plain(s) => s.is_write_vectored(),
            ClientStream::Tls(s) => s.is_write_vectored(),
        }
    }
}

/// `InsecureSkipVerify`: accepts any server certificate.
#[derive(Debug)]
struct NoVerification;

impl ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        provider().signature_verification_algorithms.supported_schemes()
    }
}
