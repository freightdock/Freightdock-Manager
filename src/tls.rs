//! The worker API's listener: TLS from the cluster CA, a client certificate optional at the handshake (enrolment has
//! none yet) and required by every service but Enrollment, which checks it itself from `Peer`.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::Result;
use rustls::pki_types::CertificateDer;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::server::TlsStream;
use tonic::transport::server::Connected;

use crate::ca::{Ca, ServerIdentity};

/// How long a manager's own certificate is used before a new one is made: well inside its 90 days.
pub const RENEW_AFTER: Duration = Duration::from_secs(30 * 24 * 3600);

/// A manager's own certificate (server or client), made again by `make` at the first handshake after it is `every`
/// old, so a manager that runs for months never shows an expired one (G2.7). A failed renewal keeps the one there is
/// and is tried again at the next handshake.
pub struct Rotating {
    make: Box<dyn Fn() -> Result<ServerIdentity> + Send + Sync>,
    every: Duration,
    provider: Arc<rustls::crypto::CryptoProvider>,
    cur: std::sync::Mutex<(Arc<rustls::sign::CertifiedKey>, Instant)>,
}

impl std::fmt::Debug for Rotating {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rotating").field("every", &self.every).finish_non_exhaustive()
    }
}

impl Rotating {
    pub fn new(make: impl Fn() -> Result<ServerIdentity> + Send + Sync + 'static, every: Duration) -> Result<Arc<Self>> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let first = certified(make()?, &provider)?;
        Ok(Arc::new(Rotating { make: Box::new(make), every, provider, cur: std::sync::Mutex::new((first, Instant::now())) }))
    }

    /// The certificate to show now, made again first when it is old.
    pub fn current(&self) -> Arc<rustls::sign::CertifiedKey> {
        let mut cur = self.cur.lock().unwrap_or_else(|e| e.into_inner());
        if cur.1.elapsed() >= self.every {
            match (self.make)().and_then(|id| certified(id, &self.provider)) {
                Ok(k) => {
                    *cur = (k, Instant::now());
                    tracing::info!("made a new certificate for this manager");
                }
                Err(e) => tracing::warn!("renewing this manager's certificate: {e:#}; keeping the one there is"),
            }
        }
        cur.0.clone()
    }
}

fn certified(id: ServerIdentity, provider: &rustls::crypto::CryptoProvider) -> Result<Arc<rustls::sign::CertifiedKey>> {
    Ok(Arc::new(rustls::sign::CertifiedKey::from_der(id.chain, id.key, provider)?))
}

impl rustls::server::ResolvesServerCert for Rotating {
    fn resolve(&self, _: rustls::server::ClientHello<'_>) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(self.current())
    }
}

impl rustls::client::ResolvesClientCert for Rotating {
    fn resolve(&self, _: &[&[u8]], _: &[rustls::SignatureScheme]) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(self.current())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

pub fn server_config(ca: &Ca, id: Arc<Rotating>) -> Result<Arc<rustls::ServerConfig>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca.cert_der.clone())?;
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone()).allow_unauthenticated().build()?;
    let mut c = rustls::ServerConfig::builder_with_provider(provider).with_safe_default_protocol_versions()?.with_client_cert_verifier(verifier).with_cert_resolver(id);
    c.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(c))
}

/// TLS for the admin API on the network: this manager's server certificate, no client certificate (the API takes
/// bearer tokens), HTTP/1.1.
pub fn api_server_config(id: Arc<Rotating>) -> Result<Arc<rustls::ServerConfig>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut c = rustls::ServerConfig::builder_with_provider(provider).with_safe_default_protocol_versions()?.with_no_client_auth().with_cert_resolver(id);
    c.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(c))
}

/// Who is on the other end of a connection: its address, and its client certificate if it showed one (already checked
/// against the cluster CA by the handshake).
#[derive(Debug, Clone)]
pub struct Peer {
    pub addr: SocketAddr,
    pub cert: Option<CertificateDer<'static>>,
}

pub struct Conn {
    inner: TlsStream<TcpStream>,
    peer: Peer,
}

impl Connected for Conn {
    type ConnectInfo = Peer;
    fn connect_info(&self) -> Peer {
        self.peer.clone()
    }
}

impl AsyncRead for Conn {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Conn {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
    fn poll_write_vectored(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bufs: &[std::io::IoSlice<'_>]) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

fn keepalive(tcp: &TcpStream) {
    let _ = tcp.set_nodelay(true);
    let k = socket2::TcpKeepalive::new().with_time(std::time::Duration::from_secs(30)).with_interval(std::time::Duration::from_secs(10));
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let k = k.with_retries(3);
    let _ = socket2::SockRef::from(tcp).set_tcp_keepalive(&k);
}

/// Accepts connections on `listener` and hands each, after its handshake, to the gRPC server. A slow or failed
/// handshake holds up no other connection.
pub fn incoming(listener: TcpListener, tls: Arc<rustls::ServerConfig>) -> tokio_stream::wrappers::ReceiverStream<std::io::Result<Conn>> {
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    tokio::spawn(async move {
        loop {
            // Ends with the server: once it lets go of the stream, the listener is closed and the port free.
            let accepted = tokio::select! {
                _ = tx.closed() => break,
                a = listener.accept() => a,
            };
            let (tcp, addr) = match accepted {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!("accept: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
            keepalive(&tcp);
            let (acceptor, tx) = (acceptor.clone(), tx.clone());
            tokio::spawn(async move {
                match tokio::time::timeout(std::time::Duration::from_secs(10), acceptor.accept(tcp)).await {
                    Ok(Ok(inner)) => {
                        let cert = inner.get_ref().1.peer_certificates().and_then(|c| c.first()).map(|c| c.clone().into_owned());
                        let _ = tx.send(Ok(Conn { inner, peer: Peer { addr, cert } })).await;
                    }
                    Ok(Err(e)) => tracing::debug!("TLS handshake with {addr}: {e}"),
                    Err(_) => tracing::debug!("TLS handshake with {addr} took too long"),
                }
            });
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// TLS for calling other managers: this manager's client certificate, the other end checked against the cluster CA
/// for `MANAGER_SERVER_NAME`.
pub fn peer_client_config(ca: &Ca, id: Arc<Rotating>) -> Result<Arc<rustls::ClientConfig>> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca.cert_der.clone())?;
    let mut c = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_client_cert_resolver(id);
    c.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(c))
}

/// A gRPC channel to the manager at `addr`, over our TLS (tonic is given our connector, so the rustls settings are
/// ours). Connects on first use, and again after a break.
pub fn peer_channel(addr: &str, tls: Arc<rustls::ClientConfig>) -> Result<tonic::transport::Channel> {
    use std::time::Duration;
    let ep = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))?
        .connect_timeout(Duration::from_secs(3))
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);
    let addr = addr.to_string();
    let connector = tower::service_fn(move |_: tonic::transport::Uri| {
        let (addr, tls) = (addr.clone(), tls.clone());
        async move {
            let tcp = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(&addr)).await.map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, format!("{addr} did not answer")))??;
            keepalive(&tcp);
            let name = rustls::pki_types::ServerName::try_from(freightdock_proto::MANAGER_SERVER_NAME).expect("a valid name");
            let tls = tokio_rustls::TlsConnector::from(tls).connect(name, tcp).await?;
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(tls))
        }
    });
    Ok(ep.connect_with_connector_lazy(connector))
}

/// Checks the manager's certificate against the CA in its chain whose fingerprint a join token pins, as a normal
/// check against that CA as the only root. For a manager's join, before it has the CA.
#[derive(Debug)]
struct PinnedCa {
    pin: String,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl rustls::client::danger::ServerCertVerifier for PinnedCa {
    fn verify_server_cert(&self, end: &CertificateDer<'_>, intermediates: &[CertificateDer<'_>], name: &rustls::pki_types::ServerName<'_>, ocsp: &[u8], now: rustls::pki_types::UnixTime) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let ca = intermediates.iter().find(|c| freightdock_proto::sha256_hex(c) == self.pin).ok_or_else(|| rustls::Error::General("this is not the cluster the join token is for".into()))?;
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca.clone().into_owned()).map_err(|e| rustls::Error::General(e.to_string()))?;
        let v = rustls::client::WebPkiServerVerifier::builder_with_provider(Arc::new(roots), self.provider.clone()).build().map_err(|e| rustls::Error::General(e.to_string()))?;
        v.verify_server_cert(end, &[], name, ocsp, now)
    }

    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &rustls::DigitallySignedStruct) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &rustls::DigitallySignedStruct) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

/// For a manager's join: no client certificate, the other manager checked against the CA the token pins.
pub fn pinned_client_config(pin: &str) -> Result<Arc<rustls::ClientConfig>> {
    let p = Arc::new(rustls::crypto::ring::default_provider());
    let mut c = rustls::ClientConfig::builder_with_provider(p.clone())
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCa { pin: pin.to_string(), provider: p }))
        .with_no_client_auth();
    c.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(c))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_certificate_is_made_again_once_it_is_old_and_kept_when_that_fails() {
        let dir = tempfile::tempdir().unwrap();
        let ca = Arc::new(Ca::load_or_create(dir.path()).unwrap());
        let c = ca.clone();
        let young = Rotating::new(move || c.server_identity(), Duration::from_secs(3600)).unwrap();
        assert_eq!(young.current().cert[0], young.current().cert[0], "kept while it is young");
        let c = ca.clone();
        let old = Rotating::new(move || c.server_identity(), Duration::ZERO).unwrap();
        assert_ne!(old.current().cert[0], old.current().cert[0], "made again once it is old");

        let fails = std::sync::atomic::AtomicBool::new(false);
        let fails = Arc::new(fails);
        let (c, f) = (ca.clone(), fails.clone());
        let r = Rotating::new(move || if f.load(std::sync::atomic::Ordering::SeqCst) { anyhow::bail!("no") } else { c.server_identity() }, Duration::ZERO).unwrap();
        let before = r.current();
        fails.store(true, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(r.current().cert[0], before.cert[0], "a failed renewal keeps the one there is");
    }
}
