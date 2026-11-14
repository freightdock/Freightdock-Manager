//! The Proxmox fencing provider (G6.5, plan 7.3): powers a node's guest off through the Proxmox VE API and calls the
//! node fenced only once Proxmox says the guest is stopped. A stop that was accepted is no proof: the status is.
//!
//! A node opts in with labels: `fence.provider=proxmox`, `proxmox.node=<the PVE node>`, `proxmox.vmid=<id>` and
//! `proxmox.type=lxc` or `qemu` (lxc when not given). The managers hold the API's address, an API token and the
//! fingerprint of the API's certificate (Proxmox makes its own; the fingerprint is on the node's Certificates page):
//! `FREIGHTDOCK_FENCE_PROXMOX_URL`, `FREIGHTDOCK_FENCE_PROXMOX_TOKEN_FILE` (or `_TOKEN`; `user@realm!id=secret`, with
//! VM.PowerMgmt and VM.Audit on the guests) and `FREIGHTDOCK_FENCE_PROXMOX_FINGERPRINT`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use rustls::pki_types::CertificateDer;
use serde_json::Value;

use crate::state::Node;

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProxmoxConfig {
    /// `host:port` of the API.
    pub host: String,
    /// `user@realm!tokenid=secret`.
    pub token: String,
    /// The API certificate's SHA-256, lowercase hex without separators.
    pub fingerprint: String,
}

impl std::fmt::Debug for ProxmoxConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxmoxConfig").field("host", &self.host).field("token", &"(hidden)").field("fingerprint", &self.fingerprint).finish()
    }
}

/// `AB:CD:...` or `abcd...` to `abcd...`; `None` unless it is 32 bytes of hex.
pub fn fingerprint(s: &str) -> Option<String> {
    let h: String = s.chars().filter(|c| *c != ':' && !c.is_whitespace()).collect::<String>().to_ascii_lowercase();
    (h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit())).then_some(h)
}

/// `https://host[:port][/]` to `host:port` (8006 when not given); nothing but a host and a port.
pub fn host_of(url: &str) -> Result<String> {
    let host = url.trim().strip_prefix("https://").context("the Proxmox API's address is https://host:port")?.trim_end_matches('/');
    let ok = !host.is_empty() && host.bytes().all(|b| b.is_ascii_alphanumeric() || b"-.:[]".contains(&b));
    anyhow::ensure!(ok, "the Proxmox API's address is https://host:port, nothing after it");
    Ok(if host.rsplit_once(':').is_some_and(|(_, p)| p.parse::<u16>().is_ok()) && !host.ends_with(']') { host.to_string() } else { format!("{host}:8006") })
}

impl ProxmoxConfig {
    /// From the API's address, the token and the certificate's fingerprint, each checked.
    pub fn checked(url: &str, token: &str, fp: &str) -> Result<Self> {
        let host = host_of(url)?;
        let token = token.trim().to_string();
        let (id, secret) = token.split_once('=').unwrap_or_default();
        if !id.contains('@') || !id.contains('!') || secret.is_empty() || token.chars().any(char::is_whitespace) {
            bail!("the Proxmox token is user@realm!tokenid=secret");
        }
        let fingerprint = fingerprint(fp).context("the fingerprint is the API certificate's SHA-256 (as Proxmox shows it)")?;
        Ok(ProxmoxConfig { host, token, fingerprint })
    }

    pub fn from(get: &impl Fn(&str) -> Option<String>) -> Result<Option<Self>> {
        let Some(url) = get("FREIGHTDOCK_FENCE_PROXMOX_URL").map(|u| u.trim().to_string()).filter(|u| !u.is_empty()) else { return Ok(None) };
        let token = match (get("FREIGHTDOCK_FENCE_PROXMOX_TOKEN_FILE"), get("FREIGHTDOCK_FENCE_PROXMOX_TOKEN")) {
            (Some(f), _) => std::fs::read_to_string(f.trim()).with_context(|| format!("reading {f}"))?.trim().to_string(),
            (None, Some(t)) => t.trim().to_string(),
            (None, None) => bail!("FREIGHTDOCK_FENCE_PROXMOX_URL needs FREIGHTDOCK_FENCE_PROXMOX_TOKEN_FILE (or _TOKEN)"),
        };
        let fp = get("FREIGHTDOCK_FENCE_PROXMOX_FINGERPRINT").unwrap_or_default();
        Self::checked(&url, &token, &fp).map(Some).context("the FREIGHTDOCK_FENCE_PROXMOX_* settings")
    }

    pub fn url(&self) -> String {
        format!("https://{}", self.host)
    }

    /// The token's `user@realm!tokenid`, without its secret: what may be shown.
    pub fn token_id(&self) -> &str {
        self.token.split_once('=').map_or("", |t| t.0)
    }
}

/// Where a node's guest is, from its labels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Guest {
    pub node: String,
    pub kind: String,
    pub vmid: u32,
}

impl std::fmt::Display for Guest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.node, self.kind, self.vmid)
    }
}

/// The node's provider, from its `fence.provider` label: `self` when there is none.
pub fn provider_of(n: &Node) -> String {
    n.all_labels().get("fence.provider").cloned().unwrap_or_else(|| "self".into())
}

pub fn guest_of(n: &Node) -> Result<Guest> {
    let l = n.all_labels();
    let node = l.get("proxmox.node").filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_alphanumeric() || b"-.".contains(&b))).context("the node has no proxmox.node label")?.clone();
    let vmid = l.get("proxmox.vmid").and_then(|v| v.parse::<u32>().ok()).filter(|v| *v >= 100).context("the node has no proxmox.vmid label (a number from 100)")?;
    let kind = l.get("proxmox.type").cloned().unwrap_or_else(|| "lxc".into());
    if !matches!(kind.as_str(), "lxc" | "qemu") {
        bail!("proxmox.type is lxc or qemu");
    }
    Ok(Guest { node, kind, vmid })
}

/// Accepts the one certificate whose SHA-256 is pinned, whatever its name and issuer: the server's own (Proxmox's, a
/// BMC's), which no CA signed.
#[derive(Debug)]
struct PinnedLeaf {
    pin: String,
    what: &'static str,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

/// A TLS client that trusts the one certificate `pin` (its SHA-256, lowercase hex); `what` names the server in errors.
pub(crate) fn pinned_tls(pin: &str, what: &'static str) -> Result<Arc<rustls::ClientConfig>> {
    let p = Arc::new(rustls::crypto::ring::default_provider());
    let tls = rustls::ClientConfig::builder_with_provider(p.clone())
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedLeaf { pin: pin.to_string(), what, provider: p }))
        .with_no_client_auth();
    Ok(Arc::new(tls))
}

/// One HTTPS request to `host` (`host:port`) over `tls`, answered within 15 s: the status and the body.
pub(crate) async fn https(tls: &Arc<rustls::ClientConfig>, host: &str, method: hyper::Method, path: &str, headers: &[(&str, String)], body: Option<Vec<u8>>) -> Result<(hyper::StatusCode, Bytes)> {
    let tcp = tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(host)).await.map_err(|_| anyhow!("{host} did not answer"))??;
    let name = host.rsplit_once(':').map_or(host, |h| h.0).trim_start_matches('[').trim_end_matches(']').to_string();
    let name = rustls::pki_types::ServerName::try_from(name).unwrap_or_else(|_| rustls::pki_types::ServerName::try_from("server").expect("a name"));
    let tls = tokio_rustls::TlsConnector::from(tls.clone()).connect(name, tcp).await.with_context(|| format!("TLS to {host}"))?;
    let (mut send, conn) = hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tls)).await?;
    tokio::spawn(conn);
    let mut req = hyper::Request::builder().method(method).uri(path).header("host", host);
    for (k, v) in headers {
        req = req.header(*k, v);
    }
    let req = req.body(Full::new(Bytes::from(body.unwrap_or_default())))?;
    let res = tokio::time::timeout(Duration::from_secs(15), send.send_request(req)).await.map_err(|_| anyhow!("{host} did not answer within 15s"))??;
    let status = res.status();
    Ok((status, res.into_body().collect().await?.to_bytes()))
}

impl rustls::client::danger::ServerCertVerifier for PinnedLeaf {
    fn verify_server_cert(&self, end: &CertificateDer<'_>, _: &[CertificateDer<'_>], _: &rustls::pki_types::ServerName<'_>, _: &[u8], _: rustls::pki_types::UnixTime) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if freightdock_proto::sha256_hex(end) == self.pin {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!("the {}'s certificate is not the pinned one", self.what)))
        }
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

/// Accepts any certificate and keeps the leaf's SHA-256: only to show an administrator what to pin.
#[derive(Debug)]
struct Seen {
    leaf: std::sync::Mutex<Option<String>>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl rustls::client::danger::ServerCertVerifier for Seen {
    fn verify_server_cert(&self, end: &CertificateDer<'_>, _: &[CertificateDer<'_>], _: &rustls::pki_types::ServerName<'_>, _: &[u8], _: rustls::pki_types::UnixTime) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        *self.leaf.lock().expect("seen lock") = Some(freightdock_proto::sha256_hex(end));
        Err(rustls::Error::General("only looking".into()))
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

/// The SHA-256 of the certificate the API at `url` shows, read in the handshake, which is then broken off: nothing
/// is sent. For an administrator to compare with the one Proxmox shows, and pin.
pub async fn fingerprint_of(url: &str) -> Result<String> {
    fingerprint_at(&host_of(url)?).await
}

/// The SHA-256 of the certificate the server at `host` (`host:port`) shows; nothing is sent.
pub async fn fingerprint_at(host: &str) -> Result<String> {
    let p = Arc::new(rustls::crypto::ring::default_provider());
    let seen = Arc::new(Seen { leaf: Default::default(), provider: p.clone() });
    let tls = rustls::ClientConfig::builder_with_provider(p).with_safe_default_protocol_versions()?.dangerous().with_custom_certificate_verifier(seen.clone()).with_no_client_auth();
    let tcp = tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(&host)).await.map_err(|_| anyhow!("{host} did not answer"))?.with_context(|| format!("connecting to {host}"))?;
    let name = rustls::pki_types::ServerName::try_from("proxmox").expect("a name");
    let _ = tokio::time::timeout(Duration::from_secs(5), tokio_rustls::TlsConnector::from(Arc::new(tls)).connect(name, tcp)).await;
    let fp = seen.leaf.lock().expect("seen lock").clone().with_context(|| format!("{host} showed no certificate"))?;
    Ok(fp)
}

pub struct Proxmox {
    cfg: ProxmoxConfig,
    tls: Arc<rustls::ClientConfig>,
}

impl Proxmox {
    pub fn new(cfg: ProxmoxConfig) -> Result<Self> {
        let tls = pinned_tls(&cfg.fingerprint, "Proxmox API")?;
        Ok(Proxmox { cfg, tls })
    }

    /// One call to the API: its `data`.
    async fn call(&self, method: hyper::Method, path: &str) -> Result<Value> {
        let (status, body) = https(&self.tls, &self.cfg.host, method, path, &[("authorization", format!("PVEAPIToken={}", self.cfg.token))], None).await.context("the Proxmox API")?;
        let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        if !status.is_success() {
            bail!("Proxmox answered {status}: {}", v.get("message").and_then(Value::as_str).unwrap_or_else(|| std::str::from_utf8(&body).unwrap_or("")).trim());
        }
        Ok(v["data"].clone())
    }

    /// Proxmox's version: proof that the address, the pin and the token all work.
    pub async fn version(&self) -> Result<String> {
        let d = self.call(hyper::Method::GET, "/api2/json/version").await?;
        d["version"].as_str().map(str::to_string).context("Proxmox gave no version")
    }

    pub async fn status(&self, g: &Guest) -> Result<String> {
        let d = self.call(hyper::Method::GET, &format!("/api2/json/nodes/{}/{}/{}/status/current", g.node, g.kind, g.vmid)).await?;
        d["status"].as_str().map(str::to_string).context("Proxmox gave no status")
    }

    /// Powers the guest off (a hard stop, not a shutdown) and waits up to `wait` for Proxmox to say it is stopped.
    /// Ok only then: the message says what was seen.
    pub async fn fence(&self, g: &Guest, wait: Duration) -> Result<String> {
        if self.status(g).await? == "stopped" {
            return Ok(format!("verified: Proxmox says {g} is stopped"));
        }
        self.call(hyper::Method::POST, &format!("/api2/json/nodes/{}/{}/{}/status/stop", g.node, g.kind, g.vmid)).await.context("asking Proxmox to stop the guest")?;
        let until = tokio::time::Instant::now() + wait;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            match self.status(g).await {
                Ok(s) if s == "stopped" => return Ok(format!("verified: Proxmox stopped {g} and says it is stopped")),
                Ok(s) if tokio::time::Instant::now() >= until => bail!("Proxmox still says {g} is {s} after {}s", wait.as_secs()),
                Err(e) if tokio::time::Instant::now() >= until => return Err(e.context("the guest's status")),
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn node(labels: &[(&str, &str)]) -> Node {
        Node { id: "n".into(), hostname: "h".into(), cert_sha256: String::new(), prev_cert_sha256: None, enrolled_at: 0, report: None, reported_at: None, labels: labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(), fencing: Default::default(), recovered: false, overlay: None }
    }

    #[test]
    fn a_node_s_guest_comes_from_its_labels() {
        assert_eq!(provider_of(&node(&[])), "self");
        let n = node(&[("fence.provider", "proxmox"), ("proxmox.node", "pve1"), ("proxmox.vmid", "110")]);
        assert_eq!(provider_of(&n), "proxmox");
        assert_eq!(guest_of(&n).unwrap().to_string(), "pve1/lxc/110");
        assert!(guest_of(&node(&[("proxmox.node", "pve1")])).is_err());
        assert!(guest_of(&node(&[("proxmox.node", "../x"), ("proxmox.vmid", "110")])).is_err(), "nothing that changes the path");
        assert!(guest_of(&node(&[("proxmox.node", "pve1"), ("proxmox.vmid", "110"), ("proxmox.type", "vm")])).is_err());
    }

    #[test]
    fn the_settings_need_a_token_and_a_fingerprint_and_hide_the_token() {
        let fp = "AB:".repeat(31) + "AB";
        let env = |pairs: Vec<(&'static str, String)>| move |k: &str| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| v.clone());
        assert_eq!(ProxmoxConfig::from(&env(vec![])).unwrap(), None);
        let c = ProxmoxConfig::from(&env(vec![("FREIGHTDOCK_FENCE_PROXMOX_URL", "https://pve".into()), ("FREIGHTDOCK_FENCE_PROXMOX_TOKEN", "fd@pve!fence=s3cret".into()), ("FREIGHTDOCK_FENCE_PROXMOX_FINGERPRINT", fp.clone())])).unwrap().unwrap();
        assert_eq!(c.host, "pve:8006");
        assert_eq!(c.fingerprint, "ab".repeat(32));
        assert!(!format!("{c:?}").contains("s3cret"));
        assert!(ProxmoxConfig::from(&env(vec![("FREIGHTDOCK_FENCE_PROXMOX_URL", "https://pve".into()), ("FREIGHTDOCK_FENCE_PROXMOX_TOKEN", "x".into()), ("FREIGHTDOCK_FENCE_PROXMOX_FINGERPRINT", fp)])).is_err());
    }

    /// A stand-in for the Proxmox API over TLS with its own certificate: the guest runs until it is stopped.
    async fn fake(stops: bool) -> (ProxmoxConfig, Arc<Mutex<Vec<String>>>) {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["pve".into()]).unwrap().self_signed(&key).unwrap();
        let der = cert.der().clone();
        let fp = freightdock_proto::sha256_hex(&der);
        let p = Arc::new(rustls::crypto::ring::default_provider());
        let server = rustls::ServerConfig::builder_with_provider(p).with_safe_default_protocol_versions().unwrap().with_no_client_auth().with_single_cert(vec![der], rustls::pki_types::PrivateKeyDer::try_from(key.serialize_der()).unwrap()).unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let seen: Arc<Mutex<Vec<String>>> = Default::default();
        let stopped = Arc::new(Mutex::new(false));
        let (seen2, stopped2) = (seen.clone(), stopped.clone());
        tokio::spawn(async move {
            loop {
                let (tcp, _) = l.accept().await.unwrap();
                let Ok(tls) = acceptor.accept(tcp).await else { continue };
                let (seen, stopped) = (seen2.clone(), stopped2.clone());
                tokio::spawn(async move {
                    let svc = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                        let (seen, stopped) = (seen.clone(), stopped.clone());
                        async move {
                            let auth = req.headers().get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
                            seen.lock().unwrap().push(format!("{} {} {}", req.method(), req.uri().path(), auth));
                            let body = if req.uri().path() == "/api2/json/version" {
                                r#"{"data":{"version":"9.2.26","release":"9.2"}}"#.to_string()
                            } else if req.method() == hyper::Method::POST {
                                if stops {
                                    *stopped.lock().unwrap() = true;
                                }
                                r#"{"data":"UPID:pve1:1"}"#.to_string()
                            } else {
                                format!(r#"{{"data":{{"status":"{}"}}}}"#, if *stopped.lock().unwrap() { "stopped" } else { "running" })
                            };
                            Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(Bytes::from(body))))
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new().serve_connection(hyper_util::rt::TokioIo::new(tls), svc).await;
                });
            }
        });
        (ProxmoxConfig { host: addr.to_string(), token: "fd@pve!fence=s3cret".into(), fingerprint: fp }, seen)
    }

    fn guest() -> Guest {
        Guest { node: "pve1".into(), kind: "lxc".into(), vmid: 110 }
    }

    #[tokio::test]
    async fn fenced_once_proxmox_says_stopped() {
        let (cfg, seen) = fake(true).await;
        let msg = Proxmox::new(cfg).unwrap().fence(&guest(), Duration::from_secs(5)).await.unwrap();
        assert!(msg.contains("verified") && msg.contains("pve1/lxc/110"), "{msg}");
        let seen = seen.lock().unwrap();
        assert!(seen.iter().any(|l| l.starts_with("POST /api2/json/nodes/pve1/lxc/110/status/stop PVEAPIToken=fd@pve!fence=s3cret")), "{seen:?}");
    }

    #[tokio::test]
    async fn a_stop_accepted_is_no_proof() {
        let (cfg, _) = fake(false).await;
        let e = Proxmox::new(cfg).unwrap().fence(&guest(), Duration::from_secs(2)).await.unwrap_err();
        assert!(e.to_string().contains("still says pve1/lxc/110 is running"), "{e}");
    }

    #[test]
    fn settings_from_fields_are_checked_like_the_environment() {
        let fp = "AB:".repeat(31) + "AB";
        let c = ProxmoxConfig::checked("https://pve.example/", "fd@pve!fence=s3cret", &fp).unwrap();
        assert_eq!((c.host.as_str(), c.url().as_str(), c.token_id()), ("pve.example:8006", "https://pve.example:8006", "fd@pve!fence"));
        assert!(ProxmoxConfig::checked("http://pve", "fd@pve!fence=s", &fp).is_err(), "https only");
        assert!(ProxmoxConfig::checked("https://pve", "nope", &fp).is_err());
        assert!(ProxmoxConfig::checked("https://pve", "fd@pve!fence=s", "12").is_err());
        assert!(ProxmoxConfig::checked("https://pve/x y", "fd@pve!fence=s", &fp).is_err(), "a host and port only");
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(serde_json::from_str::<ProxmoxConfig>(&json).unwrap(), c, "kept in the replicated settings");
    }

    #[tokio::test]
    async fn the_certificate_s_fingerprint_is_read_from_the_server() {
        let (cfg, seen) = fake(true).await;
        assert_eq!(fingerprint_of(&format!("https://{}", cfg.host)).await.unwrap(), cfg.fingerprint);
        assert!(seen.lock().unwrap().is_empty(), "nothing sent, not even a request");
        assert!(fingerprint_of("https://127.0.0.1:1").await.is_err());
    }

    #[tokio::test]
    async fn a_test_answers_proxmox_s_version() {
        let (cfg, _) = fake(true).await;
        assert_eq!(Proxmox::new(cfg).unwrap().version().await.unwrap(), "9.2.26");
    }

    #[tokio::test]
    async fn another_certificate_is_refused() {
        let (mut cfg, seen) = fake(true).await;
        cfg.fingerprint = "00".repeat(32);
        assert!(Proxmox::new(cfg).unwrap().status(&guest()).await.is_err());
        assert!(seen.lock().unwrap().is_empty(), "the token never went out");
    }
}
