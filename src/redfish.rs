//! The Redfish fencing provider (G12.5): powers a bare-metal node off through its BMC (iDRAC, iLO, XClarity, any
//! Redfish service) and calls the node fenced only once the BMC says its power is off. A reset that was accepted is no
//! proof: the power state is.
//!
//! A node opts in with labels: `fence.provider=redfish`, `redfish.host=<the BMC, host[:port], 443 when not given>`,
//! `redfish.fingerprint=<the BMC certificate's SHA-256>` (each BMC makes its own) and, when the BMC manages more than
//! one system, `redfish.system=<its id under /redfish/v1/Systems>`. The cluster holds one account for its BMCs
//! (`settings.redfish`: a user name and a password that is never shown) with the right to reset the system.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine;
use serde_json::{Value, json};

use crate::settings::Secret;
use crate::state::Node;

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RedfishConfig {
    pub username: String,
    pub password: Secret,
}

impl std::fmt::Debug for RedfishConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedfishConfig").field("username", &self.username).field("password", &"(hidden)").finish()
    }
}

impl RedfishConfig {
    pub fn checked(username: &str, password: &str) -> Result<Self> {
        let username = username.trim();
        if username.is_empty() || username.contains(':') || username.chars().any(char::is_control) {
            bail!("the Redfish user name is not empty and has no colon");
        }
        if password.is_empty() || password.chars().any(char::is_control) {
            bail!("the Redfish password is not empty");
        }
        Ok(RedfishConfig { username: username.to_string(), password: Secret(password.to_string()) })
    }

    fn basic(&self) -> String {
        format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", self.username, self.password.0)))
    }
}

/// The BMC of a node, from its labels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bmc {
    /// `host:port`.
    pub host: String,
    pub fingerprint: String,
    pub system: Option<String>,
}

impl std::fmt::Display for Bmc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.system {
            Some(s) => write!(f, "{} system {s}", self.host),
            None => f.write_str(&self.host),
        }
    }
}

/// `host`, `host:port` or `[v6]:port` to `host:port` (443 when not given); nothing but a host and a port.
pub fn host_port(s: &str) -> Result<String> {
    let h = s.trim().trim_start_matches("https://").trim_end_matches('/');
    let ok = !h.is_empty() && h.bytes().all(|b| b.is_ascii_alphanumeric() || b"-.:[]".contains(&b));
    anyhow::ensure!(ok, "redfish.host is the BMC's host[:port]");
    Ok(if h.rsplit_once(':').is_some_and(|(_, p)| p.parse::<u16>().is_ok()) && !h.ends_with(']') && (h.starts_with('[') || h.matches(':').count() == 1) { h.to_string() } else { format!("{h}:443") })
}

pub fn bmc_of(n: &Node) -> Result<Bmc> {
    let l = n.all_labels();
    let host = host_port(l.get("redfish.host").context("the node has no redfish.host label")?)?;
    let fingerprint = crate::proxmox::fingerprint(l.get("redfish.fingerprint").map_or("", String::as_str)).context("the node has no redfish.fingerprint label (the BMC certificate's SHA-256)")?;
    let system = l.get("redfish.system").cloned();
    if let Some(s) = &system
        && (s.is_empty() || !s.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)))
    {
        bail!("redfish.system is the system's id under /redfish/v1/Systems");
    }
    Ok(Bmc { host, fingerprint, system })
}

pub struct Redfish {
    cfg: RedfishConfig,
    bmc: Bmc,
    tls: Arc<rustls::ClientConfig>,
}

/// An `@odata.id` the BMC gave: a path on it, nothing else.
fn path_of(v: &Value) -> Option<String> {
    v.as_str().filter(|p| p.starts_with("/redfish/v1/") && !p.contains("..") && p.bytes().all(|b| b.is_ascii_graphic())).map(str::to_string)
}

impl Redfish {
    pub fn new(cfg: RedfishConfig, bmc: Bmc) -> Result<Self> {
        let tls = crate::proxmox::pinned_tls(&bmc.fingerprint, "BMC")?;
        Ok(Redfish { cfg, bmc, tls })
    }

    async fn call(&self, method: hyper::Method, path: &str, body: Option<Value>) -> Result<Value> {
        let mut headers = vec![("authorization", self.cfg.basic()), ("accept", "application/json".to_string())];
        if body.is_some() {
            headers.push(("content-type", "application/json".to_string()));
        }
        let (status, bytes) = crate::proxmox::https(&self.tls, &self.bmc.host, method, path, &headers, body.map(|b| b.to_string().into_bytes())).await.with_context(|| format!("the BMC at {}", self.bmc.host))?;
        let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        if !status.is_success() {
            let msg = v["error"]["message"].as_str().map(str::to_string).unwrap_or_else(|| String::from_utf8_lossy(&bytes).trim().chars().take(200).collect());
            bail!("the BMC answered {status}: {msg}");
        }
        Ok(v)
    }

    /// The system's path: the one named, else the only one the BMC manages.
    pub async fn system(&self) -> Result<String> {
        if let Some(s) = &self.bmc.system {
            return Ok(format!("/redfish/v1/Systems/{s}"));
        }
        let v = self.call(hyper::Method::GET, "/redfish/v1/Systems", None).await?;
        let members: Vec<String> = v["Members"].as_array().into_iter().flatten().filter_map(|m| path_of(&m["@odata.id"])).collect();
        match members.as_slice() {
            [one] => Ok(one.clone()),
            [] => bail!("the BMC at {} manages no system", self.bmc.host),
            many => bail!("the BMC at {} manages {} systems; label the node redfish.system=<id>", self.bmc.host, many.len()),
        }
    }

    /// The system's power state (`On`, `Off`, `PoweringOn`, `PoweringOff`) and the path of its reset action.
    async fn power(&self, sys: &str) -> Result<(String, String)> {
        let v = self.call(hyper::Method::GET, sys, None).await?;
        let state = v["PowerState"].as_str().context("the BMC gave no PowerState")?.to_string();
        let reset = path_of(&v["Actions"]["#ComputerSystem.Reset"]["target"]).unwrap_or_else(|| format!("{sys}/Actions/ComputerSystem.Reset"));
        Ok((state, reset))
    }

    /// The system and its power state: proof that the address, the pin and the account all work.
    pub async fn status(&self) -> Result<(String, String)> {
        let sys = self.system().await?;
        let (state, _) = self.power(&sys).await?;
        Ok((sys, state))
    }

    /// Powers the system off (`ForceOff`, not a shutdown) and waits up to `wait` for the BMC to say it is off. Ok only
    /// then: the message says what was seen.
    pub async fn fence(&self, wait: Duration) -> Result<String> {
        let sys = self.system().await?;
        let (state, reset) = self.power(&sys).await?;
        let at = format!("{} {sys}", self.bmc.host);
        if state == "Off" {
            return Ok(format!("verified: the BMC at {at} says its power is off"));
        }
        self.call(hyper::Method::POST, &reset, Some(json!({ "ResetType": "ForceOff" }))).await.context("asking the BMC to power the system off")?;
        let until = tokio::time::Instant::now() + wait;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            match self.power(&sys).await {
                Ok((s, _)) if s == "Off" => return Ok(format!("verified: the BMC at {at} powered it off and says its power is off")),
                Ok((s, _)) if tokio::time::Instant::now() >= until => bail!("the BMC at {at} still says its power is {s} after {}s", wait.as_secs()),
                Err(e) if tokio::time::Instant::now() >= until => return Err(e.context("the system's power state")),
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{BodyExt, Full};
    use hyper::body::Bytes;
    use std::sync::Mutex;

    fn node(labels: &[(&str, &str)]) -> Node {
        Node { id: "n".into(), hostname: "h".into(), cert_sha256: String::new(), prev_cert_sha256: None, enrolled_at: 0, report: None, reported_at: None, labels: labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(), fencing: Default::default(), recovered: false, overlay: None }
    }

    #[test]
    fn a_node_s_bmc_comes_from_its_labels() {
        let fp = "AB:".repeat(31) + "AB";
        let n = node(&[("fence.provider", "redfish"), ("redfish.host", "bmc1.example"), ("redfish.fingerprint", &fp)]);
        assert_eq!(crate::proxmox::provider_of(&n), "redfish");
        assert_eq!(bmc_of(&n).unwrap(), Bmc { host: "bmc1.example:443".into(), fingerprint: "ab".repeat(32), system: None });
        assert_eq!(bmc_of(&node(&[("redfish.host", "192.0.2.9:8443"), ("redfish.fingerprint", &fp), ("redfish.system", "System.Embedded.1")])).unwrap().to_string(), "192.0.2.9:8443 system System.Embedded.1");
        assert_eq!(host_port("[2001:db8::9]").unwrap(), "[2001:db8::9]:443");
        assert_eq!(host_port("[2001:db8::9]:8443").unwrap(), "[2001:db8::9]:8443");
        assert!(bmc_of(&node(&[("redfish.host", "bmc1")])).is_err(), "a pinned certificate is needed");
        assert!(bmc_of(&node(&[("redfish.host", "bmc1/x"), ("redfish.fingerprint", &fp)])).is_err());
        assert!(bmc_of(&node(&[("redfish.host", "bmc1"), ("redfish.fingerprint", &fp), ("redfish.system", "../x")])).is_err(), "nothing that changes the path");
    }

    #[test]
    fn the_account_is_checked_and_its_password_hidden() {
        let c = RedfishConfig::checked(" fence ", "s3cret").unwrap();
        assert_eq!(c.username, "fence");
        assert!(!format!("{c:?}").contains("s3cret"));
        assert_eq!(c.basic(), "Basic ZmVuY2U6czNjcmV0");
        assert!(RedfishConfig::checked("a:b", "x").is_err());
        assert!(RedfishConfig::checked("fence", "").is_err());
    }

    /// A stand-in for a BMC over TLS with its own certificate: the system is on until it is reset with ForceOff
    /// (when `obeys`); `systems` is how many it manages.
    async fn fake(obeys: bool, systems: usize) -> (Bmc, Arc<Mutex<Vec<String>>>) {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["bmc".into()]).unwrap().self_signed(&key).unwrap();
        let der = cert.der().clone();
        let fp = freightdock_proto::sha256_hex(&der);
        let p = Arc::new(rustls::crypto::ring::default_provider());
        let server = rustls::ServerConfig::builder_with_provider(p).with_safe_default_protocol_versions().unwrap().with_no_client_auth().with_single_cert(vec![der], rustls::pki_types::PrivateKeyDer::try_from(key.serialize_der()).unwrap()).unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let seen: Arc<Mutex<Vec<String>>> = Default::default();
        let off = Arc::new(Mutex::new(false));
        let (seen2, off2) = (seen.clone(), off.clone());
        tokio::spawn(async move {
            loop {
                let (tcp, _) = l.accept().await.unwrap();
                let Ok(tls) = acceptor.accept(tcp).await else { continue };
                let (seen, off) = (seen2.clone(), off2.clone());
                tokio::spawn(async move {
                    let svc = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                        let (seen, off) = (seen.clone(), off.clone());
                        async move {
                            let (method, path) = (req.method().clone(), req.uri().path().to_string());
                            let auth = req.headers().get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
                            let body = String::from_utf8(req.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
                            seen.lock().unwrap().push(format!("{method} {path} {auth} {body}").trim().to_string());
                            let (status, body) = if auth != "Basic ZmVuY2U6czNjcmV0" {
                                (401, json!({ "error": { "message": "bad credentials" } }))
                            } else if path == "/redfish/v1/Systems" {
                                (200, json!({ "Members": (0..systems).map(|i| json!({ "@odata.id": format!("/redfish/v1/Systems/S{i}") })).collect::<Vec<_>>() }))
                            } else if method == hyper::Method::POST && path == "/redfish/v1/Systems/S0/Actions/Reset" {
                                if obeys && body.contains("ForceOff") {
                                    *off.lock().unwrap() = true;
                                }
                                (204, Value::Null)
                            } else if path == "/redfish/v1/Systems/S0" {
                                let state = if *off.lock().unwrap() { "Off" } else { "On" };
                                (200, json!({ "PowerState": state, "Actions": { "#ComputerSystem.Reset": { "target": "/redfish/v1/Systems/S0/Actions/Reset" } } }))
                            } else {
                                (404, json!({ "error": { "message": "no such resource" } }))
                            };
                            let body = if body.is_null() { String::new() } else { body.to_string() };
                            let mut res = hyper::Response::new(Full::new(Bytes::from(body)));
                            *res.status_mut() = hyper::StatusCode::from_u16(status).unwrap();
                            Ok::<_, std::convert::Infallible>(res)
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new().serve_connection(hyper_util::rt::TokioIo::new(tls), svc).await;
                });
            }
        });
        (Bmc { host: addr.to_string(), fingerprint: fp, system: None }, seen)
    }

    fn account() -> RedfishConfig {
        RedfishConfig::checked("fence", "s3cret").unwrap()
    }

    #[tokio::test]
    async fn fenced_once_the_bmc_says_the_power_is_off() {
        let (bmc, seen) = fake(true, 1).await;
        let r = Redfish::new(account(), bmc).unwrap();
        assert_eq!(r.status().await.unwrap(), ("/redfish/v1/Systems/S0".into(), "On".into()));
        let msg = r.fence(Duration::from_secs(5)).await.unwrap();
        assert!(msg.contains("verified") && msg.contains("powered it off"), "{msg}");
        let posted = seen.lock().unwrap().clone();
        assert!(posted.iter().any(|l| l == r#"POST /redfish/v1/Systems/S0/Actions/Reset Basic ZmVuY2U6czNjcmV0 {"ResetType":"ForceOff"}"#), "{posted:?}");
        let again = r.fence(Duration::from_secs(5)).await.unwrap();
        assert!(again.contains("says its power is off"), "an off system is fenced without a reset: {again}");
    }

    #[tokio::test]
    async fn a_reset_accepted_is_no_proof() {
        let (bmc, _) = fake(false, 1).await;
        let e = Redfish::new(account(), bmc).unwrap().fence(Duration::from_secs(2)).await.unwrap_err();
        assert!(e.to_string().contains("still says its power is On"), "{e}");
    }

    #[tokio::test]
    async fn more_than_one_system_needs_its_label() {
        let (mut bmc, _) = fake(true, 2).await;
        let e = Redfish::new(account(), bmc.clone()).unwrap().status().await.unwrap_err();
        assert!(e.to_string().contains("manages 2 systems"), "{e}");
        bmc.system = Some("S0".into());
        assert_eq!(Redfish::new(account(), bmc).unwrap().status().await.unwrap().1, "On");
    }

    #[tokio::test]
    async fn a_wrong_account_or_certificate_is_refused() {
        let (bmc, seen) = fake(true, 1).await;
        let wrong = RedfishConfig::checked("fence", "nope").unwrap();
        let e = Redfish::new(wrong, bmc.clone()).unwrap().status().await.unwrap_err();
        assert!(format!("{e:#}").contains("401") && format!("{e:#}").contains("bad credentials"), "{e:#}");
        seen.lock().unwrap().clear();
        let other = Bmc { fingerprint: "00".repeat(32), ..bmc };
        assert!(Redfish::new(account(), other).unwrap().status().await.is_err());
        assert!(seen.lock().unwrap().is_empty(), "the password never went out");
    }
}
