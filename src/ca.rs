//! The cluster CA. Made by the first manager on its first start and kept in `<data>/ca/` (key 0600). It signs each
//! manager's server certificate (for the fixed name `MANAGER_SERVER_NAME`, made again at every start) and each worker's
//! client certificate (client authentication only, so a worker can never pass for a manager).
//!
//! When more managers join (Raft), they get the CA's key through the replicated join, so any manager can enrol a worker
//! and every manager trusts every worker's certificate.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rcgen::{BasicConstraints, CertificateParams, CertificateSigningRequestParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use freightdock_proto::{MANAGER_CN_PREFIX, MANAGER_SERVER_NAME, sha256_hex};

pub struct Ca {
    pub cert_pem: String,
    pub cert_der: CertificateDer<'static>,
    key: KeyPair,
}

/// What a manager serves its TLS with: a certificate for `MANAGER_SERVER_NAME` signed by the CA, the CA after it.
pub struct ServerIdentity {
    pub chain: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
}

fn now_and(days: i64) -> (time::OffsetDateTime, time::OffsetDateTime) {
    let now = time::OffsetDateTime::now_utc();
    (now - time::Duration::minutes(5), now + time::Duration::days(days))
}

pub fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    #[cfg(unix)]
    o.mode(0o600);
    let mut f = o.open(path).with_context(|| format!("writing {}", path.display()))?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}

impl Ca {
    /// The CA in `dir`, made there first if there is none.
    pub fn load_or_create(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).with_context(|| format!("making {}", dir.display()))?;
        let (cert_path, key_path): (PathBuf, PathBuf) = (dir.join("ca.pem"), dir.join("ca.key"));
        if !cert_path.exists() {
            let key = KeyPair::generate()?;
            let mut p = CertificateParams::new(Vec::<String>::new())?;
            p.distinguished_name.push(DnType::CommonName, "Freightdock cluster CA");
            p.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
            p.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign, KeyUsagePurpose::DigitalSignature];
            (p.not_before, p.not_after) = now_and(365 * 20);
            let cert = p.self_signed(&key)?;
            write_private(&key_path, key.serialize_pem().as_bytes())?;
            std::fs::write(&cert_path, cert.pem())?;
            tracing::info!("made the cluster CA in {}", dir.display());
        }
        let cert_pem = std::fs::read_to_string(&cert_path)?;
        let key = KeyPair::from_pem(&std::fs::read_to_string(&key_path)?).context("the CA's key")?;
        let cert_der = rustls::pki_types::pem::PemObject::from_pem_slice(cert_pem.as_bytes()).context("the CA's certificate")?;
        Ok(Ca { cert_pem, cert_der, key })
    }

    /// Writes a CA handed over by the cluster (a manager's join) into `dir`, where `load_or_create` finds it.
    pub fn install(dir: &Path, cert_pem: &str, key_pem: &str) -> Result<()> {
        KeyPair::from_pem(key_pem).context("the handed-over CA key")?;
        std::fs::create_dir_all(dir).with_context(|| format!("making {}", dir.display()))?;
        write_private(&dir.join("ca.key"), key_pem.as_bytes())?;
        std::fs::write(dir.join("ca.pem"), cert_pem)?;
        Ok(())
    }

    /// The CA's private key, for a manager that joins (over its authenticated join only).
    pub fn key_pem(&self) -> String {
        self.key.serialize_pem()
    }

    /// What a join token pins.
    pub fn fingerprint(&self) -> String {
        sha256_hex(&self.cert_der)
    }

    fn issuer(&self) -> Result<Issuer<'_, &KeyPair>> {
        Ok(Issuer::from_ca_cert_pem(&self.cert_pem, &self.key)?)
    }

    /// A fresh server certificate for this manager.
    pub fn server_identity(&self) -> Result<ServerIdentity> {
        let key = KeyPair::generate()?;
        let mut p = CertificateParams::new(vec![MANAGER_SERVER_NAME.to_string()])?;
        p.distinguished_name.push(DnType::CommonName, MANAGER_SERVER_NAME);
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        (p.not_before, p.not_after) = now_and(90);
        let cert = p.signed_by(&key, &self.issuer()?)?;
        Ok(ServerIdentity { chain: vec![cert.der().clone(), self.cert_der.clone()], key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())) })
    }

    /// A fresh client certificate for this manager, to call the other managers with: common name
    /// `manager:<raft id>`, client authentication only. Made at every start, like the server certificate.
    pub fn manager_client_identity(&self, raft_id: u64) -> Result<ServerIdentity> {
        let key = KeyPair::generate()?;
        let mut p = CertificateParams::new(Vec::<String>::new())?;
        p.distinguished_name.push(DnType::CommonName, format!("{MANAGER_CN_PREFIX}{raft_id}"));
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        (p.not_before, p.not_after) = now_and(90);
        let cert = p.signed_by(&key, &self.issuer()?)?;
        Ok(ServerIdentity { chain: vec![cert.der().clone(), self.cert_der.clone()], key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())) })
    }

    /// Signs a worker's request: only its public key is taken; the name, usage and dates are the CA's. Answers the
    /// certificate's PEM and its SHA-256 (DER), which is how the node is known from then on.
    pub fn sign_worker(&self, csr_pem: &str, node_id: &str) -> Result<(String, String)> {
        let csr = CertificateSigningRequestParams::from_pem(csr_pem).context("not a certificate request")?;
        let mut p = CertificateParams::new(Vec::<String>::new())?;
        p.distinguished_name.push(DnType::CommonName, node_id);
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        (p.not_before, p.not_after) = now_and(365 * 5);
        let cert = p.signed_by(&csr.public_key, &self.issuer()?)?;
        Ok((cert.pem(), sha256_hex(cert.der())))
    }
}

/// The Raft id of the manager a certificate (already checked against the cluster CA) was given to, or `None` when it
/// is not a manager's (a worker's common name is its node UUID, which never starts with `manager:`).
pub fn manager_of(cert: &CertificateDer<'_>) -> Option<u64> {
    let (_, x) = x509_parser::parse_x509_certificate(cert).ok()?;
    let cn = x.subject().iter_common_name().next()?.as_str().ok()?;
    cn.strip_prefix(MANAGER_CN_PREFIX)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ca_is_made_once_and_kept() {
        let dir = tempfile::tempdir().unwrap();
        let a = Ca::load_or_create(dir.path()).unwrap();
        let b = Ca::load_or_create(dir.path()).unwrap();
        assert_eq!(a.fingerprint(), b.fingerprint());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join("ca.key")).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[test]
    fn a_manager_certificate_names_its_manager_and_a_worker_s_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let ca = Ca::load_or_create(dir.path()).unwrap();
        let m = ca.manager_client_identity(42).unwrap();
        assert_eq!(manager_of(&m.chain[0]), Some(42));
        let key = KeyPair::generate().unwrap();
        // A worker asking for a manager's name in its request gets its node id: the CA sets the name.
        let mut asked = CertificateParams::new(Vec::<String>::new()).unwrap();
        asked.distinguished_name.push(DnType::CommonName, "manager:1");
        let (pem, _) = ca.sign_worker(&asked.serialize_request(&key).unwrap().pem().unwrap(), "6f1c7c2e-0000-4000-8000-000000000000").unwrap();
        let der: CertificateDer = rustls::pki_types::pem::PemObject::from_pem_slice(pem.as_bytes()).unwrap();
        assert_eq!(manager_of(&der), None);
    }

    #[test]
    fn a_worker_s_request_is_signed_with_the_ca_s_terms() {
        let dir = tempfile::tempdir().unwrap();
        let ca = Ca::load_or_create(dir.path()).unwrap();
        let key = KeyPair::generate().unwrap();
        let mut asked = CertificateParams::new(vec!["evil.example".to_string()]).unwrap();
        asked.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let csr = asked.serialize_request(&key).unwrap().pem().unwrap();
        let (pem, sha) = ca.sign_worker(&csr, "node-1").unwrap();
        let der: CertificateDer = rustls::pki_types::pem::PemObject::from_pem_slice(pem.as_bytes()).unwrap();
        assert_eq!(sha256_hex(&der), sha);
        // It passes as a client of this CA and as nothing else: not as a manager, whatever it asked for.
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca.cert_der.clone()).unwrap();
        let roots = std::sync::Arc::new(roots);
        let p = std::sync::Arc::new(rustls::crypto::ring::default_provider());
        let now = rustls::pki_types::UnixTime::now();
        use rustls::client::danger::ServerCertVerifier;
        let client = rustls::server::WebPkiClientVerifier::builder_with_provider(roots.clone(), p.clone()).build().unwrap();
        assert!(client.verify_client_cert(&der, &[], now).is_ok());
        let server = rustls::client::WebPkiServerVerifier::builder_with_provider(roots, p).build().unwrap();
        let name = rustls::pki_types::ServerName::try_from(MANAGER_SERVER_NAME).unwrap();
        assert!(server.verify_server_cert(&der, &[], &name, &[], now).is_err());
        let evil = rustls::pki_types::ServerName::try_from("evil.example").unwrap();
        assert!(server.verify_server_cert(&der, &[], &evil, &[], now).is_err());
    }
}
