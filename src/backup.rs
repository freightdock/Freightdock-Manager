//! Backups of the cluster: the replicated state (stacks, nodes, settings, the UI's database) and the cluster CA, so a
//! cluster lost whole comes back with the same identity and its workers' certificates still good. A backup holds the
//! CA's key and the settings' secrets, so it is always sealed with a passphrase: PBKDF2-HMAC-SHA256 (600 000 rounds)
//! to an AES-256-GCM key, the header bound in as associated data.
//!
//! File: `FDBK1` (5 bytes), the salt (16), the nonce (12), then the sealed JSON of a `Bundle`.
//!
//! Each manager writes its scheduled backups to `<data>/backups/` (the settings say how often and how many to keep).
//! A backup comes back with `freightdock-manager restore <file>` on a new manager's empty data folder: the CA is put
//! in place and the state left in `restore.json`, which the manager takes as its cluster when it first starts.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use serde::{Deserialize, Serialize};

use crate::state::{ClusterState, Ms};

const MAGIC: &[u8; 5] = b"FDBK1";
/// PBKDF2 rounds: fewer in tests, which only check the sealing.
const ROUNDS: u32 = if cfg!(test) { 1_000 } else { 600_000 };
/// The shortest passphrase taken.
pub const MIN_PASSPHRASE: usize = 12;

#[derive(Serialize, Deserialize)]
pub struct Bundle {
    pub format: String,
    pub taken_at: Ms,
    pub cluster_id: Option<String>,
    pub state: ClusterState,
    pub ca_pem: String,
    pub ca_key: String,
}

impl std::fmt::Debug for Bundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bundle").field("format", &self.format).field("taken_at", &self.taken_at).field("cluster_id", &self.cluster_id).finish_non_exhaustive()
    }
}

impl Bundle {
    pub const FORMAT: &'static str = "freightdock-backup-1";
}

fn key(passphrase: &str, salt: &[u8]) -> LessSafeKey {
    let mut k = [0u8; 32];
    ring::pbkdf2::derive(ring::pbkdf2::PBKDF2_HMAC_SHA256, std::num::NonZeroU32::new(ROUNDS).expect("rounds"), salt, passphrase.as_bytes(), &mut k);
    LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &k).expect("a 32-byte key"))
}

pub fn seal(b: &Bundle, passphrase: &str) -> Result<Vec<u8>> {
    if passphrase.chars().count() < MIN_PASSPHRASE {
        bail!("a backup's passphrase is at least {MIN_PASSPHRASE} characters");
    }
    let salt: [u8; 16] = rand::random();
    let nonce: [u8; 12] = rand::random();
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&salt);
    out.extend_from_slice(&nonce);
    let mut body = serde_json::to_vec(b)?;
    key(passphrase, &salt).seal_in_place_append_tag(Nonce::assume_unique_for_key(nonce), Aad::from(&out[..]), &mut body).map_err(|_| anyhow::anyhow!("sealing the backup"))?;
    out.extend_from_slice(&body);
    Ok(out)
}

pub fn open(bytes: &[u8], passphrase: &str) -> Result<Bundle> {
    if bytes.len() < 33 + 16 || &bytes[..5] != MAGIC {
        bail!("this is not a Freightdock backup");
    }
    let (head, sealed) = bytes.split_at(33);
    let nonce: [u8; 12] = head[21..33].try_into().expect("12 bytes");
    let mut body = sealed.to_vec();
    let plain = key(passphrase, &head[5..21]).open_in_place(Nonce::assume_unique_for_key(nonce), Aad::from(head), &mut body).map_err(|_| anyhow::anyhow!("the passphrase is not this backup's, or the file was changed"))?;
    let b: Bundle = serde_json::from_slice(plain).context("reading the backup")?;
    if b.format != Bundle::FORMAT {
        bail!("a backup of format {:?}; this manager reads {}", b.format, Bundle::FORMAT);
    }
    Ok(b)
}

/// The scheduled backups' folder in a manager's data folder.
pub fn dir(data: &Path) -> PathBuf {
    data.join("backups")
}

/// When the backup of this name was taken: `None` unless it is a name `file_name` makes.
pub fn taken_at(name: &str) -> Option<Ms> {
    let t = name.strip_prefix("freightdock-")?.strip_suffix(".fdbk")?;
    let (d, h) = t.split_once('-')?;
    if d.len() != 8 || h.len() != 6 || !d.bytes().chain(h.bytes()).all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n = |s: &str| s.parse::<u32>().ok();
    let month = time::Month::try_from(n(&d[4..6])? as u8).ok()?;
    let date = time::Date::from_calendar_date(n(&d[..4])? as i32, month, n(&d[6..])? as u8).ok()?;
    let at = date.with_hms(n(&h[..2])? as u8, n(&h[2..4])? as u8, n(&h[4..])? as u8).ok()?.assume_utc();
    Some(at.unix_timestamp() * 1000)
}

/// A scheduled backup is due: one every `every_h` hours (0: never), the first right away.
pub fn due(newest: Option<Ms>, now: Ms, every_h: u32) -> bool {
    every_h > 0 && newest.is_none_or(|t| now - t >= every_h as Ms * 3_600_000)
}

/// The backups in `dir`, newest first: (name, size in bytes). Other files are left out.
pub fn list(dir: &Path) -> Vec<(String, u64)> {
    let mut out: Vec<(String, u64)> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            taken_at(&name)?;
            Some((name, e.metadata().ok()?.len()))
        })
        .collect();
    out.sort_by(|a, b| b.0.cmp(&a.0));
    out
}

/// The backup of this name in `dir`, if it is one (a name that is not a backup's is never opened).
pub fn read(dir: &Path, name: &str) -> Option<Vec<u8>> {
    taken_at(name)?;
    std::fs::read(dir.join(name)).ok()
}

/// Seals `b` into `dir` (readable by root only) and removes the oldest beyond `keep`. Answers the new file's name.
pub fn write(dir: &Path, b: &Bundle, passphrase: &str, keep: usize) -> Result<String> {
    let sealed = seal(b, passphrase)?;
    std::fs::create_dir_all(dir).with_context(|| format!("making {}", dir.display()))?;
    let name = file_name(b.taken_at);
    crate::ca::write_private(&dir.join(&name), &sealed)?;
    for (old, _) in list(dir).into_iter().skip(keep.max(1)) {
        let _ = std::fs::remove_file(dir.join(old));
    }
    Ok(name)
}

/// Where a staged restore waits for the manager's first start.
fn restore_file(data: &Path) -> PathBuf {
    data.join("restore.json")
}

/// Readies an empty data folder to bring the backed up cluster back: the CA in place, the state in `restore.json`.
/// Refused where a manager has already run (its Raft log is the cluster's; a restore never mixes with it).
pub fn stage_restore(data: &Path, bytes: &[u8], passphrase: &str) -> Result<String> {
    let raft = data.join("raft");
    if std::fs::read_dir(&raft).is_ok_and(|mut d| d.next().is_some()) {
        bail!("{} already holds a cluster: restore onto a new manager's empty data folder", data.display());
    }
    let b = open(bytes, passphrase)?;
    let cluster = b.cluster_id.clone().context("the backup holds no cluster")?;
    std::fs::create_dir_all(data).with_context(|| format!("making {}", data.display()))?;
    crate::ca::Ca::install(&data.join("ca"), &b.ca_pem, &b.ca_key)?;
    crate::ca::write_private(&restore_file(data), &serde_json::to_vec(&b.state)?)?;
    Ok(cluster)
}

/// The state staged by `stage_restore`, if there is one; `done` removes it once the cluster has taken it.
pub fn staged(data: &Path) -> Result<Option<ClusterState>> {
    match std::fs::read(restore_file(data)) {
        Ok(b) => Ok(Some(serde_json::from_slice(&b).context("reading restore.json")?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).context("reading restore.json"),
    }
}

pub fn done(data: &Path) {
    let _ = std::fs::remove_file(restore_file(data));
}

/// A backup's file name: when it was taken, sortable.
pub fn file_name(taken_at: Ms) -> String {
    let t = time::OffsetDateTime::from_unix_timestamp(taken_at / 1000).unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    format!("freightdock-{:04}{:02}{:02}-{:02}{:02}{:02}.fdbk", t.year(), t.month() as u8, t.day(), t.hour(), t.minute(), t.second())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bundle() -> Bundle {
        let state = ClusterState { cluster_id: Some("c1".into()), ..Default::default() };
        Bundle { format: Bundle::FORMAT.into(), taken_at: 1_794_600_000_000, cluster_id: Some("c1".into()), state, ca_pem: "PEM".into(), ca_key: "KEY s3cret".into() }
    }

    #[test]
    fn a_backup_opens_with_its_passphrase_only_and_hides_what_it_holds() {
        let sealed = seal(&bundle(), "correct horse battery").unwrap();
        assert!(!sealed.windows(6).any(|w| w == b"s3cret"), "nothing readable");
        let b = open(&sealed, "correct horse battery").unwrap();
        assert_eq!((b.cluster_id.as_deref(), b.ca_key.as_str()), (Some("c1"), "KEY s3cret"));
        assert!(open(&sealed, "wrong horse battery").unwrap_err().to_string().contains("passphrase"));
        let mut changed = sealed.clone();
        let last = changed.len() - 1;
        changed[last] ^= 1;
        assert!(open(&changed, "correct horse battery").is_err(), "a change anywhere is found");
        let mut salt = sealed.clone();
        salt[6] ^= 1;
        assert!(open(&salt, "correct horse battery").is_err(), "the header too");
        assert!(open(b"not a backup at all, really not", "correct horse battery").unwrap_err().to_string().contains("not a Freightdock backup"));
        assert_ne!(seal(&bundle(), "correct horse battery").unwrap(), sealed, "a new salt and nonce each time");
    }

    #[test]
    fn a_short_passphrase_is_refused() {
        assert!(seal(&bundle(), "short").unwrap_err().to_string().contains("12"));
    }

    #[test]
    fn a_backup_s_name_says_when() {
        assert_eq!(file_name(1_794_600_000_000), "freightdock-20261113-200000.fdbk");
        assert_eq!(taken_at("freightdock-20261113-200000.fdbk"), Some(1_794_600_000_000));
        for bad in ["freightdock-20261313-200000.fdbk", "../admin.token", "freightdock-2026111-200000.fdbk", "freightdock-20261113-200000.fdbk.tmp"] {
            assert_eq!(taken_at(bad), None, "{bad}");
        }
    }

    #[test]
    fn a_scheduled_backup_is_due_after_its_interval_and_never_at_zero() {
        let h = 3_600_000;
        assert!(due(None, 10 * h, 24));
        assert!(!due(Some(0), 23 * h, 24));
        assert!(due(Some(0), 24 * h, 24));
        assert!(!due(None, 10 * h, 0));
    }

    #[test]
    fn backups_are_written_for_root_only_and_the_oldest_beyond_keep_go() {
        use std::os::unix::fs::PermissionsExt;
        let d = tempfile::tempdir().unwrap();
        let mut b = bundle();
        for i in 0..4 {
            b.taken_at = 1_794_600_000_000 + i * 1000;
            write(d.path(), &b, "correct horse battery", 2).unwrap();
        }
        std::fs::write(d.path().join("notes.txt"), "x").unwrap();
        let names: Vec<String> = list(d.path()).into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["freightdock-20261113-200003.fdbk", "freightdock-20261113-200002.fdbk"]);
        assert_eq!(std::fs::metadata(d.path().join(&names[0])).unwrap().permissions().mode() & 0o777, 0o600);
        assert!(read(d.path(), &names[0]).is_some());
        assert!(read(d.path(), "notes.txt").is_none());
    }

    #[test]
    fn a_restore_is_staged_in_an_empty_data_folder_only() {
        let d = tempfile::tempdir().unwrap();
        let mut b = bundle();
        let ca = crate::ca::Ca::load_or_create(&d.path().join("src-ca")).unwrap();
        (b.ca_pem, b.ca_key) = (ca.cert_pem.clone(), ca.key_pem());
        let sealed = seal(&b, "correct horse battery").unwrap();
        let data = d.path().join("data");
        assert_eq!(stage_restore(&data, &sealed, "correct horse battery").unwrap(), "c1");
        assert_eq!(crate::ca::Ca::load_or_create(&data.join("ca")).unwrap().fingerprint(), ca.fingerprint());
        assert_eq!(staged(&data).unwrap().and_then(|s| s.cluster_id).as_deref(), Some("c1"));
        done(&data);
        assert!(staged(&data).unwrap().is_none());
        std::fs::create_dir_all(data.join("raft")).unwrap();
        std::fs::write(data.join("raft").join("log"), "x").unwrap();
        assert!(stage_restore(&data, &sealed, "correct horse battery").unwrap_err().to_string().contains("already"));
    }
}
