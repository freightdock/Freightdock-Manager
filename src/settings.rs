//! The cluster's settings an administrator changes from the UI or the CLI: how the nodes are watched, the fencing
//! timers and provider, recovery. Each takes the cluster's value when one is set (replicated: the same on every
//! manager), else this manager's environment variable, else the default. What a node needs before it can reach the
//! cluster at all (addresses, its data directory, a join token) stays in its environment.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::fencing::FenceConfig;
use crate::proxmox::ProxmoxConfig;
use crate::sessions::Thresholds;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Settings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat_ms: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspect_ms: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unreachable_ms: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fence: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fence_stop_s: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fence_storage_s: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fence_reboot_s: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_authorize: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_delay_s: Option<u32>,
    /// The Proxmox provider: its address, token (never shown) and pinned certificate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxmox: Option<ProxmoxConfig>,
    /// Hours between the backups each manager writes of the cluster's state; 0 is none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_every_h: Option<u32>,
    /// How many of its backups each manager keeps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_keep: Option<u32>,
    /// The passphrase the scheduled backups are sealed with: set, never shown. No scheduled backup without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_passphrase: Option<Secret>,
}

/// A value that is stored and used but never shown: `Debug` hides it, the API's view says only whether it is set.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(pub String);

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(hidden)")
    }
}

/// The settings that are a number: name, smallest, largest.
const NUMBERS: [(&str, u32, u32); 9] = [
    ("heartbeat_ms", 500, 30_000),
    ("suspect_ms", 1_000, 600_000),
    ("unreachable_ms", 1_500, 3_600_000),
    ("fence_stop_s", 1, 3_600),
    ("fence_storage_s", 1, 3_600),
    ("fence_reboot_s", 1, 3_600),
    ("recovery_delay_s", 0, 86_400),
    ("backup_every_h", 0, 168),
    ("backup_keep", 1, 100),
];
/// Daily, the last week of them.
pub const BACKUP_EVERY_H: u32 = 24;
pub const BACKUP_KEEP: u32 = 7;
const SWITCHES: [&str; 2] = ["fence", "auto_authorize"];

impl Settings {
    /// Every value, as a manager without any setting has it.
    pub fn defaults() -> Self {
        Self::of(&Thresholds::default(), &FenceConfig::default())
    }

    /// Every value of a manager's own configuration (its environment over the defaults).
    pub fn of(t: &Thresholds, f: &FenceConfig) -> Self {
        Settings {
            heartbeat_ms: Some(t.heartbeat.as_millis() as u32),
            suspect_ms: Some(t.suspect.as_millis() as u32),
            unreachable_ms: Some(t.unreachable.as_millis() as u32),
            fence: Some(f.enabled),
            fence_stop_s: Some(f.stop_s),
            fence_storage_s: Some(f.storage_s),
            fence_reboot_s: Some(f.reboot_s),
            auto_authorize: Some(f.auto_authorize),
            recovery_delay_s: Some(f.recovery_delay_s),
            proxmox: f.proxmox.clone(),
            backup_every_h: Some(BACKUP_EVERY_H),
            backup_keep: Some(BACKUP_KEEP),
            backup_passphrase: None,
        }
    }

    /// These settings where set, `base` for the rest.
    pub fn over(&self, base: &Settings) -> Settings {
        Settings {
            heartbeat_ms: self.heartbeat_ms.or(base.heartbeat_ms),
            suspect_ms: self.suspect_ms.or(base.suspect_ms),
            unreachable_ms: self.unreachable_ms.or(base.unreachable_ms),
            fence: self.fence.or(base.fence),
            fence_stop_s: self.fence_stop_s.or(base.fence_stop_s),
            fence_storage_s: self.fence_storage_s.or(base.fence_storage_s),
            fence_reboot_s: self.fence_reboot_s.or(base.fence_reboot_s),
            auto_authorize: self.auto_authorize.or(base.auto_authorize),
            recovery_delay_s: self.recovery_delay_s.or(base.recovery_delay_s),
            proxmox: self.proxmox.clone().or_else(|| base.proxmox.clone()),
            backup_every_h: self.backup_every_h.or(base.backup_every_h),
            backup_keep: self.backup_keep.or(base.backup_keep),
            backup_passphrase: self.backup_passphrase.clone().or_else(|| base.backup_passphrase.clone()),
        }
    }

    pub fn thresholds(&self) -> Thresholds {
        let d = Thresholds::default();
        let ms = |v: Option<u32>, d: std::time::Duration| v.map_or(d, |v| std::time::Duration::from_millis(v as u64));
        Thresholds { heartbeat: ms(self.heartbeat_ms, d.heartbeat), suspect: ms(self.suspect_ms, d.suspect), unreachable: ms(self.unreachable_ms, d.unreachable) }
    }

    pub fn fencing(&self) -> FenceConfig {
        let d = FenceConfig::default();
        FenceConfig {
            stop_s: self.fence_stop_s.unwrap_or(d.stop_s),
            storage_s: self.fence_storage_s.unwrap_or(d.storage_s),
            reboot_s: self.fence_reboot_s.unwrap_or(d.reboot_s),
            auto_authorize: self.auto_authorize.unwrap_or(d.auto_authorize),
            enabled: self.fence.unwrap_or(d.enabled),
            proxmox: self.proxmox.clone(),
            recovery_delay_s: self.recovery_delay_s.unwrap_or(d.recovery_delay_s),
        }
    }

    fn number(&self, key: &str) -> Option<u32> {
        match key {
            "heartbeat_ms" => self.heartbeat_ms,
            "suspect_ms" => self.suspect_ms,
            "unreachable_ms" => self.unreachable_ms,
            "fence_stop_s" => self.fence_stop_s,
            "fence_storage_s" => self.fence_storage_s,
            "fence_reboot_s" => self.fence_reboot_s,
            "recovery_delay_s" => self.recovery_delay_s,
            "backup_every_h" => self.backup_every_h,
            "backup_keep" => self.backup_keep,
            _ => None,
        }
    }

    fn number_mut(&mut self, key: &str) -> Option<&mut Option<u32>> {
        Some(match key {
            "heartbeat_ms" => &mut self.heartbeat_ms,
            "suspect_ms" => &mut self.suspect_ms,
            "unreachable_ms" => &mut self.unreachable_ms,
            "fence_stop_s" => &mut self.fence_stop_s,
            "fence_storage_s" => &mut self.fence_storage_s,
            "fence_reboot_s" => &mut self.fence_reboot_s,
            "recovery_delay_s" => &mut self.recovery_delay_s,
            "backup_every_h" => &mut self.backup_every_h,
            "backup_keep" => &mut self.backup_keep,
            _ => return None,
        })
    }

    fn switch(&self, key: &str) -> Option<bool> {
        match key {
            "fence" => self.fence,
            "auto_authorize" => self.auto_authorize,
            _ => None,
        }
    }

    fn switch_mut(&mut self, key: &str) -> Option<&mut Option<bool>> {
        Some(match key {
            "fence" => &mut self.fence,
            "auto_authorize" => &mut self.auto_authorize,
            _ => return None,
        })
    }

    /// Each value set is in its range, the ones set are in order, and the Proxmox settings hold. Checked when the
    /// cluster's settings change (they may leave some to each manager).
    pub fn check(&self) -> Result<(), String> {
        for (key, lo, hi) in NUMBERS {
            if let Some(v) = self.number(key)
                && !(lo..=hi).contains(&v)
            {
                return Err(format!("{key} is from {lo} to {hi}"));
            }
        }
        if let (Some(h), Some(s)) = (self.heartbeat_ms, self.suspect_ms)
            && h >= s
        {
            return Err("the heartbeat must be shorter than the suspect time".into());
        }
        if let (Some(s), Some(u)) = (self.suspect_ms, self.unreachable_ms)
            && s >= u
        {
            return Err("the suspect time must be shorter than the unreachable time".into());
        }
        if let Some(p) = &self.proxmox {
            ProxmoxConfig::checked(&p.url(), &p.token, &p.fingerprint).map_err(|e| format!("{e:#}"))?;
        }
        Ok(())
    }

    /// These settings with `patch` (a JSON object of the API) applied: a value sets one, `null` lets it go back to
    /// the manager's own. `proxmox` is `{url, fingerprint, token}` (the token may be left out to keep the one set)
    /// or `null`.
    pub fn patched(&self, patch: &Value) -> Result<Settings, String> {
        let Some(patch) = patch.as_object() else { return Err("the settings are a JSON object".into()) };
        let mut s = self.clone();
        for (key, v) in patch {
            if let Some(slot) = s.number_mut(key) {
                *slot = match v {
                    Value::Null => None,
                    v => Some(v.as_u64().and_then(|n| u32::try_from(n).ok()).ok_or_else(|| format!("{key} is a whole number"))?),
                };
            } else if let Some(slot) = s.switch_mut(key) {
                *slot = match v {
                    Value::Null => None,
                    v => Some(v.as_bool().ok_or_else(|| format!("{key} is true or false"))?),
                };
            } else if key == "backup_passphrase" {
                s.backup_passphrase = match v {
                    Value::Null => None,
                    Value::String(p) if p.chars().count() >= crate::backup::MIN_PASSPHRASE => Some(Secret(p.clone())),
                    _ => return Err(format!("the backup passphrase is at least {} characters", crate::backup::MIN_PASSPHRASE)),
                };
            } else if key == "proxmox" {
                s.proxmox = match v {
                    Value::Null => None,
                    Value::Object(p) => {
                        let text = |k: &str| p.get(k).and_then(Value::as_str).map(str::trim).filter(|t| !t.is_empty());
                        let token = match (text("token"), &self.proxmox) {
                            (Some(t), _) => t.to_string(),
                            (None, Some(old)) => old.token.clone(),
                            (None, None) => return Err("the Proxmox provider needs a token".into()),
                        };
                        let url = text("url").ok_or("the Proxmox provider needs the API's address")?;
                        let fp = text("fingerprint").ok_or("the Proxmox provider needs the certificate's fingerprint")?;
                        Some(ProxmoxConfig::checked(url, &token, fp).map_err(|e| format!("{e:#}"))?)
                    }
                    _ => return Err("proxmox is {url, fingerprint, token} or null".into()),
                };
            } else {
                return Err(format!("{key} is not a setting"));
            }
        }
        Ok(s)
    }
}

/// `AB:CD:...` as Proxmox shows a fingerprint.
pub fn colons(hex: &str) -> String {
    hex.as_bytes().chunks(2).map(|c| std::str::from_utf8(c).unwrap_or("").to_ascii_uppercase()).collect::<Vec<_>>().join(":")
}

/// The settings for the API: each value in use, where it comes from (`cluster`, `manager` (this manager's
/// environment) or `default`) and its default; the Proxmox provider without its token's secret.
pub fn view(cluster: &Settings, manager: &Settings) -> Value {
    let d = Settings::defaults();
    let source = |c: bool, m: bool| if c { "cluster" } else if m { "manager" } else { "default" };
    let mut out = Map::new();
    for (key, lo, hi) in NUMBERS {
        let m = manager.number(key);
        let value = cluster.number(key).or(m).or(d.number(key));
        out.insert(key.into(), json!({ "value": value, "source": source(cluster.number(key).is_some(), m.is_some() && m != d.number(key)), "default": d.number(key), "min": lo, "max": hi }));
    }
    for key in SWITCHES {
        let m = manager.switch(key);
        let value = cluster.switch(key).or(m).or(d.switch(key));
        out.insert(key.into(), json!({ "value": value, "source": source(cluster.switch(key).is_some(), m.is_some() && m != d.switch(key)), "default": d.switch(key) }));
    }
    let p = cluster.proxmox.as_ref().map(|p| (p, "cluster")).or_else(|| manager.proxmox.as_ref().map(|p| (p, "manager")));
    out.insert("backup_passphrase_set".into(), json!(cluster.backup_passphrase.is_some()));
    out.insert("proxmox".into(), p.map_or(Value::Null, |(p, src)| json!({ "url": p.url(), "token_id": p.token_id(), "fingerprint": colons(&p.fingerprint), "source": src })));
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp() -> String {
        "ab".repeat(32)
    }

    #[test]
    fn the_cluster_s_value_wins_then_the_manager_s_then_the_default() {
        let manager = Settings { fence_stop_s: Some(20), suspect_ms: Some(6000), ..Settings::defaults() };
        let cluster = Settings { fence_stop_s: Some(45), ..Default::default() };
        let s = cluster.over(&manager);
        assert_eq!(s.fence_stop_s, Some(45));
        assert_eq!(s.suspect_ms, Some(6000));
        assert_eq!(s.fencing().stop_s, 45);
        assert_eq!(s.thresholds().suspect.as_millis(), 6000);
        assert_eq!(Settings::default().fencing(), FenceConfig::default(), "nothing set is the defaults");
        assert_eq!(Settings::default().thresholds(), Thresholds::default());
    }

    #[test]
    fn values_are_checked_for_range_and_order() {
        assert!(Settings::defaults().check().is_ok());
        assert!(Settings { fence_stop_s: Some(0), ..Default::default() }.check().unwrap_err().contains("fence_stop_s"));
        assert!(Settings { heartbeat_ms: Some(100), ..Default::default() }.check().is_err(), "the worker clamps below 500");
        assert!(Settings { heartbeat_ms: Some(5000), suspect_ms: Some(5000), ..Default::default() }.check().is_err());
        assert!(Settings { suspect_ms: Some(9000), unreachable_ms: Some(8000), ..Default::default() }.check().is_err());
        assert!(Settings { recovery_delay_s: Some(0), ..Default::default() }.check().is_ok());
        let mut bad = ProxmoxConfig::checked("https://pve", "a@pve!b=c", &fp()).unwrap();
        bad.token = "nope".into();
        assert!(Settings { proxmox: Some(bad), ..Default::default() }.check().is_err(), "a replicated entry is checked again");
    }

    #[test]
    fn a_patch_sets_and_lets_go() {
        let s = Settings { fence_stop_s: Some(40), ..Default::default() };
        let p = s.patched(&json!({ "fence_stop_s": null, "recovery_delay_s": 30, "fence": false })).unwrap();
        assert_eq!((p.fence_stop_s, p.recovery_delay_s, p.fence), (None, Some(30), Some(false)));
        assert!(s.patched(&json!({ "nope": 1 })).unwrap_err().contains("not a setting"));
        assert!(s.patched(&json!({ "fence_stop_s": "x" })).is_err());
        assert!(s.patched(&json!({ "fence_stop_s": -1 })).is_err());
        assert!(s.patched(&json!({ "fence": 1 })).is_err());
    }

    #[test]
    fn the_proxmox_token_is_kept_when_left_out_and_never_shown() {
        let s = Settings::default().patched(&json!({ "proxmox": { "url": "https://pve", "fingerprint": fp(), "token": "fd@pve!fence=s3cret" } })).unwrap();
        let again = s.patched(&json!({ "proxmox": { "url": "https://pve2:8443", "fingerprint": fp() } })).unwrap();
        let p = again.proxmox.as_ref().unwrap();
        assert_eq!((p.host.as_str(), p.token.as_str()), ("pve2:8443", "fd@pve!fence=s3cret"));
        assert!(Settings::default().patched(&json!({ "proxmox": { "url": "https://pve", "fingerprint": fp() } })).unwrap_err().contains("token"));
        assert_eq!(again.patched(&json!({ "proxmox": null })).unwrap().proxmox, None);
        let v = view(&again, &Settings::defaults());
        assert!(!v.to_string().contains("s3cret"), "{v}");
        assert_eq!(v["proxmox"]["token_id"], "fd@pve!fence");
        assert_eq!(v["proxmox"]["source"], "cluster");
        assert_eq!(v["proxmox"]["fingerprint"], colons(&fp()));
        assert!(!format!("{again:?}").contains("s3cret"));
    }

    #[test]
    fn the_backup_passphrase_is_set_and_never_shown() {
        let s = Settings::default().patched(&json!({ "backup_passphrase": "correct horse battery", "backup_every_h": 6, "backup_keep": 3 })).unwrap();
        assert_eq!(s.backup_passphrase.as_ref().map(|p| p.0.as_str()), Some("correct horse battery"));
        assert!(!format!("{s:?}").contains("horse"), "{s:?}");
        let v = view(&s, &Settings::defaults());
        assert!(!v.to_string().contains("horse"), "{v}");
        assert_eq!((v["backup_passphrase_set"].as_bool(), v["backup_every_h"]["value"].as_u64(), v["backup_keep"]["value"].as_u64()), (Some(true), Some(6), Some(3)));
        assert_eq!(view(&Settings::default(), &Settings::defaults())["backup_every_h"]["value"], 24, "daily by default, once there is a passphrase");
        assert!(Settings::default().patched(&json!({ "backup_passphrase": "short" })).unwrap_err().contains("12"));
        assert_eq!(s.patched(&json!({ "backup_passphrase": null })).unwrap().backup_passphrase, None);
        assert!(Settings { backup_keep: Some(0), ..Default::default() }.check().is_err());
        assert!(Settings { backup_every_h: Some(0), ..Default::default() }.check().is_ok(), "0: off");
    }

    #[test]
    fn the_view_says_where_each_value_comes_from() {
        let manager = Settings { fence_stop_s: Some(20), ..Settings::defaults() };
        let cluster = Settings { recovery_delay_s: Some(60), ..Default::default() };
        let v = view(&cluster, &manager);
        assert_eq!((v["recovery_delay_s"]["value"].as_u64(), v["recovery_delay_s"]["source"].as_str()), (Some(60), Some("cluster")));
        assert_eq!((v["fence_stop_s"]["value"].as_u64(), v["fence_stop_s"]["source"].as_str()), (Some(20), Some("manager")));
        assert_eq!((v["heartbeat_ms"]["value"].as_u64(), v["heartbeat_ms"]["source"].as_str()), (Some(2000), Some("default")));
        assert_eq!(v["fence"]["value"], true);
        assert_eq!(v["proxmox"], Value::Null);
    }
}
