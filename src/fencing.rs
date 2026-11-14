//! Fencing on the managers' side (G6, plan 7.2 to 7.4). The worker fences itself (Freightdock-Worker `fence.rs`); the
//! managers keep where each node is in it, as entries in the replicated state, and never take "unreachable" for
//! "fenced".
//!
//! Two sets of rules, both run each second by `housekeeping`:
//!
//! - **The leader** (`fence_round`) walks the nodes: one the cluster agrees is unreachable is requested to be fenced,
//!   the provider takes it in hand, and with the `self` provider it is called fenced once the worker's own timers must
//!   have run out (presumed, and said so: there is no proof of power-off). The leader waits `STARTUP_GRACE` after it
//!   starts, since a manager that has just started has heard from nobody yet.
//! - **The manager a worker is connected to** (`fence_local`) follows what the worker says: fenced, so awaiting
//!   authorization; not fenced while the state says it is being fenced, so it was heard again first and goes back to
//!   rest; authorized, so it is sent `Authorize` until it says it runs again. With `auto_authorize` (the default)
//!   that manager authorizes a fenced worker that came back by itself: the workloads still placed on it are its own,
//!   and the reconcile runs only those. Without, an administrator does (`POST /v1/nodes/{id}/authorize`).
//!
//! The reconcile runs nothing on a node unless its state is at rest and its worker says it is not fenced.

use std::time::Duration;

use anyhow::{Result, bail};

use crate::sessions::Health;
use crate::state::{Entry, FenceState, Fencing, Ms};
use crate::{Manager, now_ms};

/// How long the leader waits after it starts before it asks for any fencing.
pub const STARTUP_GRACE: Duration = Duration::from_secs(30);
/// Added to the worker's timers before a self-fenced node is called fenced: clocks, a heartbeat or two.
pub const SELF_GRACE_MS: i64 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceConfig {
    /// The cluster's self-fencing timers, sent to every worker in its Welcome.
    pub stop_s: u32,
    pub storage_s: u32,
    pub reboot_s: u32,
    /// The managers authorize a worker that came back fenced by themselves.
    pub auto_authorize: bool,
    /// The leader asks for fencing at all. Off, nodes are only ever unreachable.
    pub enabled: bool,
    /// The Proxmox provider's settings, for nodes labelled `fence.provider=proxmox`.
    pub proxmox: Option<crate::proxmox::ProxmoxConfig>,
    /// The Redfish provider's account, for nodes labelled `fence.provider=redfish`.
    pub redfish: Option<crate::redfish::RedfishConfig>,
    /// How long a node stays fenced before its stateless workloads are started elsewhere (G7).
    pub recovery_delay_s: u32,
}

impl Default for FenceConfig {
    fn default() -> Self {
        FenceConfig { stop_s: 30, storage_s: 15, reboot_s: 15, auto_authorize: true, enabled: true, proxmox: None, redfish: None, recovery_delay_s: 0 }
    }
}

impl FenceConfig {
    /// `FREIGHTDOCK_FENCE` (on, off), `FREIGHTDOCK_FENCE_STOP_S`, `_STORAGE_S`, `_REBOOT_S` and
    /// `FREIGHTDOCK_FENCE_AUTO_AUTHORIZE` (on, off), the Proxmox settings and `FREIGHTDOCK_RECOVERY_DELAY_S` (0 or more).
    pub fn from_env() -> Result<Self> {
        Self::from(|k| std::env::var(k).ok())
    }

    pub fn from(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let d = FenceConfig::default();
        let onoff = |k: &str, d: bool| -> Result<bool> {
            match get(k).as_deref().map(str::trim) {
                None => Ok(d),
                Some("on") => Ok(true),
                Some("off") => Ok(false),
                Some(v) => bail!("{k} is on or off, not {v}"),
            }
        };
        let secs = |k: &str, d: u32| -> Result<u32> {
            match get(k) {
                Some(v) => v.trim().parse::<u32>().ok().filter(|n| *n > 0).ok_or_else(|| anyhow::anyhow!("{k} is a number of seconds above 0")),
                None => Ok(d),
            }
        };
        Ok(FenceConfig {
            stop_s: secs("FREIGHTDOCK_FENCE_STOP_S", d.stop_s)?,
            storage_s: secs("FREIGHTDOCK_FENCE_STORAGE_S", d.storage_s)?,
            reboot_s: secs("FREIGHTDOCK_FENCE_REBOOT_S", d.reboot_s)?,
            auto_authorize: onoff("FREIGHTDOCK_FENCE_AUTO_AUTHORIZE", d.auto_authorize)?,
            enabled: onoff("FREIGHTDOCK_FENCE", d.enabled)?,
            proxmox: crate::proxmox::ProxmoxConfig::from(&get)?,
            redfish: None,
            recovery_delay_s: match get("FREIGHTDOCK_RECOVERY_DELAY_S") {
                Some(v) => v.trim().parse::<u32>().map_err(|_| anyhow::anyhow!("FREIGHTDOCK_RECOVERY_DELAY_S is a number of seconds"))?,
                None => d.recovery_delay_s,
            },
        })
    }

    /// How long after it was last heard a self-fencing worker has surely stopped its workloads (or rebooted).
    pub fn self_fenced_after_ms(&self) -> i64 {
        (self.stop_s + self.storage_s + self.reboot_s) as i64 * 1000 + SELF_GRACE_MS
    }
}

/// How long after a failed fencing it is tried again.
pub const RETRY_FAILED_MS: i64 = 60_000;

/// The leader's next step for one node, if any: (state, message). `provider` is the node's (`self` or `proxmox`); a
/// provider's power-off itself runs as a task (`Manager::fence_round`), not as a step here.
pub fn leader_step(f: &Fencing, provider: &str, health: Health, reported: bool, now: Ms, cfg: &FenceConfig) -> Option<(FenceState, String)> {
    let unreachable = health == Health::Unreachable && reported;
    match f.state {
        FenceState::None if unreachable => Some((FenceState::Requested, "the cluster hears it no more".into())),
        FenceState::Failed if unreachable && now - f.since >= RETRY_FAILED_MS => Some((FenceState::Requested, "trying again".into())),
        FenceState::Requested if provider == "self" => Some((FenceState::InProgress, format!("waiting for its own fencing timers ({}s)", cfg.self_fenced_after_ms() / 1000))),
        FenceState::Requested => Some((FenceState::InProgress, format!("asking {provider} to power it off"))),
        FenceState::InProgress if f.provider == "self" && now - f.since >= cfg.self_fenced_after_ms() => {
            Some((FenceState::Fenced, "presumed: its self-fencing timers have run out (no proof of power-off)".into()))
        }
        _ => None,
    }
}

/// What the manager a worker is connected to does about it.
#[derive(Debug, PartialEq, Eq)]
pub enum LocalStep {
    Set(FenceState, &'static str),
    Authorize,
    SendAuthorize,
}

pub fn local_step(state: FenceState, worker_fenced: bool, auto_authorize: bool) -> Option<LocalStep> {
    use FenceState as F;
    match (worker_fenced, state) {
        (true, F::AwaitingAuthorization) if auto_authorize => Some(LocalStep::Authorize),
        (true, F::AwaitingAuthorization) => None,
        (true, F::Authorized) => Some(LocalStep::SendAuthorize),
        (true, _) => Some(LocalStep::Set(F::AwaitingAuthorization, "it came back fenced")),
        (false, F::Authorized) => Some(LocalStep::Set(F::None, "it runs again")),
        (false, F::Requested | F::InProgress | F::Fenced | F::Failed) => Some(LocalStep::Set(F::None, "heard again; it had not fenced")),
        (false, _) => None,
    }
}

impl Manager {
    async fn set_fencing(&self, node: &str, state: FenceState, provider: &str, message: String) {
        let e = Entry::NodeFencing { node_id: node.to_string(), state, provider: provider.to_string(), message: message.clone(), at: now_ms() };
        match self.store.propose(e).await {
            Ok(_) => {
                tracing::warn!(node = %node, ?state, "fencing: {message}");
                self.events.push("fencing", Some(node), format!("{}: {message}", word(state)));
            }
            Err(e) => tracing::debug!(node = %node, ?state, "fencing step not taken: {e}"),
        }
    }

    /// The leader's round (see the module).
    pub async fn fence_round(self: &std::sync::Arc<Self>) {
        let cfg = self.fence();
        if !cfg.enabled || self.store.leader() != Some(self.store.id) || self.started.elapsed() < STARTUP_GRACE {
            return;
        }
        let nodes: Vec<(crate::state::Node, String)> = self.store.read(|s| s.nodes.values().map(|n| (n.clone(), crate::proxmox::provider_of(n))).collect());
        for (n, provider) in nodes {
            let health = self.sessions.liveness(&n.id).health;
            if let Some((state, message)) = leader_step(&n.fencing, &provider, health, n.report.is_some(), now_ms(), &cfg) {
                self.set_fencing(&n.id, state, &provider, message).await;
            } else if n.fencing.state == FenceState::InProgress && matches!(n.fencing.provider.as_str(), "proxmox" | "redfish") {
                self.provider_task(n);
            }
        }
        self.evict_round().await;
    }

    /// Powers the node off through its provider (its guest through Proxmox, its system through its BMC), once at a
    /// time per node (a new leader takes over one that was in progress: powering off what is off is harmless), and
    /// records what the provider says.
    fn provider_task(self: &std::sync::Arc<Self>, n: crate::state::Node) {
        if !self.fence_tasks.lock().expect("fence tasks lock").insert(n.id.clone()) {
            return;
        }
        let m = self.clone();
        tokio::spawn(async move {
            let provider = n.fencing.provider.clone();
            let r = async {
                if provider == "redfish" {
                    let cfg = m.fence().redfish.ok_or_else(|| anyhow::anyhow!("the cluster has no Redfish account set (Integrations)"))?;
                    return crate::redfish::Redfish::new(cfg, crate::redfish::bmc_of(&n)?)?.fence(Duration::from_secs(60)).await;
                }
                let cfg = m.fence().proxmox.ok_or_else(|| anyhow::anyhow!("the cluster has no Proxmox provider set (Integrations, or FREIGHTDOCK_FENCE_PROXMOX_URL)"))?;
                let g = crate::proxmox::guest_of(&n)?;
                crate::proxmox::Proxmox::new(cfg)?.fence(&g, Duration::from_secs(60)).await
            }
            .await;
            match r {
                Ok(msg) => m.set_fencing(&n.id, FenceState::Fenced, &provider, msg).await,
                Err(e) => m.set_fencing(&n.id, FenceState::Failed, &provider, format!("{e:#}")).await,
            }
            m.fence_tasks.lock().expect("fence tasks lock").remove(&n.id);
        });
    }

    /// The round for the workers connected here (see the module).
    pub async fn fence_local(&self) {
        let auto = self.fence().auto_authorize;
        for (id, worker_fenced) in self.sessions.local_fencing() {
            let Some(state) = self.store.read(|s| s.nodes.get(&id).map(|n| n.fencing.state)) else { continue };
            match local_step(state, worker_fenced, auto) {
                Some(LocalStep::Set(to, why)) => self.set_fencing(&id, to, "worker", why.to_string()).await,
                Some(LocalStep::Authorize) => {
                    if self.authorize(&id, "the managers (automatic)").await.is_ok() {
                        self.send_authorize(&id, "the managers (automatic)");
                    }
                }
                Some(LocalStep::SendAuthorize) => {
                    let by = self.store.read(|s| s.nodes.get(&id).map(|n| n.fencing.provider.clone())).unwrap_or_default();
                    self.send_authorize(&id, &by);
                }
                None => {}
            }
        }
    }

    /// Authorizes a node that came back fenced (plan 7.4).
    pub async fn authorize(&self, node: &str, by: &str) -> Result<(), crate::store::ProposeError> {
        self.store.propose(Entry::NodeAuthorized { node_id: node.to_string(), by: by.to_string(), at: now_ms() }).await?;
        tracing::warn!(node = %node, "authorized by {by}");
        self.events.push("fencing", Some(node), format!("authorized by {by}"));
        Ok(())
    }

    fn send_authorize(&self, node: &str, by: &str) {
        use freightdock_proto::v1::{Authorize, ManagerMessage, manager_message::Body};
        if let Some(tx) = self.sessions.sender(node) {
            let _ = tx.try_send(Ok(ManagerMessage { body: Some(Body::Authorize(Authorize { by: by.to_string() })) }));
        }
    }
}

pub fn word(s: FenceState) -> &'static str {
    match s {
        FenceState::None => "at rest",
        FenceState::Requested => "fencing requested",
        FenceState::InProgress => "fencing in progress",
        FenceState::Fenced => "fenced",
        FenceState::Failed => "fencing failed",
        FenceState::AwaitingAuthorization => "awaiting authorization",
        FenceState::Authorized => "authorized",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(state: FenceState, since: Ms, provider: &str) -> Fencing {
        Fencing { state, since, provider: provider.into(), message: String::new() }
    }

    #[test]
    fn the_leader_requests_then_waits_out_the_worker_s_timers() {
        let c = FenceConfig::default();
        let rest = Fencing::default();
        assert_eq!(leader_step(&rest, "self", Health::Healthy, true, 0, &c), None);
        assert_eq!(leader_step(&rest, "self", Health::Suspect, true, 0, &c), None, "suspect is not unreachable");
        assert_eq!(leader_step(&rest, "self", Health::Unreachable, false, 0, &c), None, "a node that never spoke has nothing to fence");
        assert_eq!(leader_step(&rest, "self", Health::Unreachable, true, 0, &c).map(|s| s.0), Some(FenceState::Requested));
        assert_eq!(leader_step(&at(FenceState::Requested, 0, "self"), "self", Health::Unreachable, true, 1, &c).map(|s| s.0), Some(FenceState::InProgress));
        let going = at(FenceState::InProgress, 1_000, "self");
        assert_eq!(leader_step(&going, "self", Health::Unreachable, true, 1_000 + 69_999, &c), None, "30+15+15 s and 10 s grace");
        let done = leader_step(&going, "self", Health::Unreachable, true, 1_000 + 70_000, &c).unwrap();
        assert_eq!(done.0, FenceState::Fenced);
        assert!(done.1.contains("presumed"), "{}", done.1);
        assert_eq!(leader_step(&at(FenceState::Fenced, 0, "self"), "self", Health::Unreachable, true, 1 << 40, &c), None, "it stays fenced");
    }

    #[test]
    fn a_provider_powers_off_and_a_failure_is_tried_again_after_a_minute() {
        let c = FenceConfig::default();
        let s = leader_step(&at(FenceState::Requested, 0, "proxmox"), "proxmox", Health::Unreachable, true, 1, &c).unwrap();
        assert_eq!(s, (FenceState::InProgress, "asking proxmox to power it off".into()));
        assert_eq!(leader_step(&at(FenceState::InProgress, 0, "proxmox"), "proxmox", Health::Unreachable, true, 1 << 40, &c), None, "the task decides, not the clock");
        let failed = at(FenceState::Failed, 1_000, "proxmox");
        assert_eq!(leader_step(&failed, "proxmox", Health::Unreachable, true, 1_000 + 59_999, &c), None);
        assert_eq!(leader_step(&failed, "proxmox", Health::Unreachable, true, 1_000 + 60_000, &c).map(|s| s.0), Some(FenceState::Requested));
    }

    #[test]
    fn the_worker_s_manager_follows_what_the_worker_says() {
        use FenceState::*;
        assert_eq!(local_step(None, false, true), Option::None);
        assert_eq!(local_step(None, true, true), Some(LocalStep::Set(AwaitingAuthorization, "it came back fenced")));
        assert_eq!(local_step(Fenced, true, false), Some(LocalStep::Set(AwaitingAuthorization, "it came back fenced")));
        assert_eq!(local_step(AwaitingAuthorization, true, true), Some(LocalStep::Authorize));
        assert_eq!(local_step(AwaitingAuthorization, true, false), Option::None, "an administrator authorizes");
        assert_eq!(local_step(Authorized, true, false), Some(LocalStep::SendAuthorize));
        assert_eq!(local_step(Authorized, false, false), Some(LocalStep::Set(None, "it runs again")));
        assert!(matches!(local_step(InProgress, false, true), Some(LocalStep::Set(None, _))), "heard again first");
    }

    #[test]
    fn the_settings_read_from_the_environment() {
        let env = |pairs: &'static [(&'static str, &'static str)]| move |k: &str| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| v.to_string());
        let c = FenceConfig::from(env(&[("FREIGHTDOCK_FENCE_STOP_S", "20"), ("FREIGHTDOCK_FENCE_AUTO_AUTHORIZE", "off")])).unwrap();
        assert_eq!((c.stop_s, c.auto_authorize, c.enabled), (20, false, true));
        assert_eq!(c.self_fenced_after_ms(), 60_000);
        assert!(FenceConfig::from(env(&[("FREIGHTDOCK_FENCE_STOP_S", "0")])).is_err());
    }
}
