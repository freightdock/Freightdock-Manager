//! Ceph on the managers' side (G12.6). A CephFS is shared storage the workers' kernels mount (`state::Storage`,
//! driver `cephfs`); what the managers ask of Ceph itself goes as a `CephCommand` to a healthy worker, which runs the
//! `ceph` command line in a throwaway container of the Ceph image, the key filled in only as the command is sent.
//!
//! Fencing (plan 7.1, the storage's own revocation): once a node is fenced (verified, or presumed after its own timers
//! ran out), the leader evicts its CephFS clients, each storage with a fencing client: `ceph tell mds.<fs>:0 client
//! evict` of the mounts with the node's hostname and the storage's client name. Ceph blocklists what it evicts, so a
//! node that did not stop writing (its fencing hung, it was cut off from the managers but not from Ceph) can write no
//! more, and the files it held are free at once instead of after the MDS's own session timeout. Not before the node is
//! fenced: a node that is only unreachable for a moment keeps its mounts. A mount made later (the node authorized,
//! back) is a new session and is not blocklisted, so nothing has to be undone.

//!
//! Ceph made from the UI (`CephSetup`): an administrator picks workers and their empty disks; the leader runs the
//! steps one at a time, each a `Cephadm` command to one worker (prepare each node, bootstrap on the first, trust its
//! key and add the others, place the monitors and managers, an OSD on each disk, the filesystem, the clients' keys),
//! the steps and what they learned kept in the replicated state, so a new leader goes on where the old one was. The
//! last step's keys go into a `cephfs` storage as its answer comes in and are kept nowhere else.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::state::{ClusterState, CommandRecord, CommandStatus, FenceState, Ms, Op, StorageDriver};
use crate::{Manager, now_ms};

/// Where cephadm is fetched from: the release of the image the cluster runs.
pub const CEPHADM_URL: &str = "https://download.ceph.com/rpm-20.2.0/el9/noarch/cephadm";
/// The clients a cluster made from the UI is mounted and fenced with.
pub const MOUNT_CLIENT: &str = "freightdock";
pub const FENCE_CLIENT: &str = "freightdock-fence";

/// A node Ceph is set up on, and the disks it takes whole.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CephHost {
    pub node_id: String,
    pub disks: Vec<String>,
    /// Learned as it is prepared.
    #[serde(default)]
    pub hostname: String,
    #[serde(default)]
    pub address: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepState {
    Pending,
    Running,
    Done,
    Failed,
}

/// One step: what it is (for the person reading), the worker that runs it, and the `Cephadm` action with its
/// arguments. An argument may be `{address:<node>}`, `{hostname:<node>}` or `{public_key}`, filled in from what the
/// earlier steps learned as the step is run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetupStep {
    pub kind: String,
    /// The node it is about (prepare, trust, add a host, an OSD), else the first host's.
    pub about: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub disk: String,
    pub node_id: String,
    pub action: String,
    pub args: Vec<String>,
    pub state: StepState,
    #[serde(default)]
    pub command_id: Option<String>,
    #[serde(default)]
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupState {
    Running,
    Failed,
    Done,
}

/// What a step learned: a node's address and name (prepare), the cluster's fsid and cephadm's key (bootstrap).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Learned {
    #[serde(default)]
    pub address: String,
    #[serde(default)]
    pub hostname: String,
    #[serde(default)]
    pub fsid: String,
    #[serde(default)]
    pub public_key: String,
}

/// Ceph being made, or made, from the UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CephSetup {
    /// The cephfs storage it becomes, and the filesystem.
    pub storage: String,
    pub fs: String,
    pub image: String,
    pub hosts: Vec<CephHost>,
    pub steps: Vec<SetupStep>,
    pub state: SetupState,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub fsid: String,
    #[serde(default)]
    pub public_key: String,
    pub started_at: Ms,
    pub updated_at: Ms,
    pub by: String,
}

fn plain(s: &str, extra: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || extra.contains(c))
}

/// How many monitors for so many hosts: odd, at most five.
pub fn monitors(hosts: usize) -> usize {
    match hosts {
        0..=2 => 1,
        3 | 4 => 3,
        _ => 5,
    }
}

impl CephSetup {
    /// A setup of these hosts (the first bootstraps), every step pending.
    pub fn new(storage: &str, fs: &str, image: &str, hosts: Vec<CephHost>, by: &str, at: Ms) -> CephSetup {
        let first = hosts.first().map(|h| h.node_id.clone()).unwrap_or_default();
        let step = |kind: &str, about: &str, disk: &str, on: &str, action: &str, args: Vec<String>| SetupStep { kind: kind.into(), about: about.into(), disk: disk.into(), node_id: on.into(), action: action.into(), args, state: StepState::Pending, command_id: None, message: String::new() };
        let shell = |kind: &str, about: &str, disk: &str, args: &[&str]| step(kind, about, disk, &first, "shell", args.iter().map(|a| a.to_string()).collect());
        let mut steps: Vec<SetupStep> = hosts.iter().map(|h| step("prepare", &h.node_id, "", &h.node_id, "prepare", vec![CEPHADM_URL.into()])).collect();
        let single = if hosts.len() == 1 { "single-host" } else { "" };
        steps.push(step("bootstrap", &first, "", &first, "bootstrap", vec![format!("{{address:{first}}}"), single.into()]));
        for h in hosts.iter().skip(1) {
            steps.push(step("trust", &h.node_id, "", &h.node_id, "trust", vec!["{public_key}".into()]));
        }
        for h in hosts.iter().skip(1) {
            steps.push(shell("add_host", &h.node_id, "", &["orch", "host", "add", &format!("{{hostname:{}}}", h.node_id), &format!("{{address:{}}}", h.node_id)]));
        }
        let names = |n: usize| hosts.iter().take(n).map(|h| format!("{{hostname:{}}}", h.node_id)).collect::<Vec<_>>().join(",");
        if hosts.len() > 1 {
            steps.push(shell("monitors", &first, "", &["orch", "apply", "mon", &format!("--placement={}", names(monitors(hosts.len())))]));
            steps.push(shell("managers", &first, "", &["orch", "apply", "mgr", &format!("--placement={}", names(2))]));
        }
        if hosts.len() == 2 {
            steps.push(shell("pool_size", &first, "", &["config", "set", "global", "osd_pool_default_size", "2"]));
            steps.push(shell("pool_min_size", &first, "", &["config", "set", "global", "osd_pool_default_min_size", "1"]));
        }
        // The OSDs share their nodes with the cluster's workloads: a third of the memory, not cephadm's 70 %.
        steps.push(shell("memory", &first, "", &["config", "set", "mgr", "mgr/cephadm/autotune_memory_target_ratio", "0.3"]));
        for h in &hosts {
            for d in &h.disks {
                steps.push(shell("osd", &h.node_id, d, &["orch", "daemon", "add", "osd", &format!("{{hostname:{}}}:{d}", h.node_id)]));
            }
        }
        steps.push(shell("filesystem", &first, "", &["fs", "volume", "create", fs]));
        steps.push(step("keys", &first, "", &first, "keys", vec![fs.into(), MOUNT_CLIENT.into(), FENCE_CLIENT.into()]));
        CephSetup { storage: storage.into(), fs: fs.into(), image: image.into(), hosts, steps, state: SetupState::Running, message: String::new(), fsid: String::new(), public_key: String::new(), started_at: at, updated_at: at, by: by.into() }
    }

    /// Well formed: a storage and filesystem name, hosts each once with at least one disk, every disk a /dev path.
    pub fn valid(&self) -> bool {
        let disk = |d: &str| d.strip_prefix("/dev/").is_some_and(|r| plain(r, "/_-.") && !r.contains(".."));
        let mut seen = std::collections::BTreeSet::new();
        crate::state::valid_stack_name(&self.storage)
            && plain(&self.fs, "_-.")
            && !self.hosts.is_empty()
            && self.hosts.iter().all(|h| seen.insert(h.node_id.clone()) && !h.disks.is_empty() && h.disks.iter().all(|d| disk(d)))
    }

    /// The step to run now: the first not done, while it runs.
    pub fn current(&self) -> Option<(usize, &SetupStep)> {
        (self.state == SetupState::Running).then(|| self.steps.iter().enumerate().find(|(_, s)| s.state != StepState::Done)).flatten()
    }

    /// A step's arguments with what the earlier steps learned filled in; None while something is not known yet.
    pub fn resolve(&self, step: &SetupStep) -> Option<Vec<String>> {
        let host = |id: &str| self.hosts.iter().find(|h| h.node_id == id);
        step.args
            .iter()
            .map(|a| {
                let mut out = a.clone();
                while let Some(i) = out.find('{') {
                    let j = out[i..].find('}')? + i;
                    let (key, node) = out[i + 1..j].split_once(':').unwrap_or((&out[i + 1..j], ""));
                    let v = match key {
                        "address" => host(node)?.address.clone(),
                        "hostname" => host(node)?.hostname.clone(),
                        "public_key" => self.public_key.clone(),
                        _ => return None,
                    };
                    if v.is_empty() {
                        return None;
                    }
                    out.replace_range(i..=j, &v);
                }
                Some(out)
            })
            .collect()
    }

    /// The monitors' addresses, as a cephfs source names them.
    pub fn mons(&self) -> String {
        self.hosts.iter().take(monitors(self.hosts.len())).map(|h| h.address.clone()).collect::<Vec<_>>().join(",")
    }

    /// A step's progress taken in.
    pub fn record(&mut self, index: usize, state: StepState, command_id: Option<String>, message: &str, learned: Option<&Learned>, at: Ms) {
        let Some(step) = self.steps.get_mut(index) else { return };
        step.state = state;
        if command_id.is_some() {
            step.command_id = command_id;
        }
        step.message = message.to_string();
        let about = step.about.clone();
        if let Some(l) = learned {
            if let Some(h) = self.hosts.iter_mut().find(|h| h.node_id == about) {
                if !l.address.is_empty() {
                    h.address = l.address.clone();
                }
                if !l.hostname.is_empty() {
                    h.hostname = l.hostname.clone();
                }
            }
            if !l.fsid.is_empty() {
                self.fsid = l.fsid.clone();
            }
            if !l.public_key.is_empty() {
                self.public_key = l.public_key.clone();
            }
        }
        match state {
            StepState::Failed => {
                self.state = SetupState::Failed;
                self.message = message.to_string();
            }
            StepState::Done if self.steps.iter().all(|s| s.state == StepState::Done) => self.state = SetupState::Done,
            _ => {}
        }
        self.updated_at = at;
    }

    /// A failed setup goes on from its failed step.
    pub fn retry(&mut self, at: Ms) {
        for s in self.steps.iter_mut().filter(|s| s.state != StepState::Done) {
            s.state = StepState::Pending;
            s.command_id = None;
        }
        self.state = SetupState::Running;
        self.message.clear();
        self.updated_at = at;
    }
}

/// What a step's command answered that the setup keeps.
pub fn learned(action: &str, result_json: &str) -> Option<Learned> {
    let v: serde_json::Value = serde_json::from_str(result_json).ok()?;
    let s = |k: &str| v[k].as_str().unwrap_or("").to_string();
    match action {
        "prepare" => Some(Learned { address: s("address"), hostname: s("hostname"), ..Default::default() }),
        "bootstrap" => Some(Learned { fsid: s("fsid"), public_key: s("public_key"), ..Default::default() }),
        _ => None,
    }
}

/// How long an eviction's command may take before it is tried again on another worker.
pub const RETRY_MS: i64 = 30_000;

/// The arguments after `ceph` that evict `client`'s mounts from the node named `hostname`.
pub fn evict_args(fs: &str, hostname: &str, client: &str) -> Vec<String> {
    vec!["tell".into(), format!("mds.{fs}:0"), "client".into(), "evict".into(), format!("client_metadata.hostname={hostname}"), format!("client_metadata.entity_id={client}")]
}

/// One eviction the leader owes: a fenced node's mounts of one storage, for this fencing of it (`since`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Due {
    pub node_id: String,
    pub since: i64,
    pub storage: String,
}

/// Every eviction owed now: each fenced node, each CephFS that has a fencing client.
pub fn due(s: &ClusterState) -> Vec<Due> {
    let evicting: Vec<&str> = s.storage.values().filter(|st| st.driver == StorageDriver::Cephfs && st.ceph.as_ref().is_some_and(|k| !k.fence_client.is_empty())).map(|st| st.name.as_str()).collect();
    s.nodes
        .values()
        .filter(|n| n.fencing.state == FenceState::Fenced)
        .flat_map(|n| evicting.iter().map(move |st| Due { node_id: n.id.clone(), since: n.fencing.since, storage: st.to_string() }))
        .collect()
}

/// The worker to run it: one heard and at rest, with Docker, not the fenced one; first one that says the storage
/// answers it (it reaches the monitors), else any, by id so each round picks the same.
pub fn executor(s: &ClusterState, d: &Due, healthy: impl Fn(&str) -> bool, answers: impl Fn(&str) -> bool) -> Option<String> {
    let fit: Vec<&str> = s.nodes.values().filter(|n| n.id != d.node_id && n.fencing.state == FenceState::None && n.report.as_ref().is_some_and(|r| r.docker) && healthy(&n.id)).map(|n| n.id.as_str()).collect();
    fit.iter().find(|n| answers(n)).or(fit.first()).map(|n| n.to_string())
}

/// The command that evicts it, to run on `on`.
pub fn command(s: &ClusterState, d: &Due, on: &str, now: i64) -> Option<CommandRecord> {
    let st = s.storage.get(&d.storage)?;
    let node = s.nodes.get(&d.node_id)?;
    let op = Op::CephCommand { storage: st.name.clone(), args: evict_args(st.ceph_fs()?, &node.hostname, st.ceph_client()?), timeout_s: 30 };
    Some(CommandRecord {
        id: uuid::Uuid::new_v4().to_string(),
        node_id: on.to_string(),
        op,
        config_version: s.applied,
        created_at: now,
        deadline: now + 60_000,
        status: CommandStatus::Pending,
        message: String::new(),
        result_json: String::new(),
        updated_at: now,
    })
}

/// Where one owed eviction is.
#[derive(Debug, Clone)]
pub struct Tracked {
    pub command_id: String,
    pub at: i64,
    pub done: bool,
}

/// What to do about one owed eviction, given the command made for it last (if any) and how that went.
#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    /// Make a command for it (none yet, or the last failed, or took too long).
    Issue,
    /// It worked: say so once.
    Done,
    Wait,
}

pub fn step(t: Option<&Tracked>, status: Option<CommandStatus>, now: i64) -> Step {
    match (t, status) {
        (None, _) => Step::Issue,
        (Some(t), _) if t.done => Step::Wait,
        (Some(_), Some(CommandStatus::Succeeded)) => Step::Done,
        (Some(_), Some(CommandStatus::Failed | CommandStatus::Rejected | CommandStatus::Expired) | None) => Step::Issue,
        (Some(t), _) if now - t.at >= RETRY_MS => Step::Issue,
        _ => Step::Wait,
    }
}

impl Manager {
    /// The leader's evictions (see the module), each round of `fence_round`.
    pub async fn evict_round(&self) {
        let owed = self.store.read(due);
        let mut tracked = std::mem::take(&mut *self.evictions.lock().expect("evictions lock"));
        // Only what is still owed is kept: a node no longer fenced, or fenced again later, starts afresh.
        tracked.retain(|k, _| owed.contains(k));
        for d in owed {
            let status = tracked.get(&d).and_then(|t| self.store.read(|s| s.commands.get(&t.command_id).map(|c| c.status)));
            match step(tracked.get(&d), status, now_ms()) {
                Step::Wait => {}
                Step::Done => {
                    let name = self.store.read(|s| s.nodes.get(&d.node_id).map(|n| n.display_name())).unwrap_or_else(|| d.node_id.clone());
                    let what = format!("{name}'s CephFS clients evicted from {} (Ceph blocklists them)", d.storage);
                    tracing::warn!(node = %d.node_id, "fencing: {what}");
                    self.events.push("fencing", Some(&d.node_id), what);
                    if let Some(t) = tracked.get_mut(&d) {
                        t.done = true;
                    }
                }
                Step::Issue => {
                    let made = self.store.read(|s| {
                        let on = executor(s, &d, |n| self.sessions.liveness(n).health == crate::sessions::Health::Healthy, |n| self.sessions.storage(n).is_some_and(|r| r.storages.iter().any(|h| h.name == d.storage && h.ok)))?;
                        command(s, &d, &on, now_ms())
                    });
                    let Some(c) = made else {
                        tracing::warn!(node = %d.node_id, storage = %d.storage, "fencing: no worker can evict its CephFS clients now");
                        continue;
                    };
                    let id = c.id.clone();
                    match self.store.propose(crate::state::Entry::CommandCreated { command: c }).await {
                        Ok(_) => {
                            tracked.insert(d, Tracked { command_id: id, at: now_ms(), done: false });
                        }
                        Err(e) => tracing::debug!("eviction not asked: {e}"),
                    }
                }
            }
        }
        *self.evictions.lock().expect("evictions lock") = tracked;
    }
}

/// How long each step's command may run on its worker.
fn timeout_of(action: &str) -> u32 {
    match action {
        "bootstrap" => 1200,
        "prepare" | "keys" => 600,
        _ => 300,
    }
}

impl Manager {
    /// The leader's round of Ceph made from the UI (see the module): the current step's command made, or its end
    /// taken in.
    pub async fn setup_round(&self) {
        if self.store.leader() != Some(self.store.id) {
            return;
        }
        let Some((i, step, setup)) = self.store.read(|s| s.ceph_setup.as_ref().and_then(|c| c.current().map(|(i, st)| (i, st.clone(), c.clone())))) else { return };
        let entry = |state, command_id, message: String, learned| crate::state::Entry::CephSetupStep { index: i, state, command_id, message, learned, at: now_ms() };
        let e = match (&step.command_id, step.state) {
            (None, _) | (_, StepState::Pending) => {
                let Some(args) = setup.resolve(&step) else {
                    let _ = self.store.propose(entry(StepState::Failed, None, "what an earlier step was to learn is not known".into(), None)).await;
                    return;
                };
                let now = now_ms();
                let c = CommandRecord {
                    id: uuid::Uuid::new_v4().to_string(),
                    node_id: step.node_id.clone(),
                    op: Op::Cephadm { action: step.action.clone(), args, image: setup.image.clone(), timeout_s: timeout_of(&step.action) },
                    config_version: self.store.read(|s| s.applied),
                    created_at: now,
                    deadline: now + 300_000,
                    status: CommandStatus::Pending,
                    message: String::new(),
                    result_json: String::new(),
                    updated_at: now,
                };
                let id = c.id.clone();
                if let Err(e) = self.store.propose(crate::state::Entry::CommandCreated { command: c }).await {
                    tracing::warn!("ceph setup: step {i} ({}) not asked: {e}", step.kind);
                    return;
                }
                entry(StepState::Running, Some(id), String::new(), None)
            }
            (Some(id), _) => match self.store.read(|s| s.commands.get(id).map(|c| (c.status, c.message.clone(), c.result_json.clone()))) {
                None => entry(StepState::Failed, None, "its command is gone".into(), None),
                Some((CommandStatus::Succeeded, msg, result)) => entry(StepState::Done, None, msg, learned(&step.action, &result)),
                Some((CommandStatus::Failed | CommandStatus::Rejected | CommandStatus::Expired, msg, _)) => entry(StepState::Failed, None, if msg.is_empty() { "it failed".into() } else { msg }, None),
                Some(_) => return,
            },
        };
        let what = match &e {
            crate::state::Entry::CephSetupStep { state: StepState::Done, .. } if i + 1 == setup.steps.len() => Some(format!("Ceph is set up: storage {} on {} node(s)", setup.storage, setup.hosts.len())),
            crate::state::Entry::CephSetupStep { state: StepState::Failed, message, .. } => Some(format!("Ceph setup: {} failed: {message}", step.kind)),
            _ => None,
        };
        match self.store.propose(e).await {
            Ok(_) => {
                if let Some(w) = what {
                    tracing::warn!("{w}");
                    self.events.push("ceph", Some(&step.node_id), w);
                }
            }
            Err(e) => tracing::debug!("ceph setup step not taken: {e}"),
        }
    }

    /// A `keys` step's answer (Ceph made from the UI): the keys go into the setup's cephfs storage and the answer
    /// kept says only which. Any other command's answer is given back as it is.
    pub async fn take_ceph_keys(&self, command_id: &str, status: CommandStatus, result_json: String) -> anyhow::Result<String> {
        let is_keys = self.store.read(|s| s.commands.get(command_id).is_some_and(|c| matches!(&c.op, Op::Cephadm { action, .. } if action == "keys")));
        if !is_keys || status != CommandStatus::Succeeded {
            return Ok(result_json);
        }
        let v: serde_json::Value = serde_json::from_str(&result_json).map_err(|_| anyhow::anyhow!("the keys' answer could not be read"))?;
        let key = |k: &str| v[k].as_str().filter(|x| !x.is_empty()).map(|x| crate::settings::Secret(x.to_string())).ok_or_else(|| anyhow::anyhow!("the answer has no {k}"));
        let (mount, fence) = (key("mount_key")?, key("fence_key")?);
        let setup = self.store.read(|s| s.ceph_setup.clone()).ok_or_else(|| anyhow::anyhow!("no Ceph setup to take the keys"))?;
        let ceph = crate::state::CephKeys { secret: mount, fence_client: FENCE_CLIENT.into(), fence_secret: Some(fence), image: setup.image.clone() };
        let exists = self.store.read(|s| s.storage.contains_key(&setup.storage));
        let e = if exists {
            crate::state::Entry::StorageKeysSet { name: setup.storage.clone(), ceph, at: now_ms() }
        } else {
            let storage = crate::state::Storage {
                name: setup.storage.clone(),
                driver: StorageDriver::Cephfs,
                source: format!("{}:/", setup.mons()),
                options: format!("name={MOUNT_CLIENT},fs={}", setup.fs),
                added_at: now_ms(),
                ceph: Some(ceph),
                password: None,
            };
            crate::state::Entry::StorageAdded { storage }
        };
        self.store.propose(e).await.map_err(|e| anyhow::anyhow!("keeping the storage {}: {e}", setup.storage))?;
        self.events.push("storage", None, format!("storage {} added: the CephFS {} Ceph was set up with", setup.storage, setup.fs));
        Ok(serde_json::json!({ "storage": setup.storage, "fsid": v["fsid"] }).to_string())
    }
}

/// The evictions in hand, by what is owed.
pub type Evictions = BTreeMap<Due, Tracked>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::Secret;

    fn hosts(n: usize) -> Vec<CephHost> {
        (1..=n).map(|i| CephHost { node_id: format!("n{i}"), disks: vec!["/dev/vdb".into()], hostname: String::new(), address: String::new() }).collect()
    }

    fn kinds(s: &CephSetup) -> Vec<String> {
        s.steps.iter().map(|x| if x.disk.is_empty() { format!("{}@{}", x.kind, x.node_id) } else { format!("{}:{}@{}", x.kind, x.disk, x.node_id) }).collect()
    }

    #[test]
    fn one_setup_at_a_time_of_known_nodes_for_a_storage_name_not_taken() {
        use crate::state::Rejected;
        let mut s = with_nodes(&["n1", "n2"]);
        let setup = CephSetup::new("ceph", "fs1", "", hosts(2), "admin", 1);
        assert_eq!(next(&mut s, &Entry::CephSetupStarted { setup: CephSetup::new("ceph", "fs1", "", hosts(3), "admin", 1) }), Err(Rejected::BadCephSetup), "n3 is no node");
        next(&mut s, &Entry::CephSetupStarted { setup: setup.clone() }).unwrap();
        assert_eq!(next(&mut s, &Entry::CephSetupStarted { setup: setup.clone() }), Err(Rejected::CephSetupBusy));
        assert_eq!(next(&mut s, &Entry::CephSetupRetried { at: 2 }), Err(Rejected::CephSetupUnknown), "it has not failed");
        next(&mut s, &Entry::CephSetupStep { index: 0, state: StepState::Failed, command_id: Some("c".into()), message: "no python3".into(), learned: None, at: 2 }).unwrap();
        assert_eq!(s.ceph_setup.as_ref().unwrap().state, SetupState::Failed);
        assert_eq!(next(&mut s, &Entry::CephSetupStep { index: 0, state: StepState::Done, command_id: None, message: String::new(), learned: None, at: 3 }), Err(Rejected::CephSetupUnknown), "a failed setup takes no step");
        next(&mut s, &Entry::CephSetupRetried { at: 3 }).unwrap();
        next(&mut s, &Entry::CephSetupCleared { at: 4 }).unwrap();
        assert!(s.ceph_setup.is_none());
        next(&mut s, &Entry::StorageAdded { storage: cephfs(true) }).unwrap();
        assert_eq!(next(&mut s, &Entry::CephSetupStarted { setup }), Err(Rejected::BadCephSetup), "the storage's name is taken");
    }

    #[test]
    fn three_hosts_are_prepared_bootstrapped_joined_and_each_disk_made_an_osd() {
        let mut h = hosts(3);
        h[1].disks.push("/dev/vdc".into());
        let s = CephSetup::new("ceph", "fs1", "quay.io/ceph/ceph:v20", h, "admin", 1);
        assert!(s.valid());
        assert_eq!(kinds(&s), [
            "prepare@n1", "prepare@n2", "prepare@n3", "bootstrap@n1", "trust@n2", "trust@n3", "add_host@n1", "add_host@n1", "monitors@n1", "managers@n1", "memory@n1",
            "osd:/dev/vdb@n1", "osd:/dev/vdb@n1", "osd:/dev/vdc@n1", "osd:/dev/vdb@n1", "filesystem@n1", "keys@n1",
        ]);
        assert_eq!(s.steps[8].args, ["orch", "apply", "mon", "--placement={hostname:n1},{hostname:n2},{hostname:n3}"]);
        assert_eq!(s.steps[13].about, "n2", "an OSD is about its node, run where the admin key is");
        assert_eq!(s.steps[3].args, ["{address:n1}", ""]);
    }

    #[test]
    fn one_host_is_bootstrapped_for_a_single_host_and_two_keep_two_copies() {
        let one = CephSetup::new("ceph", "fs1", "", hosts(1), "admin", 1);
        assert_eq!(kinds(&one), ["prepare@n1", "bootstrap@n1", "memory@n1", "osd:/dev/vdb@n1", "filesystem@n1", "keys@n1"]);
        assert_eq!(one.steps[1].args[1], "single-host");
        let two = CephSetup::new("ceph", "fs1", "", hosts(2), "admin", 1);
        assert!(kinds(&two).contains(&"pool_size@n1".to_string()) && kinds(&two).contains(&"pool_min_size@n1".to_string()));
        assert_eq!((monitors(2), monitors(4), monitors(7)), (1, 3, 5));
    }

    #[test]
    fn a_setup_is_checked_before_it_starts() {
        let ok = CephSetup::new("ceph", "fs1", "", hosts(2), "admin", 1);
        let mut dup = hosts(2);
        dup[1].node_id = "n1".into();
        let mut no_disk = hosts(1);
        no_disk[0].disks.clear();
        let mut bad_disk = hosts(1);
        bad_disk[0].disks = vec!["/dev/../etc/passwd".into()];
        for bad in [
            CephSetup { storage: "Bad Name".into(), ..ok.clone() },
            CephSetup { fs: "fs 1".into(), ..ok.clone() },
            CephSetup { hosts: vec![], ..ok.clone() },
            CephSetup { hosts: dup, ..ok.clone() },
            CephSetup { hosts: no_disk, ..ok.clone() },
            CephSetup { hosts: bad_disk, ..ok.clone() },
        ] {
            assert!(!bad.valid(), "{bad:?}");
        }
    }

    #[test]
    fn a_step_runs_with_what_the_earlier_ones_learned_and_a_failure_stops_it_until_retried() {
        let mut s = CephSetup::new("ceph", "fs1", "", hosts(2), "admin", 1);
        assert_eq!(s.current().map(|c| c.0), Some(0));
        let bootstrap = s.steps[2].clone();
        assert_eq!(s.resolve(&bootstrap), None, "the address is not known yet");
        s.record(0, StepState::Done, Some("c0".into()), "", learned("prepare", r#"{"address":"192.0.2.7","hostname":"node-a"}"#).as_ref(), 2);
        s.record(1, StepState::Done, Some("c1".into()), "", learned("prepare", r#"{"address":"192.0.2.8","hostname":"node-b"}"#).as_ref(), 3);
        assert_eq!(s.resolve(&bootstrap).unwrap(), ["192.0.2.7", ""]);
        assert_eq!(s.resolve(&s.steps[3].clone()), None, "trust needs cephadm's key");
        s.record(2, StepState::Done, None, "", learned("bootstrap", r#"{"fsid":"f-1","public_key":"ssh-rsa AAAA ceph"}"#).as_ref(), 4);
        assert_eq!(s.resolve(&s.steps[3].clone()).unwrap(), ["ssh-rsa AAAA ceph"]);
        assert_eq!(s.resolve(&s.steps[4].clone()).unwrap(), ["orch", "host", "add", "node-b", "192.0.2.8"]);
        assert_eq!((s.fsid.as_str(), s.mons()), ("f-1", "192.0.2.7".to_string()));
        s.record(3, StepState::Failed, Some("c3".into()), "no route to host", None, 5);
        assert_eq!((s.state, s.message.as_str(), s.current()), (SetupState::Failed, "no route to host", None));
        s.retry(6);
        assert_eq!((s.state, s.current().map(|c| c.0), s.steps[3].command_id.clone()), (SetupState::Running, Some(3), None));
        for i in 3..s.steps.len() {
            s.record(i, StepState::Done, None, "", None, 7);
        }
        assert_eq!(s.state, SetupState::Done);
        assert_eq!(s.current(), None);
    }
    use crate::state::tests::{next, with_nodes};
    use crate::state::{CephKeys, Entry, Storage};

    fn cephfs(fence: bool) -> Storage {
        let keys = CephKeys { secret: Secret("AQBtb3VudA==".into()), fence_client: if fence { "fd-fence".into() } else { String::new() }, fence_secret: fence.then(|| Secret("AgBmZW5jZQ==".into())), image: String::new() };
        Storage { name: if fence { "ceph".into() } else { "quiet".into() }, driver: StorageDriver::Cephfs, source: "192.0.2.1:/".into(), options: "name=fd,fs=fs1".into(), added_at: 1, ceph: Some(keys), password: None }
    }

    fn fenced(s: &mut ClusterState, node: &str, state: FenceState) {
        let n = s.nodes.get_mut(node).unwrap();
        n.fencing.state = state;
        n.fencing.since = 7;
    }

    #[test]
    fn a_fenced_node_s_mounts_are_evicted_from_each_cephfs_that_has_a_fencing_client() {
        let mut s = with_nodes(&["n1", "n2", "n3"]);
        next(&mut s, &Entry::StorageAdded { storage: cephfs(true) }).unwrap();
        next(&mut s, &Entry::StorageAdded { storage: cephfs(false) }).unwrap();
        assert!(due(&s).is_empty(), "nothing fenced");
        for st in [FenceState::Requested, FenceState::InProgress, FenceState::Failed, FenceState::AwaitingAuthorization] {
            fenced(&mut s, "n1", st);
            assert!(due(&s).is_empty(), "{st:?}: not before it is fenced, and not once it is back");
        }
        fenced(&mut s, "n1", FenceState::Fenced);
        let d = due(&s);
        assert_eq!(d, vec![Due { node_id: "n1".into(), since: 7, storage: "ceph".into() }]);
        let c = command(&s, &d[0], "n2", 100).unwrap();
        assert_eq!(c.node_id, "n2");
        let host = s.nodes["n1"].hostname.clone();
        assert_eq!(c.op, Op::CephCommand { storage: "ceph".into(), args: evict_args("fs1", &host, "fd"), timeout_s: 30 });
        assert_eq!(evict_args("fs1", "node-a", "fd"), ["tell", "mds.fs1:0", "client", "evict", "client_metadata.hostname=node-a", "client_metadata.entity_id=fd"]);
        assert!(!serde_json::to_string(&c).unwrap().contains("AgBm"), "the key is filled in as it is sent");
    }

    #[test]
    fn a_healthy_worker_at_rest_runs_it_one_that_reaches_the_storage_first() {
        let mut s = with_nodes(&["n1", "n2", "n3"]);
        for n in s.nodes.values_mut() {
            n.report = Some(crate::state::NodeReport { docker: true, ..Default::default() });
        }
        let d = Due { node_id: "n1".into(), since: 7, storage: "ceph".into() };
        assert_eq!(executor(&s, &d, |_| true, |_| false).as_deref(), Some("n2"), "never the fenced node itself");
        assert_eq!(executor(&s, &d, |_| true, |n| n == "n3").as_deref(), Some("n3"));
        assert_eq!(executor(&s, &d, |n| n == "n3", |_| false).as_deref(), Some("n3"), "only one heard");
        fenced(&mut s, "n3", FenceState::AwaitingAuthorization);
        assert_eq!(executor(&s, &d, |n| n == "n3", |_| true), None, "not one being fenced itself");
    }

    #[test]
    fn an_eviction_is_asked_again_until_it_worked_and_said_once() {
        let t = Tracked { command_id: "c".into(), at: 0, done: false };
        assert_eq!(step(None, None, 0), Step::Issue);
        assert_eq!(step(Some(&t), Some(CommandStatus::Pending), RETRY_MS - 1), Step::Wait);
        assert_eq!(step(Some(&t), Some(CommandStatus::Accepted), RETRY_MS), Step::Issue, "it took too long");
        assert_eq!(step(Some(&t), Some(CommandStatus::Failed), 1), Step::Issue);
        assert_eq!(step(Some(&t), None, 1), Step::Issue, "its record pruned");
        assert_eq!(step(Some(&t), Some(CommandStatus::Succeeded), 1), Step::Done);
        assert_eq!(step(Some(&Tracked { done: true, ..t }), Some(CommandStatus::Succeeded), 1), Step::Wait);
    }
}
