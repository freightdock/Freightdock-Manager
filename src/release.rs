//! Upgrades from the managers (the UI's Upgrades page): the cluster's release, and the leader rolling it out a node at
//! a time.
//!
//! The release is in the replicated state: its version and each binary's SHA-256 and size. The binaries themselves
//! are files in each manager's data folder (`releases/<sha256>`), never in the log: given to one manager (uploaded,
//! or fetched from a published release and checked against its SHA256SUMS), the others copy them from it
//! (ManagerPeer.Fetch, checked again) within seconds.
//!
//! An upgrade is a list of steps made when it starts: every worker not on the release's version, by name, then every
//! manager, the leader last. The leader takes one step at a time (`decide`, each second): a worker is sent the
//! Upgrade command and fetches its binary from its own manager; a manager is asked through ManagerPeer.Upgrade (the
//! leader stages its own). A step is done when the node is back on the version and healthy (a manager: heard by the
//! leader and the cluster quorate); the first step that fails ends the upgrade, and the nodes after it are left as they
//! are. A node that cannot take it (an older worker, another architecture, unreachable) is skipped and said why. The
//! upgrade's progress is in the state, so a new leader (the old one upgrading itself) carries on with it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::state::{CommandStatus, Ms};

/// How long a node has to come back on the new version.
pub const STEP_LIMIT_MS: Ms = 180_000;

/// One binary of a release, by its checksum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binary {
    pub sha256: String,
    pub size: u64,
}

impl Binary {
    pub fn valid(&self) -> bool {
        self.size > 0 && self.sha256.len() == 64 && self.sha256.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Part {
    Worker,
    Manager,
}

impl Part {
    pub fn word(self) -> &'static str {
        match self {
            Part::Worker => "worker",
            Part::Manager => "manager",
        }
    }
}

/// The cluster's release: what an upgrade rolls out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Release {
    pub version: String,
    /// The architecture its binaries are for (Rust's name: x86_64, aarch64).
    pub arch: String,
    #[serde(default)]
    pub worker: Option<Binary>,
    #[serde(default)]
    pub manager: Option<Binary>,
    /// Where it came from: "upload" or the release's address.
    #[serde(default)]
    pub source: String,
    pub at: Ms,
}

impl Release {
    pub fn valid(&self) -> bool {
        crate::admin::version_ok(&self.version)
            && !self.arch.is_empty()
            && self.arch.len() <= 32
            && self.worker.as_ref().is_none_or(Binary::valid)
            && self.manager.as_ref().is_none_or(Binary::valid)
            && (self.worker.is_some() || self.manager.is_some())
    }

    pub fn binary(&self, part: Part) -> Option<&Binary> {
        match part {
            Part::Worker => self.worker.as_ref(),
            Part::Manager => self.manager.as_ref(),
        }
    }

    /// Whether `sha256` is one of its binaries (what Fetch hands out).
    pub fn has(&self, sha256: &str) -> bool {
        [&self.worker, &self.manager].into_iter().flatten().any(|b| b.sha256 == sha256)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Running,
    Done,
    Failed,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepState {
    Waiting,
    Upgrading,
    Done,
    Skipped,
    Failed,
}

impl StepState {
    pub fn finished(self) -> bool {
        matches!(self, StepState::Done | StepState::Skipped | StepState::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    pub part: Part,
    /// A node's id, or a manager's Raft id.
    pub target: String,
    /// What a person calls it: a node's name, a manager's address.
    pub name: String,
    pub state: StepState,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub command_id: Option<String>,
    #[serde(default)]
    pub started_at: Option<Ms>,
    #[serde(default)]
    pub at: Ms,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Upgrade {
    pub version: String,
    pub by: String,
    pub started_at: Ms,
    pub state: RunState,
    pub steps: Vec<Step>,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub at: Ms,
}

/// A worker, as an upgrade sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerFacts {
    pub name: String,
    pub version: String,
    pub healthy: bool,
    /// Its worker takes the Upgrade command.
    pub upgrade: bool,
    pub arch: String,
}

/// A manager, as an upgrade sees it: its address, its version (when heard), whether it leads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagerFacts {
    pub addr: String,
    pub version: Option<String>,
    pub leader: bool,
}

/// What the leader knows each second.
#[derive(Debug, Clone, Default)]
pub struct Facts {
    pub workers: BTreeMap<String, WorkerFacts>,
    pub managers: BTreeMap<String, ManagerFacts>,
    pub commands: BTreeMap<String, (CommandStatus, String)>,
    pub quorate: bool,
}

/// The steps of an upgrade to `release`: workers not on its version by name, then managers, the leader last.
pub fn steps(release: &Release, f: &Facts, now: Ms) -> Vec<Step> {
    let step = |part, target: &str, name: &str| Step { part, target: target.to_string(), name: name.to_string(), state: StepState::Waiting, message: String::new(), command_id: None, started_at: None, at: now };
    let mut out = vec![];
    if release.worker.is_some() {
        let mut ws: Vec<(&String, &WorkerFacts)> = f.workers.iter().filter(|(_, w)| w.version != release.version).collect();
        ws.sort_by(|a, b| (&a.1.name, a.0).cmp(&(&b.1.name, b.0)));
        out.extend(ws.into_iter().map(|(id, w)| step(Part::Worker, id, &w.name)));
    }
    if release.manager.is_some() {
        let mut ms: Vec<(&String, &ManagerFacts)> = f.managers.iter().filter(|(_, m)| m.version.as_deref() != Some(release.version.as_str())).collect();
        ms.sort_by(|a, b| (a.1.leader, &a.1.addr).cmp(&(b.1.leader, &b.1.addr)));
        out.extend(ms.into_iter().map(|(id, m)| step(Part::Manager, id, &m.addr)));
    }
    out
}

/// What the leader does next with a running upgrade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Nothing,
    /// Start step `index`: send the command, or ask the manager.
    Start(usize),
    Mark(usize, StepState, String),
    End(RunState, String),
}

pub fn decide(run: &Upgrade, release: &Release, f: &Facts, now: Ms) -> Decision {
    if run.state != RunState::Running {
        return Decision::Nothing;
    }
    if let Some(s) = run.steps.iter().find(|s| s.state == StepState::Failed) {
        return Decision::End(RunState::Failed, format!("{} {} failed: {}", s.part.word(), s.name, s.message));
    }
    let Some((i, s)) = run.steps.iter().enumerate().find(|(_, s)| !s.state.finished()) else {
        let done = run.steps.iter().filter(|s| s.state == StepState::Done).count();
        let skipped: Vec<&str> = run.steps.iter().filter(|s| s.state == StepState::Skipped).map(|s| s.name.as_str()).collect();
        let tail = if skipped.is_empty() { String::new() } else { format!(", {} skipped ({})", skipped.len(), skipped.join(", ")) };
        return Decision::End(RunState::Done, format!("{done} upgraded to {}{tail}", run.version));
    };
    let on = |v: Option<&str>| v == Some(run.version.as_str());
    match (s.state, s.part) {
        (StepState::Waiting, _) if !f.quorate => Decision::Nothing,
        (StepState::Waiting, Part::Worker) => match f.workers.get(&s.target) {
            None => Decision::Mark(i, StepState::Skipped, "the node is gone".into()),
            Some(w) if on(Some(&w.version)) => Decision::Mark(i, StepState::Done, format!("already on {}", run.version)),
            Some(w) if !w.upgrade => Decision::Mark(i, StepState::Skipped, format!("its worker ({}) is older than upgrades: upgrade it by hand (install.sh upgrade)", w.version)),
            Some(w) if !w.arch.is_empty() && w.arch != release.arch => Decision::Mark(i, StepState::Skipped, format!("it is {}, the release is {}", w.arch, release.arch)),
            Some(w) if !w.healthy => Decision::Mark(i, StepState::Skipped, "not healthy now: upgrade it when it is".into()),
            Some(_) => Decision::Start(i),
        },
        (StepState::Waiting, Part::Manager) => match f.managers.get(&s.target) {
            None => Decision::Mark(i, StepState::Skipped, "the manager is gone".into()),
            Some(m) if on(m.version.as_deref()) => Decision::Mark(i, StepState::Done, format!("already on {}", run.version)),
            Some(m) if m.version.is_none() => Decision::Mark(i, StepState::Skipped, "not heard now: upgrade it when it is".into()),
            Some(_) => Decision::Start(i),
        },
        (_, part) => {
            if let Some((CommandStatus::Failed | CommandStatus::Rejected | CommandStatus::Expired, msg)) = s.command_id.as_ref().and_then(|c| f.commands.get(c)) {
                return Decision::Mark(i, StepState::Failed, if msg.is_empty() { "its command failed".into() } else { msg.clone() });
            }
            let (back, says) = match part {
                Part::Worker => f.workers.get(&s.target).map_or((false, None), |w| (on(Some(&w.version)) && w.healthy, Some(w.version.clone()))),
                Part::Manager => f.managers.get(&s.target).map_or((false, None), |m| (on(m.version.as_deref()) && f.quorate, m.version.clone())),
            };
            if back {
                return Decision::Mark(i, StepState::Done, format!("on {}", run.version));
            }
            if now - s.started_at.unwrap_or(s.at) > STEP_LIMIT_MS {
                let says = says.map_or("nothing heard".to_string(), |v| format!("it runs {v}"));
                return Decision::Mark(i, StepState::Failed, format!("not back on {} within {} minutes ({says})", run.version, STEP_LIMIT_MS / 60_000));
            }
            Decision::Nothing
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// The binaries on disk, and the leader's round.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use freightdock_proto::sha256_hex;
use freightdock_proto::v1;
use tonic::Status;

use crate::state::{CommandRecord, Entry, Op};
use crate::{Manager, now_ms};

/// The largest binary taken (an upload, a fetch): a static build is about 10 MB.
pub const MAX_BINARY: u64 = 256 * 1024 * 1024;
const CHUNK: usize = 64 * 1024;

/// A binary's file in a manager's data folder.
pub fn blob_path(data: &Path, sha256: &str) -> PathBuf {
    data.join("releases").join(sha256)
}

/// Keeps `bytes` under their checksum (written whole, then renamed into place); gives the checksum back.
pub fn put_blob(data: &Path, bytes: &[u8]) -> Result<String> {
    if bytes.is_empty() || bytes.len() as u64 > MAX_BINARY {
        bail!("a binary is 1 byte to {} MB", MAX_BINARY / 1024 / 1024);
    }
    let sum = sha256_hex(bytes);
    let path = blob_path(data, &sum);
    std::fs::create_dir_all(path.parent().expect("has a parent")).context("making the releases folder")?;
    write_whole(&path, bytes, 0o600)?;
    Ok(sum)
}

/// The binary of `b`, if this manager has it whole (checked again as it is read).
pub fn get_blob(data: &Path, b: &Binary) -> Option<Vec<u8>> {
    let bytes = std::fs::read(blob_path(data, &b.sha256)).ok()?;
    (bytes.len() as u64 == b.size && sha256_hex(&bytes) == b.sha256).then_some(bytes)
}

/// Writes `bytes` beside `path`, syncs, and renames over it: the file at `path` is the old one or the new one, whole.
pub fn write_whole(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let dir = path.parent().context("no folder")?;
    let tmp = dir.join(format!(".{}.new", path.file_name().and_then(|n| n.to_str()).unwrap_or("binary")));
    let _ = std::fs::remove_file(&tmp);
    let mut f = std::fs::OpenOptions::new().write(true).create_new(true).mode(mode).open(&tmp).with_context(|| format!("writing {}", tmp.display()))?;
    f.write_all(bytes)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })?;
    Ok(())
}

/// The running binary's path (Linux says a replaced one is "(deleted)").
pub fn running_exe() -> Result<PathBuf> {
    let p = std::env::current_exe().context("finding the manager's own binary")?;
    let s = p.to_string_lossy();
    Ok(PathBuf::from(s.strip_suffix(" (deleted)").unwrap_or(&s).to_string()))
}

/// Where a manager that cannot replace its own binary (its unit's ProtectSystem) leaves the new one, for
/// `freightdock-manager upgrade-apply` (the unit's ExecStartPre, run as root) to put in place at its next start.
pub fn staged(data: &Path) -> (PathBuf, PathBuf) {
    (data.join("upgrade").join("freightdock-manager"), data.join("upgrade").join("freightdock-manager.sha256"))
}

/// Puts a staged binary in place of `exe`, if one is staged and whole; says what it did. A staged binary that does not
/// match its checksum is thrown away and the manager starts on the one it has.
pub fn apply_staged(data: &Path, exe: &Path) -> Result<Option<String>> {
    let (bin, sum) = staged(data);
    let Ok(want) = std::fs::read_to_string(&sum) else { return Ok(None) };
    let bytes = std::fs::read(&bin).context("reading the staged binary")?;
    let clear = || {
        let _ = std::fs::remove_file(&bin);
        let _ = std::fs::remove_file(&sum);
    };
    if sha256_hex(&bytes) != want.trim() {
        clear();
        bail!("the staged binary does not match its checksum: thrown away");
    }
    write_whole(exe, &bytes, 0o755)?;
    clear();
    Ok(Some(want.trim().to_string()))
}

/// The architecture as releases name it (install.sh's names).
pub fn arch_name(a: &str) -> String {
    match a {
        "x86_64" | "amd64" => "amd64".into(),
        "aarch64" | "arm64" => "arm64".into(),
        other => other.to_string(),
    }
}

impl Manager {
    /// A binary of the release, streamed (WorkerSession.Fetch and ManagerPeer.Fetch).
    pub fn serve_binary(&self, sha256: &str) -> Result<tokio_stream::wrappers::ReceiverStream<Result<v1::FetchChunk, Status>>, Status> {
        let b = self.store.read(|s| s.release.as_ref().and_then(|r| [&r.worker, &r.manager].into_iter().flatten().find(|b| b.sha256 == sha256).cloned()));
        let b = b.ok_or_else(|| Status::not_found("no binary of the cluster's release has that checksum"))?;
        let bytes = get_blob(&self.data_dir, &b).ok_or_else(|| Status::unavailable("this manager does not have that binary yet"))?;
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            for c in bytes.chunks(CHUNK) {
                if tx.send(Ok(v1::FetchChunk { data: c.to_vec() })).await.is_err() {
                    return;
                }
            }
        });
        Ok(tokio_stream::wrappers::ReceiverStream::new(rx))
    }

    /// The binary `b`, from this manager's folder or, missing, from another manager (checked, then kept).
    pub async fn ensure_binary(&self, b: &Binary) -> Result<Vec<u8>> {
        if let Some(bytes) = get_blob(&self.data_dir, b) {
            return Ok(bytes);
        }
        let others: Vec<String> = self.store.members().into_iter().filter(|(id, _)| *id != self.store.id).map(|(_, a)| a).collect();
        let mut why = String::from("no other manager");
        for addr in others {
            let Ok(ch) = self.peer(&addr) else { continue };
            match fetch_from(v1::manager_peer_client::ManagerPeerClient::new(ch), b).await {
                Ok(bytes) => {
                    put_blob(&self.data_dir, &bytes)?;
                    tracing::info!(sha256 = %b.sha256, "a binary of the release copied from the manager at {addr}");
                    return Ok(bytes);
                }
                Err(e) => why = format!("{addr}: {e:#}"),
            }
        }
        bail!("the binary {} is not here and could not be copied ({why})", &b.sha256[..12])
    }

    /// Copies the release's binaries this manager is missing from the others (every few seconds).
    pub async fn sync_release(&self) {
        let Some(r) = self.store.read(|s| s.release.clone()) else { return };
        for b in [&r.worker, &r.manager].into_iter().flatten() {
            if get_blob(&self.data_dir, b).is_none()
                && let Err(e) = self.ensure_binary(b).await
            {
                tracing::debug!("{e:#}");
            }
        }
    }

    /// Upgrades this manager to the release's binary: put in place (or staged, when its unit keeps it from writing
    /// there), then the process ends and systemd starts it again on the new one.
    pub async fn upgrade_self(&self, version: &str, sha256: &str, size: u64) -> Result<()> {
        let b = self.store.read(|s| s.release.as_ref().filter(|r| r.version == version).and_then(|r| r.manager.clone()));
        let b = b.filter(|b| b.sha256 == sha256 && b.size == size).context("that is not the manager binary of the cluster's release")?;
        if version == env!("CARGO_PKG_VERSION") {
            return Ok(());
        }
        // It ends to come back on the new binary: only where something starts it again.
        if std::env::var_os("INVOCATION_ID").is_none() {
            bail!("this manager does not run under systemd, which would start it again: upgrade it by hand");
        }
        let bytes = self.ensure_binary(&b).await?;
        let exe = running_exe()?;
        match write_whole(&exe, &bytes, 0o755) {
            Ok(()) => tracing::warn!("upgrading to {version}: {} replaced; restarting on it", exe.display()),
            Err(e) => {
                let (bin, sum) = staged(&self.data_dir);
                std::fs::create_dir_all(bin.parent().expect("has a parent"))?;
                write_whole(&bin, &bytes, 0o700)?;
                write_whole(&sum, b.sha256.as_bytes(), 0o600)?;
                tracing::warn!("upgrading to {version}: staged for the unit's ExecStartPre ({} not written: {e:#}); restarting", exe.display());
            }
        }
        self.events.push("upgrade", None, format!("this manager restarts on {version}"));
        tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
            std::process::exit(0);
        });
        Ok(())
    }

    /// What an upgrade knows now: the workers, the managers with their versions, the commands of its steps.
    pub fn upgrade_facts(&self) -> Facts {
        let quorate = self.store.in_quorum();
        let leader = self.store.leader();
        let versions = self.peer_versions.lock().expect("peer versions lock").clone();
        let managers = self
            .store
            .members()
            .into_iter()
            .map(|(id, addr)| {
                let version = if id == self.store.id {
                    Some(env!("CARGO_PKG_VERSION").to_string())
                } else {
                    versions.get(&addr).filter(|(_, at)| at.elapsed() < std::time::Duration::from_secs(5)).map(|(v, _)| v.clone())
                };
                (id.to_string(), ManagerFacts { addr, version, leader: leader == Some(id) })
            })
            .collect();
        self.store.read(|s| {
            let workers = s
                .nodes
                .values()
                .filter_map(|n| {
                    let r = n.report.as_ref()?;
                    let healthy = self.sessions.liveness(&n.id).health == crate::sessions::Health::Healthy && n.fencing.state == crate::state::FenceState::None;
                    Some((n.id.clone(), WorkerFacts { name: n.labels.get("name").cloned().unwrap_or_else(|| r.hostname.clone()), version: r.agent_version.clone(), healthy, upgrade: r.upgrade, arch: arch_name(&r.arch) }))
                })
                .collect();
            let commands = s.upgrade.iter().flat_map(|u| u.steps.iter().filter_map(|st| st.command_id.clone())).filter_map(|id| s.commands.get(&id).map(|c| (id, (c.status, c.message.clone())))).collect();
            Facts { workers, managers, commands, quorate }
        })
    }

    /// The leader's round of a running upgrade (see the module), each second.
    pub async fn upgrade_round(self: &std::sync::Arc<Self>) {
        if self.store.leader() != Some(self.store.id) || self.started.elapsed() < crate::fencing::STARTUP_GRACE {
            return;
        }
        let Some((run, release)) = self.store.read(|s| s.upgrade.clone().filter(|u| u.state == RunState::Running).zip(s.release.clone())) else { return };
        let now = now_ms();
        let step = |index: usize, state: StepState, message: String, command_id: Option<String>, started_at: Option<Ms>| Entry::UpgradeStep { index, state, message, command_id, started_at, at: now };
        match decide(&run, &release, &self.upgrade_facts(), now) {
            Decision::Nothing => {}
            Decision::Mark(i, state, message) => {
                let st = &run.steps[i];
                let said = format!("{} {}: {}", st.part.word(), st.name, message);
                if self.store.propose(step(i, state, message, None, None)).await.is_ok() && state != StepState::Waiting {
                    self.events.push("upgrade", (st.part == Part::Worker).then_some(st.target.as_str()), said);
                }
            }
            Decision::End(state, message) => {
                if self.store.propose(Entry::UpgradeEnded { state, message: message.clone(), at: now }).await.is_ok() {
                    tracing::warn!("upgrade to {}: {message}", run.version);
                    self.events.push("upgrade", None, format!("the upgrade to {} ended: {message}", run.version));
                    let me = self.advertise.first().cloned().unwrap_or_else(|| self.manager_id.clone());
                    let kind = if state == RunState::Done { "upgrade.done" } else { "upgrade.failed" };
                    self.tell(crate::notify::Event { kind, title: format!("The upgrade to {} ended", run.version), message, node: None, target: me });
                }
            }
            Decision::Start(i) => {
                let st = run.steps[i].clone();
                let Some(b) = release.binary(st.part).cloned() else { return };
                match st.part {
                    Part::Worker => {
                        let c = CommandRecord {
                            id: uuid::Uuid::new_v4().to_string(),
                            node_id: st.target.clone(),
                            op: Op::Upgrade { version: run.version.clone(), sha256: b.sha256.clone(), size: b.size },
                            config_version: self.store.read(|s| s.applied),
                            created_at: now,
                            deadline: now + 120_000,
                            status: CommandStatus::Pending,
                            message: String::new(),
                            result_json: String::new(),
                            updated_at: now,
                        };
                        let id = c.id.clone();
                        if self.store.propose(Entry::CommandCreated { command: c }).await.is_ok() {
                            let _ = self.store.propose(step(i, StepState::Upgrading, "sent the upgrade".into(), Some(id), Some(now))).await;
                            self.events.push("upgrade", Some(&st.target), format!("worker {}: upgrading to {}", st.name, run.version));
                        }
                    }
                    Part::Manager if st.target == self.store.id.to_string() => {
                        // This one, the leader, last: the step is in the state before it goes, so the next leader
                        // sees it upgrading and waits for it to come back.
                        if self.store.propose(step(i, StepState::Upgrading, "restarting on it".into(), None, Some(now))).await.is_ok()
                            && let Err(e) = self.upgrade_self(&run.version, &b.sha256, b.size).await
                        {
                            let _ = self.store.propose(step(i, StepState::Failed, format!("{e:#}"), None, None)).await;
                        }
                    }
                    Part::Manager => {
                        let asked = match self.peer(&st.name) {
                            Ok(ch) => tokio::time::timeout(
                                std::time::Duration::from_secs(90),
                                v1::manager_peer_client::ManagerPeerClient::new(ch).upgrade(v1::PeerUpgradeRequest { version: run.version.clone(), sha256: b.sha256.clone(), size: b.size }),
                            )
                            .await
                            .map_err(|_| "it did not answer within 90 seconds".to_string())
                            .and_then(|r| r.map(|_| ()).map_err(|s| s.message().to_string())),
                            Err(e) => Err(format!("{e:#}")),
                        };
                        let e = match asked {
                            Ok(()) => step(i, StepState::Upgrading, "restarting on it".into(), None, Some(now)),
                            Err(why) => step(i, StepState::Failed, why, None, None),
                        };
                        let _ = self.store.propose(e).await;
                        self.events.push("upgrade", None, format!("manager {}: upgrading to {}", st.name, run.version));
                    }
                }
            }
        }
    }
}

/// A binary from another manager, no larger than it should be, checked.
async fn fetch_from(mut c: v1::manager_peer_client::ManagerPeerClient<tonic::transport::Channel>, b: &Binary) -> Result<Vec<u8>> {
    let mut s = c.fetch(v1::FetchRequest { sha256: b.sha256.clone() }).await.map_err(|s| anyhow::anyhow!("{}", s.message()))?.into_inner();
    let mut bytes = Vec::with_capacity(b.size as usize);
    while let Some(chunk) = s.message().await.map_err(|s| anyhow::anyhow!("{}", s.message()))? {
        bytes.extend_from_slice(&chunk.data);
        if bytes.len() as u64 > b.size {
            bail!("more than its size");
        }
    }
    if bytes.len() as u64 != b.size || sha256_hex(&bytes) != b.sha256 {
        bail!("it does not match its checksum");
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bin(c: char) -> Binary {
        Binary { sha256: c.to_string().repeat(64), size: 10 }
    }

    fn release() -> Release {
        Release { version: "1.1.0".into(), arch: "x86_64".into(), worker: Some(bin('a')), manager: Some(bin('b')), source: "upload".into(), at: 0 }
    }

    fn w(name: &str, version: &str) -> WorkerFacts {
        WorkerFacts { name: name.into(), version: version.into(), healthy: true, upgrade: true, arch: "x86_64".into() }
    }

    fn facts() -> Facts {
        let mut f = Facts { quorate: true, ..Default::default() };
        f.workers.insert("n-b".into(), w("beta", "1.0.0"));
        f.workers.insert("n-a".into(), w("alpha", "1.0.0"));
        f.workers.insert("n-c".into(), w("gamma", "1.1.0"));
        f.managers.insert("1".into(), ManagerFacts { addr: "m1:7450".into(), version: Some("1.0.0".into()), leader: true });
        f.managers.insert("2".into(), ManagerFacts { addr: "m2:7450".into(), version: Some("1.0.0".into()), leader: false });
        f.managers.insert("3".into(), ManagerFacts { addr: "m3:7450".into(), version: Some("1.1.0".into()), leader: false });
        f
    }

    fn run(f: &Facts) -> Upgrade {
        Upgrade { version: "1.1.0".into(), by: "admin".into(), started_at: 0, state: RunState::Running, steps: steps(&release(), f, 0), message: String::new(), at: 0 }
    }

    #[test]
    fn a_release_names_valid_binaries_and_a_version() {
        assert!(release().valid());
        assert!(!Release { version: "dev".into(), ..release() }.valid());
        assert!(!Release { worker: None, manager: None, ..release() }.valid(), "at least one binary");
        assert!(!Release { worker: Some(Binary { sha256: "A".repeat(64), size: 1 }), ..release() }.valid(), "lowercase hex");
        assert!(!Release { worker: Some(Binary { sha256: "a".repeat(64), size: 0 }), ..release() }.valid(), "never empty");
        assert!(release().has(&"a".repeat(64)) && !release().has(&"c".repeat(64)));
    }

    #[test]
    fn workers_go_first_by_name_then_managers_the_leader_last_and_what_is_on_it_already_is_left_out() {
        let s = steps(&release(), &facts(), 5);
        let order: Vec<(Part, &str)> = s.iter().map(|s| (s.part, s.name.as_str())).collect();
        assert_eq!(order, [(Part::Worker, "alpha"), (Part::Worker, "beta"), (Part::Manager, "m2:7450"), (Part::Manager, "m1:7450")]);
        assert!(s.iter().all(|s| s.state == StepState::Waiting && s.at == 5));
        let only_workers = Release { manager: None, ..release() };
        assert!(steps(&only_workers, &facts(), 0).iter().all(|s| s.part == Part::Worker), "no manager binary, no manager step");
    }

    #[test]
    fn a_step_starts_when_the_cluster_is_quorate_and_its_node_can_take_it() {
        let f = facts();
        let r = run(&f);
        assert_eq!(decide(&r, &release(), &f, 1), Decision::Start(0));
        assert_eq!(decide(&r, &release(), &Facts { quorate: false, ..f.clone() }, 1), Decision::Nothing, "not without a quorum");
        let mut older = f.clone();
        older.workers.get_mut("n-a").unwrap().upgrade = false;
        assert!(matches!(decide(&r, &release(), &older, 1), Decision::Mark(0, StepState::Skipped, m) if m.contains("by hand")));
        let mut arm = f.clone();
        arm.workers.get_mut("n-a").unwrap().arch = "aarch64".into();
        assert!(matches!(decide(&r, &release(), &arm, 1), Decision::Mark(0, StepState::Skipped, m) if m.contains("aarch64")));
        let mut down = f.clone();
        down.workers.get_mut("n-a").unwrap().healthy = false;
        assert!(matches!(decide(&r, &release(), &down, 1), Decision::Mark(0, StepState::Skipped, m) if m.contains("not healthy")));
        let mut gone = f.clone();
        gone.workers.remove("n-a");
        assert!(matches!(decide(&r, &release(), &gone, 1), Decision::Mark(0, StepState::Skipped, _)));
        let mut there = f.clone();
        there.workers.get_mut("n-a").unwrap().version = "1.1.0".into();
        assert!(matches!(decide(&r, &release(), &there, 1), Decision::Mark(0, StepState::Done, _)), "upgraded by hand meanwhile");
    }

    #[test]
    fn a_started_step_is_done_when_its_node_is_back_on_the_version_and_healthy_and_fails_otherwise() {
        let mut f = facts();
        let mut r = run(&f);
        r.steps[0].state = StepState::Upgrading;
        r.steps[0].command_id = Some("c1".into());
        r.steps[0].started_at = Some(100);
        f.commands.insert("c1".into(), (CommandStatus::Succeeded, "staged".into()));
        assert_eq!(decide(&r, &release(), &f, 200), Decision::Nothing, "still on the old version");
        f.workers.get_mut("n-a").unwrap().version = "1.1.0".into();
        f.workers.get_mut("n-a").unwrap().healthy = false;
        assert_eq!(decide(&r, &release(), &f, 200), Decision::Nothing, "back, not yet healthy");
        f.workers.get_mut("n-a").unwrap().healthy = true;
        assert!(matches!(decide(&r, &release(), &f, 200), Decision::Mark(0, StepState::Done, _)));

        let mut f = facts();
        f.commands.insert("c1".into(), (CommandStatus::Failed, "does not match its checksum".into()));
        assert_eq!(decide(&r, &release(), &f, 200), Decision::Mark(0, StepState::Failed, "does not match its checksum".into()));
        f.commands.insert("c1".into(), (CommandStatus::Succeeded, String::new()));
        assert!(matches!(decide(&r, &release(), &f, 100 + STEP_LIMIT_MS + 1), Decision::Mark(0, StepState::Failed, m) if m.contains("1.0.0")), "too long: says what it runs");
    }

    #[test]
    fn a_manager_is_done_when_heard_on_the_version_and_the_first_failure_ends_the_upgrade() {
        let mut f = facts();
        let mut r = run(&f);
        for s in &mut r.steps[..2] {
            s.state = StepState::Done;
        }
        r.steps[2].state = StepState::Upgrading;
        r.steps[2].started_at = Some(0);
        assert_eq!(decide(&r, &release(), &f, 10), Decision::Nothing);
        f.managers.get_mut("2").unwrap().version = Some("1.1.0".into());
        assert!(matches!(decide(&r, &release(), &f, 10), Decision::Mark(2, StepState::Done, _)));
        r.steps[2].state = StepState::Failed;
        r.steps[2].message = "it did not answer".into();
        assert!(matches!(decide(&r, &release(), &f, 10), Decision::End(RunState::Failed, m) if m.contains("m2:7450") && m.contains("did not answer")));
    }

    #[test]
    fn with_every_step_finished_the_upgrade_ends_done_and_says_what_was_skipped() {
        let f = facts();
        let mut r = run(&f);
        for s in &mut r.steps {
            s.state = StepState::Done;
        }
        r.steps[1].state = StepState::Skipped;
        assert!(matches!(decide(&r, &release(), &f, 10), Decision::End(RunState::Done, m) if m.contains("3 upgraded") && m.contains("1 skipped")));
        r.state = RunState::Stopped;
        assert_eq!(decide(&r, &release(), &f, 10), Decision::Nothing, "a stopped upgrade is left alone");
    }

    #[test]
    fn a_binary_is_kept_under_its_checksum_and_given_back_only_whole() {
        let d = tempfile::tempdir().unwrap();
        let sum = put_blob(d.path(), b"binary").unwrap();
        let b = Binary { sha256: sum.clone(), size: 6 };
        assert_eq!(get_blob(d.path(), &b).as_deref(), Some(&b"binary"[..]));
        std::fs::write(blob_path(d.path(), &sum), b"binarY").unwrap();
        assert!(get_blob(d.path(), &b).is_none(), "a changed file is not handed out");
        assert!(put_blob(d.path(), b"").is_err());
    }

    #[test]
    fn a_staged_binary_is_put_in_place_only_when_it_matches_its_checksum() {
        let d = tempfile::tempdir().unwrap();
        let exe = d.path().join("freightdock-manager");
        std::fs::write(&exe, b"old").unwrap();
        assert_eq!(apply_staged(d.path(), &exe).unwrap(), None, "nothing staged, nothing done");
        let (bin, sum) = staged(d.path());
        std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
        std::fs::write(&bin, b"new").unwrap();
        std::fs::write(&sum, sha256_hex(b"other")).unwrap();
        assert!(apply_staged(d.path(), &exe).is_err());
        assert_eq!(std::fs::read(&exe).unwrap(), b"old", "a wrong one never goes in");
        assert!(!bin.exists() && !sum.exists(), "and is thrown away");
        std::fs::write(&bin, b"new").unwrap();
        std::fs::write(&sum, sha256_hex(b"new")).unwrap();
        assert!(apply_staged(d.path(), &exe).unwrap().is_some());
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::read(&exe).unwrap(), b"new");
        assert_eq!(std::fs::metadata(&exe).unwrap().permissions().mode() & 0o777, 0o755);
        assert!(!bin.exists());
    }

    #[test]
    fn while_an_upgrade_runs_the_release_stays_and_another_waits() {
        use crate::state::{Entry, Rejected};
        let mut s = crate::state::tests::cluster();
        crate::state::tests::next(&mut s, &Entry::ReleaseSet { release: release() }).unwrap();
        let r = run(&facts());
        assert_eq!(crate::state::tests::next(&mut s, &Entry::UpgradeStarted { run: Upgrade { version: "9.9.9".into(), ..r.clone() } }), Err(Rejected::BadRelease), "to the release's version only");
        crate::state::tests::next(&mut s, &Entry::UpgradeStarted { run: r.clone() }).unwrap();
        assert_eq!(crate::state::tests::next(&mut s, &Entry::UpgradeStarted { run: r.clone() }), Err(Rejected::UpgradeBusy));
        assert_eq!(crate::state::tests::next(&mut s, &Entry::ReleaseSet { release: release() }), Err(Rejected::UpgradeBusy));
        crate::state::tests::next(&mut s, &Entry::UpgradeStep { index: 0, state: StepState::Upgrading, message: "sent".into(), command_id: Some("c1".into()), started_at: Some(7), at: 7 }).unwrap();
        crate::state::tests::next(&mut s, &Entry::UpgradeStep { index: 0, state: StepState::Done, message: "on it".into(), command_id: None, started_at: None, at: 9 }).unwrap();
        let st = &s.upgrade.as_ref().unwrap().steps[0];
        assert_eq!((st.state, st.command_id.as_deref(), st.started_at), (StepState::Done, Some("c1"), Some(7)), "what it started with is kept");
        crate::state::tests::next(&mut s, &Entry::UpgradeEnded { state: RunState::Stopped, message: "stopped".into(), at: 10 }).unwrap();
        assert_eq!(crate::state::tests::next(&mut s, &Entry::UpgradeStep { index: 1, state: StepState::Done, message: String::new(), command_id: None, started_at: None, at: 11 }), Err(Rejected::UpgradeUnknown), "nothing goes on after it ends");
        crate::state::tests::next(&mut s, &Entry::ReleaseSet { release: release() }).unwrap();
    }
}
