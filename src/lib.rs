//! freightdock-manager: the control plane. See README.md for the shape of the whole and ROADMAP.md for what comes next.

pub mod admin;
pub mod backup;
pub mod ca;
pub mod ceph;
pub mod compose;
pub mod docker;
pub mod events;
pub mod fencing;
pub mod grpc;
pub mod peer;
pub mod prepull;
pub mod proxmox;
pub mod redfish;
pub mod raft;
pub mod recovery;
pub mod release;
pub mod registry;
pub mod sessions;
pub mod settings;
pub mod shell;
pub mod stacks;
pub mod state;
pub mod store;
pub mod tls;
pub mod metrics;
pub mod network;
pub mod notify;
pub mod traffic;
pub mod ui;
pub mod usage;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use freightdock_proto::v1::{enrollment_server::EnrollmentServer, manager_peer_server::ManagerPeerServer, worker_session_server::WorkerSessionServer};

use crate::ca::Ca;
use crate::sessions::{Sessions, Thresholds};
use crate::state::{CommandRecord, Entry};
use crate::store::Store;

pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: PathBuf,
    /// Where workers dial: the worker API (gRPC, mTLS).
    pub bind: SocketAddr,
    /// The admin HTTP API; `None` turns it off.
    pub api_bind: Option<SocketAddr>,
    /// The same API over TLS for the network (G8.2), for API tokens: the UI on another node, a remote CLI.
    pub api_tls_bind: Option<SocketAddr>,
    /// The manager endpoints handed to workers (host:port), this one's first.
    pub advertise: Vec<String>,
    pub thresholds: Thresholds,
    /// Start a cluster of its own when this manager has never been in one. Off for a manager that is to join one.
    pub bootstrap: bool,
    /// Join the cluster of the manager at this address (host:port) with a manager join token, on the first start.
    pub join: Option<Join>,
    /// Where to leave a worker join token for the worker on this same node (`FREIGHTDOCK_JOIN_TOKEN_FILE` there), so a
    /// single node runs the whole cluster without a token copied by hand. Made again whenever the file is missing.
    pub local_worker_token: Option<PathBuf>,
    pub fencing: fencing::FenceConfig,
}

#[derive(Debug, Clone)]
pub struct Join {
    pub addr: String,
    pub token: String,
}

fn env_ms(name: &str, default: Duration) -> Result<Duration> {
    match std::env::var(name) {
        Ok(v) => Ok(Duration::from_millis(v.trim().parse().with_context(|| format!("{name} is not a number of milliseconds"))?)),
        Err(_) => Ok(default),
    }
}

impl Config {
    /// Where the cluster CA is kept: `ca.pem`, `ca.key`.
    /// What only the environment sets, for the API to show: the addresses, the data directory, how it joined.
    pub fn local(&self) -> serde_json::Value {
        let opt = |a: Option<SocketAddr>| a.map_or("off".to_string(), |a| a.to_string());
        serde_json::json!({
            "FREIGHTDOCK_MANAGER_BIND": self.bind.to_string(),
            "FREIGHTDOCK_MANAGER_API_BIND": opt(self.api_bind),
            "FREIGHTDOCK_MANAGER_API_TLS_BIND": opt(self.api_tls_bind),
            "FREIGHTDOCK_MANAGER_ADVERTISE": self.advertise.join(","),
            "FREIGHTDOCK_MANAGER_DATA": self.data_dir.display().to_string(),
            "FREIGHTDOCK_MANAGER_JOIN": self.join.as_ref().map_or(String::new(), |j| j.addr.clone()),
            "FREIGHTDOCK_MANAGER_LOCAL_WORKER_TOKEN": self.local_worker_token.as_ref().map_or(String::new(), |p| p.display().to_string()),
        })
    }

    pub fn ca_dir(&self) -> PathBuf {
        self.data_dir.join("ca")
    }

    pub fn from_env() -> Result<Self> {
        let get = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        let bind: SocketAddr = get("FREIGHTDOCK_MANAGER_BIND", "0.0.0.0:7450").parse().context("FREIGHTDOCK_MANAGER_BIND")?;
        let api = get("FREIGHTDOCK_MANAGER_API_BIND", "127.0.0.1:7451");
        let api_bind = if api == "off" { None } else { Some(api.parse().context("FREIGHTDOCK_MANAGER_API_BIND")?) };
        let api_tls = get("FREIGHTDOCK_MANAGER_API_TLS_BIND", "0.0.0.0:7452");
        let api_tls_bind = if api_tls == "off" { None } else { Some(api_tls.parse().context("FREIGHTDOCK_MANAGER_API_TLS_BIND")?) };
        let advertise = match std::env::var("FREIGHTDOCK_MANAGER_ADVERTISE") {
            Ok(v) => v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(freightdock_proto::with_default_port).collect(),
            Err(_) => {
                let host = std::fs::read_to_string("/etc/hostname").map(|h| h.trim().to_string()).unwrap_or_else(|_| "localhost".into());
                vec![format!("{host}:{}", bind.port())]
            }
        };
        let d = Thresholds::default();
        let thresholds = Thresholds {
            heartbeat: env_ms("FREIGHTDOCK_HEARTBEAT_MS", d.heartbeat)?,
            suspect: env_ms("FREIGHTDOCK_SUSPECT_MS", d.suspect)?,
            unreachable: env_ms("FREIGHTDOCK_UNREACHABLE_MS", d.unreachable)?,
        };
        anyhow::ensure!(thresholds.heartbeat < thresholds.suspect && thresholds.suspect < thresholds.unreachable, "the heartbeat must be shorter than the suspect threshold, and that shorter than the unreachable one");
        let join = match (std::env::var("FREIGHTDOCK_MANAGER_JOIN"), std::env::var("FREIGHTDOCK_MANAGER_JOIN_TOKEN")) {
            (Ok(addr), Ok(token)) => Some(Join { addr: freightdock_proto::with_default_port(addr.trim()), token: token.trim().to_string() }),
            (Err(_), Err(_)) => None,
            _ => anyhow::bail!("FREIGHTDOCK_MANAGER_JOIN and FREIGHTDOCK_MANAGER_JOIN_TOKEN go together"),
        };
        Ok(Config { data_dir: get("FREIGHTDOCK_MANAGER_DATA", "/var/lib/freightdock-manager").into(), bind, api_bind, api_tls_bind, advertise, thresholds, bootstrap: join.is_none(), join, local_worker_token: std::env::var("FREIGHTDOCK_MANAGER_LOCAL_WORKER_TOKEN").ok().filter(|p| !p.trim().is_empty()).map(PathBuf::from), fencing: fencing::FenceConfig::from_env()? })
    }
}

pub struct Manager {
    pub store: Store,
    pub events: events::Events,
    pub sessions: Sessions,
    pub ca: Arc<Ca>,
    pub manager_id: String,
    /// This manager's own addresses (`Config::advertise`).
    pub advertise: Vec<String>,
    pub admin_token: String,
    /// TLS to the other managers, with this manager's client certificate.
    peer_tls: Arc<rustls::ClientConfig>,
    /// A channel per other manager, made once (it reconnects by itself).
    peer_channels: std::sync::Mutex<std::collections::HashMap<String, tonic::transport::Channel>>,
    local_worker_token: Option<PathBuf>,
    /// The reconcile's pace per workload.
    paced: stacks::Paced,
    /// This manager's own settings (its environment over the defaults); the cluster's settings win over them.
    pub own: settings::Settings,
    /// What this manager was started with that only its environment sets: shown, not changed, by the API.
    pub local: serde_json::Value,
    /// The heartbeat and fencing timers last sent to the workers here, to send them again when they change.
    timers_sent: std::sync::Mutex<Option<(u32, u32, u32, u32)>>,
    /// When this manager started: the fencing waits a while after it, until it has heard from the workers.
    pub started: std::time::Instant,
    /// Since when this manager has been cut off from a majority of managers, if it is.
    cut_off_since: std::sync::Mutex<Option<std::time::Instant>>,
    /// Nodes a fencing provider is working on here.
    fence_tasks: std::sync::Mutex<std::collections::HashSet<String>>,
    /// The leader's CephFS evictions in hand (G12.6, `ceph::evict_round`).
    evictions: std::sync::Mutex<ceph::Evictions>,
    /// This manager's data folder: its scheduled backups are in `backups/` there.
    pub data_dir: PathBuf,
    /// Docker requests sent to the workers here, waiting for their replies (`docker.rs`).
    docker_waits: docker::Waits,
    shell_waits: shell::Waits,
    /// Each other manager's version as it last answered Heard, and when (`release`).
    pub peer_versions: std::sync::Mutex<std::collections::HashMap<String, (String, std::time::Instant)>>,
    /// The leader's watch over the cluster and the quiet window of what was told (`notify.rs`).
    pub notifier: notify::Notifier,
    /// A pre-pull round is running (`prepull.rs`).
    prepulling: std::sync::atomic::AtomicBool,
    /// Pulls ahead that failed and were said, by (node, image), so the next failure is not said again.
    prepull_failed: std::sync::Mutex<std::collections::HashSet<(String, String)>>,
}

/// How long a manager may be cut off from the majority before it sends its workers away.
pub const CUT_OFF_GRACE: Duration = Duration::from_secs(5);

/// A file's contents, made with `make` first if it is not there (0600).
fn kept(path: &std::path::Path, make: impl FnOnce() -> String) -> Result<String> {
    if !path.exists() {
        ca::write_private(path, make().as_bytes())?;
    }
    Ok(std::fs::read_to_string(path)?.trim().to_string())
}

impl Manager {
    /// The settings in use: the cluster's where set, else this manager's own.
    pub fn settings(&self) -> settings::Settings {
        self.store.read(|s| s.settings.clone()).over(&self.own)
    }

    /// What a backup holds now: the replicated state and the cluster CA.
    pub fn bundle(&self) -> backup::Bundle {
        let state = self.store.read(|s| s.clone());
        backup::Bundle { format: backup::Bundle::FORMAT.into(), taken_at: now_ms(), cluster_id: state.cluster_id.clone(), state, ca_pem: self.ca.cert_pem.clone(), ca_key: self.ca.key_pem() }
    }

    /// A backup sealed with `passphrase`, to hand out (PBKDF2 is slow on purpose, so off the async threads).
    pub async fn backup(&self, passphrase: String) -> Result<Vec<u8>> {
        let b = self.bundle();
        tokio::task::spawn_blocking(move || backup::seal(&b, &passphrase)).await?
    }

    /// Writes this manager's scheduled backup if one is due (a passphrase set, the interval passed since the newest).
    /// Answers the new file's name.
    pub async fn scheduled_backup(&self) -> Result<Option<String>> {
        let s = self.settings();
        let Some(passphrase) = s.backup_passphrase.clone() else { return Ok(None) };
        let dir = backup::dir(&self.data_dir);
        let newest = backup::list(&dir).first().and_then(|(n, _)| backup::taken_at(n));
        if !backup::due(newest, now_ms(), s.backup_every_h.unwrap_or(settings::BACKUP_EVERY_H)) {
            return Ok(None);
        }
        let b = self.bundle();
        let keep = s.backup_keep.unwrap_or(settings::BACKUP_KEEP) as usize;
        let name = tokio::task::spawn_blocking(move || backup::write(&dir, &b, &passphrase.0, keep)).await??;
        tracing::info!(file = %name, "wrote a scheduled backup");
        Ok(Some(name))
    }

    pub fn fence(&self) -> fencing::FenceConfig {
        self.settings().fencing()
    }

    /// The Welcome a worker gets when it connects, and again when the settings it carries change.
    pub fn welcome(&self, renew_certificate: bool) -> freightdock_proto::v1::ManagerMessage {
        let (s, f) = (self.settings(), self.fence());
        freightdock_proto::v1::ManagerMessage {
            body: Some(freightdock_proto::v1::manager_message::Body::Welcome(freightdock_proto::v1::Welcome {
                cluster_id: self.store.read(|s| s.cluster_id.clone()).unwrap_or_default(),
                manager_id: self.manager_id.clone(),
                heartbeat_interval_ms: s.thresholds().heartbeat.as_millis() as u32,
                managers: self.managers(),
                fence_stop_s: f.stop_s,
                fence_storage_s: f.storage_s,
                fence_reboot_s: f.reboot_s,
                renew_certificate,
            })),
        }
    }

    /// Each second: health is judged by the settings in use, and the workers here are sent a Welcome again when
    /// the heartbeat or the fencing timers changed, so they take them without reconnecting.
    pub fn apply_settings(&self) {
        let (t, f) = (self.settings().thresholds(), self.fence());
        if self.sessions.thresholds() != t {
            self.sessions.set_thresholds(t);
        }
        let timers = (t.heartbeat.as_millis() as u32, f.stop_s, f.storage_s, f.reboot_s);
        let before = self.timers_sent.lock().expect("timers lock").replace(timers);
        if before.is_some_and(|b| b != timers) {
            let welcome = self.welcome(false);
            for node in self.sessions.local_nodes() {
                if let Some(tx) = self.sessions.sender(&node) {
                    let _ = tx.try_send(Ok(welcome.clone()));
                }
            }
            tracing::info!("settings changed: the workers here were sent the new heartbeat and fencing timers");
        }
    }

    pub async fn open(config: &Config) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&config.data_dir).with_context(|| format!("making {}", config.data_dir.display()))?;
        // The Raft node id: a number for the manager's whole life, never given to another (openraft's rule).
        let raft_id: u64 = kept(&config.data_dir.join("raft.id"), || (rand::random::<u64>() >> 1 | 1).to_string())?.parse().context("raft.id")?;
        let addr = config.advertise.first().cloned().unwrap_or_else(|| config.bind.to_string());
        let ca_dir = config.ca_dir();
        if let Some(j) = &config.join
            && !ca_dir.join("ca.pem").exists()
        {
            join_cluster(j, raft_id, &addr, &ca_dir).await?;
        }
        let ca = Arc::new(Ca::load_or_create(&ca_dir)?);
        let c = ca.clone();
        let peer_tls = tls::peer_client_config(&ca, tls::Rotating::new(move || c.manager_client_identity(raft_id), tls::RENEW_AFTER)?)?;
        let store = Store::open(&config.data_dir, raft_id, &addr, config.bootstrap && config.join.is_none(), raft::network::Network::new(raft_id, peer_tls.clone()), Some(peer_tls.clone())).await?;
        store.migrate_pre_raft(&config.data_dir).await?;
        let restore = if config.bootstrap && store.read(|s| s.cluster_id.is_none()) { backup::staged(&config.data_dir)? } else { None };
        if let Some(state) = restore {
            let cluster_id = state.cluster_id.clone().unwrap_or_default();
            store.propose(Entry::StateRestored { state: Box::new(state), at: now_ms() }).await.map_err(|e| anyhow::anyhow!("{e}"))?;
            backup::done(&config.data_dir);
            tracing::info!(%cluster_id, "brought the cluster back from a backup");
        } else if config.bootstrap && store.read(|s| s.cluster_id.is_none()) {
            let cluster_id = uuid::Uuid::new_v4().to_string();
            store.propose(Entry::ClusterCreated { cluster_id: cluster_id.clone(), at: now_ms() }).await.map_err(|e| anyhow::anyhow!("{e}"))?;
            tracing::info!(%cluster_id, "made a new cluster");
        }
        let manager_id = kept(&config.data_dir.join("manager.id"), || uuid::Uuid::new_v4().to_string())?;
        let admin_token = kept(&config.data_dir.join("admin.token"), || hex::encode(rand::random::<[u8; 32]>()))?;
        let commands = store.commands();
        let m = Arc::new(Manager { store, events: Default::default(), sessions: Sessions::new(config.thresholds), ca, manager_id, advertise: config.advertise.clone(), admin_token, peer_tls, peer_channels: Default::default(), local_worker_token: config.local_worker_token.clone(), paced: Default::default(), own: settings::Settings::of(&config.thresholds, &config.fencing), local: config.local(), timers_sent: Default::default(), started: std::time::Instant::now(), cut_off_since: Default::default(), fence_tasks: Default::default(), evictions: Default::default(), data_dir: config.data_dir.clone(), docker_waits: Default::default(), shell_waits: Default::default(), peer_versions: Default::default(), notifier: Default::default(), prepulling: Default::default(), prepull_failed: Default::default() });
        tokio::spawn(deliver_applied(Arc::downgrade(&m), commands));
        Ok(m)
    }

    /// Where workers reach the managers: every manager in the replicated membership, this one first. Workers learn of
    /// a new manager from it at their next hello.
    pub fn managers(&self) -> Vec<String> {
        let mut v: Vec<String> = self.store.members().into_iter().map(|(_, a)| a).collect();
        for a in self.advertise.iter().rev() {
            if !v.contains(a) {
                v.insert(0, a.clone());
            }
        }
        v
    }

    /// Until this manager is a voter: asks the cluster (through `via`, any manager) to make it one. A joined manager
    /// runs it once its server is up, since the leader first sends it the log.
    pub async fn become_voter(self: Arc<Self>, via: String) {
        use freightdock_proto::v1::{AddVoterRequest, manager_peer_client::ManagerPeerClient};
        let me = self.store.id;
        let addr = self.advertise.first().cloned().unwrap_or_default();
        let mut wait = Duration::from_secs(1);
        while !self.store.voters().contains(&me) {
            // Through ourselves once we know a leader (it forwards), else through the manager we joined.
            let r = match self.store.leader() {
                Some(_) => self.store.add_voter_anywhere(me, &addr).await.map_err(|e| e.to_string()),
                None => match self.peer(&via) {
                    Ok(ch) => ManagerPeerClient::new(ch).add_voter(AddVoterRequest { raft_id: me, addr: addr.clone() }).await.map(|_| ()).map_err(|s| s.message().to_string()),
                    Err(e) => Err(format!("{e:#}")),
                },
            };
            match r {
                Ok(()) => tracing::info!("this manager is a voter now"),
                Err(e) => {
                    tracing::warn!("could not become a voter yet: {e}; again in {}s", wait.as_secs());
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(Duration::from_secs(30));
                }
            }
        }
    }

    /// Writes a worker join token for the worker on this node when the file for it is missing (`Config::local_worker_token`).
    /// A week to be spent; a spent or expired one is harmless to leave, since the worker keeps its certificate.
    pub async fn local_worker_token(&self) {
        let Some(path) = &self.local_worker_token else { return };
        if path.exists() {
            return;
        }
        let token = freightdock_proto::token::JoinToken { ca_sha256: self.ca.fingerprint(), secret: hex::encode(rand::random::<[u8; 32]>()) };
        let now = now_ms();
        if let Err(e) = self.store.propose(Entry::JoinTokenIssued { secret_hash: token.secret_hash(), expires_at: now + 7 * 86_400_000, at: now, for_manager: false }).await {
            tracing::debug!("no token for the local worker yet: {e}");
            return;
        }
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        match ca::write_private(path, format!("{token}\n").as_bytes()) {
            Ok(()) => tracing::info!("left a join token for the worker on this node in {}", path.display()),
            Err(e) => tracing::warn!("could not write the local worker's token: {e:#}"),
        }
    }

    /// The channel to the manager at `addr`.
    fn peer(&self, addr: &str) -> Result<tonic::transport::Channel> {
        let mut g = self.peer_channels.lock().expect("peer channels lock");
        if let Some(ch) = g.get(addr) {
            return Ok(ch.clone());
        }
        let ch = tls::peer_channel(addr, self.peer_tls.clone())?;
        g.insert(addr.to_string(), ch.clone());
        Ok(ch)
    }

    /// Asks every other manager which workers it hears, so liveness is the cluster's and not only this manager's.
    /// One that does not answer within a second simply has no fresh report; its old one ages out.
    pub async fn gather_heard(&self) {
        use freightdock_proto::v1::{HeardRequest, manager_peer_client::ManagerPeerClient};
        let others: Vec<String> = self.store.members().into_iter().filter(|(id, _)| *id != self.store.id).map(|(_, a)| a).collect();
        self.sessions.forget_reports(&others);
        let asks = others.into_iter().filter_map(|addr| {
            let ch = self.peer(&addr).ok()?;
            Some(async move {
                let r = tokio::time::timeout(Duration::from_secs(1), ManagerPeerClient::new(ch).heard(HeardRequest {})).await;
                (addr, r)
            })
        });
        for (addr, r) in futures_util::future::join_all(asks).await {
            match r {
                Ok(Ok(reply)) => {
                    let reply = reply.into_inner();
                    if !reply.version.is_empty() {
                        self.peer_versions.lock().expect("peer versions lock").insert(addr.clone(), (reply.version, std::time::Instant::now()));
                    }
                    self.sessions.reports(&addr, reply.nodes)
                }
                Ok(Err(s)) => tracing::debug!(manager = %addr, "no liveness report: {}", s.message()),
                Err(_) => tracing::debug!(manager = %addr, "no liveness report within a second"),
            }
        }
    }

    /// Sends each worker connected here its network (G9), when it changed.
    pub fn push_network(&self) {
        for node in self.sessions.local_nodes() {
            let state = self.store.read(|s| {
                network::state_for(s, &node, |other| {
                    // A worker on this manager's host, connected over loopback, is reached at this manager's address:
                    // the endpoint goes to other nodes, for which 127.0.0.1 is themselves.
                    let l = self.sessions.liveness(other);
                    network::endpoint(l.peer.as_deref()?, l.via.as_deref().or(self.advertise.first().map(String::as_str)), network::WG_PORT)
                }, |w| {
                    // Down only when its node's last report has it and not running; no report says nothing.
                    let Some(n) = w.node_id.as_deref() else { return false };
                    self.sessions.reported(n).is_some_and(|r| r.iter().find(|a| a.workload_id == w.id).is_none_or(|a| a.state != "running"))
                })
            });
            self.sessions.send_network(&node, state);
        }
    }

    /// The leader gives each floating address with no holder at rest (none yet, or its holder fenced) to the healthy
    /// worker holding fewest (G10.3, `network::floating_moves`).
    pub async fn float_round(&self) {
        if self.store.leader() != Some(self.store.id) {
            return;
        }
        let moves = self.store.read(|s| network::floating_moves(s, |n| self.sessions.liveness(n).health == sessions::Health::Healthy, now_ms()));
        for e in moves {
            let Entry::FloatingHeld { address, node_id, .. } = &e else { continue };
            let what = format!("floating address {address} to {}", node_id.as_deref().map(|n| &n[..n.len().min(8)]).unwrap_or("nobody"));
            match self.store.propose(e.clone()).await {
                Ok(_) => {
                    tracing::warn!("{what}");
                    self.events.push("floating", node_id.as_deref(), what);
                }
                Err(e) => tracing::debug!("{what} not made: {e}"),
            }
        }
    }

    /// A command as it goes to its worker: the UI's placeholders filled here, never in the state (`ui::fill`).
    pub fn wire(&self, c: &CommandRecord) -> freightdock_proto::v1::ManagerMessage {
        let mut c = c.clone();
        if c.op.workload_id().is_some_and(|w| w.starts_with(&format!("{}.", ui::STACK))) {
            let addresses = self.store.read(|s| s.stacks.get(ui::STACK).map(|st| st.exposures.iter().map(|e| e.address.clone()).collect()).unwrap_or_default());
            ui::fill(&mut c.op, &ui::Fill { token: ui::token(&self.ca.key_pem()), ca_pem: self.ca.cert_pem.clone(), managers: self.managers(), addresses });
        }
        let mut m = grpc::command_to_proto(&c);
        // A private registry's login for the image, a CephFS's keys and an SMB share's password, filled in here only: never kept with the
        // command.
        if let Some(freightdock_proto::v1::manager_message::Body::Command(cmd)) = &mut m.body {
            use freightdock_proto::v1::command::Op as P;
            match &mut cmd.op {
                Some(P::RunContainer(r)) => self.store.read(|s| {
                    r.auth = registry::auth_for(&s.registries, &r.image);
                    for v in &mut r.storage {
                        if let Some(st) = s.storage.get(&v.storage) {
                            if let Some(k) = &st.ceph {
                                v.secret = k.secret.0.clone();
                            } else if let Some(p) = &st.password {
                                v.secret = p.0.clone();
                            }
                        }
                    }
                }),
                Some(P::CephCommand(x)) => self.store.read(|s| {
                    if let state::Op::CephCommand { storage, .. } = &c.op
                        && let Some(st) = s.storage.get(storage)
                        && let Some(k) = &st.ceph
                    {
                        x.image = st.ceph_image().to_string();
                        x.mons = st.ceph_mons().to_string();
                        x.client = k.fence_client.clone();
                        x.secret = k.fence_secret.as_ref().map(|k| k.0.clone()).unwrap_or_default();
                    }
                }),
                _ => {}
            }
        }
        m
    }

    /// Sends a command to its node if the node is connected to this manager. If not, it waits as pending and is sent
    /// when the node connects.
    pub fn deliver(&self, c: &CommandRecord) {
        if let Some(tx) = self.sessions.sender(&c.node_id)
            && tx.try_send(Ok(self.wire(c))).is_err()
        {
            tracing::warn!(node = %c.node_id, command = %c.id, "the stream is full or closed; the command is sent again at the next connect");
        }
    }
}

impl Manager {
    /// Whether this manager has been out of the majority for longer than `CUT_OFF_GRACE`. Such a manager takes no
    /// worker streams and ends those it has: a worker that hears only it is, for the cluster, cut off too, and must
    /// count down to fencing instead of believing it is heard (G6).
    pub fn cut_off(&self) -> bool {
        self.cut_off_since.lock().expect("cut off lock").is_some_and(|at| at.elapsed() >= CUT_OFF_GRACE)
    }

    /// Notes whether this manager is with a majority now, and sends its workers away once it has not been for long.
    pub fn quorum_guard(&self) {
        let ok = self.store.in_quorum();
        {
            let mut g = self.cut_off_since.lock().expect("cut off lock");
            match (ok, g.is_some()) {
                (true, true) => {
                    *g = None;
                    tracing::info!("back with a majority of managers: workers may connect here again");
                }
                (false, false) => *g = Some(std::time::Instant::now()),
                _ => {}
            }
        }
        if self.cut_off() && !self.sessions.connected().is_empty() {
            tracing::warn!("cut off from a majority of managers: sending this manager's workers away");
            self.sessions.kick_all();
        }
    }
}

/// How long past its deadline a pending command is given before the manager calls it expired.
pub const EXPIRY_GRACE_MS: i64 = 5_000;

impl Manager {
    /// Marks pending commands past their deadline as expired. Run every second by `housekeeping`.
    /// The leader puts a workload whose node no longer qualifies (its labels changed, or it is no longer chosen for
    /// its stack) on one that does (`stacks::replacements`); the reconcile moves its container.
    pub async fn replace_round(&self) {
        if self.store.leader() != Some(self.store.id) {
            return;
        }
        let todo = self.store.read(|s| stacks::replacements(s, &self.eligible(s), &self.sick(s)));
        for (name, placements) in todo {
            let what = placements.iter().map(|(w, n)| format!("{w} to {}", &n[..n.len().min(8)])).collect::<Vec<_>>().join(", ");
            match self.store.propose(Entry::StackPlaced { name: name.clone(), placements, at: now_ms() }).await {
                Ok(_) => {
                    tracing::info!(stack = %name, "placed again, its nodes no longer qualifying: {what}");
                    self.events.push("stack_placed", None, format!("stack {name}: {what} (its node no longer qualifies)"));
                }
                Err(e) => tracing::debug!(stack = %name, "not placed again: {e}"),
            }
        }
    }

    /// The leader drops finished commands (`ClusterState::prunable`): kept an hour, and at most [`COMMANDS_KEPT`],
    /// so the state does not grow with every run. In batches, so no one entry is large.
    pub async fn prune_commands(&self) {
        if self.store.leader() != Some(self.store.id) {
            return;
        }
        let ids = self.store.read(|s| s.prunable(now_ms(), COMMANDS_KEEP_MS, COMMANDS_KEPT));
        for chunk in ids.chunks(500) {
            if let Err(e) = self.store.propose(Entry::CommandsPruned { ids: chunk.to_vec(), at: now_ms() }).await {
                return tracing::debug!("finished commands not pruned: {e}");
            }
        }
        if !ids.is_empty() {
            tracing::info!("{} finished commands pruned", ids.len());
        }
    }

    pub async fn expire_overdue(&self) {
        for id in self.store.read(|s| s.overdue(now_ms(), EXPIRY_GRACE_MS)) {
            let e = Entry::CommandStatus { command_id: id.clone(), status: state::CommandStatus::Expired, message: "its deadline passed before its node was reached".into(), result_json: String::new(), at: now_ms() };
            match self.store.propose(e).await {
                Ok(_) => {
                    let node = self.store.read(|s| s.commands.get(&id).map(|c| c.node_id.clone()));
                    self.events.push("command_expired", node.as_deref(), format!("command {id} expired before its node was reached"));
                }
                Err(e) => tracing::warn!(command = %id, "could not expire: {e}"),
            }
        }
    }
}

/// How long a finished command is kept in the state, and how many at most.
pub const COMMANDS_KEEP_MS: state::Ms = 60 * 60 * 1000;
pub const COMMANDS_KEPT: usize = 1000;

/// Delivers each command as this manager applies it, to its node if the node is connected here. Wherever the command
/// was made, the manager its worker is connected to sends it. Ends with the manager.
async fn deliver_applied(m: std::sync::Weak<Manager>, mut commands: tokio::sync::broadcast::Receiver<CommandRecord>) {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match commands.recv().await {
            Ok(c) => match m.upgrade() {
                Some(m) => m.deliver(&c),
                None => return,
            },
            // Missed ones are still pending in the state and sent at the node's next connect.
            Err(RecvError::Lagged(n)) => tracing::warn!("{n} commands applied too fast to deliver at once; they go at the next connect"),
            Err(RecvError::Closed) => return,
        }
    }
}

/// A manager's first start in an existing cluster: the join token is checked by the manager at `j.addr` (which this
/// end checks against the CA the token pins), and the CA comes back, key included. The log follows over Raft.
async fn join_cluster(j: &Join, raft_id: u64, addr: &str, ca_dir: &std::path::Path) -> Result<()> {
    use freightdock_proto::v1::{JoinRequest, manager_peer_client::ManagerPeerClient};
    let token = freightdock_proto::token::JoinToken::parse_manager(&j.token).context("FREIGHTDOCK_MANAGER_JOIN_TOKEN is not a manager join token (FDMT1-...)")?;
    let ch = tls::peer_channel(&j.addr, tls::pinned_client_config(&token.ca_sha256)?)?;
    let r = ManagerPeerClient::new(ch)
        .join(JoinRequest { join_token: j.token.clone(), raft_id, addr: addr.to_string() })
        .await
        .map_err(|s| anyhow::anyhow!("joining through {}: {}", j.addr, s.message()))?
        .into_inner();
    let ca: rustls::pki_types::CertificateDer = rustls::pki_types::pem::PemObject::from_pem_slice(r.ca_pem.as_bytes()).context("the CA handed over")?;
    anyhow::ensure!(freightdock_proto::sha256_hex(&ca) == token.ca_sha256, "the CA handed over is not the one the token pins");
    Ca::install(ca_dir, &r.ca_pem, &r.ca_key_pem)?;
    tracing::info!(cluster_id = %r.cluster_id, managers = ?r.managers, "joined the cluster");
    Ok(())
}

/// The manager's periodic work, until the process ends.
pub async fn housekeeping(m: Arc<Manager>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut pruned = std::time::Instant::now();
    let mut ticks: u64 = 0;
    loop {
        ticks += 1;
        tick.tick().await;
        if pruned.elapsed() >= Duration::from_secs(60) {
            pruned = std::time::Instant::now();
            m.prune_commands().await;
            let b = m.clone();
            tokio::spawn(async move {
                if let Err(e) = b.scheduled_backup().await {
                    tracing::warn!("the scheduled backup failed: {e:#}");
                    b.events.push("backup", None, format!("the scheduled backup failed: {e:#}"));
                    let me = b.advertise.first().cloned().unwrap_or_else(|| b.manager_id.clone());
                    b.tell(notify::Event { kind: "backup.failed", title: format!("A backup failed on the manager at {me}"), message: format!("{e:#}"), node: None, target: me });
                }
            });
        }
        if ticks.is_multiple_of(5) {
            m.replace_round().await;
        }
        m.apply_settings();
        m.quorum_guard();
        m.gather_heard().await;
        m.expire_overdue().await;
        m.fence_local().await;
        m.fence_round().await;
        m.setup_round().await;
        m.upgrade_round().await;
        if ticks.is_multiple_of(5) {
            m.sync_release().await;
        }
        m.recover_round().await;
        if ticks.is_multiple_of(prepull::EVERY_S) {
            m.prepull_round();
        }
        m.float_round().await;
        m.reconcile().await;
        m.push_network();
        m.local_worker_token().await;
        if ticks.is_multiple_of(2) {
            m.notify_round();
        }
        let health: Vec<_> = m.store.read(|s| s.nodes.keys().cloned().collect::<Vec<_>>()).into_iter().map(|id| {
            let h = m.sessions.liveness(&id).health;
            (id, h)
        }).collect();
        m.events.observe(&health);
    }
}

/// Serves the worker API on `listener` until the process ends.
pub async fn serve_workers(m: Arc<Manager>, listener: tokio::net::TcpListener) -> Result<()> {
    let ca = m.ca.clone();
    let tls = tls::server_config(&m.ca, tls::Rotating::new(move || ca.server_identity(), tls::RENEW_AFTER)?)?;
    tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(10)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .add_service(EnrollmentServer::new(grpc::EnrollmentSvc(m.clone())))
        .add_service(WorkerSessionServer::new(grpc::SessionSvc(m.clone())))
        .add_service(ManagerPeerServer::new(peer::PeerSvc(m)).max_decoding_message_size(64 << 20).max_encoding_message_size(64 << 20))
        .serve_with_incoming(tls::incoming(listener, tls))
        .await?;
    Ok(())
}

pub async fn serve_admin(m: Arc<Manager>, listener: tokio::net::TcpListener) -> Result<()> {
    axum::serve(listener, admin::router(m)).await?;
    Ok(())
}

/// Serves the admin API over TLS (HTTP/1.1) on `listener`, with this manager's server certificate (the cluster CA's,
/// for `MANAGER_SERVER_NAME`). Every route but health and the CA's certificate needs a token (`admin::guard`).
pub async fn serve_admin_tls(m: Arc<Manager>, listener: tokio::net::TcpListener) -> Result<()> {
    use tower::ServiceExt;
    let ca = m.ca.clone();
    let acceptor = tokio_rustls::TlsAcceptor::from(tls::api_server_config(tls::Rotating::new(move || ca.server_identity(), tls::RENEW_AFTER)?)?);
    let app = admin::router(m);
    loop {
        let (tcp, addr) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("accept: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let (acceptor, app) = (acceptor.clone(), app.clone());
        tokio::spawn(async move {
            let tls = match tokio::time::timeout(Duration::from_secs(10), acceptor.accept(tcp)).await {
                Ok(Ok(t)) => t,
                _ => return tracing::debug!("API TLS handshake with {addr} failed or took too long"),
            };
            let svc = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| app.clone().oneshot(req.map(axum::body::Body::new)));
            let conn = hyper::server::conn::http1::Builder::new().timer(hyper_util::rt::TokioTimer::new()).header_read_timeout(Duration::from_secs(10)).serve_connection(hyper_util::rt::TokioIo::new(tls), svc).with_upgrades();
            // with_upgrades: a shell is a WebSocket on this connection, taken over once the 101 has gone.
            if let Err(e) = conn.await {
                tracing::debug!("API connection from {addr}: {e}");
            }
        });
    }
}
