//! The authoritative cluster state, as a deterministic state machine over log entries.
//!
//! Every authoritative change is an `Entry`, proposed to the store (`store.rs`) and applied here in log order. `apply`
//! is a pure function of the state and the entry, with no clock, no randomness and no I/O, so every manager that applies
//! the same log reaches the same state. That is the contract Raft needs: today the log is a single manager's file; with
//! openraft it is the replicated log, and this module does not change.
//!
//! What is not here, on purpose: heartbeats and liveness. A heartbeat every two seconds from every worker would be most
//! of the log and say nothing durable; each manager keeps liveness for the workers connected to it in memory
//! (`sessions.rs`), and only transitions that matter (a node marked down, fenced, authorized) become entries.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub type Ms = i64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Entry {
    ClusterCreated { cluster_id: String, at: Ms },
    /// A backup's state, taken whole by a cluster that has none yet (`backup.rs`): the cluster it was comes back.
    StateRestored { state: Box<ClusterState>, at: Ms },
    JoinTokenIssued {
        secret_hash: String,
        expires_at: Ms,
        at: Ms,
        /// A manager join token (FDMT1) rather than a worker's (FDCT1).
        #[serde(default)]
        for_manager: bool,
    },
    /// A token spent on a certificate. Refused (and so not applied) if the token is unknown, spent or expired, or the
    /// node id is taken: two workers racing with one token get one certificate between them.
    NodeEnrolled { node_id: String, hostname: String, secret_hash: String, cert_sha256: String, at: Ms },
    /// What a worker said of itself in its hello, recorded when it differs from what is known.
    NodeReported { node_id: String, report: NodeReport, at: Ms },
    NodeRemoved { node_id: String, at: Ms },
    /// The node is given its overlay address (G10) if it has none: nodes enrolled before G10 get theirs so.
    NodeAddressed { node_id: String },
    /// A worker's new certificate (G2.7), asked for with its current one; that one stays good until the next.
    NodeCertRenewed { node_id: String, cert_sha256: String, at: Ms },
    /// A step of the node's fencing. Only a step the state machine allows is applied (`FenceState::allows`).
    NodeFencing { node_id: String, state: FenceState, provider: String, message: String, at: Ms },
    /// The node may run workloads again (plan 7.4): from awaiting authorization to authorized.
    NodeAuthorized { node_id: String, by: String, at: Ms },
    /// The node's API labels, all of them (an empty map clears them).
    NodeLabelsSet { node_id: String, labels: BTreeMap<String, String>, at: Ms },
    /// A manager token spent by a manager joining. Raft's membership is what makes it a manager; this entry makes the
    /// token good once and records who used it.
    ManagerJoined { raft_id: u64, addr: String, secret_hash: String, at: Ms },
    CommandCreated { command: CommandRecord },
    /// A stack made or changed from its Compose file. `placements` names the node of each workload it adds (and of
    /// each it has with no node); a workload already placed stays where it is. Refused, with nothing changed, when a
    /// new workload has no placement or its node is not enrolled. `nodes` are the nodes chosen for it (none: any);
    /// `exposures`, when given, replace its exposures on the pool, checked as `StackExposed` checks them.
    StackDeployed {
        name: String,
        compose: String,
        services: BTreeMap<String, ServiceSpec>,
        placements: BTreeMap<String, String>,
        at: Ms,
        #[serde(default)]
        nodes: Vec<String>,
        #[serde(default)]
        exposures: Option<Vec<Exposure>>,
        /// Its named volumes kept on shared storage (G11); the others are local to each node.
        #[serde(default)]
        volumes: BTreeMap<String, StackVolume>,
        /// What the deploy chose beside the Compose file, already applied to `services` and `volumes`; kept to show
        /// and to edit.
        #[serde(default)]
        options: StackOptions,
        /// Made or changed stopped: placed, its workloads not run until it is started (an import of a stack that
        /// still runs elsewhere). An entry from before runs, as every deploy did.
        #[serde(default)]
        stopped: bool,
    },
    /// Workloads of a stack put on other nodes, their specs as they were: the leader's, when one is on a node that no
    /// longer qualifies (its labels changed). Refused unless each is the stack's and each node enrolled.
    StackPlaced { name: String, placements: BTreeMap<String, String>, at: Ms },
    /// The stack's workloads are to run (`stopped: false`) or not.
    StackStopped { name: String, stopped: bool, at: Ms },
    /// The stack and its workloads are gone from the state; the workers' reconcile removes their containers.
    StackRemoved { name: String, at: Ms },
    CommandStatus { command_id: String, status: CommandStatus, message: String, result_json: String, at: Ms },
    /// Workloads moved off `from` (a fenced node) or, with `from: None`, given a node after theirs was removed (G7).
    /// `moves` is workload to new node; `left` is workload to why it stays, for the history. Refused, with nothing
    /// changed, unless `from` is fenced (or back, fenced, awaiting authorization), every moved workload is still on
    /// `from` and may be moved, and every new node is enrolled.
    WorkloadsRecovered { from: Option<String>, moves: BTreeMap<String, String>, left: BTreeMap<String, String>, at: Ms },
    /// A token for the API (G8.2), kept only as the SHA-256 of its secret, so any manager can check it.
    ApiTokenIssued { id: String, name: String, role: ApiRole, secret_hash: String, at: Ms },
    ApiTokenRevoked { id: String, at: Ms },
    /// The UI's database as a whole (G8.1), taken at change `head`: the changes up to it are dropped. Refused unless
    /// `head` is the latest change the managers hold, so a UI behind them cannot overwrite what another wrote.
    UiDbSnapshot { head: u64, data: Arc<str>, at: Ms },
    /// One write of the UI's database: refused unless `seq` is the next number, so only one UI writes at a time.
    UiDbChange { seq: u64, data: Arc<str>, at: Ms },
    /// A floating address given to the cluster (G10.3): a unicast IPv4 address of a LAN, outside the cluster's range.
    FloatingAdded { address: String, at: Ms },
    /// Addresses added to the pool at once (a range): refused whole if one is not a LAN's; those there already stay
    /// as they are.
    FloatingPoolAdded { addresses: Vec<String>, at: Ms },
    /// The address taken back; its holder lets it go at its next network state.
    FloatingRemoved { address: String, at: Ms },
    /// A stack's exposures on the pool (G10.5), all of them (an empty list takes them away). Refused whole unless each
    /// is to a service of the stack, tcp or udp, a container port and a port not the host's own, on an address of the
    /// pool, and no other exposure (of any stack) has that address, port and protocol.
    StackExposed { name: String, exposures: Vec<Exposure>, at: Ms },
    /// The address's holder, or none. Refused unless the address has no holder now, or its holder is fenced (or back,
    /// fenced, awaiting authorization), or is the one named; and a node named must be at rest. So an address is never
    /// given to a second node while the first may still answer for it.
    FloatingHeld { address: String, node_id: Option<String>, at: Ms },
    /// Shared storage given to the cluster (G11). Refused if the name is taken or it is not well formed.
    StorageAdded { storage: Storage },
    /// Refused while a stack keeps a volume on it.
    StorageRemoved { name: String, at: Ms },
    /// A CephFS's keys set anew (G12.6): a mount from now on takes the new mount key.
    StorageKeysSet { name: String, ceph: CephKeys, at: Ms },
    /// A private registry's login, set or replaced whole (`registry.rs`).
    RegistrySet { registry: crate::registry::Registry },
    RegistryRemoved { host: String, at: Ms },
    /// A notification channel, set or replaced whole (`notify.rs`); what it last told is kept.
    ChannelSet { channel: crate::notify::Channel },
    ChannelRemoved { name: String, at: Ms },
    /// How a channel's last delivery went: None it went through.
    ChannelOutcome { name: String, error: Option<String>, at: Ms },
    /// The node a workload moved away from has no copy of it running any more (G5.5).
    WorkloadLeft { workload_id: String, node_id: String, at: Ms },
    /// A workload with a limit on its attempts was run (again) for its spec `generation`.
    WorkloadAttempted { workload_id: String, generation: u64, at: Ms },
    /// It stayed as wanted for its restart window: its attempts count from nothing again.
    WorkloadSettled { workload_id: String, at: Ms },
    /// The cluster's settings, replaced whole (`settings.rs`).
    SettingsSet { settings: crate::settings::Settings, at: Ms },
    /// A volume for one writer (`stack/volume`) leased to the node about to run its workload. Refused while another
    /// node holds it, unless that node is fenced (or back, fenced, awaiting authorization) or gone: a second writer
    /// never starts while the first may still write (plan 7.2).
    LeaseTaken { volume: String, node_id: String, at: Ms },
    /// Its holder lets the volume go: nothing of it runs there any more.
    LeaseReleased { volume: String, node_id: String, at: Ms },
    /// Finished commands dropped from the state (`ClusterState::prunable`); one not finished, or not there, is left.
    CommandsPruned { ids: Vec<String>, at: Ms },
    /// Ceph to be made from the UI (G12.6): refused while another runs, or if its storage's name is taken.
    CephSetupStarted { setup: crate::ceph::CephSetup },
    /// A step of it went on: its command made, done (and what it learned), or failed (and so the setup).
    CephSetupStep { index: usize, state: crate::ceph::StepState, command_id: Option<String>, message: String, learned: Option<crate::ceph::Learned>, at: Ms },
    /// A failed setup goes on from where it failed.
    CephSetupRetried { at: Ms },
    /// The setup forgotten (what it made stays).
    CephSetupCleared { at: Ms },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Rejected {
    NoCluster,
    TokenUnknown,
    TokenSpent,
    TokenExpired,
    /// A worker's token used to join a manager, or a manager's to enrol a worker.
    TokenWrongKind,
    NodeExists,
    NodeUnknown,
    CommandExists,
    CommandUnknown,
    StackUnknown,
    /// A new workload of a stack with no node to run on.
    Unplaced,
    /// A command for a workload that has one open already: one at a time, so two managers never race on it.
    WorkloadBusy,
    /// A fencing step the state machine does not allow from where the node is.
    FenceStep,
    /// A stack or service name that is not 1 to 40 of a-z 0-9 and - (services may have _ too), starting alphanumeric.
    BadName,
    /// A container name another workload in the cluster has (its own or `<stack>-<service>-<slot>`).
    NameTaken,
    /// A recovery for a workload that is no longer where it said, or may not move by itself (pinned or local data).
    NotRecoverable,
    /// An API token with that id or secret exists.
    TokenExists,
    /// A UI database write or snapshot that is not the next: another UI wrote since (or none was made yet).
    UiDbStale,
    FloatingExists,
    FloatingUnknown,
    /// Not an address a LAN could give the cluster.
    BadAddress,
    /// A floating address a stack exposes something on.
    FloatingInUse,
    /// An exposure to no service of the stack, or with a protocol or port that cannot be.
    BadExposure,
    /// An address, port and protocol another exposure has.
    ExposureTaken,
    StorageExists,
    StorageUnknown,
    /// Storage a stack keeps a volume on.
    StorageInUse,
    /// A storage name, source or options that cannot be.
    BadStorage,
    /// A registry that is not a host and maybe a port, or a login without a user.
    BadRegistry,
    RegistryUnknown,
    /// A channel name, kind or event that cannot be.
    BadChannel,
    ChannelUnknown,
    /// A volume for one writer used by more than one workload.
    VolumeShared,
    /// A volume another node may still be writing to.
    LeaseHeld,
    /// Settings out of their range or order, or a Proxmox provider that cannot be.
    BadSettings,
    /// A backup restored over a cluster that has a state already.
    NotEmpty,
    /// A Ceph setup that cannot be: see `CephSetup::valid`, its nodes known, its storage's name free.
    BadCephSetup,
    /// Another Ceph setup runs.
    CephSetupBusy,
    /// No Ceph setup, or none in the state the step needs.
    CephSetupUnknown,
}

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Rejected::NoCluster => "the cluster is not initialised",
            Rejected::TokenUnknown => "the join token is not this cluster's",
            Rejected::TokenSpent => "the join token has been used",
            Rejected::TokenExpired => "the join token has expired",
            Rejected::TokenWrongKind => "the join token is for the other kind of node (worker or manager)",
            Rejected::NodeExists => "a node with that id is already enrolled",
            Rejected::NodeUnknown => "no such node",
            Rejected::CommandExists => "a command with that id exists",
            Rejected::CommandUnknown => "no such command",
            Rejected::StackUnknown => "no such stack",
            Rejected::Unplaced => "a workload of the stack has no node to run on",
            Rejected::WorkloadBusy => "the workload has a command open already",
            Rejected::FenceStep => "the node's fencing is not at a point where that step can be taken",
            Rejected::NotRecoverable => "the workload is not on that node any more, or may not be moved by itself",
            Rejected::UiDbStale => "the UI's database has moved on since: load it again",
            Rejected::TokenExists => "an API token with that id exists",
            Rejected::FloatingExists => "the cluster has that floating address already",
            Rejected::FloatingUnknown => "no such floating address",
            Rejected::FloatingInUse => "a stack exposes something on that floating address: take the exposure away first",
            Rejected::BadExposure => "an exposure is to a service of the stack, over tcp or udp, from a port that is not the host's own (22, 7450 to 7452, 51820) to a container port",
            Rejected::ExposureTaken => "that address already has that port (and protocol) exposed",
            Rejected::BadAddress => "a floating address must be a unicast IPv4 address of a LAN, outside the cluster's range 10.240.0.0/16",
            Rejected::BadName => "a stack or service name must be 1 to 40 of a-z, 0-9 and - (services may have _), starting with a letter or digit; a container name 1 to 63 of a-z, 0-9 and -",
            Rejected::NameTaken => "another workload in the cluster has that container name",
            Rejected::StorageExists => "the cluster has storage of that name already",
            Rejected::StorageUnknown => "no such storage (or volume on it)",
            Rejected::StorageInUse => "a stack keeps a volume on that storage: remove the stack first",
            Rejected::BadStorage => "storage is named 1 to 40 of a-z, 0-9 and -; nfs is host:/export with options of letters, digits and = , . _ : - /; cephfs is the monitors' addresses:/path with name=<client> in its options (and fs=<filesystem> to evict), its keys apart; path is an absolute directory other than /",
            Rejected::VolumeShared => "a volume for one writer (access: one) is used by one workload only: one service with one replica, or access: many",
            Rejected::LeaseHeld => "another node holds that volume and may still be writing to it",
            Rejected::NotEmpty => "a backup is restored into a new cluster only, not over one that has a state",
            Rejected::BadRegistry => "a registry is a host and maybe a port (ghcr.io, registry.example:5000, docker.io), and its login has a user and a password",
            Rejected::RegistryUnknown => "the cluster has no login for that registry",
            Rejected::BadCephSetup => "a Ceph setup names its storage (1 to 40 of a-z, 0-9 and -, not taken) and filesystem, and each node once, known, with its disks as /dev/ paths",
            Rejected::CephSetupBusy => "Ceph is being set up already: wait for it, or clear it",
            Rejected::CephSetupUnknown => "no Ceph setup in that state",
            Rejected::BadChannel => "a channel is named 1 to 64 of a-z, 0-9, - and _, is a webhook, email, ntfy or gotify, and tells only of events the cluster knows",
            Rejected::ChannelUnknown => "the cluster has no notification channel of that name",
            Rejected::BadSettings => "a setting is out of its range or order, or the Proxmox provider's settings do not hold",
        })
    }
}

impl std::error::Error for Rejected {}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeReport {
    pub hostname: String,
    pub agent_version: String,
    pub boot_id: String,
    pub fencing_state: String,
    pub labels: BTreeMap<String, String>,
    pub docker: bool,
    pub docker_version: String,
    pub os: String,
    pub arch: String,
    pub kernel: String,
    pub cpu_count: u32,
    pub memory_total_bytes: u64,
    /// The node's WireGuard public key (G9); empty for a worker without the overlay.
    #[serde(default)]
    pub wg_public_key: String,
    /// The worker answers DockerRequest (the per-node Docker pages); an older one does not.
    #[serde(default)]
    pub docker_requests: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub hostname: String,
    pub cert_sha256: String,
    /// The certificate before the last renewal, still good: a worker that failed to keep the new one comes back.
    #[serde(default)]
    pub prev_cert_sha256: Option<String>,
    pub enrolled_at: Ms,
    pub report: Option<NodeReport>,
    pub reported_at: Option<Ms>,
    /// Labels set through the API (G5.1). They go over the worker's own of the same key.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Where the node is in fencing (G6). Separate from health: unreachable does not mean fenced (plan 7.2).
    #[serde(default)]
    pub fencing: Fencing,
    /// Its workloads were recovered for the current fencing (G7); cleared when fencing starts again or ends.
    #[serde(default)]
    pub recovered: bool,
    /// Its overlay address's last byte (G10), `<range>.0.<n>`: given at enrolment (the lowest free), kept for its life.
    #[serde(default)]
    pub overlay: Option<u8>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FenceState {
    /// Nothing going on: the node may run workloads.
    #[default]
    None,
    /// The cluster saw it unreachable and wants it fenced.
    Requested,
    /// A provider is fencing it (the worker's own timers, or a power-off).
    InProgress,
    /// Fenced: by a provider's verified result, or presumed after the self-fencing timers ran out (the message says
    /// which).
    Fenced,
    Failed,
    /// It came back fenced and runs nothing until it is authorized.
    AwaitingAuthorization,
    /// Authorized; until the worker says it runs again.
    Authorized,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fencing {
    pub state: FenceState,
    #[serde(default)]
    pub since: Ms,
    /// Who acts: `self` (the worker's timers), a provider's name, or who authorized it.
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub message: String,
}

impl FenceState {
    /// Whether the step to `to` is allowed from here. Requested only from rest; in progress only once requested;
    /// fenced or failed only while in progress; awaiting authorization from anywhere (the worker says so); back to
    /// rest from a fencing that never finished (it was heard again first), or once an authorized worker runs again.
    pub fn allows(self, to: FenceState) -> bool {
        use FenceState::*;
        match to {
            Requested => matches!(self, None | Failed),
            InProgress => self == Requested,
            Fenced | Failed => self == InProgress,
            AwaitingAuthorization => !matches!(self, AwaitingAuthorization | Authorized),
            Authorized => self == AwaitingAuthorization,
            None => matches!(self, Requested | InProgress | Fenced | Failed | Authorized),
        }
    }
}

impl Node {
    /// The labels placement sees: the worker's own (`FREIGHTDOCK_LABELS`), then the API's over them.
    pub fn all_labels(&self) -> BTreeMap<String, String> {
        let mut l = self.report.as_ref().map(|r| r.labels.clone()).unwrap_or_default();
        l.extend(self.labels.clone());
        l
    }

    /// What a person calls it: its `name` label, else its hostname (two nodes made from one template share that).
    pub fn display_name(&self) -> String {
        self.all_labels().get("name").cloned().unwrap_or_else(|| self.hostname.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinToken {
    pub expires_at: Ms,
    pub spent_by: Option<String>,
    #[serde(default)]
    pub for_manager: bool,
}

/// The operation of a command, as the API takes it and the log keeps it. `grpc.rs` turns it into the protobuf op.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
// A run is much bigger than the rest; ops are few and short-lived, so it is not boxed.
#[allow(clippy::large_enum_variant)]
pub enum Op {
    Ping,
    DockerInfo,
    RunContainer {
        workload_id: String,
        image: String,
        #[serde(default)]
        name: String,
        #[serde(default)]
        command: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
        #[serde(default)]
        labels: BTreeMap<String, String>,
        /// The workload's generation (`Workload::generation`); 0 for a container run by hand.
        #[serde(default)]
        generation: u64,
        #[serde(default)]
        ports: Vec<Port>,
        #[serde(default)]
        volumes: Vec<String>,
        #[serde(default, skip_serializing_if = "Resources::is_zero")]
        limits: Resources,
        /// On its stack's network (G9); none: Docker's default network.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        net: Option<Attach>,
        /// Its named volumes on shared storage (G11), for the worker to make ready and bind.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        storage: Vec<StorageMount>,
    },
    StopContainer {
        workload_id: String,
        #[serde(default)]
        timeout_s: u32,
    },
    RemoveContainer {
        workload_id: String,
    },
    /// `ceph <args>` run once on the node, against the Ceph of a CephFS storage as its fencing client (G12.6). The
    /// monitors, the image and the key come from the storage as the command is sent, never kept with it.
    CephCommand {
        storage: String,
        args: Vec<String>,
        #[serde(default)]
        timeout_s: u32,
    },
    /// A step of Ceph made from the UI (G12.6, `ceph::CephSetup`), run on the node's host by its worker.
    Cephadm {
        action: String,
        args: Vec<String>,
        #[serde(default)]
        image: String,
        #[serde(default)]
        timeout_s: u32,
    },
}

impl Op {
    /// The workload the op is about, if it is about one.
    pub fn workload_id(&self) -> Option<&str> {
        match self {
            Op::RunContainer { workload_id, .. } | Op::StopContainer { workload_id, .. } | Op::RemoveContainer { workload_id } => Some(workload_id),
            Op::Ping | Op::DockerInfo | Op::CephCommand { .. } | Op::Cephadm { .. } => None,
        }
    }
}

/// A published port: `host_port` 0 lets Docker pick one.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Port {
    #[serde(default)]
    pub host_port: u32,
    pub container_port: u32,
    /// tcp or udp.
    #[serde(default = "tcp")]
    pub protocol: String,
    #[serde(default)]
    pub host_ip: String,
}

fn tcp() -> String {
    "tcp".into()
}

fn one() -> u32 {
    1
}

/// One service of a stack, as read from its Compose file (`compose.rs`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceSpec {
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub ports: Vec<Port>,
    #[serde(default)]
    pub volumes: Vec<String>,
    #[serde(default = "one")]
    pub replicas: u32,
    /// What the container may use at most (Compose `deploy.resources.limits`).
    #[serde(default)]
    pub limits: Resources,
    /// Where its workloads may go and what they take from a node: placement's business, not the container's.
    #[serde(default)]
    pub placement: Placement,
    /// Whether a workload of it is recovered on another node when its node fails (plan 5.3, used by G7). Off: it is
    /// pinned, and stays stopped with its node until someone moves it.
    #[serde(default = "yes")]
    pub ha: bool,
    /// When a workload of it that exited is run again, and how soon (Compose `restart`, `deploy.restart_policy`).
    #[serde(default)]
    pub restart: Restart,
    /// A name of its own for its containers (Compose `container_name`, or one chosen when deploying), each replica's
    /// with `-<slot>` when it has more than one; empty: `<stack>-<service>-<slot>`. Also a name for it inside the stack.
    #[serde(default)]
    pub name: String,
    /// A move starts the new copy before the old one stopped (Compose `deploy.update_config.order: start-first`);
    /// by default the old one stops first, as Docker's does.
    #[serde(default)]
    pub start_first: bool,
}

fn yes() -> bool {
    true
}

/// When an exited workload is run again (G4.5): after any exit, after a failure only, or never.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RestartWhen {
    #[default]
    Any,
    OnFailure,
    Never,
}

/// A service's restart policy: the condition; the first wait (`delay_ms`, doubling for each run in a row, to five
/// minutes); how many runs in a row at most (`max_attempts`, 0 for no end); and how long it must run to count as
/// started (`window_ms`), so one that dies within it still counts against the attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Restart {
    pub condition: RestartWhen,
    pub delay_ms: u64,
    pub max_attempts: u32,
    pub window_ms: u64,
}

impl Default for Restart {
    fn default() -> Self {
        Restart { condition: RestartWhen::Any, delay_ms: 5000, max_attempts: 0, window_ms: 10_000 }
    }
}

impl Restart {
    /// Whether a workload that exited with `code` is run again.
    pub fn again_after(&self, code: i32) -> bool {
        match self.condition {
            RestartWhen::Any => true,
            RestartWhen::OnFailure => code != 0,
            RestartWhen::Never => false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resources {
    /// Thousandths of a CPU; 0 is no figure.
    #[serde(default)]
    pub cpu_millis: u64,
    #[serde(default)]
    pub memory_bytes: u64,
}

impl Resources {
    pub fn is_zero(&self) -> bool {
        self.cpu_millis == 0 && self.memory_bytes == 0
    }
}

/// A test on a node: `field` is `node.id`, `node.hostname`, `node.platform.os`, `node.platform.arch` or
/// `node.labels.<key>`; it holds when the field's value equals `value` (or, with `equal` false, when it does not).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Constraint {
    pub field: String,
    pub equal: bool,
    pub value: String,
}

impl Constraint {
    pub fn holds(&self, n: &Node) -> bool {
        let value = match self.field.as_str() {
            "node.id" => Some(n.id.clone()),
            "node.hostname" => Some(n.hostname.clone()),
            "node.platform.os" => n.report.as_ref().map(|r| r.os.clone()),
            "node.platform.arch" => n.report.as_ref().map(|r| r.arch.clone()),
            f => f.strip_prefix("node.labels.").and_then(|k| n.all_labels().get(k).cloned()),
        };
        (value.as_deref() == Some(self.value.as_str())) == self.equal
    }
}

impl std::fmt::Display for Constraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}{}", self.field, if self.equal { "==" } else { "!=" }, self.value)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    /// Every one must hold (required and excluded labels).
    #[serde(default)]
    pub constraints: Vec<Constraint>,
    /// Nodes where these hold come first (preferred labels).
    #[serde(default)]
    pub prefer: Vec<Constraint>,
    /// Label keys to spread the replicas over, the first first (Swarm's `spread: node.labels.<key>`).
    #[serde(default)]
    pub spread: Vec<String>,
    /// What each workload is counted as taking from its node (Compose `deploy.resources.reservations`).
    #[serde(default)]
    pub reserve: Resources,
    /// Services never on a node with one of its workloads (anti-affinity, `x-freightdock.apart`): a service of the
    /// stack, or `<stack>/<service>`. It holds both ways.
    #[serde(default)]
    pub apart: Vec<String>,
}

impl ServiceSpec {
    /// The spec as one workload's container runs it: the replica count, the placement and HA are the stack's
    /// business, so changing them makes no container again.
    /// Its name is the replica's own: `<name>-<slot>` when it has more than one.
    pub fn for_workload(&self, slot: u32) -> ServiceSpec {
        let name = if self.name.is_empty() || self.replicas <= 1 { self.name.clone() } else { format!("{}-{slot}", self.name) };
        ServiceSpec { replicas: 1, placement: Placement::default(), ha: true, name, ..self.clone() }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stack {
    pub name: String,
    /// The Compose file as given, to show and to edit.
    pub compose: String,
    pub services: BTreeMap<String, ServiceSpec>,
    pub stopped: bool,
    pub created_at: Ms,
    pub updated_at: Ms,
    /// Its /24 in the cluster's range (G9): the third byte. Given at its first deploy after G9, kept until it goes.
    #[serde(default)]
    pub net: Option<u8>,
    /// What it exposes on the pool's floating addresses (G10.5), chosen outside its Compose file.
    #[serde(default)]
    pub exposures: Vec<Exposure>,
    /// The nodes chosen for it to run on; none: any.
    #[serde(default)]
    pub nodes: Vec<String>,
    /// Its named volumes kept on shared storage (G11), by volume name.
    #[serde(default)]
    pub volumes: BTreeMap<String, StackVolume>,
    #[serde(default)]
    pub options: StackOptions,
}

/// What a deploy chooses beside the Compose file (`compose::apply_options`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct StackOptions {
    /// What the stack is for, in the words of whoever deployed it.
    pub description: String,
    /// The env file its `${VAR}`s and its environment names with no value are filled from (`KEY=value` lines).
    pub env: String,
    /// Node labels every workload of it needs (constraints, on top of its services' own).
    pub selector: Vec<Constraint>,
    /// How its workloads behave when their node fails, over what the file says; none: as it says.
    pub mode: Option<Mode>,
    /// Container names by service, over `container_name`; an empty one keeps the file's.
    pub names: BTreeMap<String, String>,
}

/// A stack's way of surviving a node: moved freely, its named volumes local (stateless); waiting for its node
/// (pinned); or moved with its named volumes on shared storage, one writer at a time (shared).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Mode {
    Stateless,
    Pinned,
    Shared { storage: String },
}

/// Where a stack keeps a named volume (G11): on the cluster's storage of that name, for one writer at a time or many.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StackVolume {
    pub storage: String,
    #[serde(default)]
    pub access: Access,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Access {
    /// One workload writes it, on one node at a time (leased): a database's files.
    #[default]
    One,
    /// Any number of workloads, on any nodes at once: what the application itself keeps safe to share.
    Many,
}

/// Shared storage given to the cluster (G11, plan 8). The workers make it ready where a workload needs it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Storage {
    pub name: String,
    pub driver: StorageDriver,
    /// nfs: `host:/export`; path: an absolute directory that is the same shared storage on every node.
    pub source: String,
    /// nfs: mount options, as `mount -o` takes them; cephfs: the kernel client's (`name=` the client, `fs=` the
    /// filesystem); none for path.
    #[serde(default)]
    pub options: String,
    pub added_at: Ms,
    /// cephfs: the keys (G12.6), set once and never shown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ceph: Option<CephKeys>,
}

/// The Ceph image a `ceph` command runs in when the storage names none: the release Freightdock was tried with.
pub const DEFAULT_CEPH_IMAGE: &str = "quay.io/ceph/ceph:v20";

/// A CephFS's keys (G12.6). The mount key goes to the kernel with each mount; the fencing client, when there is one,
/// evicts a fenced node's mount (`ceph tell mds.<fs>:0 client evict`), which Ceph then blocklists, so it writes no
/// more and the files it held are free at once. Its caps: `mon 'allow r' mds 'allow * fsname=<fs>'`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CephKeys {
    /// The key of the client the options name (`name=`). The kernel takes only the older key type (`AQ...`).
    pub secret: crate::settings::Secret,
    /// The client that evicts, without `client.`; empty: no eviction (the node's own fencing only).
    #[serde(default)]
    pub fence_client: String,
    #[serde(default)]
    pub fence_secret: Option<crate::settings::Secret>,
    /// The Ceph image the evicting command runs in; empty: `DEFAULT_CEPH_IMAGE`.
    #[serde(default)]
    pub image: String,
}

/// A Ceph key as it is written: base64.
fn ceph_key(k: &str) -> bool {
    !k.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || "+/=".contains(c))
}

/// A CephFS source as the kernel takes it: the monitors' addresses (IPs, maybe ports: it resolves no names), `:`, and
/// an absolute path, `/` for the whole filesystem.
pub fn ceph_source(s: &str, abs: impl Fn(&str) -> bool) -> bool {
    let Some(i) = s.find(":/") else { return false };
    let (mons, path) = (&s[..i], &s[i + 1..]);
    !mons.is_empty() && mons.split(',').all(ceph_mon) && (path == "/" || abs(path))
}

fn ceph_mon(m: &str) -> bool {
    m.parse::<std::net::IpAddr>().is_ok() || m.parse::<std::net::SocketAddr>().is_ok() || m.strip_prefix('[').and_then(|r| r.strip_suffix(']')).is_some_and(|v6| v6.parse::<std::net::Ipv6Addr>().is_ok())
}

impl CephKeys {
    pub fn valid(&self) -> bool {
        let plain = |s: &str| s.chars().all(|c| c.is_ascii_alphanumeric() || "._-".contains(c));
        let image = |s: &str| s.chars().all(|c| c.is_ascii_alphanumeric() || "._-/:@".contains(c));
        ceph_key(&self.secret.0)
            && plain(&self.fence_client)
            && !self.fence_client.starts_with("client.")
            && match &self.fence_secret {
                Some(k) => !self.fence_client.is_empty() && ceph_key(&k.0),
                None => self.fence_client.is_empty(),
            }
            && image(&self.image)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageDriver {
    /// An NFS export the worker mounts itself.
    Nfs,
    /// A directory made shared outside Freightdock (a mount the host has, a bind mount into a container).
    Path,
    /// A CephFS the worker's kernel mounts (G12.6).
    Cephfs,
}

/// What a storage driver can do (plan 8.3), for the scheduler and the person reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Capabilities {
    pub multi_host_mount: bool,
    pub multi_host_write: bool,
    /// The worker can take writes away itself when it fences (it unmounts).
    pub write_revocation: bool,
    /// The managers keep one writer at a time on a volume for one writer.
    pub leases: bool,
    /// The managers can cut a fenced node's client off at the storage itself (CephFS eviction), whatever the node does.
    pub client_eviction: bool,
}

impl StorageDriver {
    pub fn capabilities(self) -> Capabilities {
        let mounted = matches!(self, StorageDriver::Nfs | StorageDriver::Cephfs);
        Capabilities { multi_host_mount: true, multi_host_write: true, write_revocation: mounted, leases: true, client_eviction: self == StorageDriver::Cephfs }
    }

    /// Its name on the wire.
    pub fn word(self) -> &'static str {
        match self {
            StorageDriver::Nfs => "nfs",
            StorageDriver::Path => "path",
            StorageDriver::Cephfs => "cephfs",
        }
    }
}

impl Storage {
    /// Whether it is well formed: a name like a stack's; an nfs source `host:/export` and options of plain
    /// characters; a path source absolute, not `/`, and nothing that would read as another part of a bind.
    pub fn valid(&self) -> bool {
        let plain = |s: &str, extra: &str| s.chars().all(|c| c.is_ascii_alphanumeric() || extra.contains(c));
        let abs = |p: &str| p.starts_with('/') && p != "/" && !p.split('/').any(|c| c == "..") && plain(p, "/._-");
        valid_stack_name(&self.name)
            && match self.driver {
                StorageDriver::Nfs => self.source.split_once(":/").is_some_and(|(host, path)| !host.is_empty() && plain(host, ".-[]") && abs(&format!("/{path}"))) && plain(&self.options, "=,._:-/") && self.ceph.is_none(),
                StorageDriver::Path => abs(&self.source) && self.options.is_empty() && self.ceph.is_none(),
                StorageDriver::Cephfs => {
                    let keyless = !self.options.split(',').any(|o| matches!(o.split('=').next(), Some("secret" | "secretfile" | "key" | "keyfile")));
                    ceph_source(&self.source, abs)
                        && plain(&self.options, "=,._:-/")
                        && keyless
                        && self.ceph_client().is_some_and(|c| !c.is_empty())
                        && self.ceph.as_ref().is_some_and(|k| k.valid() && (k.fence_client.is_empty() || self.ceph_fs().is_some_and(|f| !f.is_empty())))
                }
            }
    }

    fn option(&self, key: &str) -> Option<&str> {
        self.options.split(',').find_map(|o| o.strip_prefix(key)?.strip_prefix('='))
    }

    /// cephfs: the client it mounts as (`name=`).
    pub fn ceph_client(&self) -> Option<&str> {
        self.option("name")
    }

    /// cephfs: the filesystem (`fs=`, or the older `mds_namespace=`).
    pub fn ceph_fs(&self) -> Option<&str> {
        self.option("fs").or_else(|| self.option("mds_namespace"))
    }

    /// cephfs: the image its `ceph` commands run in.
    pub fn ceph_image(&self) -> &str {
        self.ceph.as_ref().map(|k| k.image.as_str()).filter(|i| !i.is_empty()).unwrap_or(DEFAULT_CEPH_IMAGE)
    }

    /// cephfs: the monitors, as `mon_host` takes them.
    pub fn ceph_mons(&self) -> &str {
        self.source.find(":/").map_or("", |i| &self.source[..i])
    }
}

/// A named volume on shared storage as a worker needs it (G11): the storage, and the volume's directory on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMount {
    pub volume: String,
    pub storage: String,
    pub driver: StorageDriver,
    pub source: String,
    #[serde(default)]
    pub options: String,
    /// `<stack>/<volume>`.
    pub dir: String,
}

/// A volume for one writer held by a node (G11).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub node_id: String,
    pub since: Ms,
}

/// One of a stack's services reached from outside the cluster on a pool address (G10.5): `address:port` goes to the
/// service's `container_port`, wherever its workloads run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exposure {
    pub service: String,
    pub container_port: u16,
    /// tcp or udp.
    pub protocol: String,
    pub address: String,
    pub port: u16,
}

/// One replica of one service: one container on one node. Its id, `<stack>.<service>.<slot>`, is the container's
/// workload label.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Workload {
    pub id: String,
    pub stack: String,
    pub service: String,
    pub slot: u32,
    /// None once its node was removed, until the stack is deployed again (G7 moves it by itself).
    pub node_id: Option<String>,
    pub spec: ServiceSpec,
    /// The log index at which its spec last changed: a container made for an older one is made again.
    pub generation: u64,
    /// Its address in its stack's /24 (G9): the last byte, 2 to 254 (.1 is the bridge). Kept when it moves.
    #[serde(default)]
    pub host: Option<u8>,
    /// The node it moved away from, until that node's copy is gone (`WorkloadLeft`): its new node runs it only then,
    /// or once that node is fenced or removed (G5.5). None for a start-first service.
    #[serde(default)]
    pub leaving: Option<String>,
    /// Its runs in a row for one spec, kept for a service with `max_attempts` so that any manager acting on it counts
    /// on from the same number (G4.5): cleared once it stays as wanted for its restart window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempts: Option<Attempts>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempts {
    pub generation: u64,
    pub runs: u32,
    /// When the last of them was asked for.
    pub at: Ms,
}

impl Workload {
    /// Placed on `to`: a move from another node remembers the node it leaves (unless start-first); back onto the node
    /// it was leaving, there is nothing left to wait for.
    fn place(&mut self, to: &str) {
        match self.node_id.as_deref() {
            Some(from) if from != to && !self.spec.start_first => {
                self.leaving = Some(from.to_string());
            }
            _ => {}
        }
        if self.leaving.as_deref() == Some(to) {
            self.leaving = None;
        }
        self.node_id = Some(to.to_string());
    }
    /// Its container's name on its node: its own (`ServiceSpec::name`), else `<stack>-<service>-<slot>`.
    pub fn container_name(&self) -> String {
        if self.spec.name.is_empty() { format!("{}-{}-{}", self.stack, self.service, self.slot) } else { self.spec.name.clone() }
    }
}

/// Where a container joins the overlay (G9): its stack's network, its address there, and the stack's names as
/// `name:address` (each service, and each replica as `<service>-<slot>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attach {
    pub stack: String,
    pub address: String,
    pub hosts: Vec<String>,
}

/// The cluster's internal range (G9): a /16, a /24 in it per stack.
pub const CLUSTER_RANGE: [u8; 2] = [10, 240];

impl ClusterState {
    /// A stack's subnet, as `10.240.3.0/24`, once it has one.
    pub fn subnet(&self, stack: &str) -> Option<String> {
        let n = self.stacks.get(stack)?.net?;
        Some(format!("{}.{}.{n}.0/24", CLUSTER_RANGE[0], CLUSTER_RANGE[1]))
    }

    /// How a workload joins its stack's network, once it has an address.
    pub fn attach(&self, w: &Workload) -> Option<Attach> {
        let address = self.address(&w.id)?;
        let mut hosts = vec![];
        for o in self.workloads.values().filter(|o| o.stack == w.stack) {
            if let Some(a) = self.address(&o.id) {
                hosts.push(format!("{}:{a}", o.service));
                hosts.push(format!("{}-{}:{a}", o.service, o.slot));
                if !o.spec.name.is_empty() {
                    hosts.push(format!("{}:{a}", o.spec.name));
                }
            }
        }
        Some(Attach { stack: w.stack.clone(), address, hosts })
    }

    /// A workload's address on its stack's network, once it has one.
    pub fn address(&self, workload: &str) -> Option<String> {
        let w = self.workloads.get(workload)?;
        let n = self.stacks.get(&w.stack)?.net?;
        Some(format!("{}.{}.{n}.{}", CLUSTER_RANGE[0], CLUSTER_RANGE[1], w.host?))
    }
}

pub fn workload_id(stack: &str, service: &str, slot: u32) -> String {
    format!("{stack}.{service}.{slot}")
}

fn good_name(n: &str, underscore: bool) -> bool {
    !n.is_empty() && n.len() <= 40 && n.as_bytes()[0].is_ascii_alphanumeric() && n.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || (underscore && b == b'_'))
}

/// A container's own name: 1 to 63 of a-z, 0-9 and -, starting with a letter or digit (a host name too).
pub fn valid_container_name(n: &str) -> bool {
    !n.is_empty() && n.len() <= 63 && n.as_bytes()[0].is_ascii_alphanumeric() && n.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

pub fn valid_stack_name(n: &str) -> bool {
    good_name(n, false)
}

pub fn valid_service_name(n: &str) -> bool {
    good_name(n, true)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    Pending,
    Received,
    Accepted,
    Succeeded,
    Failed,
    Rejected,
    Expired,
}

impl CommandStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, CommandStatus::Succeeded | CommandStatus::Failed | CommandStatus::Rejected | CommandStatus::Expired)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandRecord {
    pub id: String,
    pub node_id: String,
    pub op: Op,
    pub config_version: u64,
    pub created_at: Ms,
    pub deadline: Ms,
    pub status: CommandStatus,
    pub message: String,
    pub result_json: String,
    pub updated_at: Ms,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ClusterState {
    pub cluster_id: Option<String>,
    /// The index of the last entry applied.
    pub applied: u64,
    pub join_tokens: BTreeMap<String, JoinToken>,
    pub nodes: BTreeMap<String, Node>,
    pub commands: BTreeMap<String, CommandRecord>,
    #[serde(default)]
    pub stacks: BTreeMap<String, Stack>,
    #[serde(default)]
    pub workloads: BTreeMap<String, Workload>,
    /// The latest recoveries, oldest first, at most `RECOVERY_HISTORY` (G7).
    #[serde(default)]
    pub recoveries: std::collections::VecDeque<Recovery>,
    /// API tokens by id (G8.2).
    #[serde(default)]
    pub api_tokens: BTreeMap<String, ApiToken>,
    /// The UI's database (G8.1): opaque to the managers.
    #[serde(default)]
    pub ui_db: UiDb,
    /// The floating addresses (G10.3), the pool, by address.
    #[serde(default)]
    pub floating: BTreeMap<String, Floating>,
    /// Shared storage (G11), by name.
    #[serde(default)]
    pub storage: BTreeMap<String, Storage>,
    /// Volumes for one writer and the node each is leased to, by `stack/volume`.
    #[serde(default)]
    pub leases: BTreeMap<String, Lease>,
    /// The cluster's settings; what is not set here each manager takes from its own environment.
    #[serde(default)]
    pub settings: crate::settings::Settings,
    /// Private registries' logins, by registry.
    #[serde(default)]
    pub registries: BTreeMap<String, crate::registry::Registry>,
    /// Notification channels, by name.
    #[serde(default)]
    pub notifications: BTreeMap<String, crate::notify::Channel>,
    /// Ceph made from the UI (G12.6): the one being made, or the last.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ceph_setup: Option<crate::ceph::CephSetup>,
}

/// A floating address: on its holder's LAN, answering the published ports as the nodes' own addresses do.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Floating {
    pub holder: Option<String>,
    /// When it was added or last given a holder.
    pub since: Ms,
}

/// The most addresses one range adds to the pool.
pub const POOL_RANGE_MAX: u32 = 1024;

/// The addresses a pool entry is written as: one address, `a.b.c.d-e` (the last byte), `a.b.c.d-e.f.g.h`, or a CIDR
/// prefix written from its first address (without its network and broadcast addresses, but for a /31 or /32). None
/// when it is none of those, runs backwards or holds more than [`POOL_RANGE_MAX`]. Whether each is a LAN's address is
/// checked when it is added.
pub fn pool_range(s: &str) -> Option<Vec<String>> {
    use std::net::Ipv4Addr;
    let s = s.trim();
    let (from, to) = if let Some((ip, len)) = s.split_once('/') {
        let (ip, len): (Ipv4Addr, u32) = (ip.parse().ok()?, len.parse().ok()?);
        if len > 32 {
            return None;
        }
        let mask = if len == 0 { 0 } else { u32::MAX << (32 - len) };
        let net = u32::from(ip);
        if net & !mask != 0 {
            return None;
        }
        let last = net | !mask;
        if len >= 31 { (net, last) } else { (net + 1, last - 1) }
    } else if let Some((a, b)) = s.split_once('-') {
        let a: Ipv4Addr = a.parse().ok()?;
        let b: Ipv4Addr = if b.contains('.') { b.parse().ok()? } else { let o = a.octets(); Ipv4Addr::new(o[0], o[1], o[2], b.parse().ok()?) };
        (u32::from(a), u32::from(b))
    } else {
        let a: Ipv4Addr = s.parse().ok()?;
        (u32::from(a), u32::from(a))
    };
    if to < from || to - from >= POOL_RANGE_MAX {
        return None;
    }
    Some((from..=to).map(|i| Ipv4Addr::from(i).to_string()).collect())
}

/// Whether `a` can be a floating address: a unicast IPv4 address of a LAN, outside the cluster's range.
pub fn floating_ok(a: &str) -> bool {
    a.parse::<std::net::Ipv4Addr>().is_ok_and(|ip| {
        !(ip.is_unspecified() || ip.is_loopback() || ip.is_link_local() || ip.is_multicast() || ip.is_broadcast() || ip.octets()[0] == 0 || ip.octets()[..2] == CLUSTER_RANGE) && ip.to_string() == a
    })
}

/// The UI's database as the managers keep it: a snapshot and the changes after it, both as the UI wrote them.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UiDb {
    /// The latest change's number (0: none yet).
    pub head: u64,
    /// The change the snapshot includes everything up to.
    pub snapshot_head: u64,
    pub snapshot: Option<Arc<str>>,
    pub changes: Vec<UiDbChange>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UiDbChange {
    pub seq: u64,
    pub data: Arc<str>,
    pub at: Ms,
}

/// What an API token may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiRole {
    /// Everything the admin token may.
    Admin,
    /// Only reading (GET).
    Read,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiToken {
    pub id: String,
    pub name: String,
    pub role: ApiRole,
    /// The SHA-256 of the token's secret; the API never shows it.
    pub secret_hash: String,
    pub created_at: Ms,
}

/// How many recoveries the state keeps.
pub const RECOVERY_HISTORY: usize = 200;

/// One workload's recovery, or why it was left where it was.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recovery {
    pub at: Ms,
    pub workload_id: String,
    /// None: its node had been removed.
    pub from: Option<String>,
    /// None: it was left (see `reason`).
    pub to: Option<String>,
    pub reason: String,
}

/// What may happen to a workload when its node fails (plan 5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryKind {
    /// No data of its own: started again elsewhere once its node is fenced.
    Stateless,
    /// Has volumes, which live on its node: it waits for the node.
    LocalData,
    /// Its volumes are all on shared storage (G11): it moves once its node is fenced, as a stateless one does.
    SharedStorage,
    /// `x-freightdock: {ha: false}`: stays until someone moves it.
    Pinned,
}

impl ClusterState {
    /// Whether manager `raft_id` joined with a manager token (the first manager made the cluster, so never did).
    pub fn manager_joined(&self, raft_id: u64) -> bool {
        let who = format!("manager:{raft_id}");
        self.join_tokens.values().any(|t| t.spent_by.as_deref() == Some(who.as_str()))
    }

    /// Gives `node_id` the lowest free overlay address if it has none (a full /24 leaves it without: off the mesh).
    fn give_overlay(&mut self, node_id: &str) {
        if self.nodes.get(node_id).is_none_or(|n| n.overlay.is_some()) {
            return;
        }
        let used: std::collections::BTreeSet<u8> = self.nodes.values().filter_map(|n| n.overlay).collect();
        let free = (1..=254u8).find(|h| !used.contains(h));
        if let Some(n) = self.nodes.get_mut(node_id) {
            n.overlay = free;
        }
    }

    /// The node's overlay address, `10.240.0.7`.
    pub fn node_address(&self, node_id: &str) -> Option<String> {
        let h = self.nodes.get(node_id)?.overlay?;
        Some(format!("{}.{}.0.{h}", CLUSTER_RANGE[0], CLUSTER_RANGE[1]))
    }

    pub fn node_by_cert(&self, cert_sha256: &str) -> Option<&Node> {
        self.nodes.values().find(|n| n.cert_sha256 == cert_sha256 || n.prev_cert_sha256.as_deref() == Some(cert_sha256))
    }

    /// Commands for `node_id` that are not finished, oldest first: what to send again when it connects.
    pub fn open_commands(&self, node_id: &str) -> Vec<&CommandRecord> {
        let mut v: Vec<_> = self.commands.values().filter(|c| c.node_id == node_id && !c.status.is_terminal()).collect();
        v.sort_by_key(|c| c.created_at);
        v
    }

    /// Commands still pending (never reached their node) whose deadline passed more than `grace` ago. The grace covers
    /// clock skew between manager and worker: the worker also refuses a command past its deadline, by its own clock,
    /// and the manager must not call a command expired while a worker with a slow clock may still start it.
    pub fn overdue(&self, now: Ms, grace: Ms) -> Vec<String> {
        self.commands.values().filter(|c| c.status == CommandStatus::Pending && now > c.deadline + grace).map(|c| c.id.clone()).collect()
    }

    /// The finished commands to drop, oldest first: those finished more than `keep_ms` before `now`, and past the
    /// newest `max` finished ones. Each workload's latest command stays, since its status is read from it; an open
    /// command always stays.
    pub fn prunable(&self, now: Ms, keep_ms: Ms, max: usize) -> Vec<String> {
        let mut latest: BTreeMap<&str, &CommandRecord> = BTreeMap::new();
        for c in self.commands.values() {
            if let Some(w) = c.op.workload_id()
                && latest.get(w).is_none_or(|l| (l.created_at, l.id.as_str()) <= (c.created_at, c.id.as_str()))
            {
                latest.insert(w, c);
            }
        }
        let finished: Vec<&CommandRecord> = self.commands.values().filter(|c| c.status.is_terminal()).collect();
        let mut over = finished.len().saturating_sub(max);
        let mut candidates: Vec<&CommandRecord> = finished.into_iter().filter(|c| c.op.workload_id().is_none_or(|w| latest.get(w).is_none_or(|l| l.id != c.id))).collect();
        candidates.sort_by(|a, b| (a.updated_at, &a.id).cmp(&(b.updated_at, &b.id)));
        let mut out = vec![];
        for c in candidates {
            if c.updated_at.saturating_add(keep_ms) < now || over > 0 {
                over = over.saturating_sub(1);
                out.push(c.id.clone());
            }
        }
        out
    }

    /// Whether `exposures` may be stack `name`'s (with `services`): each to one of its services, over tcp or udp, on a
    /// pool address, at a port that is not the host's own, and no address, port and protocol another stack's or twice.
    fn check_exposures(&self, name: &str, services: &BTreeMap<String, ServiceSpec>, exposures: &[Exposure]) -> Result<(), Rejected> {
        let mut taken: std::collections::BTreeSet<(&str, u16, &str)> = self.stacks.values().filter(|o| o.name != name).flat_map(|o| &o.exposures).map(|e| (e.address.as_str(), e.port, e.protocol.as_str())).collect();
        for e in exposures {
            if !services.contains_key(&e.service) || !matches!(e.protocol.as_str(), "tcp" | "udp") || e.container_port == 0 || e.port == 0 || freightdock_proto::RESERVED_PORTS.contains(&u32::from(e.port)) {
                return Err(Rejected::BadExposure);
            }
            if !self.floating.contains_key(&e.address) {
                return Err(Rejected::FloatingUnknown);
            }
            if !taken.insert((e.address.as_str(), e.port, e.protocol.as_str())) {
                return Err(Rejected::ExposureTaken);
            }
        }
        Ok(())
    }

    /// The workloads a stack of these services has: (id, service, slot), in order.
    pub fn wanted(stack: &str, services: &BTreeMap<String, ServiceSpec>) -> Vec<(String, String, u32)> {
        services.iter().flat_map(|(svc, spec)| (1..=spec.replicas).map(move |slot| (workload_id(stack, svc, slot), svc.clone(), slot))).collect()
    }

    /// Whether the workload is to run: its stack is not stopped.
    /// The API token whose secret hashes to `secret_hash`.
    pub fn api_token(&self, secret_hash: &str) -> Option<&ApiToken> {
        self.api_tokens.values().find(|t| t.secret_hash == secret_hash)
    }

    /// The workload's recovery kind, from its stack's service (a workload's own spec has no `ha`).
    pub fn recovery_kind(&self, w: &Workload) -> RecoveryKind {
        let sp = self.stacks.get(&w.stack).and_then(|s| s.services.get(&w.service));
        if sp.is_some_and(|sp| !sp.ha) {
            RecoveryKind::Pinned
        } else if w.spec.volumes.is_empty() {
            RecoveryKind::Stateless
        } else if w.spec.volumes.iter().all(|v| self.shared_volume(&w.stack, v).is_some()) {
            RecoveryKind::SharedStorage
        } else {
            RecoveryKind::LocalData
        }
    }

    /// The stack's volume on shared storage that `volume` (`source:target[:mode]`) names, if it is one.
    pub fn shared_volume(&self, stack: &str, volume: &str) -> Option<(&str, &StackVolume)> {
        let src = volume.split(':').next()?;
        self.stacks.get(stack)?.volumes.get_key_value(src).map(|(k, v)| (k.as_str(), v))
    }

    /// What the worker needs to bind the workload's volumes on shared storage.
    pub fn storage_mounts(&self, w: &Workload) -> Vec<StorageMount> {
        w.spec
            .volumes
            .iter()
            .filter_map(|v| self.shared_volume(&w.stack, v))
            .filter_map(|(name, v)| {
                let st = self.storage.get(&v.storage)?;
                Some(StorageMount { volume: name.to_string(), storage: st.name.clone(), driver: st.driver, source: st.source.clone(), options: st.options.clone(), dir: format!("{}/{name}", w.stack) })
            })
            .collect()
    }

    /// The workload's volumes for one writer, as leases are named (`stack/volume`).
    pub fn exclusive_volumes(&self, w: &Workload) -> Vec<String> {
        w.spec.volumes.iter().filter_map(|v| self.shared_volume(&w.stack, v)).filter(|(_, v)| v.access == Access::One).map(|(name, _)| format!("{}/{name}", w.stack)).collect()
    }

    /// The workloads of `stack` that use its volume `name`.
    pub fn volume_users<'a>(&'a self, stack: &'a str, name: &'a str) -> impl Iterator<Item = &'a Workload> + 'a {
        self.workloads.values().filter(move |w| w.stack == stack && w.spec.volumes.iter().any(|v| v.split(':').next() == Some(name)))
    }

    /// Whether `node` may hold a volume another node holds now: that one is fenced (or back, fenced, awaiting
    /// authorization) or gone.
    fn may_take_over(&self, holder: &str) -> bool {
        self.nodes.get(holder).is_none_or(|n| matches!(n.fencing.state, FenceState::Fenced | FenceState::AwaitingAuthorization))
    }

    /// Whether a moved workload still waits for its old node: one heard (not fenced, not removed) that has not said
    /// its copy is gone.
    pub fn waits_for_old(&self, w: &Workload) -> bool {
        w.leaving.as_ref().and_then(|n| self.nodes.get(n)).is_some_and(|n| !matches!(n.fencing.state, FenceState::Fenced | FenceState::AwaitingAuthorization))
    }

    pub fn should_run(&self, w: &Workload) -> bool {
        self.stacks.get(&w.stack).is_some_and(|s| !s.stopped)
    }

    /// The workloads placed on `node_id`.
    pub fn workloads_on<'a>(&'a self, node_id: &'a str) -> impl Iterator<Item = &'a Workload> + 'a {
        self.workloads.values().filter(move |w| w.node_id.as_deref() == Some(node_id))
    }

    /// Workloads with an open command: the reconcile leaves them alone until it ends.
    pub fn busy_workloads(&self) -> std::collections::BTreeSet<String> {
        self.commands.values().filter(|c| !c.status.is_terminal()).filter_map(|c| c.op.workload_id().map(str::to_string)).collect()
    }

    /// Whether the token of `secret_hash` may be spent now on a node of this kind.
    fn spendable(&self, secret_hash: &str, for_manager: bool, at: Ms) -> Result<(), Rejected> {
        let t = self.join_tokens.get(secret_hash).ok_or(Rejected::TokenUnknown)?;
        if t.for_manager != for_manager {
            return Err(Rejected::TokenWrongKind);
        }
        if t.spent_by.is_some() {
            return Err(Rejected::TokenSpent);
        }
        if at > t.expires_at {
            return Err(Rejected::TokenExpired);
        }
        Ok(())
    }

    /// Checks an entry against the state without applying it, so a proposal that would be refused is never written.
    pub fn check(&self, entry: &Entry) -> Result<(), Rejected> {
        self.clone().apply_at(self.applied + 1, entry)
    }

    /// Applies the entry at log `index`. A refused entry changes nothing (but its index still counts as applied, as a
    /// replicated log carries refused entries too).
    pub fn apply_at(&mut self, index: u64, entry: &Entry) -> Result<(), Rejected> {
        self.applied = index;
        if self.cluster_id.is_none() && !matches!(entry, Entry::ClusterCreated { .. } | Entry::StateRestored { .. }) {
            return Err(Rejected::NoCluster);
        }
        match entry {
            Entry::ClusterCreated { cluster_id, .. } => {
                if self.cluster_id.is_none() {
                    self.cluster_id = Some(cluster_id.clone());
                }
            }
            Entry::StateRestored { state, at } => {
                if self.cluster_id.is_some() || state.cluster_id.is_none() {
                    return Err(Rejected::NotEmpty);
                }
                *self = (**state).clone();
                self.applied = index;
                for c in self.commands.values_mut().filter(|c| !c.status.is_terminal()) {
                    c.status = CommandStatus::Expired;
                    c.message = "restored from a backup: not sent".into();
                    c.updated_at = *at;
                }
            }
            Entry::JoinTokenIssued { secret_hash, expires_at, for_manager, .. } => {
                self.join_tokens.insert(secret_hash.clone(), JoinToken { expires_at: *expires_at, spent_by: None, for_manager: *for_manager });
            }
            Entry::NodeEnrolled { node_id, hostname, secret_hash, cert_sha256, at } => {
                self.spendable(secret_hash, false, *at)?;
                if self.nodes.contains_key(node_id) {
                    return Err(Rejected::NodeExists);
                }
                self.join_tokens.get_mut(secret_hash).expect("checked").spent_by = Some(node_id.clone());
                self.nodes.insert(node_id.clone(), Node { id: node_id.clone(), hostname: hostname.clone(), cert_sha256: cert_sha256.clone(), prev_cert_sha256: None, enrolled_at: *at, report: None, reported_at: None, labels: BTreeMap::new(), fencing: Fencing::default(), recovered: false, overlay: None });
                self.give_overlay(node_id);
            }
            Entry::NodeReported { node_id, report, at } => {
                let n = self.nodes.get_mut(node_id).ok_or(Rejected::NodeUnknown)?;
                n.hostname = report.hostname.clone();
                n.report = Some(report.clone());
                n.reported_at = Some(*at);
            }
            Entry::ManagerJoined { raft_id, secret_hash, at, .. } => {
                self.spendable(secret_hash, true, *at)?;
                self.join_tokens.get_mut(secret_hash).expect("checked").spent_by = Some(format!("manager:{raft_id}"));
            }
            Entry::NodeFencing { node_id, state, provider, message, at } => {
                let n = self.nodes.get_mut(node_id).ok_or(Rejected::NodeUnknown)?;
                if !n.fencing.state.allows(*state) || *state == FenceState::Authorized {
                    return Err(Rejected::FenceStep);
                }
                n.fencing = Fencing { state: *state, since: *at, provider: provider.clone(), message: message.clone() };
                if matches!(state, FenceState::None | FenceState::Requested) {
                    n.recovered = false;
                }
            }
            Entry::NodeAuthorized { node_id, by, at } => {
                let n = self.nodes.get_mut(node_id).ok_or(Rejected::NodeUnknown)?;
                if !n.fencing.state.allows(FenceState::Authorized) {
                    return Err(Rejected::FenceStep);
                }
                n.fencing = Fencing { state: FenceState::Authorized, since: *at, provider: by.clone(), message: format!("authorized by {by}") };
            }
            Entry::NodeLabelsSet { node_id, labels, .. } => {
                self.nodes.get_mut(node_id).ok_or(Rejected::NodeUnknown)?.labels = labels.clone();
            }
            Entry::NodeRemoved { node_id, .. } => {
                self.nodes.remove(node_id).ok_or(Rejected::NodeUnknown)?;
                for w in self.workloads.values_mut().filter(|w| w.node_id.as_ref() == Some(node_id)) {
                    w.node_id = None;
                }
                for c in self.commands.values_mut().filter(|c| &c.node_id == node_id && !c.status.is_terminal()) {
                    c.status = CommandStatus::Rejected;
                    c.message = "the node was removed".into();
                }
                for f in self.floating.values_mut().filter(|f| f.holder.as_ref() == Some(node_id)) {
                    f.holder = None;
                }
                self.leases.retain(|_, l| l.node_id != *node_id);
            }
            Entry::FloatingAdded { address, at } => {
                if !floating_ok(address) {
                    return Err(Rejected::BadAddress);
                }
                if self.floating.contains_key(address) {
                    return Err(Rejected::FloatingExists);
                }
                self.floating.insert(address.clone(), Floating { holder: None, since: *at });
            }
            Entry::FloatingPoolAdded { addresses, at } => {
                if !addresses.iter().all(|a| floating_ok(a)) {
                    return Err(Rejected::BadAddress);
                }
                for a in addresses {
                    self.floating.entry(a.clone()).or_insert(Floating { holder: None, since: *at });
                }
            }
            Entry::FloatingRemoved { address, .. } => {
                if !self.floating.contains_key(address) {
                    return Err(Rejected::FloatingUnknown);
                }
                if self.stacks.values().any(|s| s.exposures.iter().any(|e| e.address == *address)) {
                    return Err(Rejected::FloatingInUse);
                }
                self.floating.remove(address);
            }
            Entry::FloatingHeld { address, node_id, at } => {
                let Some(f) = self.floating.get(address) else { return Err(Rejected::FloatingUnknown) };
                if let Some(n) = node_id {
                    let node = self.nodes.get(n).ok_or(Rejected::NodeUnknown)?;
                    if node.fencing.state != FenceState::None {
                        return Err(Rejected::FenceStep);
                    }
                }
                let free = match f.holder.as_ref().and_then(|h| self.nodes.get(h).map(|n| (h, n))) {
                    None => true,
                    Some((h, n)) => Some(h) == node_id.as_ref() || matches!(n.fencing.state, FenceState::Fenced | FenceState::AwaitingAuthorization),
                };
                if !free {
                    return Err(Rejected::FenceStep);
                }
                self.floating.insert(address.clone(), Floating { holder: node_id.clone(), since: *at });
            }
            Entry::NodeAddressed { node_id } => {
                if !self.nodes.contains_key(node_id) {
                    return Err(Rejected::NodeUnknown);
                }
                self.give_overlay(node_id);
            }
            Entry::NodeCertRenewed { node_id, cert_sha256, .. } => {
                let n = self.nodes.get_mut(node_id).ok_or(Rejected::NodeUnknown)?;
                if n.cert_sha256 != *cert_sha256 {
                    n.prev_cert_sha256 = Some(std::mem::replace(&mut n.cert_sha256, cert_sha256.clone()));
                }
            }
            Entry::CommandCreated { command } => {
                if !self.nodes.contains_key(&command.node_id) {
                    return Err(Rejected::NodeUnknown);
                }
                if self.commands.contains_key(&command.id) {
                    return Err(Rejected::CommandExists);
                }
                if let Some(w) = command.op.workload_id()
                    && self.busy_workloads().contains(w)
                {
                    return Err(Rejected::WorkloadBusy);
                }
                self.commands.insert(command.id.clone(), command.clone());
            }
            Entry::StackDeployed { name, compose, services, placements, at, nodes, exposures: given, volumes, options, stopped } => {
                if !valid_stack_name(name) || !services.keys().all(|s| valid_service_name(s)) || services.values().any(|s| !s.name.is_empty() && !valid_container_name(&s.name)) {
                    return Err(Rejected::BadName);
                }
                // A container name is one workload's in the cluster: Docker's names are per node, and a workload may
                // land on any node.
                let taken: std::collections::BTreeSet<String> = self.workloads.values().filter(|w| w.stack != *name).map(Workload::container_name).collect();
                let mut mine = std::collections::BTreeSet::new();
                for (id, svc, slot) in Self::wanted(name, services) {
                    let w = Workload { id, stack: name.clone(), service: svc.clone(), slot, node_id: None, spec: services[&svc].for_workload(slot), generation: 0, host: None, leaving: None, attempts: None };
                    let n = w.container_name();
                    if taken.contains(&n) || !mine.insert(n) {
                        return Err(Rejected::NameTaken);
                    }
                }
                for (vol, v) in volumes {
                    if !self.storage.contains_key(&v.storage) {
                        return Err(Rejected::StorageUnknown);
                    }
                    let users: u32 = services.values().filter(|sp| sp.volumes.iter().any(|x| x.split(':').next() == Some(vol.as_str()))).map(|sp| sp.replicas).sum();
                    if v.access == Access::One && users > 1 {
                        return Err(Rejected::VolumeShared);
                    }
                }
                if nodes.iter().any(|n| !self.nodes.contains_key(n)) {
                    return Err(Rejected::NodeUnknown);
                }
                if let Some(list) = given {
                    self.check_exposures(name, services, list)?;
                }
                let wanted = Self::wanted(name, services);
                // Checked whole first, so a refusal changes nothing.
                for (id, _, _) in &wanted {
                    match placements.get(id) {
                        Some(node) if !self.nodes.contains_key(node) => return Err(Rejected::NodeUnknown),
                        Some(_) => {}
                        None if self.workloads.get(id).and_then(|w| w.node_id.as_ref()).is_some() => {}
                        None => return Err(Rejected::Unplaced),
                    }
                }
                let keep: std::collections::BTreeSet<&String> = wanted.iter().map(|w| &w.0).collect();
                self.workloads.retain(|id, w| w.stack != *name || keep.contains(id));
                for (id, svc, slot) in &wanted {
                    let spec = services[svc].for_workload(*slot);
                    match self.workloads.get_mut(id) {
                        Some(w) => {
                            // A placement for a placed workload moves it: its old node's reconcile removes the
                            // container there, its new node's runs one.
                            if let Some(n) = placements.get(id) {
                                w.place(n);
                            }
                            if w.spec != spec {
                                w.spec = spec;
                                w.generation = index;
                            }
                        }
                        None => {
                            self.workloads.insert(id.clone(), Workload { id: id.clone(), stack: name.clone(), service: svc.clone(), slot: *slot, node_id: placements.get(id).cloned(), spec, generation: index, host: None, leaving: None, attempts: None });
                        }
                    }
                }
                // Addresses (G9): the stack's /24 and each workload's host, the lowest free, kept once given. A full
                // range (254 stacks, 253 workloads each) leaves the rest without one: they run off the overlay.
                let net = self.stacks.get(name).and_then(|s| s.net).or_else(|| {
                    let used: std::collections::BTreeSet<u8> = self.stacks.values().filter_map(|s| s.net).collect();
                    (1..=254u8).find(|n| !used.contains(n))
                });
                if net.is_some() {
                    let mut used: std::collections::BTreeSet<u8> = self.workloads.values().filter(|w| w.stack == *name).filter_map(|w| w.host).collect();
                    for w in self.workloads.values_mut().filter(|w| w.stack == *name && w.host.is_none()) {
                        w.host = (2..=254u8).find(|h| !used.contains(h));
                        used.extend(w.host);
                        // A container made before it had one is made again, on the overlay.
                        w.generation = index;
                    }
                }
                let created_at = self.stacks.get(name).map_or(*at, |s| s.created_at);
                // An exposure goes with its service, unless the deploy gave the list.
                let exposures = match given {
                    Some(list) => list.clone(),
                    None => self.stacks.get(name).map(|s| s.exposures.iter().filter(|e| services.contains_key(&e.service)).cloned().collect()).unwrap_or_default(),
                };
                self.stacks.insert(name.clone(), Stack { name: name.clone(), compose: compose.clone(), services: services.clone(), stopped: *stopped, created_at, updated_at: *at, net, exposures, nodes: nodes.clone(), volumes: volumes.clone(), options: options.clone() });
            }
            Entry::StackPlaced { name, placements, at } => {
                if placements.keys().any(|id| self.workloads.get(id).is_none_or(|w| w.stack != *name)) {
                    return Err(Rejected::StackUnknown);
                }
                if placements.values().any(|n| !self.nodes.contains_key(n)) {
                    return Err(Rejected::NodeUnknown);
                }
                for (id, n) in placements {
                    self.workloads.get_mut(id).expect("checked").place(n);
                }
                if let Some(st) = self.stacks.get_mut(name) {
                    st.updated_at = *at;
                }
            }
            Entry::StackExposed { name, exposures, at } => {
                let st = self.stacks.get(name).ok_or(Rejected::StackUnknown)?;
                self.check_exposures(name, &st.services, exposures)?;
                let st = self.stacks.get_mut(name).expect("checked");
                st.exposures = exposures.clone();
                st.updated_at = *at;
            }
            Entry::StorageAdded { storage } => {
                if !storage.valid() {
                    return Err(Rejected::BadStorage);
                }
                if self.storage.contains_key(&storage.name) {
                    return Err(Rejected::StorageExists);
                }
                self.storage.insert(storage.name.clone(), storage.clone());
            }
            Entry::WorkloadLeft { workload_id, node_id, .. } => {
                if let Some(w) = self.workloads.get_mut(workload_id)
                    && w.leaving.as_deref() == Some(node_id.as_str())
                {
                    w.leaving = None;
                }
            }
            Entry::WorkloadAttempted { workload_id, generation, at } => {
                if let Some(w) = self.workloads.get_mut(workload_id) {
                    let runs = w.attempts.filter(|a| a.generation == *generation).map_or(1, |a| a.runs.saturating_add(1));
                    w.attempts = Some(Attempts { generation: *generation, runs, at: *at });
                }
            }
            Entry::WorkloadSettled { workload_id, .. } => {
                if let Some(w) = self.workloads.get_mut(workload_id) {
                    w.attempts = None;
                }
            }
            Entry::SettingsSet { settings, .. } => {
                if settings.check().is_err() {
                    return Err(Rejected::BadSettings);
                }
                self.settings = settings.clone();
            }
            Entry::RegistrySet { registry } => {
                if !crate::registry::valid_host(&registry.host) || registry.username.trim().is_empty() || registry.password.0.is_empty() || registry.username.len() > 255 {
                    return Err(Rejected::BadRegistry);
                }
                self.registries.insert(registry.host.clone(), registry.clone());
            }
            Entry::RegistryRemoved { host, .. } => {
                self.registries.remove(host).ok_or(Rejected::RegistryUnknown)?;
            }
            Entry::ChannelSet { channel } => {
                if !crate::notify::valid_channel(channel) {
                    return Err(Rejected::BadChannel);
                }
                let mut c = channel.clone();
                if let Some(old) = self.notifications.get(&c.name) {
                    c.last_sent_at = old.last_sent_at;
                    c.last_error = old.last_error.clone();
                }
                self.notifications.insert(c.name.clone(), c);
            }
            Entry::ChannelRemoved { name, .. } => {
                self.notifications.remove(name).ok_or(Rejected::ChannelUnknown)?;
            }
            Entry::ChannelOutcome { name, error, at } => {
                if let Some(c) = self.notifications.get_mut(name) {
                    match error {
                        None => {
                            c.last_sent_at = Some(*at);
                            c.last_error = None;
                        }
                        Some(e) => c.last_error = Some(e.clone()),
                    }
                }
            }
            Entry::CephSetupStarted { setup } => {
                if self.ceph_setup.as_ref().is_some_and(|c| c.state == crate::ceph::SetupState::Running) {
                    return Err(Rejected::CephSetupBusy);
                }
                if !setup.valid() || self.storage.contains_key(&setup.storage) || !setup.hosts.iter().all(|h| self.nodes.contains_key(&h.node_id)) {
                    return Err(Rejected::BadCephSetup);
                }
                self.ceph_setup = Some(setup.clone());
            }
            Entry::CephSetupStep { index, state, command_id, message, learned, at } => {
                let c = self.ceph_setup.as_mut().filter(|c| c.state == crate::ceph::SetupState::Running && *index < c.steps.len()).ok_or(Rejected::CephSetupUnknown)?;
                c.record(*index, *state, command_id.clone(), message, learned.as_ref(), *at);
            }
            Entry::CephSetupRetried { at } => {
                let c = self.ceph_setup.as_mut().filter(|c| c.state == crate::ceph::SetupState::Failed).ok_or(Rejected::CephSetupUnknown)?;
                c.retry(*at);
            }
            Entry::CephSetupCleared { .. } => {
                if self.ceph_setup.take().is_none() {
                    return Err(Rejected::CephSetupUnknown);
                }
            }
            Entry::StorageKeysSet { name, ceph, .. } => {
                let st = self.storage.get(name).ok_or(Rejected::StorageUnknown)?;
                let mut next = st.clone();
                next.ceph = Some(ceph.clone());
                if !next.valid() {
                    return Err(Rejected::BadStorage);
                }
                self.storage.insert(name.clone(), next);
            }
            Entry::StorageRemoved { name, .. } => {
                if !self.storage.contains_key(name) {
                    return Err(Rejected::StorageUnknown);
                }
                if self.stacks.values().any(|st| st.volumes.values().any(|v| v.storage == *name)) {
                    return Err(Rejected::StorageInUse);
                }
                self.storage.remove(name);
            }
            Entry::LeaseTaken { volume, node_id, at } => {
                if !self.nodes.contains_key(node_id) {
                    return Err(Rejected::NodeUnknown);
                }
                let (stack, vol) = volume.split_once('/').ok_or(Rejected::StorageUnknown)?;
                let v = self.stacks.get(stack).and_then(|st| st.volumes.get(vol)).ok_or(Rejected::StorageUnknown)?;
                if v.access != Access::One {
                    return Err(Rejected::BadStorage);
                }
                match self.leases.get(volume) {
                    Some(l) if l.node_id == *node_id => {}
                    Some(l) if !self.may_take_over(&l.node_id) => return Err(Rejected::LeaseHeld),
                    _ => {
                        self.leases.insert(volume.clone(), Lease { node_id: node_id.clone(), since: *at });
                    }
                }
            }
            Entry::LeaseReleased { volume, node_id, .. } => match self.leases.get(volume) {
                Some(l) if l.node_id == *node_id => {
                    self.leases.remove(volume);
                }
                Some(_) => return Err(Rejected::LeaseHeld),
                None => {}
            },
            Entry::StackStopped { name, stopped, at } => {
                let s = self.stacks.get_mut(name).ok_or(Rejected::StackUnknown)?;
                s.stopped = *stopped;
                s.updated_at = *at;
            }
            Entry::WorkloadsRecovered { from, moves, left, at } => {
                if let Some(f) = from {
                    let n = self.nodes.get(f).ok_or(Rejected::NodeUnknown)?;
                    if !matches!(n.fencing.state, FenceState::Fenced | FenceState::AwaitingAuthorization) {
                        return Err(Rejected::FenceStep);
                    }
                }
                for (id, to) in moves {
                    let w = self.workloads.get(id).ok_or(Rejected::NotRecoverable)?;
                    if w.node_id != *from || (from.is_some() && !matches!(self.recovery_kind(w), RecoveryKind::Stateless | RecoveryKind::SharedStorage)) {
                        return Err(Rejected::NotRecoverable);
                    }
                    if !self.nodes.contains_key(to) || Some(to) == from.as_ref() {
                        return Err(Rejected::NodeUnknown);
                    }
                }
                for (id, to) in moves {
                    self.workloads.get_mut(id).expect("checked").place(to);
                    // A command still open for it on the old node is done with.
                    for c in self.commands.values_mut().filter(|c| !c.status.is_terminal() && c.op.workload_id() == Some(id.as_str())) {
                        c.status = CommandStatus::Rejected;
                        c.message = "the workload moved".into();
                    }
                    self.recoveries.push_back(Recovery { at: *at, workload_id: id.clone(), from: from.clone(), to: Some(to.clone()), reason: String::new() });
                }
                for (id, why) in left {
                    self.recoveries.push_back(Recovery { at: *at, workload_id: id.clone(), from: from.clone(), to: None, reason: why.clone() });
                }
                while self.recoveries.len() > RECOVERY_HISTORY {
                    self.recoveries.pop_front();
                }
                if let Some(n) = from.as_ref().and_then(|f| self.nodes.get_mut(f)) {
                    n.recovered = true;
                }
            }
            Entry::ApiTokenIssued { id, name, role, secret_hash, at } => {
                if self.api_tokens.contains_key(id) || self.api_token(secret_hash).is_some() {
                    return Err(Rejected::TokenExists);
                }
                if name.trim().is_empty() || name.len() > 64 {
                    return Err(Rejected::BadName);
                }
                self.api_tokens.insert(id.clone(), ApiToken { id: id.clone(), name: name.clone(), role: *role, secret_hash: secret_hash.clone(), created_at: *at });
            }
            Entry::ApiTokenRevoked { id, .. } => {
                self.api_tokens.remove(id).ok_or(Rejected::TokenUnknown)?;
            }
            Entry::UiDbSnapshot { head, data, .. } => {
                if *head != self.ui_db.head {
                    return Err(Rejected::UiDbStale);
                }
                self.ui_db.snapshot = Some(data.clone());
                self.ui_db.snapshot_head = *head;
                self.ui_db.changes.retain(|c| c.seq > *head);
            }
            Entry::UiDbChange { seq, data, at } => {
                if *seq != self.ui_db.head + 1 || self.ui_db.snapshot.is_none() {
                    return Err(Rejected::UiDbStale);
                }
                self.ui_db.head = *seq;
                self.ui_db.changes.push(UiDbChange { seq: *seq, data: data.clone(), at: *at });
            }
            Entry::StackRemoved { name, .. } => {
                self.stacks.remove(name).ok_or(Rejected::StackUnknown)?;
                self.workloads.retain(|_, w| w.stack != *name);
            }
            Entry::CommandsPruned { ids, .. } => {
                for id in ids {
                    if self.commands.get(id).is_some_and(|c| c.status.is_terminal()) {
                        self.commands.remove(id);
                    }
                }
            }
            Entry::CommandStatus { command_id, status, message, result_json, at } => {
                let c = self.commands.get_mut(command_id).ok_or(Rejected::CommandUnknown)?;
                // A status only moves forward, and a finished command stays as it finished: a late or repeated report
                // (a resend after a reconnect, another manager's copy) changes nothing.
                if c.status.is_terminal() || *status <= c.status {
                    return Ok(());
                }
                c.status = *status;
                c.message = message.clone();
                if !result_json.is_empty() {
                    c.result_json = result_json.clone();
                }
                c.updated_at = *at;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn cluster() -> ClusterState {
        let mut s = ClusterState::default();
        s.apply_at(1, &Entry::ClusterCreated { cluster_id: "c1".into(), at: 0 }).unwrap();
        s
    }

    fn with_token(s: &mut ClusterState, hash: &str, expires_at: Ms) {
        let i = s.applied + 1;
        s.apply_at(i, &Entry::JoinTokenIssued { secret_hash: hash.into(), expires_at, at: 0, for_manager: false }).unwrap();
    }

    fn enrol(node: &str, hash: &str, at: Ms) -> Entry {
        Entry::NodeEnrolled { node_id: node.into(), hostname: "h".into(), secret_hash: hash.into(), cert_sha256: format!("cert-{node}"), at }
    }

    pub fn command(id: &str, node: &str) -> CommandRecord {
        CommandRecord { id: id.into(), node_id: node.into(), op: Op::Ping, config_version: 0, created_at: 1, deadline: 100, status: CommandStatus::Pending, message: String::new(), result_json: String::new(), updated_at: 1 }
    }

    fn status(id: &str, st: CommandStatus) -> Entry {
        Entry::CommandStatus { command_id: id.into(), status: st, message: format!("{st:?}"), result_json: String::new(), at: 2 }
    }

    #[test]
    fn nothing_happens_before_the_cluster_is_made() {
        let mut s = ClusterState::default();
        assert_eq!(s.apply_at(1, &Entry::JoinTokenIssued { secret_hash: "t".into(), expires_at: 1, at: 0, for_manager: false }), Err(Rejected::NoCluster));
        assert!(s.join_tokens.is_empty());
    }

    #[test]
    fn a_token_enrols_one_node_once() {
        let mut s = cluster();
        with_token(&mut s, "t", 1000);
        s.apply_at(3, &enrol("n1", "t", 10)).unwrap();
        assert_eq!(s.node_by_cert("cert-n1").map(|n| n.id.as_str()), Some("n1"));
        assert_eq!(s.apply_at(4, &enrol("n2", "t", 11)), Err(Rejected::TokenSpent), "the second racer");
        assert!(!s.nodes.contains_key("n2"));
    }

    #[test]
    fn an_expired_or_unknown_token_enrols_nothing() {
        let mut s = cluster();
        with_token(&mut s, "t", 1000);
        assert_eq!(s.check(&enrol("n1", "t", 1001)), Err(Rejected::TokenExpired));
        assert_eq!(s.check(&enrol("n1", "nope", 10)), Err(Rejected::TokenUnknown));
        assert!(s.nodes.is_empty());
    }

    #[test]
    fn a_manager_token_makes_a_manager_once_and_never_a_worker() {
        let mut s = cluster();
        s.apply_at(2, &Entry::JoinTokenIssued { secret_hash: "m".into(), expires_at: 1000, at: 0, for_manager: true }).unwrap();
        with_token(&mut s, "w", 1000);
        assert_eq!(s.check(&enrol("n1", "m", 10)), Err(Rejected::TokenWrongKind));
        let joined = |hash: &str| Entry::ManagerJoined { raft_id: 9, addr: "a:1".into(), secret_hash: hash.into(), at: 10 };
        assert_eq!(s.check(&joined("w")), Err(Rejected::TokenWrongKind));
        assert!(!s.manager_joined(9));
        s.apply_at(4, &joined("m")).unwrap();
        assert!(s.manager_joined(9), "the join is on record, so it may become a voter");
        assert!(!s.manager_joined(8));
        assert_eq!(s.join_tokens["m"].spent_by.as_deref(), Some("manager:9"));
        assert_eq!(s.check(&joined("m")), Err(Rejected::TokenSpent));
    }

    #[test]
    fn every_stack_gets_a_subnet_and_every_workload_an_address_kept_until_it_goes() {
        let mut s = with_nodes(&["n1", "n2"]);
        let addr = |s: &ClusterState, id: &str| s.address(id);
        next(&mut s, &deploy("a", &[("web", spec("nginx", 2))], &[("a.web.1", "n1"), ("a.web.2", "n2")])).unwrap();
        next(&mut s, &deploy("b", &[("db", spec("pg", 1))], &[("b.db.1", "n1")])).unwrap();
        assert_eq!(s.subnet("a").as_deref(), Some("10.240.1.0/24"));
        assert_eq!(s.subnet("b").as_deref(), Some("10.240.2.0/24"));
        assert_eq!((addr(&s, "a.web.1").as_deref(), addr(&s, "a.web.2").as_deref()), (Some("10.240.1.2"), Some("10.240.1.3")), ".1 is the gateway");
        assert_eq!(addr(&s, "b.db.1").as_deref(), Some("10.240.2.2"));

        // Scaled down and up again: a new replica takes a free address, the others keep theirs.
        next(&mut s, &deploy("a", &[("web", spec("nginx", 1))], &[])).unwrap();
        next(&mut s, &deploy("a", &[("web", spec("nginx", 3))], &[("a.web.2", "n1"), ("a.web.3", "n2")])).unwrap();
        assert_eq!(addr(&s, "a.web.1").as_deref(), Some("10.240.1.2"));
        let mut all: Vec<_> = ["a.web.1", "a.web.2", "a.web.3"].iter().map(|w| addr(&s, w).unwrap()).collect();
        all.dedup();
        assert_eq!(all.len(), 3, "never two on one address: {all:?}");

        // Moved by recovery: the same address.
        let before = addr(&s, "b.db.1");
        next(&mut s, &deploy("b", &[("db", spec("pg", 1))], &[("b.db.1", "n2")])).unwrap();
        assert_eq!(addr(&s, "b.db.1"), before);

        // A stack that goes frees its subnet for the next.
        next(&mut s, &Entry::StackRemoved { name: "a".into(), at: 60 }).unwrap();
        next(&mut s, &deploy("c", &[("x", spec("i", 1))], &[("c.x.1", "n1")])).unwrap();
        assert_eq!(s.subnet("c").as_deref(), Some("10.240.1.0/24"));
    }

    #[test]
    fn finished_commands_are_pruned_past_an_age_or_a_count_but_each_workload_keeps_its_latest() {
        let mut s = cluster();
        with_token(&mut s, "t", 1000);
        s.apply_at(3, &enrol("n1", "t", 10)).unwrap();
        let mut i = 4;
        let mut add = |s: &mut ClusterState, id: &str, op: Op, at: Ms, st: CommandStatus| {
            let c = CommandRecord { op, created_at: at, updated_at: at, status: st, ..command(id, "n1") };
            s.apply_at(i, &Entry::CommandCreated { command: c }).unwrap();
            i += 1;
        };
        let run = |w: &str| Op::StopContainer { workload_id: w.into(), timeout_s: 0 };
        add(&mut s, "old-ping", Op::Ping, 100, CommandStatus::Succeeded);
        add(&mut s, "old-w1", run("a.web.1"), 100, CommandStatus::Succeeded);
        add(&mut s, "new-w1", run("a.web.1"), 200, CommandStatus::Failed);
        add(&mut s, "only-w2", run("a.web.2"), 100, CommandStatus::Succeeded);
        add(&mut s, "open", Op::Ping, 100, CommandStatus::Pending);
        add(&mut s, "recent", Op::Ping, 950, CommandStatus::Succeeded);
        let mut p = s.prunable(1000, 500, 100);
        p.sort();
        assert_eq!(p, vec!["old-ping", "old-w1"], "old and finished; a workload's latest and an open one stay");
        assert_eq!(s.prunable(1000, 5000, 4), vec!["old-ping"], "5 finished, 4 kept: the oldest goes, sparing each workload's latest");
        s.apply_at(20, &Entry::CommandsPruned { ids: vec!["old-ping".into(), "open".into(), "gone".into()], at: 1000 }).unwrap();
        assert!(!s.commands.contains_key("old-ping"));
        assert!(s.commands.contains_key("open"), "an open command is never pruned, whatever the list says");
    }

    #[test]
    fn a_container_name_is_one_workload_s_in_the_cluster() {
        let mut s = with_nodes(&["n1"]);
        let named = |n: &str, replicas| ServiceSpec { name: n.into(), ..spec("x", replicas) };
        let a = deploy("a", &[("db", named("main-db", 1)), ("web", named("site", 2))], &[("a.db.1", "n1"), ("a.web.1", "n1"), ("a.web.2", "n1")]);
        next(&mut s, &a).unwrap();
        assert_eq!(s.workloads["a.db.1"].container_name(), "main-db");
        assert_eq!((s.workloads["a.web.1"].container_name(), s.workloads["a.web.2"].container_name()), ("site-1".to_string(), "site-2".to_string()));
        assert!(s.attach(&s.workloads["a.web.1"]).unwrap().hosts.contains(&format!("main-db:{}", s.address("a.db.1").unwrap())), "reached by it inside the stack");
        next(&mut s, &a).unwrap();
        assert_eq!(next(&mut s, &deploy("b", &[("db", named("main-db", 1))], &[("b.db.1", "n1")])), Err(Rejected::NameTaken));
        assert_eq!(next(&mut s, &deploy("b", &[("db", named("site-2", 1))], &[("b.db.1", "n1")])), Err(Rejected::NameTaken));
        next(&mut s, &deploy("c", &[("x", spec("x", 1))], &[("c.x.1", "n1")])).unwrap();
        assert_eq!(s.workloads["c.x.1"].container_name(), "c-x-1");
        assert_eq!(next(&mut s, &deploy("b", &[("db", named("c-x-1", 1))], &[("b.db.1", "n1")])), Err(Rejected::NameTaken), "another's own name");
        assert_eq!(next(&mut s, &deploy("b", &[("db", named("Bad_", 1))], &[("b.db.1", "n1")])), Err(Rejected::BadName));
    }

    #[test]
    fn a_stack_s_workloads_are_placed_again_only_on_nodes_there_are() {
        let mut s = with_nodes(&["n1", "n2"]);
        next(&mut s, &deploy("a", &[("web", spec("x", 1))], &[("a.web.1", "n1")])).unwrap();
        let g = s.workloads["a.web.1"].generation;
        let placed = |p: &[(&str, &str)]| Entry::StackPlaced { name: "a".into(), placements: p.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(), at: 9 };
        assert_eq!(next(&mut s, &placed(&[("a.web.1", "n9")])), Err(Rejected::NodeUnknown));
        assert_eq!(next(&mut s, &placed(&[("b.web.1", "n2")])), Err(Rejected::StackUnknown), "not a workload of the stack");
        next(&mut s, &placed(&[("a.web.1", "n2")])).unwrap();
        assert_eq!((s.workloads["a.web.1"].node_id.as_deref(), s.workloads["a.web.1"].generation), (Some("n2"), g), "moved, its spec as it was");
    }

    #[test]
    fn check_changes_nothing() {
        let mut s = cluster();
        with_token(&mut s, "t", 1000);
        let before = s.clone();
        s.check(&enrol("n1", "t", 10)).unwrap();
        assert_eq!(s, before);
    }

    #[test]
    fn a_command_status_only_moves_forward() {
        let mut s = cluster();
        with_token(&mut s, "t", 1000);
        s.apply_at(3, &enrol("n1", "t", 10)).unwrap();
        s.apply_at(4, &Entry::CommandCreated { command: command("c", "n1") }).unwrap();
        s.apply_at(5, &status("c", CommandStatus::Accepted)).unwrap();
        s.apply_at(6, &status("c", CommandStatus::Received)).unwrap();
        assert_eq!(s.commands["c"].status, CommandStatus::Accepted, "a late Received is ignored");
        s.apply_at(7, &status("c", CommandStatus::Succeeded)).unwrap();
        s.apply_at(8, &status("c", CommandStatus::Failed)).unwrap();
        assert_eq!(s.commands["c"].status, CommandStatus::Succeeded, "finished stays finished");
        assert!(s.open_commands("n1").is_empty());
    }

    #[test]
    fn a_command_needs_a_node_and_a_new_id() {
        let mut s = cluster();
        assert_eq!(s.check(&Entry::CommandCreated { command: command("c", "ghost") }), Err(Rejected::NodeUnknown));
        with_token(&mut s, "t", 1000);
        s.apply_at(3, &enrol("n1", "t", 10)).unwrap();
        s.apply_at(4, &Entry::CommandCreated { command: command("c", "n1") }).unwrap();
        assert_eq!(s.check(&Entry::CommandCreated { command: command("c", "n1") }), Err(Rejected::CommandExists));
        assert_eq!(s.open_commands("n1").len(), 1);
    }

    #[test]
    fn only_pending_commands_past_deadline_and_grace_are_overdue() {
        let mut s = cluster();
        with_token(&mut s, "t", 1000);
        s.apply_at(3, &enrol("n1", "t", 10)).unwrap();
        s.apply_at(4, &Entry::CommandCreated { command: command("a", "n1") }).unwrap();
        s.apply_at(5, &Entry::CommandCreated { command: command("b", "n1") }).unwrap();
        s.apply_at(6, &status("b", CommandStatus::Received)).unwrap();
        assert!(s.overdue(104, 5).is_empty(), "within the grace");
        assert_eq!(s.overdue(106, 5), vec!["a".to_string()], "b reached its node: the worker reports its end");
    }

    #[test]
    fn a_renewed_certificate_opens_and_the_one_before_it_still_does_until_the_next() {
        let mut s = cluster();
        with_token(&mut s, "t", 1000);
        s.apply_at(3, &enrol("n1", "t", 10)).unwrap();
        s.apply_at(4, &Entry::NodeCertRenewed { node_id: "n1".into(), cert_sha256: "cert-2".into(), at: 20 }).unwrap();
        assert_eq!(s.node_by_cert("cert-2").map(|n| n.id.as_str()), Some("n1"));
        assert_eq!(s.node_by_cert("cert-n1").map(|n| n.id.as_str()), Some("n1"), "a worker that failed to keep the new one is not locked out");
        s.apply_at(5, &Entry::NodeCertRenewed { node_id: "n1".into(), cert_sha256: "cert-3".into(), at: 30 }).unwrap();
        assert!(s.node_by_cert("cert-n1").is_none(), "two back opens nothing");
        assert!(s.node_by_cert("cert-2").is_some() && s.node_by_cert("cert-3").is_some());
        assert_eq!(s.apply_at(6, &Entry::NodeCertRenewed { node_id: "n9".into(), cert_sha256: "x".into(), at: 40 }), Err(Rejected::NodeUnknown));
        s.apply_at(7, &Entry::NodeRemoved { node_id: "n1".into(), at: 50 }).unwrap();
        assert!(s.node_by_cert("cert-2").is_none(), "nor any of a removed node's");
    }

    #[test]
    fn every_node_has_an_overlay_address_of_its_own_kept_for_its_life() {
        let mut s = with_nodes(&["n1", "n2"]);
        assert_eq!((s.node_address("n1").as_deref(), s.node_address("n2").as_deref()), (Some("10.240.0.1"), Some("10.240.0.2")));
        next(&mut s, &Entry::NodeRemoved { node_id: "n1".into(), at: 5 }).unwrap();
        with_token(&mut s, "t9", 1000);
        next(&mut s, &enrol("n3", "t9", 6)).unwrap();
        assert_eq!(s.node_address("n3").as_deref(), Some("10.240.0.1"), "the lowest free one");
        assert_eq!(s.node_address("n2").as_deref(), Some("10.240.0.2"), "the others keep theirs");
        // A node enrolled before G10 has none until it is given one.
        s.nodes.get_mut("n2").unwrap().overlay = None;
        next(&mut s, &Entry::NodeAddressed { node_id: "n2".into() }).unwrap();
        assert_eq!(s.node_address("n2").as_deref(), Some("10.240.0.2"));
        next(&mut s, &Entry::NodeAddressed { node_id: "n2".into() }).unwrap();
        assert_eq!(s.node_address("n2").as_deref(), Some("10.240.0.2"), "once given, kept");
    }

    #[test]
    fn removing_a_node_closes_its_commands() {
        let mut s = cluster();
        with_token(&mut s, "t", 1000);
        s.apply_at(3, &enrol("n1", "t", 10)).unwrap();
        s.apply_at(4, &Entry::CommandCreated { command: command("c", "n1") }).unwrap();
        s.apply_at(5, &Entry::NodeRemoved { node_id: "n1".into(), at: 20 }).unwrap();
        assert!(s.nodes.is_empty());
        assert_eq!(s.commands["c"].status, CommandStatus::Rejected);
        assert!(s.node_by_cert("cert-n1").is_none(), "its certificate opens nothing more");
    }

    pub fn spec(image: &str, replicas: u32) -> ServiceSpec {
        ServiceSpec { image: image.into(), command: vec![], env: Default::default(), labels: Default::default(), ports: vec![], volumes: vec![], replicas, limits: Default::default(), placement: Default::default(), ha: true, restart: Default::default(), name: String::new(), start_first: false }
    }

    pub fn with_nodes(nodes: &[&str]) -> ClusterState {
        let mut s = cluster();
        for (i, n) in nodes.iter().enumerate() {
            let hash = format!("t{i}");
            with_token(&mut s, &hash, 1000);
            let at = s.applied + 1;
            s.apply_at(at, &enrol(n, &hash, 10)).unwrap();
        }
        s
    }

    pub fn deploy(name: &str, services: &[(&str, ServiceSpec)], placements: &[(&str, &str)]) -> Entry {
        Entry::StackDeployed {
            name: name.into(),
            compose: "services: {}".into(),
            services: services.iter().map(|(k, v)| (k.to_string(), v.clone())).collect(),
            placements: placements.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            at: 50,
            nodes: vec![],
            exposures: None,
            volumes: Default::default(),
            options: Default::default(),
            stopped: false,
        }
    }

    pub fn next(s: &mut ClusterState, e: &Entry) -> Result<(), Rejected> {
        let i = s.applied + 1;
        s.apply_at(i, e)
    }

    #[test]
    fn a_stack_makes_one_workload_per_replica_where_it_was_placed() {
        let mut s = with_nodes(&["n1", "n2"]);
        next(&mut s, &deploy("web", &[("nginx", spec("nginx", 2))], &[("web.nginx.1", "n1"), ("web.nginx.2", "n2")])).unwrap();
        assert_eq!(s.workloads.len(), 2);
        assert_eq!(s.workloads["web.nginx.2"].node_id.as_deref(), Some("n2"));
        assert_eq!(s.workloads["web.nginx.1"].spec.replicas, 1, "a workload is one replica");
        assert!(s.should_run(&s.workloads["web.nginx.1"]));
        assert_eq!(s.workloads_on("n1").count(), 1);
    }

    #[test]
    fn a_stack_with_a_workload_left_unplaced_or_on_no_node_changes_nothing() {
        let s = with_nodes(&["n1"]);
        let before = s.clone();
        assert_eq!(s.check(&deploy("web", &[("a", spec("nginx", 2))], &[("web.a.1", "n1")])), Err(Rejected::Unplaced));
        assert_eq!(s.check(&deploy("web", &[("a", spec("nginx", 1))], &[("web.a.1", "ghost")])), Err(Rejected::NodeUnknown));
        assert_eq!(s.check(&deploy("Web", &[("a", spec("nginx", 1))], &[("Web.a.1", "n1")])), Err(Rejected::BadName));
        assert_eq!(s.check(&deploy("web", &[("a.b", spec("nginx", 1))], &[("web.a.b.1", "n1")])), Err(Rejected::BadName));
        assert_eq!(s, before);
    }

    #[test]
    fn deploying_again_keeps_placements_and_bumps_only_what_changed() {
        let mut s = with_nodes(&["n1", "n2"]);
        next(&mut s, &deploy("web", &[("a", spec("nginx:1", 2)), ("b", spec("redis", 1))], &[("web.a.1", "n1"), ("web.a.2", "n2"), ("web.b.1", "n1")])).unwrap();
        let g = s.workloads["web.b.1"].generation;
        // a's image changes and it shrinks to one; b stays as it was; with no placement given, nothing moves.
        next(&mut s, &deploy("web", &[("a", spec("nginx:2", 1)), ("b", spec("redis", 1))], &[])).unwrap();
        assert_eq!(s.workloads.keys().collect::<Vec<_>>(), vec!["web.a.1", "web.b.1"]);
        assert_eq!(s.workloads["web.a.1"].node_id.as_deref(), Some("n1"), "a placed workload stays");
        assert_eq!(s.workloads["web.a.1"].generation, s.applied, "its spec changed");
        assert_eq!(s.workloads["web.b.1"].generation, g, "b did not change");
    }

    #[test]
    fn a_stack_deployed_stopped_is_there_and_runs_nothing_until_it_is_started() {
        let mut s = with_nodes(&["n1"]);
        let Entry::StackDeployed { name, compose, services, placements, at, nodes, exposures, volumes, options, .. } = deploy("web", &[("a", spec("nginx", 1))], &[("web.a.1", "n1")]) else { unreachable!() };
        next(&mut s, &Entry::StackDeployed { name, compose, services, placements, at, nodes, exposures, volumes, options, stopped: true }).unwrap();
        assert!(s.stacks["web"].stopped && !s.should_run(&s.workloads["web.a.1"]), "placed, and not run");
        next(&mut s, &Entry::StackStopped { name: "web".into(), stopped: false, at: 60 }).unwrap();
        assert!(s.should_run(&s.workloads["web.a.1"]));
        let old = serde_json::json!({ "type": "stack_deployed", "name": "x", "compose": "", "services": {}, "placements": {}, "at": 1 });
        let Entry::StackDeployed { stopped, .. } = serde_json::from_value::<Entry>(old).unwrap() else { panic!() };
        assert!(!stopped, "an entry from before runs, as it did");
    }

    #[test]
    fn a_stack_stops_starts_and_goes() {
        let mut s = with_nodes(&["n1"]);
        next(&mut s, &deploy("web", &[("a", spec("nginx", 1))], &[("web.a.1", "n1")])).unwrap();
        next(&mut s, &Entry::StackStopped { name: "web".into(), stopped: true, at: 60 }).unwrap();
        assert!(!s.should_run(&s.workloads["web.a.1"]));
        next(&mut s, &deploy("web", &[("a", spec("nginx", 1))], &[])).unwrap();
        assert!(s.should_run(&s.workloads["web.a.1"]), "a deploy runs it again");
        next(&mut s, &Entry::StackRemoved { name: "web".into(), at: 70 }).unwrap();
        assert!(s.workloads.is_empty() && s.stacks.is_empty());
        assert_eq!(s.check(&Entry::StackStopped { name: "web".into(), stopped: true, at: 80 }), Err(Rejected::StackUnknown));
    }

    #[test]
    fn a_removed_node_leaves_its_workloads_unplaced_until_the_next_deploy() {
        let mut s = with_nodes(&["n1", "n2"]);
        next(&mut s, &deploy("web", &[("a", spec("nginx", 1))], &[("web.a.1", "n1")])).unwrap();
        next(&mut s, &Entry::NodeRemoved { node_id: "n1".into(), at: 60 }).unwrap();
        assert_eq!(s.workloads["web.a.1"].node_id, None);
        next(&mut s, &deploy("web", &[("a", spec("nginx", 1))], &[("web.a.1", "n2")])).unwrap();
        assert_eq!(s.workloads["web.a.1"].node_id.as_deref(), Some("n2"));
    }

    #[test]
    fn a_workload_takes_one_command_at_a_time() {
        let mut s = with_nodes(&["n1"]);
        let run = |id: &str| Entry::CommandCreated { command: CommandRecord { op: Op::StopContainer { workload_id: "web.a.1".into(), timeout_s: 0 }, ..command(id, "n1") } };
        next(&mut s, &run("c1")).unwrap();
        assert_eq!(s.check(&run("c2")), Err(Rejected::WorkloadBusy));
        next(&mut s, &status("c1", CommandStatus::Succeeded)).unwrap();
        next(&mut s, &run("c2")).unwrap();
        next(&mut s, &Entry::CommandCreated { command: command("p", "n1") }).unwrap();
        next(&mut s, &Entry::CommandCreated { command: command("q", "n1") }).expect("pings are about no workload");
    }

    #[test]
    fn a_placement_moves_a_placed_workload_and_placement_alone_makes_no_container_again() {
        let mut s = with_nodes(&["n1", "n2"]);
        next(&mut s, &deploy("web", &[("a", spec("nginx", 1))], &[("web.a.1", "n1")])).unwrap();
        let g = s.workloads["web.a.1"].generation;
        let mut pinned = spec("nginx", 1);
        pinned.ha = false;
        pinned.placement.constraints.push(Constraint { field: "node.id".into(), equal: true, value: "n2".into() });
        next(&mut s, &deploy("web", &[("a", pinned)], &[("web.a.1", "n2")])).unwrap();
        assert_eq!(s.workloads["web.a.1"].node_id.as_deref(), Some("n2"));
        assert_eq!(s.workloads["web.a.1"].generation, g, "the same container spec");
        let mut limited = spec("nginx", 1);
        limited.limits.memory_bytes = 1 << 20;
        next(&mut s, &deploy("web", &[("a", limited)], &[])).unwrap();
        assert_ne!(s.workloads["web.a.1"].generation, g, "a limit is the container's");
    }

    #[test]
    fn the_api_s_labels_go_over_the_worker_s() {
        let mut s = with_nodes(&["n1"]);
        let report = NodeReport { labels: [("zone".to_string(), "a".to_string()), ("ssd".to_string(), "true".to_string())].into(), ..Default::default() };
        next(&mut s, &Entry::NodeReported { node_id: "n1".into(), report, at: 1 }).unwrap();
        next(&mut s, &Entry::NodeLabelsSet { node_id: "n1".into(), labels: [("zone".to_string(), "b".to_string())].into(), at: 2 }).unwrap();
        let n = &s.nodes["n1"];
        assert_eq!(n.all_labels()["zone"], "b");
        assert_eq!(n.all_labels()["ssd"], "true");
        let c = |f: &str, eq: bool, v: &str| Constraint { field: f.into(), equal: eq, value: v.into() }.holds(n);
        assert!(c("node.labels.zone", true, "b") && c("node.labels.zone", false, "a") && c("node.labels.gpu", false, "x"));
        assert!(!c("node.labels.gpu", true, "x") && c("node.id", true, "n1"));
        assert_eq!(s.check(&Entry::NodeLabelsSet { node_id: "ghost".into(), labels: Default::default(), at: 3 }), Err(Rejected::NodeUnknown));
    }

    fn fence(s: &mut ClusterState, to: FenceState) -> Result<(), Rejected> {
        next(s, &Entry::NodeFencing { node_id: "n1".into(), state: to, provider: "self".into(), message: String::new(), at: 9 })
    }

    #[test]
    fn a_pool_range_is_written_three_ways_and_kept_small() {
        assert_eq!(pool_range("192.0.2.10"), Some(vec!["192.0.2.10".to_string()]));
        assert_eq!(pool_range("192.0.2.10-12"), Some(vec!["192.0.2.10".to_string(), "192.0.2.11".into(), "192.0.2.12".into()]));
        assert_eq!(pool_range("192.0.2.254-198.51.100.0").map(|v| v.len()), None, "a range across networks this far is too big");
        assert_eq!(pool_range("192.0.2.250-192.0.3.1").map(|v| v.len()), Some(8), "across a /24 boundary, written whole");
        assert_eq!(pool_range("192.0.2.8/29"), Some((9..=14).map(|i| format!("192.0.2.{i}")).collect()), "without the network and broadcast");
        assert_eq!(pool_range("192.0.2.9/32").map(|v| v.len()), Some(1));
        assert_eq!(pool_range("192.0.2.8/31").map(|v| v.len()), Some(2));
        assert_eq!(pool_range("10.0.0.0/22").map(|v| v.len()), Some(1022), "1024 at most");
        for bad in ["10.0.0.0/21", "192.0.2.12-10", "192.0.2.10-", "192.0.2.10-300", "192.0.2.9/29", "x", "", "192.0.2.1/33"] {
            assert_eq!(pool_range(bad), None, "{bad}");
        }
    }

    #[test]
    fn a_pool_range_is_added_whole_or_not_at_all_and_what_is_there_stays() {
        let mut s = with_nodes(&["n1"]);
        next(&mut s, &Entry::FloatingAdded { address: "192.0.2.11".into(), at: 1 }).unwrap();
        next(&mut s, &Entry::FloatingHeld { address: "192.0.2.11".into(), node_id: Some("n1".into()), at: 2 }).unwrap();
        let add = |a: &[&str]| Entry::FloatingPoolAdded { addresses: a.iter().map(|x| x.to_string()).collect(), at: 3 };
        assert_eq!(next(&mut s, &add(&["192.0.2.12", "10.240.0.9"])), Err(Rejected::BadAddress));
        assert!(!s.floating.contains_key("192.0.2.12"), "nothing of a refused range");
        next(&mut s, &add(&["192.0.2.10", "192.0.2.11", "192.0.2.12"])).unwrap();
        assert_eq!(s.floating.len(), 3);
        assert_eq!(s.floating["192.0.2.11"].holder.as_deref(), Some("n1"), "one already there kept as it was");
    }

    #[test]
    fn a_stack_exposes_its_services_on_pool_addresses_a_port_its_own_on_each() {
        let mut s = with_nodes(&["n1"]);
        for a in ["192.0.2.10", "192.0.2.11"] {
            next(&mut s, &Entry::FloatingAdded { address: a.into(), at: 1 }).unwrap();
        }
        next(&mut s, &deploy("a", &[("web", spec("nginx", 1)), ("db", spec("pg", 1))], &[("a.web.1", "n1"), ("a.db.1", "n1")])).unwrap();
        next(&mut s, &deploy("b", &[("shop", spec("shop", 1))], &[("b.shop.1", "n1")])).unwrap();
        let x = |service: &str, cport: u16, address: &str, port: u16| Exposure { service: service.into(), container_port: cport, protocol: "tcp".into(), address: address.into(), port };
        let set = |name: &str, e: Vec<Exposure>| Entry::StackExposed { name: name.into(), exposures: e, at: 2 };

        next(&mut s, &set("a", vec![x("web", 80, "192.0.2.10", 443), x("web", 80, "192.0.2.10", 80)])).unwrap();
        assert_eq!(s.stacks["a"].exposures.len(), 2);
        assert_eq!(next(&mut s, &set("b", vec![x("shop", 8080, "192.0.2.10", 443)])), Err(Rejected::ExposureTaken), "443 on .10 is a's");
        next(&mut s, &set("b", vec![x("shop", 8080, "192.0.2.11", 443)])).unwrap();
        assert_eq!(next(&mut s, &set("a", vec![x("web", 80, "192.0.2.10", 443), x("db", 5432, "192.0.2.10", 443)])), Err(Rejected::ExposureTaken), "twice in one list");
        let udp = Exposure { protocol: "udp".into(), ..x("db", 53, "192.0.2.11", 443) };
        next(&mut s, &set("a", vec![x("web", 80, "192.0.2.10", 443), udp])).unwrap();
        assert_eq!(s.stacks["a"].exposures.len(), 2, "the whole list replaced; udp 443 is not tcp 443");

        assert_eq!(next(&mut s, &set("a", vec![x("cache", 80, "192.0.2.10", 81)])), Err(Rejected::BadExposure), "no such service");
        assert_eq!(next(&mut s, &set("a", vec![x("web", 80, "192.0.2.10", 22)])), Err(Rejected::BadExposure), "the host's own port");
        assert_eq!(next(&mut s, &set("a", vec![x("web", 0, "192.0.2.10", 81)])), Err(Rejected::BadExposure));
        assert_eq!(next(&mut s, &set("a", vec![Exposure { protocol: "sctp".into(), ..x("web", 80, "192.0.2.10", 81) }])), Err(Rejected::BadExposure));
        assert_eq!(next(&mut s, &set("a", vec![x("web", 80, "192.0.2.99", 81)])), Err(Rejected::FloatingUnknown), "not in the pool");
        assert_eq!(next(&mut s, &set("zz", vec![])), Err(Rejected::StackUnknown));

        assert_eq!(next(&mut s, &Entry::FloatingRemoved { address: "192.0.2.11".into(), at: 3 }), Err(Rejected::FloatingInUse));
        next(&mut s, &deploy("a", &[("db", spec("pg", 1))], &[])).unwrap();
        assert_eq!(s.stacks["a"].exposures.iter().map(|e| e.service.as_str()).collect::<Vec<_>>(), vec!["db"], "web's went with web");
        next(&mut s, &Entry::StackRemoved { name: "b".into(), at: 4 }).unwrap();
        next(&mut s, &set("a", vec![])).unwrap();
        next(&mut s, &Entry::FloatingRemoved { address: "192.0.2.11".into(), at: 5 }).unwrap();
    }

    #[test]
    fn a_stack_keeps_the_nodes_chosen_for_it_and_takes_its_exposures_with_its_deploy() {
        let mut s = with_nodes(&["n1", "n2"]);
        next(&mut s, &Entry::FloatingAdded { address: "192.0.2.10".into(), at: 1 }).unwrap();
        let x = |port: u16| Exposure { service: "web".into(), container_port: 80, protocol: "tcp".into(), address: "192.0.2.10".into(), port };
        let with = |nodes: &[&str], exposures: Option<Vec<Exposure>>| match deploy("a", &[("web", spec("nginx", 1))], &[("a.web.1", "n2")]) {
            Entry::StackDeployed { name, compose, services, placements, at, .. } => Entry::StackDeployed { name, compose, services, placements, at, nodes: nodes.iter().map(|n| n.to_string()).collect(), exposures, volumes: Default::default(), options: Default::default(), stopped: false },
            _ => unreachable!(),
        };
        next(&mut s, &with(&["n2"], Some(vec![x(80)]))).unwrap();
        assert_eq!((s.stacks["a"].nodes.clone(), s.stacks["a"].exposures.clone()), (vec!["n2".to_string()], vec![x(80)]));
        next(&mut s, &with(&[], None)).unwrap();
        assert_eq!((s.stacks["a"].nodes.len(), s.stacks["a"].exposures.clone()), (0, vec![x(80)]), "none chosen: any node; no list: the exposures kept");
        assert_eq!(next(&mut s, &with(&["n9"], None)), Err(Rejected::NodeUnknown));
        assert_eq!(next(&mut s, &with(&[], Some(vec![Exposure { address: "192.0.2.99".into(), ..x(80) }]))), Err(Rejected::FloatingUnknown), "checked as a StackExposed is");
        assert_eq!(s.stacks["a"].exposures, vec![x(80)], "and a refusal changes nothing");
        next(&mut s, &deploy("b", &[("web", spec("nginx", 1))], &[("b.web.1", "n1")])).unwrap();
        let b = match with(&[], Some(vec![Exposure { ..x(80) }])) {
            Entry::StackDeployed { services, at, nodes, exposures, .. } => Entry::StackDeployed { name: "b".into(), compose: String::new(), services, placements: Default::default(), at, nodes, exposures, volumes: Default::default(), options: Default::default(), stopped: false },
            _ => unreachable!(),
        };
        assert_eq!(next(&mut s, &b), Err(Rejected::ExposureTaken), "80 on .10 is a's");
        next(&mut s, &with(&[], Some(vec![]))).unwrap();
        assert!(s.stacks["a"].exposures.is_empty(), "an empty list takes them all away");
    }

    #[test]
    fn a_floating_address_moves_only_off_a_fenced_or_removed_node_and_only_to_one_at_rest() {
        use FenceState::*;
        let mut s = with_nodes(&["n1", "n2", "n3"]);
        let add = |a: &str| Entry::FloatingAdded { address: a.into(), at: 1 };
        let held = |n: Option<&str>| Entry::FloatingHeld { address: "192.0.2.10".into(), node_id: n.map(str::to_string), at: 2 };
        next(&mut s, &add("192.0.2.10")).unwrap();
        assert_eq!(next(&mut s, &add("192.0.2.10")), Err(Rejected::FloatingExists));
        for bad in ["10.240.0.9", "127.0.0.1", "224.0.0.1", "0.0.0.0", "x"] {
            assert_eq!(next(&mut s, &add(bad)), Err(Rejected::BadAddress), "{bad}");
        }
        assert!(s.floating["192.0.2.10"].holder.is_none(), "added with no holder");
        assert_eq!(next(&mut s, &Entry::FloatingHeld { address: "192.0.2.99".into(), node_id: Some("n1".into()), at: 2 }), Err(Rejected::FloatingUnknown));
        assert_eq!(next(&mut s, &held(Some("n9"))), Err(Rejected::NodeUnknown));
        next(&mut s, &held(Some("n1"))).unwrap();
        assert_eq!(next(&mut s, &held(Some("n2"))), Err(Rejected::FenceStep), "n1 may still answer for it");
        for st in [Requested, InProgress] {
            fence(&mut s, st).unwrap();
            assert_eq!(next(&mut s, &held(Some("n2"))), Err(Rejected::FenceStep), "{st:?}: not fenced yet");
        }
        fence(&mut s, Fenced).unwrap();
        next(&mut s, &Entry::NodeFencing { node_id: "n3".into(), state: Requested, provider: "self".into(), message: String::new(), at: 9 }).unwrap();
        assert_eq!(next(&mut s, &held(Some("n3"))), Err(Rejected::FenceStep), "not to a node being fenced");
        next(&mut s, &held(Some("n2"))).unwrap();
        assert_eq!(s.floating["192.0.2.10"].holder.as_deref(), Some("n2"));
        next(&mut s, &Entry::NodeRemoved { node_id: "n2".into(), at: 3 }).unwrap();
        assert!(s.floating["192.0.2.10"].holder.is_none(), "a removed node holds nothing");
        next(&mut s, &Entry::FloatingRemoved { address: "192.0.2.10".into(), at: 4 }).unwrap();
        assert_eq!(next(&mut s, &Entry::FloatingRemoved { address: "192.0.2.10".into(), at: 4 }), Err(Rejected::FloatingUnknown));
    }

    #[test]
    fn the_ui_database_takes_only_the_next_change_and_a_snapshot_at_the_head() {
        let mut s = cluster();
        let change = |seq| Entry::UiDbChange { seq, data: "c".into(), at: 1 };
        let snap = |head, d: &str| Entry::UiDbSnapshot { head, data: d.into(), at: 1 };
        assert_eq!(next(&mut s, &change(1)), Err(Rejected::UiDbStale), "nothing to change before the first snapshot");
        next(&mut s, &snap(0, "s0")).unwrap();
        next(&mut s, &change(1)).unwrap();
        assert_eq!(next(&mut s, &change(1)), Err(Rejected::UiDbStale), "a second UI with the same number loses");
        assert_eq!(next(&mut s, &change(3)), Err(Rejected::UiDbStale));
        next(&mut s, &change(2)).unwrap();
        assert_eq!(next(&mut s, &snap(1, "old")), Err(Rejected::UiDbStale), "a UI behind cannot overwrite");
        next(&mut s, &change(3)).unwrap();
        next(&mut s, &snap(3, "s3")).unwrap();
        assert_eq!((s.ui_db.head, s.ui_db.snapshot_head, s.ui_db.changes.len(), s.ui_db.snapshot.as_deref()), (3, 3, 0, Some("s3")));
        next(&mut s, &change(4)).unwrap();
        assert_eq!(s.ui_db.changes.iter().map(|c| c.seq).collect::<Vec<_>>(), [4]);
    }

    #[test]
    fn api_tokens_are_found_by_their_hash_and_revoked_by_id() {
        let mut s = cluster();
        let issue = |id: &str, hash: &str| Entry::ApiTokenIssued { id: id.into(), name: "ui".into(), role: ApiRole::Read, secret_hash: hash.into(), at: 1 };
        next(&mut s, &issue("t1", "h1")).unwrap();
        assert_eq!(next(&mut s, &issue("t2", "h1")), Err(Rejected::TokenExists));
        assert_eq!(s.api_token("h1").map(|t| (t.id.as_str(), t.role)), Some(("t1", ApiRole::Read)));
        assert!(s.api_token("h2").is_none());
        next(&mut s, &Entry::ApiTokenRevoked { id: "t1".into(), at: 2 }).unwrap();
        assert!(s.api_token("h1").is_none());
        assert_eq!(next(&mut s, &Entry::ApiTokenRevoked { id: "t1".into(), at: 2 }), Err(Rejected::TokenUnknown));
        let json = serde_json::to_value(ApiToken { id: "x".into(), name: "n".into(), role: ApiRole::Admin, secret_hash: "secret".into(), created_at: 0 }).unwrap();
        assert!(json.get("secret_hash").is_some(), "kept in the state (serialized for the log)");
    }

    fn recovered(from: Option<&str>, moves: &[(&str, &str)], left: &[(&str, &str)]) -> Entry {
        let m = |l: &[(&str, &str)]| l.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        Entry::WorkloadsRecovered { from: from.map(str::to_string), moves: m(moves), left: m(left), at: 77 }
    }

    #[test]
    fn workloads_move_off_a_node_only_once_it_is_fenced_and_only_stateless_ones() {
        use FenceState::*;
        let mut s = with_nodes(&["n1", "n2"]);
        let vol = ServiceSpec { volumes: vec!["data:/data".into()], ..spec("redis", 1) };
        let pinned = ServiceSpec { ha: false, ..spec("cron", 1) };
        next(&mut s, &deploy("app", &[("web", spec("nginx", 1)), ("db", vol), ("cron", pinned)], &[("app.web.1", "n1"), ("app.db.1", "n1"), ("app.cron.1", "n1")])).unwrap();
        assert_eq!(s.recovery_kind(&s.workloads["app.web.1"]), RecoveryKind::Stateless);
        assert_eq!(s.recovery_kind(&s.workloads["app.db.1"]), RecoveryKind::LocalData);
        assert_eq!(s.recovery_kind(&s.workloads["app.cron.1"]), RecoveryKind::Pinned);
        let c = CommandRecord { op: Op::RunContainer { workload_id: "app.web.1".into(), image: "nginx".into(), name: String::new(), command: vec![], env: Default::default(), labels: Default::default(), generation: 0, ports: vec![], volumes: vec![], limits: Default::default(), net: Default::default(), storage: vec![] }, ..command("c", "n1") };
        next(&mut s, &Entry::CommandCreated { command: c }).unwrap();

        let web = recovered(Some("n1"), &[("app.web.1", "n2")], &[("app.db.1", "keeps local data")]);
        assert_eq!(next(&mut s, &web), Err(Rejected::FenceStep), "n1 is not fenced");
        for st in [Requested, InProgress, Fenced] {
            fence(&mut s, st).unwrap();
        }
        assert_eq!(next(&mut s, &recovered(Some("n1"), &[("app.db.1", "n2")], &[])), Err(Rejected::NotRecoverable), "local data stays");
        assert_eq!(next(&mut s, &recovered(Some("n1"), &[("app.cron.1", "n2")], &[])), Err(Rejected::NotRecoverable), "pinned stays");
        assert_eq!(next(&mut s, &recovered(Some("n1"), &[("app.web.1", "n9")], &[])), Err(Rejected::NodeUnknown));
        next(&mut s, &web).unwrap();
        assert_eq!(s.workloads["app.web.1"].node_id.as_deref(), Some("n2"));
        assert_eq!(s.commands["c"].status, CommandStatus::Rejected, "its open command on n1 is done with");
        assert!(s.nodes["n1"].recovered);
        assert_eq!(s.recoveries.len(), 2);
        assert_eq!(s.recoveries[1].to, Option::None);
        assert_eq!(next(&mut s, &web), Err(Rejected::NotRecoverable), "not on n1 any more");
    }

    fn nas() -> Storage {
        Storage { name: "nas".into(), driver: StorageDriver::Nfs, source: "192.0.2.50:/export/fd".into(), options: String::new(), added_at: 1, ceph: None }
    }

    fn cephfs() -> Storage {
        let keys = CephKeys { secret: crate::settings::Secret("AQBtb3VudC1rZXktb2YtdGVzdA==".into()), fence_client: "fd-fence".into(), fence_secret: Some(crate::settings::Secret("AgBmZW5jZS1rZXktb2YtdGVzdA==".into())), image: String::new() };
        Storage { name: "ceph".into(), driver: StorageDriver::Cephfs, source: "192.0.2.1,192.0.2.2:6789,[2001:db8::3]:/".into(), options: "name=fd,fs=fs1".into(), added_at: 1, ceph: Some(keys) }
    }

    #[test]
    fn a_cephfs_is_its_monitors_addresses_a_client_and_its_key_kept_apart_from_the_options() {
        let mut s = with_nodes(&["n1"]);
        let c = cephfs();
        assert_eq!((c.ceph_client(), c.ceph_fs()), (Some("fd"), Some("fs1")));
        assert_eq!(c.ceph_image(), DEFAULT_CEPH_IMAGE);
        let k = |f: fn(&mut CephKeys)| {
            let mut c = cephfs();
            f(c.ceph.as_mut().unwrap());
            c
        };
        for bad in [
            Storage { source: "mon.example:/".into(), ..cephfs() },
            Storage { source: "192.0.2.1:/../etc".into(), ..cephfs() },
            Storage { options: "fs=fs1".into(), ..cephfs() },
            Storage { options: "name=fd,fs=fs1,secret=AQB".into(), ..cephfs() },
            Storage { options: "name=fd,secretfile=/k".into(), ..cephfs() },
            Storage { ceph: None, ..cephfs() },
            Storage { ceph: cephfs().ceph, ..nas() },
            k(|k| k.secret = crate::settings::Secret(String::new())),
            k(|k| k.secret = crate::settings::Secret("a b".into())),
            k(|k| k.fence_client = "client.admin".into()),
            k(|k| k.fence_secret = None),
            k(|k| k.image = "x y".into()),
        ] {
            assert_eq!(s.check(&Entry::StorageAdded { storage: bad.clone() }), Err(Rejected::BadStorage), "{bad:?}");
        }
        let no_fence = k(|k| {
            k.fence_client = String::new();
            k.fence_secret = None;
        });
        assert!(no_fence.valid(), "fencing through Ceph is the cluster's choice");
        assert_eq!(s.check(&Entry::StorageAdded { storage: Storage { options: "name=fd".into(), ..cephfs() } }), Err(Rejected::BadStorage), "evicting needs the filesystem's name");
        next(&mut s, &Entry::StorageAdded { storage: cephfs() }).unwrap();
        assert!(StorageDriver::Cephfs.capabilities().client_eviction && !StorageDriver::Nfs.capabilities().client_eviction);
        let fresh = k(|k| k.secret = crate::settings::Secret("AQBuZXcta2V5".into())).ceph;
        next(&mut s, &Entry::StorageKeysSet { name: "ceph".into(), ceph: fresh.clone().unwrap(), at: 2 }).unwrap();
        assert_eq!(s.storage["ceph"].ceph, fresh);
        assert_eq!(s.check(&Entry::StorageKeysSet { name: "nope".into(), ceph: fresh.clone().unwrap(), at: 2 }), Err(Rejected::StorageUnknown));
        next(&mut s, &Entry::StorageAdded { storage: nas() }).unwrap();
        assert_eq!(s.check(&Entry::StorageKeysSet { name: "nas".into(), ceph: fresh.unwrap(), at: 2 }), Err(Rejected::BadStorage), "an NFS export has no Ceph keys");
    }

    #[test]
    fn a_volume_on_a_cephfs_goes_to_the_worker_without_its_key() {
        let mut s = with_nodes(&["n1"]);
        next(&mut s, &Entry::StorageAdded { storage: cephfs() }).unwrap();
        let db = ServiceSpec { volumes: vec!["data:/var/lib/data".into()], ..spec("postgres", 1) };
        next(&mut s, &on_storage("app", &[("db", db)], &[("app.db.1", "n1")], &[("data", "ceph", Access::One)])).unwrap();
        let m = s.storage_mounts(&s.workloads["app.db.1"]);
        assert_eq!(m.len(), 1);
        assert_eq!((m[0].driver, m[0].source.as_str(), m[0].options.as_str()), (StorageDriver::Cephfs, "192.0.2.1,192.0.2.2:6789,[2001:db8::3]:/", "name=fd,fs=fs1"));
        assert!(!serde_json::to_string(&m).unwrap().contains("AQBtb3Vud"), "the key is filled in as the command is sent, never kept with it");
    }

    fn on_storage(name: &str, services: &[(&str, ServiceSpec)], placements: &[(&str, &str)], volumes: &[(&str, &str, Access)]) -> Entry {
        match deploy(name, services, placements) {
            Entry::StackDeployed { name, compose, services, placements, at, nodes, exposures, .. } => {
                let volumes = volumes.iter().map(|(v, st, a)| (v.to_string(), StackVolume { storage: st.to_string(), access: *a })).collect();
                Entry::StackDeployed { name, compose, services, placements, at, nodes, exposures, volumes, options: Default::default(), stopped: false }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn a_backup_is_restored_into_a_new_cluster_only_and_its_open_commands_expire() {
        let mut old = with_nodes(&["n1"]);
        next(&mut old, &Entry::CommandCreated { command: command("c1", "n1") }).unwrap();
        old.settings.fence_stop_s = Some(40);
        let mut s = ClusterState::default();
        s.apply_at(1, &Entry::StateRestored { state: Box::new(old.clone()), at: 5 }).unwrap();
        assert_eq!(s.cluster_id, old.cluster_id, "the same cluster, its identity kept");
        assert_eq!(s.applied, 1, "this log's index, not the backup's");
        assert_eq!((s.nodes.len(), s.settings.fence_stop_s), (1, Some(40)));
        assert_eq!(s.commands["c1"].status, CommandStatus::Expired, "nothing runs late from an old backup");
        assert_eq!(s.apply_at(2, &Entry::StateRestored { state: Box::new(old), at: 6 }), Err(Rejected::NotEmpty));
        let mut fresh = ClusterState::default();
        fresh.apply_at(1, &Entry::ClusterCreated { cluster_id: "other".into(), at: 1 }).unwrap();
        assert_eq!(fresh.apply_at(2, &Entry::StateRestored { state: Box::new(s), at: 7 }), Err(Rejected::NotEmpty), "not over a cluster of its own");
    }

    #[test]
    fn a_workload_s_attempts_are_counted_per_spec_in_the_state_until_it_settles() {
        let mut s = with_nodes(&["a"]);
        let services = crate::compose::parse("services:\n  job:\n    image: x\n    restart: on-failure:2\n").unwrap().services;
        next(&mut s, &Entry::StackDeployed { name: "st".into(), compose: String::new(), services, placements: [("st.job.1".to_string(), "a".to_string())].into(), at: 1, nodes: vec![], exposures: None, volumes: Default::default(), options: Default::default(), stopped: false }).unwrap();
        let g = s.workloads["st.job.1"].generation;
        next(&mut s, &Entry::WorkloadAttempted { workload_id: "st.job.1".into(), generation: g, at: 10 }).unwrap();
        next(&mut s, &Entry::WorkloadAttempted { workload_id: "st.job.1".into(), generation: g, at: 20 }).unwrap();
        assert_eq!(s.workloads["st.job.1"].attempts, Some(Attempts { generation: g, runs: 2, at: 20 }));
        next(&mut s, &Entry::WorkloadAttempted { workload_id: "st.job.1".into(), generation: g + 7, at: 30 }).unwrap();
        assert_eq!(s.workloads["st.job.1"].attempts, Some(Attempts { generation: g + 7, runs: 1, at: 30 }), "a new spec counts from one");
        next(&mut s, &Entry::WorkloadSettled { workload_id: "st.job.1".into(), at: 40 }).unwrap();
        assert_eq!(s.workloads["st.job.1"].attempts, None);
        next(&mut s, &Entry::WorkloadAttempted { workload_id: "gone".into(), generation: 1, at: 50 }).unwrap();
    }

    #[test]
    fn a_move_remembers_the_node_left_until_its_copy_is_gone_there() {
        let mut s = with_nodes(&["a", "b"]);
        let services = crate::compose::parse("services:\n  web:\n    image: nginx\n").unwrap().services;
        next(&mut s, &Entry::StackDeployed { name: "st".into(), compose: String::new(), services, placements: [("st.web.1".to_string(), "a".to_string())].into(), at: 1, nodes: vec![], exposures: None, volumes: Default::default(), options: Default::default(), stopped: false }).unwrap();
        assert_eq!(s.workloads["st.web.1"].leaving, None, "a first placement leaves nothing");
        next(&mut s, &Entry::StackPlaced { name: "st".into(), placements: [("st.web.1".to_string(), "b".to_string())].into(), at: 2 }).unwrap();
        assert_eq!(s.workloads["st.web.1"].leaving.as_deref(), Some("a"));
        assert!(s.waits_for_old(&s.workloads["st.web.1"]), "a is healthy and may still run it");
        next(&mut s, &Entry::WorkloadLeft { workload_id: "st.web.1".into(), node_id: "b".into(), at: 3 }).unwrap();
        assert_eq!(s.workloads["st.web.1"].leaving.as_deref(), Some("a"), "only the node it left says so");
        next(&mut s, &Entry::WorkloadLeft { workload_id: "st.web.1".into(), node_id: "a".into(), at: 4 }).unwrap();
        assert!(!s.waits_for_old(&s.workloads["st.web.1"]));

        next(&mut s, &Entry::StackPlaced { name: "st".into(), placements: [("st.web.1".to_string(), "a".to_string())].into(), at: 5 }).unwrap();
        assert_eq!(s.workloads["st.web.1"].leaving.as_deref(), Some("b"));
        next(&mut s, &Entry::StackPlaced { name: "st".into(), placements: [("st.web.1".to_string(), "b".to_string())].into(), at: 6 }).unwrap();
        assert_eq!(s.workloads["st.web.1"].leaving.as_deref(), Some("a"), "back before it left: it waits for a again");
        s.nodes.get_mut("a").unwrap().fencing.state = FenceState::Fenced;
        assert!(!s.waits_for_old(&s.workloads["st.web.1"]), "a fenced runs nothing");
        s.nodes.get_mut("a").unwrap().fencing.state = FenceState::None;
        s.nodes.remove("a");
        assert!(!s.waits_for_old(&s.workloads["st.web.1"]), "a removed runs nothing for the cluster");

        let mut s = with_nodes(&["a", "b"]);
        let services = crate::compose::parse("services:\n  web:\n    image: nginx\n    deploy: {update_config: {order: start-first}}\n").unwrap().services;
        next(&mut s, &Entry::StackDeployed { name: "st".into(), compose: String::new(), services, placements: [("st.web.1".to_string(), "a".to_string())].into(), at: 1, nodes: vec![], exposures: None, volumes: Default::default(), options: Default::default(), stopped: false }).unwrap();
        next(&mut s, &Entry::StackPlaced { name: "st".into(), placements: [("st.web.1".to_string(), "b".to_string())].into(), at: 2 }).unwrap();
        assert_eq!(s.workloads["st.web.1"].leaving, None, "start-first: the new copy may start beside the old");
    }

    #[test]
    fn a_registry_s_login_is_set_replaced_and_removed() {
        use crate::registry::Registry;
        use crate::settings::Secret;
        let mut s = with_nodes(&["n1"]);
        let r = |host: &str, user: &str| Registry { host: host.into(), username: user.into(), password: Secret("p".into()), at: 1 };
        next(&mut s, &Entry::RegistrySet { registry: r("ghcr.io", "a") }).unwrap();
        next(&mut s, &Entry::RegistrySet { registry: r("ghcr.io", "b") }).unwrap();
        assert_eq!(s.registries["ghcr.io"].username, "b", "one login per registry");
        assert_eq!(next(&mut s, &Entry::RegistrySet { registry: r("https://x", "a") }), Err(Rejected::BadRegistry));
        assert_eq!(next(&mut s, &Entry::RegistrySet { registry: r("x.io", "") }), Err(Rejected::BadRegistry), "a login has a user");
        next(&mut s, &Entry::RegistryRemoved { host: "ghcr.io".into(), at: 2 }).unwrap();
        assert_eq!(next(&mut s, &Entry::RegistryRemoved { host: "ghcr.io".into(), at: 3 }), Err(Rejected::RegistryUnknown));
        let old: ClusterState = serde_json::from_str(r#"{"cluster_id":null,"applied":0,"join_tokens":{},"nodes":{},"commands":{}}"#).unwrap();
        assert!(old.registries.is_empty());
    }

    #[test]
    fn a_node_is_called_by_its_name_label_before_its_hostname() {
        let mut s = with_nodes(&["n1"]);
        let n = s.nodes.get_mut("n1").unwrap();
        n.hostname = "docker".into();
        assert_eq!(n.display_name(), "docker");
        n.labels.insert("name".into(), "w3".into());
        assert_eq!(n.display_name(), "w3");
    }

    #[test]
    fn a_notification_channel_is_set_kept_by_outcome_and_removed() {
        use crate::notify::{Channel, Config};
        let mut s = with_nodes(&["n1"]);
        let ch = |name: &str, events: &[&str]| Channel { name: name.into(), kind: "ntfy".into(), enabled: true, events: events.iter().map(|e| e.to_string()).collect(), config: Config::default(), secret: None, at: 1, last_sent_at: None, last_error: None };
        next(&mut s, &Entry::ChannelSet { channel: ch("ops", &["node.down"]) }).unwrap();
        next(&mut s, &Entry::ChannelOutcome { name: "ops".into(), error: Some("refused".into()), at: 5 }).unwrap();
        assert_eq!(s.notifications["ops"].last_error.as_deref(), Some("refused"));
        next(&mut s, &Entry::ChannelOutcome { name: "ops".into(), error: None, at: 6 }).unwrap();
        assert_eq!((s.notifications["ops"].last_sent_at, s.notifications["ops"].last_error.as_deref()), (Some(6), None), "a delivery that went through clears the error");
        next(&mut s, &Entry::ChannelSet { channel: ch("ops", &["node.back"]) }).unwrap();
        assert_eq!((s.notifications["ops"].events.clone(), s.notifications["ops"].last_sent_at), (vec!["node.back".to_string()], Some(6)), "an edit keeps the channel's history");
        assert_eq!(next(&mut s, &Entry::ChannelSet { channel: ch("Bad Name", &[]) }), Err(Rejected::BadChannel));
        assert_eq!(next(&mut s, &Entry::ChannelSet { channel: ch("x", &["node.melted"]) }), Err(Rejected::BadChannel));
        next(&mut s, &Entry::ChannelRemoved { name: "ops".into(), at: 7 }).unwrap();
        assert_eq!(next(&mut s, &Entry::ChannelRemoved { name: "ops".into(), at: 8 }), Err(Rejected::ChannelUnknown));
        next(&mut s, &Entry::ChannelOutcome { name: "ops".into(), error: None, at: 9 }).unwrap();
        assert!(s.notifications.is_empty(), "an outcome for a channel gone is no harm");
    }

    #[test]
    fn the_cluster_s_settings_are_replaced_whole_and_checked() {
        let mut s = with_nodes(&["n1"]);
        let set = crate::settings::Settings { fence_stop_s: Some(40), ..Default::default() };
        next(&mut s, &Entry::SettingsSet { settings: set.clone(), at: 1 }).unwrap();
        assert_eq!(s.settings, set);
        let bad = crate::settings::Settings { fence_stop_s: Some(0), ..Default::default() };
        assert_eq!(next(&mut s, &Entry::SettingsSet { settings: bad, at: 2 }), Err(Rejected::BadSettings));
        assert_eq!(s.settings, set, "unchanged");
        let old: ClusterState = serde_json::from_str(r#"{"cluster_id":null,"applied":0,"join_tokens":{},"nodes":{},"commands":{}}"#).unwrap();
        assert_eq!(old.settings, Default::default(), "a state from before has none");
    }

    #[test]
    fn storage_is_added_once_checked_and_kept_while_a_stack_uses_it() {
        let mut s = with_nodes(&["n1"]);
        next(&mut s, &Entry::StorageAdded { storage: nas() }).unwrap();
        assert_eq!(next(&mut s, &Entry::StorageAdded { storage: nas() }), Err(Rejected::StorageExists));
        for bad in [
            Storage { name: "Bad name".into(), ..nas() },
            Storage { source: "no-export".into(), ..nas() },
            Storage { source: "192.0.2.50:relative".into(), ..nas() },
            Storage { name: "p".into(), driver: StorageDriver::Path, source: "relative/dir".into(), ..nas() },
            Storage { name: "p".into(), driver: StorageDriver::Path, source: "/".into(), ..nas() },
            Storage { name: "o".into(), options: "rw;reboot".into(), ..nas() },
        ] {
            assert_eq!(next(&mut s, &Entry::StorageAdded { storage: bad.clone() }), Err(Rejected::BadStorage), "{bad:?}");
        }
        next(&mut s, &Entry::StorageAdded { storage: Storage { name: "shared".into(), driver: StorageDriver::Path, source: "/mnt/shared".into(), ..nas() } }).unwrap();
        let db = ServiceSpec { volumes: vec!["data:/var/lib/data".into()], ..spec("postgres", 1) };
        assert_eq!(next(&mut s, &on_storage("app", &[("db", db.clone())], &[("app.db.1", "n1")], &[("data", "nope", Access::One)])), Err(Rejected::StorageUnknown));
        next(&mut s, &on_storage("app", &[("db", db.clone())], &[("app.db.1", "n1")], &[("data", "nas", Access::One)])).unwrap();
        assert_eq!(s.stacks["app"].volumes["data"].storage, "nas");
        assert_eq!(s.recovery_kind(&s.workloads["app.db.1"]), RecoveryKind::SharedStorage, "its data is on shared storage: it may move");
        assert_eq!(next(&mut s, &Entry::StorageRemoved { name: "nas".into(), at: 2 }), Err(Rejected::StorageInUse));
        let two = ServiceSpec { replicas: 2, ..db.clone() };
        assert_eq!(next(&mut s, &on_storage("app", &[("db", two.clone())], &[("app.db.1", "n1"), ("app.db.2", "n1")], &[("data", "nas", Access::One)])), Err(Rejected::VolumeShared), "one writer: one workload");
        next(&mut s, &on_storage("app", &[("db", two)], &[("app.db.1", "n1"), ("app.db.2", "n1")], &[("data", "nas", Access::Many)])).unwrap();
        let mixed = ServiceSpec { volumes: vec!["data:/d".into(), "/srv/local:/l".into()], ..spec("x", 1) };
        next(&mut s, &on_storage("mix", &[("x", mixed)], &[("mix.x.1", "n1")], &[("data", "nas", Access::One)])).unwrap();
        assert_eq!(s.recovery_kind(&s.workloads["mix.x.1"]), RecoveryKind::LocalData, "a path on its node keeps it there");
        next(&mut s, &Entry::StackRemoved { name: "mix".into(), at: 3 }).unwrap();
        next(&mut s, &Entry::StackRemoved { name: "app".into(), at: 3 }).unwrap();
        next(&mut s, &Entry::StorageRemoved { name: "nas".into(), at: 4 }).unwrap();
        assert_eq!(next(&mut s, &Entry::StorageRemoved { name: "nas".into(), at: 4 }), Err(Rejected::StorageUnknown));
    }

    #[test]
    fn a_volume_for_one_writer_is_leased_to_one_node_and_taken_over_only_once_it_is_fenced() {
        use FenceState::*;
        let mut s = with_nodes(&["n1", "n2"]);
        next(&mut s, &Entry::StorageAdded { storage: nas() }).unwrap();
        let db = ServiceSpec { volumes: vec!["data:/var/lib/data".into()], ..spec("postgres", 1) };
        next(&mut s, &on_storage("app", &[("db", db)], &[("app.db.1", "n1")], &[("data", "nas", Access::One)])).unwrap();
        let take = |n: &str| Entry::LeaseTaken { volume: "app/data".into(), node_id: n.into(), at: 5 };
        let give = |n: &str| Entry::LeaseReleased { volume: "app/data".into(), node_id: n.into(), at: 6 };
        assert_eq!(next(&mut s, &Entry::LeaseTaken { volume: "app/nope".into(), node_id: "n1".into(), at: 5 }), Err(Rejected::StorageUnknown), "no such volume");
        next(&mut s, &take("n1")).unwrap();
        next(&mut s, &take("n1")).unwrap();
        assert_eq!(s.leases["app/data"].node_id, "n1");
        assert_eq!(next(&mut s, &take("n2")), Err(Rejected::LeaseHeld), "n1 may still write");
        assert_eq!(next(&mut s, &give("n2")), Err(Rejected::LeaseHeld), "only its holder lets it go");
        next(&mut s, &give("n1")).unwrap();
        next(&mut s, &take("n2")).unwrap();
        next(&mut s, &give("n2")).unwrap();
        next(&mut s, &take("n1")).unwrap();
        for st in [Requested, InProgress] {
            fence(&mut s, st).unwrap();
            assert_eq!(next(&mut s, &take("n2")), Err(Rejected::LeaseHeld), "{st:?}: not fenced yet");
        }
        fence(&mut s, Fenced).unwrap();
        next(&mut s, &take("n2")).unwrap();
        assert_eq!(s.leases["app/data"].node_id, "n2", "taken over from a fenced node");
        next(&mut s, &recovered(Some("n1"), &[("app.db.1", "n2")], &[])).unwrap();
        assert_eq!(s.workloads["app.db.1"].node_id.as_deref(), Some("n2"), "its data is shared: it moves off the fenced node");
        next(&mut s, &Entry::NodeRemoved { node_id: "n2".into(), at: 7 }).unwrap();
        assert!(!s.leases.contains_key("app/data"), "a removed node holds nothing");
        assert_eq!(next(&mut s, &Entry::LeaseTaken { volume: "app/data".into(), node_id: "n9".into(), at: 5 }), Err(Rejected::NodeUnknown));
    }

    #[test]
    fn a_new_fencing_clears_recovered_and_a_removed_node_s_workloads_get_a_node() {
        use FenceState::*;
        let mut s = with_nodes(&["n1", "n2"]);
        next(&mut s, &deploy("app", &[("web", spec("nginx", 1))], &[("app.web.1", "n2")])).unwrap();
        for st in [Requested, InProgress, Fenced] {
            fence(&mut s, st).unwrap();
        }
        next(&mut s, &recovered(Some("n1"), &[], &[])).unwrap();
        assert!(s.nodes["n1"].recovered);
        fence(&mut s, AwaitingAuthorization).unwrap();
        next(&mut s, &Entry::NodeAuthorized { node_id: "n1".into(), by: "x".into(), at: 1 }).unwrap();
        fence(&mut s, None).unwrap();
        assert!(!s.nodes["n1"].recovered);
        next(&mut s, &Entry::NodeRemoved { node_id: "n2".into(), at: 1 }).unwrap();
        assert_eq!(next(&mut s, &recovered(Some("n1"), &[("app.web.1", "n1")], &[])), Err(Rejected::FenceStep));
        next(&mut s, &recovered(Option::None, &[("app.web.1", "n1")], &[])).unwrap();
        assert_eq!(s.workloads["app.web.1"].node_id.as_deref(), Some("n1"));
    }

    #[test]
    fn fencing_goes_requested_in_progress_fenced_and_back_only_by_authorization() {
        use FenceState::*;
        let mut s = with_nodes(&["n1"]);
        assert_eq!(fence(&mut s, InProgress), Err(Rejected::FenceStep), "not requested");
        assert_eq!(fence(&mut s, Fenced), Err(Rejected::FenceStep));
        fence(&mut s, Requested).unwrap();
        assert_eq!(fence(&mut s, Requested), Err(Rejected::FenceStep), "once");
        fence(&mut s, InProgress).unwrap();
        fence(&mut s, Fenced).unwrap();
        assert_eq!(s.nodes["n1"].fencing.state, Fenced);
        let auth = Entry::NodeAuthorized { node_id: "n1".into(), by: "admin".into(), at: 10 };
        assert_eq!(s.check(&auth), Err(Rejected::FenceStep), "a fenced node that has not come back is not authorized");
        fence(&mut s, AwaitingAuthorization).unwrap();
        assert_eq!(fence(&mut s, Authorized), Err(Rejected::FenceStep), "only through NodeAuthorized");
        next(&mut s, &auth).unwrap();
        assert_eq!(s.nodes["n1"].fencing.provider, "admin");
        fence(&mut s, None).unwrap();
        assert_eq!(s.nodes["n1"].fencing.state, None);
    }

    #[test]
    fn a_node_heard_again_before_it_fenced_goes_back_to_rest() {
        use FenceState::*;
        let mut s = with_nodes(&["n1"]);
        assert_eq!(fence(&mut s, None), Err(Rejected::FenceStep));
        fence(&mut s, Requested).unwrap();
        fence(&mut s, InProgress).unwrap();
        fence(&mut s, None).unwrap();
        fence(&mut s, AwaitingAuthorization).unwrap();
        assert_eq!(fence(&mut s, None), Err(Rejected::FenceStep), "a fenced worker is not let off without authorization");
    }

    #[test]
    fn state_written_before_stacks_reads_back() {
        let old = r#"{"cluster_id":"c","applied":3,"join_tokens":{},"nodes":{},"commands":{}}"#;
        let s: ClusterState = serde_json::from_str(old).unwrap();
        assert!(s.stacks.is_empty());
    }

    #[test]
    fn entries_read_back_as_written() {
        let e = Entry::CommandCreated { command: CommandRecord { op: Op::RunContainer { workload_id: "w".into(), image: "nginx".into(), name: String::new(), command: vec![], env: Default::default(), labels: Default::default(), generation: 4, ports: vec![Port { host_port: 80, container_port: 8080, protocol: "tcp".into(), host_ip: String::new() }], volumes: vec!["d:/d".into()], limits: Resources { cpu_millis: 500, memory_bytes: 0 }, net: Default::default(), storage: vec![] }, ..command("c", "n1") } };
        let back: Entry = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(back, e);
        let old = r#"{"type":"command_created","command":{"id":"c","node_id":"n","op":{"op":"run_container","workload_id":"w","image":"i"},"config_version":0,"created_at":0,"deadline":0,"status":"pending","message":"","result_json":"","updated_at":0}}"#;
        assert!(serde_json::from_str::<Entry>(old).is_ok(), "a run logged before generations reads back");
    }
}
