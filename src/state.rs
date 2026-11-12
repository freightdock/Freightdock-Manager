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
    /// new workload has no placement or its node is not enrolled.
    StackDeployed { name: String, compose: String, services: BTreeMap<String, ServiceSpec>, placements: BTreeMap<String, String>, at: Ms },
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
    /// A recovery for a workload that is no longer where it said, or may not move by itself (pinned or local data).
    NotRecoverable,
    /// An API token with that id or secret exists.
    TokenExists,
    /// A UI database write or snapshot that is not the next: another UI wrote since (or none was made yet).
    UiDbStale,
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
            Rejected::BadName => "a stack or service name must be 1 to 40 of a-z, 0-9 and - (services may have _), starting with a letter or digit",
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub hostname: String,
    pub cert_sha256: String,
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
    },
    StopContainer {
        workload_id: String,
        #[serde(default)]
        timeout_s: u32,
    },
    RemoveContainer {
        workload_id: String,
    },
}

impl Op {
    /// The workload the op is about, if it is about one.
    pub fn workload_id(&self) -> Option<&str> {
        match self {
            Op::RunContainer { workload_id, .. } | Op::StopContainer { workload_id, .. } | Op::RemoveContainer { workload_id } => Some(workload_id),
            Op::Ping | Op::DockerInfo => None,
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
}

fn yes() -> bool {
    true
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
}

impl ServiceSpec {
    /// The spec as one workload's container runs it: the replica count, the placement and HA are the stack's
    /// business, so changing them makes no container again.
    pub fn for_workload(&self) -> ServiceSpec {
        ServiceSpec { replicas: 1, placement: Placement::default(), ha: true, ..self.clone() }
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
    /// Has volumes, which live on its node: it waits for the node (G11 brings shared storage).
    LocalData,
    /// `x-freightdock: {ha: false}`: stays until someone moves it.
    Pinned,
}

impl ClusterState {
    /// Whether manager `raft_id` joined with a manager token (the first manager made the cluster, so never did).
    pub fn manager_joined(&self, raft_id: u64) -> bool {
        let who = format!("manager:{raft_id}");
        self.join_tokens.values().any(|t| t.spent_by.as_deref() == Some(who.as_str()))
    }

    pub fn node_by_cert(&self, cert_sha256: &str) -> Option<&Node> {
        self.nodes.values().find(|n| n.cert_sha256 == cert_sha256)
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
        } else if !w.spec.volumes.is_empty() {
            RecoveryKind::LocalData
        } else {
            RecoveryKind::Stateless
        }
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
        if self.cluster_id.is_none() && !matches!(entry, Entry::ClusterCreated { .. }) {
            return Err(Rejected::NoCluster);
        }
        match entry {
            Entry::ClusterCreated { cluster_id, .. } => {
                if self.cluster_id.is_none() {
                    self.cluster_id = Some(cluster_id.clone());
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
                self.nodes.insert(node_id.clone(), Node { id: node_id.clone(), hostname: hostname.clone(), cert_sha256: cert_sha256.clone(), enrolled_at: *at, report: None, reported_at: None, labels: BTreeMap::new(), fencing: Fencing::default(), recovered: false });
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
            Entry::StackDeployed { name, compose, services, placements, at } => {
                if !valid_stack_name(name) || !services.keys().all(|s| valid_service_name(s)) {
                    return Err(Rejected::BadName);
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
                    let spec = services[svc].for_workload();
                    match self.workloads.get_mut(id) {
                        Some(w) => {
                            // A placement for a placed workload moves it: its old node's reconcile removes the
                            // container there, its new node's runs one.
                            if let Some(n) = placements.get(id) {
                                w.node_id = Some(n.clone());
                            }
                            if w.spec != spec {
                                w.spec = spec;
                                w.generation = index;
                            }
                        }
                        None => {
                            self.workloads.insert(id.clone(), Workload { id: id.clone(), stack: name.clone(), service: svc.clone(), slot: *slot, node_id: placements.get(id).cloned(), spec, generation: index, host: None });
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
                self.stacks.insert(name.clone(), Stack { name: name.clone(), compose: compose.clone(), services: services.clone(), stopped: false, created_at, updated_at: *at, net });
            }
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
                    if w.node_id != *from || (from.is_some() && self.recovery_kind(w) != RecoveryKind::Stateless) {
                        return Err(Rejected::NotRecoverable);
                    }
                    if !self.nodes.contains_key(to) || Some(to) == from.as_ref() {
                        return Err(Rejected::NodeUnknown);
                    }
                }
                for (id, to) in moves {
                    self.workloads.get_mut(id).expect("checked").node_id = Some(to.clone());
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
        ServiceSpec { image: image.into(), command: vec![], env: Default::default(), labels: Default::default(), ports: vec![], volumes: vec![], replicas, limits: Default::default(), placement: Default::default(), ha: true }
    }

    fn with_nodes(nodes: &[&str]) -> ClusterState {
        let mut s = cluster();
        for (i, n) in nodes.iter().enumerate() {
            let hash = format!("t{i}");
            with_token(&mut s, &hash, 1000);
            let at = s.applied + 1;
            s.apply_at(at, &enrol(n, &hash, 10)).unwrap();
        }
        s
    }

    fn deploy(name: &str, services: &[(&str, ServiceSpec)], placements: &[(&str, &str)]) -> Entry {
        Entry::StackDeployed {
            name: name.into(),
            compose: "services: {}".into(),
            services: services.iter().map(|(k, v)| (k.to_string(), v.clone())).collect(),
            placements: placements.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            at: 50,
        }
    }

    fn next(s: &mut ClusterState, e: &Entry) -> Result<(), Rejected> {
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
        let c = CommandRecord { op: Op::RunContainer { workload_id: "app.web.1".into(), image: "nginx".into(), name: String::new(), command: vec![], env: Default::default(), labels: Default::default(), generation: 0, ports: vec![], volumes: vec![], limits: Default::default(), net: Default::default() }, ..command("c", "n1") };
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
        let e = Entry::CommandCreated { command: CommandRecord { op: Op::RunContainer { workload_id: "w".into(), image: "nginx".into(), name: String::new(), command: vec![], env: Default::default(), labels: Default::default(), generation: 4, ports: vec![Port { host_port: 80, container_port: 8080, protocol: "tcp".into(), host_ip: String::new() }], volumes: vec!["d:/d".into()], limits: Resources { cpu_millis: 500, memory_bytes: 0 }, net: Default::default() }, ..command("c", "n1") } };
        let back: Entry = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(back, e);
        let old = r#"{"type":"command_created","command":{"id":"c","node_id":"n","op":{"op":"run_container","workload_id":"w","image":"i"},"config_version":0,"created_at":0,"deadline":0,"status":"pending","message":"","result_json":"","updated_at":0}}"#;
        assert!(serde_json::from_str::<Entry>(old).is_ok(), "a run logged before generations reads back");
    }
}
