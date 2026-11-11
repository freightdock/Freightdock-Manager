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
    /// A manager token spent by a manager joining. Raft's membership is what makes it a manager; this entry makes the
    /// token good once and records who used it.
    ManagerJoined { raft_id: u64, addr: String, secret_hash: String, at: Ms },
    CommandCreated { command: CommandRecord },
    CommandStatus { command_id: String, status: CommandStatus, message: String, result_json: String, at: Ms },
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
                self.nodes.insert(node_id.clone(), Node { id: node_id.clone(), hostname: hostname.clone(), cert_sha256: cert_sha256.clone(), enrolled_at: *at, report: None, reported_at: None });
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
            Entry::NodeRemoved { node_id, .. } => {
                self.nodes.remove(node_id).ok_or(Rejected::NodeUnknown)?;
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
                self.commands.insert(command.id.clone(), command.clone());
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

    #[test]
    fn entries_read_back_as_written() {
        let e = Entry::CommandCreated { command: CommandRecord { op: Op::RunContainer { workload_id: "w".into(), image: "nginx".into(), name: String::new(), command: vec![], env: Default::default(), labels: Default::default() }, ..command("c", "n1") } };
        let back: Entry = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(back, e);
    }
}
