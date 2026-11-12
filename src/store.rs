//! Where entries are proposed, committed and applied: the face the rest of the manager sees of consensus.
//!
//! `propose` hands an entry to Raft (`raft/`) and returns once a quorum of managers has committed it and this manager
//! has applied it, with what applying it answered; `read` answers from this manager's applied state. Before proposing,
//! the entry is checked against the local state, so one that would be refused (a spent token) fails fast without
//! being written; `apply` checks again, deterministically, as the state may have moved on in between.
//!
//! A manager that is not the leader cannot propose yet (G3.3 forwards to the leader); with a single manager it always
//! is, once its election (a second or two after a start) is over, which `propose` waits for.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use openraft::BasicNode;
use openraft::error::{ClientWriteError, RaftError};

use crate::raft::{NodeId, Raft, config, log::LogStore, network::Network, sm::StateMachine};
use crate::state::{ClusterState, Entry, Rejected};

pub struct Store {
    pub raft: Raft,
    pub id: NodeId,
    sm: StateMachine,
    /// For forwarding to the leader; `None` for a manager alone (tests).
    peer_tls: Option<Arc<rustls::ClientConfig>>,
    /// On a follower: when an append from the leader last came in.
    leader_heard: std::sync::Mutex<Option<std::time::Instant>>,
}

#[derive(Debug)]
pub enum ProposeError {
    Rejected(Rejected),
    /// No leader to commit it, or no quorum behind the leader: nothing can change until managers are back.
    Unavailable(String),
    /// Another manager leads (at this address, when known). `propose` forwards it there; only `propose_here` answers it.
    NotLeader(Option<String>),
    Io(anyhow::Error),
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposeError::Rejected(r) => write!(f, "{r}"),
            ProposeError::Unavailable(why) => write!(f, "the cluster cannot take changes now: {why}"),
            ProposeError::NotLeader(at) => write!(f, "this manager is not the leader (the leader is {})", at.as_deref().unwrap_or("not known")),
            ProposeError::Io(e) => write!(f, "the change could not be committed: {e:#}"),
        }
    }
}

impl std::error::Error for ProposeError {}

/// How long a proposal waits for a leader (an election after a start or a leader's loss) before giving up.
/// How long a change may wait to be committed before it is given up on.
const COMMIT_WAIT: Duration = Duration::from_secs(5);
/// How old the leader's last word from a quorum may be. Heartbeats go every 250 ms, so a second or two of silence
/// means the others are gone or cut off.
const QUORUM_STALE: Duration = Duration::from_secs(3);
const LEADER_WAIT: Duration = Duration::from_secs(5);

impl Store {
    /// The store in `<dir>/raft`, as Raft node `id` reachable at `addr`. A manager that has never been in a cluster
    /// starts one of its own, with itself as its only member, when `bootstrap` is set.
    pub async fn open(dir: &Path, id: NodeId, addr: &str, bootstrap: bool, network: Network, peer_tls: Option<Arc<rustls::ClientConfig>>) -> Result<Self> {
        let raft_dir = dir.join("raft");
        let log = LogStore::open(&raft_dir).context("the Raft log")?;
        let sm = StateMachine::open(&raft_dir).context("the Raft snapshot")?;
        let pristine = log.is_pristine().await;
        let raft = Raft::new(id, Arc::new(config().validate()?), network, log, sm.clone()).await.context("starting Raft")?;
        if pristine && bootstrap {
            raft.initialize(BTreeMap::from([(id, BasicNode { addr: addr.to_string() })])).await.context("making a one-manager cluster")?;
            tracing::info!(raft_id = id, "started a new Raft cluster with this manager as its only member");
        }
        Ok(Store { raft, id, sm, peer_tls, leader_heard: Default::default() })
    }

    /// A store in a temporary folder, for tests: a one-manager cluster, its leader elected.
    #[cfg(test)]
    pub async fn temporary() -> (Self, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(dir.path(), 1, "127.0.0.1:0", true, Network::alone(1), None).await.unwrap();
        (s, dir)
    }

    /// Commits and applies `entry`. Answers its log index.
    /// On a follower, the entry is handed to the leader (`ManagerPeer.Propose`), so any manager takes any change.
    pub async fn propose(&self, entry: Entry) -> Result<u64, ProposeError> {
        let started = tokio::time::Instant::now();
        loop {
            match self.propose_here(entry.clone()).await {
                Err(ProposeError::NotLeader(Some(addr))) => match self.forward(&addr, &entry).await {
                    // The leader changed under us: ask again who leads.
                    Err(ProposeError::NotLeader(_)) | Err(ProposeError::Unavailable(_)) if started.elapsed() < LEADER_WAIT => {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    other => return other,
                },
                Err(ProposeError::NotLeader(None)) => return Err(ProposeError::Unavailable("no leader elected".into())),
                other => return other,
            }
        }
    }

    /// `add_voter` wherever the leader is: here, or handed to it (`ManagerPeer.AddVoter`) when another manager leads.
    pub async fn add_voter_anywhere(&self, id: NodeId, addr: &str) -> Result<(), ProposeError> {
        use freightdock_proto::v1::{AddVoterRequest, manager_peer_client::ManagerPeerClient};
        match self.add_voter(id, addr).await {
            Err(ProposeError::NotLeader(Some(leader))) => {
                let tls = self.peer_tls.clone().ok_or_else(|| ProposeError::Unavailable("this manager cannot reach the others".into()))?;
                let ch = crate::tls::peer_channel(&leader, tls).map_err(|e| ProposeError::Unavailable(format!("{e:#}")))?;
                // The leader waits for the new manager to catch up with the log first, which can take a while.
                tokio::time::timeout(Duration::from_secs(120), ManagerPeerClient::new(ch).add_voter(AddVoterRequest { raft_id: id, addr: addr.to_string() }))
                    .await
                    .map_err(|_| ProposeError::Unavailable(format!("the leader at {leader} did not answer")))?
                    .map_err(|s| ProposeError::Unavailable(format!("the leader at {leader}: {}", s.message())))?;
                Ok(())
            }
            Err(ProposeError::NotLeader(None)) => Err(ProposeError::Unavailable("no leader elected".into())),
            other => other,
        }
    }

    async fn forward(&self, leader: &str, entry: &Entry) -> Result<u64, ProposeError> {
        use freightdock_proto::v1::{ProposeRequest, manager_peer_client::ManagerPeerClient};
        let tls = self.peer_tls.clone().ok_or_else(|| ProposeError::Unavailable("this manager cannot reach the others".into()))?;
        let ch = crate::tls::peer_channel(leader, tls).map_err(|e| ProposeError::Unavailable(format!("{e:#}")))?;
        let body = serde_json::to_vec(entry).map_err(|e| ProposeError::Io(e.into()))?;
        let r = tokio::time::timeout(Duration::from_secs(10), ManagerPeerClient::new(ch).propose(ProposeRequest { entry: body }))
            .await
            .map_err(|_| ProposeError::Unavailable(format!("the leader at {leader} did not answer")))?
            .map_err(|s| ProposeError::Unavailable(format!("the leader at {leader}: {}", s.message())))?
            .into_inner();
        if !r.rejected.is_empty() {
            return Err(ProposeError::Rejected(serde_json::from_slice(&r.rejected).map_err(|e| ProposeError::Io(e.into()))?));
        }
        if !r.unavailable.is_empty() {
            return Err(if r.unavailable.contains("not the leader") { ProposeError::NotLeader(None) } else { ProposeError::Unavailable(r.unavailable) });
        }
        Ok(r.index)
    }

    /// Commits `entry` through this manager's own Raft (it must lead). What `ManagerPeer.Propose` calls.
    pub async fn propose_here(&self, entry: Entry) -> Result<u64, ProposeError> {
        // Only the leader's state is sure to be current; a follower's may lag behind, so it leaves the check to the leader.
        if self.is_leader() {
            self.read(|s| s.check(&entry)).map_err(ProposeError::Rejected)?;
            self.quorum_heard()?;
        }
        let started = tokio::time::Instant::now();
        loop {
            // A leader that lost its quorum just after the check above would wait for the commit forever.
            let written = tokio::time::timeout(COMMIT_WAIT, self.raft.client_write(entry.clone())).await.map_err(|_| {
                ProposeError::Unavailable(format!("no quorum: the change was not committed within {}s (it is applied if the managers come back in time)", COMMIT_WAIT.as_secs()))
            })?;
            match written {
                Ok(r) => return r.data.0.map(|_| r.log_id.index).map_err(ProposeError::Rejected),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                    if f.leader_id.is_some() && f.leader_id != Some(self.id) {
                        return Err(ProposeError::NotLeader(f.leader_node.map(|n| n.addr)));
                    }
                    if started.elapsed() > LEADER_WAIT {
                        return Err(ProposeError::Unavailable("no leader elected".into()));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                // Dropped by a leader change before it was committed: never applied, so trying again is safe.
                Err(RaftError::APIError(ClientWriteError::LogEntryDiscarded(_))) if started.elapsed() < LEADER_WAIT => {}
                Err(RaftError::APIError(e)) => return Err(ProposeError::Unavailable(e.to_string())),
                Err(RaftError::Fatal(e)) => return Err(ProposeError::Io(anyhow::anyhow!("Raft stopped: {e}"))),
            }
        }
    }

    /// On the leader: whether a quorum of voters answered lately. When not, nothing is written, so the refusal is
    /// clean: the change was not made and will not be.
    fn quorum_heard(&self) -> Result<(), ProposeError> {
        use openraft::Instant;
        use openraft::async_runtime::WatchReceiver;
        if self.voters().len() < 2 {
            return Ok(());
        }
        let acked = self.raft.metrics().borrow_watched().last_quorum_acked;
        match acked {
            Some(at) if at.elapsed() < QUORUM_STALE => Ok(()),
            Some(at) => Err(ProposeError::Unavailable(format!("no quorum: a majority of managers has not answered for {}s", at.elapsed().as_secs()))),
            None => Err(ProposeError::Unavailable("no quorum: a majority of managers has not answered yet".into())),
        }
    }

    /// The leader's append came in (`peer.rs`).
    pub fn heard_leader(&self) {
        *self.leader_heard.lock().expect("leader heard lock") = Some(std::time::Instant::now());
    }

    /// Whether this manager is with a majority now: alone in a cluster of one; as the leader, a quorum answered
    /// lately; as a follower, the leader's appends come in. A manager that is not cannot change anything, and a worker
    /// that hears only it must not count that as hearing the cluster (G6).
    pub fn in_quorum(&self) -> bool {
        if self.voters().len() < 2 {
            return true;
        }
        match self.leader() {
            Some(l) if l == self.id => self.quorum_heard().is_ok(),
            Some(_) => self.leader_heard.lock().expect("leader heard lock").is_some_and(|at| at.elapsed() < QUORUM_STALE),
            None => false,
        }
    }

    /// Commands as this manager applies them, made here or on any other manager.
    pub fn commands(&self) -> tokio::sync::broadcast::Receiver<crate::state::CommandRecord> {
        self.sm.commands()
    }

    /// The address of the leader, as the membership has it.
    pub fn leader_addr(&self) -> Option<String> {
        let leader = self.leader()?;
        self.members().into_iter().find(|(id, _)| *id == leader).map(|(_, a)| a)
    }

    /// Every manager in the membership (voters and learners) with its address, this one first.
    pub fn members(&self) -> Vec<(NodeId, String)> {
        use openraft::async_runtime::WatchReceiver;
        let m = self.raft.metrics().borrow_watched().membership_config.clone();
        let mut v: Vec<(NodeId, String)> = m.membership().nodes().map(|(id, n)| (*id, n.addr.clone())).collect();
        v.sort_by_key(|(id, _)| (*id != self.id, *id));
        v
    }

    /// The Raft ids of the voters.
    pub fn voters(&self) -> Vec<NodeId> {
        use openraft::async_runtime::WatchReceiver;
        self.raft.metrics().borrow_watched().membership_config.membership().voter_ids().collect()
    }

    /// Makes manager `id` (at `addr`) a voter: a learner first, waited on until it has caught up with the log, then a
    /// voter, so it never joins the quorum while it cannot yet acknowledge anything. On the leader only.
    pub async fn add_voter(&self, id: NodeId, addr: &str) -> Result<(), ProposeError> {
        if self.voters().contains(&id) {
            return Ok(());
        }
        let not_leader = |e: &dyn std::fmt::Display| ProposeError::Unavailable(e.to_string());
        match self.raft.add_learner(id, BasicNode { addr: addr.to_string() }, true).await {
            Ok(_) => {}
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => return Err(ProposeError::NotLeader(f.leader_node.map(|n| n.addr))),
            Err(e) => return Err(not_leader(&e)),
        }
        match self.raft.change_membership(openraft::ChangeMembers::AddVoterIds(std::collections::BTreeSet::from([id])), false).await {
            Ok(_) => Ok(()),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => Err(ProposeError::NotLeader(f.leader_node.map(|n| n.addr))),
            Err(e) => Err(not_leader(&e)),
        }
    }

    /// The Raft id of the leader as this manager knows it now.
    pub fn leader(&self) -> Option<NodeId> {
        use openraft::async_runtime::WatchReceiver;
        self.raft.metrics().borrow_watched().current_leader
    }

    pub fn is_leader(&self) -> bool {
        self.leader() == Some(self.id)
    }

    /// Reads the applied state.
    pub fn read<T>(&self, f: impl FnOnce(&ClusterState) -> T) -> T {
        self.sm.read(f)
    }

    /// Moves a pre-Raft log (`<dir>/state.log`, one manager's JSON lines, up to G3.1) into Raft, once: each entry is
    /// proposed again in order, and the file is renamed `state.log.migrated`. Only into an empty cluster state.
    pub async fn migrate_pre_raft(&self, dir: &Path) -> Result<usize> {
        let old = dir.join("state.log");
        if !old.exists() || self.read(|s| s.cluster_id.is_some()) {
            return Ok(0);
        }
        #[derive(serde::Deserialize)]
        struct Line {
            entry: Entry,
        }
        let text = std::fs::read_to_string(&old)?;
        let mut n = 0;
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let Ok(l) = serde_json::from_str::<Line>(line) else { break };
            match self.propose(l.entry).await {
                Ok(_) => n += 1,
                Err(ProposeError::Rejected(_)) => {}
                Err(e) => return Err(anyhow::anyhow!("{e}")),
            }
        }
        std::fs::rename(&old, dir.join("state.log.migrated"))?;
        tracing::info!("moved {n} entries of the pre-Raft log into Raft");
        Ok(n)
    }

    pub async fn shutdown(&self) {
        let _ = self.raft.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn what_was_committed_is_there_after_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = Store::open(dir.path(), 7, "127.0.0.1:0", true, Network::alone(7), None).await.unwrap();
            s.propose(Entry::ClusterCreated { cluster_id: "c1".into(), at: 0 }).await.unwrap();
            s.propose(Entry::JoinTokenIssued { secret_hash: "t".into(), expires_at: 9, at: 0, for_manager: false }).await.unwrap();
            s.shutdown().await;
        }
        let s = Store::open(dir.path(), 7, "127.0.0.1:0", true, Network::alone(7), None).await.unwrap();
        // The log is applied again after the start; the restarted manager also has to win its election first.
        s.raft.wait(Some(Duration::from_secs(10))).applied_index_at_least(Some(3), "replayed").await.unwrap();
        assert_eq!(s.read(|st| (st.cluster_id.clone(), st.join_tokens.len())), (Some("c1".into()), 1));
        s.propose(Entry::JoinTokenIssued { secret_hash: "u".into(), expires_at: 9, at: 0, for_manager: false }).await.unwrap();
        assert_eq!(s.read(|st| st.join_tokens.len()), 2, "and it takes changes again");
    }

    #[tokio::test]
    async fn a_snapshot_and_the_log_after_it_read_back_after_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = Store::open(dir.path(), 7, "127.0.0.1:0", true, Network::alone(7), None).await.unwrap();
            s.propose(Entry::ClusterCreated { cluster_id: "c1".into(), at: 0 }).await.unwrap();
            s.propose(Entry::JoinTokenIssued { secret_hash: "before".into(), expires_at: 9, at: 0, for_manager: false }).await.unwrap();
            s.raft.trigger().snapshot().await.unwrap();
            s.raft.wait(Some(Duration::from_secs(5))).metrics(|m| m.snapshot.is_some(), "snapshot").await.unwrap();
            s.propose(Entry::JoinTokenIssued { secret_hash: "after".into(), expires_at: 9, at: 0, for_manager: false }).await.unwrap();
            s.shutdown().await;
        }
        assert!(dir.path().join("raft/snapshot.json").exists());
        let s = Store::open(dir.path(), 7, "127.0.0.1:0", true, Network::alone(7), None).await.unwrap();
        s.raft.wait(Some(Duration::from_secs(10))).applied_index_at_least(Some(4), "replayed").await.unwrap();
        let mut tokens: Vec<String> = s.read(|st| st.join_tokens.keys().cloned().collect());
        tokens.sort();
        assert_eq!(tokens, vec!["after", "before"]);
    }

    #[tokio::test]
    async fn a_proposal_that_would_be_refused_fails_fast() {
        let (s, _d) = Store::temporary().await;
        assert!(matches!(s.propose(Entry::JoinTokenIssued { secret_hash: "t".into(), expires_at: 9, at: 0, for_manager: false }).await, Err(ProposeError::Rejected(Rejected::NoCluster))));
    }

    #[tokio::test]
    async fn the_pre_raft_log_is_moved_in_once() {
        let dir = tempfile::tempdir().unwrap();
        let lines = [
            serde_json::json!({ "index": 1, "entry": { "type": "cluster_created", "cluster_id": "old", "at": 0 } }),
            serde_json::json!({ "index": 2, "entry": { "type": "join_token_issued", "secret_hash": "t", "expires_at": 9, "at": 0 } }),
        ];
        std::fs::write(dir.path().join("state.log"), lines.iter().map(|l| l.to_string() + "\n").collect::<String>()).unwrap();
        let s = Store::open(dir.path(), 1, "127.0.0.1:0", true, Network::alone(1), None).await.unwrap();
        assert_eq!(s.migrate_pre_raft(dir.path()).await.unwrap(), 2);
        assert_eq!(s.read(|st| st.cluster_id.clone()), Some("old".into()), "the cluster keeps its id");
        assert!(dir.path().join("state.log.migrated").exists());
        assert_eq!(s.migrate_pre_raft(dir.path()).await.unwrap(), 0);
    }
}
