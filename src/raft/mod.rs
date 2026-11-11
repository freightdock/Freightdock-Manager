//! Consensus: openraft over the cluster state.
//!
//! - `TypeConfig`: the log carries `state::Entry`; applying one answers `Applied` (applied, or refused and why).
//! - `log.rs`: the Raft log and vote, one fsynced file of records, compacted now and then.
//! - `sm.rs`: the state machine (`ClusterState`) and its snapshots.
//! - `network.rs`: how this manager reaches the other managers (G3.2; a single manager needs none).
//!
//! `store.rs` is the face the rest of the manager sees: `propose` and `read`, as before Raft.

pub mod log;
pub mod network;
pub mod sm;

use serde::{Deserialize, Serialize};

use crate::state::{Entry, Rejected};

/// What applying an entry answered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Applied(pub Result<(), Rejected>);

impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = serde_json::to_value(self).ok().and_then(|v| v["type"].as_str().map(str::to_string)).unwrap_or_default();
        f.write_str(&s)
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Entry,
        R = Applied,
        Node = openraft::BasicNode,
);

pub type NodeId = u64;
pub type Raft = openraft::Raft<TypeConfig, sm::StateMachine>;
pub type LogId = openraft::type_config::alias::LogIdOf<TypeConfig>;
pub type Vote = openraft::type_config::alias::VoteOf<TypeConfig>;
pub type RaftEntry = openraft::type_config::alias::EntryOf<TypeConfig>;
pub type Membership = openraft::type_config::alias::StoredMembershipOf<TypeConfig>;
pub type SnapshotMeta = openraft::type_config::alias::SnapshotMetaOf<TypeConfig>;
pub type Snapshot = openraft::type_config::alias::SnapshotOf<TypeConfig, sm::SnapshotData>;

/// Timings for managers on one LAN: a heartbeat every 250 ms, a new election after 1 to 2 s without one.
pub fn config() -> openraft::Config {
    openraft::Config {
        cluster_name: "freightdock".into(),
        heartbeat_interval: 250,
        election_timeout_min: 1000,
        election_timeout_max: 2000,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
        max_in_snapshot_log_to_keep: 100,
        ..Default::default()
    }
}

pub fn io_err(e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::StorageError;
    use openraft::testing::log::{StoreBuilder, Suite};
    use openraft::type_config::TypeConfigExt;

    struct OnDisk;

    impl StoreBuilder<TypeConfig, log::LogStore, sm::StateMachine, tempfile::TempDir> for OnDisk {
        async fn build(&self) -> Result<(tempfile::TempDir, log::LogStore, sm::StateMachine), StorageError<TypeConfig>> {
            let dir = tempfile::tempdir().unwrap();
            let l = log::LogStore::open(&dir.path().join("raft")).unwrap();
            let s = sm::StateMachine::open(&dir.path().join("raft")).unwrap();
            Ok((dir, l, s))
        }
    }

    /// openraft's conformance suite for log stores and state machines, against ours on disk.
    #[test]
    fn the_store_passes_openraft_s_suite() {
        TypeConfig::run(async {
            Suite::test_all(OnDisk).await.unwrap();
        });
    }
}
