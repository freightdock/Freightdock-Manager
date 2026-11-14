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

use std::collections::BTreeMap;

use crate::state::{ClusterState, CommandRecord, CommandStatus, FenceState, Op, StorageDriver};
use crate::{Manager, now_ms};

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

/// The evictions in hand, by what is owed.
pub type Evictions = BTreeMap<Due, Tracked>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::Secret;
    use crate::state::tests::{next, with_nodes};
    use crate::state::{CephKeys, Entry, Storage};

    fn cephfs(fence: bool) -> Storage {
        let keys = CephKeys { secret: Secret("AQBtb3VudA==".into()), fence_client: if fence { "fd-fence".into() } else { String::new() }, fence_secret: fence.then(|| Secret("AgBmZW5jZQ==".into())), image: String::new() };
        Storage { name: if fence { "ceph".into() } else { "quiet".into() }, driver: StorageDriver::Cephfs, source: "192.0.2.1:/".into(), options: "name=fd,fs=fs1".into(), added_at: 1, ceph: Some(keys) }
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
