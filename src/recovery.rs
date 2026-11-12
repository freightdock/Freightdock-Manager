//! Recovery (G7): the workloads of a fenced node are started again elsewhere, by kind (plan 5.2).
//!
//! The leader looks at every node whose fencing has ended in fenced (and, after `FenceConfig::recovery_delay_s`,
//! still is), or which came back fenced and waits for authorization: its stateless workloads are placed on other
//! nodes with `stacks::place`, as a deploy would; pinned ones (`x-freightdock: {ha: false}`) and ones with volumes
//! (their data is on that node) stay, and the history says why. Workloads whose node was removed get a node too.
//!
//! Only a fenced node gives up workloads, and the state checks that again when it applies the move
//! (`Entry::WorkloadsRecovered`), so no manager can move a workload whose old copy may still run. When the node is
//! back and authorized, its reconcile removes the containers of the workloads that moved: the state no longer
//! places them there (duplicate prevention), and the fence kept them stopped until then.

use std::collections::{BTreeMap, BTreeSet};

use crate::state::{ClusterState, Entry, FenceState, Ms, Node, RecoveryKind};
use crate::{Manager, now_ms};

/// What to do with the workloads on `from` (or, for `None`, those with no node): where each one that can go goes,
/// and why each one that stays, stays.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub moves: BTreeMap<String, String>,
    pub left: BTreeMap<String, String>,
}

pub fn plan(s: &ClusterState, from: Option<&str>, eligible: &[String]) -> Plan {
    let mut out = Plan::default();
    let mut s = s.clone();
    let mut go: BTreeSet<String> = BTreeSet::new();
    for w in s.workloads.values().filter(|w| w.node_id.as_deref() == from && s.stacks.contains_key(&w.stack)) {
        match (from, s.recovery_kind(w)) {
            (Some(_), RecoveryKind::Pinned) => {
                out.left.insert(w.id.clone(), "pinned (x-freightdock ha: false): it waits for its node".into());
            }
            (Some(_), RecoveryKind::LocalData) => {
                out.left.insert(w.id.clone(), "its volumes are on its node: it waits for the node".into());
            }
            _ => {
                go.insert(w.id.clone());
            }
        }
    }
    // As if their node were gone, so `place` finds them a new one.
    for id in &go {
        s.workloads.get_mut(id).expect("listed").node_id = None;
    }
    let stacks: BTreeSet<String> = go.iter().map(|id| s.workloads[id].stack.clone()).collect();
    for stack in stacks {
        let services = s.stacks[&stack].services.clone();
        match crate::stacks::place(&s, &stack, &services, eligible) {
            Ok(p) => {
                for (id, node) in p.into_iter().filter(|(id, _)| go.contains(id)) {
                    s.workloads.get_mut(&id).expect("placed").node_id = Some(node.clone());
                    out.moves.insert(id, node);
                }
            }
            Err(why) => {
                for id in go.iter().filter(|id| s.workloads[*id].stack == stack) {
                    out.left.insert(id.clone(), why.clone());
                }
            }
        }
    }
    out
}

/// Whether the leader recovers `n`'s workloads now: fenced for at least `delay_ms`, or back and fenced.
pub fn due(n: &Node, now: Ms, delay_ms: Ms) -> bool {
    match n.fencing.state {
        FenceState::Fenced => now - n.fencing.since >= delay_ms,
        FenceState::AwaitingAuthorization => true,
        _ => false,
    }
}

impl Manager {
    /// Nodes a workload may be placed on now: with Docker, healthy as the managers agree, and not in fencing.
    pub fn eligible(&self, s: &ClusterState) -> Vec<String> {
        s.nodes
            .values()
            .filter(|n| n.report.as_ref().is_some_and(|r| r.docker) && n.fencing.state == FenceState::None && self.sessions.liveness(&n.id).health == crate::sessions::Health::Healthy)
            .map(|n| n.id.clone())
            .collect()
    }

    /// The leader's recovery round (see the module), each second after fencing's.
    pub async fn recover_round(&self) {
        if self.store.leader() != Some(self.store.id) || self.started.elapsed() < crate::fencing::STARTUP_GRACE {
            return;
        }
        let delay = self.fencing.recovery_delay_s as Ms * 1000;
        let now = now_ms();
        let todo: Vec<(Option<String>, Plan)> = self.store.read(|s| {
            let eligible = self.eligible(s);
            let mut todo = vec![];
            for n in s.nodes.values().filter(|n| due(n, now, delay)) {
                if s.workloads_on(&n.id).next().is_none() {
                    continue;
                }
                let mut p = plan(s, Some(&n.id), &eligible);
                // The first time says what stays and why; after that only a move is worth an entry (one that had
                // nowhere to go is tried again each round).
                if n.recovered {
                    p.left.clear();
                }
                if !p.moves.is_empty() || !n.recovered {
                    todo.push((Some(n.id.clone()), p));
                }
            }
            if s.workloads.values().any(|w| w.node_id.is_none()) {
                let p = plan(s, None, &eligible);
                if !p.moves.is_empty() {
                    todo.push((None, Plan { moves: p.moves, left: BTreeMap::new() }));
                }
            }
            todo
        });
        for (from, p) in todo {
            let what = format!(
                "{}{}",
                p.moves.iter().map(|(w, n)| format!("{w} to {}", &n[..n.len().min(8)])).collect::<Vec<_>>().join(", "),
                p.left.iter().map(|(w, why)| format!("; {w} stays: {why}")).collect::<String>()
            );
            let e = Entry::WorkloadsRecovered { from: from.clone(), moves: p.moves, left: p.left, at: now };
            match self.store.propose(e).await {
                Ok(_) => {
                    tracing::warn!(from = ?from, "recovery: {what}");
                    self.events.push("recovery", from.as_deref(), what.trim_start_matches("; ").to_string());
                }
                Err(e) => tracing::debug!(from = ?from, "recovery not made: {e}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::tests::spec;
    use crate::state::{Fencing, ServiceSpec, Stack, Workload};

    fn node(id: &str, state: FenceState, since: Ms) -> Node {
        Node { id: id.into(), hostname: id.into(), cert_sha256: String::new(), enrolled_at: 0, report: None, reported_at: None, labels: Default::default(), fencing: Fencing { state, since, provider: "self".into(), message: String::new() }, recovered: false }
    }

    /// Nodes a, b, c; stack `app` with web x2 (a, b), db with a volume (a), cron pinned (a), and `solo` on port 80 x1 (a).
    fn cluster() -> ClusterState {
        let mut s = crate::state::tests::cluster();
        for id in ["a", "b", "c"] {
            s.nodes.insert(id.into(), node(id, FenceState::None, 0));
        }
        s.nodes.get_mut("a").unwrap().fencing.state = FenceState::Fenced;
        let services: BTreeMap<String, ServiceSpec> = [
            ("web".to_string(), spec("nginx", 2)),
            ("db".to_string(), ServiceSpec { volumes: vec!["data:/data".into()], ..spec("redis", 1) }),
            ("cron".to_string(), ServiceSpec { ha: false, ..spec("cron", 1) }),
        ]
        .into();
        s.stacks.insert("app".into(), Stack { name: "app".into(), compose: String::new(), services: services.clone(), stopped: false, created_at: 0, updated_at: 0 });
        for (id, svc, slot, on) in [("app.web.1", "web", 1, "a"), ("app.web.2", "web", 2, "b"), ("app.db.1", "db", 1, "a"), ("app.cron.1", "cron", 1, "a")] {
            let sp = services[svc].for_workload();
            s.workloads.insert(id.into(), Workload { id: id.into(), stack: "app".into(), service: svc.into(), slot, node_id: Some(on.into()), spec: sp, generation: 1 });
        }
        s
    }

    fn eligible() -> Vec<String> {
        vec!["b".into(), "c".into()]
    }

    #[test]
    fn stateless_workloads_move_to_where_a_deploy_would_put_them_and_the_rest_stay_with_why() {
        let p = plan(&cluster(), Some("a"), &eligible());
        assert_eq!(p.moves, BTreeMap::from([("app.web.1".to_string(), "c".to_string())]), "away from its other replica on b");
        assert_eq!(p.left.keys().collect::<Vec<_>>(), ["app.cron.1", "app.db.1"]);
        assert!(p.left["app.cron.1"].contains("pinned"));
        assert!(p.left["app.db.1"].contains("volumes"));
    }

    #[test]
    fn a_workload_with_nowhere_to_go_stays_with_the_reason() {
        let p = plan(&cluster(), Some("a"), &[]);
        assert!(p.moves.is_empty());
        assert!(p.left["app.web.1"].contains("no node can take app.web.1"), "{:?}", p.left);
    }

    #[test]
    fn a_removed_node_s_workloads_all_get_a_node() {
        let mut s = cluster();
        for w in s.workloads.values_mut().filter(|w| w.node_id.as_deref() == Some("a")) {
            w.node_id = None;
        }
        let p = plan(&s, None, &eligible());
        assert_eq!(p.moves.keys().collect::<Vec<_>>(), ["app.cron.1", "app.db.1", "app.web.1"], "the node is gone: nothing to wait for");
        assert!(p.left.is_empty());
    }

    #[test]
    fn recovery_waits_for_fenced_and_the_delay() {
        assert!(!due(&node("n", FenceState::InProgress, 0), 1 << 40, 0), "in progress is not fenced");
        assert!(!due(&node("n", FenceState::Failed, 0), 1 << 40, 0));
        assert!(!due(&node("n", FenceState::Fenced, 1_000), 30_999, 30_000));
        assert!(due(&node("n", FenceState::Fenced, 1_000), 31_000, 30_000));
        assert!(due(&node("n", FenceState::AwaitingAuthorization, 1_000), 1_000, 30_000), "back and fenced: its containers are stopped");
        assert!(!due(&node("n", FenceState::Authorized, 0), 1 << 40, 0));
    }
}
