//! The workers connected to this manager, and how long since each was heard from, merged with what the other managers
//! report of theirs (`ManagerPeer.Heard`, asked about once a second), so a node is unreachable only when no manager
//! hears it. In memory only: liveness is never written to the log (`state.rs` says why).
//!
//! Unreachable does not mean stopped. A worker this manager calls unreachable may be running its containers still, cut
//! off from the managers; nothing here may treat it as fenced.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::mpsc;

use freightdock_proto::v1::{HeardNode, ManagerMessage, NodeResources};

#[derive(Debug, Clone, Copy)]
pub struct Thresholds {
    pub heartbeat: Duration,
    pub suspect: Duration,
    pub unreachable: Duration,
}

impl Default for Thresholds {
    fn default() -> Self {
        Thresholds { heartbeat: Duration::from_secs(2), suspect: Duration::from_secs(5), unreachable: Duration::from_secs(12) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Health {
    /// Heard from within the suspect threshold.
    Healthy,
    /// Missed a couple of heartbeats; maybe a blip.
    Suspect,
    /// Not heard from for longer than the unreachable threshold, or never connected to this manager.
    Unreachable,
}

/// The health of a node last heard from `since` ago (None: not connected here).
pub fn health(since: Option<Duration>, t: &Thresholds) -> Health {
    match since {
        Some(d) if d < t.suspect => Health::Healthy,
        Some(d) if d < t.unreachable => Health::Suspect,
        _ => Health::Unreachable,
    }
}

pub struct Session {
    /// Which stream this is, so an old stream ending after a new one opened does not take the new one away.
    pub generation: u64,
    pub tx: mpsc::Sender<Result<ManagerMessage, tonic::Status>>,
    pub last_heard: Instant,
    pub connected_at: Instant,
    pub resources: Option<NodeResources>,
    pub peer: String,
}

/// Ends a stream with a status. Dropping the sender is not enough: the task reading the worker's side holds a clone.
fn end(s: &Session, why: &str) {
    let _ = s.tx.try_send(Err(tonic::Status::unavailable(why)));
}

#[derive(Debug, Clone, Serialize)]
pub struct Liveness {
    pub health: Health,
    /// The manager that heard it last, when not this one.
    pub via: Option<String>,
    pub last_heard_ms_ago: Option<u64>,
    pub connected_for_ms: Option<u64>,
    pub peer: Option<String>,
    pub cpu_percent: Option<f32>,
    pub memory_used_bytes: Option<u64>,
    pub disk_total_bytes: Option<u64>,
    pub disk_used_bytes: Option<u64>,
    pub containers_running: Option<u32>,
}

#[derive(Default)]
pub struct Sessions {
    inner: Mutex<(u64, HashMap<String, Session>)>,
    /// Per other manager (by address): when it last reported, and what.
    remote: Mutex<HashMap<String, (Instant, Vec<HeardNode>)>>,
    pub thresholds: Thresholds,
}

impl Sessions {
    pub fn new(thresholds: Thresholds) -> Self {
        Sessions { inner: Mutex::default(), remote: Mutex::default(), thresholds }
    }

    /// A new stream for `node`, replacing any older one (whose sender is dropped, which ends it). Answers its
    /// generation.
    pub fn open(&self, node: &str, tx: mpsc::Sender<Result<ManagerMessage, tonic::Status>>, peer: String) -> u64 {
        let mut g = self.inner.lock().expect("sessions lock");
        g.0 += 1;
        let generation = g.0;
        let now = Instant::now();
        if let Some(old) = g.1.insert(node.to_string(), Session { generation, tx, last_heard: now, connected_at: now, resources: None, peer }) {
            end(&old, "a newer stream from this node replaced this one");
        }
        generation
    }

    /// Ends `node`'s stream if it is still the one of `generation`.
    pub fn close(&self, node: &str, generation: u64) {
        let mut g = self.inner.lock().expect("sessions lock");
        if g.1.get(node).is_some_and(|s| s.generation == generation) {
            g.1.remove(node);
        }
    }

    /// Ends `node`'s stream whatever its generation (the node was removed).
    pub fn kick(&self, node: &str) {
        if let Some(s) = self.inner.lock().expect("sessions lock").1.remove(node) {
            end(&s, "this node was removed from the cluster");
        }
    }

    /// Ends every stream (the manager is stopping): each worker goes to another manager at once instead of waiting
    /// for this one to fall silent.
    pub fn kick_all(&self) {
        for (_, s) in self.inner.lock().expect("sessions lock").1.drain() {
            end(&s, "this manager is stopping");
        }
    }

    pub fn heard(&self, node: &str, generation: u64, resources: Option<NodeResources>) {
        let mut g = self.inner.lock().expect("sessions lock");
        if let Some(s) = g.1.get_mut(node).filter(|s| s.generation == generation) {
            s.last_heard = Instant::now();
            if resources.is_some() {
                s.resources = resources;
            }
        }
    }

    pub fn sender(&self, node: &str) -> Option<mpsc::Sender<Result<ManagerMessage, tonic::Status>>> {
        self.inner.lock().expect("sessions lock").1.get(node).map(|s| s.tx.clone())
    }

    /// What manager `via` hears now; replaces what it said before.
    pub fn reports(&self, via: &str, nodes: Vec<HeardNode>) {
        self.remote.lock().expect("remote lock").insert(via.to_string(), (Instant::now(), nodes));
    }

    /// Forgets a manager's reports (it left the cluster).
    pub fn forget_reports(&self, keep: &[String]) {
        self.remote.lock().expect("remote lock").retain(|via, _| keep.contains(via));
    }

    /// The workers connected to this manager, for the others.
    pub fn connected(&self) -> Vec<HeardNode> {
        let g = self.inner.lock().expect("sessions lock");
        g.1.iter()
            .map(|(id, s)| HeardNode { node_id: id.clone(), heard_ms_ago: s.last_heard.elapsed().as_millis() as u64, connected_for_ms: s.connected_at.elapsed().as_millis() as u64, peer: s.peer.clone(), resources: s.resources })
            .collect()
    }

    /// The node's liveness from the freshest word any manager has of it: this one's own session, or another's report
    /// (aged by how long ago that report came).
    pub fn liveness(&self, node: &str) -> Liveness {
        let mine = {
            let g = self.inner.lock().expect("sessions lock");
            g.1.get(node).map(|s| (None, s.last_heard.elapsed(), s.connected_at.elapsed(), s.peer.clone(), s.resources))
        };
        let theirs = {
            let g = self.remote.lock().expect("remote lock");
            g.iter()
                .filter_map(|(via, (at, nodes))| nodes.iter().find(|n| n.node_id == node).map(|n| (Some(via.clone()), Duration::from_millis(n.heard_ms_ago) + at.elapsed(), Duration::from_millis(n.connected_for_ms), n.peer.clone(), n.resources)))
                .min_by_key(|c| c.1)
        };
        let best = match (mine, theirs) {
            (Some(m), Some(t)) => Some(if t.1 < m.1 { t } else { m }),
            (m, t) => m.or(t),
        };
        let since = best.as_ref().map(|b| b.1);
        let r = best.as_ref().and_then(|b| b.4.as_ref());
        Liveness {
            health: health(since, &self.thresholds),
            via: best.as_ref().and_then(|b| b.0.clone()),
            last_heard_ms_ago: since.map(|d| d.as_millis() as u64),
            connected_for_ms: best.as_ref().map(|b| b.2.as_millis() as u64),
            peer: best.as_ref().map(|b| b.3.clone()),
            cpu_percent: r.map(|r| r.cpu_percent),
            memory_used_bytes: r.map(|r| r.memory_used_bytes),
            disk_total_bytes: r.map(|r| r.disk_total_bytes),
            disk_used_bytes: r.map(|r| r.disk_used_bytes),
            containers_running: r.map(|r| r.containers_running),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_follows_the_thresholds() {
        let t = Thresholds::default();
        assert_eq!(health(Some(Duration::from_secs(1)), &t), Health::Healthy);
        assert_eq!(health(Some(Duration::from_secs(5)), &t), Health::Suspect);
        assert_eq!(health(Some(Duration::from_secs(11)), &t), Health::Suspect);
        assert_eq!(health(Some(Duration::from_secs(12)), &t), Health::Unreachable);
        assert_eq!(health(None, &t), Health::Unreachable, "never connected here");
    }

    #[test]
    fn an_old_stream_ending_leaves_the_new_one() {
        let s = Sessions::default();
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let g1 = s.open("n", tx1, "a".into());
        let g2 = s.open("n", tx2, "b".into());
        s.close("n", g1);
        assert_eq!(s.liveness("n").peer.as_deref(), Some("b"));
        s.close("n", g2);
        assert_eq!(s.liveness("n").health, Health::Unreachable);
    }

    fn heard(node: &str, ms: u64) -> HeardNode {
        HeardNode { node_id: node.into(), heard_ms_ago: ms, connected_for_ms: 60_000, peer: "10.0.0.9:1".into(), resources: None }
    }

    #[test]
    fn a_node_another_manager_hears_is_not_unreachable() {
        let s = Sessions::default();
        assert_eq!(s.liveness("n").health, Health::Unreachable);
        s.reports("m2:7450", vec![heard("n", 7_000)]);
        s.reports("m3:7450", vec![heard("n", 500)]);
        let l = s.liveness("n");
        assert_eq!(l.health, Health::Healthy, "the freshest word counts: {l:?}");
        assert_eq!(l.via.as_deref(), Some("m3:7450"));
        assert_eq!(l.peer.as_deref(), Some("10.0.0.9:1"));
        // Its own session wins when it is fresher.
        let (tx, _rx) = mpsc::channel(1);
        s.open("n", tx, "here".into());
        let l = s.liveness("n");
        assert_eq!((l.via, l.peer.as_deref()), (None, Some("here")));
        assert_eq!(s.connected().len(), 1, "what it tells the others is only its own");
    }

    #[test]
    fn a_report_that_stopped_coming_counts_for_nothing() {
        let t = Thresholds { heartbeat: Duration::from_millis(10), suspect: Duration::from_millis(30), unreachable: Duration::from_millis(60) };
        let s = Sessions::new(t);
        s.reports("m2:7450", vec![heard("n", 0)]);
        assert_eq!(s.liveness("n").health, Health::Healthy);
        // The report ages as it sits here: m2 last said so 70 ms ago.
        std::thread::sleep(Duration::from_millis(70));
        assert_eq!(s.liveness("n").health, Health::Unreachable);
    }
}
