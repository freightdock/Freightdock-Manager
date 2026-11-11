//! What happened to the nodes, recently, as this manager saw it: enrolled, connected, disconnected, health changes,
//! removed, commands expired. In memory, the last `KEEP` events, numbered so a reader can ask for those after the last
//! it saw (`GET /v1/events?after=`).
//!
//! Per manager on purpose for now: with Raft (G3) the events that are part of the cluster's history (enrolled,
//! removed, marked unreachable by the cluster, fenced) become log entries; connection events stay per manager.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use serde::Serialize;

use crate::sessions::Health;

pub const KEEP: usize = 1000;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Event {
    pub seq: u64,
    pub at: i64,
    pub kind: String,
    pub node_id: Option<String>,
    pub message: String,
}

#[derive(Default)]
struct Inner {
    next: u64,
    buf: VecDeque<Event>,
    health: HashMap<String, Health>,
}

#[derive(Default)]
pub struct Events {
    inner: Mutex<Inner>,
}

impl Events {
    pub fn push(&self, kind: &str, node_id: Option<&str>, message: impl Into<String>) {
        let mut g = self.inner.lock().expect("events lock");
        g.next += 1;
        let e = Event { seq: g.next, at: crate::now_ms(), kind: kind.to_string(), node_id: node_id.map(str::to_string), message: message.into() };
        tracing::debug!(seq = e.seq, kind = %e.kind, node = ?e.node_id, "{}", e.message);
        g.buf.push_back(e);
        while g.buf.len() > KEEP {
            g.buf.pop_front();
        }
    }

    /// Events after `after`, oldest first, at most `limit`.
    pub fn after(&self, after: u64, limit: usize) -> Vec<Event> {
        self.inner.lock().expect("events lock").buf.iter().filter(|e| e.seq > after).take(limit).cloned().collect()
    }

    /// The newest `n`, oldest first.
    pub fn last(&self, n: usize) -> Vec<Event> {
        let g = self.inner.lock().expect("events lock");
        g.buf.iter().skip(g.buf.len().saturating_sub(n)).cloned().collect()
    }

    /// Records each node's health now, with an event for every change since the last look. A node seen for the first
    /// time is recorded without an event (at start every node is unreachable here until it connects). Nodes no longer
    /// listed are forgotten.
    pub fn observe(&self, now: &[(String, Health)]) {
        let changes: Vec<(String, Health, Health)> = {
            let mut g = self.inner.lock().expect("events lock");
            let mut changes = vec![];
            for (id, h) in now {
                match g.health.insert(id.clone(), *h) {
                    Some(before) if before != *h => changes.push((id.clone(), before, *h)),
                    _ => {}
                }
            }
            g.health.retain(|id, _| now.iter().any(|(n, _)| n == id));
            changes
        };
        for (id, before, after) in changes {
            self.push("health", Some(&id), format!("{} to {}", name(before), name(after)));
        }
    }
}

fn name(h: Health) -> &'static str {
    match h {
        Health::Healthy => "healthy",
        Health::Suspect => "suspect",
        Health::Unreachable => "unreachable",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_are_numbered_and_read_after_a_number() {
        let e = Events::default();
        e.push("a", None, "one");
        e.push("b", Some("n"), "two");
        e.push("c", None, "three");
        let after: Vec<_> = e.after(1, 10).into_iter().map(|e| e.kind).collect();
        assert_eq!(after, vec!["b", "c"]);
        assert_eq!(e.last(1)[0].kind, "c");
    }

    #[test]
    fn only_the_newest_are_kept() {
        let e = Events::default();
        for i in 0..KEEP + 5 {
            e.push("x", None, i.to_string());
        }
        let all = e.after(0, usize::MAX);
        assert_eq!(all.len(), KEEP);
        assert_eq!(all[0].seq, 6);
    }

    #[test]
    fn a_health_change_is_an_event_the_first_look_is_not() {
        let e = Events::default();
        e.observe(&[("n".into(), Health::Unreachable)]);
        assert!(e.last(10).is_empty());
        e.observe(&[("n".into(), Health::Healthy)]);
        e.observe(&[("n".into(), Health::Healthy)]);
        e.observe(&[("n".into(), Health::Suspect)]);
        let m: Vec<_> = e.last(10).into_iter().map(|e| e.message).collect();
        assert_eq!(m, vec!["unreachable to healthy", "healthy to suspect"]);
    }
}
