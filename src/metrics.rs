//! The cluster's metrics in Prometheus' text format (G12): `GET /v1/metrics`, asked of any manager with a read token
//! (over the TLS API, with the cluster CA), so one scrape sees the whole cluster as that manager knows it: managers,
//! nodes and their health, fencing and resources, stacks and their workloads, shared storage per node, floating
//! addresses, notification channels. Read from the replicated state and the managers' shared view of the workers;
//! nothing is counted here, so a manager that restarts or a new leader shows the same.

use std::collections::BTreeMap;
use std::fmt::Write;

use crate::sessions::Health;

/// One sample: its labels and value.
type Sample = (Vec<(&'static str, String)>, f64);

/// A metric family: name, help, type and samples.
pub struct Family {
    pub name: &'static str,
    pub help: &'static str,
    pub kind: &'static str,
    pub samples: Vec<Sample>,
}

impl Family {
    fn gauge(name: &'static str, help: &'static str) -> Family {
        Family { name, help, kind: "gauge", samples: vec![] }
    }

    fn add(&mut self, labels: Vec<(&'static str, String)>, v: f64) -> &mut Self {
        self.samples.push((labels, v));
        self
    }
}

/// A label value as the format wants it: `\`, `"` and a newline escaped.
fn escape(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

/// The families in the text format, each with its HELP and TYPE, one line a sample.
pub fn render(families: &[Family]) -> String {
    let mut out = String::new();
    for f in families {
        let _ = writeln!(out, "# HELP {} {}", f.name, f.help);
        let _ = writeln!(out, "# TYPE {} {}", f.name, f.kind);
        for (labels, v) in &f.samples {
            out.push_str(f.name);
            if !labels.is_empty() {
                let l: Vec<String> = labels.iter().map(|(k, v)| format!("{k}=\"{}\"", escape(v))).collect();
                let _ = write!(out, "{{{}}}", l.join(","));
            }
            let _ = writeln!(out, " {}", if v.fract() == 0.0 && v.abs() < 1e15 { format!("{}", *v as i64) } else { format!("{v}") });
        }
    }
    out
}

/// A node as the metrics see it.
#[derive(Debug, Clone, Default)]
pub struct NodeM {
    pub id: String,
    pub name: String,
    pub health: Option<Health>,
    pub fencing: String,
    pub cpu_percent: Option<f32>,
    pub memory_used_bytes: Option<u64>,
    pub memory_total_bytes: Option<u64>,
    pub containers_running: Option<u32>,
}

/// What the metrics are made from.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub manager: String,
    pub leader: bool,
    pub applied: u64,
    /// Every manager: its address and whether it answers.
    pub managers: Vec<(String, bool)>,
    pub nodes: Vec<NodeM>,
    /// Each stack: whether it is stopped, its workloads wanted and running.
    pub stacks: Vec<(String, bool, u64, u64)>,
    /// Each storage on each node: storage, node id, node name, ok, free, total, probe ms.
    pub storage: Vec<(String, String, String, bool, u64, u64, u32)>,
    /// Each floating address and whether a node holds it.
    pub floating: Vec<(String, bool)>,
    /// Each notification channel: name, kind, enabled, failing.
    pub channels: Vec<(String, String, bool, bool)>,
    pub recoveries: u64,
}

fn b(v: bool) -> f64 {
    if v { 1.0 } else { 0.0 }
}

/// The families for a snapshot.
pub fn families(s: &Snapshot) -> Vec<Family> {
    let me = || ("manager", s.manager.clone());
    let mut up = Family::gauge("freightdock_manager_up", "This manager answers; with its address.");
    up.add(vec![me()], 1.0);
    let mut leader = Family::gauge("freightdock_manager_leader", "Whether this manager is the Raft leader.");
    leader.add(vec![me()], b(s.leader));
    let mut applied = Family::gauge("freightdock_raft_applied_index", "The last Raft log entry this manager applied.");
    applied.add(vec![me()], s.applied as f64);
    let mut managers = Family::gauge("freightdock_managers_online", "Each manager, 1 while it answers the others (this one always).");
    for (addr, on) in &s.managers {
        managers.add(vec![("addr", addr.clone())], b(*on));
    }

    let mut health = Family::gauge("freightdock_node_health", "Each node's health as the managers agree: 1 for the state it is in.");
    let mut fencing = Family::gauge("freightdock_node_fencing", "Each node's fencing state: 1 for the state it is in (none while it is well).");
    let mut cpu = Family::gauge("freightdock_node_cpu_percent", "Each node's CPU use over all its CPUs, 0 to 100.");
    let mut mem = Family::gauge("freightdock_node_memory_used_bytes", "Each node's memory in use.");
    let mut memt = Family::gauge("freightdock_node_memory_total_bytes", "Each node's memory.");
    let mut cont = Family::gauge("freightdock_node_containers_running", "Containers running on each node, the cluster's and others.");
    for n in &s.nodes {
        let l = || vec![("node", n.id.clone()), ("name", n.name.clone())];
        for (h, word) in [(Health::Healthy, "healthy"), (Health::Suspect, "suspect"), (Health::Unreachable, "unreachable")] {
            let mut x = l();
            x.push(("state", word.into()));
            health.add(x, b(n.health == Some(h)));
        }
        let mut x = l();
        x.push(("state", n.fencing.clone()));
        fencing.add(x, 1.0);
        if let Some(v) = n.cpu_percent {
            cpu.add(l(), (f64::from(v) * 10.0).round() / 10.0);
        }
        if let Some(v) = n.memory_used_bytes {
            mem.add(l(), v as f64);
        }
        if let Some(v) = n.memory_total_bytes {
            memt.add(l(), v as f64);
        }
        if let Some(v) = n.containers_running {
            cont.add(l(), f64::from(v));
        }
    }

    let mut wanted = Family::gauge("freightdock_stack_workloads_wanted", "Each stack's workloads that should run (none while it is stopped).");
    let mut running = Family::gauge("freightdock_stack_workloads_running", "Each stack's workloads whose container runs on the node they are placed on.");
    let mut stopped = Family::gauge("freightdock_stack_stopped", "Whether each stack is stopped on purpose.");
    for (name, st, w, r) in &s.stacks {
        wanted.add(vec![("stack", name.clone())], *w as f64);
        running.add(vec![("stack", name.clone())], *r as f64);
        stopped.add(vec![("stack", name.clone())], b(*st));
    }

    let mut sok = Family::gauge("freightdock_storage_ok", "Each shared storage on each node that uses it: 1 while it is mounted, writable and answering.");
    let mut sfree = Family::gauge("freightdock_storage_free_bytes", "Each shared storage's free space as each node sees it.");
    let mut stotal = Family::gauge("freightdock_storage_total_bytes", "Each shared storage's size as each node sees it.");
    let mut sprobe = Family::gauge("freightdock_storage_probe_seconds", "How long each node's last probe of each storage took.");
    for (st, id, name, ok, free, total, ms) in &s.storage {
        let l = || vec![("storage", st.clone()), ("node", id.clone()), ("name", name.clone())];
        sok.add(l(), b(*ok));
        if *ok {
            sfree.add(l(), *free as f64);
            stotal.add(l(), *total as f64);
        }
        sprobe.add(l(), f64::from(*ms) / 1000.0);
    }

    let mut floating = Family::gauge("freightdock_floating_held", "Each floating address: 1 while a node holds it.");
    for (a, held) in &s.floating {
        floating.add(vec![("address", a.clone())], b(*held));
    }
    let mut channels = Family::gauge("freightdock_notification_channel_failing", "Each notification channel: 1 while its last delivery failed.");
    for (name, kind, enabled, failing) in &s.channels {
        channels.add(vec![("channel", name.clone()), ("kind", kind.clone()), ("enabled", enabled.to_string())], b(*failing));
    }
    let mut rec = Family::gauge("freightdock_recoveries_recorded", "Recoveries in the history the managers keep (the latest 200).");
    rec.add(vec![], s.recoveries as f64);

    vec![up, leader, applied, managers, health, fencing, cpu, mem, memt, cont, wanted, running, stopped, sok, sfree, stotal, sprobe, floating, channels, rec]
}

impl crate::Manager {
    /// The snapshot the metrics are made from, now.
    pub fn metrics_snapshot(&self) -> Snapshot {
        let me = self.advertise.first().cloned().unwrap_or_else(|| self.manager_id.clone());
        let mut managers: Vec<(String, bool)> = self.store.members().into_iter().map(|(id, a)| {
            let on = id == self.store.id || self.sessions.manager_online(&a);
            (a, on)
        }).collect();
        managers.sort();
        let (mut snap, placed) = self.store.read(|s| {
            let nodes = s
                .nodes
                .values()
                .map(|n| NodeM { id: n.id.clone(), name: n.display_name(), fencing: serde_json::to_value(n.fencing.state).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default(), memory_total_bytes: n.report.as_ref().map(|r| r.memory_total_bytes), ..Default::default() })
                .collect();
            // Each stack's workloads that should run: (stack, workload, node, generation).
            let placed: Vec<(String, String, String, u64)> = s
                .workloads
                .values()
                .filter(|w| s.stacks.get(&w.stack).is_some_and(|st| !st.stopped))
                .filter_map(|w| Some((w.stack.clone(), w.id.clone(), w.node_id.clone()?, w.generation)))
                .collect();
            let stacks = s.stacks.values().map(|st| (st.name.clone(), st.stopped, 0, 0)).collect();
            let floating = s.floating.iter().map(|(a, f)| (a.clone(), f.holder.is_some())).collect();
            let channels = s.notifications.values().map(|c| (c.name.clone(), c.kind.clone(), c.enabled, c.last_error.is_some())).collect();
            (Snapshot { manager: me, leader: self.store.is_leader(), applied: s.applied, managers, nodes, stacks, floating, channels, recoveries: s.recoveries.len() as u64, ..Default::default() }, placed)
        });
        let mut reports: BTreeMap<String, Vec<freightdock_proto::v1::WorkloadState>> = BTreeMap::new();
        for n in &mut snap.nodes {
            let l = self.sessions.liveness(&n.id);
            n.health = Some(l.health);
            n.cpu_percent = l.cpu_percent;
            n.memory_used_bytes = l.memory_used_bytes;
            n.containers_running = l.containers_running;
            reports.insert(n.id.clone(), self.sessions.reported(&n.id).unwrap_or_default());
            for h in self.sessions.storage(&n.id).map(|r| r.storages).unwrap_or_default() {
                snap.storage.push((h.name, n.id.clone(), n.name.clone(), h.ok, h.free_bytes, h.total_bytes, h.probe_ms));
            }
        }
        let mut counts: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        for (stack, id, node, generation) in placed {
            let c = counts.entry(stack).or_default();
            c.0 += 1;
            if reports.get(&node).is_some_and(|r| r.iter().any(|w| w.workload_id == id && w.state == "running" && w.generation == generation)) {
                c.1 += 1;
            }
        }
        for st in &mut snap.stacks {
            let (w, r) = counts.get(&st.0).copied().unwrap_or_default();
            (st.2, st.3) = (w, r);
        }
        snap
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_text_format_has_help_type_and_escaped_labels() {
        let mut f = Family::gauge("freightdock_x", "An example.");
        f.add(vec![("name", "a \"b\"\\c\nd".into())], 2.0).add(vec![], 0.25);
        let text = render(&[f]);
        assert_eq!(text, "# HELP freightdock_x An example.\n# TYPE freightdock_x gauge\nfreightdock_x{name=\"a \\\"b\\\"\\\\c\\nd\"} 2\nfreightdock_x 0.25\n");
    }

    #[test]
    fn a_snapshot_says_each_node_s_state_stack_counts_and_storage() {
        let s = Snapshot {
            manager: "m1:7450".into(),
            leader: true,
            applied: 42,
            managers: vec![("m1:7450".into(), true), ("m2:7450".into(), false)],
            nodes: vec![NodeM { id: "n1".into(), name: "w1".into(), health: Some(Health::Suspect), fencing: "none".into(), cpu_percent: Some(12.345), ..Default::default() }],
            stacks: vec![("web".into(), false, 3, 2)],
            storage: vec![("nas".into(), "n1".into(), "w1".into(), false, 0, 0, 10_000)],
            floating: vec![("192.0.2.10".into(), true)],
            channels: vec![("ops".into(), "ntfy".into(), true, true)],
            recoveries: 4,
        };
        let text = render(&families(&s));
        for line in [
            "freightdock_manager_leader{manager=\"m1:7450\"} 1",
            "freightdock_raft_applied_index{manager=\"m1:7450\"} 42",
            "freightdock_managers_online{addr=\"m2:7450\"} 0",
            "freightdock_node_health{node=\"n1\",name=\"w1\",state=\"suspect\"} 1",
            "freightdock_node_health{node=\"n1\",name=\"w1\",state=\"healthy\"} 0",
            "freightdock_node_fencing{node=\"n1\",name=\"w1\",state=\"none\"} 1",
            "freightdock_node_cpu_percent{node=\"n1\",name=\"w1\"} 12.3",
            "freightdock_stack_workloads_wanted{stack=\"web\"} 3",
            "freightdock_stack_workloads_running{stack=\"web\"} 2",
            "freightdock_storage_ok{storage=\"nas\",node=\"n1\",name=\"w1\"} 0",
            "freightdock_storage_probe_seconds{storage=\"nas\",node=\"n1\",name=\"w1\"} 10",
            "freightdock_floating_held{address=\"192.0.2.10\"} 1",
            "freightdock_notification_channel_failing{channel=\"ops\",kind=\"ntfy\",enabled=\"true\"} 1",
            "freightdock_recoveries_recorded 4",
        ] {
            assert!(text.lines().any(|l| l == line), "missing {line:?} in\n{text}");
        }
        assert!(!text.contains("freightdock_storage_free_bytes{"), "a failing storage's space is not a number to trust");
    }
}
