//! Stacks (G4): where a stack's new workloads go, and the reconcile that makes each worker run what the state says.
//!
//! Placement is decided by the manager that takes the deploy, from its copy of the state, and written into the entry
//! (`Entry::StackDeployed::placements`), so applying it stays deterministic. `place` (G5) weighs constraints,
//! preferences, spread, host ports and reservations.
//!
//! The reconcile runs on the manager each worker is connected to, against the containers that worker reported in its
//! latest heartbeat (`Sessions::fresh_reports`). Whatever differs becomes a command, one per workload at a time (the
//! state refuses a second while one is open, which is also what keeps two managers from racing on a workload). Every
//! op is idempotent on the worker, so a command made from a report a moment old does no harm.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use freightdock_proto::v1::WorkloadState;

use crate::state::{ClusterState, CommandRecord, CommandStatus, Entry, Node, Op, Resources, ServiceSpec, Workload};
use crate::{Manager, now_ms};

/// What a node holds already, as placement counts it.
#[derive(Default)]
struct Held {
    load: usize,
    ports: BTreeSet<(u32, String)>,
    reserved: Resources,
}

fn host_ports(spec: &ServiceSpec) -> BTreeSet<(u32, String)> {
    spec.ports.iter().filter(|p| p.host_port != 0).map(|p| (p.host_port, p.protocol.clone())).collect()
}

/// Why `services` cannot be deployed as stack `name` for their published ports, if they cannot (G10): every node
/// answers a published port, so it is one stack's in the whole cluster, and never one the host keeps for itself.
pub fn port_conflict(s: &ClusterState, name: &str, services: &BTreeMap<String, ServiceSpec>) -> Option<String> {
    for (svc, spec) in services {
        for (port, proto) in host_ports(spec) {
            if freightdock_proto::RESERVED_PORTS.contains(&port) {
                return Some(format!("service {svc}: port {port} is the cluster's own (ssh, the managers, WireGuard)"));
            }
            if let Some(other) = s.stacks.values().find(|o| o.name != name && o.services.values().any(|os| host_ports(os).contains(&(port, proto.clone())))) {
                return Some(format!("service {svc}: port {port}/{proto} is published by stack {} already, and every node answers it", other.name));
            }
        }
    }
    None
}

/// Whether `need` fits beside `held` on `n`. A node that did not report its size takes anything.
fn fits(n: &Node, held: &Resources, need: &Resources) -> bool {
    let (cpus, mem) = n.report.as_ref().map_or((0, 0), |r| (r.cpu_count as u64 * 1000, r.memory_total_bytes));
    (cpus == 0 || need.cpu_millis == 0 || held.cpu_millis + need.cpu_millis <= cpus) && (mem == 0 || need.memory_bytes == 0 || held.memory_bytes + need.memory_bytes <= mem)
}

/// Places the workloads of `stack` that need a node: new ones, ones whose node is gone, and ones whose node no longer
/// meets their service's constraints (moved). Candidates are the nodes in `eligible` that meet every constraint, have
/// none of the host ports taken (only off the overlay: on it the mesh publishes them, G10.2) and room for the reservation; of those, the one that meets the most preferences, then
/// holds the fewest of the service's replicas in its spread groups and on itself, then the fewest workloads, then the
/// lowest id. An error names the first workload with nowhere to go, and why.
pub fn place(s: &ClusterState, stack: &str, services: &BTreeMap<String, ServiceSpec>, eligible: &[String]) -> Result<BTreeMap<String, String>, String> {
    let wanted = ClusterState::wanted(stack, services);
    let keep: BTreeSet<&String> = wanted.iter().map(|w| &w.0).collect();
    // A placed workload of this stack stays where it is while its node exists and meets its constraints.
    let stays = |w: &Workload| -> bool {
        let Some(n) = w.node_id.as_ref().and_then(|id| s.nodes.get(id)) else { return false };
        w.stack != stack || services[&w.service].placement.constraints.iter().all(|c| c.holds(n))
    };
    let spec_of = |w: &Workload| -> Option<&ServiceSpec> { if w.stack == stack { services.get(&w.service) } else { s.stacks.get(&w.stack)?.services.get(&w.service) } };
    let mut held: HashMap<&str, Held> = HashMap::new();
    // Where the replicas of each of this stack's services are: (service, node).
    let mut replicas: Vec<(&str, &str)> = vec![];
    for w in s.workloads.values().filter(|w| (w.stack != stack || keep.contains(&w.id)) && stays(w)) {
        let node = w.node_id.as_deref().expect("stays");
        let h = held.entry(node).or_default();
        h.load += 1;
        // Only a workload off the overlay has Docker bind its ports on the node (G10.2).
        if s.address(&w.id).is_none() {
            h.ports.extend(host_ports(&w.spec));
        }
        if let Some(sp) = spec_of(w) {
            h.reserved.cpu_millis += sp.placement.reserve.cpu_millis;
            h.reserved.memory_bytes += sp.placement.reserve.memory_bytes;
        }
        if w.stack == stack {
            replicas.push((w.service.as_str(), node));
        }
    }
    let nodes: Vec<&Node> = eligible.iter().filter_map(|id| s.nodes.get(id)).collect();
    // On the overlay (it has, or will get, a /24) the mesh answers its ports and Docker binds none on the node.
    let on_overlay = s.stacks.get(stack).and_then(|st| st.net).is_some() || s.stacks.values().filter(|st| st.net.is_some()).count() < 254;
    let mut out = BTreeMap::new();
    for (id, svc, _) in &wanted {
        if s.workloads.get(id).is_some_and(stays) {
            continue;
        }
        let spec = &services[svc];
        let p = &spec.placement;
        let needs = if on_overlay { BTreeSet::new() } else { host_ports(spec) };
        let meets: Vec<&Node> = nodes.iter().copied().filter(|n| p.constraints.iter().all(|c| c.holds(n))).collect();
        let free: Vec<&Node> = meets.iter().copied().filter(|n| held.get(n.id.as_str()).is_none_or(|h| h.ports.is_disjoint(&needs))).collect();
        let room: Vec<&Node> = free.iter().copied().filter(|n| fits(n, &held.get(n.id.as_str()).map(|h| h.reserved).unwrap_or_default(), &p.reserve)).collect();
        let why = if nodes.is_empty() {
            Some("none is connected with Docker".to_string())
        } else if meets.is_empty() {
            Some(format!("none meets {}", p.constraints.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")))
        } else if free.is_empty() {
            Some("every node that could already publishes one of its host ports".to_string())
        } else if room.is_empty() {
            Some("none has room for its reservation".to_string())
        } else {
            None
        };
        if let Some(why) = why {
            return Err(format!("no node can take {id}: {why}"));
        }
        let score = |n: &Node| {
            let labels = n.all_labels();
            let misses = p.prefer.iter().filter(|c| !c.holds(n)).count();
            let spread: Vec<usize> = p
                .spread
                .iter()
                .map(|k| {
                    let mine = labels.get(k);
                    replicas.iter().filter(|(sv, on)| sv == svc && s.nodes.get(*on).map(|o| o.all_labels().get(k).cloned()) == Some(mine.cloned())).count()
                })
                .collect();
            let here = replicas.iter().filter(|(sv, on)| sv == svc && *on == n.id).count();
            let load = held.get(n.id.as_str()).map_or(0, |h| h.load);
            (misses, spread, here, load, n.id.clone())
        };
        let node = room.into_iter().min_by_key(|n| score(n)).expect("room is not empty");
        let h = held.entry(node.id.as_str()).or_default();
        h.load += 1;
        h.ports.extend(needs);
        h.reserved.cpu_millis += p.reserve.cpu_millis;
        h.reserved.memory_bytes += p.reserve.memory_bytes;
        replicas.push((svc.as_str(), node.id.as_str()));
        out.insert(id.clone(), node.id.clone());
    }
    Ok(out)
}

/// The op that brings one workload from what its node reported to what the state wants, if one is needed.
fn op_for(w: &Workload, net: Option<crate::state::Attach>, run: bool, actual: Option<&WorkloadState>) -> Option<Op> {
    let up = actual.is_some_and(|a| matches!(a.state.as_str(), "running" | "restarting" | "paused" | "removing"));
    let current = actual.is_some_and(|a| a.generation == w.generation);
    if run && !(up && current) {
        return Some(Op::RunContainer {
            workload_id: w.id.clone(),
            image: w.spec.image.clone(),
            name: format!("{}-{}-{}", w.stack, w.service, w.slot),
            command: w.spec.command.clone(),
            env: w.spec.env.clone(),
            labels: w.spec.labels.iter().map(|(k, v)| (k.clone(), v.clone())).chain([("io.freightdock.stack".to_string(), w.stack.clone()), ("io.freightdock.service".to_string(), w.service.clone())]).collect(),
            generation: w.generation,
            ports: w.spec.ports.clone(),
            volumes: w.spec.volumes.clone(),
            limits: w.spec.limits,
            net,
        });
    }
    if !run && actual.is_some_and(|a| a.state == "running") {
        return Some(Op::StopContainer { workload_id: w.id.clone(), timeout_s: 0 });
    }
    None
}

/// Whether a reported container is a stack's (its workload id is `<stack>.<service>.<slot>`). Only those are removed
/// when the state does not hold them; one run by hand under a plain id is left alone.
pub fn is_stack_workload(id: &str) -> bool {
    let p: Vec<&str> = id.split('.').collect();
    p.len() == 3 && !p[0].is_empty() && !p[1].is_empty() && p[2].parse::<u32>().is_ok()
}

/// What `node` needs done: (workload id, op), at most one per workload, none for a busy one.
pub fn reconcile(s: &ClusterState, node: &str, actual: &[WorkloadState], busy: &BTreeSet<String>) -> Vec<(String, Op)> {
    let mut out = vec![];
    let mut wanted = BTreeSet::new();
    for w in s.workloads_on(node) {
        wanted.insert(w.id.as_str());
        if busy.contains(&w.id) {
            continue;
        }
        if let Some(op) = op_for(w, s.attach(w), s.should_run(w), actual.iter().find(|a| a.workload_id == w.id)) {
            out.push((w.id.clone(), op));
        }
    }
    for a in actual {
        if !wanted.contains(a.workload_id.as_str()) && is_stack_workload(&a.workload_id) && !busy.contains(&a.workload_id) {
            out.push((a.workload_id.clone(), Op::RemoveContainer { workload_id: a.workload_id.clone() }));
        }
    }
    out
}

/// How soon a workload is acted on again: 5s after one action, doubling for each one in a row, to 5 minutes, so a
/// container that dies at once is not started again in a tight loop.
pub fn backoff(in_a_row: u32) -> Duration {
    Duration::from_secs(5).saturating_mul(1 << in_a_row.min(6)).min(Duration::from_secs(300))
}

/// What this manager did lately per workload, in memory only.
#[derive(Default)]
pub struct Paced(Mutex<HashMap<String, (Instant, u32)>>);

impl Paced {
    /// Whether the workload may be acted on now.
    fn ready(&self, id: &str) -> bool {
        self.0.lock().expect("paced lock").get(id).is_none_or(|(at, n)| at.elapsed() >= backoff(n.saturating_sub(1)))
    }

    fn acted(&self, id: &str) {
        let mut g = self.0.lock().expect("paced lock");
        let e = g.entry(id.to_string()).or_insert((Instant::now(), 0));
        *e = (Instant::now(), e.1.saturating_add(1));
    }

    /// The workload is as wanted: its next action, whenever, starts the count again.
    fn settled(&self, id: &str) {
        self.0.lock().expect("paced lock").remove(id);
    }
}

impl Manager {
    /// One round of the reconcile, for the nodes connected here. Run every second by `housekeeping`.
    pub async fn reconcile(&self) {
        let reports = self.sessions.fresh_reports();
        if reports.is_empty() {
            return;
        }
        let mut todo = vec![];
        self.store.read(|s| {
            let busy = s.busy_workloads();
            for (node, actual) in &reports {
                // Nothing runs on a node anywhere in fencing (G6); its worker refuses runs too.
                if s.nodes.get(node).is_none_or(|n| n.fencing.state != crate::state::FenceState::None) {
                    continue;
                }
                let ops = reconcile(s, node, actual, &busy);
                for w in s.workloads_on(node) {
                    if !busy.contains(&w.id) && !ops.iter().any(|(id, _)| *id == w.id) {
                        self.paced.settled(&w.id);
                    }
                }
                todo.extend(ops.into_iter().map(|(id, op)| (node.clone(), id, op)));
            }
        });
        for (node, id, op) in todo {
            if !self.paced.ready(&id) {
                continue;
            }
            self.paced.acted(&id);
            let now = now_ms();
            let what = match &op {
                Op::RunContainer { .. } => "run",
                Op::StopContainer { .. } => "stop",
                _ => "remove",
            };
            let c = CommandRecord { id: uuid::Uuid::new_v4().to_string(), node_id: node.clone(), op, config_version: self.store.read(|s| s.applied), created_at: now, deadline: now + 120_000, status: CommandStatus::Pending, message: String::new(), result_json: String::new(), updated_at: now };
            match self.store.propose(Entry::CommandCreated { command: c }).await {
                Ok(_) => tracing::info!(node = %node, workload = %id, "reconcile: {what}"),
                Err(e) => tracing::debug!(node = %node, workload = %id, "reconcile: {what} not sent: {e}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Port;
    use crate::state::tests::{cluster, spec};

    fn state_with(nodes: &[&str]) -> ClusterState {
        let mut s = cluster();
        for n in nodes {
            s.nodes.insert(n.to_string(), crate::state::Node { id: n.to_string(), hostname: n.to_string(), cert_sha256: String::new(), prev_cert_sha256: None, enrolled_at: 0, report: None, reported_at: None, labels: Default::default(), fencing: Default::default(), recovered: false, overlay: None });
        }
        s
    }

    #[test]
    fn a_published_port_is_the_cluster_s_once_and_never_the_host_s_own() {
        let mut s = state_with(&["n1"]);
        let with = |port: u32, proto: &str| {
            let mut w = spec("nginx", 1);
            w.ports = vec![Port { host_port: port, container_port: 80, protocol: proto.into(), host_ip: String::new() }];
            BTreeMap::from([("web".to_string(), w)])
        };
        assert_eq!(port_conflict(&s, "a", &with(8088, "tcp")), None);
        deploy(&mut s, "a", &with(8088, "tcp"), BTreeMap::from([("a.web.1".to_string(), "n1".to_string())]));
        assert!(port_conflict(&s, "b", &with(8088, "tcp")).unwrap().contains("stack a"), "every node answers it: one stack's");
        assert_eq!(port_conflict(&s, "a", &with(8088, "tcp")), None, "its own again, redeployed");
        assert_eq!(port_conflict(&s, "b", &with(8088, "udp")), None, "another protocol");
        for p in [22, 7450, 7451, 7452, 51820] {
            assert!(port_conflict(&s, "b", &with(p, "tcp")).is_some(), "{p}");
        }
        assert_eq!(port_conflict(&s, "b", &with(0, "tcp")), None, "not published");
    }

    fn deploy(s: &mut ClusterState, stack: &str, services: &BTreeMap<String, ServiceSpec>, placements: BTreeMap<String, String>) {
        let i = s.applied + 1;
        s.apply_at(i, &Entry::StackDeployed { name: stack.into(), compose: String::new(), services: services.clone(), placements, at: 1 }).unwrap();
    }

    fn svc(list: &[(&str, ServiceSpec)]) -> BTreeMap<String, ServiceSpec> {
        list.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    fn published(image: &str, replicas: u32, host_port: u32) -> ServiceSpec {
        ServiceSpec { ports: vec![Port { host_port, container_port: 80, protocol: "tcp".into(), host_ip: String::new() }], ..spec(image, replicas) }
    }

    fn eligible(n: &[&str]) -> Vec<String> {
        n.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn workloads_spread_over_the_least_loaded_nodes() {
        let s = state_with(&["a", "b", "c"]);
        let p = place(&s, "web", &svc(&[("x", spec("nginx", 4))]), &eligible(&["a", "b", "c"])).unwrap();
        let on: Vec<&str> = p.values().map(String::as_str).collect();
        assert_eq!(on, vec!["a", "b", "c", "a"]);
    }

    #[test]
    fn a_workload_already_placed_stays_and_counts() {
        let mut s = state_with(&["a", "b"]);
        let one = svc(&[("x", spec("nginx", 1))]);
        let p = place(&s, "web", &one, &eligible(&["a", "b"])).unwrap();
        deploy(&mut s, "web", &one, p);
        let two = svc(&[("x", spec("nginx", 2))]);
        let p = place(&s, "web", &two, &eligible(&["a", "b"])).unwrap();
        assert_eq!(p.len(), 1, "only the new one: {p:?}");
        assert_eq!(p["web.x.2"], "b");
    }

    #[test]
    fn a_published_port_holds_a_node_only_off_the_overlay() {
        let mut s = state_with(&["a", "b"]);
        // On the overlay the mesh answers the port and Docker binds nothing (G10.2): replicas may share a node.
        let p = place(&s, "web", &svc(&[("x", published("nginx", 3, 8080))]), &eligible(&["a"])).unwrap();
        assert_eq!(p.values().filter(|n| *n == "a").count(), 3, "{p:?}");
        // Off it (the range full) Docker publishes the port on the node, once.
        for n in 1..=254u8 {
            let name = format!("s{n}");
            s.stacks.insert(name.clone(), crate::state::Stack { name, compose: String::new(), services: BTreeMap::new(), stopped: false, created_at: 0, updated_at: 0, net: Some(n) });
        }
        let p = place(&s, "web", &svc(&[("x", published("nginx", 2, 8080))]), &eligible(&["a", "b"])).unwrap();
        assert_ne!(p["web.x.1"], p["web.x.2"]);
        let e = place(&s, "web", &svc(&[("x", published("nginx", 3, 8080))]), &eligible(&["a", "b"])).unwrap_err();
        assert!(e.contains("web.x.3"), "{e}");
        assert!(place(&s, "web", &svc(&[("x", spec("nginx", 1))]), &[]).unwrap_err().contains("none is connected"));
    }

    /// Nodes with labels and a size: (id, labels as k=v, cpus, memory in MiB).
    fn sized(nodes: &[(&str, &[&str], u32, u64)]) -> ClusterState {
        let mut s = state_with(&nodes.iter().map(|n| n.0).collect::<Vec<_>>());
        for (id, labels, cpus, mib) in nodes {
            let n = s.nodes.get_mut(*id).unwrap();
            n.labels = labels.iter().map(|l| l.split_once('=').map(|(k, v)| (k.to_string(), v.to_string())).unwrap()).collect();
            n.report = Some(crate::state::NodeReport { cpu_count: *cpus, memory_total_bytes: mib << 20, docker: true, ..Default::default() });
        }
        s
    }

    fn with(mut sp: ServiceSpec, f: impl FnOnce(&mut ServiceSpec)) -> ServiceSpec {
        f(&mut sp);
        sp
    }

    fn c(text: &str) -> crate::state::Constraint {
        crate::compose::constraint(text).unwrap()
    }

    #[test]
    fn constraints_require_and_exclude() {
        let s = sized(&[("a", &["ssd=true"], 4, 1024), ("b", &[], 4, 1024), ("c", &["ssd=true", "old=yes"], 4, 1024)]);
        let all = eligible(&["a", "b", "c"]);
        let sp = with(spec("db", 2), |x| x.placement.constraints = vec![c("node.labels.ssd==true"), c("node.labels.old!=yes")]);
        let p = place(&s, "st", &svc(&[("db", sp)]), &all).unwrap();
        assert!(p.values().all(|n| n == "a"), "only a has ssd and no old: {p:?}");
        let e = place(&s, "st", &svc(&[("x", with(spec("x", 1), |x| x.placement.constraints = vec![c("node.labels.gpu==yes")]))]), &all).unwrap_err();
        assert!(e.contains("none meets node.labels.gpu==yes"), "{e}");
    }

    #[test]
    fn two_replicas_land_on_one_node_only_when_nothing_else_will_do() {
        let s = sized(&[("a", &["ssd=true"], 4, 1024), ("b", &[], 4, 1024)]);
        let sp = with(spec("db", 2), |x| x.placement.constraints = vec![c("node.labels.ssd==true")]);
        let p = place(&s, "st", &svc(&[("db", sp)]), &eligible(&["a", "b"])).unwrap();
        assert_eq!((p["st.db.1"].as_str(), p["st.db.2"].as_str()), ("a", "a"));
    }

    #[test]
    fn preferences_come_first_and_replicas_spread_over_a_label() {
        let s = sized(&[("a", &["zone=1"], 4, 1024), ("b", &["zone=1", "fast=yes"], 4, 1024), ("c", &["zone=2"], 4, 1024)]);
        let all = eligible(&["a", "b", "c"]);
        let p = place(&s, "st", &svc(&[("w", with(spec("w", 1), |x| x.placement.prefer = vec![c("node.labels.fast==yes")]))]), &all).unwrap();
        assert_eq!(p["st.w.1"], "b");
        let p = place(&s, "st", &svc(&[("w", with(spec("w", 2), |x| x.placement.spread = vec!["zone".into()]))]), &all).unwrap();
        let zones: BTreeSet<&str> = p.values().map(|n| if n == "c" { "2" } else { "1" }).collect();
        assert_eq!(zones.len(), 2, "one per zone: {p:?}");
    }

    #[test]
    fn reservations_count_against_the_node_s_size() {
        let s = sized(&[("a", &[], 2, 1024), ("b", &[], 1, 512)]);
        let big = with(spec("x", 1), |x| x.placement.reserve = Resources { cpu_millis: 1500, memory_bytes: 0 });
        assert_eq!(place(&s, "st", &svc(&[("x", big.clone())]), &eligible(&["a", "b"])).unwrap()["st.x.1"], "a");
        let e = place(&s, "st", &svc(&[("x", with(big, |x| x.replicas = 2))]), &eligible(&["a", "b"])).unwrap_err();
        assert!(e.contains("room"), "{e}");
        let mem = with(spec("m", 3), |x| x.placement.reserve = Resources { cpu_millis: 0, memory_bytes: 512 << 20 });
        let p = place(&s, "st", &svc(&[("m", mem)]), &eligible(&["a", "b"])).unwrap();
        assert_eq!(p.values().filter(|n| *n == "a").count(), 2, "1 GiB holds two of 512 MiB: {p:?}");
    }

    #[test]
    fn a_workload_whose_node_no_longer_qualifies_is_moved_and_the_others_stay() {
        let mut s = sized(&[("a", &["ssd=true"], 4, 1024), ("b", &[], 4, 1024)]);
        let two = svc(&[("x", spec("x", 2))]);
        let p = place(&s, "st", &two, &eligible(&["a", "b"])).unwrap();
        deploy(&mut s, "st", &two, p);
        let only_ssd = svc(&[("x", with(spec("x", 2), |x| x.placement.constraints = vec![c("node.labels.ssd==true")]))]);
        let p = place(&s, "st", &only_ssd, &eligible(&["a", "b"])).unwrap();
        let on_b = s.workloads.values().find(|w| w.node_id.as_deref() == Some("b")).unwrap().id.clone();
        assert_eq!(p.len(), 1, "{p:?}");
        assert_eq!(p[&on_b], "a");
    }

    fn running(id: &str, generation: u64) -> WorkloadState {
        WorkloadState { workload_id: id.into(), state: "running".into(), generation, ..Default::default() }
    }

    fn placed(s: &mut ClusterState, services: &[(&str, ServiceSpec)]) {
        let services = svc(services);
        let p = place(s, "web", &services, &eligible(&["a"])).unwrap();
        deploy(s, "web", &services, p);
    }

    #[test]
    fn a_missing_stopped_or_old_container_is_run() {
        let mut s = state_with(&["a"]);
        placed(&mut s, &[("x", spec("nginx", 1))]);
        let g = s.workloads["web.x.1"].generation;
        let ops = |actual: &[WorkloadState]| reconcile(&s, "a", actual, &BTreeSet::new());
        assert!(matches!(ops(&[])[..], [(_, Op::RunContainer { .. })]), "missing");
        assert!(ops(&[running("web.x.1", g)]).is_empty(), "as wanted");
        let exited = WorkloadState { state: "exited".into(), ..running("web.x.1", g) };
        assert!(matches!(ops(&[exited])[..], [(_, Op::RunContainer { .. })]), "died");
        match &ops(&[running("web.x.1", g - 1)])[..] {
            [(_, Op::RunContainer { generation, name, labels, .. })] => {
                assert_eq!(*generation, g, "an old spec's container is made again");
                assert_eq!(name, "web-x-1");
                assert_eq!(labels["io.freightdock.stack"], "web");
            }
            o => panic!("{o:?}"),
        }
        assert!(reconcile(&s, "a", &[], &BTreeSet::from(["web.x.1".to_string()])).is_empty(), "busy");
    }

    #[test]
    fn a_run_puts_the_workload_on_its_stack_s_network_with_its_address_and_the_stack_s_names() {
        let mut s = state_with(&["a"]);
        placed(&mut s, &[("x", spec("nginx", 2)), ("db", spec("pg", 1))]);
        let ops = reconcile(&s, "a", &[], &BTreeSet::new());
        let net = ops.iter().find_map(|(_, o)| match o {
            Op::RunContainer { workload_id, net, .. } if workload_id == "web.x.1" => net.clone(),
            _ => None,
        });
        let net = net.expect("on the overlay");
        assert_eq!((net.stack.as_str(), Some(net.address.clone())), ("web", s.address("web.x.1")));
        let db = s.address("web.db.1").unwrap();
        let x2 = s.address("web.x.2").unwrap();
        assert!(net.hosts.contains(&format!("db:{db}")) && net.hosts.contains(&format!("x:{x2}")), "{:?}", net.hosts);
        assert!(net.hosts.contains(&format!("x-2:{x2}")) && net.hosts.contains(&format!("db-1:{db}")), "and each replica by itself: {:?}", net.hosts);
    }

    #[test]
    fn a_stopped_stack_s_running_container_is_stopped() {
        let mut s = state_with(&["a"]);
        placed(&mut s, &[("x", spec("nginx", 1))]);
        let g = s.workloads["web.x.1"].generation;
        let i = s.applied + 1;
        s.apply_at(i, &Entry::StackStopped { name: "web".into(), stopped: true, at: 2 }).unwrap();
        assert!(matches!(reconcile(&s, "a", &[running("web.x.1", g)], &BTreeSet::new())[..], [(_, Op::StopContainer { .. })]));
        assert!(reconcile(&s, "a", &[], &BTreeSet::new()).is_empty(), "nothing to stop");
    }

    #[test]
    fn a_stack_container_the_state_does_not_hold_is_removed_and_others_are_left() {
        let s = state_with(&["a"]);
        let ops = reconcile(&s, "a", &[running("gone.x.1", 3), running("by-hand", 0)], &BTreeSet::new());
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], (id, Op::RemoveContainer { .. }) if id == "gone.x.1"));
        assert!(is_stack_workload("web.api_1.12"));
        assert!(!is_stack_workload("web.api.x") && !is_stack_workload("a.b") && !is_stack_workload("a.b.c.1"));
    }

    #[test]
    fn the_backoff_doubles_to_five_minutes() {
        assert_eq!(backoff(0), Duration::from_secs(5));
        assert_eq!(backoff(1), Duration::from_secs(10));
        assert_eq!(backoff(30), Duration::from_secs(300));
    }
}
