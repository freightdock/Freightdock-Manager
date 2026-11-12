//! The overlay (G9), the managers' side: what network each node should have, worked out from the replicated state.
//! A node gets every other node that has a WireGuard key as a peer, with the addresses of the workloads placed there
//! routed to it, and a network for each stack it runs a part of. The worker makes its node so (Freightdock-Network);
//! the whole state goes each time, numbered by the log index it was read at, so a worker can tell an old one from a
//! new one whichever manager sent it.

use freightdock_proto::v1;

use crate::state::{CLUSTER_RANGE, ClusterState, Entry, FenceState, Ms, Workload};

/// The overlay's MTU: 1500 less WireGuard's 80 over IPv6, so nothing a container sends is fragmented.
pub const MTU: u32 = 1420;
/// WireGuard's port on every node.
pub const WG_PORT: u32 = 51820;

/// The network `node` should have. `endpoint` gives another node's WireGuard `host:port`, if the managers know where
/// it is; `down` says of a workload that its node reports it not running (see [`exposures`]).
pub fn state_for(s: &ClusterState, node: &str, endpoint: impl Fn(&str) -> Option<String>, down: impl Fn(&Workload) -> bool) -> v1::NetworkState {
    let peers = s
        .nodes
        .values()
        .filter(|n| n.id != node)
        .filter_map(|n| {
            let key = n.report.as_ref().map(|r| r.wg_public_key.clone()).filter(|k| !k.is_empty())?;
            let addresses = s.node_address(&n.id).into_iter().chain(s.workloads_on(&n.id).filter_map(|w| s.address(&w.id))).map(|a| format!("{a}/32")).collect();
            Some(v1::NetworkPeer { node_id: n.id.clone(), public_key: key, endpoint: endpoint(&n.id).unwrap_or_default(), addresses })
        })
        .collect();
    let mut stacks: Vec<v1::StackNetwork> = vec![];
    for w in s.workloads_on(node) {
        if stacks.iter().any(|st| st.stack == w.stack) {
            continue;
        }
        if let (Some(subnet), Some(n)) = (s.subnet(&w.stack), s.stacks.get(&w.stack).and_then(|st| st.net)) {
            stacks.push(v1::StackNetwork { stack: w.stack.clone(), subnet, gateway: format!("{}.{}.{n}.1", CLUSTER_RANGE[0], CLUSTER_RANGE[1]) });
        }
    }
    v1::NetworkState { version: s.applied, cluster_range: format!("{}.{}.0.0/16", CLUSTER_RANGE[0], CLUSTER_RANGE[1]), mtu: MTU, peers, stacks, node_address: s.node_address(node).unwrap_or_default(), exposures: exposures(s, down), floating: floating_of(s, node) }
}

/// The floating addresses `node` holds and something is exposed on, while it is at rest: a node the cluster starts
/// to fence lets them go at once, before the managers may give them to another. An address of the pool no stack uses
/// is held but not claimed on the LAN, so a large range takes nothing from the LAN until it is used.
fn floating_of(s: &ClusterState, node: &str) -> Vec<String> {
    if s.nodes.get(node).is_none_or(|n| n.fencing.state != FenceState::None) {
        return vec![];
    }
    let used: std::collections::BTreeSet<&str> = s.stacks.values().flat_map(|st| &st.exposures).map(|e| e.address.as_str()).collect();
    s.floating.iter().filter(|(a, f)| f.holder.as_deref() == Some(node) && used.contains(a.as_str())).map(|(a, _)| a.clone()).collect()
}

/// The leader's moves of the floating addresses (G10.3): each with no holder, or whose holder is fenced, to the
/// `healthy` node at rest that holds fewest (ties by id). One whose holder is unreachable but not yet fenced waits:
/// that node may still answer for it.
pub fn floating_moves(s: &ClusterState, healthy: impl Fn(&str) -> bool, at: Ms) -> Vec<Entry> {
    let mut held: std::collections::BTreeMap<&str, usize> = s.nodes.values().filter(|n| n.fencing.state == FenceState::None && healthy(&n.id)).map(|n| (n.id.as_str(), 0)).collect();
    for f in s.floating.values() {
        if let Some(c) = f.holder.as_deref().and_then(|h| held.get_mut(h)) {
            *c += 1;
        }
    }
    let mut out = vec![];
    for (address, f) in &s.floating {
        let free = match f.holder.as_deref().and_then(|h| s.nodes.get(h)) {
            None => true,
            Some(n) => matches!(n.fencing.state, FenceState::Fenced | FenceState::AwaitingAuthorization),
        };
        if !free {
            continue;
        }
        let Some((&to, _)) = held.iter().min_by_key(|(id, c)| (**c, **id)) else { break };
        *held.get_mut(to).expect("a candidate") += 1;
        out.push(Entry::FloatingHeld { address: address.clone(), node_id: Some(to.to_string()), at });
    }
    out
}

/// The cluster's published ports (G10, the routing mesh), the same for every node: each running stack's ports with
/// a host port, to its placed workloads' addresses at the container's port.
///
/// A workload `down` says is not running takes no new connections: while its container restarts or is made again a
/// share of them would wait on an address with nothing behind it. One not heard of yet still takes its share, so a
/// manager that has not heard a node's report yet does not send all the traffic elsewhere.
pub fn exposures(s: &ClusterState, down: impl Fn(&Workload) -> bool) -> Vec<v1::Exposure> {
    let mut out = vec![];
    for st in s.stacks.values().filter(|st| !st.stopped) {
        // A Compose port opens nothing on the nodes' own addresses but for the managers' own stack, whose port
        // they forward the UI to: a user's stack leaves the cluster only on the pool.
        for (svc, spec) in st.services.iter().filter(|_| st.name == crate::ui::STACK) {
            for p in spec.ports.iter().filter(|p| p.host_port != 0) {
                let mut targets: Vec<String> = s.workloads.values().filter(|w| w.stack == st.name && &w.service == svc && w.node_id.is_some() && !down(w)).filter_map(|w| s.address(&w.id)).map(|a| format!("{a}:{}", p.container_port)).collect();
                targets.sort();
                out.push(v1::Exposure { stack: st.name.clone(), protocol: p.protocol.clone(), port: p.host_port, targets, address: String::new() });
            }
        }
        // On the pool (G10.5): each on its own address.
        for e in &st.exposures {
            let mut targets: Vec<String> = s.workloads.values().filter(|w| w.stack == st.name && w.service == e.service && w.node_id.is_some() && !down(w)).filter_map(|w| s.address(&w.id)).map(|a| format!("{a}:{}", e.container_port)).collect();
            targets.sort();
            out.push(v1::Exposure { stack: st.name.clone(), protocol: e.protocol.clone(), port: e.port.into(), targets, address: e.address.clone() });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::tests::{deploy, next, spec, with_nodes};
    use crate::state::{Entry, NodeReport};

    #[test]
    fn a_node_s_network_is_its_peers_with_their_workloads_and_its_own_stacks() {
        let mut s = with_nodes(&["n1", "n2", "n3"]);
        for (n, key) in [("n1", "K1"), ("n2", "K2")] {
            next(&mut s, &Entry::NodeReported { node_id: n.into(), report: NodeReport { wg_public_key: key.into(), ..Default::default() }, at: 1 }).unwrap();
        }
        next(&mut s, &deploy("a", &[("web", spec("nginx", 2))], &[("a.web.1", "n1"), ("a.web.2", "n2")])).unwrap();
        next(&mut s, &deploy("b", &[("db", spec("pg", 1))], &[("b.db.1", "n2")])).unwrap();

        let n1 = state_for(&s, "n1", |id| Some(format!("{id}:51820")), |_| false);
        assert_eq!(n1.peers.len(), 1, "n3 has no key yet: no peer");
        let p = &n1.peers[0];
        assert_eq!((p.node_id.as_str(), p.public_key.as_str(), p.endpoint.as_str()), ("n2", "K2", "n2:51820"));
        let mut want = vec![format!("{}/32", s.node_address("n2").unwrap()), format!("{}/32", s.address("a.web.2").unwrap()), format!("{}/32", s.address("b.db.1").unwrap())];
        want.sort();
        let mut got = p.addresses.clone();
        got.sort();
        assert_eq!(got, want);
        assert_eq!(n1.stacks.len(), 1, "only the stacks it runs a part of");
        assert_eq!((n1.stacks[0].stack.as_str(), n1.stacks[0].subnet.as_str(), n1.stacks[0].gateway.as_str()), ("a", "10.240.1.0/24", "10.240.1.1"));
        assert_eq!((n1.cluster_range.as_str(), n1.mtu, n1.version), ("10.240.0.0/16", MTU, s.applied));

        assert_eq!(n1.node_address, s.node_address("n1").unwrap(), "its own overlay address");
        assert!(state_for(&s, "n3", |_| None, |_| false).peers.iter().any(|p| p.node_id == "n1" && p.addresses.contains(&format!("{}/32", s.node_address("n1").unwrap()))), "and the others route to it");

        let n3 = state_for(&s, "n3", |_| None, |_| false);
        assert_eq!(n3.peers.len(), 2, "n3 still reaches the others");
        assert!(n3.stacks.is_empty());
    }

    #[test]
    fn a_compose_port_is_answered_on_the_nodes_only_for_the_managers_own_stack() {
        let mut s = with_nodes(&["n1", "n2"]);
        let mut web = spec("nginx", 2);
        web.ports = vec![crate::state::Port { host_port: 8088, container_port: 80, protocol: "tcp".into(), host_ip: String::new() }];
        next(&mut s, &deploy("a", &[("web", web)], &[("a.web.1", "n1"), ("a.web.2", "n2")])).unwrap();
        assert!(state_for(&s, "n1", |_| None, |_| false).exposures.is_empty(), "a user's Compose port opens nothing on a node's own address");
        next(&mut s, &deploy(crate::ui::STACK, &[(crate::ui::SERVICE, crate::ui::spec("freightdock:x"))], &[("freightdock.ui.1", "n1")])).unwrap();
        for n in ["n1", "n2"] {
            let e = state_for(&s, n, |_| None, |_| false).exposures;
            assert_eq!(e.len(), 1, "the UI's port, on every node, for the managers to forward to: {e:?}");
            assert_eq!((e[0].stack.as_str(), e[0].port, e[0].address.as_str()), (crate::ui::STACK, crate::ui::HOST_PORT, ""));
            assert_eq!(e[0].targets, vec![format!("{}:7443", s.address("freightdock.ui.1").unwrap())]);
        }
        next(&mut s, &Entry::StackStopped { name: crate::ui::STACK.into(), stopped: true, at: 9 }).unwrap();
        assert!(state_for(&s, "n1", |_| None, |_| false).exposures.is_empty(), "a stopped stack answers on nothing");
    }

    #[test]
    fn a_workload_known_to_be_down_takes_no_connections() {
        let mut s = with_nodes(&["n1", "n2"]);
        next(&mut s, &Entry::FloatingAdded { address: "192.0.2.10".into(), at: 1 }).unwrap();
        next(&mut s, &deploy("a", &[("web", spec("nginx", 2))], &[("a.web.1", "n1"), ("a.web.2", "n2")])).unwrap();
        next(&mut s, &Entry::StackExposed { name: "a".into(), exposures: vec![crate::state::Exposure { service: "web".into(), container_port: 80, protocol: "tcp".into(), address: "192.0.2.10".into(), port: 80 }], at: 2 }).unwrap();
        let e = exposures(&s, |w| w.id == "a.web.1");
        assert_eq!(e[0].targets, vec![format!("{}:80", s.address("a.web.2").unwrap())], "only the one running");
        assert!(exposures(&s, |_| true)[0].targets.is_empty(), "none up: the port answers nothing");
        assert_eq!(exposures(&s, |_| false)[0].targets.len(), 2, "not known to be down: it takes its share");
    }

    #[test]
    fn a_floating_address_goes_to_its_holder_only_while_it_is_at_rest() {
        let mut s = with_nodes(&["n1", "n2"]);
        next(&mut s, &Entry::FloatingAdded { address: "192.0.2.10".into(), at: 1 }).unwrap();
        assert!(state_for(&s, "n1", |_| None, |_| false).floating.is_empty(), "nobody holds it yet");
        next(&mut s, &Entry::FloatingHeld { address: "192.0.2.10".into(), node_id: Some("n1".into()), at: 2 }).unwrap();
        assert!(state_for(&s, "n1", |_| None, |_| false).floating.is_empty(), "nothing exposed on it: not claimed on the LAN");
        next(&mut s, &deploy("a", &[("web", spec("nginx", 1))], &[("a.web.1", "n2")])).unwrap();
        next(&mut s, &Entry::StackExposed { name: "a".into(), exposures: vec![crate::state::Exposure { service: "web".into(), container_port: 80, protocol: "tcp".into(), address: "192.0.2.10".into(), port: 80 }], at: 2 }).unwrap();
        assert_eq!(state_for(&s, "n1", |_| None, |_| false).floating, vec!["192.0.2.10".to_string()], "in use: its holder claims it");
        assert!(state_for(&s, "n2", |_| None, |_| false).floating.is_empty());
        next(&mut s, &Entry::NodeFencing { node_id: "n1".into(), state: crate::state::FenceState::Requested, provider: "self".into(), message: String::new(), at: 3 }).unwrap();
        assert!(state_for(&s, "n1", |_| None, |_| false).floating.is_empty(), "a node being fenced lets it go at once");
    }

    #[test]
    fn a_floating_address_without_a_holder_at_rest_goes_to_the_healthy_node_holding_fewest() {
        use crate::state::FenceState::*;
        let mut s = with_nodes(&["n1", "n2", "n3"]);
        for a in ["192.0.2.10", "192.0.2.11", "192.0.2.12"] {
            next(&mut s, &Entry::FloatingAdded { address: a.into(), at: 1 }).unwrap();
        }
        next(&mut s, &Entry::FloatingHeld { address: "192.0.2.10".into(), node_id: Some("n1".into()), at: 2 }).unwrap();
        let healthy = |n: &str| n != "n3";
        let moves = floating_moves(&s, healthy, 5);
        assert_eq!(moves, vec![
            Entry::FloatingHeld { address: "192.0.2.11".into(), node_id: Some("n2".into()), at: 5 },
            Entry::FloatingHeld { address: "192.0.2.12".into(), node_id: Some("n1".into()), at: 5 },
        ], "fewest held first, ties by id; n3 is not healthy");
        for m in &moves {
            next(&mut s, m).unwrap();
        }
        assert!(floating_moves(&s, healthy, 6).is_empty(), "all held: nothing moves");
        for st in [Requested, InProgress] {
            next(&mut s, &Entry::NodeFencing { node_id: "n1".into(), state: st, provider: "self".into(), message: String::new(), at: 7 }).unwrap();
            assert!(floating_moves(&s, healthy, 7).is_empty(), "{st:?}: n1 may still answer, its addresses wait");
        }
        next(&mut s, &Entry::NodeFencing { node_id: "n1".into(), state: Fenced, provider: "self".into(), message: String::new(), at: 8 }).unwrap();
        let moves = floating_moves(&s, healthy, 8);
        assert_eq!(moves.len(), 2, "both of n1's: {moves:?}");
        assert!(moves.iter().all(|m| matches!(m, Entry::FloatingHeld { node_id: Some(n), .. } if n == "n2")));
        assert!(floating_moves(&s, |_| false, 8).is_empty(), "no healthy node: they wait");
    }

    #[test]
    fn an_exposure_goes_to_every_node_with_its_address_and_the_compose_port_nowhere() {
        let mut s = with_nodes(&["n1", "n2"]);
        next(&mut s, &Entry::FloatingAdded { address: "192.0.2.10".into(), at: 1 }).unwrap();
        let mut web = spec("nginx", 1);
        web.ports = vec![crate::state::Port { host_port: 8088, container_port: 80, protocol: "tcp".into(), host_ip: String::new() }];
        next(&mut s, &deploy("a", &[("web", web)], &[("a.web.1", "n1")])).unwrap();
        next(&mut s, &Entry::StackExposed { name: "a".into(), exposures: vec![crate::state::Exposure { service: "web".into(), container_port: 8443, protocol: "tcp".into(), address: "192.0.2.10".into(), port: 443 }], at: 2 }).unwrap();
        let e = state_for(&s, "n2", |_| None, |_| false).exposures;
        let web1 = s.address("a.web.1").unwrap();
        let got: Vec<(&str, u32, Vec<String>)> = e.iter().map(|e| (e.address.as_str(), e.port, e.targets.clone())).collect();
        assert_eq!(got, vec![("192.0.2.10", 443, vec![format!("{web1}:8443")])], "8088 is only what the Compose file asked for");
    }
}

