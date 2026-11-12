//! The overlay (G9), the managers' side: what network each node should have, worked out from the replicated state.
//! A node gets every other node that has a WireGuard key as a peer, with the addresses of the workloads placed there
//! routed to it, and a network for each stack it runs a part of. The worker makes its node so (Freightdock-Network);
//! the whole state goes each time, numbered by the log index it was read at, so a worker can tell an old one from a
//! new one whichever manager sent it.

use freightdock_proto::v1;

use crate::state::{CLUSTER_RANGE, ClusterState, Workload};

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
    v1::NetworkState { version: s.applied, cluster_range: format!("{}.{}.0.0/16", CLUSTER_RANGE[0], CLUSTER_RANGE[1]), mtu: MTU, peers, stacks, node_address: s.node_address(node).unwrap_or_default(), exposures: exposures(s, down) }
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
        for (svc, spec) in &st.services {
            for p in spec.ports.iter().filter(|p| p.host_port != 0) {
                let mut targets: Vec<String> = s.workloads.values().filter(|w| w.stack == st.name && &w.service == svc && w.node_id.is_some() && !down(w)).filter_map(|w| s.address(&w.id)).map(|a| format!("{a}:{}", p.container_port)).collect();
                targets.sort();
                out.push(v1::Exposure { stack: st.name.clone(), protocol: p.protocol.clone(), port: p.host_port, targets });
            }
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
    fn every_node_answers_every_published_port_for_the_workloads_behind_it() {
        let mut s = with_nodes(&["n1", "n2"]);
        let mut web = spec("nginx", 2);
        web.ports = vec![crate::state::Port { host_port: 8088, container_port: 80, protocol: "tcp".into(), host_ip: String::new() }, crate::state::Port { host_port: 0, container_port: 9000, protocol: "tcp".into(), host_ip: String::new() }];
        next(&mut s, &deploy("a", &[("web", web)], &[("a.web.1", "n1"), ("a.web.2", "n2")])).unwrap();
        for n in ["n1", "n2"] {
            let e = state_for(&s, n, |_| None, |_| false).exposures;
            assert_eq!(e.len(), 1, "a published port only, on every node: {e:?}");
            assert_eq!((e[0].stack.as_str(), e[0].protocol.as_str(), e[0].port), ("a", "tcp", 8088));
            let mut want = vec![format!("{}:80", s.address("a.web.1").unwrap()), format!("{}:80", s.address("a.web.2").unwrap())];
            want.sort();
            assert_eq!(e[0].targets, want);
        }
        next(&mut s, &Entry::StackStopped { name: "a".into(), stopped: true, at: 9 }).unwrap();
        assert!(state_for(&s, "n1", |_| None, |_| false).exposures.is_empty(), "a stopped stack answers on nothing");
    }

    #[test]
    fn a_workload_known_to_be_down_takes_no_connections() {
        let mut s = with_nodes(&["n1", "n2"]);
        let mut web = spec("nginx", 2);
        web.ports = vec![crate::state::Port { host_port: 8088, container_port: 80, protocol: "tcp".into(), host_ip: String::new() }];
        next(&mut s, &deploy("a", &[("web", web)], &[("a.web.1", "n1"), ("a.web.2", "n2")])).unwrap();
        let e = exposures(&s, |w| w.id == "a.web.1");
        assert_eq!(e[0].targets, vec![format!("{}:80", s.address("a.web.2").unwrap())], "only the one running");
        assert!(exposures(&s, |_| true)[0].targets.is_empty(), "none up: the port answers nothing");
        assert_eq!(exposures(&s, |_| false)[0].targets.len(), 2, "not known to be down: it takes its share");
    }
}
