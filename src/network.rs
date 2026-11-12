//! The overlay (G9), the managers' side: what network each node should have, worked out from the replicated state.
//! A node gets every other node that has a WireGuard key as a peer, with the addresses of the workloads placed there
//! routed to it, and a network for each stack it runs a part of. The worker makes its node so (Freightdock-Network);
//! the whole state goes each time, numbered by the log index it was read at, so a worker can tell an old one from a
//! new one whichever manager sent it.

use freightdock_proto::v1;

use crate::state::{CLUSTER_RANGE, ClusterState};

/// The overlay's MTU: 1500 less WireGuard's 80 over IPv6, so nothing a container sends is fragmented.
pub const MTU: u32 = 1420;
/// WireGuard's port on every node.
pub const WG_PORT: u32 = 51820;

/// The network `node` should have. `endpoint` gives another node's WireGuard `host:port`, if the managers know where
/// it is.
pub fn state_for(s: &ClusterState, node: &str, endpoint: impl Fn(&str) -> Option<String>) -> v1::NetworkState {
    let peers = s
        .nodes
        .values()
        .filter(|n| n.id != node)
        .filter_map(|n| {
            let key = n.report.as_ref().map(|r| r.wg_public_key.clone()).filter(|k| !k.is_empty())?;
            let addresses = s.workloads_on(&n.id).filter_map(|w| s.address(&w.id)).map(|a| format!("{a}/32")).collect();
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
    v1::NetworkState { version: s.applied, cluster_range: format!("{}.{}.0.0/16", CLUSTER_RANGE[0], CLUSTER_RANGE[1]), mtu: MTU, peers, stacks }
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

        let n1 = state_for(&s, "n1", |id| Some(format!("{id}:51820")));
        assert_eq!(n1.peers.len(), 1, "n3 has no key yet: no peer");
        let p = &n1.peers[0];
        assert_eq!((p.node_id.as_str(), p.public_key.as_str(), p.endpoint.as_str()), ("n2", "K2", "n2:51820"));
        let mut want = vec![format!("{}/32", s.address("a.web.2").unwrap()), format!("{}/32", s.address("b.db.1").unwrap())];
        want.sort();
        let mut got = p.addresses.clone();
        got.sort();
        assert_eq!(got, want);
        assert_eq!(n1.stacks.len(), 1, "only the stacks it runs a part of");
        assert_eq!((n1.stacks[0].stack.as_str(), n1.stacks[0].subnet.as_str(), n1.stacks[0].gateway.as_str()), ("a", "10.240.1.0/24", "10.240.1.1"));
        assert_eq!((n1.cluster_range.as_str(), n1.mtu, n1.version), ("10.240.0.0/16", MTU, s.applied));

        let n3 = state_for(&s, "n3", |_| None);
        assert_eq!(n3.peers.len(), 2, "n3 still reaches the others");
        assert!(n3.stacks.is_empty());
    }
}
