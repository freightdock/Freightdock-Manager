//! The cluster's traffic as the UI's live view draws it (G9.8): each node's latest report (`TrafficReport`, counted
//! by the node a connection started on or came in on, so once) with the overlay addresses turned into workloads and
//! nodes from the state, and bytes into bytes per second over the report's time.

use freightdock_proto::v1;
use serde_json::{Value, json};

use crate::state::ClusterState;

/// One end of a flow: a workload, a node (its overlay address), outside the cluster, or an overlay address the state
/// does not know (a workload just removed).
fn end(s: &ClusterState, addr: &str) -> Value {
    if addr.is_empty() {
        return json!({ "kind": "outside" });
    }
    if let Some(w) = s.workloads.values().find(|w| s.address(&w.id).as_deref() == Some(addr)) {
        return json!({ "kind": "workload", "id": w.id, "stack": w.stack, "service": w.service, "node_id": w.node_id });
    }
    if let Some(n) = s.nodes.keys().find(|n| s.node_address(n).as_deref() == Some(addr)) {
        return json!({ "kind": "node", "id": n });
    }
    json!({ "kind": "unknown", "address": addr })
}

/// `reports` is each node's latest, by node id.
pub fn view(s: &ClusterState, reports: &[(String, v1::TrafficReport)]) -> Value {
    let mut flows = vec![];
    for (node, r) in reports {
        let rate = |b: u64| b.saturating_mul(1000).checked_div(r.over_ms).unwrap_or(0);
        for f in &r.flows {
            flows.push(json!({
                "node": node,
                "from": end(s, &f.src),
                "to": end(s, &f.dst),
                "port": f.port,
                "published_port": f.published_port,
                "protocol": f.protocol,
                "out_bps": rate(f.bytes_out),
                "in_bps": rate(f.bytes_in),
                "connections": f.connections,
            }));
        }
    }
    let workloads: Vec<Value> = s.workloads.values().filter(|w| w.node_id.is_some()).map(|w| json!({ "id": w.id, "stack": w.stack, "service": w.service, "node_id": w.node_id, "address": s.address(&w.id) })).collect();
    json!({ "reporting": reports.iter().map(|(n, _)| n).collect::<Vec<_>>(), "workloads": workloads, "flows": flows })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::tests::{deploy, next, spec, with_nodes};

    #[test]
    fn a_flow_s_ends_are_named_and_its_bytes_a_rate() {
        let mut s = with_nodes(&["n1", "n2"]);
        next(&mut s, &deploy("a", &[("web", spec("nginx", 1)), ("api", spec("api", 1))], &[("a.web.1", "n1"), ("a.api.1", "n2")])).unwrap();
        let (web, api, n2) = (s.address("a.web.1").unwrap(), s.address("a.api.1").unwrap(), s.node_address("n2").unwrap());
        let flow = |src: &str, dst: &str, out, inb| v1::Flow { src: src.into(), dst: dst.into(), port: 80, published_port: 0, protocol: "tcp".into(), bytes_out: out, bytes_in: inb, connections: 1 };
        let r = v1::TrafficReport { flows: vec![flow(&web, &api, 2000, 4000), flow("", &web, 500, 0), flow(&web, "", 0, 0), flow(&web, &n2, 1, 1), flow(&web, "10.240.9.9", 1, 1)], over_ms: 2000 };
        let v = view(&s, &[("n1".into(), r)]);
        let f = &v["flows"];
        assert_eq!((f[0]["from"]["id"].as_str(), f[0]["to"]["id"].as_str(), f[0]["out_bps"].as_u64(), f[0]["in_bps"].as_u64()), (Some("a.web.1"), Some("a.api.1"), Some(1000), Some(2000)));
        assert_eq!((f[0]["node"].as_str(), f[0]["to"]["node_id"].as_str()), (Some("n1"), Some("n2")));
        assert_eq!((f[1]["from"]["kind"].as_str(), f[2]["to"]["kind"].as_str()), (Some("outside"), Some("outside")));
        assert_eq!((f[3]["to"]["kind"].as_str(), f[3]["to"]["id"].as_str()), (Some("node"), Some("n2")));
        assert_eq!((f[4]["to"]["kind"].as_str(), f[4]["to"]["address"].as_str()), (Some("unknown"), Some("10.240.9.9")));
        assert_eq!(v["workloads"].as_array().map(Vec::len), Some(2));
        let first = view(&s, &[("n1".into(), v1::TrafficReport { flows: vec![flow(&web, &api, 9, 9)], over_ms: 0 })]);
        assert_eq!(first["flows"][0]["out_bps"].as_u64(), Some(0), "a first report covers no time: no rate yet");
    }
}
