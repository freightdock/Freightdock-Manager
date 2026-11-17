//! The Storage page's numbers (`GET /v1/usage`): each node's disk, what its stacks take of it, and each shared
//! storage's size, free space and what each stack's folders on it take. Made from what the workers last measured
//! (`UsageReport`, every few minutes) and last probed (`StorageReport`); nothing here is replicated, and a number no
//! node has measured is null, never a guess.

use std::collections::BTreeMap;

use freightdock_proto::v1::{FolderSize, StorageHealth, StorageReport, UsageReport};
use serde_json::{Value, json};

use crate::state::ClusterState;

/// What a manager knows of a node now: its disk (the heartbeat's), its health word, and its latest measure and probe.
#[derive(Debug, Default, Clone)]
pub struct Seen {
    pub id: String,
    pub health: String,
    pub disk_total: Option<u64>,
    pub disk_used: Option<u64>,
    pub usage: Option<UsageReport>,
    pub storage: Option<StorageReport>,
}

/// The page's numbers; see the module.
pub fn view(s: &ClusterState, seen: &[Seen]) -> Value {
    let name = |id: &str| s.nodes.get(id).map_or_else(|| id.to_string(), |n| n.display_name());
    let mut nodes: Vec<Value> = seen
        .iter()
        .filter(|n| s.nodes.contains_key(&n.id))
        .map(|n| {
            let u = n.usage.as_ref();
            let mut stacks: Vec<(String, u64, u64)> = u.map(|u| u.stacks.iter().map(|d| (d.stack.clone(), d.containers_bytes, d.volumes_bytes)).collect()).unwrap_or_default();
            stacks.sort_by(|a, b| (b.1 + b.2).cmp(&(a.1 + a.2)).then(a.0.cmp(&b.0)));
            let counted: u64 = stacks.iter().map(|x| x.1 + x.2).sum::<u64>() + u.map_or(0, |u| u.images_bytes + u.loose_volumes_bytes + u.build_cache_bytes);
            json!({
                "id": n.id, "name": name(&n.id), "health": n.health,
                "total_bytes": n.disk_total, "used_bytes": n.disk_used, "free_bytes": n.disk_total.zip(n.disk_used).map(|(t, us)| t.saturating_sub(us)),
                "measured_at": u.map(|u| u.at_ms), "took_ms": u.map(|u| u.took_ms),
                "images_bytes": u.map(|u| u.images_bytes), "loose_volumes_bytes": u.map(|u| u.loose_volumes_bytes), "build_cache_bytes": u.map(|u| u.build_cache_bytes),
                // The rest of what is used: the system, logs, what is not Docker's. Not below nothing: Docker's data
                // may be on another filesystem than the one measured.
                "other_bytes": u.and(n.disk_used).map(|us| us.saturating_sub(counted)),
                "stacks": stacks.iter().map(|(st, c, v)| json!({ "stack": st, "bytes": c + v, "containers_bytes": c, "volumes_bytes": v })).collect::<Vec<_>>(),
            })
        })
        .collect();
    nodes.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));

    let healths: Vec<(&str, &StorageHealth)> = seen.iter().flat_map(|n| n.storage.iter().flat_map(|r| r.storages.iter()).map(move |h| (n.id.as_str(), h))).collect();
    let folders: Vec<(&str, i64, &FolderSize)> = seen.iter().flat_map(|n| n.usage.iter().flat_map(move |u| u.folders.iter().map(move |f| (n.id.as_str(), u.at_ms, f)))).collect();
    let storage: Vec<Value> = s
        .storage
        .values()
        .map(|st| {
            // Its size from the newest probe that answered.
            let probe = healths.iter().filter(|(_, h)| h.name == st.name && h.ok && h.total_bytes > 0).max_by_key(|(_, h)| h.at_ms);
            let (total, free) = (probe.map(|(_, h)| h.total_bytes), probe.map(|(_, h)| h.free_bytes));
            let mut by: BTreeMap<&str, Vec<Value>> = BTreeMap::new();
            for stack in s.stacks.values() {
                for (vol, v) in stack.volumes.iter().filter(|(_, v)| v.storage == st.name) {
                    let dir = v.dir(&stack.name, vol);
                    let m = folders.iter().filter(|(_, _, f)| f.storage == st.name && f.dir == dir && f.error.is_empty()).max_by_key(|(_, at, _)| *at);
                    by.entry(stack.name.as_str()).or_default().push(json!({
                        "volume": vol, "dir": dir, "bytes": m.map(|(_, _, f)| f.bytes), "partial": m.is_some_and(|(_, _, f)| f.partial),
                        "measured_at": m.map(|(_, at, _)| at), "measured_by": m.map(|(id, _, _)| name(id)),
                    }));
                }
            }
            let mut stacks: Vec<Value> = by
                .into_iter()
                .map(|(stack, vols)| {
                    let known: Vec<u64> = vols.iter().filter_map(|v| v["bytes"].as_u64()).collect();
                    json!({ "stack": stack, "bytes": if known.is_empty() { Value::Null } else { json!(known.iter().sum::<u64>()) }, "measured": known.len() == vols.len(), "partial": vols.iter().any(|v| v["partial"] == true), "volumes": vols })
                })
                .collect();
            stacks.sort_by(|a, b| b["bytes"].as_u64().cmp(&a["bytes"].as_u64()).then(a["stack"].as_str().cmp(&b["stack"].as_str())));
            let used = total.zip(free).map(|(t, f)| t.saturating_sub(f));
            let in_stacks: u64 = stacks.iter().filter_map(|x| x["bytes"].as_u64()).sum();
            json!({
                "name": st.name, "driver": st.driver.word(), "total_bytes": total, "free_bytes": free, "used_bytes": used,
                "probed_at": probe.map(|(_, h)| h.at_ms), "probed_by": probe.map(|(id, _)| name(id)),
                "other_bytes": used.map(|u| u.saturating_sub(in_stacks)), "stacks": stacks,
            })
        })
        .collect();
    json!({ "nodes": nodes, "storage": storage })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Access, Entry, StackVolume, Storage, StorageDriver};
    use freightdock_proto::v1::StackDisk;

    fn cluster() -> ClusterState {
        let mut s = crate::stacks::tests::state_with(&["n1", "n2"]);
        s.nodes.get_mut("n1").unwrap().labels.insert("name".into(), "w1".into());
        s.storage.insert("ceph".into(), Storage { name: "ceph".into(), driver: StorageDriver::Cephfs, source: "192.0.2.1:/".into(), options: String::new(), added_at: 0, ceph: None, password: None });
        let v = |path: Option<&str>| StackVolume { storage: "ceph".into(), access: Access::One, path: path.map(str::to_string) };
        for (stack, vols) in [("ntfy", vec![("data", v(Some("ntfy/data")))]), ("db", vec![("pg", v(None)), ("wal", v(None))])] {
            let i = s.applied + 1;
            s.apply_at(i, &Entry::StackDeployed { name: stack.into(), compose: String::new(), services: Default::default(), placements: Default::default(), at: 1, nodes: vec![], exposures: None, volumes: vols.into_iter().map(|(n, v)| (n.to_string(), v)).collect(), options: Default::default(), stopped: false }).unwrap();
        }
        s
    }

    fn folder(dir: &str, bytes: u64) -> FolderSize {
        FolderSize { storage: "ceph".into(), dir: dir.into(), bytes, ..Default::default() }
    }

    #[test]
    fn a_node_s_disk_is_its_stacks_largest_first_and_what_else_takes_it() {
        let u = UsageReport { stacks: vec![StackDisk { stack: "small".into(), containers_bytes: 1, volumes_bytes: 2 }, StackDisk { stack: "big".into(), containers_bytes: 100, volumes_bytes: 900 }], images_bytes: 5_000, loose_volumes_bytes: 10, build_cache_bytes: 0, at_ms: 7, ..Default::default() };
        let v = view(&cluster(), &[Seen { id: "n1".into(), health: "healthy".into(), disk_total: Some(100_000), disk_used: Some(40_000), usage: Some(u), storage: None }, Seen { id: "gone".into(), ..Default::default() }]);
        let n = &v["nodes"];
        assert_eq!(n.as_array().unwrap().len(), 1, "a node the cluster no longer has is left out");
        assert_eq!((n[0]["name"].as_str(), n[0]["free_bytes"].as_u64(), n[0]["measured_at"].as_i64()), (Some("w1"), Some(60_000), Some(7)));
        assert_eq!(n[0]["stacks"][0], json!({ "stack": "big", "bytes": 1000, "containers_bytes": 100, "volumes_bytes": 900 }));
        assert_eq!(n[0]["stacks"][1]["stack"], "small");
        assert_eq!(n[0]["other_bytes"], 40_000 - 1_003 - 5_010);
    }

    #[test]
    fn a_node_not_measured_yet_has_its_disk_and_no_breakdown() {
        let v = view(&cluster(), &[Seen { id: "n2".into(), disk_total: Some(10), disk_used: Some(4), ..Default::default() }]);
        assert_eq!(v["nodes"][0]["used_bytes"], 4);
        assert!(v["nodes"][0]["other_bytes"].is_null() && v["nodes"][0]["measured_at"].is_null());
        assert_eq!(v["nodes"][0]["stacks"], json!([]));
    }

    #[test]
    fn a_storage_is_its_newest_probe_and_each_stack_its_folders_newest_measure() {
        let probe = |ok: bool, total: u64, free: u64, at: i64| StorageReport { storages: vec![StorageHealth { name: "ceph".into(), ok, total_bytes: total, free_bytes: free, at_ms: at, ..Default::default() }] };
        let seen = [
            Seen { id: "n1".into(), storage: Some(probe(true, 1_000_000, 600_000, 10)), usage: Some(UsageReport { at_ms: 50, folders: vec![folder("ntfy/data", 1_000), folder("db/pg", 50_000)], ..Default::default() }), ..Default::default() },
            Seen { id: "n2".into(), storage: Some(probe(false, 9, 9, 99)), usage: Some(UsageReport { at_ms: 80, folders: vec![folder("ntfy/data", 2_000), FolderSize { error: "gone".into(), ..folder("db/wal", 0) }], ..Default::default() }), ..Default::default() },
        ];
        let v = view(&cluster(), &seen);
        let st = &v["storage"][0];
        assert_eq!((st["total_bytes"].as_u64(), st["free_bytes"].as_u64(), st["used_bytes"].as_u64()), (Some(1_000_000), Some(600_000), Some(400_000)), "a probe that failed says nothing of the size");
        assert_eq!(st["stacks"][0]["stack"], "db", "largest first");
        assert_eq!((st["stacks"][0]["bytes"].as_u64(), st["stacks"][0]["measured"].as_bool()), (Some(50_000), Some(false)), "wal not measured: at least what is known");
        assert_eq!(st["stacks"][1]["volumes"][0]["bytes"], 2_000, "the newer measure, from the other node");
        assert_eq!(st["stacks"][1]["volumes"][0]["measured_by"], "n2");
        assert_eq!(st["other_bytes"], 400_000 - 52_000);
    }

    #[test]
    fn a_storage_nobody_probed_or_measured_has_nulls_not_guesses() {
        let v = view(&cluster(), &[]);
        let st = &v["storage"][0];
        assert!(st["total_bytes"].is_null() && st["used_bytes"].is_null() && st["other_bytes"].is_null());
        assert!(st["stacks"][0]["bytes"].is_null());
    }
}
