//! Images ready where a workload would go (setting `prepull`, on by default): a node that fails has its workloads
//! started elsewhere (`recovery`), and on a node that never ran them that waits for the image to download, minutes
//! for a large one on a slow link. So every few minutes the leader works out, for each healthy node, where recovery
//! would put its workloads if that node failed now (`recovery::plan` with the node left out, the same placement),
//! and has each of those nodes pull the images it lacks. A pinned workload, or one with its data on its node, never
//! moves, so its image is not wanted elsewhere. What is pulled stays tagged, so `images.prune` (dangling only) keeps it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::Ordering;

use serde_json::Value;

use crate::Manager;
use crate::state::ClusterState;

/// Seconds between rounds.
pub const EVERY_S: u64 = 300;

/// Node to the images it would need if another node failed now, for the workloads it would take over.
pub fn wanted(s: &ClusterState, eligible: &[String], sick: &crate::stacks::Sick) -> BTreeMap<String, BTreeSet<String>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for n in eligible {
        if s.workloads_on(n).next().is_none() {
            continue;
        }
        let others: Vec<String> = eligible.iter().filter(|e| *e != n).cloned().collect();
        let p = crate::recovery::plan(s, Some(n), &others, sick);
        for (w, to) in p.moves {
            out.entry(to).or_default().insert(s.workloads[&w].spec.image.clone());
        }
    }
    out
}

/// `want` and each (node, image) a round could not pull, tried again each round: so a node is a candidate for that
/// image again once it gets it (pushed after all, or loaded by hand).
pub fn with_retries(mut want: BTreeMap<String, BTreeSet<String>>, failed: impl IntoIterator<Item = (String, String)>) -> BTreeMap<String, BTreeSet<String>> {
    for (node, image) in failed {
        want.entry(node).or_default().insert(image);
    }
    want
}

/// An image name as Docker lists its tags: `nginx` and `docker.io/library/nginx` are `nginx:latest`.
pub fn canonical(image: &str) -> String {
    let short = image.strip_prefix("docker.io/library/").or_else(|| image.strip_prefix("docker.io/")).unwrap_or(image);
    let last = short.rfind('/').map_or(0, |i| i + 1);
    if short.contains('@') || short[last..].contains(':') { short.to_string() } else { format!("{short}:latest") }
}

/// Of `want`, those not among the tags of a node's `images` list. One named by digest is always pulled: that is
/// answered from the node's own store when it has it.
pub fn missing(have: &Value, want: &BTreeSet<String>) -> Vec<String> {
    let tags: BTreeSet<String> = have.as_array().into_iter().flatten().flat_map(|i| i["tags"].as_array().into_iter().flatten()).filter_map(|t| t.as_str()).map(canonical).collect();
    want.iter().filter(|i| i.contains('@') || !tags.contains(&canonical(i))).cloned().collect()
}

impl Manager {
    /// The leader's round (see the module), every `EVERY_S`, one at a time; the pulls run in the background.
    pub fn prepull_round(self: &std::sync::Arc<Self>) {
        if self.store.leader() != Some(self.store.id) || self.started.elapsed() < crate::fencing::STARTUP_GRACE || self.settings().prepull == Some(false) {
            return;
        }
        if self.prepulling.swap(true, Ordering::SeqCst) {
            return;
        }
        let want = self.store.read(|s| wanted(s, &self.eligible(s), &self.sick(s)));
        let failed: Vec<(String, String)> = self.prepull_failed.lock().expect("prepull lock").iter().cloned().collect();
        let want = with_retries(want, failed);
        let m = self.clone();
        tokio::spawn(async move {
            for (node, images) in want {
                let have = match m.docker(&node, "images", "", HashMap::new()).await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::debug!(node = %node, "prepull: the images could not be listed: {e}");
                        continue;
                    }
                };
                let lacks = missing(&have, &images);
                // One it has now (loaded by hand) is no longer one it cannot get.
                m.prepull_failed.lock().expect("prepull lock").retain(|(n, i)| *n != node || lacks.contains(i) || !images.contains(i));
                for image in lacks {
                    let key = (node.clone(), image.clone());
                    match m.docker(&node, "image.pull", "", HashMap::from([("image".to_string(), image.clone())])).await {
                        Ok(_) => {
                            m.prepull_failed.lock().expect("prepull lock").remove(&key);
                            if !image.contains('@') {
                                tracing::info!(node = %node, "prepull: {image} pulled");
                                m.events.push("prepull", Some(&node), format!("{image} pulled, for the workloads this node would take over"));
                            }
                        }
                        Err(e) => {
                            tracing::warn!(node = %node, "prepull: {image}: {e}");
                            // Said once, not every round, until it works.
                            if m.prepull_failed.lock().expect("prepull lock").insert(key) {
                                m.events.push("prepull", Some(&node), format!("{image} could not be pulled ahead: {e}"));
                            }
                        }
                    }
                }
            }
            m.prepulling.store(false, Ordering::SeqCst);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::FenceState;
    use serde_json::json;

    fn all() -> Vec<String> {
        ["a", "b", "c"].map(String::from).to_vec()
    }

    /// The recovery fixture with node a healthy: web x2 on a and b, db (a volume) and cron (pinned) on a.
    fn healthy() -> ClusterState {
        let mut s = crate::recovery::tests::cluster();
        s.nodes.get_mut("a").unwrap().fencing.state = FenceState::None;
        s
    }

    #[test]
    fn a_node_gets_the_images_of_what_it_would_take_over_and_nothing_that_never_moves() {
        let w = wanted(&healthy(), &all(), &Default::default());
        // a fails: web.1 to c (b has web.2); b fails: web.2 to c. db and cron stay on a whatever happens.
        assert_eq!(w, BTreeMap::from([("c".to_string(), BTreeSet::from(["nginx".to_string()]))]));
    }

    #[test]
    fn a_node_that_could_not_get_an_image_is_passed_over_for_one_that_can() {
        let sick = crate::stacks::Sick { images: vec![("c".to_string(), "nginx".to_string())], ..Default::default() };
        let w = wanted(&healthy(), &all(), &sick);
        assert!(w.get("c").is_none_or(|i| !i.contains("nginx")), "{w:?}");
        assert!(w.values().any(|i| i.contains("nginx")), "it goes where it can be pulled: {w:?}");
    }

    #[test]
    fn what_could_not_be_pulled_is_tried_again() {
        let want = with_retries(BTreeMap::from([("a".to_string(), BTreeSet::from(["x".to_string()]))]), [("b".to_string(), "y".to_string())]);
        assert_eq!(want, BTreeMap::from([("a".to_string(), BTreeSet::from(["x".to_string()])), ("b".to_string(), BTreeSet::from(["y".to_string()]))]));
    }

    #[test]
    fn with_no_other_node_nothing_is_wanted() {
        assert!(wanted(&healthy(), &["a".to_string()], &Default::default()).is_empty());
    }

    #[test]
    fn names_compare_as_docker_lists_them() {
        assert_eq!(canonical("nginx"), "nginx:latest");
        assert_eq!(canonical("docker.io/library/nginx:1.27"), "nginx:1.27");
        assert_eq!(canonical("docker.io/grafana/grafana"), "grafana/grafana:latest");
        assert_eq!(canonical("registry.local:5000/app"), "registry.local:5000/app:latest");
        assert_eq!(canonical("ghcr.io/a/b@sha256:abc"), "ghcr.io/a/b@sha256:abc");
    }

    #[test]
    fn only_what_the_node_lacks_is_pulled_and_a_digest_always() {
        let have = json!([{ "tags": ["nginx:latest", "redis:7"] }, { "tags": null }]);
        let want = BTreeSet::from(["nginx".to_string(), "redis:7".to_string(), "redis:8".to_string(), "x/y@sha256:abc".to_string()]);
        assert_eq!(missing(&have, &want), ["redis:8", "x/y@sha256:abc"]);
    }
}
