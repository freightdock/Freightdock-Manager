//! A node's Docker for a person (the UI's per-node pages), through its worker: a `DockerRequest` on the worker's
//! stream and its `DockerReply`, waited for here by id. Not a command and not in the log: a read is large and only for
//! now, a change is done once for whoever asked. The manager holding the node's stream sends it; another manager
//! forwards to that one (`ManagerPeer.Docker`).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::oneshot;

use freightdock_proto::v1::{self, DockerReply, DockerRequest, PeerDockerRequest, manager_message};

use crate::Manager;

/// What a read may ask; anything else a change.
pub const READS: [&str; 8] = ["info", "containers", "container", "logs", "images", "volumes", "networks", "disks"];
pub const CHANGES: [&str; 9] = ["container.start", "container.stop", "container.restart", "container.remove", "image.remove", "image.pull", "images.prune", "volume.remove", "network.remove"];

/// How long a reply is waited for: a pull may take minutes, anything else seconds.
pub fn patience(action: &str) -> Duration {
    if action == "image.pull" { Duration::from_secs(600) } else { Duration::from_secs(30) }
}

pub type Waits = Mutex<HashMap<String, oneshot::Sender<DockerReply>>>;

#[derive(Debug, Clone, PartialEq)]
pub enum DockerError {
    UnknownNode,
    UnknownAction(String),
    NotConnected,
    Older,
    NoAnswer,
    /// The worker's (or Docker's) sentence.
    Refused(String),
    /// Another manager could not be asked.
    Peer(String),
}

impl std::fmt::Display for DockerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DockerError::UnknownNode => f.write_str("no such node"),
            DockerError::UnknownAction(a) => write!(f, "{a:?} is not something a node's Docker is asked"),
            DockerError::NotConnected => f.write_str("the node is not connected to any manager"),
            DockerError::Older => f.write_str("this node's worker is older and has no Docker pages: update it"),
            DockerError::NoAnswer => f.write_str("the worker did not answer in time"),
            DockerError::Refused(s) | DockerError::Peer(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for DockerError {}

impl Manager {
    /// A reply from a worker here: handed to whoever waits for it (none: it came too late).
    pub fn docker_replied(&self, r: DockerReply) {
        if let Some(w) = self.docker_waits.lock().expect("docker waits lock").remove(&r.request_id) {
            let _ = w.send(r);
        }
    }

    /// Asks the node's Docker: `action` (one of `READS` or `CHANGES`) on `target`. The answer's JSON.
    pub async fn docker(&self, node: &str, action: &str, target: &str, params: HashMap<String, String>) -> Result<Value, DockerError> {
        self.docker_within(node, action, target, params, patience(action)).await
    }

    pub async fn docker_within(&self, node: &str, action: &str, target: &str, params: HashMap<String, String>, wait: Duration) -> Result<Value, DockerError> {
        if !READS.contains(&action) && !CHANGES.contains(&action) {
            return Err(DockerError::UnknownAction(action.to_string()));
        }
        let Some(report) = self.store.read(|s| s.nodes.get(node).map(|n| n.report.clone())) else { return Err(DockerError::UnknownNode) };
        // A pull from a private registry: its login, filled in here and nowhere kept.
        let auth = (action == "image.pull").then(|| params.get("image").and_then(|i| self.store.read(|s| crate::registry::auth_for(&s.registries, i)))).flatten();
        let req = DockerRequest { request_id: uuid::Uuid::new_v4().to_string(), action: action.to_string(), target: target.to_string(), params, auth };
        let reply = if self.sessions.sender(node).is_some() {
            if !report.as_ref().is_some_and(|r| r.docker_requests) {
                return Err(DockerError::Older);
            }
            self.docker_local(node, req, wait).await?
        } else {
            let via = self.sessions.liveness(node).via.ok_or(DockerError::NotConnected)?;
            if !report.as_ref().is_some_and(|r| r.docker_requests) {
                return Err(DockerError::Older);
            }
            let ch = self.peer(&via).map_err(|e| DockerError::Peer(format!("the manager at {via}: {e:#}")))?;
            let mut client = v1::manager_peer_client::ManagerPeerClient::new(ch);
            let call = client.docker(PeerDockerRequest { node_id: node.to_string(), request: Some(req) });
            match tokio::time::timeout(wait + Duration::from_secs(2), call).await {
                Err(_) => return Err(DockerError::NoAnswer),
                Ok(Err(s)) if s.code() == tonic::Code::DeadlineExceeded => return Err(DockerError::NoAnswer),
                Ok(Err(s)) if s.code() == tonic::Code::Unavailable && s.message().contains("not connected") => return Err(DockerError::NotConnected),
                Ok(Err(s)) => return Err(DockerError::Peer(format!("the manager at {via}: {}", s.message()))),
                Ok(Ok(r)) => r.into_inner(),
            }
        };
        if !reply.error.is_empty() {
            return Err(DockerError::Refused(reply.error));
        }
        Ok(if reply.json.is_empty() { Value::Null } else { serde_json::from_str(&reply.json).unwrap_or(Value::Null) })
    }

    /// Sends the request to the worker connected here and waits for its reply.
    pub async fn docker_local(&self, node: &str, req: DockerRequest, wait: Duration) -> Result<DockerReply, DockerError> {
        let tx = self.sessions.sender(node).ok_or(DockerError::NotConnected)?;
        let id = req.request_id.clone();
        let (done, reply) = oneshot::channel();
        self.docker_waits.lock().expect("docker waits lock").insert(id.clone(), done);
        let msg = v1::ManagerMessage { body: Some(manager_message::Body::Docker(req)) };
        if tx.send(Ok(msg)).await.is_err() {
            self.docker_waits.lock().expect("docker waits lock").remove(&id);
            return Err(DockerError::NotConnected);
        }
        let r = tokio::time::timeout(wait, reply).await;
        self.docker_waits.lock().expect("docker waits lock").remove(&id);
        match r {
            Ok(Ok(r)) => Ok(r),
            _ => Err(DockerError::NoAnswer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_and_changes_are_apart_and_a_pull_may_take_long() {
        assert!(READS.iter().all(|r| !CHANGES.contains(r)));
        assert!(patience("image.pull") > patience("containers"));
    }
}
