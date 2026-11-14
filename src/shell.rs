//! Shells (the UI's terminal) on a node or in one of its containers. The manager holding the node's session sends it
//! ShellOpen and waits for the worker to dial back (WorkerSession.Shell) with the session's id; another manager asks
//! that one through ManagerPeer.Shell. Either way a shell is a `Pipe`: what the shell writes comes out of `up`, what is
//! typed goes into `down`, and dropping `down` ends it. Nothing typed or written is kept, logged or put in the
//! cluster's log; that a shell was opened, on what, is an event.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Status, Streaming};

use freightdock_proto::v1::{self, PeerShellDown, ShellDown, ShellEnd, ShellOpen, ShellUp, manager_message, shell_up};

use crate::Manager;

/// How long the worker has to dial back once asked.
pub const DIAL_BACK: Duration = Duration::from_secs(15);

/// A shell, wherever it runs: its output and end come out of `up`, what is typed goes into `down`.
pub struct Pipe {
    pub up: mpsc::Receiver<ShellUp>,
    pub down: mpsc::Sender<ShellDown>,
}

/// A stream of a shell's output, from a worker or another manager.
pub type UpStream = std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ShellUp, Status>> + Send>>;

/// What the worker's dial-back hands the manager waiting for it: its stream up, and where to send down.
type Dialled = (UpStream, mpsc::Sender<Result<ShellDown, Status>>);

/// The shells asked for and not yet dialled back: by session id, the node asked and who waits.
#[derive(Default)]
pub struct Waits(Mutex<HashMap<String, (String, oneshot::Sender<Dialled>)>>);

impl Waits {
    fn put(&self, id: &str, node: &str, tx: oneshot::Sender<Dialled>) {
        self.0.lock().expect("shell waits lock").insert(id.to_string(), (node.to_string(), tx));
    }

    fn drop_id(&self, id: &str) {
        self.0.lock().expect("shell waits lock").remove(id);
    }

    /// The waiter of session `id`, if it was asked of `node` (a node dials back for its own shells only).
    pub fn take(&self, id: &str, node: &str) -> Option<oneshot::Sender<Dialled>> {
        let mut w = self.0.lock().expect("shell waits lock");
        match w.get(id) {
            Some((n, _)) if n == node => w.remove(id).map(|(_, tx)| tx),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ShellError {
    UnknownNode,
    Off,
    Older,
    HostOff,
    NotConnected,
    NoAnswer,
    Peer(String),
    BadContainer(String),
}

impl std::fmt::Display for ShellError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShellError::UnknownNode => f.write_str("no such node"),
            ShellError::Off => f.write_str("shells are turned off for this cluster (its settings)"),
            ShellError::Older => f.write_str("the node's worker is older and has no shells; upgrade it"),
            ShellError::HostOff => f.write_str("the host's shell is turned off on this node (FREIGHTDOCK_WORKER_HOST_SHELL)"),
            ShellError::NotConnected => f.write_str("the node is not connected to any manager"),
            ShellError::NoAnswer => f.write_str("the worker did not open the shell in time"),
            ShellError::Peer(m) => f.write_str(m),
            ShellError::BadContainer(c) => write!(f, "{c:?} is not a container's name or id"),
        }
    }
}

/// A container as `docker exec` takes it: a name or an id, nothing else.
pub fn container_ok(c: &str) -> bool {
    c.is_empty() || (c.len() <= 128 && c.chars().all(|ch| ch.is_ascii_alphanumeric() || "_.-".contains(ch)) && !c.starts_with(['.', '-']))
}

/// Copies a worker's (or a peer's) stream up into a pipe's `up`, its session id left out; ends with its end, or says
/// it broke.
fn pump_up(mut s: UpStream, tx: mpsc::Sender<ShellUp>) {
    use futures_util::StreamExt;
    tokio::spawn(async move {
        loop {
            match s.next().await.transpose() {
                Ok(Some(ShellUp { body: Some(shell_up::Body::SessionId(_)) })) => continue,
                Ok(Some(m)) => {
                    let end = matches!(m.body, Some(shell_up::Body::End(_)));
                    if tx.send(m).await.is_err() || end {
                        return;
                    }
                }
                Ok(None) => return,
                Err(e) => {
                    let _ = tx.send(ShellUp { body: Some(shell_up::Body::End(ShellEnd { exit_code: -1, error: format!("the shell's stream broke: {}", e.message()) })) }).await;
                    return;
                }
            }
        }
    });
}

impl Manager {
    /// Whether a shell may be opened on `node` (`container` empty for the host): the cluster allows shells, the node
    /// is known and its worker opens them (on the host too, for the host's).
    pub fn shell_allowed(&self, node: &str, container: &str) -> Result<(), ShellError> {
        if !container_ok(container) {
            return Err(ShellError::BadContainer(container.to_string()));
        }
        if self.settings().shells == Some(false) {
            return Err(ShellError::Off);
        }
        let report = self.store.read(|s| s.nodes.get(node).map(|n| n.report.clone())).ok_or(ShellError::UnknownNode)?;
        let caps = report.as_ref().map(|r| (r.shells, r.host_shell)).unwrap_or_default();
        if !caps.0 {
            return Err(ShellError::Older);
        }
        if container.is_empty() && !caps.1 {
            return Err(ShellError::HostOff);
        }
        Ok(())
    }

    /// Opens a shell on `node` (`container` empty for the host), here or through the manager that holds its stream.
    pub async fn shell(&self, node: &str, container: &str, cols: u32, rows: u32) -> Result<Pipe, ShellError> {
        self.shell_allowed(node, container)?;
        let open = ShellOpen { session_id: uuid::Uuid::new_v4().to_string(), container: container.to_string(), cols, rows };
        let pipe = if self.sessions.sender(node).is_some() {
            self.shell_local(node, open).await?
        } else {
            let via = self.sessions.liveness(node).via.ok_or(ShellError::NotConnected)?;
            let ch = self.peer(&via).map_err(|e| ShellError::Peer(format!("the manager at {via}: {e:#}")))?;
            let (tx, rx) = mpsc::channel::<PeerShellDown>(64);
            let _ = tx.send(PeerShellDown { node_id: node.to_string(), open: Some(open), down: None }).await;
            let up = v1::manager_peer_client::ManagerPeerClient::new(ch).shell(ReceiverStream::new(rx)).await.map_err(|s| match s.code() {
                tonic::Code::Unavailable if s.message().contains("not connected") => ShellError::NotConnected,
                tonic::Code::DeadlineExceeded => ShellError::NoAnswer,
                _ => ShellError::Peer(format!("the manager at {via}: {}", s.message())),
            })?;
            let (up_tx, up_rx) = mpsc::channel(64);
            pump_up(Box::pin(up.into_inner()), up_tx);
            let (down_tx, mut down_rx) = mpsc::channel::<ShellDown>(64);
            tokio::spawn(async move {
                while let Some(d) = down_rx.recv().await {
                    if tx.send(PeerShellDown { node_id: String::new(), open: None, down: Some(d) }).await.is_err() {
                        return;
                    }
                }
            });
            Pipe { up: up_rx, down: down_tx }
        };
        let on = if container.is_empty() { "the host".to_string() } else { format!("container {container}") };
        self.events.push("shell", Some(node), format!("a shell opened on {on}"));
        Ok(pipe)
    }

    /// Opens a shell on a node whose stream this manager holds: asks it, and waits for it to dial back.
    pub async fn shell_local(&self, node: &str, open: ShellOpen) -> Result<Pipe, ShellError> {
        let tx = self.sessions.sender(node).ok_or(ShellError::NotConnected)?;
        let id = open.session_id.clone();
        let (done, dialled) = oneshot::channel();
        self.shell_waits.put(&id, node, done);
        if tx.send(Ok(v1::ManagerMessage { body: Some(manager_message::Body::ShellOpen(open)) })).await.is_err() {
            self.shell_waits.drop_id(&id);
            return Err(ShellError::NotConnected);
        }
        let r = tokio::time::timeout(DIAL_BACK, dialled).await;
        self.shell_waits.drop_id(&id);
        let Ok(Ok((up, down))) = r else { return Err(ShellError::NoAnswer) };
        let (up_tx, up_rx) = mpsc::channel(64);
        pump_up(up, up_tx);
        let (down_tx, mut down_rx) = mpsc::channel::<ShellDown>(64);
        tokio::spawn(async move {
            while let Some(d) = down_rx.recv().await {
                if down.send(Ok(d)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Pipe { up: up_rx, down: down_tx })
    }

    /// A worker dialling back (WorkerSession.Shell) as `node`: its first message names the session it was asked for.
    pub async fn shell_dialled(&self, node: &str, mut up: UpStream) -> Result<ReceiverStream<Result<ShellDown, Status>>, Status> {
        use futures_util::StreamExt;
        let id = match tokio::time::timeout(Duration::from_secs(10), up.next()).await {
            Ok(Some(Ok(ShellUp { body: Some(shell_up::Body::SessionId(id)) }))) => id,
            _ => return Err(Status::invalid_argument("the first message must name the shell's session")),
        };
        let waiter = self.shell_waits.take(&id, node).ok_or_else(|| Status::not_found("no such shell was asked of this node"))?;
        let (tx, rx) = mpsc::channel(64);
        waiter.send((up, tx)).map_err(|_| Status::cancelled("nobody waits for this shell any more"))?;
        Ok(ReceiverStream::new(rx))
    }

    /// Another manager's shell on a node whose stream this one holds (ManagerPeer.Shell).
    pub async fn shell_for_peer(self: &Arc<Self>, mut down: Streaming<PeerShellDown>) -> Result<ReceiverStream<Result<ShellUp, Status>>, Status> {
        let first = match tokio::time::timeout(Duration::from_secs(10), down.message()).await {
            Ok(Ok(Some(f))) => f,
            _ => return Err(Status::invalid_argument("no shell asked")),
        };
        let open = first.open.ok_or_else(|| Status::invalid_argument("the first message must open the shell"))?;
        if !container_ok(&open.container) {
            return Err(Status::invalid_argument("not a container's name or id"));
        }
        let mut pipe = match self.shell_local(&first.node_id, open).await {
            Ok(p) => p,
            Err(ShellError::NoAnswer) => return Err(Status::deadline_exceeded("the worker did not open the shell in time")),
            Err(e) => return Err(Status::unavailable(e.to_string())),
        };
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            while let Some(m) = pipe.up.recv().await {
                if tx.send(Ok(m)).await.is_err() {
                    return;
                }
            }
        });
        let typed = pipe.down;
        tokio::spawn(async move {
            while let Ok(Some(m)) = down.message().await {
                if let Some(d) = m.down
                    && typed.send(d).await.is_err()
                {
                    return;
                }
            }
        });
        Ok(ReceiverStream::new(rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_container_s_name_or_id_reaches_docker_exec() {
        for ok in ["", "web", "a1b2c3", "stack.web.1", "my_app-2"] {
            assert!(container_ok(ok), "{ok}");
        }
        for bad in ["-it", ".hidden", "a b", "a;b", "../x", &"x".repeat(129)] {
            assert!(!container_ok(bad), "{bad}");
        }
    }

    #[test]
    fn a_dial_back_is_taken_by_the_node_it_was_asked_of_only() {
        let w = Waits::default();
        let (tx, _rx) = oneshot::channel();
        w.put("s1", "n1", tx);
        assert!(w.take("s1", "n2").is_none(), "another node cannot take it");
        assert!(w.take("s1", "n1").is_some());
        assert!(w.take("s1", "n1").is_none(), "taken once");
    }
}
