//! The state machine: `ClusterState`, the last log id applied, and the membership, in memory; durable through
//! snapshots (`<data>/raft/snapshot.json`, written whole, synced and renamed into place). After a restart the state is
//! the last snapshot's and Raft applies the log after it again, which is why `applied_state` may report the snapshot's
//! log id: only what a snapshot holds is known to be durable here.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use futures_util::TryStreamExt;
use openraft::storage::{EntryResponder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder};
use serde::{Deserialize, Serialize};

use super::{Applied, LogId, Membership, Snapshot, SnapshotMeta, TypeConfig, io_err};
use crate::state::{ClusterState, CommandRecord, Entry};

pub type SnapshotData = Vec<u8>;

#[derive(Clone, Default, Serialize, Deserialize)]
struct Image {
    state: ClusterState,
    last_applied: Option<LogId>,
    membership: Membership,
}

#[derive(Clone, Serialize, Deserialize)]
struct Stored {
    meta: SnapshotMeta,
    data: SnapshotData,
}

#[derive(Default)]
struct Inner {
    now: Image,
    snapshot: Option<Stored>,
}

#[derive(Clone)]
pub struct StateMachine {
    inner: Arc<Mutex<Inner>>,
    path: Option<PathBuf>,
    /// Each command as it is applied, so every manager can deliver it to the workers connected to it, wherever it was
    /// made.
    commands: tokio::sync::broadcast::Sender<CommandRecord>,
}

impl StateMachine {
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("snapshot.json");
        let snapshot: Option<Stored> = match std::fs::read(&path) {
            Ok(b) => Some(serde_json::from_slice(&b).map_err(io_err)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };
        let now = match &snapshot {
            Some(s) => serde_json::from_slice(&s.data).map_err(io_err)?,
            None => Image::default(),
        };
        Ok(StateMachine { inner: Arc::new(Mutex::new(Inner { now, snapshot })), path: Some(path), commands: tokio::sync::broadcast::channel(1024).0 })
    }

    /// Commands as they are applied from now on.
    pub fn commands(&self) -> tokio::sync::broadcast::Receiver<CommandRecord> {
        self.commands.subscribe()
    }

    /// Reads the applied state.
    pub fn read<T>(&self, f: impl FnOnce(&ClusterState) -> T) -> T {
        f(&self.inner.lock().expect("sm lock").now.state)
    }

    fn keep(&self, s: &Stored) -> std::io::Result<()> {
        let Some(path) = &self.path else { return Ok(()) };
        let tmp = path.with_extension("json.tmp");
        let f = std::fs::File::create(&tmp)?;
        serde_json::to_writer(&f, s).map_err(io_err)?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        if let Some(dir) = path.parent() {
            std::fs::File::open(dir)?.sync_all()?;
        }
        Ok(())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    type SnapshotData = SnapshotData;

    async fn build_snapshot(&mut self) -> Result<Snapshot, std::io::Error> {
        let image = self.inner.lock().expect("sm lock").now.clone();
        let data = serde_json::to_vec(&image).map_err(io_err)?;
        let meta = SnapshotMeta { last_log_id: image.last_applied, last_membership: image.membership.clone() };
        let stored = Stored { meta: meta.clone(), data: data.clone() };
        self.keep(&stored)?;
        self.inner.lock().expect("sm lock").snapshot = Some(stored);
        Ok(Snapshot { meta, snapshot: data })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, Membership), std::io::Error> {
        let g = self.inner.lock().expect("sm lock");
        Ok((g.now.last_applied, g.now.membership.clone()))
    }

    async fn apply<S>(&mut self, mut entries: S) -> Result<(), std::io::Error>
    where S: futures_util::Stream<Item = Result<EntryResponder<TypeConfig>, std::io::Error>> + Unpin + OptionalSend {
        while let Some((entry, responder)) = entries.try_next().await? {
            let answer = {
                let mut g = self.inner.lock().expect("sm lock");
                g.now.last_applied = Some(entry.log_id);
                match entry.payload {
                    EntryPayload::Blank => Applied(Ok(())),
                    EntryPayload::Normal(e) => {
                        let r = g.now.state.apply_at(entry.log_id.index, &e);
                        if let (Ok(()), Entry::CommandCreated { command }) = (&r, &e) {
                            // No one listening (a start replaying its log) is fine.
                            let _ = self.commands.send(command.clone());
                        }
                        Applied(r)
                    }
                    EntryPayload::Membership(m) => {
                        g.now.membership = Membership::new(Some(entry.log_id), m);
                        Applied(Ok(()))
                    }
                }
            };
            if let Some(r) = responder {
                r.send(answer);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(&mut self, meta: &SnapshotMeta, snapshot: SnapshotData) -> Result<(), std::io::Error> {
        let image: Image = serde_json::from_slice(&snapshot).map_err(io_err)?;
        let stored = Stored { meta: meta.clone(), data: snapshot };
        self.keep(&stored)?;
        let mut g = self.inner.lock().expect("sm lock");
        g.now = image;
        g.now.last_applied = meta.last_log_id;
        g.now.membership = meta.last_membership.clone();
        g.snapshot = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, std::io::Error> {
        Ok(self.inner.lock().expect("sm lock").snapshot.clone().map(|s| Snapshot { meta: s.meta, snapshot: s.data }))
    }
}
