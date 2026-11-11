//! The Raft log and vote on disk: `<data>/raft/log.jsonl`, one record per line, each synced before the call returns.
//! One file for votes and entries keeps them in the order they were written, which Raft needs (a vote and a log write
//! must never be reordered). Everything is also kept in memory. After `COMPACT_AFTER` records the file is rewritten
//! from memory (written whole, synced, renamed into place), so truncations and purges do not pile up.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Write;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::OptionalSend;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use super::{LogId, RaftEntry, TypeConfig, Vote, io_err};

const COMPACT_AFTER: usize = 2000;

// Externally tagged (serde's default), not `tag = "..."`: an internally tagged enum buffers its content, and a buffered
// map with number keys (a membership's nodes) cannot be read back.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Record {
    Vote { vote: Vote },
    Committed { log_id: Option<LogId> },
    Append { entries: Vec<RaftEntry> },
    TruncateAfter { log_id: Option<LogId> },
    Purge { log_id: LogId },
}

#[derive(Default)]
struct Inner {
    path: Option<PathBuf>,
    file: Option<std::fs::File>,
    records: usize,
    vote: Option<Vote>,
    committed: Option<LogId>,
    last_purged: Option<LogId>,
    log: BTreeMap<u64, RaftEntry>,
}

/// Shared by the store and its readers, so a reader sees entries appended after it was made.
#[derive(Clone, Default)]
pub struct LogStore {
    inner: Arc<Mutex<Inner>>,
}

impl Inner {
    fn apply(&mut self, r: Record) {
        match r {
            Record::Vote { vote } => self.vote = Some(vote),
            Record::Committed { log_id } => self.committed = log_id,
            Record::Append { entries } => {
                for e in entries {
                    self.log.insert(e.log_id.index, e);
                }
            }
            Record::TruncateAfter { log_id } => {
                let keep = log_id.map(|l| l.index + 1).unwrap_or(0);
                self.log.split_off(&keep);
            }
            Record::Purge { log_id } => {
                self.log = self.log.split_off(&(log_id.index + 1));
                self.last_purged = Some(log_id);
            }
        }
    }

    fn write(&mut self, r: Record) -> std::io::Result<()> {
        if let Some(f) = self.file.as_mut() {
            let mut line = serde_json::to_vec(&r).map_err(io_err)?;
            line.push(b'\n');
            f.write_all(&line)?;
            f.sync_data()?;
            self.records += 1;
        }
        self.apply(r);
        if self.records > COMPACT_AFTER {
            self.compact()?;
        }
        Ok(())
    }

    fn compact(&mut self) -> std::io::Result<()> {
        let Some(path) = self.path.clone() else { return Ok(()) };
        let tmp = path.with_extension("jsonl.tmp");
        let mut f = std::fs::File::create(&tmp)?;
        let mut n = 0;
        let mut put = |r: Record| -> std::io::Result<()> {
            let mut line = serde_json::to_vec(&r).map_err(io_err)?;
            line.push(b'\n');
            n += 1;
            f.write_all(&line)
        };
        if let Some(vote) = self.vote {
            put(Record::Vote { vote })?;
        }
        if let Some(log_id) = self.last_purged {
            put(Record::Purge { log_id })?;
        }
        put(Record::Append { entries: self.log.values().cloned().collect() })?;
        put(Record::Committed { log_id: self.committed })?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, &path)?;
        if let Some(dir) = path.parent() {
            std::fs::File::open(dir)?.sync_all()?;
        }
        self.file = Some(std::fs::OpenOptions::new().append(true).open(&path)?);
        self.records = n;
        Ok(())
    }
}

impl LogStore {
    /// The log in `dir`, read back.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("log.jsonl");
        let mut inner = Inner { path: Some(path.clone()), ..Default::default() };
        if path.exists() {
            let text = std::fs::read_to_string(&path)?;
            let mut good = 0;
            let lines: Vec<&str> = text.split_inclusive('\n').collect();
            for (n, line) in lines.iter().enumerate() {
                let parsed = if line.ends_with('\n') { serde_json::from_str::<Record>(line.trim()).map_err(|e| e.to_string()) } else { Err("no end of line".into()) };
                match parsed {
                    Ok(r) => {
                        inner.apply(r);
                        inner.records += 1;
                        good += line.len();
                    }
                    // A torn last record (a crash mid-write, before its sync) was never acknowledged: cut it off.
                    Err(_) if n + 1 == lines.len() => {
                        tracing::warn!("{}: a torn last record was cut off", path.display());
                        std::fs::OpenOptions::new().write(true).open(&path)?.set_len(good as u64)?;
                    }
                    // Anything else unreadable is damage: refuse to start rather than lose what follows.
                    Err(e) => return Err(std::io::Error::other(format!("{} record {} is unreadable ({e}); the manager will not start on a damaged log", path.display(), n + 1))),
                }
            }
        }
        inner.file = Some(std::fs::OpenOptions::new().create(true).append(true).open(&path)?);
        Ok(LogStore { inner: Arc::new(Mutex::new(inner)) })
    }

    /// Whether this manager has ever been part of a cluster (a vote or a log).
    pub async fn is_pristine(&self) -> bool {
        let g = self.inner.lock().await;
        g.vote.is_none() && g.log.is_empty() && g.last_purged.is_none()
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(&mut self, range: RB) -> Result<Vec<RaftEntry>, std::io::Error> {
        Ok(self.inner.lock().await.log.range(range).map(|(_, e)| e.clone()).collect())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, std::io::Error> {
        Ok(self.inner.lock().await.vote)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, std::io::Error> {
        let g = self.inner.lock().await;
        let last = g.log.values().next_back().map(|e| e.log_id).or_else(|| g.last_purged);
        Ok(LogState { last_purged_log_id: g.last_purged, last_log_id: last })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), std::io::Error> {
        self.inner.lock().await.write(Record::Vote { vote: *vote })
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), std::io::Error> {
        self.inner.lock().await.write(Record::Committed { log_id: committed })
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, std::io::Error> {
        Ok(self.inner.lock().await.committed)
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> Result<(), std::io::Error>
    where
        I: IntoIterator<Item = RaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<RaftEntry> = entries.into_iter().collect();
        let r = self.inner.lock().await.write(Record::Append { entries });
        let ok = r.as_ref().map(|_| ()).map_err(|e| std::io::Error::new(e.kind(), e.to_string()));
        callback.io_completed(ok);
        r
    }

    async fn truncate_after(&mut self, log_id: Option<LogId>) -> Result<(), std::io::Error> {
        self.inner.lock().await.write(Record::TruncateAfter { log_id })
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), std::io::Error> {
        self.inner.lock().await.write(Record::Purge { log_id })
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use openraft::entry::RaftEntry as _;
    use openraft::type_config::TypeConfigExt;
    use openraft::Membership;
    use std::collections::BTreeSet;

    fn log_id(term: u64, index: u64) -> LogId {
        openraft::testing::log_id::<TypeConfig>(term, 1, index)
    }

    async fn append(s: &mut LogStore, entries: Vec<RaftEntry>) {
        s.append(entries, openraft::storage::IOFlushed::<TypeConfig>::noop()).await.unwrap();
    }

    #[test]
    fn every_kind_of_record_reads_back_after_a_restart() {
        TypeConfig::run(async {
            let dir = tempfile::tempdir().unwrap();
            let membership: openraft::type_config::alias::MembershipOf<TypeConfig> = Membership::new_with_defaults(vec![BTreeSet::from([7u64])], []);
            {
                let mut s = LogStore::open(dir.path()).unwrap();
                append(&mut s, vec![RaftEntry::new_membership(log_id(0, 0), membership.clone())]).await;
                append(&mut s, vec![RaftEntry::new_normal(log_id(1, 1), crate::state::Entry::ClusterCreated { cluster_id: "c".into(), at: 0 })]).await;
                append(&mut s, vec![RaftEntry::new_blank(log_id(1, 2))]).await;
                s.save_committed(Some(log_id(1, 2))).await.unwrap();
            }
            let mut s = LogStore::open(dir.path()).unwrap();
            assert_eq!(s.get_log_state().await.unwrap().last_log_id, Some(log_id(1, 2)));
            assert_eq!(s.try_get_log_entries(0..3).await.unwrap().len(), 3);
            assert_eq!(s.read_committed().await.unwrap(), Some(log_id(1, 2)));
        });
    }

    #[test]
    fn a_torn_last_record_is_cut_and_damage_before_it_stops_the_start() {
        TypeConfig::run(async {
            let dir = tempfile::tempdir().unwrap();
            {
                let mut s = LogStore::open(dir.path()).unwrap();
                append(&mut s, vec![RaftEntry::new_blank(log_id(1, 1))]).await;
            }
            let path = dir.path().join("log.jsonl");
            let good = std::fs::read_to_string(&path).unwrap();
            std::fs::write(&path, format!("{good}{{\"append\":{{\"entr")).unwrap();
            let mut s = LogStore::open(dir.path()).unwrap();
            assert_eq!(s.get_log_state().await.unwrap().last_log_id, Some(log_id(1, 1)));
            assert_eq!(std::fs::read_to_string(&path).unwrap(), good, "the torn tail is gone");
            drop(s);
            std::fs::write(&path, format!("garbage\n{good}")).unwrap();
            assert!(LogStore::open(dir.path()).is_err(), "damage in the middle is not cut away");
        });
    }
}
