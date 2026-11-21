//! The store as Pando sees it: segments, refs, checkpoints, write logs, change lists and hints
//! over [`Store`].

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use bytes::Bytes;
use pando_format::{BranchId, Checkpoint, Hash, Recording, Ref, SegId, SegmentIndex, keys, u64_from};

use crate::ports::{Rng, Store, StoreError, Version};
use crate::{Error, Result};

/// Counts of the store requests one [`Remote`] made since it was opened.
#[derive(Debug, Default)]
pub struct RemoteStats {
    /// Successful GETs, ranged or whole.
    pub gets: AtomicU64,
    /// The body bytes those GETs returned.
    pub get_bytes: AtomicU64,
    /// Objects written, counting a segment's data and index apart.
    pub puts: AtomicU64,
    /// The body bytes of those writes.
    pub put_bytes: AtomicU64,
    /// Ref updates that took effect.
    pub ref_puts: AtomicU64,
}

/// A stored segment: the size and upload time of its data.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct SegmentInfo {
    pub(crate) size: u64,
    pub(crate) modified: SystemTime,
}

/// A stored change list: the root it leads to, the root it starts from, and its upload time.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct ChangeListInfo {
    pub(crate) root: Hash,
    pub(crate) from: Hash,
    pub(crate) modified: SystemTime,
}

#[derive(Debug, Clone)]
pub(crate) struct Remote {
    store: Arc<dyn Store>,
    stats: Arc<RemoteStats>,
}

impl Remote {
    pub(crate) fn new(store: Arc<dyn Store>) -> Remote {
        Remote {
            store,
            stats: Arc::default(),
        }
    }

    pub(crate) fn stats(&self) -> &RemoteStats {
        &self.stats
    }

    fn got(&self, n: usize) {
        self.stats.gets.fetch_add(1, Ordering::Relaxed);
        self.stats.get_bytes.fetch_add(u64_from(n), Ordering::Relaxed);
    }

    fn put_done(&self, n: usize) {
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        self.stats.put_bytes.fetch_add(u64_from(n), Ordering::Relaxed);
    }

    /// Stores a segment and its index; neither may exist yet.
    pub(crate) async fn put_segment(&self, id: SegId, data: Bytes, index: Bytes) -> Result<()> {
        let (n, m) = (data.len(), index.len());
        let (dk, ik) = (keys::segment(id), keys::segment_index(id));
        let (a, b) = futures::join!(self.store.create(&dk, data), self.store.create(&ik, index));
        for r in [a, b] {
            match r {
                Ok(_) => {}
                Err(StoreError::AlreadyExists { .. }) => return Err(Error::SegmentExists(id)),
                Err(e) => return Err(e.into()),
            }
        }
        self.put_done(n);
        self.put_done(m);
        Ok(())
    }

    pub(crate) async fn get_range(&self, id: SegId, off: u32, n: u32) -> Result<Bytes> {
        let b = self
            .store
            .get_range(&keys::segment(id), u64::from(off)..u64::from(off) + u64::from(n))
            .await?;
        if b.len() != n as usize {
            return Err(Error::Corrupt(format!(
                "segment {id}: asked {n} bytes at {off}, got {}",
                b.len()
            )));
        }
        self.got(b.len());
        Ok(b)
    }

    /// The last `n` bytes of a segment (all of it when shorter) and the segment's size.
    pub(crate) async fn get_tail(&self, id: SegId, n: u32) -> Result<(Bytes, u32)> {
        let (b, size) = self.store.get_tail(&keys::segment(id), u64::from(n)).await?;
        self.got(b.len());
        let size =
            u32::try_from(size).map_err(|_| Error::Corrupt(format!("segment {id} is {size} bytes")))?;
        Ok((b, size))
    }

    pub(crate) async fn get_index(&self, id: SegId) -> Result<SegmentIndex> {
        let o = self.store.get(&keys::segment_index(id)).await?;
        self.got(o.body.len());
        Ok(SegmentIndex::decode(&o.body)?)
    }

    /// The ref and its version, or `None` when there is no such ref.
    pub(crate) async fn get_ref(&self, name: &str) -> Result<Option<(Ref, Version)>> {
        match self.store.get(&keys::reference(name)).await {
            Ok(o) => {
                self.got(o.body.len());
                Ok(Some((Ref::decode(&o.body)?, o.version)))
            }
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Replaces a ref only if it still has version `expected`; [`Error::Fenced`] when another
    /// writer moved it. When the store's reply is lost, the ref is read back: if it holds `r`,
    /// the update took effect.
    pub(crate) async fn put_ref(&self, name: &str, r: &Ref, expected: &Version) -> Result<Version> {
        let body = Bytes::from(r.encode());
        let n = body.len();
        match self.store.update(&keys::reference(name), body, expected).await {
            Ok(v) => {
                self.put_done(n);
                self.stats.ref_puts.fetch_add(1, Ordering::Relaxed);
                Ok(v)
            }
            Err(StoreError::PreconditionFailed { .. } | StoreError::NotFound { .. }) => Err(Error::Fenced {
                branch: name.to_owned(),
            }),
            Err(e @ StoreError::Transient { .. }) => match self.get_ref(name).await {
                Ok(Some((now, v))) if now == *r => Ok(v),
                _ => Err(e.into()),
            },
            Err(e) => Err(e.into()),
        }
    }

    /// Creates the ref of a new life of branch `name`: the ref `make` builds for a branch id
    /// drawn from `rng`. [`Error::RefExists`] when there is one.
    pub(crate) async fn create_ref(
        &self,
        rng: &dyn Rng,
        name: &str,
        make: impl FnOnce(BranchId) -> Ref,
    ) -> Result<Version> {
        let r = make(new_branch_id(rng));
        let body = Bytes::from(r.encode());
        let n = body.len();
        match self.store.create(&keys::reference(name), body).await {
            Ok(v) => {
                self.put_done(n);
                Ok(v)
            }
            Err(StoreError::AlreadyExists { .. }) => Err(Error::RefExists(name.to_owned())),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn delete_ref(&self, name: &str) -> Result<()> {
        Ok(self.store.delete(&keys::reference(name)).await?)
    }

    pub(crate) async fn list_refs(&self) -> Result<Vec<String>> {
        let prefix = format!("{}/", keys::REFS);
        Ok(self
            .store
            .list(keys::REFS)
            .await?
            .into_iter()
            .filter_map(|o| o.key.strip_prefix(&prefix).map(str::to_owned))
            .collect())
    }

    /// The checkpoint, or `None` when there is no such checkpoint.
    pub(crate) async fn get_checkpoint(&self, name: &str) -> Result<Option<Checkpoint>> {
        match self.store.get(&keys::checkpoint(name)).await {
            Ok(o) => {
                self.got(o.body.len());
                Ok(Some(Checkpoint::decode(&o.body)?))
            }
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Creates a checkpoint; [`Error::CheckpointExists`] when there is one.
    pub(crate) async fn create_checkpoint(&self, name: &str, c: &Checkpoint) -> Result<()> {
        let body = Bytes::from(c.encode());
        let n = body.len();
        match self.store.create(&keys::checkpoint(name), body).await {
            Ok(_) => {
                self.put_done(n);
                Ok(())
            }
            Err(StoreError::AlreadyExists { .. }) => Err(Error::CheckpointExists(name.to_owned())),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn delete_checkpoint(&self, name: &str) -> Result<()> {
        Ok(self.store.delete(&keys::checkpoint(name)).await?)
    }

    pub(crate) async fn list_checkpoints(&self) -> Result<Vec<String>> {
        let prefix = format!("{}/", keys::CHECKPOINTS);
        Ok(self
            .store
            .list(keys::CHECKPOINTS)
            .await?
            .into_iter()
            .filter_map(|o| o.key.strip_prefix(&prefix).map(str::to_owned))
            .collect())
    }

    /// The bytes of record `n` of writer `generation` in the write log of life `id` of branch
    /// `name`, or `None`.
    pub(crate) async fn get_log_record(
        &self,
        name: &str,
        id: BranchId,
        generation: u64,
        n: u64,
    ) -> Result<Option<Bytes>> {
        match self.store.get(&keys::log_record(name, id, generation, n)).await {
            Ok(o) => {
                self.got(o.body.len());
                Ok(Some(o.body))
            }
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Creates a log record; `false` when another record is there already. A record that is
    /// there already, or a create whose reply is lost, is read back: if it holds `body`, an
    /// earlier try of this create took effect.
    pub(crate) async fn create_log_record(
        &self,
        name: &str,
        id: BranchId,
        generation: u64,
        n: u64,
        body: Bytes,
    ) -> Result<bool> {
        let len = body.len();
        match self
            .store
            .create(&keys::log_record(name, id, generation, n), body.clone())
            .await
        {
            Ok(_) => {
                self.put_done(len);
                Ok(true)
            }
            Err(StoreError::AlreadyExists { .. }) => Ok(self
                .get_log_record(name, id, generation, n)
                .await?
                .is_some_and(|now| now == body)),
            Err(e @ StoreError::Transient { .. }) => {
                match self.get_log_record(name, id, generation, n).await {
                    Ok(Some(now)) if now == body => Ok(true),
                    _ => Err(e.into()),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn delete_log_record(
        &self,
        name: &str,
        id: BranchId,
        generation: u64,
        n: u64,
    ) -> Result<()> {
        Ok(self
            .store
            .delete(&keys::log_record(name, id, generation, n))
            .await?)
    }

    /// The generation, number and size of every record in the write log of life `id` of branch
    /// `name`.
    pub(crate) async fn list_log(&self, name: &str, id: BranchId) -> Result<Vec<(u64, u64, u64)>> {
        let dir = keys::log_branch(name, id);
        let prefix = format!("{dir}/");
        Ok(self
            .store
            .list(&dir)
            .await?
            .into_iter()
            .filter_map(|o| {
                let (g, n) = keys::parse_log_record(o.key.strip_prefix(&prefix)?)?;
                Some((g, n, o.size))
            })
            .collect())
    }

    /// Every write log record in the store: the key and its upload time, with the hash of its
    /// branch's name and its branch's life.
    pub(crate) async fn list_all_logs(&self) -> Result<Vec<(String, Hash, BranchId, SystemTime)>> {
        let prefix = format!("{}/", keys::LOG);
        Ok(self
            .store
            .list(keys::LOG)
            .await?
            .into_iter()
            .filter_map(|o| {
                let (branch, id, _, _) = keys::parse_log_key(o.key.strip_prefix(&prefix)?)?;
                Some((o.key.clone(), branch, id, o.modified))
            })
            .collect())
    }

    pub(crate) async fn delete_key(&self, key: &str) -> Result<()> {
        Ok(self.store.delete(key).await?)
    }

    pub(crate) async fn put_change_list(&self, root: Hash, from: Hash, b: Bytes) -> Result<()> {
        let n = b.len();
        match self.store.create(&keys::change_list(root, from), b).await {
            Ok(_) => self.put_done(n),
            Err(StoreError::AlreadyExists { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    pub(crate) async fn get_change_list(&self, root: Hash, from: Hash) -> Result<Option<Bytes>> {
        match self.store.get(&keys::change_list(root, from)).await {
            Ok(o) => {
                self.got(o.body.len());
                Ok(Some(o.body))
            }
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// The recording of mounts at `root`; `None` when there is none or it does not decode.
    pub(crate) async fn get_recording(&self, root: Hash) -> Result<Option<Recording>> {
        match self.store.get(&keys::recording(root)).await {
            Ok(o) => {
                self.got(o.body.len());
                Ok(Recording::decode(&o.body).ok())
            }
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn put_recording(&self, root: Hash, r: &Recording) -> Result<()> {
        let b = Bytes::from(r.encode());
        let n = b.len();
        self.store.put(&keys::recording(root), b).await?;
        self.put_done(n);
        Ok(())
    }

    /// Every stored segment. A segment whose data is missing shows its index.
    pub(crate) async fn list_segments(&self) -> Result<BTreeMap<SegId, SegmentInfo>> {
        let prefix = format!("{}/", keys::SEGMENTS);
        let mut out = BTreeMap::new();
        for o in self.store.list(keys::SEGMENTS).await? {
            let Some(name) = o.key.strip_prefix(&prefix) else {
                continue;
            };
            let Some(id) = keys::parse_segment(name) else {
                continue;
            };
            let info = SegmentInfo {
                size: o.size,
                modified: o.modified,
            };
            if std::path::Path::new(name)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("idx"))
            {
                out.entry(id).or_insert(info);
            } else {
                out.insert(id, info);
            }
        }
        Ok(out)
    }

    /// Deletes a segment: its index first, so a half-deleted segment has no index to read.
    pub(crate) async fn delete_segment(&self, id: SegId) -> Result<()> {
        self.store.delete(&keys::segment_index(id)).await?;
        Ok(self.store.delete(&keys::segment(id)).await?)
    }

    pub(crate) async fn list_change_lists(&self) -> Result<Vec<ChangeListInfo>> {
        let prefix = format!("{}/", keys::CHANGES);
        Ok(self
            .store
            .list(keys::CHANGES)
            .await?
            .into_iter()
            .filter_map(|o| {
                let (root, from) = keys::parse_change_list(o.key.strip_prefix(&prefix)?)?;
                Some(ChangeListInfo {
                    root,
                    from,
                    modified: o.modified,
                })
            })
            .collect())
    }

    pub(crate) async fn delete_change_list(&self, root: Hash, from: Hash) -> Result<()> {
        Ok(self.store.delete(&keys::change_list(root, from)).await?)
    }
}

/// A branch id drawn from `rng`, never [`BranchId::NONE`].
pub(crate) fn new_branch_id(rng: &dyn Rng) -> BranchId {
    loop {
        let id = BranchId::from_parts(rng.next_u64(), rng.next_u64());
        if id != BranchId::NONE {
            return id;
        }
    }
}
