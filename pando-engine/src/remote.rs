//! The store as Pando sees it: segments, refs, change lists and hints over [`Store`].

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use pando_format::{Hash, Ref, SegId, SegmentIndex, keys};

use crate::ports::{Store, StoreError, Version};
use crate::{Error, Result};

#[derive(Debug, Default)]
pub struct RemoteStats {
    pub gets: AtomicU64,
    pub get_bytes: AtomicU64,
    pub puts: AtomicU64,
    pub put_bytes: AtomicU64,
    pub ref_puts: AtomicU64,
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
        self.stats.get_bytes.fetch_add(n as u64, Ordering::Relaxed);
    }

    fn put_done(&self, n: usize) {
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        self.stats.put_bytes.fetch_add(n as u64, Ordering::Relaxed);
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

    /// Creates a ref; [`Error::RefExists`] when there is one.
    pub(crate) async fn create_ref(&self, name: &str, r: &Ref) -> Result<Version> {
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
}
