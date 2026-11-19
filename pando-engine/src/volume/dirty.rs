//! The chunks written since the last commit. Their bytes stay in the journal: memory keeps
//! what lay under each chunk's first write and where the journal keeps each write since.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use pando_format::{CHUNK_SIZE, Entry, u64_from};

use super::{CHUNK, ZEROS};
use crate::Result;
use crate::base::BaseView;
use crate::cache::Cache;
use crate::local::Extent;

/// Writes a chunk keeps before a write folds them into one record of the whole chunk.
pub(super) const WRITES_MAX: usize = 64;

/// What a dirty chunk held before its first write since the last commit.
#[derive(Clone, Debug)]
pub(super) enum Under {
    Zero,
    Bytes(Bytes),
    /// The committed chunk.
    Stored(Entry),
    /// The base image's bytes, and zeros past its end.
    Base(Arc<BaseView>),
}

/// A chunk written since the last commit: what lay under it, then its writes in order.
#[derive(Clone, Debug)]
pub(super) struct Dirty {
    under: Under,
    writes: Vec<Extent>,
}

impl Dirty {
    pub(super) fn new(under: Under) -> Dirty {
        Dirty {
            under,
            writes: Vec::new(),
        }
    }

    pub(super) fn is_zero(&self) -> bool {
        matches!(self.under, Under::Zero) && self.writes.is_empty()
    }

    pub(super) fn writes(&self) -> usize {
        self.writes.len()
    }

    /// The committed chunk a read may need from the cache.
    pub(super) fn stored(&self) -> Option<&Entry> {
        match &self.under {
            Under::Stored(e) if !e.is_empty() => Some(e),
            Under::Zero | Under::Bytes(_) | Under::Stored(_) | Under::Base(_) => None,
        }
    }

    /// The bytes this chunk holds in memory and in the journal.
    fn held(&self) -> usize {
        let under = match &self.under {
            Under::Bytes(b) => b.len(),
            Under::Zero | Under::Stored(_) | Under::Base(_) => 0,
        };
        under + self.writes.iter().map(Extent::logged).sum::<usize>()
    }

    /// Adds a write after the others, dropping each earlier one it covers.
    fn push(&mut self, w: Extent) {
        self.writes.retain(|o| o.at() < w.at() || o.end() > w.end());
        if w.at() == 0 && w.end() == CHUNK_SIZE {
            self.under = Under::Zero;
        }
        self.writes.push(w);
    }

    /// The part of this chunk that a read of `[at, at + n)` needs: nothing under the last
    /// write that covers the range, and only the writes that touch it.
    pub(super) fn part(&self, at: usize, n: usize) -> Dirty {
        let end = at + n;
        let covered = self.writes.iter().rposition(|w| w.at() <= at && w.end() >= end);
        Dirty {
            under: if covered.is_some() {
                Under::Zero
            } else {
                self.under.clone()
            },
            writes: self.writes[covered.unwrap_or(0)..]
                .iter()
                .filter(|w| w.at() < end && w.end() > at)
                .cloned()
                .collect(),
        }
    }

    /// Bytes `[at, at + n)` of chunk `idx`.
    pub(super) async fn read(&self, cache: &Cache, idx: u64, at: usize, n: usize) -> Result<Bytes> {
        if let Some(b) = self.covered(at, n).await? {
            return Ok(b);
        }
        let under = self.under.read(cache, idx, at, n).await?;
        self.overlay(under, at).await
    }

    /// [`Self::read`] from this host only: `None` when it needs the store.
    pub(super) async fn read_held(
        &self,
        cache: &Cache,
        idx: u64,
        at: usize,
        n: usize,
    ) -> Result<Option<Bytes>> {
        if let Some(b) = self.covered(at, n).await? {
            return Ok(Some(b));
        }
        let Some(under) = self.under.read_held(cache, idx, at, n).await? else {
            return Ok(None);
        };
        self.overlay(under, at).await.map(Some)
    }

    /// [`Self::read`] into `dst` from memory and the page cache only: `false` when it would
    /// wait.
    pub(super) fn read_nowait(&self, cache: &Cache, idx: u64, at: usize, dst: &mut [u8]) -> Result<bool> {
        if !self.under.read_nowait(cache, idx, at, dst)? {
            return Ok(false);
        }
        let end = at + dst.len();
        for w in &self.writes {
            let (from, to) = (w.at().max(at), w.end().min(end));
            if from < to && !w.read_nowait(from, &mut dst[from - at..to - at])? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// The bytes of the one write that covers `[at, at + n)` when it is the only write left.
    async fn covered(&self, at: usize, n: usize) -> Result<Option<Bytes>> {
        match self.writes.as_slice() {
            [w] if w.at() <= at && w.end() >= at + n => w.read(at, n).await.map(Some),
            _ => Ok(None),
        }
    }

    async fn overlay(&self, under: Bytes, at: usize) -> Result<Bytes> {
        if self.writes.is_empty() {
            return Ok(under);
        }
        let end = at + under.len();
        let mut buf = BytesMut::from(under.as_ref());
        for w in &self.writes {
            let (from, to) = (w.at().max(at), w.end().min(end));
            if from < to {
                buf[from - at..to - at].copy_from_slice(&w.read(from, to - from).await?);
            }
        }
        Ok(buf.freeze())
    }
}

impl Under {
    async fn read(&self, cache: &Cache, idx: u64, at: usize, n: usize) -> Result<Bytes> {
        match self {
            Under::Zero => Ok(Bytes::from_static(&ZEROS[..n])),
            Under::Bytes(b) => Ok(b.slice(at..at + n)),
            Under::Stored(e) => read_stored(cache, e, at, n).await,
            Under::Base(v) => Ok(v.read_padded(idx * CHUNK + u64_from(at), n).await?),
        }
    }

    async fn read_held(&self, cache: &Cache, idx: u64, at: usize, n: usize) -> Result<Option<Bytes>> {
        let e = match self {
            Under::Stored(e) => e,
            Under::Zero | Under::Bytes(_) | Under::Base(_) => {
                return self.read(cache, idx, at, n).await.map(Some);
            }
        };
        if e.is_empty() {
            return Ok(Some(Bytes::from_static(&ZEROS[..n])));
        }
        cache.read_held(e, at, n).await
    }

    fn read_nowait(&self, cache: &Cache, idx: u64, at: usize, dst: &mut [u8]) -> Result<bool> {
        let e = match self {
            Under::Zero => {
                dst.fill(0);
                return Ok(true);
            }
            Under::Bytes(b) => {
                dst.copy_from_slice(&b[at..at + dst.len()]);
                return Ok(true);
            }
            Under::Base(v) => return Ok(v.read_padded_nowait(idx * CHUNK + u64_from(at), dst)),
            Under::Stored(e) => e,
        };
        if e.is_empty() {
            dst.fill(0);
            return Ok(true);
        }
        cache.read_nowait(e, at, dst)
    }
}

async fn read_stored(cache: &Cache, e: &Entry, at: usize, n: usize) -> Result<Bytes> {
    if e.is_empty() {
        return Ok(Bytes::from_static(&ZEROS[..n]));
    }
    cache.read(e, at, n).await
}

/// The chunks written since the last commit, with the journal bytes their writes took since
/// and the bytes of the buffers they hold.
#[derive(Debug, Default)]
pub(super) struct DirtySet {
    chunks: BTreeMap<u64, Dirty>,
    pub(super) bytes: usize,
}

impl DirtySet {
    pub(super) fn get(&self, idx: u64) -> Option<&Dirty> {
        self.chunks.get(&idx)
    }

    pub(super) fn contains_key(&self, idx: u64) -> bool {
        self.chunks.contains_key(&idx)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &u64> {
        self.chunks.keys()
    }

    /// The committed chunks the dirty chunks were written over.
    pub(super) fn stored(&self) -> impl Iterator<Item = &Entry> {
        self.chunks.values().filter_map(Dirty::stored)
    }

    /// Makes chunk `idx` hold `d`, counting the bytes `d` holds.
    pub(super) fn insert(&mut self, idx: u64, d: Dirty) {
        self.bytes += d.held();
        self.chunks.insert(idx, d);
    }

    /// Adds write `w` to chunk `idx`, which starts from `from` when it is not dirty yet.
    /// Returns how many writes the chunk keeps; `None` when it is not dirty and no `from` is
    /// given.
    pub(super) fn push(&mut self, idx: u64, from: Option<Dirty>, w: Extent) -> Option<usize> {
        self.bytes += w.logged();
        let d = match (self.chunks.get_mut(&idx), from) {
            (Some(d), _) => d,
            (None, Some(f)) => self.chunks.entry(idx).or_insert(f),
            (None, None) => return None,
        };
        d.push(w);
        Some(d.writes.len())
    }

    pub(super) fn remove(&mut self, idx: u64) -> Option<Dirty> {
        self.chunks.remove(&idx)
    }

    pub(super) fn take(&mut self) -> BTreeMap<u64, Dirty> {
        self.bytes = 0;
        std::mem::take(&mut self.chunks)
    }
}
