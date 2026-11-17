//! What a handoff needs from a volume: the chunks that differ from a root, copies of the
//! branch into an image, and the chunks the kernel wrote into an image, taken back as writes.

use std::collections::{BTreeSet, HashSet};
use std::path::Path;
use std::sync::Arc;

use pando_format::{CHUNK_SIZE, Entry, Hash, is_zero, usize_from};
use tokio::sync::watch;

use super::{CHUNK, ChunkRef, Volume};
use crate::Result;
use crate::dedup::PreparedChunks;
use crate::flight::bounded;
use crate::ports::{CpuClass, LocalFile, LocalImage, on_cpu};
use crate::tree::Tree;

/// Chunks a copy or an absorb reads at once.
pub(crate) const ABSORB_PARALLEL: usize = 16;

impl Volume {
    pub(crate) fn generation(&self) -> u64 {
        self.inner.generation
    }

    /// Lets an absorb prepare the uploads of the chunks it takes.
    pub(crate) fn set_prepared(&self, p: Option<Arc<PreparedChunks>>) {
        *self.inner.prepared.lock() = p;
    }

    /// Records the index of every chunk written from now on.
    pub(crate) fn start_tracking(&self) {
        self.inner.mem.lock().track = Some(HashSet::new());
    }

    /// The chunks written since tracking started or since the last call.
    pub(crate) fn take_tracked(&self) -> Vec<u64> {
        let mut m = self.inner.mem.lock();
        m.track
            .as_mut()
            .map_or_default(|t| std::mem::take(t).into_iter().collect())
    }

    /// Tracks `idxs` again, as if just written.
    pub(crate) fn keep_tracked(&self, idxs: &[u64]) {
        if let Some(t) = &mut self.inner.mem.lock().track {
            t.extend(idxs);
        }
    }

    pub(crate) fn stop_tracking(&self) {
        self.inner.mem.lock().track = None;
    }

    /// The chunks written but not committed, the current root, the root the branch opened
    /// at as a root of the same tree, and that tree.
    fn uncommitted(&self) -> (BTreeSet<u64>, Entry, Entry, Arc<Tree>) {
        let m = self.inner.mem.lock();
        let mut out: BTreeSet<u64> = m.dirty.keys().copied().collect();
        if let Some(f) = &m.flushing {
            out.extend(f.keys());
        }
        (out, m.root, m.opened_now, Arc::clone(&m.tree))
    }

    /// The chunks that differ now from the root the branch opened at: committed changes and
    /// writes not committed yet, in index order.
    pub(crate) async fn changed_since_opened(&self) -> Result<Vec<u64>> {
        let (mut out, root, opened, tree) = self.uncommitted();
        let changed = tree.changed_leaves(&opened, &root, ABSORB_PARALLEL).await?;
        out.extend(changed.into_iter().map(|(idx, _)| idx));
        Ok(out.into_iter().collect())
    }

    /// The chunks that hold data now, in index order.
    pub(crate) async fn used_chunks(&self) -> Result<Vec<u64>> {
        let (mut out, root, _, tree) = self.uncommitted();
        tree.leaves(&root, |idx, _| {
            out.insert(idx);
        })
        .await?;
        Ok(out.into_iter().collect())
    }

    /// Writes the current bytes of chunks `idxs` into `f`. Zero chunks are skipped unless
    /// `overwrite` is set, so a fresh image stays sparse. Returns how many it wrote.
    pub(crate) async fn copy_chunks(
        &self,
        f: &dyn LocalFile,
        idxs: &[u64],
        overwrite: bool,
    ) -> Result<usize> {
        let size = self.size();
        let mut futs = Vec::with_capacity(idxs.len());
        for &idx in idxs.iter().filter(|i| **i * CHUNK < size) {
            futs.push(async move {
                let off = idx * CHUNK;
                let b = self
                    .inner
                    .read_chunk(idx, 0, usize_from(CHUNK.min(size - off)))
                    .await?;
                if !overwrite && is_zero(&b) {
                    return Ok(false);
                }
                f.write_at(vec![b], off).await?;
                Ok(true)
            });
        }
        let done = bounded(futs, ABSORB_PARALLEL)
            .await
            .into_iter()
            .collect::<Result<Vec<bool>>>()?;
        Ok(done.into_iter().filter(|w| *w).count())
    }

    /// Reads chunks `idxs` from the image `f` at `path` and takes each one that differs from
    /// the root as a write. The file must not change until those writes are published: a
    /// flush records them as references into it. Returns how many it took.
    pub(crate) async fn absorb_chunks(
        &self,
        f: &Arc<dyn LocalFile>,
        path: &Path,
        idxs: &[u64],
    ) -> Result<usize> {
        let (size, tree, root) = {
            let m = self.inner.mem.lock();
            (m.size, Arc::clone(&m.tree), m.root)
        };
        let tree = &tree;
        let mut futs = Vec::with_capacity(idxs.len());
        for &idx in idxs.iter().filter(|i| **i * CHUNK < size) {
            futs.push(async move {
                let off = idx * CHUNK;
                let n = usize_from(CHUNK.min(size - off));
                let got = f.read_at(n, off).await?;
                let full = got.len() == CHUNK_SIZE;
                let mut buf = got.to_vec();
                buf.resize(CHUNK_SIZE, 0);
                let e = tree.lookup(&root, idx, true).await?;
                let (buf, h) = on_cpu(self.inner.cpu.as_ref(), CpuClass::Background, move || {
                    let h = (!is_zero(&buf)).then(|| Hash::of(&buf));
                    (buf, h)
                })
                .await;
                let r = match h {
                    None if e.is_empty() => return Ok(false),
                    None => None,
                    Some(h) if h == e.hash => return Ok(false),
                    Some(h) => {
                        if full {
                            self.prepare(h, &buf).await;
                        }
                        full.then(|| ChunkRef {
                            path: path.to_owned(),
                            offset: off,
                            hash: h,
                        })
                    }
                };
                self.take_write(idx, buf, r).await;
                Ok(true)
            });
        }
        let done = bounded(futs, ABSORB_PARALLEL)
            .await
            .into_iter()
            .collect::<Result<Vec<bool>>>()?;
        Ok(done.into_iter().filter(|w| *w).count())
    }

    /// [`Self::absorb_chunks`] over every chunk the root holds or the image has data in.
    pub(crate) async fn absorb_image(
        &self,
        image: &dyn LocalImage,
        f: &Arc<dyn LocalFile>,
        path: &Path,
    ) -> Result<usize> {
        let (_, root, _, tree) = self.uncommitted();
        let mut cand = BTreeSet::new();
        tree.leaves(&root, |idx, _| {
            cand.insert(idx);
        })
        .await?;
        for x in image.extents(path).await? {
            if x.len > 0 {
                cand.extend(x.logical / CHUNK..=(x.logical + x.len - 1) / CHUNK);
            }
        }
        let idxs: Vec<u64> = cand.into_iter().collect();
        self.absorb_chunks(f, path, &idxs).await
    }

    async fn prepare(&self, h: Hash, b: &[u8]) {
        let Some(p) = self.inner.prepared.lock().clone() else {
            return;
        };
        let dedup = self.dedup();
        let next = self.inner.mem.lock().flush_seq + 1;
        p.add(h, b, dedup, next).await;
    }

    async fn take_write(&self, idx: u64, buf: Vec<u8>, r: Option<ChunkRef>) {
        let _g = self.inner.stripe(idx).write().await;
        let mut m = self.inner.mem.lock();
        m.dirty.insert(idx, buf);
        m.touched.insert(idx);
        if let Some(t) = &mut m.track {
            t.insert(idx);
        }
        match r {
            Some(r) => m.dirty_ref.insert(idx, r),
            None => m.dirty_ref.remove(&idx),
        };
    }

    /// Whether a flush recorded chunks as references into the file at `path` that no upload
    /// has published yet.
    pub(crate) fn referenced(&self, path: &Path) -> bool {
        let seq = self.inner.mem.lock().ref_seq.get(path).copied();
        seq.is_some_and(|s| s > *self.inner.uploaded_seq.borrow())
    }

    /// Marks every flush so far as one that references the files at `paths`.
    pub(crate) fn reference_until_published(&self, paths: &[impl AsRef<Path>]) {
        let mut m = self.inner.mem.lock();
        let seq = m.flush_seq;
        for p in paths {
            let e = m.ref_seq.entry(p.as_ref().to_owned()).or_default();
            *e = (*e).max(seq);
        }
    }

    /// Changes each time an upload publishes more flushes.
    pub(crate) fn uploads(&self) -> watch::Receiver<u64> {
        self.inner.uploaded_seq.subscribe()
    }
}
