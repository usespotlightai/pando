//! Bases the cache reads chunks from by hash: a base image while it is built, and a finished
//! one a new base is seeded from. A chunk is ready only once its bytes are in the image, and
//! they were checked against its hash before they were written. Readers look chunks up by
//! hash, so a chunk that a branch changed since the base root never matches.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use pando_format::{CHUNK_SIZE, Hash, u64_from, usize_from};

use super::Cache;
use crate::ports::LocalFile;

#[derive(Debug)]
struct Place {
    off: u64,
    /// The chunk's length plus one once it is in the image; zero before.
    ready: AtomicU64,
}

/// The chunks of one base image, by hash, at their first place in it.
#[derive(Debug)]
pub(crate) struct PartialBase {
    file: Arc<dyn LocalFile>,
    chunks: HashMap<Hash, Place>,
}

impl PartialBase {
    /// A base over `file` that will hold the chunks of `places` (hash, chunk index).
    pub(crate) fn new(
        file: Arc<dyn LocalFile>,
        places: impl IntoIterator<Item = (Hash, u64)>,
    ) -> PartialBase {
        let chunks = places
            .into_iter()
            .map(|(h, idx)| {
                (
                    h,
                    Place {
                        off: idx * u64_from(CHUNK_SIZE),
                        ready: AtomicU64::new(0),
                    },
                )
            })
            .collect();
        PartialBase { file, chunks }
    }

    /// Marks chunk `h` ready once its `n` bytes are at its first place.
    pub(crate) fn written(&self, h: &Hash, n: usize) {
        if let Some(p) = self.chunks.get(h) {
            p.ready.store(u64_from(n) + 1, Ordering::Release);
        }
    }

    /// Whether chunk `h` is first written at chunk `idx`.
    pub(crate) fn first_at(&self, h: &Hash, idx: u64) -> bool {
        self.chunks
            .get(h)
            .is_some_and(|p| p.off == idx * u64_from(CHUNK_SIZE))
    }

    fn ready(&self, h: &Hash) -> Option<(u64, usize)> {
        let p = self.chunks.get(h)?;
        let n = p.ready.load(Ordering::Acquire);
        (n > 0).then(|| (p.off, usize_from(n - 1)))
    }

    pub(crate) fn has(&self, h: &Hash) -> bool {
        self.ready(h).is_some()
    }

    async fn read(&self, h: &Hash, inner: usize, len: usize) -> Option<Bytes> {
        let (off, n) = self.ready(h)?;
        if inner + len > n {
            return None;
        }
        let b = self.file.read_at(len, off + u64_from(inner)).await.ok()?;
        (b.len() == len).then_some(b)
    }

    async fn object(&self, h: &Hash) -> Option<Bytes> {
        let (_, n) = self.ready(h)?;
        self.read(h, 0, n).await
    }

    fn read_nowait(&self, h: &Hash, inner: usize, dst: &mut [u8]) -> bool {
        let Some((off, n)) = self.ready(h) else {
            return false;
        };
        inner + dst.len() <= n
            && matches!(self.file.read_at_nowait(dst, off + u64_from(inner)), Ok(Some(k)) if k == dst.len())
    }
}

/// Names a base the cache reads from: the root it is the image of, and whether it is a seed.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum BaseKey {
    Building(Hash),
    Seed(Hash),
}

impl Cache {
    /// Lets reads take the chunks of `base` as they become ready.
    pub(crate) fn serve_base(&self, key: BaseKey, base: Arc<PartialBase>) {
        self.bases.write().insert(key, base);
    }

    /// Ends reads from base `key`. A read that already holds it finishes on the file it has.
    pub(crate) fn stop_serving_base(&self, key: BaseKey) {
        self.bases.write().remove(&key);
    }

    pub(crate) fn serves_base(&self, key: BaseKey) -> bool {
        self.bases.read().contains_key(&key)
    }

    fn served(&self) -> Vec<Arc<PartialBase>> {
        self.bases.read().values().cloned().collect()
    }

    pub(super) fn base_has(&self, h: &Hash) -> bool {
        self.bases.read().values().any(|b| b.has(h))
    }

    pub(super) async fn base_object(&self, h: &Hash) -> Option<Bytes> {
        for b in self.served() {
            if let Some(o) = b.object(h).await {
                self.stats.base_hits.fetch_add(1, Ordering::Relaxed);
                return Some(o);
            }
        }
        None
    }

    pub(super) async fn base_read(&self, h: &Hash, inner: usize, len: usize) -> Option<Bytes> {
        for b in self.served() {
            if let Some(o) = b.read(h, inner, len).await {
                self.stats.base_hits.fetch_add(1, Ordering::Relaxed);
                return Some(o);
            }
        }
        None
    }

    pub(super) fn base_read_nowait(&self, h: &Hash, inner: usize, dst: &mut [u8]) -> bool {
        let hit = self.bases.read().values().any(|b| b.read_nowait(h, inner, dst));
        if hit {
            self.stats.base_hits.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }
}
