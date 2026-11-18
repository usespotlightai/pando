//! The cache: this host's objects, and the remote frames that hold the rest.
//!
//! - [`fetch`]: a read's own fetch of the frame it needs, and the decoding all fetches share.
//! - [`sched`]: background fetches of many entries through one window of bytes in flight.
//! - [`held`]: recipe chunks whose sources are not here yet, kept encoded until asked for.
//! - [`kept`]: compressed frames decoded in part, kept for the next fetch of them.

mod fetch;
mod held;
mod kept;
mod partial;
mod recorder;
mod sched;
#[cfg(test)]
#[cfg(feature = "sim")]
mod tests;

use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::future::BoxFuture;
use pando_format::{Entry, Hash, SegId, SegmentIndex};
pub(crate) use partial::{BaseKey, PartialBase};
pub(crate) use sched::{FetchOpts, Sink};

use crate::flight::Flights;
use crate::local::{ObjectLog, WritebackHold};
use crate::ports::{Cpu, CpuClass, LocalFs};
use crate::remote::Remote;
use crate::{Error, Result};

/// Counts of what a [`Cache`] served and fetched since it was opened.
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Reads and gets served from this host without a fetch.
    pub hits: AtomicU64,
    /// Range GETs of frames.
    pub runs: AtomicU64,
    /// Frames fetched from the store and decoded.
    pub frames_fetched: AtomicU64,
    /// The compressed bytes fetched from the store.
    pub bytes_fetched: AtomicU64,
    /// Foreground reads that waited for the store.
    pub stalls: AtomicU64,
    /// How long those reads waited in all, in nanoseconds.
    pub stall_nanos: AtomicU64,
    /// GETs served from a kept frame.
    pub kept_hits: AtomicU64,
    /// Recipe chunks rebuilt from a held encoding.
    pub held_rebuilt: AtomicU64,
    /// Objects read from a base image being built or seeded from.
    pub base_hits: AtomicU64,
}

/// A frame: its segment and its number there.
pub(crate) type FrameKey = (SegId, u32);

/// The local tier, keyed by content hash, in front of the store. It holds every object this
/// host wrote and has not uploaded yet, so it is also the write-back log.
#[derive(Debug)]
pub struct Cache {
    remote: Remote,
    cpu: Arc<dyn Cpu>,
    log: ObjectLog,
    indexes: Mutex<HashMap<SegId, Arc<SegmentIndex>>>,
    index_flights: Flights<SegId>,
    frame_flights: Flights<FrameKey>,
    tails: Mutex<HashMap<SegId, Arc<fetch::Tail>>>,
    held: held::Held,
    kept: Arc<kept::Kept>,
    window: sched::Window,
    bases: RwLock<HashMap<BaseKey, Arc<PartialBase>>>,
    recorder: Mutex<recorder::Recorder>,
    stats: CacheStats,
}

fn shared(e: Error) -> Arc<Error> {
    match e {
        Error::Shared(e) => e,
        e @ (Error::Fenced { .. }
        | Error::ChunkLost { .. }
        | Error::Store(_)
        | Error::Format(_)
        | Error::UnknownFormat { .. }
        | Error::Io(_)
        | Error::BeyondEnd
        | Error::Shrink { .. }
        | Error::Corrupt(_)
        | Error::SegmentExists(_)
        | Error::RefExists(_)
        | Error::CheckpointExists(_)
        | Error::Cancelled) => Arc::new(e),
    }
}

fn class(background: bool) -> CpuClass {
    if background {
        CpuClass::Background
    } else {
        CpuClass::Foreground
    }
}

fn not_in_frame(e: &Entry) -> Error {
    Error::Corrupt(format!(
        "object {} is not in frame {} of {}",
        e.hash, e.loc.frame, e.loc.seg
    ))
}

impl Cache {
    /// Opens the cache of this host in `dir`, in front of the store of `ports`.
    pub async fn open(ports: &crate::Ports, dir: &Path) -> Result<Cache> {
        Cache::open_with(
            Arc::clone(&ports.fs),
            Arc::clone(&ports.cpu),
            Remote::new(Arc::clone(&ports.store)),
            dir,
        )
        .await
    }

    pub(crate) async fn open_with(
        fs: Arc<dyn LocalFs>,
        cpu: Arc<dyn Cpu>,
        remote: Remote,
        dir: &Path,
    ) -> Result<Cache> {
        let log = ObjectLog::open(fs, &dir.join("objects")).await?;
        Ok(Cache {
            remote,
            cpu,
            log,
            indexes: Mutex::default(),
            index_flights: Flights::default(),
            frame_flights: Flights::default(),
            tails: Mutex::default(),
            held: held::Held::default(),
            kept: Arc::default(),
            window: sched::Window::default(),
            bases: RwLock::default(),
            recorder: Mutex::default(),
            stats: CacheStats::default(),
        })
    }

    /// The counters of this cache.
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    pub(crate) fn remote(&self) -> &Remote {
        &self.remote
    }

    /// A whole object from this host, or `None`.
    pub(crate) async fn local(&self, h: &Hash) -> Result<Option<Bytes>> {
        match self.log.get(h).await {
            Ok(Some(b)) => return Ok(Some(b)),
            Err(e) if !matches!(e.kind(), Error::ChunkLost { .. }) => return Err(e),
            _ => {}
        }
        Ok(self.base_object(h).await)
    }

    /// Whether [`Self::local`] would return `h`.
    pub(crate) async fn has(&self, h: &Hash) -> bool {
        self.log.has_own(h)
            || self.base_has(h)
            || (self.log.has(h) && matches!(self.local(h).await, Ok(Some(_))))
    }

    /// Writes an object to this host. It is durable after the next [`Self::sync_local`].
    pub(crate) async fn put_local(&self, h: Hash, b: Bytes) -> Result<()> {
        self.log.put(h, b).await
    }

    /// Records that object `h` is the `len` bytes at `offset` of the file at `path`.
    pub(crate) async fn put_ref(&self, h: Hash, path: &Path, offset: u64, len: u32) -> Result<()> {
        self.log.put_ref(h, path, offset, len).await
    }

    /// Turns every object recorded in `path` into a miss; call it before removing the file.
    pub(crate) fn drop_file(&self, path: &Path) {
        self.log.drop_file(path);
    }

    pub(crate) fn hold_writeback(&self) -> WritebackHold<'_> {
        self.log.hold_writeback()
    }

    /// Makes every [`Self::put_local`] so far durable.
    pub(crate) async fn sync_local(&self) -> Result<()> {
        self.log.sync().await
    }

    /// Reads bytes of `e` only from the page cache; `false` when that is not enough.
    pub(crate) fn read_nowait(&self, e: &Entry, inner: usize, dst: &mut [u8]) -> Result<bool> {
        let hit = self.log.read_nowait(&e.hash, inner, dst)? || self.base_read_nowait(&e.hash, inner, dst);
        if hit {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            self.touched(e);
        }
        Ok(hit)
    }

    /// The whole object `e`, fetching its frame when this host does not have it.
    pub(crate) async fn get(&self, e: &Entry) -> Result<Bytes> {
        let b = self.get_as(e, false).await?;
        self.touched(e);
        Ok(b)
    }

    /// [`Self::get`] for background work: it neither counts as a stall nor takes over fetches.
    pub(crate) async fn get_background(&self, e: &Entry) -> Result<Bytes> {
        self.get_as(e, true).await
    }

    fn get_boxed<'a>(&'a self, e: &'a Entry, background: bool) -> BoxFuture<'a, Result<Bytes>> {
        Box::pin(self.get_as(e, background))
    }

    async fn get_as(&self, e: &Entry, background: bool) -> Result<Bytes> {
        if let Some(b) = self.local(&e.hash).await? {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(b);
        }
        if let Some(b) = self.rebuild_if_held(e, background).await? {
            return Ok(b);
        }
        // A miss that waited for a background fetch may find the object left out (it decodes
        // only what it was asked for); the second miss fetches the frame itself.
        for _ in 0..2 {
            self.miss(e, background).await?;
            if let Some(b) = self.local(&e.hash).await? {
                return Ok(b);
            }
            if let Some(b) = self.rebuild_if_held(e, background).await? {
                return Ok(b);
            }
        }
        Err(not_in_frame(e))
    }

    /// Bytes `[inner, inner + len)` of object `e`, fetching its frame when needed.
    pub(crate) async fn read(&self, e: &Entry, inner: usize, len: usize) -> Result<Bytes> {
        let b = self.read_as(e, inner, len).await?;
        self.touched(e);
        Ok(b)
    }

    /// Fetches object `e` to this host when it is not here, so a later [`Self::read`] of it
    /// needs no store. Counts no hit.
    pub(crate) async fn warm(&self, e: &Entry) -> Result<()> {
        self.read_counted(e, 0, 1, false).await.map(drop)
    }

    async fn read_as(&self, e: &Entry, inner: usize, len: usize) -> Result<Bytes> {
        self.read_counted(e, inner, len, true).await
    }

    async fn read_counted(&self, e: &Entry, inner: usize, len: usize, count: bool) -> Result<Bytes> {
        if let Some(b) = self.log.read_at(&e.hash, inner, len).await? {
            if count {
                self.stats.hits.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(b);
        }
        if let Some(b) = self.base_read(&e.hash, inner, len).await {
            return Ok(b);
        }
        if let Some(b) = self.rebuild_if_held(e, false).await? {
            return Ok(b.slice(inner..inner + len));
        }
        for _ in 0..2 {
            self.miss(e, false).await?;
            if let Some(b) = self.log.read_at(&e.hash, inner, len).await? {
                return Ok(b);
            }
            if let Some(b) = self.rebuild_if_held(e, false).await? {
                return Ok(b.slice(inner..inner + len));
            }
        }
        Err(not_in_frame(e))
    }
}
