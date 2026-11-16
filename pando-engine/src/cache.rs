//! The cache: this host's objects, and the remote frames that hold the rest.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::future::BoxFuture;
use pando_format::{Entry, Hash, Recipe, SegId, SegmentIndex, decode_frame};
use tokio::time::Instant;

use crate::flight::{Claim, Flights, Joined, bounded};
use crate::local::{ObjectLog, WritebackHold};
use crate::ports::{Cpu, CpuClass, LocalFs, on_cpu};
use crate::remote::Remote;
use crate::{Error, Result};

/// One range GET takes at most this many bytes; larger ranges get more throughput per request.
const RANGE_MAX: u32 = 16 << 20;
/// The range GETs one fetch of a segment's frames sends at once.
const RUNS_IN_FLIGHT: usize = 8;
/// How much of a segment's end [`Cache::fetch_nodes`] asks for with its index: an upload
/// writes its tree nodes after its chunks.
const NODE_TAIL: u32 = 1 << 20;

#[derive(Debug, Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    /// Range GETs of frames.
    pub runs: AtomicU64,
    pub frames_fetched: AtomicU64,
    pub bytes_fetched: AtomicU64,
    /// Foreground reads that waited for the store, and how long in all.
    pub stalls: AtomicU64,
    pub stall_nanos: AtomicU64,
}

type FrameKey = (SegId, u32);

#[derive(Debug)]
struct Tail {
    off: u32,
    bytes: Bytes,
}

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
    tails: Mutex<HashMap<SegId, Arc<Tail>>>,
    stats: CacheStats,
}

fn shared(e: Error) -> Arc<Error> {
    match e {
        Error::Shared(e) => e,
        e => Arc::new(e),
    }
}

fn class(background: bool) -> CpuClass {
    if background {
        CpuClass::Background
    } else {
        CpuClass::Foreground
    }
}

impl Cache {
    /// Opens the cache of this host in `dir`, in front of the store of `ports`.
    pub async fn open(ports: &crate::Ports, dir: &Path) -> Result<Cache> {
        Cache::open_with(
            ports.fs.clone(),
            ports.cpu.clone(),
            Remote::new(ports.store.clone()),
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
            stats: CacheStats::default(),
        })
    }

    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    pub(crate) fn remote(&self) -> &Remote {
        &self.remote
    }

    /// A whole object from this host, or `None`.
    pub(crate) async fn local(&self, h: &Hash) -> Result<Option<Bytes>> {
        match self.log.get(h).await {
            Err(e) if matches!(e.kind(), Error::ChunkLost { .. }) => Ok(None),
            r => r,
        }
    }

    /// Whether [`Self::local`] would return `h`.
    pub(crate) async fn has(&self, h: &Hash) -> bool {
        self.log.has_own(h) || (self.log.has(h) && matches!(self.local(h).await, Ok(Some(_))))
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
        let hit = self.log.read_nowait(&e.hash, inner, dst)?;
        if hit {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
        }
        Ok(hit)
    }

    /// The whole object `e`, fetching its frame when this host does not have it.
    pub(crate) async fn get(&self, e: &Entry) -> Result<Bytes> {
        self.get_as(e, false).await
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
        for _ in 0..2 {
            self.miss(e, background).await?;
            if let Some(b) = self.local(&e.hash).await? {
                return Ok(b);
            }
        }
        Err(Error::Corrupt(format!(
            "object {} is not in frame {} of {}",
            e.hash, e.loc.frame, e.loc.seg
        )))
    }

    /// Bytes `[inner, inner + len)` of object `e`, fetching its frame when needed.
    pub(crate) async fn read(&self, e: &Entry, inner: usize, len: usize) -> Result<Bytes> {
        if let Some(b) = self.log.read_at(&e.hash, inner, len).await? {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(b);
        }
        for _ in 0..2 {
            self.miss(e, false).await?;
            if let Some(b) = self.log.read_at(&e.hash, inner, len).await? {
                return Ok(b);
            }
        }
        Err(Error::Corrupt(format!(
            "object {} is not in frame {} of {}",
            e.hash, e.loc.frame, e.loc.seg
        )))
    }

    async fn miss(&self, e: &Entry, background: bool) -> Result<()> {
        if e.loc.is_pending() {
            return Err(Error::Corrupt(format!(
                "object {} is pending upload on another host",
                e.hash
            )));
        }
        let t0 = Instant::now();
        let r = self.fetch_frames(e.loc.seg, &[e.loc.frame], background).await;
        if !background {
            self.stats.stalls.fetch_add(1, Ordering::Relaxed);
            self.stats
                .stall_nanos
                .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        r.map(drop)
    }

    async fn seg_index(&self, seg: SegId) -> Result<Arc<SegmentIndex>> {
        for _ in 0..2 {
            if let Some(i) = self.indexes.lock().expect("indexes lock").get(&seg).cloned() {
                return Ok(i);
            }
            match self.index_flights.join(&seg, false) {
                Joined::Wait(c) => c.wait().await?,
                Joined::Lead(claim) => {
                    let r = self.remote.get_index(seg).await;
                    return match r {
                        Ok(i) => {
                            let i = Arc::new(i);
                            self.indexes.lock().expect("indexes lock").insert(seg, i.clone());
                            claim.finish(Ok(()))?;
                            Ok(i)
                        }
                        Err(e) => Err(claim.finish(Err(e)).expect_err("finished with an error")),
                    };
                }
            }
        }
        Err(Error::Corrupt(format!(
            "the index of segment {seg} did not arrive"
        )))
    }

    /// Fetches frames of one segment and returns how many this call fetched. Runs of adjacent
    /// frames go in one range GET of up to [`RANGE_MAX`] bytes. A frame another caller is
    /// fetching is waited for, not fetched again, unless this caller is a read and the other is
    /// background work: a background range can be many MiB, so the read fetches its frame alone.
    pub(crate) async fn fetch_frames(&self, seg: SegId, frames: &[u32], background: bool) -> Result<usize> {
        let idx = self.seg_index(seg).await?;
        let mut mine: BTreeMap<u32, Claim<'_, FrameKey>> = BTreeMap::new();
        let mut waits = Vec::new();
        for f in frames.iter().copied().collect::<BTreeSet<_>>() {
            if f as usize >= idx.frames.len() {
                return Err(Error::Corrupt(format!("frame {f} out of range in segment {seg}")));
            }
            match self.frame_flights.join(&(seg, f), background) {
                Joined::Lead(c) => {
                    mine.insert(f, c);
                }
                Joined::Wait(c) => waits.push(c),
            }
        }
        let mut runs: Vec<Vec<(u32, Claim<'_, FrameKey>)>> = Vec::new();
        for (f, c) in mine {
            let (off, len) = idx.frames[f as usize];
            match runs.last_mut() {
                Some(run)
                    if run.last().is_some_and(|(p, _)| *p + 1 == f)
                        && off + len - idx.frames[run[0].0 as usize].0 <= RANGE_MAX =>
                {
                    run.push((f, c));
                }
                _ => runs.push(vec![(f, c)]),
            }
        }
        let mut futs = Vec::with_capacity(runs.len());
        for run in runs {
            futs.push(self.fetch_run(seg, &idx, run, background));
        }
        let results = bounded(futs, RUNS_IN_FLIGHT).await;
        let mut n = 0;
        let mut err = None;
        for r in results {
            match r {
                Ok(k) => n += k,
                Err(e) => {
                    err.get_or_insert(e);
                }
            }
        }
        for w in waits {
            if let Err(e) = w.wait().await {
                err.get_or_insert(e);
            }
        }
        err.map_or(Ok(n), Err)
    }

    /// Fetches one run of claimed frames in one GET and releases each frame's waiters as soon
    /// as that frame is in. Frames a reader waits for are decoded first.
    async fn fetch_run(
        &self,
        seg: SegId,
        idx: &SegmentIndex,
        mut run: Vec<(u32, Claim<'_, FrameKey>)>,
        background: bool,
    ) -> Result<usize> {
        let off = idx.frames[run[0].0 as usize].0;
        let (last_off, last_len) = idx.frames[run[run.len() - 1].0 as usize];
        let z = match self.get_range(seg, off, last_off + last_len - off).await {
            Ok(z) => z,
            Err(e) => {
                let e = shared(e);
                for (_, c) in run {
                    let _ = c.finish(Err(Error::Shared(e.clone())));
                }
                return Err(Error::Shared(e));
            }
        };
        self.stats.runs.fetch_add(1, Ordering::Relaxed);
        let mut n = 0;
        while !run.is_empty() {
            let pick = run.iter().position(|(_, c)| c.call().awaited()).unwrap_or(0);
            let (f, claim) = run.remove(pick);
            let (fo, fl) = idx.frames[f as usize];
            let zf = z.slice((fo - off) as usize..(fo - off + fl) as usize);
            let r = claim.finish(self.put_frame(zf, background).await);
            if let Err(e) = r {
                let e = shared(e);
                for (_, c) in run {
                    let _ = c.finish(Err(Error::Shared(e.clone())));
                }
                return Err(Error::Shared(e));
            }
            self.stats.frames_fetched.fetch_add(1, Ordering::Relaxed);
            n += 1;
        }
        Ok(n)
    }

    /// Bytes `[off, off + n)` of a segment, from a tail [`Self::fetch_nodes`] holds when it
    /// covers them.
    async fn get_range(&self, seg: SegId, off: u32, n: u32) -> Result<Bytes> {
        let tail = self.tails.lock().expect("tails lock").get(&seg).cloned();
        if let Some(t) = tail
            && off >= t.off
            && u64::from(off) + u64::from(n) <= u64::from(t.off) + t.bytes.len() as u64
        {
            let at = (off - t.off) as usize;
            return Ok(t.bytes.slice(at..at + n as usize));
        }
        let b = self.remote.get_range(seg, off, n).await?;
        self.stats
            .bytes_fetched
            .fetch_add(b.len() as u64, Ordering::Relaxed);
        Ok(b)
    }

    /// Decodes one frame, stores its objects and rebuilds the recipe chunks this host lacks.
    async fn put_frame(&self, z: Bytes, background: bool) -> Result<()> {
        let d = on_cpu(self.cpu.as_ref(), class(background), move || {
            decode_frame(&z, None)
        })
        .await?;
        for (h, v) in d.objects {
            self.log.put(h, v.into_bytes()).await?;
        }
        for (h, enc) in d.recipes {
            if self.has(&h).await {
                continue;
            }
            let b = self.rebuild(h, enc, background).await?;
            self.log.put(h, b).await?;
        }
        Ok(())
    }

    async fn rebuild(&self, h: Hash, enc: Bytes, background: bool) -> Result<Bytes> {
        let mut sources = Recipe::sources_of(&enc);
        sources.sort_by_key(|s| s.hash);
        sources.dedup_by_key(|s| s.hash);
        let mut got = HashMap::with_capacity(sources.len());
        for s in &sources {
            got.insert(s.hash, self.get_boxed(s, background).await?);
        }
        on_cpu(self.cpu.as_ref(), class(background), move || {
            let r = Recipe::parse(&enc)
                .map_err(|why| Error::Corrupt(format!("recipe for {h} is malformed: {why}")))?;
            r.rebuild(h, |e| got.get(&e.hash).cloned().ok_or("source missing"))
                .map(pando_format::Verified::into_bytes)
                .map_err(|e| Error::Corrupt(e.to_string()))
        })
        .await
    }

    /// Fetches the frames of `es` that this host lacks, `parallel` segments at a time, and
    /// returns how many frames this call fetched.
    pub(crate) async fn fetch_entries(
        &self,
        es: &[Entry],
        parallel: usize,
        background: bool,
    ) -> Result<usize> {
        let mut by_seg: BTreeMap<SegId, BTreeSet<u32>> = BTreeMap::new();
        for e in es {
            if e.is_empty() || e.loc.is_pending() || self.has(&e.hash).await {
                continue;
            }
            by_seg.entry(e.loc.seg).or_default().insert(e.loc.frame);
        }
        let jobs: Vec<(SegId, Vec<u32>)> = by_seg
            .into_iter()
            .map(|(s, fs)| (s, fs.into_iter().collect()))
            .collect();
        let mut futs = Vec::with_capacity(jobs.len());
        for (seg, fs) in &jobs {
            futs.push(self.fetch_frames(*seg, fs, background));
        }
        bounded(futs, parallel).await.into_iter().sum()
    }

    /// Fetches the index and the tail of `seg` at once and holds the tail; the segment, unless
    /// another fetch holds its tail already.
    async fn fetch_tail(&self, seg: SegId) -> Option<SegId> {
        let (tail, idx) = futures::join!(self.remote.get_tail(seg, NODE_TAIL), self.seg_index(seg));
        let ((bytes, size), _) = (tail.ok()?, idx.ok()?);
        self.stats
            .bytes_fetched
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let off = size - bytes.len() as u32;
        let mut tails = self.tails.lock().expect("tails lock");
        if tails.contains_key(&seg) {
            return None;
        }
        tails.insert(seg, Arc::new(Tail { off, bytes }));
        Some(seg)
    }

    /// Fetches tree nodes as [`Self::fetch_entries`] does. For a segment whose index is not on
    /// this host, it asks for the index and the segment's last [`NODE_TAIL`] bytes at once;
    /// frames in that tail need no GET of their own.
    pub(crate) async fn fetch_nodes(&self, es: &[Entry], parallel: usize) -> Result<usize> {
        let mut segs = BTreeSet::new();
        for e in es {
            if e.is_empty() || e.loc.is_pending() || segs.contains(&e.loc.seg) {
                continue;
            }
            if self
                .indexes
                .lock()
                .expect("indexes lock")
                .contains_key(&e.loc.seg)
            {
                continue;
            }
            if !self.has(&e.hash).await {
                segs.insert(e.loc.seg);
            }
        }
        let mut futs = Vec::with_capacity(segs.len());
        for seg in segs {
            futs.push(self.fetch_tail(seg));
        }
        let mine: Vec<SegId> = bounded(futs, parallel).await.into_iter().flatten().collect();
        let r = self.fetch_entries(es, parallel, false).await;
        let mut tails = self.tails.lock().expect("tails lock");
        for seg in mine {
            tails.remove(&seg);
        }
        r
    }
}

#[cfg(all(test, feature = "sim"))]
mod tests {
    use pando_format::{BLOCKS_PER_CHUNK, CHUNK_SIZE, FrameBuilder, Loc, Pick, WriterId};

    use super::*;
    use crate::sim;

    struct Seg {
        id: SegId,
        entries: Vec<Entry>,
    }

    async fn upload(remote: &Remote, frames: Vec<Vec<(Bytes, bool, Hash)>>) -> Seg {
        let id = SegId::new(WriterId::new("main", 1), 7);
        let (mut data, mut index, mut entries) = (Vec::new(), Vec::new(), Vec::new());
        for (f, objs) in frames.into_iter().enumerate() {
            let mut fb = FrameBuilder::default();
            for (b, recipe, h) in objs {
                let (inner, len) = fb.push(h, &b, recipe);
                entries.push(Entry {
                    hash: h,
                    loc: Loc {
                        seg: id,
                        frame: f as u32,
                        inner,
                        len,
                    },
                });
            }
            let z = fb.finish().unwrap();
            index.push((data.len() as u32, z.len() as u32));
            data.extend_from_slice(&z);
        }
        let idx = SegmentIndex {
            frames: index,
            deps: vec![],
        }
        .encode();
        remote
            .put_segment(id, Bytes::from(data), Bytes::from(idx))
            .await
            .unwrap();
        Seg { id, entries }
    }

    fn chunk(seed: u8) -> Bytes {
        Bytes::from(
            (0..CHUNK_SIZE)
                .map(|i| (i as u8).wrapping_mul(seed).wrapping_add(seed))
                .collect::<Vec<_>>(),
        )
    }

    fn plain(b: Bytes) -> (Bytes, bool, Hash) {
        let h = Hash::of(&b);
        (b, false, h)
    }

    async fn cache(ports: &crate::Ports, dir: &str) -> Cache {
        let remote = Remote::new(ports.store.clone());
        Cache::open_with(ports.fs.clone(), ports.cpu.clone(), remote, Path::new(dir))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_miss_fetches_its_frame_once_for_every_reader() {
        let (ports, _, _) = sim::ports(1);
        let c = cache(&ports, "/c").await;
        let seg = upload(
            c.remote(),
            vec![vec![plain(chunk(1)), plain(chunk(2))], vec![plain(chunk(3))]],
        )
        .await;
        let e = seg.entries[1];
        let (a, b) = futures::join!(c.get(&e), c.read(&e, 100, 10));
        assert_eq!(a.unwrap(), chunk(2));
        assert_eq!(b.unwrap(), chunk(2).slice(100..110));
        assert_eq!(c.stats().frames_fetched.load(Ordering::Relaxed), 1);
        assert!(c.has(&seg.entries[0].hash).await, "the whole frame is stored");
        assert!(!c.has(&seg.entries[2].hash).await);

        assert_eq!(c.fetch_entries(&seg.entries, 4, true).await.unwrap(), 1);
        assert_eq!(c.get(&seg.entries[2]).await.unwrap(), chunk(3));
        let gets = c.remote().stats().gets.load(Ordering::Relaxed);
        assert_eq!(gets, 3, "one index and two ranges");
        assert_eq!(seg.id, e.loc.seg);
    }

    #[tokio::test]
    async fn recipes_rebuild_from_their_sources() {
        let (ports, _, _) = sim::ports(2);
        let c = cache(&ports, "/c").await;
        let src = chunk(5);
        let first = upload(c.remote(), vec![vec![plain(src.clone())]]).await;
        let mut want = vec![0; CHUNK_SIZE];
        want[..CHUNK_SIZE / 2].copy_from_slice(&src[CHUNK_SIZE / 2..]);
        let mut picks = [Pick::Zero; BLOCKS_PER_CHUNK];
        for (i, p) in picks.iter_mut().enumerate().take(BLOCKS_PER_CHUNK / 2) {
            *p = Pick::Source {
                src: 0,
                block: (i + BLOCKS_PER_CHUNK / 2) as u8,
            };
        }
        let enc = Recipe::encode(&first.entries, &picks, &want);
        let h = Hash::of(&want);
        let other = Cache::open_with(
            ports.fs.clone(),
            ports.cpu.clone(),
            Remote::new(ports.store.clone()),
            Path::new("/d"),
        )
        .await
        .unwrap();
        let id = SegId::new(WriterId::new("main", 1), 8);
        let mut fb = FrameBuilder::default();
        let (inner, len) = fb.push(h, &enc, true);
        let z = fb.finish().unwrap();
        let idx = SegmentIndex {
            frames: vec![(0, z.len() as u32)],
            deps: vec![first.id],
        }
        .encode();
        other
            .remote()
            .put_segment(id, Bytes::from(z), Bytes::from(idx))
            .await
            .unwrap();
        let e = Entry {
            hash: h,
            loc: Loc {
                seg: id,
                frame: 0,
                inner,
                len,
            },
        };
        assert_eq!(other.get(&e).await.unwrap(), Bytes::from(want));
        assert!(other.has(&first.entries[0].hash).await, "the source came along");
    }

    #[tokio::test]
    async fn a_failed_fetch_leaves_nothing_behind() {
        let (ports, store, _) = sim::ports(3);
        let c = cache(&ports, "/c").await;
        let seg = upload(c.remote(), vec![vec![plain(chunk(9))], vec![plain(chunk(10))]]).await;
        assert_eq!(c.get(&seg.entries[0]).await.unwrap(), chunk(9));
        store.inject(sim::Fault {
            op: sim::StoreOp::Get,
            applied: false,
        });
        assert!(c.get(&seg.entries[1]).await.is_err());
        assert_eq!(c.get(&seg.entries[1]).await.unwrap(), chunk(10));
    }
}
