//! The packer: objects into frames, frames into segments, segments into the store.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use pando_format::{
    BlockKeys, CHUNK_SIZE, FrameBuilder, Hash, Loc, SegId, SegmentIndex, Writer, u64_from, usize_from,
};
use tokio::sync::{Semaphore, oneshot};
use tokio::task::JoinSet;

use crate::dedup::learned::{LearnedRec, Learner};
use crate::dedup::{Dedup, Prepared, PreparedChunks, prepare};
use crate::gate::Gate;
use crate::ports::{Cpu, CpuClass};
use crate::remote::Remote;
use crate::{Error, Result};

/// A segment goes up once its frames are estimated to compress to this many bytes.
pub(crate) const SEG_MAX: usize = 8 << 20;
/// Segment PUTs in flight at once.
const PUTS_IN_FLIGHT: usize = 8;

/// What one upload appended and sent to the store.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PackStats {
    /// Segments shipped.
    pub segments: usize,
    /// The compressed bytes of the segments stored; it counts only once the upload finishes.
    pub bytes: u64,
    /// The uncompressed bytes of the objects appended.
    pub raw_bytes: u64,
    /// Objects appended, recipes included.
    pub objects: usize,
    /// Chunks that went up as recipes.
    pub recipes: usize,
    /// Chunks that equal one in the store and went up only as its location.
    pub reused: usize,
    /// The chunk bytes that recipes and reused chunks kept from going up.
    pub recipe_saved: u64,
    /// Chunks an absorb had prepared.
    pub prepared: usize,
}

/// Makes chunks ready to append; it runs on many tasks at once, apart from the packer.
#[derive(Clone, Debug, Default)]
pub(crate) struct Preparer {
    dedup: Option<Arc<Dedup>>,
    pre: Option<Arc<PreparedChunks>>,
    taken: Arc<AtomicUsize>,
    gate: Option<Arc<Gate>>,
}

impl Preparer {
    /// Chunk `h` as an absorb prepared it.
    pub(crate) fn take(&self, h: &Hash) -> Option<Prepared> {
        let c = self.pre.as_ref()?.take(h, self.dedup.as_ref())?;
        self.taken.fetch_add(1, Ordering::Relaxed);
        Some(c)
    }

    /// Makes chunk `h` ready from its bytes.
    pub(crate) async fn prepare(&self, h: Hash, b: Bytes) -> Prepared {
        let _pass = match &self.gate {
            Some(g) => Some(g.enter().await),
            None => None,
        };
        prepare(self.dedup.as_deref(), h, b).await
    }
}

type Compressed = oneshot::Receiver<Result<Vec<u8>, pando_format::FormatError>>;

/// Appends objects to frames and frames to segments, and uploads full segments in parallel.
/// An object's [`Loc`] is known as soon as it is appended, so parents can embed their
/// children's locations before anything is uploaded. The locations hold only once
/// [`Packer::finish`] returns.
#[derive(Debug)]
pub(crate) struct Packer {
    remote: Remote,
    cpu: Arc<dyn Cpu>,
    writer: Arc<Writer>,
    seg_max: usize,
    known: Arc<HashMap<Hash, Loc>>,
    added: HashMap<Hash, Loc>,
    keys: HashMap<Hash, BlockKeys>,
    deps: BTreeSet<SegId>,
    seg_id: SegId,
    seg: Vec<Compressed>,
    seg_raw: usize,
    frame_no: u32,
    frame: FrameBuilder,
    z_in: Arc<AtomicU64>,
    z_out: Arc<AtomicU64>,
    puts: JoinSet<Result<u64>>,
    slots: Arc<Semaphore>,
    stats: PackStats,
    preparer: Preparer,
    learner: Option<Arc<Learner>>,
    learned: Vec<LearnedRec>,
    used_learned: Vec<u32>,
}

impl Packer {
    /// A packer for `writer`; objects in `known` are uploaded already and go up again only
    /// as their locations.
    pub(crate) fn new(
        remote: Remote,
        cpu: Arc<dyn Cpu>,
        writer: Arc<Writer>,
        seg_max: usize,
        known: Arc<HashMap<Hash, Loc>>,
    ) -> Packer {
        let seg_id = writer.next_seg_id();
        Packer {
            remote,
            cpu,
            writer,
            seg_max,
            known,
            added: HashMap::new(),
            keys: HashMap::new(),
            deps: BTreeSet::new(),
            seg_id,
            seg: Vec::new(),
            seg_raw: 0,
            frame_no: 0,
            frame: FrameBuilder::default(),
            z_in: Arc::default(),
            z_out: Arc::default(),
            puts: JoinSet::new(),
            slots: Arc::new(Semaphore::new(PUTS_IN_FLIGHT)),
            stats: PackStats::default(),
            preparer: Preparer::default(),
            learner: None,
            learned: Vec::new(),
            used_learned: Vec::new(),
        }
    }

    /// Lets chunks go up as recipes of the blocks `dedup` finds, and takes the chunks `pre`
    /// holds instead of preparing them again.
    pub(crate) fn with_dedup(
        mut self,
        dedup: Option<Arc<Dedup>>,
        pre: Option<Arc<PreparedChunks>>,
    ) -> Packer {
        self.preparer = Preparer {
            dedup,
            pre,
            taken: Arc::default(),
            gate: self.preparer.gate.take(),
        };
        self
    }

    /// Lets at most as many chunks be prepared at once as `gate` admits.
    pub(crate) fn with_gate(mut self, gate: Arc<Gate>) -> Packer {
        self.preparer.gate = Some(gate);
        self
    }

    /// Keeps the chunks that go up as themselves for later runs on this host.
    pub(crate) fn with_learner(mut self, learner: Option<Arc<Learner>>) -> Packer {
        self.learner = learner;
        self
    }

    pub(crate) fn preparer(&self) -> &Preparer {
        &self.preparer
    }

    /// The records of the chunks this upload learned; they count once it is published.
    pub(crate) fn learned(&self) -> &[LearnedRec] {
        &self.learned
    }

    /// The learned chunks this upload uses, as recipe sources or as chunks it equals.
    pub(crate) fn used_learned(&self) -> &[u32] {
        &self.used_learned
    }

    pub(crate) fn lookup(&self, h: &Hash) -> Option<Loc> {
        self.known.get(h).or_else(|| self.added.get(h)).copied()
    }

    /// The objects this packer appended, with their locations.
    pub(crate) fn added(&self) -> &HashMap<Hash, Loc> {
        &self.added
    }

    /// The block keys of the full chunks appended, for the change list.
    pub(crate) fn keys(&self) -> &HashMap<Hash, BlockKeys> {
        &self.keys
    }

    pub(crate) fn stats(&self) -> PackStats {
        PackStats {
            prepared: self.preparer.taken.load(Ordering::Relaxed),
            ..self.stats
        }
    }

    /// Appends object `h` unless it is known or appended already.
    pub(crate) async fn append_object(&mut self, h: Hash, b: &[u8]) -> Result<Loc> {
        if let Some(l) = self.lookup(&h) {
            return Ok(l);
        }
        Ok(self.append(h, b, false).await)
    }

    /// [`Self::append_object`] for a chunk: it may go up as a recipe or as the chunk it
    /// equals, and a full chunk also gets its block keys.
    pub(crate) async fn append_chunk(&mut self, h: Hash, b: Bytes) -> Result<Loc> {
        if let Some(l) = self.lookup(&h) {
            return Ok(l);
        }
        let c = match self.preparer.take(&h) {
            Some(c) => c,
            None => self.preparer.prepare(h, b).await,
        };
        Ok(self.commit(h, c).await)
    }

    /// Appends chunk `h` as [`Preparer::prepare`] made it ready.
    pub(crate) async fn commit(&mut self, h: Hash, c: Prepared) -> Loc {
        if let Some(l) = self.lookup(&h) {
            return l;
        }
        if let Some(learned) = self.preparer.dedup.as_ref().and_then(|d| d.learned.as_ref()) {
            match &c {
                Prepared::Same(e) => learned.slots_of(&mut self.used_learned, std::slice::from_ref(e)),
                Prepared::Recipe { enc, .. } => {
                    learned.slots_of(&mut self.used_learned, &pando_format::Recipe::sources_of(enc));
                }
                Prepared::Plain { .. } => {}
            }
        }
        match c {
            Prepared::Same(e) => {
                self.added.insert(h, e.loc);
                self.stats.reused += 1;
                self.stats.recipe_saved += u64_from(CHUNK_SIZE);
                e.loc
            }
            Prepared::Recipe { enc, deps, keys } => {
                self.deps.extend(deps);
                self.stats.recipes += 1;
                self.stats.recipe_saved += u64_from(CHUNK_SIZE.saturating_sub(enc.len()));
                self.keys.insert(h, keys);
                self.append(h, &enc, true).await
            }
            Prepared::Plain { bytes, keys } => {
                let loc = self.append(h, &bytes, false).await;
                if let Some(keys) = keys {
                    self.keys.insert(h, keys);
                    if let Some(r) = self.learner.as_ref().and_then(|l| l.keep(h, loc, &bytes, &keys)) {
                        self.learned.push(r);
                    }
                }
                loc
            }
        }
    }

    async fn append(&mut self, h: Hash, b: &[u8], recipe: bool) -> Loc {
        if self.frame.is_full_for(b.len()) {
            self.close_frame().await;
        }
        let (inner, len) = self.frame.push(h, b, recipe);
        let loc = Loc {
            seg: self.seg_id,
            frame: self.frame_no,
            inner,
            len,
        };
        self.added.insert(h, loc);
        self.stats.objects += 1;
        self.stats.raw_bytes += u64_from(b.len());
        loc
    }

    /// Starts compressing the open frame. A location names a frame by its number and an object
    /// by its offset in the uncompressed frame, so no location waits for the compression.
    async fn close_frame(&mut self) {
        if self.frame.is_empty() {
            return;
        }
        let fb = std::mem::take(&mut self.frame);
        let raw = fb.payload_len();
        let (tx, rx) = oneshot::channel();
        let (z_in, z_out) = (Arc::clone(&self.z_in), Arc::clone(&self.z_out));
        self.cpu.spawn(
            CpuClass::Background,
            Box::new(move || {
                let z = fb.finish();
                if let Ok(z) = &z {
                    z_in.fetch_add(u64_from(raw), Ordering::Relaxed);
                    z_out.fetch_add(u64_from(z.len()), Ordering::Relaxed);
                }
                let _ = tx.send(z);
            }),
        );
        self.seg.push(rx);
        self.seg_raw += raw;
        self.frame_no += 1;
        if self.seg_estimate() >= self.seg_max {
            self.ship().await;
        }
    }

    /// The compressed size of the open segment, from the ratio of the frames compressed so
    /// far; before the first one it assumes no compression.
    fn seg_estimate(&self) -> usize {
        let (i, o) = (
            self.z_in.load(Ordering::Relaxed),
            self.z_out.load(Ordering::Relaxed),
        );
        u64_from(self.seg_raw)
            .saturating_mul(o)
            .checked_div(i)
            .map_or(self.seg_raw, usize_from)
    }

    async fn ship(&mut self) {
        if self.seg.is_empty() {
            return;
        }
        let id = std::mem::replace(&mut self.seg_id, self.writer.next_seg_id());
        let frames = std::mem::take(&mut self.seg);
        let deps: Vec<SegId> = std::mem::take(&mut self.deps).into_iter().collect();
        self.seg_raw = 0;
        self.frame_no = 0;
        self.stats.segments += 1;
        let slot = Arc::clone(&self.slots).acquire_owned().await;
        let remote = self.remote.clone();
        self.puts.spawn(async move {
            let _slot = slot.map_err(|_| Error::Cancelled)?;
            let mut data = Vec::new();
            let mut index = Vec::with_capacity(frames.len());
            for f in frames {
                let z = f.await.map_err(|_| Error::Cancelled)??;
                let too_big = || Error::Corrupt(format!("segment {id} passes 4 GiB"));
                let off = u32::try_from(data.len()).map_err(|_| too_big())?;
                let len = u32::try_from(z.len()).map_err(|_| too_big())?;
                index.push((off, len));
                data.extend_from_slice(&z);
            }
            let n = u64_from(data.len());
            let idx = SegmentIndex { frames: index, deps }.encode();
            remote
                .put_segment(id, Bytes::from(data), Bytes::from(idx))
                .await?;
            Ok(n)
        });
    }

    /// Uploads the last segment and waits until every segment is durable in the store. Only
    /// then may the caller trust the locations in [`Self::added`].
    pub(crate) async fn finish(&mut self) -> Result<()> {
        self.close_frame().await;
        self.ship().await;
        let mut err = None;
        while let Some(r) = self.puts.join_next().await {
            match r {
                Ok(Ok(n)) => self.stats.bytes += n,
                Ok(Err(e)) => {
                    err.get_or_insert(e);
                }
                Err(e) => {
                    err.get_or_insert(Error::Corrupt(format!("segment upload task: {e}")));
                }
            }
        }
        err.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use std::path::Path;

    use pando_format::{Entry, WriterId};

    use super::*;
    use crate::cache::Cache;
    use crate::ports::Store;
    use crate::sim;

    #[tokio::test]
    async fn packed_objects_read_back_from_another_host() {
        let (ports, store, _) = sim::ports(1);
        let remote = Remote::new(Arc::clone(&ports.store));
        let writer = Arc::new(Writer::new(WriterId::new("main", 1), 0));
        let mut p = Packer::new(
            remote.clone(),
            Arc::clone(&ports.cpu),
            writer,
            1 << 20,
            Arc::default(),
        );
        let mut es = Vec::new();
        for i in 0..40u32 {
            let mut x = u64::from(i).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
            let b: Vec<u8> = (0..CHUNK_SIZE)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x.to_le_bytes()[0]
                })
                .collect();
            let h = Hash::of(&b);
            let loc = p.append_chunk(h, Bytes::from(b.clone())).await.unwrap();
            es.push((Entry { hash: h, loc }, b));
        }
        p.finish().await.unwrap();
        let st = p.stats();
        assert!(st.segments >= 2, "{st:?}");
        assert_eq!(st.objects, 40);
        assert_eq!(p.keys().len(), 40);
        let listed = store.list("segments").await.unwrap();
        assert_eq!(listed.len(), 2 * st.segments);

        let c = Cache::open_with(
            Arc::clone(&ports.fs),
            Arc::clone(&ports.cpu),
            remote,
            Path::new("/other"),
        )
        .await
        .unwrap();
        for (e, b) in &es {
            assert_eq!(c.get(e).await.unwrap(), Bytes::from(b.clone()));
        }
    }

    #[tokio::test]
    async fn a_known_object_is_not_appended_again() {
        let (ports, _, _) = sim::ports(2);
        let h = Hash::of(b"x");
        let loc = Loc {
            seg: SegId([1; 16]),
            frame: 2,
            inner: 3,
            len: 1,
        };
        let known = Arc::new(HashMap::from([(h, loc)]));
        let writer = Arc::new(Writer::new(WriterId::new("main", 1), 0));
        let mut p = Packer::new(
            Remote::new(Arc::clone(&ports.store)),
            Arc::clone(&ports.cpu),
            writer,
            SEG_MAX,
            known,
        );
        assert_eq!(p.append_object(h, b"x").await.unwrap(), loc);
        p.finish().await.unwrap();
        assert_eq!(p.stats().segments, 0);
    }
}
