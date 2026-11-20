//! A read's own fetch of the frame it needs, and the frame decoding every fetch shares.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use pando_format::{Entry, Hash, Recipe, SegId, SegmentIndex, decode_frame, u64_from};
use tokio::time::Instant;

use super::held::HeldId;
use super::sched::FetchOpts;
use super::{Cache, FrameKey, class, shared};
use crate::flight::{Claim, Joined, bounded};
use crate::ports::on_cpu;
use crate::{Error, Result};

/// One range GET of a read takes at most this many bytes.
const RANGE_MAX: u32 = 16 << 20;
/// The range GETs one read's fetch of a segment's frames sends at once.
const RUNS_IN_FLIGHT: usize = 8;
/// How much of a segment's end [`Cache::fetch_nodes`] asks for with its index: an upload
/// writes its tree nodes after its chunks.
const NODE_TAIL: u32 = 1 << 20;

/// A test of an object hash that a decode can keep past the call.
pub(super) type HashTest = Arc<dyn Fn(&Hash) -> bool + Send + Sync>;

/// The end of a segment: its bytes from `off` on.
#[derive(Debug)]
pub(super) struct Tail {
    off: u32,
    bytes: Bytes,
}

/// Which objects of a frame to decode, store and rebuild.
pub(super) struct Decode<'a> {
    pub(super) background: bool,
    /// Limits a frame that no read waits for, and that `whole` does not name, to these chunks
    /// and the sources of their recipes.
    pub(super) asked: Option<HashTest>,
    pub(super) whole: Option<&'a (dyn Fn(FrameKey) -> bool + Sync)>,
    /// Keeps the compressed bytes of a frame decoded in part, for the next fetch of it.
    pub(super) keep_rest: bool,
    /// The held recipe chunks whose missing sources the caller wants to know.
    pub(super) follow: Option<&'a (dyn Fn(&Hash) -> bool + Sync)>,
    pub(super) sink: Option<&'a dyn super::Sink>,
}

impl Decode<'_> {
    pub(super) fn whole(background: bool) -> Decode<'static> {
        Decode {
            background,
            asked: None,
            whole: None,
            keep_rest: false,
            follow: None,
            sink: None,
        }
    }
}

/// The outcome of a run: frames decoded, and the missing sources of the held recipe chunks
/// that [`Decode::follow`] names.
pub(super) struct RunDone {
    pub(super) result: Result<usize>,
    pub(super) sources: Vec<Entry>,
}

impl Cache {
    pub(super) async fn miss(&self, e: &Entry, background: bool) -> Result<()> {
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
            self.stats.stall_nanos.fetch_add(
                u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        r.map(drop)
    }

    pub(super) fn has_index(&self, seg: &SegId) -> bool {
        self.indexes.lock().contains_key(seg)
    }

    pub(super) async fn seg_index(&self, seg: SegId) -> Result<Arc<SegmentIndex>> {
        for _ in 0..2 {
            let known = self.indexes.lock().get(&seg).cloned();
            if let Some(i) = known {
                return Ok(i);
            }
            match self.index_flights.join(&seg, false) {
                Joined::Wait(c) => c.wait().await?,
                Joined::Lead(claim) => {
                    return match self.remote.get_index(seg).await {
                        Ok(i) => {
                            let i = Arc::new(i);
                            self.indexes.lock().insert(seg, Arc::clone(&i));
                            claim.finish(Ok(()))?;
                            Ok(i)
                        }
                        Err(e) => claim.finish(Err(e)).and(Err(Error::Cancelled)),
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
        let _urgent = (!background).then(|| self.window.urgent());
        let idx = self.seg_index(seg).await?;
        let mut mine: BTreeMap<u32, Claim<'_, FrameKey>> = BTreeMap::new();
        let mut waits = Vec::new();
        for f in frames.iter().copied() {
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
        let decode = Decode::whole(background);
        let mut futs = Vec::with_capacity(runs.len());
        for run in runs {
            futs.push(self.fetch_run(seg, &idx, run, &decode));
        }
        let mut n = 0;
        let mut err = None;
        for r in bounded(futs, RUNS_IN_FLIGHT).await {
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

    async fn fetch_run(
        &self,
        seg: SegId,
        idx: &SegmentIndex,
        run: Vec<(u32, Claim<'_, FrameKey>)>,
        d: &Decode<'_>,
    ) -> Result<usize> {
        let (off, n) = span(idx, &run);
        match self.get_range(seg, off, n).await {
            Ok(z) => self.decode_run(seg, idx, run, z, d).await.result,
            Err(e) => {
                let e = shared(e);
                for (_, c) in run {
                    let _ = c.finish(Err(Error::Shared(Arc::clone(&e))));
                }
                Err(Error::Shared(e))
            }
        }
    }

    /// Decodes the frames of a run, which `z` holds from the start of its first frame on, and
    /// releases each frame's waiters as soon as that frame is in. Frames a read waits for go
    /// first. After a failed frame, the frames left get its error.
    pub(super) async fn decode_run(
        &self,
        seg: SegId,
        idx: &SegmentIndex,
        mut run: Vec<(u32, Claim<'_, FrameKey>)>,
        z: Bytes,
        d: &Decode<'_>,
    ) -> RunDone {
        let off = idx.frames[run[0].0 as usize].0;
        self.stats.runs.fetch_add(1, Ordering::Relaxed);
        let mut sources = Vec::new();
        let mut n = 0;
        while !run.is_empty() {
            let pick = run.iter().position(|(_, c)| c.call().awaited()).unwrap_or(0);
            let (f, claim) = run.remove(pick);
            let urgent = claim.call().awaited();
            let (fo, fl) = idx.frames[f as usize];
            let zf = z.slice((fo - off) as usize..(fo - off + fl) as usize);
            let whole = urgent || d.whole.is_some_and(|w| w((seg, f)));
            let keep = if whole { None } else { d.asked.clone() };
            let partial = keep.is_some();
            let r = self.put_frame((seg, f), zf.clone(), keep, d, &mut sources).await;
            if r.is_ok() && partial && d.keep_rest {
                self.keep_frame(seg, fo, &zf);
            }
            if let Err(e) = claim.finish(r) {
                let e = shared(e);
                for (_, c) in run {
                    let _ = c.finish(Err(Error::Shared(Arc::clone(&e))));
                }
                return RunDone {
                    result: Err(Error::Shared(e)),
                    sources,
                };
            }
            self.stats.frames_fetched.fetch_add(1, Ordering::Relaxed);
            n += 1;
        }
        RunDone {
            result: Ok(n),
            sources,
        }
    }

    /// Bytes `[off, off + n)` of a segment: from a kept frame, from a tail
    /// [`Self::fetch_nodes`] holds, or from the store.
    pub(super) async fn get_range(&self, seg: SegId, off: u32, n: u32) -> Result<Bytes> {
        if let Some(b) = self.kept.take(seg, off, n) {
            self.stats.kept_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(b);
        }
        let tail = self.tails.lock().get(&seg).cloned();
        if let Some(t) = tail
            && off >= t.off
            && u64::from(off) + u64::from(n) <= u64::from(t.off) + u64_from(t.bytes.len())
        {
            let at = (off - t.off) as usize;
            return Ok(t.bytes.slice(at..at + n as usize));
        }
        let b = self.remote.get_range(seg, off, n).await?;
        self.stats
            .bytes_fetched
            .fetch_add(u64_from(b.len()), Ordering::Relaxed);
        Ok(b)
    }

    /// Decodes one frame, stores its objects, and rebuilds the recipe chunks this host lacks.
    /// A recipe chunk whose sources are not here is held instead while the budget lasts; the
    /// missing sources of those `d.follow` names go to `sources`.
    async fn put_frame(
        &self,
        at: FrameKey,
        z: Bytes,
        keep: Option<HashTest>,
        d: &Decode<'_>,
        sources: &mut Vec<Entry>,
    ) -> Result<()> {
        self.room_for_fetch().await;
        let decoded = on_cpu(self.cpu.as_ref(), class(d.background), move || {
            let keep = keep.as_ref().map(|k| -> &dyn Fn(&Hash) -> bool { k.as_ref() });
            decode_frame(&z, keep)
        })
        .await?;
        let mut objects: Vec<_> = decoded.objects.into_iter().collect();
        objects.sort_unstable_by_key(|(h, _)| *h);
        let mut recipes: Vec<_> = decoded.recipes.into_iter().collect();
        recipes.sort_unstable_by_key(|(h, _)| *h);
        for (h, v) in objects {
            let b = v.into_bytes();
            if !d.sink.is_some_and(|s| s.object(&h, &b)) {
                self.log.put(h, b).await?;
            }
        }
        for (h, enc) in recipes {
            if d.sink.is_some_and(|s| s.recipe(&h, &enc)) || self.has(&h).await {
                continue;
            }
            if !self.sources_here(&enc).await && self.hold(HeldId { hash: h, at }, &enc) {
                if d.follow.is_some_and(|f| f(&h)) {
                    sources.extend(self.missing_sources(&enc).await);
                }
                continue;
            }
            let b = self.rebuild(h, enc, d.background).await?;
            self.log.put(h, b).await?;
        }
        Ok(())
    }

    /// Rebuilds recipe chunk `h` from its encoding, fetching its sources, and checks it.
    pub(super) async fn rebuild(&self, h: Hash, enc: Bytes, background: bool) -> Result<Bytes> {
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

    /// Fetches the index and the tail of `seg` at once and holds the tail; the segment, unless
    /// another fetch holds its tail already.
    async fn fetch_tail(&self, seg: SegId) -> Option<SegId> {
        let (tail, idx) = futures::join!(self.remote.get_tail(seg, NODE_TAIL), self.seg_index(seg));
        let ((bytes, size), _) = (tail.ok()?, idx.ok()?);
        self.stats
            .bytes_fetched
            .fetch_add(u64_from(bytes.len()), Ordering::Relaxed);
        let off = size.checked_sub(u32::try_from(bytes.len()).ok()?)?;
        let mut tails = self.tails.lock();
        if tails.contains_key(&seg) {
            return None;
        }
        tails.insert(seg, Arc::new(Tail { off, bytes }));
        Some(seg)
    }

    /// Fetches tree nodes, ahead of background fetches. For a segment whose index is not on
    /// this host, it asks for the index and the segment's last [`NODE_TAIL`] bytes at once;
    /// frames in that tail need no GET of their own.
    pub(crate) async fn fetch_nodes(&self, es: &[Entry], parallel: usize) -> usize {
        let _urgent = self.window.urgent();
        let mut segs = BTreeSet::new();
        for e in es {
            if e.is_empty() || e.loc.is_pending() || segs.contains(&e.loc.seg) || self.has_index(&e.loc.seg) {
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
        let n = self
            .fetch_with(
                es,
                &FetchOpts {
                    parallel,
                    urgent: true,
                    ..FetchOpts::default()
                },
            )
            .await;
        let mut tails = self.tails.lock();
        for seg in mine {
            tails.remove(&seg);
        }
        n
    }
}

/// The byte range of a run of adjacent frames.
pub(super) fn span<T>(idx: &SegmentIndex, run: &[(u32, T)]) -> (u32, u32) {
    let off = idx.frames[run[0].0 as usize].0;
    let (lo, ll) = idx.frames[run[run.len() - 1].0 as usize];
    (off, lo + ll - off)
}
