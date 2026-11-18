//! The fetch scheduler keeps the link full for background fetches of many entries.
//!
//! It gets the index of every segment involved at once, cuts the wanted frames into runs of
//! adjacent frames ([`RUN_MAX`] bytes at most), and keeps runs in flight until [`WINDOW`]
//! bytes are requested and not yet decoded, across all background fetches of the cache. A run's
//! GET starts as soon as the window has room, not when a decoder is free, so a connection never
//! waits for the CPU. A run's bytes leave the window only once it is decoded: releasing them
//! when the bytes arrived made mounts slower. Reads do not go through the window: they
//! fetch their own frame and may take over a frame a background run holds.

use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use pando_format::{Entry, Hash, SegId, SegmentIndex};
use tokio::sync::Notify;

use super::fetch::{Decode, span};
use super::{Cache, FrameKey, shared};
use crate::flight::{Claim, Joined, bounded};
use crate::{Error, Result};

/// Bytes background fetches may have requested and not yet decoded.
pub(super) const WINDOW: u64 = 24 << 20;
/// Background GETs in flight at most.
const MAX_GETS: usize = 64;
/// One background range GET takes at most this many bytes.
const RUN_MAX: u32 = 4 << 20;
/// While something waits for an urgent fetch (a read, tree nodes), other runs take a GET only
/// while fewer than this many are in flight. TCP shares the link per connection: with 50 bulk
/// GETs in flight, a read's GET gets a fiftieth of it. 16 made mounts 0.8 s faster.
pub(super) const URGENT_GETS: usize = 16;

#[derive(Debug, Default)]
struct WindowState {
    bytes: u64,
    gets: usize,
    peak: usize,
    urgent: usize,
}

/// The bytes and GETs that background fetches have in flight, on the wire or waiting for a
/// decoder.
#[derive(Debug, Default)]
pub(super) struct Window {
    state: Mutex<WindowState>,
    wake: Notify,
}

/// Marks an urgent fetch while it lives.
#[derive(Debug)]
pub(super) struct Urgent<'a>(&'a Window);

impl Drop for Urgent<'_> {
    fn drop(&mut self) {
        self.0.state.lock().urgent -= 1;
        self.0.wake.notify_waiters();
    }
}

/// A run's share of the window, given back when it drops.
#[derive(Debug)]
pub(super) struct Permit<'a> {
    window: &'a Window,
    bytes: u64,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        {
            let mut s = self.window.state.lock();
            s.bytes -= self.bytes;
            s.gets -= 1;
        }
        self.window.wake.notify_waiters();
    }
}

impl Window {
    pub(super) fn urgent(&self) -> Urgent<'_> {
        self.state.lock().urgent += 1;
        Urgent(self)
    }

    /// Waits until `n` more bytes and one more GET fit. A run larger than the whole window goes
    /// when nothing else is in flight.
    pub(super) async fn acquire(&self, n: u64, urgent: bool) -> Permit<'_> {
        loop {
            let mut woken = std::pin::pin!(self.wake.notified());
            woken.as_mut().enable();
            {
                let mut s = self.state.lock();
                let limit = if !urgent && s.urgent > 0 {
                    URGENT_GETS
                } else {
                    MAX_GETS
                };
                if s.gets == 0 || (s.bytes + n <= WINDOW && s.gets < limit) {
                    s.bytes += n;
                    s.gets += 1;
                    s.peak = s.peak.max(s.gets);
                    return Permit {
                        window: self,
                        bytes: n,
                    };
                }
            }
            woken.await;
        }
    }

    /// The most GETs in flight at once so far.
    pub(super) fn peak_gets(&self) -> usize {
        self.state.lock().peak
    }
}

/// How a fetch of many entries runs.
#[derive(Default)]
pub(crate) struct FetchOpts<'a> {
    /// Decoders at once, and recipe rebuilds after the fetch.
    pub(crate) parallel: usize,
    /// Something waits for this fetch now: its GETs do not yield to urgent fetches.
    pub(crate) urgent: bool,
    pub(crate) asked_only: Option<AskedOnly>,
    /// Told each position of the entries once that entry is here or held, or its fetch failed.
    pub(crate) ready: Option<&'a (dyn Fn(usize) + Sync)>,
    /// Takes decoded objects instead of the cache.
    pub(crate) sink: Option<&'a dyn Sink>,
}

/// Takes the objects a fetch decodes, so they go where the caller needs them (a base image)
/// instead of into the cache. An object the sink refuses goes into the cache as usual.
pub(crate) trait Sink: Sync {
    /// Takes object `h`, checked against its hash.
    fn object(&self, h: &Hash, b: &Bytes) -> bool;
    /// Takes recipe chunk `h` to rebuild later.
    fn recipe(&self, h: &Hash, enc: &Bytes) -> bool;
}

/// Decodes, checks and stores only the chunks asked for and their recipe sources, unless
/// another fetch waits for the frame. A frame that holds one of `nodes` is decoded whole: its
/// other tree nodes are the next lookups. With `keep_rest`, the compressed bytes of each frame
/// decoded in part are kept, so a later fetch of its other chunks needs no GET.
#[derive(Debug, Default)]
pub(crate) struct AskedOnly {
    pub(crate) nodes: HashSet<Hash>,
    pub(crate) keep_rest: bool,
}

/// One range GET of adjacent frames of a segment that this fetch claimed.
struct Run<'a> {
    seg: SegId,
    idx: Arc<SegmentIndex>,
    frames: Vec<(u32, Claim<'a, FrameKey>)>,
    /// The earliest position in the caller's entries that the run serves; -1 for sources.
    pos: i64,
}

#[derive(Default)]
struct SegWant {
    frames: Vec<u32>,
    pos: HashMap<u32, i64>,
    hash: HashMap<u32, Hash>,
}

enum Event<'a> {
    Indexed(Vec<(SegId, Result<Arc<SegmentIndex>>, SegWant)>),
    Got(Run<'a>, Permit<'a>, Result<Bytes>),
    Decoded(Vec<FrameKey>, Vec<Entry>),
    Waited(FrameKey),
}

type Tasks<'a> = FuturesUnordered<BoxFuture<'a, Event<'a>>>;

struct Job<'o> {
    opts: &'o FetchOpts<'o>,
    need: Arc<RwLock<HashSet<Hash>>>,
    seen: HashSet<FrameKey>,
    frame_of: HashMap<FrameKey, Vec<usize>>,
}

impl Job<'_> {
    fn ready(&self, i: usize) {
        if let Some(r) = self.opts.ready {
            r(i);
        }
    }

    fn frame_done(&mut self, k: FrameKey) {
        if let Some(is) = self.frame_of.remove(&k) {
            for i in is {
                self.ready(i);
            }
        }
    }
}

impl Cache {
    /// Downloads the frames that hold `es`, skipping objects already here, then rebuilds the
    /// held recipe chunks among `es`. Runs go out in the order of `es`, so earlier entries
    /// arrive first. A frame another fetch is getting is waited for, not fetched again. The
    /// missing sources of a recipe chunk a frame holds join the fetch as soon as that frame is
    /// in, ahead of the runs still waiting. Returns how many frames held `es`.
    pub(crate) async fn fetch_with(&self, es: &[Entry], opts: &FetchOpts<'_>) -> usize {
        let parallel = opts.parallel.max(1);
        let mut job = Job {
            opts,
            need: Arc::default(),
            seen: HashSet::new(),
            frame_of: HashMap::new(),
        };
        let mut whole = HashSet::new();
        let mut want = Vec::new();
        for (i, e) in es.iter().enumerate() {
            if e.is_empty() || e.loc.is_pending() || self.has(&e.hash).await {
                job.ready(i);
                continue;
            }
            if let Some(enc) = self.held_encoding(e) {
                want.extend(self.missing_sources(&enc).await.into_iter().map(|s| (s, -1)));
                job.ready(i);
                continue;
            }
            let k = (e.loc.seg, e.loc.frame);
            if opts
                .asked_only
                .as_ref()
                .is_some_and(|a| a.nodes.contains(&e.hash))
            {
                whole.insert(k);
            }
            job.need.write().insert(e.hash);
            if !job.frame_of.contains_key(&k) {
                want.push((*e, i64::try_from(i).unwrap_or(i64::MAX)));
            }
            job.frame_of.entry(k).or_default().push(i);
        }
        let frames = job.frame_of.len();

        let need = Arc::clone(&job.need);
        let follow = move |h: &Hash| need.read().contains(h);
        let whole_frame = |k: FrameKey| whole.contains(&k);
        let decode = Decode {
            background: true,
            asked: opts.asked_only.as_ref().map(|_| -> super::fetch::HashTest {
                let need = Arc::clone(&job.need);
                Arc::new(move |h: &Hash| need.read().contains(h))
            }),
            whole: Some(&whole_frame),
            keep_rest: opts.asked_only.as_ref().is_some_and(|a| a.keep_rest),
            follow: Some(&follow),
            sink: opts.sink,
        };

        let mut queue: Vec<Run<'_>> = Vec::new();
        let mut tasks: Tasks<'_> = FuturesUnordered::new();
        let mut got: VecDeque<(Run<'_>, Permit<'_>, Result<Bytes>)> = VecDeque::new();
        let mut decoding = 0;
        self.want(&mut job, want, &mut tasks);
        loop {
            while decoding < parallel
                && let Some((run, permit, z)) = got.pop_front()
            {
                decoding += 1;
                tasks.push(Box::pin(self.decode_scheduled(run, permit, z, &decode)));
            }
            if tasks.is_empty() && queue.is_empty() && got.is_empty() {
                break;
            }
            let next = queue.first().map(|r| span(&r.idx, &r.frames).1);
            tokio::select! {
                biased;
                Some(ev) = tasks.next(), if !tasks.is_empty() => match ev {
                    Event::Indexed(segs) => self.claim(&mut job, segs, &mut queue, &mut tasks),
                    Event::Got(run, permit, z) => got.push_back((run, permit, z)),
                    Event::Decoded(keys, sources) => {
                        decoding -= 1;
                        for k in keys {
                            job.frame_done(k);
                        }
                        self.want(&mut job, sources.into_iter().map(|s| (s, -1)).collect(), &mut tasks);
                    }
                    Event::Waited(k) => job.frame_done(k),
                },
                permit = self.window.acquire(u64::from(next.unwrap_or(0)), opts.urgent), if next.is_some() => {
                    let run = queue.remove(0);
                    tasks.push(Box::pin(async move {
                        let (off, n) = span(&run.idx, &run.frames);
                        let z = self.get_range(run.seg, off, n).await;
                        Event::Got(run, permit, z)
                    }));
                }
            }
        }
        let left: Vec<FrameKey> = job.frame_of.keys().copied().collect();
        for k in left {
            job.frame_done(k);
        }
        self.rebuild_held(es, parallel).await;
        frames
    }

    /// Fetches `es` in the background, whole frames at a time.
    pub(crate) async fn fetch_entries(&self, es: &[Entry], parallel: usize) -> usize {
        self.fetch_with(
            es,
            &FetchOpts {
                parallel,
                ..FetchOpts::default()
            },
        )
        .await
    }

    /// Fetches `es` and stores only them and their recipe sources, not the other chunks of
    /// their frames, unless another fetch waits for a frame. A frame that holds one of `nodes`
    /// is decoded whole.
    pub(crate) async fn fetch_chunks(&self, es: &[Entry], nodes: HashSet<Hash>, parallel: usize) -> usize {
        let asked_only = Some(AskedOnly {
            nodes,
            keep_rest: true,
        });
        self.fetch_with(
            es,
            &FetchOpts {
                parallel,
                asked_only,
                ..FetchOpts::default()
            },
        )
        .await
    }

    /// The most background GETs in flight at once so far.
    pub(crate) fn peak_gets(&self) -> usize {
        self.window.peak_gets()
    }

    /// Notes the frames of `want` this fetch has not seen and starts getting their segments'
    /// indexes. Sources (position -1) will go ahead of the runs already waiting.
    fn want<'a>(&'a self, job: &mut Job<'_>, want: Vec<(Entry, i64)>, tasks: &mut Tasks<'a>) {
        let mut slot_of: HashMap<SegId, usize> = HashMap::new();
        let mut order: Vec<(SegId, SegWant)> = Vec::new();
        for (e, pos) in want {
            job.need.write().insert(e.hash);
            let k = (e.loc.seg, e.loc.frame);
            if !job.seen.insert(k) {
                continue;
            }
            let slot = *slot_of.entry(k.0).or_insert_with(|| {
                order.push((k.0, SegWant::default()));
                order.len() - 1
            });
            let sw = &mut order[slot].1;
            sw.frames.push(k.1);
            sw.pos.insert(k.1, pos);
            sw.hash.insert(k.1, e.hash);
        }
        if order.is_empty() {
            return;
        }
        tasks.push(Box::pin(async move {
            let mut futs = Vec::with_capacity(order.len());
            for (seg, sw) in order {
                futs.push(async move { (seg, self.seg_index(seg).await, sw) });
            }
            Event::Indexed(bounded(futs, MAX_GETS).await)
        }));
    }

    /// Claims the wanted frames of segments whose indexes are in and queues runs of them.
    fn claim<'a>(
        &'a self,
        job: &mut Job<'_>,
        segs: Vec<(SegId, Result<Arc<SegmentIndex>>, SegWant)>,
        queue: &mut Vec<Run<'a>>,
        tasks: &mut Tasks<'a>,
    ) {
        for (seg, idx, mut sw) in segs {
            let Ok(idx) = idx else {
                for f in sw.frames {
                    job.frame_done((seg, f));
                }
                continue;
            };
            sw.frames.sort_unstable();
            let mut mine: Vec<(u32, Claim<'a, FrameKey>)> = Vec::new();
            for f in sw.frames {
                let k = (seg, f);
                if f as usize >= idx.frames.len() {
                    job.frame_done(k);
                    continue;
                }
                match self.frame_flights.join(&k, true) {
                    Joined::Wait(c) => tasks.push(Box::pin(async move {
                        let _ = c.wait().await;
                        Event::Waited(k)
                    })),
                    Joined::Lead(claim) if self.log.has_own(&sw.hash[&f]) => {
                        let _ = claim.finish(Ok(()));
                        job.frame_done(k);
                    }
                    Joined::Lead(claim) => mine.push((f, claim)),
                }
            }
            let mut runs: Vec<Run<'a>> = Vec::new();
            for (f, claim) in mine {
                let (off, len) = idx.frames[f as usize];
                let pos = sw.pos[&f];
                let kept = self.is_kept(seg, off);
                match runs.last_mut() {
                    Some(r)
                        if !kept
                            && r.frames.last().is_some_and(|(p, _)| *p + 1 == f)
                            && !self.is_kept(seg, idx.frames[r.frames[0].0 as usize].0)
                            && off + len - idx.frames[r.frames[0].0 as usize].0 <= RUN_MAX =>
                    {
                        r.frames.push((f, claim));
                        r.pos = r.pos.min(pos);
                    }
                    _ => runs.push(Run {
                        seg,
                        idx: Arc::clone(&idx),
                        frames: vec![(f, claim)],
                        pos,
                    }),
                }
            }
            queue.extend(runs);
        }
        queue.sort_by_key(|r| r.pos);
    }

    async fn decode_scheduled<'a>(
        &'a self,
        run: Run<'a>,
        permit: Permit<'a>,
        z: Result<Bytes>,
        d: &Decode<'_>,
    ) -> Event<'a> {
        let keys = run.frames.iter().map(|(f, _)| (run.seg, *f)).collect();
        let sources = match z {
            Ok(z) => self.decode_run(run.seg, &run.idx, run.frames, z, d).await.sources,
            Err(e) => {
                let e = shared(e);
                for (_, c) in run.frames {
                    let _ = c.finish(Err(Error::Shared(Arc::clone(&e))));
                }
                Vec::new()
            }
        };
        drop(permit);
        Event::Decoded(keys, sources)
    }
}
