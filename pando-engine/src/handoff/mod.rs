//! A handoff takes Pando out of the I/O path once a branch is local.
//!
//! The file system sits on a device-mapper device whose table first points at the branch's
//! frontend device. The handoff copies the branch into a local image while Pando keeps serving
//! it, then suspends the device, copies the chunks written meanwhile, flushes, points the table
//! at a loop device over the image and resumes. From then on the kernel serves every read and
//! write from the image.
//!
//! While the run goes on, background snapshots take back what the kernel wrote, every interval
//! or early after a burst of writes. Where the host has an era target, an era device between
//! the device and the loop device records which chunks each write reached. A snapshot begins
//! a new era during a short suspend, asks for the chunks written since the last one and clones
//! only those ranges of the image while the device serves; it repeats that for the chunks
//! written while it cloned until few are left, and a last short suspend clones those and the
//! ones written since. With few enough chunks written, a snapshot clones nothing: it reads the
//! chunks from the image in the same rounds. Each snapshot holds the image as it was at that
//! last suspend. When the writes do not let up while it reads, the snapshot gives up for now;
//! after a few in a row, the next one clones.
//! Elsewhere a snapshot clones the whole image and takes the chunks whose extents moved since
//! the previous clone. A clone shares its blocks with the image, so the next write to each
//! costs a copy; cloning only what changed keeps that to the chunks written in consecutive
//! intervals. At detach the handoff takes what changed since the last snapshot the same way.
//! A flush records each chunk taken from a clone as a reference into it, so a clone stays
//! until an upload has published those chunks; a chunk read from the image holds its bytes.
//!
//! With shared bases, the image starts as a clone of the base image of the root the branch
//! opened at, and only the chunks that differ from it are copied.

pub(crate) mod adopt;
mod extents;
mod owner;

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pando_format::{CHUNK_SIZE, Hash};
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use extents::{changed_chunks, snapshot_due};
use owner::Owner;

use crate::base::{Bases, BuildOptions, build};
use crate::cache::BaseKey;
use crate::dedup::Dedup;
use crate::dedup::learned::LearnedIndex;
use crate::ports::{DeviceSwitch, EraWritten, Extent, LocalFile, Open};
use crate::{Ports, Result, Volume};

const CHUNK: u64 = CHUNK_SIZE as u64;
/// How often the snapshot loop checks the bytes written to the image.
const SNAPSHOT_POLL: Duration = Duration::from_millis(100);
/// The least size an image's era records may grow to, so a branch can grow to it.
const ERA_ROOM: u64 = 1 << 40;
/// Bytes of era records per chunk: the era of each, the bits of the eras not yet folded in,
/// and their trees, with room to spare.
const ERA_PER_CHUNK: u64 = 32;
const ERA_FIXED: u64 = 64 << 20;
/// Rounds of copying the chunks written during the copy before the suspend.
const RECOPY_ROUNDS: usize = 4;
/// Rounds a snapshot reads the chunks written while the last round read, before it gives up.
const COPY_ROUNDS: usize = 4;
/// The most chunks a snapshot reads while the device is suspended.
#[cfg(not(test))]
const FREEZE_CHUNKS: u64 = 128;
#[cfg(test)]
const FREEZE_CHUNKS: u64 = 4;
/// Snapshots in a row that may give up before the next one clones instead.
const POSTPONE_MAX: u32 = 3;
/// A recopy round ends the copy once it has fewer chunks than this; the rest go under suspend.
const RECOPY_FEW: usize = 64;
const BUILD_PARALLEL: usize = 16;

/// What a handoff runs on: the host's ports and device switch, and its base images.
#[derive(Clone, Debug)]
pub(crate) struct HostEnv {
    pub(crate) ports: Ports,
    pub(crate) devices: Arc<dyn DeviceSwitch>,
    /// Shared base images, when the host keeps them.
    pub(crate) bases: Option<Bases>,
    /// Upload chunks as recipes of the base image's blocks and of learned chunks.
    pub(crate) dedup: bool,
    pub(crate) learned: Option<Arc<LearnedIndex>>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SnapshotOptions {
    /// Take a snapshot this often; zero takes none until detach.
    pub(crate) every: Duration,
    /// Take one early once this many bytes reached the image; zero never does.
    pub(crate) burst: u64,
    /// Take one early once writes reached the image and none came for this long; zero never
    /// does.
    pub(crate) idle: Duration,
    /// Read the written chunks from the image instead of cloning them when they hold at most
    /// this many bytes; zero always clones.
    pub(crate) copy_max: u64,
}

/// What a handoff did before the device moved to the image.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HandoffReport {
    /// The image started as a clone of the base image.
    pub shared: bool,
    pub copied: usize,
    /// Rounds of copying what was written during the copy.
    pub rounds: usize,
    /// Chunks copied while the device was suspended.
    pub last: usize,
    pub paused: Duration,
    /// Bytes reads took from the base image before the switch.
    pub from_base: u64,
    /// An era device tracks the chunks the kernel writes, so snapshots clone only those.
    pub tracked: bool,
}

/// What the background snapshots did.
#[derive(Debug, Default)]
pub(crate) struct SnapshotStats {
    pub(crate) taken: AtomicUsize,
    pub(crate) chunks: AtomicUsize,
    pub(crate) failed: AtomicUsize,
    pub(crate) last_error: Mutex<Option<String>>,
    /// Snapshots that read the chunks from the image instead of cloning them.
    pub(crate) copied: AtomicUsize,
    /// Snapshots that gave up because writes kept coming.
    pub(crate) postponed: AtomicUsize,
    /// Microseconds the device spent suspended for snapshots, in all and at most at once.
    pub(crate) paused_us: AtomicU64,
    pub(crate) max_paused_us: AtomicU64,
    /// The longest suspend of each [`Pause`], in microseconds.
    pub(crate) max_pause_us: [AtomicU64; 3],
}

/// Why a snapshot suspended the device.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Pause {
    /// To begin a new era.
    Mark = 0,
    /// To read the last chunks of a snapshot that clones nothing.
    Freeze = 1,
    /// To clone the image or the ranges written.
    Clone = 2,
}

impl SnapshotStats {
    fn paused(&self, kind: Pause, d: Duration) {
        let us = u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
        self.paused_us.fetch_add(us, Ordering::Relaxed);
        self.max_paused_us.fetch_max(us, Ordering::Relaxed);
        self.max_pause_us[kind as usize].fetch_max(us, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
struct Snaps {
    /// The extents the next snapshot compares with: of the last clone, or of the base image
    /// the image was cloned from.
    prev: Option<Vec<Extent>>,
    /// The last clone, which goes once a later one replaces it and its chunks are published.
    prev_path: Option<PathBuf>,
    next: u32,
    /// With an era device: the era that began with the last snapshot, or the first era.
    since: u32,
    /// Snapshots in a row that gave up because writes kept coming.
    postponed: u32,
}

impl Snaps {
    fn next_path(&mut self, image: &Path) -> PathBuf {
        let p = with_suffix(image, &format!(".snap{}", self.next));
        self.next += 1;
        p
    }
}

/// One branch's handoff to a local image.
#[derive(Debug)]
pub(crate) struct Handoff {
    env: HostEnv,
    dm: String,
    image: PathBuf,
    file: Arc<dyn LocalFile>,
    base: Option<Hash>,
    switched: AtomicBool,
    loop_dev: Mutex<Option<PathBuf>>,
    opts: SnapshotOptions,
    /// The era device that tracks the image's writes, when the host has one.
    era: Mutex<Option<String>>,
    snaps: AsyncMutex<Snaps>,
    /// Clones replaced by a later one; each stays until no unpublished flush refers to it.
    retired: Mutex<Vec<PathBuf>>,
    stop: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
    pub(crate) stats: SnapshotStats,
}

fn with_suffix(p: &Path, s: &str) -> PathBuf {
    let mut o = p.as_os_str().to_owned();
    o.push(s);
    PathBuf::from(o)
}

/// The era device between device `dm` and the loop device of its image.
pub(crate) fn era_name(dm: &str) -> String {
    format!("{dm}-era")
}

/// The file that holds the era records of `image`.
pub(crate) fn era_records(image: &Path) -> PathBuf {
    with_suffix(image, ".era")
}

/// The length of the sparse records file for an image of `size` bytes, which may grow to many
/// times that.
fn era_records_len(size: u64) -> u64 {
    ERA_FIXED + size.saturating_mul(4).max(ERA_ROOM).div_ceil(CHUNK) * ERA_PER_CHUNK
}

/// How many chunks `blocks` holds.
fn count(blocks: &[Range<u64>]) -> u64 {
    blocks.iter().map(|r| r.end - r.start).sum()
}

/// The chunks in `a` and `b`, sorted, disjoint and merged.
fn merge(a: &[Range<u64>], b: &[Range<u64>]) -> Vec<Range<u64>> {
    let mut all: Vec<Range<u64>> = a.iter().chain(b).filter(|r| r.start < r.end).cloned().collect();
    all.sort_by_key(|r| r.start);
    let mut out: Vec<Range<u64>> = Vec::with_capacity(all.len());
    for r in all {
        match out.last_mut() {
            Some(l) if r.start <= l.end => l.end = l.end.max(r.end),
            _ => out.push(r),
        }
    }
    out
}

fn chunks(blocks: &[Range<u64>]) -> Vec<u64> {
    blocks.iter().flat_map(|r| r.clone()).collect()
}

fn base36(mut n: u128) -> String {
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = Vec::new();
    loop {
        out.push(DIGITS[(n % 36) as usize]);
        n /= 36;
        if n == 0 {
            break;
        }
    }
    out.reverse();
    String::from_utf8(out).expect("ascii digits")
}

/// A new image name for branch `name` in `dir`. Each attach gets its own, from the time and a
/// random number: after a crash, cache records may still name the files of an earlier attach,
/// so a new one never reuses them.
pub(crate) fn image_path(ports: &Ports, dir: &Path, name: &str) -> PathBuf {
    let now = ports
        .clock
        .wall()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let id = base36(now.as_nanos() << 32 | u128::from(ports.rng.next_u32()));
    dir.join(format!("{name}.{id}.img"))
}

impl Handoff {
    fn new(
        env: &HostEnv,
        dm: &str,
        image: PathBuf,
        file: Arc<dyn LocalFile>,
        base: Option<Hash>,
        opts: SnapshotOptions,
    ) -> Handoff {
        Handoff {
            env: env.clone(),
            dm: dm.to_owned(),
            image,
            file,
            base,
            switched: AtomicBool::new(false),
            loop_dev: Mutex::default(),
            opts,
            era: Mutex::default(),
            snaps: AsyncMutex::new(Snaps {
                since: 1,
                ..Snaps::default()
            }),
            retired: Mutex::default(),
            stop: watch::Sender::new(false),
            task: Mutex::default(),
            stats: SnapshotStats::default(),
        }
    }

    pub(crate) fn image(&self) -> &Path {
        &self.image
    }

    pub(crate) fn switched(&self) -> bool {
        self.switched.load(Ordering::Acquire)
    }

    async fn owner(&self, v: &Volume, switched: bool) -> Result<()> {
        Owner {
            generation: v.generation(),
            base: self.base,
            switched,
        }
        .write(self.env.ports.fs.as_ref(), &self.image)
        .await
    }

    /// A clone of the image, which shares every block until the image is written.
    async fn clone_image(&self) -> Result<PathBuf> {
        let path = self.snaps.lock().await.next_path(&self.image);
        self.clone_to(&path).await?;
        Ok(path)
    }

    async fn clone_to(&self, path: &Path) -> Result<()> {
        let _ = self.env.ports.fs.remove(path).await;
        if let Err(e) = self.env.ports.image.clone_file(&self.image, path).await {
            let _ = self.env.ports.fs.remove(path).await;
            return Err(e.into());
        }
        Ok(())
    }

    /// Clones the image while the device is suspended, so the clone holds every completed
    /// write and none in flight. With era device `era`, also begins a new era during the
    /// suspend and returns it: the writes the clone lacks belong to it or later ones.
    async fn frozen_clone(&self, path: &Path, era: Option<&str>) -> Result<(Duration, Option<u32>)> {
        let t0 = Instant::now();
        let devices = self.env.devices.as_ref();
        devices.suspend(&self.dm).await?;
        let cloned = async {
            let now = match era {
                Some(e) => Some(devices.era_written(e, u32::MAX).await?.now),
                None => None,
            };
            self.clone_to(path).await?;
            Ok::<_, crate::Error>(now)
        }
        .await;
        let resumed = devices.resume(&self.dm).await;
        let paused = t0.elapsed();
        if let Err(e) = resumed {
            let _ = self.env.ports.fs.remove(path).await;
            return Err(e.into());
        }
        cloned.map(|now| (paused, now))
    }

    async fn set_prev(&self, s: &mut Snaps, path: PathBuf) -> Result<()> {
        let ext = match self.env.ports.image.extents(&path).await {
            Ok(e) => e,
            Err(e) => {
                let _ = self.env.ports.fs.remove(&path).await;
                return Err(e.into());
            }
        };
        self.retire_prev(s);
        s.prev = Some(ext);
        s.prev_path = Some(path);
        Ok(())
    }

    fn retire_prev(&self, s: &mut Snaps) {
        if let Some(p) = s.prev_path.take() {
            self.retired.lock().expect("retired lock").push(p);
        }
    }

    fn era(&self) -> Option<String> {
        self.era.lock().expect("era lock").clone()
    }

    pub(crate) fn tracked(&self) -> bool {
        self.era.lock().expect("era lock").is_some()
    }

    /// Takes one snapshot: clones what the kernel wrote to the image since the last one under
    /// suspend, takes those chunks as writes, then flushes them.
    pub(crate) async fn snapshot_once(&self, v: &Volume) -> Result<usize> {
        match self.era() {
            Some(era) => self.snapshot_era(v, &era).await,
            None => self.snapshot_extents(v).await,
        }
    }

    /// Runs `f` while device `dm` is suspended, so no write is in flight or reaches the image.
    async fn suspended<T>(&self, kind: Pause, f: impl Future<Output = Result<T>>) -> Result<T> {
        let t0 = Instant::now();
        let devices = self.env.devices.as_ref();
        devices.suspend(&self.dm).await?;
        let r = f.await;
        let resumed = devices.resume(&self.dm).await;
        self.stats.paused(kind, t0.elapsed());
        resumed?;
        r
    }

    /// Begins a new era on `era` while the device is suspended, so no write in flight
    /// straddles it, and returns it.
    async fn mark(&self, era: &str) -> Result<u32> {
        self.suspended(Pause::Mark, async { Ok(self.env.devices.era_begin(era).await?) })
            .await
    }

    /// A snapshot of the chunks era device `era` saw written. With few enough, it reads them
    /// from the image, or gives up for now when writes keep coming; else it clones them. Most
    /// are listed before the suspend; the suspend lists the rest and clones or reads them.
    async fn snapshot_era(&self, v: &Volume, era: &str) -> Result<usize> {
        let mut s = self.snaps.lock().await;
        let devices = self.env.devices.as_ref();
        let copy_max = self.opts.copy_max;
        let now = self.mark(era).await?;
        let early = EraWritten {
            blocks: devices.era_listed(era, s.since).await?,
            now,
        };
        if early.blocks.is_empty() {
            s.since = early.now;
            s.postponed = 0;
            self.stats.taken.fetch_add(1, Ordering::Relaxed);
            return Ok(0);
        }
        if copy_max > 0 && count(&early.blocks) * CHUNK <= copy_max && s.postponed < POSTPONE_MAX {
            let Some((n, now)) = self.copy_era(v, era, &early).await? else {
                s.postponed += 1;
                self.stats.postponed.fetch_add(1, Ordering::Relaxed);
                return Ok(0);
            };
            self.retire_prev(&mut s);
            s.since = now;
            s.postponed = 0;
            self.stats.taken.fetch_add(1, Ordering::Relaxed);
            self.stats.copied.fetch_add(1, Ordering::Relaxed);
            self.stats.chunks.fetch_add(n, Ordering::Relaxed);
            return Ok(n);
        }
        let path = s.next_path(&self.image);
        let taken = self.clone_era(era, &early, &path).await?;
        let n = match self.absorb_clone(v, &path, &chunks(&taken.blocks)).await {
            Ok(n) => n,
            Err(e) => {
                self.retired.lock().expect("retired lock").push(path);
                return Err(e);
            }
        };
        self.retire_prev(&mut s);
        s.prev_path = Some(path);
        s.since = taken.now;
        s.postponed = 0;
        self.stats.taken.fetch_add(1, Ordering::Relaxed);
        self.stats.chunks.fetch_add(n, Ordering::Relaxed);
        Ok(n)
    }

    /// Reads the chunks in `early`, whose era began with the device suspended, from the image
    /// while the device serves, then in rounds the chunks written while the last round read,
    /// until few are left. Then, suspended, it reads those and the ones written since, and
    /// flushes. Returns the chunks taken and the era that began at that suspend, or `None`
    /// when the writes did not let up; what it read then stays unflushed for the next try.
    async fn copy_era(&self, v: &Volume, era: &str, early: &EraWritten) -> Result<Option<(usize, u32)>> {
        let devices = self.env.devices.as_ref();
        let (mut list, mut from, mut n) = (early.blocks.clone(), early.now, 0);
        let mut rounds = 0;
        while count(&list) > FREEZE_CHUNKS {
            if rounds == COPY_ROUNDS {
                return Ok(None);
            }
            n += v.read_chunks(&self.file, &chunks(&list)).await?;
            let next = self.mark(era).await?;
            let written = devices.era_listed(era, from).await?;
            if count(&written) * 2 > count(&list) {
                return Ok(None);
            }
            (list, from) = (written, next);
            rounds += 1;
        }
        let (m, now) = self
            .suspended(Pause::Freeze, async {
                let now = devices.era_begin(era).await?;
                let newer = devices.era_listed(era, from).await?;
                let m = v.read_chunks(&self.file, &chunks(&merge(&list, &newer))).await?;
                Ok((m, now))
            })
            .await?;
        v.flush().await?;
        Ok(Some((n + m, now)))
    }

    async fn absorb_clone(&self, v: &Volume, path: &Path, idxs: &[u64]) -> Result<usize> {
        let file = self.env.ports.fs.open(path, Open::Read).await?;
        let n = v.absorb_chunks(&file, path, idxs).await?;
        v.flush().await?;
        Ok(n)
    }

    /// Clones the chunks in `early`, whose era began with the device suspended, to `path` while
    /// the device serves, then in rounds the chunks written while the last round cloned, until
    /// few are left. Then, suspended, it clones those and the ones written since. Returns every
    /// chunk it cloned and the era that began at that suspend.
    async fn clone_era(&self, era: &str, early: &EraWritten, path: &Path) -> Result<EraWritten> {
        let devices = self.env.devices.as_ref();
        let _ = self.env.ports.fs.remove(path).await;
        let taken = async {
            let (mut list, mut from, mut all) = (early.blocks.clone(), early.now, early.blocks.clone());
            for _ in 0..COPY_ROUNDS {
                if count(&list) <= FREEZE_CHUNKS {
                    break;
                }
                self.clone_chunks(path, &list).await?;
                let next = self.mark(era).await?;
                list = devices.era_listed(era, from).await?;
                all = merge(&all, &list);
                from = next;
            }
            self.suspended(Pause::Clone, async {
                let now = devices.era_begin(era).await?;
                let newer = devices.era_listed(era, from).await?;
                self.clone_chunks(path, &merge(&list, &newer)).await?;
                Ok(EraWritten {
                    blocks: merge(&all, &newer),
                    now,
                })
            })
            .await
        }
        .await;
        if taken.is_err() {
            let _ = self.env.ports.fs.remove(path).await;
        }
        taken
    }

    async fn clone_chunks(&self, path: &Path, blocks: &[Range<u64>]) -> Result<()> {
        let size = self.file.size().await?;
        let ranges: Vec<Range<u64>> = blocks
            .iter()
            .map(|r| r.start * CHUNK..(r.end * CHUNK).min(size))
            .filter(|r| r.start < r.end)
            .collect();
        Ok(self
            .env
            .ports
            .image
            .clone_ranges(&self.image, path, &ranges)
            .await?)
    }

    /// A snapshot that clones the whole image and takes the chunks whose extents moved since
    /// the last clone.
    async fn snapshot_extents(&self, v: &Volume) -> Result<usize> {
        let mut s = self.snaps.lock().await;
        let path = s.next_path(&self.image);
        let (paused, _) = self.frozen_clone(&path, None).await?;
        self.stats.paused(Pause::Clone, paused);
        let cur = match self.env.ports.image.extents(&path).await {
            Ok(c) => c,
            Err(e) => {
                let _ = self.env.ports.fs.remove(&path).await;
                return Err(e.into());
            }
        };
        let prev = s.prev.as_deref().unwrap_or_default();
        let (moved, _) = changed_chunks(prev, &cur);
        let file = self.env.ports.fs.open(&path, Open::Read).await?;
        let n = v.absorb_chunks(&file, &path, &moved).await?;
        v.flush().await?;
        self.retire_prev(&mut s);
        s.prev = Some(cur);
        s.prev_path = Some(path);
        self.stats.taken.fetch_add(1, Ordering::Relaxed);
        self.stats.chunks.fetch_add(n, Ordering::Relaxed);
        Ok(n)
    }

    /// Removes the retired clones that no unpublished flush refers to.
    pub(crate) async fn remove_retired(&self, v: &Volume) {
        let gone: Vec<PathBuf> = {
            let mut r = self.retired.lock().expect("retired lock");
            let (keep, gone) = std::mem::take(&mut *r).into_iter().partition(|p| v.referenced(p));
            *r = keep;
            gone
        };
        for p in gone {
            v.tree().cache().drop_file(&p);
            let _ = self.env.ports.fs.remove(&p).await;
        }
    }

    /// Retires clones an earlier writer left: they go once the flushes so far are published.
    pub(crate) fn retire_unpublished(&self, v: &Volume, paths: Vec<PathBuf>) {
        v.reference_until_published(&paths);
        self.retired.lock().expect("retired lock").extend(paths);
    }

    fn start_snapshots(self: &Arc<Self>, v: &Arc<Volume>) {
        if self.opts.every.is_zero() {
            return;
        }
        let (h, v) = (self.clone(), v.clone());
        *self.task.lock().expect("task lock") = Some(tokio::spawn(async move { h.snapshot_loop(v).await }));
    }

    fn stopped(&self) -> bool {
        *self.stop.borrow()
    }

    async fn written(&self) -> Option<u64> {
        let dev = self.loop_dev.lock().expect("loop lock").clone()?;
        self.env.devices.bytes_written(&dev).await.ok()
    }

    async fn snapshot_loop(self: Arc<Self>, v: Arc<Volume>) {
        let mut stop = self.stop.subscribe();
        let mut uploads = v.uploads();
        let poll = SNAPSHOT_POLL.min(self.opts.every);
        let mut last = Instant::now();
        let mut base = self.written().await;
        let (mut seen, mut changed) = (base, Instant::now());
        loop {
            tokio::select! {
                r = stop.changed() => {
                    if r.is_err() || self.stopped() {
                        return;
                    }
                    continue;
                }
                Ok(()) = uploads.changed() => {
                    self.remove_retired(&v).await;
                    continue;
                }
                () = tokio::time::sleep(poll) => {}
            }
            let now = self.written().await;
            if now != seen {
                (seen, changed) = (now, Instant::now());
            }
            let written = match (now, base) {
                (Some(w), Some(b)) if w >= b => Some(w - b),
                _ => None,
            };
            if !snapshot_due(last.elapsed(), written, changed.elapsed(), &self.opts).0 || self.stopped() {
                continue;
            }
            if let Err(e) = self.snapshot_once(&v).await {
                self.stats.failed.fetch_add(1, Ordering::Relaxed);
                *self.stats.last_error.lock().expect("stats lock") = Some(e.to_string());
            }
            last = Instant::now();
            base = now;
        }
    }

    /// Stops the snapshots and takes what changed since the last one: the chunks whose extents
    /// moved when a snapshot exists, else every chunk that differs from the root. The device
    /// must no longer write to the image.
    pub(crate) async fn absorb_final(&self, v: &Volume) -> Result<usize> {
        self.stop.send_replace(true);
        let task = self.task.lock().expect("task lock").take();
        if let Some(t) = task {
            let _ = t.await;
        }
        let mut s = self.snaps.lock().await;
        if let Some(era) = self.era()
            && let Ok(w) = self.env.devices.era_written(&era, s.since).await
        {
            let r = v.absorb_chunks(&self.file, &self.image, &chunks(&w.blocks)).await;
            self.retire_prev(&mut s);
            return r;
        }
        let r = match s.prev.as_ref().filter(|_| self.era().is_none()) {
            Some(prev) => match self.env.ports.image.extents(&self.image).await {
                Ok(cur) => {
                    v.absorb_chunks(&self.file, &self.image, &changed_chunks(prev, &cur).0)
                        .await
                }
                Err(_) => {
                    v.absorb_image(self.env.ports.image.as_ref(), &self.file, &self.image)
                        .await
                }
            },
            None => {
                v.absorb_image(self.env.ports.image.as_ref(), &self.file, &self.image)
                    .await
            }
        };
        self.retire_prev(&mut s);
        r
    }

    /// Grows the image to `size` bytes, then the loop device over it and the device-mapper
    /// device over that, between snapshots. The volume must have grown first.
    pub(crate) async fn grow(&self, size: u64) -> Result<()> {
        let _s = self.snaps.lock().await;
        if self.file.size().await? < size {
            self.file.set_len(size).await?;
        }
        let lp = self
            .loop_dev
            .lock()
            .expect("loop lock")
            .clone()
            .ok_or_else(|| crate::Error::Corrupt(format!("{}: no loop device", self.image.display())))?;
        let devices = self.env.devices.as_ref();
        devices.refresh_loop(&lp).await?;
        let Some(era) = self.era() else {
            return resize_table(devices, &self.dm, &lp, size).await;
        };
        devices.resize_era(&era, size).await?;
        let path = devices
            .era(&era)
            .await?
            .ok_or_else(|| crate::Error::Corrupt(format!("era device {era} is gone")))?
            .path;
        resize_table(devices, &self.dm, &path, size).await
    }

    /// After a checkpoint published everything: removes the era device, the image, its clones,
    /// its era records and its owner. The device must no longer map the era device.
    pub(crate) async fn remove(&self, v: &Volume) {
        if let Some(era) = self.era() {
            let _ = self.env.devices.remove(&era).await;
        }
        self.remove_retired(v).await;
        v.tree().cache().drop_file(&self.image);
        let fs = self.env.ports.fs.as_ref();
        let _ = fs.remove(&self.image).await;
        let _ = fs.remove(&era_records(&self.image)).await;
        let _ = fs.remove(&owner::path(&self.image)).await;
    }

    /// Puts era device `era_name(dm)` over loop device `lp` of the image, on a new records file,
    /// and returns it; `None` when the host cannot track eras. An era device of that name is
    /// left from an earlier run that no table maps any more, and goes first.
    async fn start_era(&self, lp: &Path, size: u64) -> Option<PathBuf> {
        let devices = self.env.devices.as_ref();
        let fs = self.env.ports.fs.as_ref();
        let name = era_name(&self.dm);
        if devices.era(&name).await.ok()?.is_some() {
            devices.remove(&name).await.ok()?;
        }
        let records = era_records(&self.image);
        let _ = fs.remove(&records).await;
        let made = async {
            fs.open(&records, Open::CreateNew)
                .await?
                .set_len(era_records_len(size))
                .await?;
            let mlp = devices.attach_loop(&records).await?;
            devices.create_era(&name, &mlp, lp, size, CHUNK).await
        }
        .await;
        match made {
            Ok(path) => {
                *self.era.lock().expect("era lock") = Some(name);
                Some(path)
            }
            Err(_) => {
                let _ = fs.remove(&records).await;
                None
            }
        }
    }
}

/// Makes device `dm` map `size` bytes of `target`, the device it maps already, without a gap
/// for the file system above.
pub(crate) async fn resize_table(
    devices: &dyn DeviceSwitch,
    dm: &str,
    target: &Path,
    size: u64,
) -> Result<()> {
    devices.load(dm, target, size).await?;
    if let Err(e) = devices.suspend(dm).await {
        let _ = devices.clear_inactive(dm).await;
        return Err(e.into());
    }
    if let Err(e) = devices.resume(dm).await {
        let _ = devices.clear_inactive(dm).await;
        let _ = devices.resume(dm).await;
        return Err(e.into());
    }
    Ok(())
}

/// Sets `v` to upload recipes of the blocks of `root`'s base image, and of learned chunks.
async fn use_dedup(env: &HostEnv, bases: &Bases, v: &Volume, root: pando_format::Entry) {
    if !env.dedup {
        return;
    }
    let base = bases.blocks_for(&v.tree(), root).await.ok().flatten();
    if base.is_some() || env.learned.is_some() {
        v.set_dedup(Some(Arc::new(Dedup {
            base,
            learned: env.learned.clone(),
        })));
    }
}

/// Copies branch `v` into a new image at `image` while it serves, then switches device `dm`
/// to the image.
pub(crate) async fn hand_off(
    env: &HostEnv,
    v: &Arc<Volume>,
    dm: &str,
    image: PathBuf,
    opts: SnapshotOptions,
) -> Result<(Arc<Handoff>, HandoffReport)> {
    let fs = env.ports.fs.as_ref();
    if let Some(dir) = image.parent() {
        fs.create_dir_all(dir).await?;
    }
    let opened = v.opened();
    let size = v.size();
    let mut shared = None;
    if let Some(bases) = env.bases.as_ref().filter(|_| !opened.is_empty()) {
        let cache = v.tree().cache().clone();
        let built = build(
            bases,
            &cache,
            v.opened_tree(),
            opened,
            v.opened_size(),
            BuildOptions {
                parallel: BUILD_PARALLEL,
                serve: true,
                after: None,
            },
        )
        .await;
        if built.is_ok() && bases.clone_to(&opened.hash, &image).await.is_ok() {
            shared = Some(bases);
        }
    }
    let file = match shared {
        Some(_) => {
            let f = fs.open(&image, Open::ReadWrite).await?;
            if f.size().await? != size {
                f.set_len(size).await?;
            }
            f
        }
        None => {
            let _ = fs.remove(&image).await;
            let f = fs.open(&image, Open::Create).await?;
            let _ = env.ports.image.prepare(&image, None).await;
            f.set_len(size).await?;
            f
        }
    };
    let h = Arc::new(Handoff::new(
        env,
        dm,
        image,
        file,
        shared.map(|_| opened.hash),
        opts,
    ));
    v.start_tracking();
    let r = copy_and_switch(&h, v, shared).await;
    v.stop_tracking();
    let mut report = r?;
    h.switched.store(true, Ordering::Release);
    v.tree().cache().stop_serving_base(BaseKey::Building(opened.hash));
    report.from_base = v.drop_base();
    h.start_snapshots(v);
    Ok((h, report))
}

async fn copy_and_switch(h: &Arc<Handoff>, v: &Arc<Volume>, shared: Option<&Bases>) -> Result<HandoffReport> {
    let mut report = HandoffReport {
        shared: shared.is_some(),
        ..HandoffReport::default()
    };
    let idxs = match shared {
        Some(bases) => {
            use_dedup(&h.env, bases, v, v.opened()).await;
            v.changed_since_opened().await?
        }
        None => v.used_chunks().await?,
    };
    report.copied = v.copy_chunks(h.file.as_ref(), &idxs, shared.is_some()).await?;
    while report.rounds < RECOPY_ROUNDS {
        let t = v.take_tracked();
        if t.len() < RECOPY_FEW {
            v.keep_tracked(&t);
            break;
        }
        report.copied += v.copy_chunks(h.file.as_ref(), &t, true).await?;
        report.rounds += 1;
    }
    h.file.sync_data().await?;
    h.owner(v, false).await?;
    let devices = h.env.devices.as_ref();
    let lp = devices.attach_loop(&h.image).await?;
    *h.loop_dev.lock().expect("loop lock") = Some(lp.clone());
    let target = h.start_era(&lp, v.size()).await.unwrap_or_else(|| lp.clone());
    report.tracked = h.tracked();
    devices.load(&h.dm, &target, v.size()).await?;
    let t0 = Instant::now();
    if let Err(e) = devices.suspend(&h.dm).await {
        let _ = devices.clear_inactive(&h.dm).await;
        return Err(e.into());
    }
    match under_suspend(h, v).await {
        Ok((last, snap)) => {
            if let Err(e) = devices.resume(&h.dm).await {
                let _ = devices.clear_inactive(&h.dm).await;
                let _ = devices.resume(&h.dm).await;
                let _ = h.owner(v, false).await;
                return Err(e.into());
            }
            report.last = last;
            report.paused = t0.elapsed();
            if let Some(p) = snap {
                let mut s = h.snaps.lock().await;
                let _ = h.set_prev(&mut s, p).await;
            }
            Ok(report)
        }
        Err(e) => {
            let _ = devices.clear_inactive(&h.dm).await;
            let _ = devices.resume(&h.dm).await;
            Err(e)
        }
    }
}

/// The steps of the switch while the device is suspended: the last copy, a flush, the owner
/// record and, without an era device, the first clone, which later snapshots compare with.
async fn under_suspend(h: &Handoff, v: &Volume) -> Result<(usize, Option<PathBuf>)> {
    let last = v.take_tracked();
    v.stop_tracking();
    let n = v.copy_chunks(h.file.as_ref(), &last, true).await?;
    h.file.sync_data().await?;
    v.flush().await?;
    h.owner(v, true).await?;
    if h.tracked() {
        return Ok((n, None));
    }
    Ok((n, h.clone_image().await.ok()))
}

#[cfg(all(test, feature = "sim"))]
mod tests;
