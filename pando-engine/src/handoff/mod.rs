//! A handoff takes Pando out of the I/O path once a branch is local.
//!
//! The file system sits on a device-mapper device whose table first points at the branch's
//! frontend device. The handoff copies the branch into a local image while Pando keeps serving
//! it, then suspends the device, copies the chunks written meanwhile, flushes, points the table
//! at a loop device over the image and resumes. From then on the kernel serves every read and
//! write from the image.
//!
//! While the run goes on, background snapshots clone the image during a short suspend, every
//! interval or early after a burst of writes. The chunks whose extents differ from the previous
//! clone changed; they are taken back as writes and uploaded. At detach the handoff takes what
//! changed since the last snapshot the same way. A flush records each taken chunk as a
//! reference into its clone, so a clone stays until an upload has published those chunks.
//!
//! With shared bases, the image starts as a clone of the base image of the root the branch
//! opened at, and only the chunks that differ from it are copied.

pub(crate) mod adopt;
mod extents;
mod owner;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pando_format::Hash;
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use extents::{changed_chunks, snapshot_due};
use owner::Owner;

use crate::base::{Bases, BuildOptions, build};
use crate::cache::BaseKey;
use crate::dedup::Dedup;
use crate::dedup::learned::LearnedIndex;
use crate::ports::{DeviceSwitch, Extent, LocalFile, Open};
use crate::{Ports, Result, Volume};

/// How often the snapshot loop checks the bytes written to the image.
const SNAPSHOT_POLL: Duration = Duration::from_millis(100);
/// Rounds of copying the chunks written during the copy before the suspend.
const RECOPY_ROUNDS: usize = 4;
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
}

/// What the background snapshots did.
#[derive(Debug, Default)]
pub(crate) struct SnapshotStats {
    pub(crate) taken: AtomicUsize,
    pub(crate) chunks: AtomicUsize,
    pub(crate) failed: AtomicUsize,
    pub(crate) last_error: Mutex<Option<String>>,
}

#[derive(Debug, Default)]
struct Snaps {
    /// The extents the next snapshot compares with: of the last clone, or of the base image
    /// the image was cloned from.
    prev: Option<Vec<Extent>>,
    /// The last clone, which goes once a later one replaces it and its chunks are published.
    prev_path: Option<PathBuf>,
    next: u32,
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
            snaps: AsyncMutex::default(),
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
    /// write and none in flight.
    async fn frozen_clone(&self, path: &Path) -> Result<Duration> {
        let t0 = Instant::now();
        self.env.devices.suspend(&self.dm).await?;
        let cloned = self.clone_to(path).await;
        let resumed = self.env.devices.resume(&self.dm).await;
        let paused = t0.elapsed();
        if let Err(e) = resumed {
            let _ = self.env.ports.fs.remove(path).await;
            return Err(e.into());
        }
        cloned.map(|()| paused)
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

    /// Takes one snapshot: clones the image under suspend and takes the chunks whose extents
    /// moved since the last clone as writes, then flushes them.
    pub(crate) async fn snapshot_once(&self, v: &Volume) -> Result<usize> {
        let mut s = self.snaps.lock().await;
        let path = s.next_path(&self.image);
        self.frozen_clone(&path).await?;
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
            let written = match (now, base) {
                (Some(w), Some(b)) if w >= b => Some(w - b),
                _ => None,
            };
            if !snapshot_due(last.elapsed(), written, self.opts.every, self.opts.burst).0 || self.stopped() {
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
        let r = match &s.prev {
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

    /// After a checkpoint published everything: removes the image, its clones and its owner.
    pub(crate) async fn remove(&self, v: &Volume) {
        self.remove_retired(v).await;
        v.tree().cache().drop_file(&self.image);
        let fs = self.env.ports.fs.as_ref();
        let _ = fs.remove(&self.image).await;
        let _ = fs.remove(&owner::path(&self.image)).await;
    }
}

/// Sets `v` to upload recipes of the blocks of `root`'s base image, and of learned chunks.
async fn use_dedup(env: &HostEnv, bases: &Bases, v: &Volume, root: pando_format::Entry) {
    if !env.dedup {
        return;
    }
    let base = bases.blocks_for(v.tree(), root).await.ok().flatten();
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
            v.tree(),
            opened,
            size,
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
        Some(_) => fs.open(&image, Open::ReadWrite).await?,
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
            v.changed_since(&v.opened()).await?
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
    devices.load(&h.dm, &lp, v.size()).await?;
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
/// record and the first clone, which later snapshots compare with.
async fn under_suspend(h: &Handoff, v: &Volume) -> Result<(usize, Option<PathBuf>)> {
    let last = v.take_tracked();
    v.stop_tracking();
    let n = v.copy_chunks(h.file.as_ref(), &last, true).await?;
    h.file.sync_data().await?;
    v.flush().await?;
    h.owner(v, true).await?;
    Ok((n, h.clone_image().await.ok()))
}

#[cfg(all(test, feature = "sim"))]
mod tests;
