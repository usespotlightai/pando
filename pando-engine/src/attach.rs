//! Serving one branch on this host, from attach to detach.
//!
//! An attach opens the branch and, while it takes the writer generation, starts what the first
//! reads need: the base image of the root it opened at (served by chunk hash while it is built),
//! and the root's recording of earlier mounts' first reads. A writer that died may have left an
//! image: the attach rejoins it when the device still serves it, else adopts what it holds.
//! Once the branch is local, the handoff moves the device to an image. A detach takes back what
//! the kernel wrote to the image, publishes everything, saves the recording and removes the
//! image.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::base::{Bases, BuildOptions, BuildReport, build};
use crate::cache::{BaseKey, Cache};
use crate::dedup::learned::{LearnedIndex, Learner};
use crate::handoff::{self, Handoff, HandoffReport, HostEnv, SnapshotOptions};
use crate::ports::DeviceSwitch;
use crate::{Ports, Result, Volume, recording};

const BUILD_PARALLEL: usize = 16;

/// How a host serves its branches.
#[derive(Clone, Debug, Default)]
pub struct AttachOptions {
    /// Where handoff images go, with the host's base images in `bases` under it. Without it the
    /// branch is served by the frontend until detach.
    pub images: Option<PathBuf>,
    /// The device-mapper device the file system sits on; a handoff needs it.
    pub device: Option<String>,
    /// Start each image as a clone of a per-host base image of the root the branch opened at.
    pub share_bases: bool,
    /// Upload chunks as recipes of the base image's blocks.
    pub dedup: bool,
    /// Keep uploaded chunks in this directory, and take blocks from them in later runs.
    pub learned: Option<PathBuf>,
    pub snapshot_every: Duration,
    pub snapshot_bytes: u64,
    /// Parallel fetches for the recording of earlier first reads; zero fetches none.
    pub prefetch: usize,
    /// Record this mount's first reads for the next one.
    pub record: bool,
    /// After the first reads, fetch every chunk with this many parallel fetches; zero does not.
    pub hydrate: usize,
    /// Take over a device whose writer died while the kernel served it from an image.
    pub rejoin: bool,
}

/// What [`Attachment::warm`] did.
#[derive(Clone, Copy, Debug, Default)]
pub struct WarmReport {
    /// The base image was on this host: no warming.
    pub base_ready: bool,
    pub recorded_frames: usize,
    pub base: Option<BuildReport>,
    pub hydrated_frames: usize,
}

/// What [`Attachment::finish`] did.
#[derive(Clone, Copy, Debug, Default)]
pub struct FinishReport {
    /// Chunks taken back from the image.
    pub absorbed: usize,
    /// Chunks the saved recording lists.
    pub recorded: usize,
}

/// What the background snapshots of a handoff did.
#[derive(Clone, Debug, Default)]
pub struct SnapshotCounts {
    pub taken: usize,
    /// Chunks they took back from the image.
    pub chunks: usize,
    pub failed: usize,
    pub last_error: Option<String>,
}

/// One branch served on this host.
#[derive(Debug)]
pub struct Attachment {
    ports: Ports,
    devices: Option<Arc<dyn DeviceSwitch>>,
    cache: Arc<Cache>,
    volume: Arc<Volume>,
    opts: AttachOptions,
    bases: Option<Bases>,
    build: Mutex<Option<JoinHandle<Result<BuildReport>>>>,
    touched: watch::Sender<bool>,
    warmed: watch::Sender<bool>,
    handoff: Mutex<Option<Arc<Handoff>>>,
    learner: Mutex<Option<Arc<Learner>>>,
    base_ready: bool,
}

impl Attachment {
    /// Opens branch `name`, with its local state in `dir`. `devices` is the host's device switch,
    /// which a handoff needs.
    pub async fn open(
        ports: &Ports,
        devices: Option<Arc<dyn DeviceSwitch>>,
        cache: Arc<Cache>,
        dir: &Path,
        name: &str,
        opts: AttachOptions,
    ) -> Result<Attachment> {
        let v = Arc::new(Volume::open_then(ports, cache.clone(), dir, name).await?);
        let bases = opts
            .images
            .as_ref()
            .filter(|_| opts.share_bases && opts.device.is_some())
            .map(|d| Bases::new(ports, &d.join("bases")));
        let touched = watch::Sender::new(false);
        let opened = v.opened();
        let mut a = Attachment {
            ports: ports.clone(),
            devices,
            cache: cache.clone(),
            volume: v.clone(),
            opts,
            bases,
            build: Mutex::default(),
            touched,
            warmed: watch::Sender::new(false),
            handoff: Mutex::default(),
            learner: Mutex::default(),
            base_ready: false,
        };
        if let Some(bases) = a.bases.clone().filter(|_| !opened.is_empty()) {
            if bases.has(&opened.hash).await {
                a.base_ready = true;
            } else {
                let (cache, tree, after) = (cache.clone(), v.tree().clone(), Some(a.touched.subscribe()));
                let size = v.size();
                *a.build.lock().expect("build lock") = Some(tokio::spawn(async move {
                    let opts = BuildOptions {
                        parallel: BUILD_PARALLEL,
                        serve: true,
                        after,
                    };
                    build(&bases, &cache, &tree, opened, size, opts).await
                }));
            }
        }
        if let Err(e) = a.open_rest().await {
            a.touched.send_replace(true);
            a.volume.close().await;
            return Err(e);
        }
        Ok(a)
    }

    async fn open_rest(&mut self) -> Result<()> {
        let v = self.volume.clone();
        v.wait_generation().await?;
        if let (Some(images), Some(from)) = (&self.opts.images, v.copied_from()) {
            let _ = Bases::new(&self.ports, &images.join("bases"))
                .note_follow(from)
                .await;
        }
        if let (Some(images), Some(dm)) = (self.opts.images.clone(), self.opts.device.clone()) {
            let env = self.env(None);
            if self.opts.rejoin
                && let Some(h) =
                    handoff::adopt::rejoin(&env, &v, &dm, &images, self.snapshot_options()).await?
            {
                *self.handoff.lock().expect("handoff lock") = Some(h);
                self.warmed.send_replace(true);
                return Ok(());
            }
            handoff::adopt::adopt_leftovers(&env, &v, &images).await?;
        }
        if self.base_ready
            && let Some(bases) = &self.bases
        {
            bases.load_nodes(v.tree(), &bases.path(&v.opened().hash)).await?;
            v.use_base(bases).await?;
        }
        Ok(())
    }

    fn snapshot_options(&self) -> SnapshotOptions {
        SnapshotOptions {
            every: self.opts.snapshot_every,
            burst: self.opts.snapshot_bytes,
        }
    }

    fn env(&self, learned: Option<Arc<LearnedIndex>>) -> HostEnv {
        HostEnv {
            ports: self.ports.clone(),
            devices: self.devices.clone().unwrap_or_else(|| Arc::new(NoDevices)),
            bases: self.bases.clone(),
            dedup: self.opts.dedup,
            learned,
        }
    }

    pub fn volume(&self) -> &Arc<Volume> {
        &self.volume
    }

    /// Whether the kernel already serves the branch from an image a dead writer left, so no
    /// frontend is needed.
    pub fn rejoined(&self) -> bool {
        self.handoff.lock().expect("handoff lock").is_some() && *self.warmed.borrow()
    }

    /// Brings what the first reads need to this host: the recording of earlier mounts' first
    /// reads, then the base image or, without shared bases, every chunk when hydration is on.
    pub async fn warm(&self) -> Result<WarmReport> {
        let mut r = WarmReport {
            base_ready: self.base_ready,
            ..WarmReport::default()
        };
        if *self.warmed.borrow() || self.base_ready {
            self.touched.send_replace(true);
            self.warmed.send_replace(true);
            return Ok(r);
        }
        let v = &self.volume;
        if self.opts.prefetch > 0 && self.opts.record {
            let rec = recording::load(&self.cache, &v.opened()).await;
            if !rec.entries.is_empty() {
                r.recorded_frames = recording::prefetch(&self.cache, &rec, self.opts.prefetch).await;
            }
        }
        self.touched.send_replace(true);
        let build = self.build.lock().expect("build lock").take();
        let built = match build {
            Some(b) => b.await.ok().and_then(|r| r.ok()),
            None => None,
        };
        r.base = built;
        if built.is_none() && self.opts.hydrate > 0 {
            r.hydrated_frames = v.tree().hydrate(&v.root(), self.opts.hydrate).await?;
        }
        self.warmed.send_replace(true);
        Ok(r)
    }

    /// Once [`Self::warm`] is done, moves the device to a local image of the branch.
    pub async fn hand_off(&self) -> Result<HandoffReport> {
        let (Some(images), Some(dm)) = (self.opts.images.clone(), self.opts.device.clone()) else {
            return Err(crate::Error::Corrupt(
                "a handoff needs an image directory and a device".into(),
            ));
        };
        let _ = self.warmed.subscribe().wait_for(|w| *w).await;
        let _hold = self.cache.hold_writeback();
        let learned = self.learn().await;
        let env = self.env(learned);
        let image = handoff::image_path(&self.ports, &images, self.volume.name());
        let (h, report) = handoff::hand_off(&env, &self.volume, &dm, image, self.snapshot_options()).await?;
        *self.handoff.lock().expect("handoff lock") = Some(h);
        Ok(report)
    }

    /// Loads the chunks earlier runs on this host uploaded, and starts keeping what this branch
    /// uploads.
    async fn learn(&self) -> Option<Arc<LearnedIndex>> {
        let dir = self
            .opts
            .learned
            .as_ref()
            .filter(|_| self.opts.dedup && self.bases.is_some())?;
        let p = &self.ports;
        let li = LearnedIndex::load(p.fs.clone(), p.cpu.clone(), p.clock.clone(), dir)
            .await
            .ok()?;
        if let Ok(l) = Learner::new(p.fs.clone(), p.clock.clone(), dir, self.volume.name()).await {
            let l = Arc::new(l);
            self.volume.set_learner(Some(l.clone()));
            *self.learner.lock().expect("learner lock") = Some(l);
        }
        Some(Arc::new(li))
    }

    /// What the background snapshots of the handoff did so far.
    pub fn snapshots(&self) -> Option<SnapshotCounts> {
        let h = self.handoff.lock().expect("handoff lock").clone()?;
        let load = |a: &std::sync::atomic::AtomicUsize| a.load(std::sync::atomic::Ordering::Relaxed);
        Some(SnapshotCounts {
            taken: load(&h.stats.taken),
            chunks: load(&h.stats.chunks),
            failed: load(&h.stats.failed),
            last_error: h.stats.last_error.lock().expect("stats lock").clone(),
        })
    }

    /// Publishes what the branch holds and frees it. The device must no longer write to the
    /// image: the caller removed it first.
    pub async fn finish(self) -> Result<FinishReport> {
        let v = &self.volume;
        let mut report = FinishReport::default();
        let h = self.handoff.lock().expect("handoff lock").clone();
        let mut done = Ok(());
        if let Some(h) = h.as_ref().filter(|h| h.switched()) {
            match h.absorb_final(v).await {
                Ok(n) => report.absorbed = n,
                Err(e) => done = Err(e),
            }
        }
        if done.is_ok() {
            done = v.checkpoint().await;
        }
        let learner = self.learner.lock().expect("learner lock").take();
        if let Some(l) = learner {
            if done.is_ok() {
                let _ = l.save().await;
            } else {
                l.discard().await;
            }
        }
        if let Some(learned) = v.dedup().and_then(|d| d.learned.clone()) {
            let _ = learned.save_uses().await;
        }
        self.cache.stop_serving_base(BaseKey::Building(v.opened().hash));
        v.drop_base();
        if let Some(h) = &h
            && done.is_ok()
        {
            h.remove(v).await;
        }
        if done.is_ok() && self.opts.record {
            report.recorded = recording::save(&self.cache, &v.opened(), &v.root())
                .await
                .unwrap_or(0);
        }
        v.close().await;
        done.map(|()| report)
    }
}

/// The device switch of a host that has none: every call fails.
#[derive(Debug)]
struct NoDevices;

#[async_trait::async_trait]
impl DeviceSwitch for NoDevices {
    async fn create(&self, _: &str, _: &Path, _: u64) -> std::io::Result<()> {
        Err(no_devices())
    }
    async fn remove(&self, _: &str) -> std::io::Result<()> {
        Err(no_devices())
    }
    async fn live_target(&self, _: &str) -> std::io::Result<Option<PathBuf>> {
        Ok(None)
    }
    async fn load(&self, _: &str, _: &Path, _: u64) -> std::io::Result<()> {
        Err(no_devices())
    }
    async fn clear_inactive(&self, _: &str) -> std::io::Result<()> {
        Err(no_devices())
    }
    async fn suspend(&self, _: &str) -> std::io::Result<()> {
        Err(no_devices())
    }
    async fn resume(&self, _: &str) -> std::io::Result<()> {
        Err(no_devices())
    }
    async fn attach_loop(&self, _: &Path) -> std::io::Result<PathBuf> {
        Err(no_devices())
    }
    async fn loop_image(&self, _: &Path) -> std::io::Result<Option<PathBuf>> {
        Ok(None)
    }
    async fn bytes_written(&self, _: &Path) -> std::io::Result<u64> {
        Err(no_devices())
    }
}

fn no_devices() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Unsupported, "this host has no device switch")
}

#[cfg(all(test, feature = "sim"))]
mod tests {
    use std::io;

    use async_trait::async_trait;
    use pando_format::CHUNK_SIZE;

    use super::*;
    use crate::sim::{self, SimBlock, SimDevices};

    const SIZE: u64 = 2 << 20;
    const FRONT: &str = "/dev/ublkb0";

    struct Front(Arc<Volume>);

    #[async_trait]
    impl SimBlock for Front {
        async fn read(&self, buf: &mut [u8], off: u64) -> io::Result<()> {
            self.0.read_at(buf, off).await.map_err(io::Error::other)
        }
        async fn write(&self, b: &[u8], off: u64) -> io::Result<()> {
            self.0.write_at(b, off).await.map_err(io::Error::other)
        }
    }

    fn opts() -> AttachOptions {
        AttachOptions {
            images: Some(PathBuf::from("/img")),
            device: Some("dm0".into()),
            share_bases: true,
            dedup: true,
            learned: Some(PathBuf::from("/img/learned")),
            prefetch: 8,
            record: true,
            ..AttachOptions::default()
        }
    }

    async fn run(
        ports: &Ports,
        devices: &SimDevices,
        cache: &Arc<Cache>,
        name: &str,
        at: u64,
        fill: u8,
    ) -> (WarmReport, HandoffReport) {
        let a = Attachment::open(
            ports,
            Some(Arc::new(devices.clone())),
            cache.clone(),
            Path::new("/v"),
            name,
            opts(),
        )
        .await
        .unwrap();
        devices.add_block(Path::new(FRONT), Arc::new(Front(a.volume().clone())));
        devices.create("dm0", Path::new(FRONT), SIZE).await.unwrap();
        let mut b = vec![0; 100];
        devices.read("dm0", &mut b, 0).await.unwrap();
        let w = a.warm().await.unwrap();
        let h = a.hand_off().await.unwrap();
        devices
            .write("dm0", &vec![fill; 4096], at * CHUNK_SIZE as u64)
            .await
            .unwrap();
        devices.remove("dm0").await.unwrap();
        devices.remove_block(Path::new(FRONT));
        let f = a.finish().await.unwrap();
        assert_eq!(f.absorbed, 1);
        (w, h)
    }

    #[tokio::test(start_paused = true)]
    async fn an_attach_warms_hands_off_and_publishes_and_the_next_one_starts_on_the_base() {
        let (ports, _, fs) = sim::ports(11);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let w = Arc::new(Cache::open(&ports, Path::new("/w")).await.unwrap());
        let v = Volume::open(&ports, w, Path::new("/wv"), "main").await.unwrap();
        v.write_at(&vec![5; SIZE as usize], 0).await.unwrap();
        v.checkpoint().await.unwrap();
        v.close().await;
        crate::branch::copy(&ports, "main", "fork", None).await.unwrap();

        let devices = SimDevices::new((*fs).clone());
        let cache = Arc::new(Cache::open(&ports, Path::new("/c")).await.unwrap());
        let (w1, h1) = run(&ports, &devices, &cache, "fork", 3, 0x33).await;
        assert!(!w1.base_ready);
        assert!(w1.base.is_some_and(|b| b.built));
        assert!(h1.shared);
        assert!(
            ports
                .fs
                .open(Path::new("/img/bases/follow/main"), crate::ports::Open::Read)
                .await
                .is_ok()
        );

        crate::branch::copy(&ports, "main", "fork2", None).await.unwrap();
        let (w2, h2) = run(&ports, &devices, &cache, "fork2", 4, 0x44).await;
        assert!(
            w2.base_ready,
            "the first attach left the base image of the same root"
        );
        assert!(h2.shared);

        let c = Arc::new(Cache::open(&ports, Path::new("/fresh")).await.unwrap());
        let v = Volume::open(&ports, c, Path::new("/fv"), "fork").await.unwrap();
        let mut got = vec![0; SIZE as usize];
        v.read_at(&mut got, 0).await.unwrap();
        let mut want = vec![5; SIZE as usize];
        want[3 * CHUNK_SIZE..3 * CHUNK_SIZE + 4096].fill(0x33);
        assert_eq!(got, want);
        v.close().await;
    }
}
