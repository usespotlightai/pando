//! Crashes across a branch's life on a host that serves it through device-mapper and hands it
//! off to a local image, as Spotlight runs it: the attach, the base image it builds in the
//! background, the handoff while the kernel writes, background snapshots, drains and the
//! detach, then the attaches after a crash that rejoin or adopt what the dead writer left; and
//! the follower building the base images of new roots.
//!
//! Each process runs in a runtime of its own. A crash stops it after a chosen number of changes
//! to the host's files: from then on none of its device or store calls returns. Then either the
//! process dies and the host goes on, keeping its devices and every write in the page cache, or
//! the host loses power, losing its devices and every write not yet durable. The next process
//! starts as `pando attach` does after each: it rejoins after a kill, and adopts after a reboot.
//!
//! Once a process detaches, the branch read from a fresh host holds in each block a write the
//! kernel could see there: the last one that returned, one still in flight, or after a power
//! cut one since the kernel's last flush. No image of the branch, era device or device stays
//! behind, and every base image under its final name holds its root whole.
//!
//! `PANDO_DST_TRACE` prints what each process did. `PANDO_DST_PLAN`, the change counts after
//! which each process of [`attach_crashes`] stops, comma separated with `-` for one that runs
//! to its end, replaces the counts the seed picks.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use pando_engine::base::Follower;
use pando_engine::branch;
use pando_engine::ports::{
    Clock, DeviceSwitch, LocalFs, Object, ObjectInfo, Open, Rng, Store, StoreError, Version,
};
use pando_engine::sim::{InlineCpu, SimBlock, SimClock, SimDevices, SimFs, SimRng, SimStore};
use pando_engine::{AttachOptions, Attachment, Cache, Ports, Volume};
use pando_format::{u64_from, usize_from};
use parking_lot::Mutex;
use tokio::time::Instant;

const BLOCK: usize = 4096;
const SIZE: u64 = 2 << 20;
const DM: &str = "dm0";
const FRONT: &str = "/dev/ublkb0";
const IMAGES: &str = "/img";
const BASES: &str = "/img/bases";
const STATE: &str = "/v";
const CACHE: &str = "/c";
const BASE: &str = "main";
const BRANCH: &str = "run";
/// The most processes one seed runs; the last one never crashes.
const LIVES: usize = 4;
/// A process that runs longer than this in simulated time hangs.
const LIFE_MAX: Duration = Duration::from_hours(1);

/// What the seeds of a scenario did.
#[derive(Debug, Default, Clone, Copy)]
pub struct Report {
    /// Processes that ran.
    pub lives: usize,
    /// Processes killed while the host went on.
    pub kills: usize,
    /// Power cuts.
    pub power_cuts: usize,
    /// Attaches that rejoined an image the kernel still served.
    pub rejoins: usize,
    /// Handoffs that moved the device to an image.
    pub handoffs: usize,
    /// Base images built.
    pub bases: usize,
    /// Writes through the device.
    pub writes: usize,
}

/// Wall time that moves with each process's `tokio::time` and goes on from where the last
/// process left it, so the host's clock never goes back.
#[derive(Debug, Default)]
struct HostClock {
    at: Mutex<(Duration, Option<Instant>)>,
}

impl HostClock {
    fn start(&self) {
        *self.at.lock() = (self.since_epoch(), Some(Instant::now()));
    }

    fn stop(&self) {
        *self.at.lock() = (self.since_epoch(), None);
    }

    fn since_epoch(&self) -> Duration {
        let (carried, started) = *self.at.lock();
        carried + started.map_or(Duration::ZERO, |s| s.elapsed())
    }
}

impl Clock for HostClock {
    fn wall(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + SimClock::EPOCH + self.since_epoch()
    }
}

/// The store as the host's processes see it: once the host's files stopped, no call returns,
/// whether or not it reached the store.
#[derive(Debug)]
struct Halting {
    store: Arc<SimStore>,
    fs: Arc<SimFs>,
}

impl Halting {
    async fn alive(&self) {
        if self.fs.stopped() {
            std::future::pending::<()>().await;
        }
    }

    async fn call<T>(&self, f: impl Future<Output = T>) -> T {
        self.alive().await;
        let r = f.await;
        self.alive().await;
        r
    }
}

#[async_trait]
impl Store for Halting {
    async fn get(&self, key: &str) -> Result<Object, StoreError> {
        self.call(self.store.get(key)).await
    }
    async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Bytes, StoreError> {
        self.call(self.store.get_range(key, range)).await
    }
    async fn get_tail(&self, key: &str, n: u64) -> Result<(Bytes, u64), StoreError> {
        self.call(self.store.get_tail(key, n)).await
    }
    async fn put(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
        self.call(self.store.put(key, body)).await
    }
    async fn create(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
        self.call(self.store.create(key, body)).await
    }
    async fn update(&self, key: &str, body: Bytes, expected: &Version) -> Result<Version, StoreError> {
        self.call(self.store.update(key, body, expected)).await
    }
    async fn list(&self, dir: &str) -> Result<Vec<ObjectInfo>, StoreError> {
        self.call(self.store.list(dir)).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.call(self.store.delete(key)).await
    }
    async fn delete_if(&self, key: &str, expected: &Version) -> Result<(), StoreError> {
        self.call(self.store.delete_if(key, expected)).await
    }
    async fn keeps_versions(&self) -> Result<bool, StoreError> {
        self.call(self.store.keeps_versions()).await
    }
}

/// The branch's frontend: the ublk device the volume serves.
struct Front(Arc<Volume>);

#[async_trait]
impl SimBlock for Front {
    async fn read(&self, buf: &mut [u8], off: u64) -> std::io::Result<()> {
        self.0.read_at(buf, off).await.map_err(std::io::Error::other)
    }
    async fn write(&self, b: &[u8], off: u64) -> std::io::Result<()> {
        self.0.write_at(b, off).await.map_err(std::io::Error::other)
    }
    async fn flush(&self) -> std::io::Result<()> {
        self.0.flush().await.map_err(std::io::Error::other)
    }
}

/// A block that write `id` fills: `id` over and over, so zeros are write 0, the empty block.
fn block_of(id: u64) -> Vec<u8> {
    id.to_le_bytes().repeat(BLOCK / 8)
}

/// The write that filled `b`, or `None` when it holds parts of several.
fn id_of(b: &[u8]) -> Option<u64> {
    let first = u64::from_le_bytes(b.get(..8)?.try_into().ok()?);
    b.as_chunks::<8>()
        .0
        .iter()
        .all(|w| *w == first.to_le_bytes())
        .then_some(first)
}

/// The writes each block may hold.
#[derive(Clone, Debug)]
struct Held {
    /// What a read may see now.
    now: Vec<u64>,
    /// What a power cut may leave.
    durable: Vec<u64>,
}

fn add(v: &mut Vec<u64>, id: u64) {
    if !v.contains(&id) {
        v.push(id);
    }
}

/// What the kernel wrote to the branch and what each block may hold.
#[derive(Debug)]
struct Model {
    blocks: Vec<Held>,
    next: u64,
    in_flight: Option<(usize, u64)>,
}

impl Model {
    fn new(base: &[u64]) -> Model {
        Model {
            blocks: base
                .iter()
                .map(|&id| Held {
                    now: vec![id],
                    durable: vec![id],
                })
                .collect(),
            next: base.iter().max().copied().unwrap_or(0) + 1,
            in_flight: None,
        }
    }

    fn begin(&mut self, b: usize) -> u64 {
        let id = self.next;
        self.next += 1;
        add(&mut self.blocks[b].durable, id);
        self.in_flight = Some((b, id));
        id
    }

    fn end(&mut self, returned: bool) {
        if let Some((b, id)) = self.in_flight.take() {
            let h = &mut self.blocks[b];
            if returned {
                h.now = vec![id];
            } else {
                add(&mut h.now, id);
            }
        }
    }

    fn flushed(&mut self) {
        for h in &mut self.blocks {
            h.durable.clone_from(&h.now);
        }
    }

    fn power_cut(&mut self) {
        self.end(false);
        for h in &mut self.blocks {
            h.now.clone_from(&h.durable);
        }
    }

    fn check(&self, b: usize, data: &[u8], when: &str) -> Result<(), String> {
        let want = &self.blocks[b].now;
        match id_of(data) {
            Some(id) if want.contains(&id) => Ok(()),
            got => Err(format!(
                "{when}: block {b} holds write {got:?}, not one of {want:?}"
            )),
        }
    }
}

/// One host: its ports, its devices, and the processes' options.
struct Host {
    ports: Ports,
    fs: Arc<SimFs>,
    clock: Arc<HostClock>,
    devices: SimDevices,
    rng: SimRng,
    opts: AttachOptions,
    model: Mutex<Model>,
    /// The blocks of each root of `main` the host may have a base image of.
    roots: Mutex<Vec<(String, Vec<u64>)>>,
    report: Mutex<Report>,
}

impl Host {
    fn new(seed: u64) -> Host {
        let clock = Arc::new(HostClock::default());
        let store = Arc::new(SimStore::new(Arc::<HostClock>::clone(&clock)));
        let fs = Arc::new(SimFs::default());
        let ports = Ports {
            store: Arc::new(Halting {
                store: Arc::clone(&store),
                fs: Arc::clone(&fs),
            }),
            fs: Arc::<SimFs>::clone(&fs),
            image: Arc::<SimFs>::clone(&fs),
            clock: Arc::<HostClock>::clone(&clock),
            rng: Arc::new(SimRng::new(seed)),
            cpu: Arc::new(InlineCpu),
        };
        let rng = SimRng::new(seed ^ 0x4a7d_0ff0);
        let devices = if seed.is_multiple_of(3) {
            SimDevices::without_eras((*fs).clone())
        } else {
            SimDevices::new((*fs).clone())
        };
        let share = seed % 4 != 1;
        let dedup = share && rng.next_u64().is_multiple_of(2);
        let pick = |xs: &[u64]| xs[usize_from(rng.next_u64() % u64_from(xs.len()))];
        let opts = AttachOptions {
            images: Some(PathBuf::from(IMAGES)),
            device: Some(DM.into()),
            share_bases: share,
            dedup,
            learned: (dedup && rng.next_u64().is_multiple_of(2)).then(|| PathBuf::from("/img/learned")),
            snapshot_every: Duration::from_secs(pick(&[0, 2, 5])),
            snapshot_bytes: pick(&[0, 256 << 10]),
            snapshot_idle: Duration::from_millis(pick(&[0, 1000])),
            snapshot_copy_max: pick(&[0, 128 << 10]),
            ..AttachOptions::default()
        };
        Host {
            ports,
            fs,
            clock,
            devices,
            rng,
            opts,
            model: Mutex::new(Model::new(&[])),
            roots: Mutex::default(),
            report: Mutex::default(),
        }
    }

    fn below(&self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        usize_from(self.rng.next_u64() % u64_from(n))
    }

    fn blocks() -> usize {
        usize_from(SIZE) / BLOCK
    }

    /// Runs `f` as one process. With `stop`, the process stops after that many changes to the
    /// host's files. Returns whether it ran to its end.
    fn process<'a>(
        &'a self,
        stop: Option<u64>,
        f: impl Future<Output = Result<(), String>> + 'a,
    ) -> Result<bool, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        self.fs.stop_after(stop);
        self.report.lock().lives += 1;
        let fs = &self.fs;
        let out = rt.block_on(async {
            self.clock.start();
            let stopped = async {
                while !fs.stopped() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            let r = tokio::select! {
                biased;
                () = stopped => Ok(false),
                r = f => match r {
                    _ if fs.stopped() => Ok(false),
                    Ok(()) => Ok(true),
                    Err(e) => Err(e),
                },
                () = tokio::time::sleep(LIFE_MAX) => Err(format!("the process hung for {LIFE_MAX:?}")),
            };
            self.clock.stop();
            r
        });
        drop(rt);
        self.devices.remove_block(Path::new(FRONT));
        out
    }

    /// After a process stopped: it dies and the host goes on, or the host loses power. Returns
    /// whether the next attach rejoins.
    fn crash(&self) -> bool {
        let kill = self.below(2) == 0;
        if std::env::var_os("PANDO_DST_TRACE").is_some() {
            eprintln!(
                "crash: {}",
                if kill {
                    "the process dies"
                } else {
                    "the host loses power"
                }
            );
        }
        if kill {
            self.fs.end_process();
            self.model.lock().end(false);
            self.report.lock().kills += 1;
            return true;
        }
        self.fs.crash();
        self.devices.power_cut();
        self.model.lock().power_cut();
        self.report.lock().power_cuts += 1;
        false
    }

    /// Writes `main` on another host, so it moves to a new root, and notes the root's blocks.
    async fn write_base(&self, n: usize) -> Result<(), String> {
        let e = |what: &'static str| move |err: pando_engine::Error| format!("{what}: {err}");
        let c = Arc::new(
            Cache::open(&self.ports, Path::new("/w-cache"))
                .await
                .map_err(e("writer cache"))?,
        );
        let v = Volume::open(&self.ports, c, Path::new("/w-state"), BASE)
            .await
            .map_err(e("writer open"))?;
        let mut blocks = self
            .roots
            .lock()
            .last()
            .map_or_else(|| vec![0; Host::blocks()], |r| r.1.clone());
        let next = blocks.iter().max().copied().unwrap_or(0) + 1;
        for (i, id) in (next..).take(n).enumerate() {
            let b = (self.below(Host::blocks()) + i) % Host::blocks();
            v.write_at(&block_of(id), u64_from(b * BLOCK))
                .await
                .map_err(e("writer write"))?;
            blocks[b] = id;
        }
        v.checkpoint().await.map_err(e("writer checkpoint"))?;
        let root = v.root().hash.to_string();
        v.close().await;
        self.roots.lock().push((root, blocks));
        Ok(())
    }

    /// Makes `main`, its first root, and the branch as a copy of it.
    fn setup(&self) -> Result<(), String> {
        self.process(None, async {
            Volume::create(&self.ports, BASE, SIZE)
                .await
                .map_err(|e| format!("create: {e}"))?;
            self.write_base(Host::blocks() / 2).await?;
            branch::copy(&self.ports, BASE, BRANCH, None)
                .await
                .map_err(|e| format!("copy: {e}"))
        })?;
        let base = self.roots.lock()[0].1.clone();
        *self.model.lock() = Model::new(&base);
        Ok(())
    }

    async fn write(&self) -> Result<(), String> {
        let b = self.below(Host::blocks());
        let id = self.model.lock().begin(b);
        let r = self.devices.write(DM, &block_of(id), u64_from(b * BLOCK)).await;
        self.model.lock().end(r.is_ok());
        self.report.lock().writes += 1;
        r.map_err(|e| format!("write of block {b}: {e}"))
    }

    async fn flush(&self) -> Result<(), String> {
        self.devices.flush(DM).await.map_err(|e| format!("flush: {e}"))?;
        self.model.lock().flushed();
        Ok(())
    }

    async fn read(&self) -> Result<(), String> {
        let b = self.below(Host::blocks());
        let mut got = vec![0; BLOCK];
        self.devices
            .read(DM, &mut got, u64_from(b * BLOCK))
            .await
            .map_err(|e| format!("read of block {b}: {e}"))?;
        self.model.lock().check(b, &got, "a read through the device")
    }

    /// The kernel's I/O: `n` writes, flushes and reads through the device.
    async fn kernel(&self, n: usize) -> Result<(), String> {
        for _ in 0..n {
            match self.below(20) {
                0..12 => self.write().await?,
                12..15 => self.flush().await?,
                _ => self.read().await?,
            }
        }
        Ok(())
    }

    /// One process of the branch on this host, from attach to detach.
    async fn life(&self, rejoin: bool, first: bool) -> Result<(), String> {
        let e = |what: &'static str| move |err: pando_engine::Error| format!("{what}: {err}");
        let io = |what: &'static str| move |err: std::io::Error| format!("{what}: {err}");
        let cache = Arc::new(
            Cache::open(&self.ports, Path::new(CACHE))
                .await
                .map_err(e("cache"))?,
        );
        let opts = AttachOptions {
            rejoin,
            ..self.opts.clone()
        };
        let devices: Arc<dyn DeviceSwitch> = Arc::new(self.devices.clone());
        let a = Attachment::open(&self.ports, Some(devices), cache, Path::new(STATE), BRANCH, opts)
            .await
            .map_err(e("attach"))?;
        if std::env::var_os("PANDO_DST_TRACE").is_some() {
            let files: Vec<String> = self
                .fs
                .list(Path::new(IMAGES))
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|d| d.name)
                .collect();
            eprintln!(
                "attached: rejoined {}, {IMAGES} holds {files:?}, devices {:?}",
                a.rejoined(),
                self.devices.names()
            );
        }
        if a.rejoined() {
            self.report.lock().rejoins += 1;
        } else {
            self.devices
                .add_block(Path::new(FRONT), Arc::new(Front(Arc::clone(a.volume()))));
            if self
                .devices
                .live_target(DM)
                .await
                .map_err(io("device"))?
                .is_none()
            {
                self.devices
                    .create(DM, Path::new(FRONT), SIZE)
                    .await
                    .map_err(io("create device"))?;
            }
        }
        self.kernel(self.below(6)).await?;
        if !a.rejoined() {
            let w = a.warm().await.map_err(e("warm"))?;
            if w.base.is_some_and(|b| b.built) {
                self.report.lock().bases += 1;
            }
            if first || self.below(2) == 0 {
                let (h, k) = futures::join!(a.hand_off(), self.kernel(self.below(10)));
                h.map_err(e("hand off"))?;
                k?;
                if std::env::var_os("PANDO_DST_TRACE").is_some() {
                    eprintln!("handed off");
                }
                self.report.lock().handoffs += 1;
            }
        }
        for _ in 0..self.below(5) {
            self.kernel(self.below(8)).await?;
            let ms = u64_from(self.below(8000));
            tokio::time::sleep(Duration::from_millis(ms)).await;
            if self.below(4) == 0 {
                trace("drain");
                a.drain().await.map_err(e("drain"))?;
            }
        }
        trace("remove the device");
        self.devices.remove(DM).await.map_err(io("remove device"))?;
        self.devices.remove_block(Path::new(FRONT));
        trace("detach");
        a.finish().await.map_err(e("detach"))?;
        trace("detached");
        Ok(())
    }

    /// The branch as a fresh host reads it, every block checked against the model.
    async fn check_published(&self, when: &str) -> Result<(), String> {
        let e = |what: &'static str| move |err: pando_engine::Error| format!("{when}: {what}: {err}");
        let c = Arc::new(
            Cache::open(&self.ports, Path::new("/fresh"))
                .await
                .map_err(e("cache"))?,
        );
        let v = Volume::open(&self.ports, c, Path::new("/fresh-state"), BRANCH)
            .await
            .map_err(e("open"))?;
        let mut all = vec![0; usize_from(SIZE)];
        let read = v.read_at(&mut all, 0).await.map_err(e("read"));
        v.close().await;
        read?;
        for p in ["/fresh", "/fresh-state"] {
            remove_all(self.fs.as_ref(), Path::new(p)).await;
        }
        let m = self.model.lock();
        for (b, data) in all.as_chunks::<BLOCK>().0.iter().enumerate() {
            m.check(b, data, when)?;
        }
        Ok(())
    }

    /// Nothing of the branch's images, and no device, stays behind.
    async fn check_left(&self, when: &str) -> Result<(), String> {
        let left: Vec<String> = self
            .fs
            .list(Path::new(IMAGES))
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|d| d.name)
            .filter(|n| n != "bases" && n != "learned")
            .collect();
        if !left.is_empty() {
            return Err(format!("{when}: the detach left {left:?} in {IMAGES}"));
        }
        let devices = self.devices.names();
        if !devices.is_empty() {
            return Err(format!("{when}: the detach left devices {devices:?}"));
        }
        Ok(())
    }

    /// Every base image under its final name is whole, with its block map and node pack, and
    /// one of a root of `main` holds that root. Returns how many there are.
    async fn check_bases(&self, when: &str) -> Result<usize, String> {
        let fs = self.fs.as_ref();
        let mut n = 0;
        for de in fs.list(Path::new(BASES)).await.unwrap_or_default() {
            let Some(root) = de.name.strip_suffix(".img") else {
                continue;
            };
            let blocks = self
                .roots
                .lock()
                .iter()
                .find(|r| r.0 == root)
                .map(|r| r.1.clone());
            let p = Path::new(BASES).join(&de.name);
            for suffix in [".blocks", ".nodes"] {
                let mut s = p.as_os_str().to_owned();
                s.push(suffix);
                if fs.open(Path::new(&s), Open::Read).await.is_err() {
                    return Err(format!("{when}: base image {} has no {suffix}", de.name));
                }
            }
            let f = fs
                .open(&p, Open::Read)
                .await
                .map_err(|e| format!("{when}: base image {}: {e}", de.name))?;
            let data = f
                .read_at(usize_from(SIZE), 0)
                .await
                .map_err(|e| format!("{when}: base image {}: {e}", de.name))?;
            if data.len() != usize_from(SIZE) {
                return Err(format!(
                    "{when}: base image {} holds {} bytes",
                    de.name,
                    data.len()
                ));
            }
            let Some(blocks) = blocks else {
                n += 1;
                continue;
            };
            for (b, got) in data.as_chunks::<BLOCK>().0.iter().enumerate() {
                if id_of(got) != Some(blocks[b]) {
                    return Err(format!(
                        "{when}: base image {} block {b} holds {:?}, not write {}",
                        de.name,
                        id_of(got),
                        blocks[b]
                    ));
                }
            }
            n += 1;
        }
        Ok(n)
    }

    /// Every base image under its final name is whole, and the follower built the latest root
    /// of `main` once a copy of it was served.
    async fn check_followed(&self, when: &str) -> Result<(), String> {
        let latest = self.roots.lock().last().map_or_default(|r| r.0.clone());
        let fs = self.ports.fs.as_ref();
        let noted = fs
            .open(&Path::new(BASES).join("follow").join(BASE), Open::Read)
            .await
            .is_ok();
        let built = fs
            .open(&Path::new(BASES).join(format!("{latest}.img")), Open::Read)
            .await
            .is_ok();
        self.check_bases(when).await?;
        if noted && !built {
            return Err(format!(
                "{when}: the follower built no base image of the latest root {latest}"
            ));
        }
        Ok(())
    }

    /// Runs the branch's lives with crashes at `plan`, the changes to the host's files each
    /// stops after, then checks what the host published and left.
    fn run(&self, seed: u64, plan: &[Option<u64>]) -> Result<(), String> {
        let mut rejoin = false;
        for life in 0..LIVES {
            let stop = plan.get(life).copied().flatten().filter(|_| life + 1 < LIVES);
            let when = format!("seed {seed} life {life}");
            let ended = self
                .process(stop, self.life(rejoin, life == 0))
                .map_err(|e| format!("{when}: {e}"))?;
            if std::env::var_os("PANDO_DST_TRACE").is_some() {
                eprintln!(
                    "{when}: rejoin {rejoin}, stop after {stop:?} changes, ended {ended}, {:?}",
                    self.opts
                );
            }
            if ended {
                return self
                    .process(None, async {
                        self.check_published(&when).await?;
                        self.check_left(&when).await?;
                        self.check_bases(&when).await.map(drop)
                    })
                    .map(drop)
                    .map_err(|e| {
                        if e.starts_with(&when) {
                            e
                        } else {
                            format!("{when}: {e}")
                        }
                    });
            }
            rejoin = self.crash();
        }
        Err(format!("seed {seed}: no life ran to its end"))
    }
}

fn trace(what: &str) {
    if std::env::var_os("PANDO_DST_TRACE").is_some() {
        eprintln!("{what}");
    }
}

async fn remove_all(fs: &dyn LocalFs, dir: &Path) {
    let Ok(entries) = fs.list(dir).await else {
        return;
    };
    for de in entries {
        let p = dir.join(&de.name);
        Box::pin(remove_all(fs, &p)).await;
        let _ = fs.remove(&p).await;
    }
}

/// The branch's lives on one host with crashes anywhere in them, for `seed`.
pub fn attach_crashes(seed: u64) -> Result<Report, String> {
    let plan_rng = SimRng::new(seed ^ 0x0c7a_5e5a);
    let dry = Host::new(seed);
    let dried = |e: String| format!("seed {seed} dry run: {e}");
    dry.setup().map_err(dried)?;
    let before = dry.fs.changes();
    if !dry.process(None, dry.life(false, true)).map_err(dried)? {
        return Err(format!("seed {seed}: the first life stopped without a crash"));
    }
    let first = dry.fs.changes() - before;
    let mut plan = vec![Some(plan_rng.next_u64() % (first + 1))];
    for _ in 1..LIVES {
        plan.push(
            plan_rng
                .next_u64()
                .is_multiple_of(2)
                .then(|| plan_rng.next_u64() % (first / 2 + 1)),
        );
    }
    if let Ok(p) = std::env::var("PANDO_DST_PLAN") {
        plan = p.split(',').map(|n| n.trim().parse().ok()).collect();
    }
    let host = Host::new(seed);
    host.setup()?;
    host.run(seed, &plan)?;
    let r = *host.report.lock();
    Ok(r)
}

/// The follower building the base images of new roots of a branch the host serves copies of,
/// with crashes anywhere in its builds and evictions, for `seed`. After each round, every base
/// image under its final name holds its root whole, and a copy that starts on one reads right.
pub fn follower_crashes(seed: u64, rounds: usize) -> Result<Report, String> {
    let host = Host::new(seed);
    host.setup()?;
    let opts = AttachOptions {
        share_bases: true,
        ..host.opts.clone()
    };
    let keep = 1 + host.below(3);
    let follow = |stop: Option<u64>, host: &Host| {
        host.process(stop, async {
            let cache = Arc::new(
                Cache::open(&host.ports, Path::new(CACHE))
                    .await
                    .map_err(|e| format!("cache: {e}"))?,
            );
            let mut f = Follower::new(&host.ports, cache, Path::new(BASES), 8, keep, 0);
            let built = f.once().await.map_err(|e| format!("follow: {e}"))?;
            host.report.lock().bases += built;
            Ok(())
        })
    };
    let mut copies = 0;
    let mut attach = |host: &Host| {
        copies += 1;
        let name = format!("copy{copies}");
        host.process(None, async {
            branch::copy(&host.ports, BASE, &name, None)
                .await
                .map_err(|e| format!("copy: {e}"))?;
            let cache = Arc::new(
                Cache::open(&host.ports, Path::new(CACHE))
                    .await
                    .map_err(|e| format!("cache: {e}"))?,
            );
            let devices: Arc<dyn DeviceSwitch> = Arc::new(host.devices.clone());
            let a = Attachment::open(
                &host.ports,
                Some(devices),
                cache,
                Path::new(STATE),
                &name,
                opts.clone(),
            )
            .await
            .map_err(|e| format!("attach {name}: {e}"))?;
            let mut all = vec![0; usize_from(SIZE)];
            a.volume()
                .read_at(&mut all, 0)
                .await
                .map_err(|e| format!("read {name}: {e}"))?;
            let blocks = host.roots.lock().last().map_or_default(|r| r.1.clone());
            for (b, got) in all.as_chunks::<BLOCK>().0.iter().enumerate() {
                if id_of(got) != blocks.get(b).copied() {
                    return Err(format!(
                        "{name} block {b} reads {:?}, not write {:?}",
                        id_of(got),
                        blocks.get(b)
                    ));
                }
            }
            a.finish().await.map_err(|e| format!("detach {name}: {e}"))?;
            Ok(())
        })
    };
    if !attach(&host)? {
        return Err(format!("seed {seed}: the first copy stopped without a crash"));
    }
    for round in 0..rounds {
        let when = format!("seed {seed} round {round}");
        host.process(None, host.write_base(1 + host.below(12)))
            .map_err(|e| format!("{when}: {e}"))?;
        let stop = u64_from(host.below(160));
        if !follow(Some(stop), &host).map_err(|e| format!("{when}: {e}"))? {
            host.crash();
        }
        if !follow(None, &host).map_err(|e| format!("{when}: after the crash: {e}"))? {
            return Err(format!("{when}: the follower stopped without a crash"));
        }
        host.process(None, host.check_followed(&when))?;
        if host.below(2) == 0 && !attach(&host).map_err(|e| format!("{when}: {e}"))? {
            return Err(format!("{when}: a copy stopped without a crash"));
        }
    }
    let r = *host.report.lock();
    Ok(r)
}
