use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};

use super::SimFs;
use crate::ports::{DeviceSwitch, EraDevice, LocalFs, Open};

/// A block device the simulated device-mapper can map onto, such as a branch's frontend.
#[async_trait]
pub trait SimBlock: Send + Sync {
    /// Fills `buf` with the bytes at `off`.
    async fn read(&self, buf: &mut [u8], off: u64) -> io::Result<()>;
    /// Writes `b` at `off`; reads see it once this returns.
    async fn write(&self, b: &[u8], off: u64) -> io::Result<()>;
    /// Makes every write that returned before this was called durable.
    async fn flush(&self) -> io::Result<()>;
}

#[derive(Debug)]
struct Dev {
    live: PathBuf,
    size: u64,
    inactive: Option<(PathBuf, u64)>,
    io: Arc<RwLock<()>>,
    held: Option<Arc<OwnedRwLockWriteGuard<()>>>,
}

#[derive(Debug)]
struct Era {
    dev: EraDevice,
    size: u64,
    block: u64,
    now: u32,
    /// The era in which each written block was last written.
    last: BTreeMap<u64, u32>,
    /// The records the last `era_begin` held.
    held: Option<BTreeMap<u64, u32>>,
    /// Writes to land, one batch at each later `era_begin`, just before the era begins.
    at_begins: VecDeque<Vec<(u64, Vec<u8>)>>,
}

#[derive(Default)]
struct State {
    devs: HashMap<String, Dev>,
    /// Each loop device's image and size.
    loops: HashMap<PathBuf, (PathBuf, u64)>,
    eras: HashMap<String, Era>,
    written: HashMap<PathBuf, u64>,
    blocks: HashMap<PathBuf, Arc<dyn SimBlock>>,
    next_loop: u32,
    no_eras: bool,
}

/// Device-mapper devices over block devices, era devices and loop devices over [`SimFs`]
/// files. Reads and writes through a device go to its live table's target and wait while it
/// is suspended; past the size of the table or of the loop device they fail. Once the host's
/// files stopped ([`SimFs::stop_after`]), no [`DeviceSwitch`] call returns, as none does from a
/// process that died.
#[derive(Clone)]
pub struct SimDevices {
    fs: SimFs,
    state: Arc<Mutex<State>>,
}

impl std::fmt::Debug for SimDevices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimDevices").finish_non_exhaustive()
    }
}

fn no_device(name: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("no device {name}"))
}

fn beyond(name: &str, end: u64, size: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("I/O to byte {end} of {name}, which has {size}"),
    )
}

enum Target {
    Block(Arc<dyn SimBlock>),
    /// An image under a loop device, and the era device above the loop device, if any.
    Image {
        image: PathBuf,
        era: Option<String>,
    },
}

impl SimDevices {
    /// No devices yet; loop devices bind to files of `fs`.
    pub fn new(fs: SimFs) -> SimDevices {
        SimDevices {
            fs,
            state: Arc::default(),
        }
    }

    /// Devices of a host whose kernel has no era target.
    pub fn without_eras(fs: SimFs) -> SimDevices {
        let d = SimDevices::new(fs);
        d.lock().no_eras = true;
        d
    }

    fn lock(&self) -> parking_lot::MutexGuard<'_, State> {
        self.state.lock()
    }

    async fn alive(&self) {
        if self.fs.stopped() {
            std::future::pending::<()>().await;
        }
    }

    /// Loses every device, loop device, era device and block device, as a host that lost
    /// power does.
    pub fn power_cut(&self) {
        let mut s = self.lock();
        s.devs.clear();
        s.loops.clear();
        s.eras.clear();
        s.written.clear();
        s.blocks.clear();
    }

    /// The device-mapper and era devices there are, sorted.
    pub fn names(&self) -> Vec<String> {
        let s = self.lock();
        let mut out: Vec<String> = s.devs.keys().chain(s.eras.keys()).cloned().collect();
        out.sort();
        out
    }

    /// Makes `block` a device at `path` that tables can map onto.
    pub fn add_block(&self, path: &Path, block: Arc<dyn SimBlock>) {
        self.lock().blocks.insert(path.to_owned(), block);
    }

    /// Removes the block device at `path`; tables that map onto it fail their I/O.
    pub fn remove_block(&self, path: &Path) {
        self.lock().blocks.remove(path);
    }

    /// Makes each later begin of a new era on era device `name` first take one of `batches`
    /// of writes at byte offsets of its origin, as if the device above had just written them.
    pub fn write_at_era_begins(&self, name: &str, batches: Vec<Vec<(u64, Vec<u8>)>>) {
        if let Some(e) = self.lock().eras.get_mut(name) {
            e.at_begins.extend(batches);
        }
    }

    fn route(&self, name: &str) -> io::Result<(Arc<RwLock<()>>, PathBuf, u64)> {
        let s = self.lock();
        let d = s.devs.get(name).ok_or_else(|| no_device(name))?;
        Ok((Arc::clone(&d.io), d.live.clone(), d.size))
    }

    /// The target of the live table of `name`, for I/O that ends at byte `end`, and the loop
    /// device it reaches.
    fn target(&self, name: &str, end: u64) -> io::Result<(PathBuf, Target)> {
        let (_, mut live, size) = self.route(name)?;
        if end > size {
            return Err(beyond(name, end, size));
        }
        let s = self.lock();
        if let Some(b) = s.blocks.get(&live) {
            return Ok((live, Target::Block(Arc::clone(b))));
        }
        let mut era = None;
        if let Some((n, e)) = s.eras.iter().find(|(_, e)| e.dev.path == live) {
            if end > e.size {
                return Err(beyond(n, end, e.size));
            }
            era = Some(n.clone());
            live.clone_from(&e.dev.origin);
        }
        let (image, cap) = s
            .loops
            .get(&live)
            .cloned()
            .ok_or_else(|| no_device(&live.display().to_string()))?;
        if end > cap {
            return Err(beyond(&live.display().to_string(), end, cap));
        }
        Ok((live, Target::Image { image, era }))
    }

    /// The size of the live table of device `name`.
    pub fn size(&self, name: &str) -> Option<u64> {
        self.lock().devs.get(name).map(|d| d.size)
    }

    /// Reads through device `name`.
    pub async fn read(&self, name: &str, buf: &mut [u8], off: u64) -> io::Result<()> {
        let (io, _, _) = self.route(name)?;
        let _g = io.read().await;
        match self.target(name, off + buf.len() as u64)?.1 {
            Target::Block(b) => b.read(buf, off).await,
            Target::Image { image, .. } => {
                let got = self
                    .fs
                    .open(&image, Open::Read)
                    .await?
                    .read_at(buf.len(), off)
                    .await?;
                buf[..got.len()].copy_from_slice(&got);
                buf[got.len()..].fill(0);
                Ok(())
            }
        }
    }

    /// Writes through device `name`; it completes once the target has it.
    pub async fn write(&self, name: &str, b: &[u8], off: u64) -> io::Result<()> {
        let (io, _, _) = self.route(name)?;
        let _g = io.read().await;
        let (live, target) = self.target(name, off + b.len() as u64)?;
        match target {
            Target::Block(t) => t.write(b, off).await,
            Target::Image { image, era } => {
                if let Some(e) = era.as_ref().filter(|_| !b.is_empty())
                    && let Some(e) = self.lock().eras.get_mut(e)
                {
                    for blk in off / e.block..=(off + b.len() as u64 - 1) / e.block {
                        e.last.insert(blk, e.now);
                    }
                }
                self.fs
                    .open(&image, Open::ReadWrite)
                    .await?
                    .write_at(vec![Bytes::copy_from_slice(b)], off)
                    .await?;
                *self.lock().written.entry(live).or_default() += b.len() as u64;
                Ok(())
            }
        }
    }

    /// Flushes device `name`: every write through it that returned is durable once this
    /// returns.
    pub async fn flush(&self, name: &str) -> io::Result<()> {
        let (io, _, _) = self.route(name)?;
        let _g = io.read().await;
        match self.target(name, 0)?.1 {
            Target::Block(b) => b.flush().await,
            Target::Image { image, .. } => self.fs.open(&image, Open::ReadWrite).await?.sync_data().await,
        }
    }

    /// Whether device `name` holds its I/O after [`DeviceSwitch::suspend`].
    pub fn is_suspended(&self, name: &str) -> bool {
        self.lock().devs.get(name).is_some_and(|d| d.held.is_some())
    }

    /// Whether device `name` has a table [`DeviceSwitch::load`] loaded and no resume swapped in.
    pub fn has_inactive(&self, name: &str) -> bool {
        self.lock().devs.get(name).is_some_and(|d| d.inactive.is_some())
    }
}

#[async_trait]
impl DeviceSwitch for SimDevices {
    async fn create(&self, name: &str, target: &Path, size: u64) -> io::Result<()> {
        self.alive().await;
        let mut s = self.lock();
        if s.devs.contains_key(name) || s.eras.contains_key(name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, name.to_owned()));
        }
        let dev = Dev {
            live: target.to_owned(),
            size,
            inactive: None,
            io: Arc::default(),
            held: None,
        };
        s.devs.insert(name.to_owned(), dev);
        Ok(())
    }

    async fn remove(&self, name: &str) -> io::Result<()> {
        self.alive().await;
        let mut s = self.lock();
        if s.devs.remove(name).is_some() {
            return Ok(());
        }
        let e = s.eras.get(name).ok_or_else(|| no_device(name))?;
        if s.devs.values().any(|d| d.live == e.dev.path) {
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                format!("{name} is mapped by another device"),
            ));
        }
        s.eras.remove(name);
        Ok(())
    }

    async fn live_target(&self, name: &str) -> io::Result<Option<PathBuf>> {
        self.alive().await;
        Ok(self.lock().devs.get(name).map(|d| d.live.clone()))
    }

    async fn load(&self, name: &str, target: &Path, size: u64) -> io::Result<()> {
        self.alive().await;
        let mut s = self.lock();
        let d = s.devs.get_mut(name).ok_or_else(|| no_device(name))?;
        d.inactive = Some((target.to_owned(), size));
        Ok(())
    }

    async fn clear_inactive(&self, name: &str) -> io::Result<()> {
        self.alive().await;
        let mut s = self.lock();
        let d = s.devs.get_mut(name).ok_or_else(|| no_device(name))?;
        d.inactive = None;
        Ok(())
    }

    async fn suspend(&self, name: &str) -> io::Result<()> {
        self.alive().await;
        let (io, _, _) = self.route(name)?;
        if self.is_suspended(name) {
            return Ok(());
        }
        let g = io.write_owned().await;
        if let Some(d) = self.lock().devs.get_mut(name) {
            d.held = Some(Arc::new(g));
        }
        Ok(())
    }

    async fn resume(&self, name: &str) -> io::Result<()> {
        self.alive().await;
        let mut s = self.lock();
        let d = s.devs.get_mut(name).ok_or_else(|| no_device(name))?;
        if let Some((t, size)) = d.inactive.take() {
            d.live = t;
            d.size = size;
        }
        d.held = None;
        Ok(())
    }

    async fn attach_loop(&self, image: &Path) -> io::Result<PathBuf> {
        self.alive().await;
        let cap = self.fs.open(image, Open::Read).await?.size().await?;
        let mut s = self.lock();
        s.next_loop += 1;
        let dev = PathBuf::from(format!("/dev/loop{}", s.next_loop));
        s.loops.insert(dev.clone(), (image.to_owned(), cap));
        Ok(dev)
    }

    async fn refresh_loop(&self, device: &Path) -> io::Result<()> {
        self.alive().await;
        let image = self
            .lock()
            .loops
            .get(device)
            .map(|(p, _)| p.clone())
            .ok_or_else(|| no_device(&device.display().to_string()))?;
        let cap = self.fs.open(&image, Open::Read).await?.size().await?;
        if let Some(l) = self.lock().loops.get_mut(device) {
            l.1 = cap;
        }
        Ok(())
    }

    async fn loop_image(&self, device: &Path) -> io::Result<Option<PathBuf>> {
        self.alive().await;
        Ok(self.lock().loops.get(device).map(|(p, _)| p.clone()))
    }

    async fn bytes_written(&self, device: &Path) -> io::Result<u64> {
        self.alive().await;
        Ok(self.lock().written.get(device).copied().unwrap_or(0))
    }

    async fn create_era(
        &self,
        name: &str,
        metadata: &Path,
        origin: &Path,
        size: u64,
        block: u64,
    ) -> io::Result<PathBuf> {
        self.alive().await;
        let mut s = self.lock();
        if s.no_eras {
            return Err(io::Error::new(io::ErrorKind::Unsupported, "no era target"));
        }
        if s.devs.contains_key(name) || s.eras.contains_key(name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, name.to_owned()));
        }
        let cap = s
            .loops
            .get(origin)
            .map(|l| l.1)
            .ok_or_else(|| no_device(&origin.display().to_string()))?;
        if size > cap || block == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("era of {size} bytes in blocks of {block} over {cap}"),
            ));
        }
        let path = PathBuf::from(format!("/dev/mapper/{name}"));
        let dev = EraDevice {
            path: path.clone(),
            origin: origin.to_owned(),
            metadata: metadata.to_owned(),
        };
        s.eras.insert(
            name.to_owned(),
            Era {
                dev,
                size,
                block,
                now: 1,
                last: BTreeMap::new(),
                held: None,
                at_begins: VecDeque::new(),
            },
        );
        Ok(path)
    }

    async fn era(&self, name: &str) -> io::Result<Option<EraDevice>> {
        self.alive().await;
        Ok(self.lock().eras.get(name).map(|e| e.dev.clone()))
    }

    async fn resize_era(&self, name: &str, size: u64) -> io::Result<()> {
        self.alive().await;
        let mut s = self.lock();
        let origin = s
            .eras
            .get(name)
            .ok_or_else(|| no_device(name))?
            .dev
            .origin
            .clone();
        let cap = s.loops.get(&origin).map_or(0, |l| l.1);
        if size > cap {
            return Err(beyond(name, size, cap));
        }
        if let Some(e) = s.eras.get_mut(name) {
            e.size = size;
        }
        Ok(())
    }

    async fn era_begin(&self, name: &str) -> io::Result<u32> {
        self.alive().await;
        let (batch, origin) = {
            let mut s = self.lock();
            let e = s.eras.get_mut(name).ok_or_else(|| no_device(name))?;
            let batch = e.at_begins.pop_front().unwrap_or_default();
            for (off, b) in &batch {
                for blk in off / e.block..=(off + b.len() as u64 - 1) / e.block {
                    e.last.insert(blk, e.now);
                }
            }
            (batch, e.dev.origin.clone())
        };
        if !batch.is_empty() {
            let image = self
                .lock()
                .loops
                .get(&origin)
                .map(|l| l.0.clone())
                .ok_or_else(|| no_device(&origin.display().to_string()))?;
            let f = self.fs.open(&image, Open::ReadWrite).await?;
            for (off, b) in batch {
                f.write_at(vec![Bytes::from(b)], off).await?;
            }
        }
        let mut s = self.lock();
        let e = s.eras.get_mut(name).ok_or_else(|| no_device(name))?;
        e.held = Some(e.last.clone());
        e.now += 1;
        Ok(e.now)
    }

    async fn era_listed(&self, name: &str, since: u32) -> io::Result<Vec<Range<u64>>> {
        self.alive().await;
        let mut s = self.lock();
        let e = s.eras.get_mut(name).ok_or_else(|| no_device(name))?;
        let held = e
            .held
            .take()
            .ok_or_else(|| io::Error::other(format!("{name} holds no records")))?;
        let mut blocks: Vec<Range<u64>> = Vec::new();
        for (&b, _) in held.iter().filter(|(_, era)| **era >= since) {
            match blocks.last_mut() {
                Some(r) if r.end == b => r.end += 1,
                _ => blocks.push(b..b + 1),
            }
        }
        Ok(blocks)
    }
}
