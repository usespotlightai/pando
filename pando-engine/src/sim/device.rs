use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};

use super::SimFs;
use crate::ports::{DeviceSwitch, LocalFs, Open};

/// A block device the simulated device-mapper can map onto, such as a branch's frontend.
#[async_trait]
pub trait SimBlock: Send + Sync {
    async fn read(&self, buf: &mut [u8], off: u64) -> io::Result<()>;
    async fn write(&self, b: &[u8], off: u64) -> io::Result<()>;
}

#[derive(Debug)]
struct Dev {
    live: PathBuf,
    size: u64,
    inactive: Option<(PathBuf, u64)>,
    io: Arc<RwLock<()>>,
    held: Option<Arc<OwnedRwLockWriteGuard<()>>>,
}

#[derive(Default)]
struct State {
    devs: HashMap<String, Dev>,
    /// Each loop device's image and size.
    loops: HashMap<PathBuf, (PathBuf, u64)>,
    written: HashMap<PathBuf, u64>,
    blocks: HashMap<PathBuf, Arc<dyn SimBlock>>,
    next_loop: u32,
}

/// Device-mapper devices over block devices and loop devices over [`SimFs`] files. Reads and
/// writes through a device go to its live table's target and wait while it is suspended; past
/// the size of the table or of the loop device they fail.
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
    Image(PathBuf),
}

impl SimDevices {
    pub fn new(fs: SimFs) -> SimDevices {
        SimDevices {
            fs,
            state: Arc::default(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("sim devices lock")
    }

    /// Makes `block` a device at `path` that tables can map onto.
    pub fn add_block(&self, path: &Path, block: Arc<dyn SimBlock>) {
        self.lock().blocks.insert(path.to_owned(), block);
    }

    pub fn remove_block(&self, path: &Path) {
        self.lock().blocks.remove(path);
    }

    fn route(&self, name: &str) -> io::Result<(Arc<RwLock<()>>, PathBuf, u64)> {
        let s = self.lock();
        let d = s.devs.get(name).ok_or_else(|| no_device(name))?;
        Ok((d.io.clone(), d.live.clone(), d.size))
    }

    /// The target of the live table of `name`, for I/O that ends at byte `end`.
    fn target(&self, name: &str, end: u64) -> io::Result<(PathBuf, Target)> {
        let (_, live, size) = self.route(name)?;
        if end > size {
            return Err(beyond(name, end, size));
        }
        let s = self.lock();
        if let Some(b) = s.blocks.get(&live) {
            return Ok((live, Target::Block(b.clone())));
        }
        let (image, cap) = s
            .loops
            .get(&live)
            .cloned()
            .ok_or_else(|| no_device(&live.display().to_string()))?;
        if end > cap {
            return Err(beyond(&live.display().to_string(), end, cap));
        }
        Ok((live, Target::Image(image)))
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
            Target::Image(p) => {
                let got = self
                    .fs
                    .open(&p, Open::Read)
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
            Target::Image(p) => {
                self.fs
                    .open(&p, Open::ReadWrite)
                    .await?
                    .write_at(vec![Bytes::copy_from_slice(b)], off)
                    .await?;
                *self.lock().written.entry(live).or_default() += b.len() as u64;
                Ok(())
            }
        }
    }

    pub fn is_suspended(&self, name: &str) -> bool {
        self.lock().devs.get(name).is_some_and(|d| d.held.is_some())
    }

    pub fn has_inactive(&self, name: &str) -> bool {
        self.lock().devs.get(name).is_some_and(|d| d.inactive.is_some())
    }
}

#[async_trait]
impl DeviceSwitch for SimDevices {
    async fn create(&self, name: &str, target: &Path, size: u64) -> io::Result<()> {
        let mut s = self.lock();
        if s.devs.contains_key(name) {
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
        self.lock()
            .devs
            .remove(name)
            .map(drop)
            .ok_or_else(|| no_device(name))
    }

    async fn live_target(&self, name: &str) -> io::Result<Option<PathBuf>> {
        Ok(self.lock().devs.get(name).map(|d| d.live.clone()))
    }

    async fn load(&self, name: &str, target: &Path, size: u64) -> io::Result<()> {
        let mut s = self.lock();
        let d = s.devs.get_mut(name).ok_or_else(|| no_device(name))?;
        d.inactive = Some((target.to_owned(), size));
        Ok(())
    }

    async fn clear_inactive(&self, name: &str) -> io::Result<()> {
        let mut s = self.lock();
        let d = s.devs.get_mut(name).ok_or_else(|| no_device(name))?;
        d.inactive = None;
        Ok(())
    }

    async fn suspend(&self, name: &str) -> io::Result<()> {
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
        let cap = self.fs.open(image, Open::Read).await?.size().await?;
        let mut s = self.lock();
        s.next_loop += 1;
        let dev = PathBuf::from(format!("/dev/loop{}", s.next_loop));
        s.loops.insert(dev.clone(), (image.to_owned(), cap));
        Ok(dev)
    }

    async fn refresh_loop(&self, device: &Path) -> io::Result<()> {
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
        Ok(self.lock().loops.get(device).map(|(p, _)| p.clone()))
    }

    async fn bytes_written(&self, device: &Path) -> io::Result<u64> {
        Ok(self.lock().written.get(device).copied().unwrap_or(0))
    }
}
