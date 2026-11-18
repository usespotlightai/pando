use std::fmt;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};

use async_trait::async_trait;

/// An era device: it maps a whole origin device and records, for each block, the era in which
/// it was last written, on its own metadata device.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EraDevice {
    /// The device node that tables map onto.
    pub path: PathBuf,
    /// The device whose blocks it maps.
    pub origin: PathBuf,
    /// The device that holds its era records.
    pub metadata: PathBuf,
}

/// What [`DeviceSwitch::era_written`] found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EraWritten {
    /// The blocks written since the era asked about, as sorted, disjoint, half-open ranges of
    /// block numbers.
    pub blocks: Vec<Range<u64>>,
    /// The era that began with the call; every later write belongs to it or a later one.
    pub now: u32,
}

/// The device-mapper device a branch is served through. A handoff loads a table that maps the
/// device to a local image and swaps it in during one suspend, so the file system above sees
/// no gap. Tables map the whole device linearly onto one target device.
#[async_trait]
pub trait DeviceSwitch: Send + Sync + fmt::Debug {
    /// Creates device `name` that maps `size` bytes onto `target`.
    async fn create(&self, name: &str, target: &Path, size: u64) -> io::Result<()>;
    /// Removes device `name`; it fails while something holds the device open.
    async fn remove(&self, name: &str) -> io::Result<()>;
    /// The target of the live table, or `None` when the device has no table.
    async fn live_target(&self, name: &str) -> io::Result<Option<PathBuf>>;
    /// Loads a table onto `target` as the inactive table; [`DeviceSwitch::resume`] swaps it in.
    async fn load(&self, name: &str, target: &Path, size: u64) -> io::Result<()>;
    /// Drops a table [`DeviceSwitch::load`] loaded, so the next resume keeps the live table.
    async fn clear_inactive(&self, name: &str) -> io::Result<()>;
    /// Holds I/O on the device; writes in flight finish first.
    async fn suspend(&self, name: &str) -> io::Result<()>;
    /// Swaps in the inactive table, if one is loaded, and releases the I/O
    /// [`DeviceSwitch::suspend`] held.
    async fn resume(&self, name: &str) -> io::Result<()>;
    /// Binds a free loop device to `image` and returns the loop device.
    async fn attach_loop(&self, image: &Path) -> io::Result<PathBuf>;
    /// Makes loop device `device` as large as its image is now.
    async fn refresh_loop(&self, device: &Path) -> io::Result<()>;
    /// The image a loop device is bound to.
    async fn loop_image(&self, device: &Path) -> io::Result<Option<PathBuf>>;
    /// The bytes written to a device since it was created.
    async fn bytes_written(&self, device: &Path) -> io::Result<u64>;
    /// Creates era device `name` over `size` bytes of `origin`, tracking blocks of `block`
    /// bytes, with its records on `metadata`, which starts zeroed. Its first era is 1. Fails
    /// where the host cannot track eras.
    async fn create_era(
        &self,
        name: &str,
        metadata: &Path,
        origin: &Path,
        size: u64,
        block: u64,
    ) -> io::Result<PathBuf>;
    /// Era device `name`, or `None` when no era device has that name.
    async fn era(&self, name: &str) -> io::Result<Option<EraDevice>>;
    /// Makes era device `name` map `size` bytes of its origin, which has grown to hold them.
    async fn resize_era(&self, name: &str, size: u64) -> io::Result<()>;
    /// Begins a new era on era device `name`, holds the records as they stand for
    /// [`Self::era_listed`], and returns the era that began. Writes the device completed before
    /// the call belong to earlier eras; a write in flight may belong to either, so a caller
    /// that needs the boundary exact begins the era with the device above suspended.
    async fn era_begin(&self, name: &str) -> io::Result<u32>;
    /// Lists, from the records the last [`Self::era_begin`] held, the blocks written since era
    /// `since` began, then releases them. Fails when no records are held.
    async fn era_listed(&self, name: &str, since: u32) -> io::Result<Vec<Range<u64>>>;
    /// Begins a new era on era device `name` and returns the blocks written since era `since`
    /// began. Writes the device completed before the call are in the list.
    async fn era_written(&self, name: &str, since: u32) -> io::Result<EraWritten> {
        let now = self.era_begin(name).await?;
        let blocks = self.era_listed(name, since).await?;
        Ok(EraWritten { blocks, now })
    }
}
