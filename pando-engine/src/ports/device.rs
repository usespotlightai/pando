use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use async_trait::async_trait;

/// The device-mapper device a branch is served through. A handoff loads a table that maps the
/// device to a local image and swaps it in during one suspend, so the file system above sees
/// no gap. Tables map the whole device linearly onto one target device.
#[async_trait]
pub trait DeviceSwitch: Send + Sync + fmt::Debug {
    /// Creates device `name` that maps `size` bytes onto `target`.
    async fn create(&self, name: &str, target: &Path, size: u64) -> io::Result<()>;
    async fn remove(&self, name: &str) -> io::Result<()>;
    /// The target of the live table, or `None` when the device has no table.
    async fn live_target(&self, name: &str) -> io::Result<Option<PathBuf>>;
    /// Loads a table onto `target` as the inactive table; [`DeviceSwitch::resume`] swaps it in.
    async fn load(&self, name: &str, target: &Path, size: u64) -> io::Result<()>;
    async fn clear_inactive(&self, name: &str) -> io::Result<()>;
    /// Holds I/O on the device; writes in flight finish first.
    async fn suspend(&self, name: &str) -> io::Result<()>;
    async fn resume(&self, name: &str) -> io::Result<()>;
    /// Binds a free loop device to `image` and returns the loop device.
    async fn attach_loop(&self, image: &Path) -> io::Result<PathBuf>;
    /// Makes loop device `device` as large as its image is now.
    async fn refresh_loop(&self, device: &Path) -> io::Result<()>;
    /// The image a loop device is bound to.
    async fn loop_image(&self, device: &Path) -> io::Result<Option<PathBuf>>;
    /// The bytes written to a device since it was created.
    async fn bytes_written(&self, device: &Path) -> io::Result<u64>;
}
