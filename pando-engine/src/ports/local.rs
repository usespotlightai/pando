use std::fmt;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Open {
    Read,
    ReadWrite,
    /// Read and write; creates the file when it is missing.
    Create,
    /// Read and write; fails when the file exists.
    CreateNew,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DirEntry {
    pub name: String,
    pub len: u64,
    pub modified: SystemTime,
    pub is_dir: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Advice {
    WillNeed,
    DontNeed,
}

/// Files on this host: the cache log, the journal, volume state, owner records, base images.
///
/// Durability is explicit: written data survives a crash only after [`LocalFile::sync_data`],
/// and a created, renamed or removed name only after [`LocalFs::sync_dir`] of its directory.
/// The simulator drops everything else on a simulated crash.
#[async_trait]
pub trait LocalFs: Send + Sync + fmt::Debug {
    async fn open(&self, path: &Path, how: Open) -> io::Result<Arc<dyn LocalFile>>;
    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    async fn remove(&self, path: &Path) -> io::Result<()>;
    async fn list(&self, dir: &Path) -> io::Result<Vec<DirEntry>>;
    async fn create_dir_all(&self, dir: &Path) -> io::Result<()>;
    async fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    /// An exclusive lock between processes on this host, held until the guard drops.
    async fn lock(&self, path: &Path) -> io::Result<Box<dyn FileLock>>;
}

#[async_trait]
pub trait LocalFile: Send + Sync + fmt::Debug {
    /// Reads up to `len` bytes at `offset`; fewer only at the end of the file.
    async fn read_at(&self, len: usize, offset: u64) -> io::Result<Bytes>;
    /// Reads only what is in the page cache: `None` when the read would wait for the disk.
    fn read_at_nowait(&self, buf: &mut [u8], offset: u64) -> io::Result<Option<usize>>;
    async fn write_at(&self, bufs: Vec<Bytes>, offset: u64) -> io::Result<()>;
    async fn sync_data(&self) -> io::Result<()>;
    async fn set_len(&self, len: u64) -> io::Result<()>;
    async fn size(&self) -> io::Result<u64>;
    /// Starts writing dirty pages back, without waiting for the disk, so a later sync waits less.
    async fn start_writeback(&self);
    fn advise(&self, offset: u64, len: u64, advice: Advice);
}

pub trait FileLock: Send + Sync + fmt::Debug {}

/// One extent of a file: `len` bytes at `logical` in the file, at `physical` on the disk. An
/// unwritten extent has its place but reads as zeros.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Extent {
    pub logical: u64,
    pub physical: u64,
    pub len: u64,
    pub unwritten: bool,
}

/// Base images: block-sized files that share extents with their clones.
#[async_trait]
pub trait LocalImage: Send + Sync + fmt::Debug {
    /// Makes `to` a copy of `from` that shares its extents where the file system can (a
    /// reflink); a plain copy elsewhere.
    async fn clone_file(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Sets up a new, empty image: no copy-on-write where the file system has a switch for it,
    /// and an extent size hint of `hint` bytes where it has those.
    async fn prepare(&self, path: &Path, hint: Option<u64>) -> io::Result<()>;
    async fn extents(&self, path: &Path) -> io::Result<Vec<Extent>>;
    async fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<()>;
    /// The bytes the file has on the disk.
    async fn allocated(&self, path: &Path) -> io::Result<u64>;
}
