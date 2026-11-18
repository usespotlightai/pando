use std::fmt;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;

/// How [`LocalFs::open`] opens a file.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Open {
    /// Read only; fails when the file is missing.
    Read,
    /// Read and write; fails when the file is missing.
    ReadWrite,
    /// Read and write; creates the file when it is missing.
    Create,
    /// Read and write; fails when the file exists.
    CreateNew,
}

/// One name in a directory, as [`LocalFs::list`] returns it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DirEntry {
    /// The file name, without the directory.
    pub name: String,
    /// The length in bytes of a file.
    pub len: u64,
    /// The last modification time.
    pub modified: SystemTime,
    /// Whether the entry is a directory.
    pub is_dir: bool,
}

/// What [`LocalFile::advise`] tells the page cache about a range.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Advice {
    /// The range will be read soon: start reading it in.
    WillNeed,
    /// The range will not be read again soon: its clean pages may go.
    DontNeed,
}

/// Files on this host: the cache log, the journal, volume state, owner records, base images.
///
/// Durability is explicit: written data survives a crash only after [`LocalFile::sync_data`],
/// and a created, renamed or removed name only after [`LocalFs::sync_dir`] of its directory.
/// The simulator drops everything else on a simulated crash.
#[async_trait]
pub trait LocalFs: Send + Sync + fmt::Debug {
    /// Opens the file at `path` as `how` says.
    async fn open(&self, path: &Path, how: Open) -> io::Result<Arc<dyn LocalFile>>;
    /// Moves `from` to `to` atomically, replacing a file at `to`.
    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Removes the name `path`; open files stay readable.
    async fn remove(&self, path: &Path) -> io::Result<()>;
    /// The entries directly in `dir`, in name order.
    async fn list(&self, dir: &Path) -> io::Result<Vec<DirEntry>>;
    /// Creates `dir` and its missing parents; succeeds when it exists.
    async fn create_dir_all(&self, dir: &Path) -> io::Result<()>;
    /// Makes the names created, renamed or removed in `dir` durable.
    async fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    /// An exclusive lock between processes on this host, held until the guard drops.
    async fn lock(&self, path: &Path) -> io::Result<Box<dyn FileLock>>;
    /// [`Self::lock`], or `ResourceBusy` at once when another holder has it.
    async fn try_lock(&self, path: &Path) -> io::Result<Box<dyn FileLock>>;
}

/// An open file of [`LocalFs`]. Reads and writes at offsets, so callers share one handle.
#[async_trait]
pub trait LocalFile: Send + Sync + fmt::Debug {
    /// Reads up to `len` bytes at `offset`; fewer only at the end of the file.
    async fn read_at(&self, len: usize, offset: u64) -> io::Result<Bytes>;
    /// Reads only what is in the page cache: `None` when the read would wait for the disk.
    fn read_at_nowait(&self, buf: &mut [u8], offset: u64) -> io::Result<Option<usize>>;
    /// Writes `bufs` one after another from `offset`, growing the file as needed.
    async fn write_at(&self, bufs: Vec<Bytes>, offset: u64) -> io::Result<()>;
    /// [`Self::write_at`] on the calling thread, without a hop to the blocking pool. Only for
    /// small writes into the page cache, which seldom wait, where the hop costs more than the
    /// write.
    fn write_at_now(&self, b: &[u8], offset: u64) -> io::Result<()>;
    /// Makes the data written so far durable.
    async fn sync_data(&self) -> io::Result<()>;
    /// Truncates or extends the file to `len` bytes; added bytes read as zeros.
    async fn set_len(&self, len: u64) -> io::Result<()>;
    /// The length in bytes; for a block device, the size of the device.
    async fn size(&self) -> io::Result<u64>;
    /// Starts writing dirty pages back, without waiting for the disk, so a later sync waits less.
    async fn start_writeback(&self);
    /// A hint about `len` bytes at `offset`; it never changes what reads return.
    fn advise(&self, offset: u64, len: u64, advice: Advice);
}

/// A lock from [`LocalFs::lock`]; dropping it releases the lock.
pub trait FileLock: Send + Sync + fmt::Debug {}

/// One extent of a file: `len` bytes at `logical` in the file, at `physical` on the disk. An
/// unwritten extent has its place but reads as zeros.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Extent {
    /// The byte offset in the file.
    pub logical: u64,
    /// The byte offset on the disk.
    pub physical: u64,
    /// The length in bytes.
    pub len: u64,
    /// The extent is allocated but reads as zeros.
    pub unwritten: bool,
}

/// Base images: block-sized files that share extents with their clones.
#[async_trait]
pub trait LocalImage: Send + Sync + fmt::Debug {
    /// Makes `to` a copy of `from` that shares its extents where the file system can (a
    /// reflink); a plain copy elsewhere.
    async fn clone_file(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Puts the bytes of `from` in each of `ranges` into `to` at the same offsets, sharing
    /// their extents where the file system can. A missing `to` starts empty; `to` grows to the
    /// length of `from` and keeps its bytes outside `ranges`. Ranges are block aligned, except
    /// that the last may end at the end of `from`.
    async fn clone_ranges(&self, from: &Path, to: &Path, ranges: &[std::ops::Range<u64>]) -> io::Result<()>;
    /// Sets up a new, empty image: no copy-on-write where the file system has a switch for it,
    /// and an extent size hint of `hint` bytes where it has those.
    async fn prepare(&self, path: &Path, hint: Option<u64>) -> io::Result<()>;
    /// The extents of the file at `path`, in file order; holes have none.
    async fn extents(&self, path: &Path) -> io::Result<Vec<Extent>>;
    /// Frees `len` bytes at `offset` of the file at `path`; they read as zeros and its length
    /// stays.
    async fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<()>;
    /// The bytes the file has on the disk.
    async fn allocated(&self, path: &Path) -> io::Result<u64>;
}
