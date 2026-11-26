use parking_lot::{Mutex, MutexGuard};
use std::collections::BTreeMap;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use pando_format::{u64_from, usize_from};

use crate::ports::{Advice, DirEntry, Extent, FileLock, LocalFile, LocalFs, LocalImage, Open};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Node {
    File(u64),
    Dir,
}

const BLOCK: u64 = 4096;

#[derive(Debug, Default)]
struct Data {
    current: Vec<u8>,
    durable: Vec<u8>,
    /// Where each allocated 4 KiB block of the file is on the disk. A write moves the blocks it
    /// covers to new places; a clone shares the places of its source.
    blocks: BTreeMap<u64, u64>,
    durable_blocks: BTreeMap<u64, u64>,
    /// The length [`LocalFile::allocate`] gave the file beyond `current`: space it holds,
    /// reading as zeros.
    reserved: u64,
}

impl Data {
    fn write(&mut self, at: u64, n: u64, next_place: &mut u64) {
        if n == 0 {
            return;
        }
        for b in at / BLOCK..=(at + n - 1) / BLOCK {
            *next_place += 1;
            self.blocks.insert(b, *next_place);
        }
    }

    fn truncate(&mut self, len: u64) {
        self.blocks.retain(|b, _| b * BLOCK < len);
    }

    fn len(&self) -> u64 {
        u64_from(self.current.len()).max(self.reserved)
    }
}

#[derive(Debug, Default)]
struct State {
    data: BTreeMap<u64, Data>,
    names: BTreeMap<PathBuf, Node>,
    durable_names: BTreeMap<PathBuf, Node>,
    next_inode: u64,
    locks: BTreeMap<PathBuf, u64>,
    next_lock: u64,
    next_place: u64,
    capacity: Option<u64>,
    /// How many more changes to the files succeed before every change fails.
    stop_after: Option<u64>,
    changes: u64,
}

impl State {
    /// Counts a change to the files; it fails once [`SimFs::stop_after`] ran out.
    fn change(&mut self) -> io::Result<()> {
        match &mut self.stop_after {
            Some(0) => Err(io::Error::other("the simulated host stopped")),
            Some(n) => {
                *n -= 1;
                self.changes += 1;
                Ok(())
            }
            None => {
                self.changes += 1;
                Ok(())
            }
        }
    }

    /// The bytes still free for files to grow into.
    fn free(&self) -> u64 {
        let Some(cap) = self.capacity else {
            return u64::MAX;
        };
        let used: u64 = self
            .names
            .values()
            .filter_map(|n| match n {
                Node::File(i) => self.data.get(i).map(Data::len),
                Node::Dir => None,
            })
            .sum();
        cap.saturating_sub(used)
    }
}

fn full() -> io::Error {
    io::Error::new(io::ErrorKind::StorageFull, "no space left on the simulated disk")
}

/// A file system in memory that keeps what a crash would keep apart from what it would lose:
/// file data is durable after `sync_data`, a name after `sync_dir` of its directory.
/// Directories are durable as soon as they exist. [`SimFs::crash`] drops the rest.
#[derive(Debug, Clone, Default)]
pub struct SimFs {
    state: Arc<Mutex<State>>,
}

fn not_found(p: &Path) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, p.display().to_string())
}

impl SimFs {
    fn lock_state(&self) -> MutexGuard<'_, State> {
        self.state.lock()
    }

    /// Limits the bytes the files may hold in all, or lifts the limit with `None`. A write
    /// past it writes what fits and fails with `StorageFull`, as a full disk does.
    pub fn set_capacity(&self, bytes: Option<u64>) {
        self.lock_state().capacity = bytes;
    }

    /// Lets `n` more changes to the files succeed and fails every change after them, as a
    /// host that stopped part way would leave them; `None` lifts it, as [`Self::crash`] does.
    pub fn stop_after(&self, n: Option<u64>) {
        self.lock_state().stop_after = n;
    }

    /// Whether the changes [`Self::stop_after`] let through ran out.
    pub fn stopped(&self) -> bool {
        self.lock_state().stop_after == Some(0)
    }

    /// How many changes to the files succeeded so far.
    pub fn changes(&self) -> u64 {
        self.lock_state().changes
    }

    /// Lifts [`Self::stop_after`] and drops every lock, as when the process that held them dies
    /// and the host goes on: what it wrote stays, durable or not.
    pub fn end_process(&self) {
        let mut s = self.lock_state();
        s.stop_after = None;
        s.locks.clear();
    }

    /// Loses everything not yet durable, as a power cut would, and drops every lock.
    pub fn crash(&self) {
        let mut s = self.lock_state();
        s.stop_after = None;
        for d in s.data.values_mut() {
            d.current = d.durable.clone();
            d.blocks = d.durable_blocks.clone();
        }
        s.names = s.durable_names.clone();
        s.locks.clear();
    }

    fn inode(s: &State, p: &Path) -> io::Result<u64> {
        match s.names.get(p) {
            Some(Node::File(i)) => Ok(*i),
            Some(Node::Dir) => Err(io::Error::new(
                io::ErrorKind::IsADirectory,
                p.display().to_string(),
            )),
            None => Err(not_found(p)),
        }
    }

    fn parent_exists(s: &State, p: &Path) -> io::Result<()> {
        match p.parent() {
            Some(d) if d.as_os_str().is_empty() || s.names.get(d) == Some(&Node::Dir) => Ok(()),
            None => Ok(()),
            Some(d) => Err(not_found(d)),
        }
    }

    fn new_file(s: &mut State, p: &Path, bytes: Vec<u8>, blocks: BTreeMap<u64, u64>) -> u64 {
        s.next_inode += 1;
        let i = s.next_inode;
        s.data.insert(
            i,
            Data {
                current: bytes,
                blocks,
                ..Data::default()
            },
        );
        s.names.insert(p.to_path_buf(), Node::File(i));
        i
    }

    fn file(&self, inode: u64) -> Arc<dyn LocalFile> {
        Arc::new(SimFile {
            fs: self.clone(),
            inode,
        })
    }
}

#[async_trait]
impl LocalFs for SimFs {
    async fn open(&self, path: &Path, how: Open) -> io::Result<Arc<dyn LocalFile>> {
        let mut s = self.lock_state();
        let found = Self::inode(&s, path);
        let inode = match (how, found) {
            (Open::CreateNew, Ok(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    path.display().to_string(),
                ));
            }
            (Open::Create | Open::CreateNew, Err(e)) if e.kind() == io::ErrorKind::NotFound => {
                Self::parent_exists(&s, path)?;
                s.change()?;
                Self::new_file(&mut s, path, Vec::new(), BTreeMap::new())
            }
            (_, found) => found?,
        };
        drop(s);
        Ok(self.file(inode))
    }

    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut s = self.lock_state();
        s.change()?;
        let node = s.names.remove(from).ok_or_else(|| not_found(from))?;
        s.names.insert(to.to_path_buf(), node);
        Ok(())
    }

    async fn remove(&self, path: &Path) -> io::Result<()> {
        let mut s = self.lock_state();
        s.change()?;
        s.names.remove(path).map(|_| ()).ok_or_else(|| not_found(path))
    }

    async fn list(&self, dir: &Path) -> io::Result<Vec<DirEntry>> {
        let s = self.lock_state();
        if s.names.get(dir) != Some(&Node::Dir) {
            return Err(not_found(dir));
        }
        Ok(s.names
            .iter()
            .filter(|(p, _)| p.parent() == Some(dir))
            .filter_map(|(p, n)| {
                Some(DirEntry {
                    name: p.file_name()?.to_string_lossy().into_owned(),
                    len: match n {
                        Node::File(i) => s.data[i].len(),
                        Node::Dir => 0,
                    },
                    modified: SystemTime::UNIX_EPOCH,
                    is_dir: *n == Node::Dir,
                })
            })
            .collect())
    }

    async fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        let mut s = self.lock_state();
        for d in dir.ancestors().filter(|d| !d.as_os_str().is_empty()) {
            match s.names.get(d) {
                Some(Node::File(_)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotADirectory,
                        d.display().to_string(),
                    ));
                }
                Some(Node::Dir) => {}
                None => {
                    s.names.insert(d.to_path_buf(), Node::Dir);
                    s.durable_names.insert(d.to_path_buf(), Node::Dir);
                }
            }
        }
        Ok(())
    }

    async fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut s = self.lock_state();
        s.change()?;
        s.durable_names
            .retain(|p, n| *n == Node::Dir || p.parent() != Some(dir));
        let now: Vec<(PathBuf, Node)> = s
            .names
            .iter()
            .filter(|(p, n)| **n != Node::Dir && p.parent() == Some(dir))
            .map(|(p, n)| (p.clone(), *n))
            .collect();
        s.durable_names.extend(now);
        Ok(())
    }

    async fn lock(&self, path: &Path) -> io::Result<Box<dyn FileLock>> {
        let mut s = self.lock_state();
        if s.locks.contains_key(path) {
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                path.display().to_string(),
            ));
        }
        s.next_lock += 1;
        let id = s.next_lock;
        s.locks.insert(path.to_path_buf(), id);
        Ok(Box::new(SimLock {
            fs: self.clone(),
            path: path.to_path_buf(),
            id,
        }))
    }

    async fn try_lock(&self, path: &Path) -> io::Result<Box<dyn FileLock>> {
        self.lock(path).await
    }

    async fn available(&self, _dir: &Path) -> io::Result<u64> {
        Ok(self.lock_state().free())
    }
}

#[async_trait]
impl LocalImage for SimFs {
    async fn clone_file(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut s = self.lock_state();
        let src = Self::inode(&s, from)?;
        Self::parent_exists(&s, to)?;
        s.change()?;
        let (bytes, blocks) = (s.data[&src].current.clone(), s.data[&src].blocks.clone());
        Self::new_file(&mut s, to, bytes, blocks);
        Ok(())
    }

    async fn clone_ranges(&self, from: &Path, to: &Path, ranges: &[Range<u64>]) -> io::Result<()> {
        let mut s = self.lock_state();
        let src = Self::inode(&s, from)?;
        Self::parent_exists(&s, to)?;
        s.change()?;
        let d = &s.data[&src];
        let len = u64_from(d.current.len());
        let (mut parts, mut spans) = (Vec::new(), Vec::new());
        let mut blocks = BTreeMap::new();
        for r in ranges {
            let end = r.end.min(len);
            if end <= r.start {
                continue;
            }
            if r.start % BLOCK != 0 || (end % BLOCK != 0 && end != len) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("range {r:?} of {} is not block aligned", from.display()),
                ));
            }
            parts.push((
                usize_from(r.start),
                d.current[usize_from(r.start)..usize_from(end)].to_vec(),
            ));
            spans.push(r.start / BLOCK..end.div_ceil(BLOCK));
            blocks.extend(
                d.blocks
                    .range(r.start / BLOCK..end.div_ceil(BLOCK))
                    .map(|(b, p)| (*b, *p)),
            );
        }
        let dst = match s.names.get(to) {
            Some(Node::File(i)) => *i,
            Some(Node::Dir) => {
                return Err(io::Error::new(
                    io::ErrorKind::IsADirectory,
                    to.display().to_string(),
                ));
            }
            None => Self::new_file(&mut s, to, Vec::new(), BTreeMap::new()),
        };
        let Some(d) = s.data.get_mut(&dst) else {
            return Err(not_found(to));
        };
        if u64_from(d.current.len()) < len {
            d.current.resize(usize_from(len), 0);
        }
        for (at, b) in parts {
            d.current[at..at + b.len()].copy_from_slice(&b);
        }
        d.blocks.retain(|b, _| !spans.iter().any(|r| r.contains(b)));
        d.blocks.extend(blocks);
        Ok(())
    }

    async fn prepare(&self, path: &Path, _hint: Option<u64>) -> io::Result<()> {
        Self::inode(&self.lock_state(), path).map(|_| ())
    }

    async fn extents(&self, path: &Path) -> io::Result<Vec<Extent>> {
        let s = self.lock_state();
        let i = Self::inode(&s, path)?;
        let d = &s.data[&i];
        let len = u64_from(d.current.len());
        let mut out: Vec<Extent> = Vec::new();
        for (&b, &place) in &d.blocks {
            let (logical, physical) = (b * BLOCK, place * BLOCK);
            let n = BLOCK.min(len - logical);
            match out.last_mut() {
                Some(x) if x.logical + x.len == logical && x.physical + x.len == physical => x.len += n,
                _ => out.push(Extent {
                    logical,
                    physical,
                    len: n,
                    unwritten: false,
                }),
            }
        }
        Ok(out)
    }

    async fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<()> {
        let mut s = self.lock_state();
        s.change()?;
        let i = Self::inode(&s, path)?;
        let d = s.data.entry(i).or_default();
        let end = usize_from(offset + len).min(d.current.len());
        let start = usize_from(offset).min(end);
        d.current[start..end].fill(0);
        let (first, last) = (u64_from(start).div_ceil(BLOCK), u64_from(end) / BLOCK);
        d.blocks.retain(|b, _| *b < first || *b >= last);
        Ok(())
    }

    async fn allocated(&self, path: &Path) -> io::Result<u64> {
        let s = self.lock_state();
        let i = Self::inode(&s, path)?;
        Ok(s.data[&i]
            .current
            .chunks(4096)
            .filter(|b| b.iter().any(|&x| x != 0))
            .count() as u64
            * 4096)
    }
}

#[derive(Debug)]
struct SimFile {
    fs: SimFs,
    inode: u64,
}

impl SimFile {
    fn with<T>(&self, f: impl FnOnce(&mut Data) -> T) -> T {
        self.with_places(|d, _| f(d))
    }

    fn with_places<T>(&self, f: impl FnOnce(&mut Data, &mut u64) -> T) -> T {
        self.with_free(|d, next, _| f(d, next))
    }

    fn with_free<T>(&self, f: impl FnOnce(&mut Data, &mut u64, u64) -> T) -> T {
        let mut s = self.fs.lock_state();
        let free = s.free();
        let s = &mut *s;
        f(s.data.entry(self.inode).or_default(), &mut s.next_place, free)
    }

    /// [`Self::with_free`] for a change to the file, which [`SimFs::stop_after`] may fail.
    fn change<T>(&self, f: impl FnOnce(&mut Data, &mut u64, u64) -> io::Result<T>) -> io::Result<T> {
        let mut s = self.fs.lock_state();
        s.change()?;
        let free = s.free();
        let s = &mut *s;
        f(s.data.entry(self.inode).or_default(), &mut s.next_place, free)
    }

    /// Writes `bufs` from `offset` as far as the free space lets the file grow.
    fn write_bufs(&self, bufs: &[Bytes], offset: u64) -> io::Result<()> {
        self.change(|d, next, free| {
            let limit = usize_from(d.len().saturating_add(free).min(u64_from(usize::MAX)));
            let mut at = usize_from(offset);
            for b in bufs {
                let n = b.len().min(limit.saturating_sub(at));
                if d.current.len() < at + n {
                    d.current.resize(at + n, 0);
                }
                d.current[at..at + n].copy_from_slice(&b[..n]);
                d.write(u64_from(at), u64_from(n), next);
                if n < b.len() {
                    return Err(full());
                }
                at += n;
            }
            Ok(())
        })
    }
}

#[async_trait]
impl LocalFile for SimFile {
    async fn read_at(&self, len: usize, offset: u64) -> io::Result<Bytes> {
        Ok(self.with(|d| {
            let end = usize_from(offset).saturating_add(len).min(d.current.len());
            let start = usize_from(offset).min(end);
            Bytes::copy_from_slice(&d.current[start..end])
        }))
    }

    fn read_at_nowait(&self, buf: &mut [u8], offset: u64) -> io::Result<Option<usize>> {
        Ok(Some(self.with(|d| {
            let end = usize_from(offset).saturating_add(buf.len()).min(d.current.len());
            let start = usize_from(offset).min(end);
            buf[..end - start].copy_from_slice(&d.current[start..end]);
            end - start
        })))
    }

    async fn write_at(&self, bufs: Vec<Bytes>, offset: u64) -> io::Result<()> {
        self.write_bufs(&bufs, offset)
    }

    fn write_at_now(&self, b: &[u8], offset: u64) -> io::Result<()> {
        self.write_bufs(&[Bytes::copy_from_slice(b)], offset)
    }

    async fn sync_data(&self) -> io::Result<()> {
        self.change(|d, _, _| {
            d.durable = d.current.clone();
            d.durable_blocks = d.blocks.clone();
            Ok(())
        })
    }

    async fn set_len(&self, len: u64) -> io::Result<()> {
        self.change(|d, _, free| {
            if len > d.len() && len - d.len() > free {
                return Err(full());
            }
            d.current.resize(usize_from(len), 0);
            d.reserved = d.reserved.min(len);
            d.truncate(len);
            Ok(())
        })
    }

    async fn allocate(&self, len: u64) -> io::Result<()> {
        self.change(|d, _, free| {
            if len > d.len() {
                if len - d.len() > free {
                    return Err(full());
                }
                d.reserved = len;
            }
            Ok(())
        })
    }

    async fn punch(&self, offset: u64, len: u64) -> io::Result<()> {
        self.change(|d, _, _| {
            let end = usize_from(offset + len).min(d.current.len());
            let start = usize_from(offset).min(end);
            d.current[start..end].fill(0);
            let (first, last) = (u64_from(start).div_ceil(BLOCK), u64_from(end) / BLOCK);
            d.blocks.retain(|b, _| *b < first || *b >= last);
            Ok(())
        })
    }

    async fn size(&self) -> io::Result<u64> {
        Ok(self.with(|d| d.len()))
    }

    async fn start_writeback(&self) {}

    async fn write_behind(&self, _below: u64) {}

    fn advise(&self, _offset: u64, _len: u64, _advice: Advice) {}
}

#[derive(Debug)]
struct SimLock {
    fs: SimFs,
    path: PathBuf,
    id: u64,
}

impl FileLock for SimLock {}

impl Drop for SimLock {
    fn drop(&mut self) {
        let mut s = self.fs.lock_state();
        if s.locks.get(&self.path) == Some(&self.id) {
            s.locks.remove(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_crash_keeps_only_synced_data_and_names() {
        let fs = SimFs::default();
        let dir = Path::new("/v");
        fs.create_dir_all(dir).await.unwrap();
        let a = fs.open(&dir.join("a"), Open::Create).await.unwrap();
        a.write_at(vec![Bytes::from_static(b"kept")], 0).await.unwrap();
        a.sync_data().await.unwrap();
        fs.sync_dir(dir).await.unwrap();
        a.write_at(vec![Bytes::from_static(b"lost")], 4).await.unwrap();
        let b = fs.open(&dir.join("b"), Open::Create).await.unwrap();
        b.write_at(vec![Bytes::from_static(b"x")], 0).await.unwrap();
        b.sync_data().await.unwrap();

        fs.crash();

        let a = fs.open(&dir.join("a"), Open::Read).await.unwrap();
        assert_eq!(&a.read_at(100, 0).await.unwrap()[..], b"kept");
        assert!(
            fs.open(&dir.join("b"), Open::Read).await.is_err(),
            "b's name was never synced"
        );
    }

    #[tokio::test]
    async fn a_lock_is_exclusive_until_dropped() {
        let fs = SimFs::default();
        let g = fs.lock(Path::new("/l")).await.unwrap();
        assert!(fs.lock(Path::new("/l")).await.is_err());
        drop(g);
        assert!(fs.lock(Path::new("/l")).await.is_ok());
    }
}
