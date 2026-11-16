use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;

use crate::ports::{Advice, DirEntry, Extent, FileLock, LocalFile, LocalFs, LocalImage, Open};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Node {
    File(u64),
    Dir,
}

#[derive(Debug, Default)]
struct Data {
    current: Vec<u8>,
    durable: Vec<u8>,
}

#[derive(Debug, Default)]
struct State {
    data: HashMap<u64, Data>,
    names: BTreeMap<PathBuf, Node>,
    durable_names: BTreeMap<PathBuf, Node>,
    next_inode: u64,
    locks: BTreeMap<PathBuf, u64>,
    next_lock: u64,
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
        self.state.lock().expect("sim fs lock")
    }

    /// Loses everything not yet durable, as a power cut would, and drops every lock.
    pub fn crash(&self) {
        let mut s = self.lock_state();
        for d in s.data.values_mut() {
            d.current = d.durable.clone();
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

    fn new_file(s: &mut State, p: &Path, bytes: Vec<u8>) -> u64 {
        s.next_inode += 1;
        let i = s.next_inode;
        s.data.insert(
            i,
            Data {
                current: bytes,
                durable: Vec::new(),
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
                Self::new_file(&mut s, path, Vec::new())
            }
            (_, found) => found?,
        };
        drop(s);
        Ok(self.file(inode))
    }

    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut s = self.lock_state();
        let node = s.names.remove(from).ok_or_else(|| not_found(from))?;
        s.names.insert(to.to_path_buf(), node);
        Ok(())
    }

    async fn remove(&self, path: &Path) -> io::Result<()> {
        self.lock_state()
            .names
            .remove(path)
            .map(|_| ())
            .ok_or_else(|| not_found(path))
    }

    async fn list(&self, dir: &Path) -> io::Result<Vec<DirEntry>> {
        let s = self.lock_state();
        if s.names.get(dir) != Some(&Node::Dir) {
            return Err(not_found(dir));
        }
        Ok(s.names
            .iter()
            .filter(|(p, _)| p.parent() == Some(dir))
            .map(|(p, n)| DirEntry {
                name: p
                    .file_name()
                    .expect("a child has a name")
                    .to_string_lossy()
                    .into_owned(),
                len: match n {
                    Node::File(i) => s.data[i].current.len() as u64,
                    Node::Dir => 0,
                },
                modified: SystemTime::UNIX_EPOCH,
                is_dir: *n == Node::Dir,
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
}

#[async_trait]
impl LocalImage for SimFs {
    async fn clone_file(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut s = self.lock_state();
        let src = Self::inode(&s, from)?;
        Self::parent_exists(&s, to)?;
        let bytes = s.data[&src].current.clone();
        Self::new_file(&mut s, to, bytes);
        Ok(())
    }

    async fn prepare(&self, path: &Path, _hint: Option<u64>) -> io::Result<()> {
        Self::inode(&self.lock_state(), path).map(|_| ())
    }

    async fn extents(&self, path: &Path) -> io::Result<Vec<Extent>> {
        let s = self.lock_state();
        let i = Self::inode(&s, path)?;
        let len = s.data[&i].current.len() as u64;
        Ok(if len == 0 {
            Vec::new()
        } else {
            vec![Extent {
                logical: 0,
                physical: i << 40,
                len,
                shared: false,
            }]
        })
    }

    async fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<()> {
        let mut s = self.lock_state();
        let i = Self::inode(&s, path)?;
        let d = s.data.get_mut(&i).expect("inode has data");
        let end = (offset + len).min(d.current.len() as u64) as usize;
        let start = (offset as usize).min(end);
        d.current[start..end].fill(0);
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
        f(self
            .fs
            .lock_state()
            .data
            .get_mut(&self.inode)
            .expect("open file has data"))
    }
}

#[async_trait]
impl LocalFile for SimFile {
    async fn read_at(&self, len: usize, offset: u64) -> io::Result<Bytes> {
        Ok(self.with(|d| {
            let end = (offset as usize).saturating_add(len).min(d.current.len());
            let start = (offset as usize).min(end);
            Bytes::copy_from_slice(&d.current[start..end])
        }))
    }

    fn read_at_nowait(&self, buf: &mut [u8], offset: u64) -> io::Result<Option<usize>> {
        Ok(Some(self.with(|d| {
            let end = (offset as usize).saturating_add(buf.len()).min(d.current.len());
            let start = (offset as usize).min(end);
            buf[..end - start].copy_from_slice(&d.current[start..end]);
            end - start
        })))
    }

    async fn write_at(&self, bufs: Vec<Bytes>, offset: u64) -> io::Result<()> {
        self.with(|d| {
            let mut at = offset as usize;
            for b in bufs {
                if d.current.len() < at + b.len() {
                    d.current.resize(at + b.len(), 0);
                }
                d.current[at..at + b.len()].copy_from_slice(&b);
                at += b.len();
            }
        });
        Ok(())
    }

    async fn sync_data(&self) -> io::Result<()> {
        self.with(|d| d.durable = d.current.clone());
        Ok(())
    }

    async fn set_len(&self, len: u64) -> io::Result<()> {
        self.with(|d| d.current.resize(len as usize, 0));
        Ok(())
    }

    async fn size(&self) -> io::Result<u64> {
        Ok(self.with(|d| d.current.len() as u64))
    }

    async fn start_writeback(&self) {}

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
