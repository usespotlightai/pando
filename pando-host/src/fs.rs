use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use pando_engine::ports::{Advice, DirEntry, Extent, FileLock, LocalFile, LocalFs, LocalImage, Open};

/// Files through the OS. Blocking calls run on tokio's blocking pool.
#[derive(Debug, Default)]
pub struct HostFs;

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> io::Result<T> + Send + 'static) -> io::Result<T> {
    tokio::task::spawn_blocking(f).await.map_err(io::Error::other)?
}

#[async_trait]
impl LocalFs for HostFs {
    async fn open(&self, path: &Path, how: Open) -> io::Result<Arc<dyn LocalFile>> {
        let path = path.to_path_buf();
        let file = blocking(move || {
            let mut o = OpenOptions::new();
            o.read(true);
            match how {
                Open::Read => {}
                Open::ReadWrite => {
                    o.write(true);
                }
                Open::Create => {
                    o.write(true).create(true);
                }
                Open::CreateNew => {
                    o.write(true).create_new(true);
                }
            }
            o.open(path)
        })
        .await?;
        Ok(Arc::new(HostFile { file: Arc::new(file) }))
    }

    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let (from, to) = (from.to_path_buf(), to.to_path_buf());
        blocking(move || std::fs::rename(from, to)).await
    }

    async fn remove(&self, path: &Path) -> io::Result<()> {
        let path = path.to_path_buf();
        blocking(move || std::fs::remove_file(path)).await
    }

    async fn list(&self, dir: &Path) -> io::Result<Vec<DirEntry>> {
        let dir = dir.to_path_buf();
        blocking(move || {
            let mut out = Vec::new();
            for e in std::fs::read_dir(dir)? {
                let e = e?;
                let m = e.metadata()?;
                out.push(DirEntry {
                    name: e.file_name().to_string_lossy().into_owned(),
                    len: m.len(),
                    modified: m.modified()?,
                    is_dir: m.is_dir(),
                });
            }
            out.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(out)
        })
        .await
    }

    async fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        let dir = dir.to_path_buf();
        blocking(move || std::fs::create_dir_all(dir)).await
    }

    async fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let dir = dir.to_path_buf();
        blocking(move || File::open(dir)?.sync_all()).await
    }

    async fn lock(&self, path: &Path) -> io::Result<Box<dyn FileLock>> {
        let path = path.to_path_buf();
        let file = blocking(move || {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?;
            f.lock()?;
            Ok(f)
        })
        .await?;
        Ok(Box::new(HostLock { _file: file }))
    }

    async fn try_lock(&self, path: &Path) -> io::Result<Box<dyn FileLock>> {
        let path = path.to_path_buf();
        let file = blocking(move || {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?;
            match f.try_lock() {
                Ok(()) => Ok(f),
                Err(std::fs::TryLockError::WouldBlock) => Err(io::Error::new(
                    io::ErrorKind::ResourceBusy,
                    format!("{} is held by another process", path.display()),
                )),
                Err(std::fs::TryLockError::Error(e)) => Err(e),
            }
        })
        .await?;
        Ok(Box::new(HostLock { _file: file }))
    }
}

#[derive(Debug)]
struct HostLock {
    _file: File,
}

impl FileLock for HostLock {}

#[derive(Debug)]
struct HostFile {
    file: Arc<File>,
}

#[async_trait]
impl LocalFile for HostFile {
    async fn read_at(&self, len: usize, offset: u64) -> io::Result<Bytes> {
        let f = self.file.clone();
        blocking(move || {
            let mut buf = BytesMut::zeroed(len);
            let mut got = 0;
            while got < len {
                match f.read_at(&mut buf[got..], offset + got as u64) {
                    Ok(0) => break,
                    Ok(n) => got += n,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            }
            buf.truncate(got);
            Ok(buf.freeze())
        })
        .await
    }

    fn read_at_nowait(&self, buf: &mut [u8], offset: u64) -> io::Result<Option<usize>> {
        #[cfg(target_os = "linux")]
        return crate::linux::read_nowait(&self.file, buf, offset);
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (buf, offset);
            Ok(None)
        }
    }

    async fn write_at(&self, bufs: Vec<Bytes>, offset: u64) -> io::Result<()> {
        let f = self.file.clone();
        blocking(move || {
            let mut at = offset;
            for b in bufs {
                f.write_all_at(&b, at)?;
                at += b.len() as u64;
            }
            Ok(())
        })
        .await
    }

    async fn sync_data(&self) -> io::Result<()> {
        let f = self.file.clone();
        blocking(move || f.sync_data()).await
    }

    async fn set_len(&self, len: u64) -> io::Result<()> {
        let f = self.file.clone();
        blocking(move || f.set_len(len)).await
    }

    async fn size(&self) -> io::Result<u64> {
        let f = self.file.clone();
        blocking(move || Ok(f.metadata()?.len())).await
    }

    async fn start_writeback(&self) {
        #[cfg(target_os = "linux")]
        {
            let f = self.file.clone();
            let _ = blocking(move || {
                crate::linux::start_writeback(&f);
                Ok(())
            })
            .await;
        }
    }

    fn advise(&self, offset: u64, len: u64, advice: Advice) {
        #[cfg(target_os = "linux")]
        crate::linux::advise(&self.file, offset, len, advice);
        #[cfg(not(target_os = "linux"))]
        let _ = (offset, len, advice);
    }
}

#[async_trait]
impl LocalImage for HostFs {
    async fn clone_file(&self, from: &Path, to: &Path) -> io::Result<()> {
        let (from, to) = (from.to_path_buf(), to.to_path_buf());
        blocking(move || {
            #[cfg(target_os = "linux")]
            if crate::linux::reflink(&from, &to)? {
                return Ok(());
            }
            std::fs::copy(&from, &to).map(|_| ())
        })
        .await
    }

    async fn prepare(&self, path: &Path, hint: Option<u64>) -> io::Result<()> {
        let path: PathBuf = path.to_path_buf();
        blocking(move || {
            #[cfg(target_os = "linux")]
            if let Some(h) = hint {
                let _refused_off_xfs = crate::linux::extent_hint(&File::open(&path)?, h);
            }
            #[cfg(not(target_os = "linux"))]
            let _ = (path, hint);
            Ok(())
        })
        .await
    }

    async fn extents(&self, path: &Path) -> io::Result<Vec<Extent>> {
        let path = path.to_path_buf();
        blocking(move || {
            #[cfg(target_os = "linux")]
            return crate::linux::extents(&File::open(path)?);
            #[cfg(not(target_os = "linux"))]
            {
                let _ = path;
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "extent maps need Linux",
                ))
            }
        })
        .await
    }

    async fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<()> {
        let path = path.to_path_buf();
        blocking(move || {
            let f = OpenOptions::new().write(true).open(path)?;
            #[cfg(target_os = "linux")]
            return crate::linux::punch_hole(&f, offset, len);
            #[cfg(not(target_os = "linux"))]
            {
                let zeros = vec![0; len.min(1 << 20) as usize];
                let mut at = offset;
                while at < offset + len {
                    let n = zeros.len().min((offset + len - at) as usize);
                    f.write_all_at(&zeros[..n], at)?;
                    at += n as u64;
                }
                Ok(())
            }
        })
        .await
    }

    async fn allocated(&self, path: &Path) -> io::Result<u64> {
        let path = path.to_path_buf();
        blocking(move || Ok(std::fs::metadata(path)?.blocks() * 512)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn files_write_read_clone_and_punch() {
        let dir = tempfile::tempdir().unwrap();
        let fs = HostFs;
        let a = dir.path().join("a");
        let f = fs.open(&a, Open::CreateNew).await.unwrap();
        f.write_at(vec![Bytes::from(vec![7u8; 8192])], 0).await.unwrap();
        f.sync_data().await.unwrap();
        fs.sync_dir(dir.path()).await.unwrap();
        assert_eq!(f.read_at(10_000, 4096).await.unwrap().len(), 4096);
        let b = dir.path().join("b");
        fs.clone_file(&a, &b).await.unwrap();
        fs.punch_hole(&b, 0, 4096).await.unwrap();
        let cb = fs
            .open(&b, Open::Read)
            .await
            .unwrap()
            .read_at(8192, 0)
            .await
            .unwrap();
        assert!(cb[..4096].iter().all(|&x| x == 0) && cb[4096..].iter().all(|&x| x == 7));
        assert_eq!(
            f.read_at(1, 0).await.unwrap()[0],
            7,
            "the clone does not change the source"
        );
        let names: Vec<String> = fs
            .list(dir.path())
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(names, ["a", "b"]);
        let _held = fs.lock(&dir.path().join("lock")).await.unwrap();
    }
}
