use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom};
use std::ops::Range;
use std::os::unix::fs::{FileExt, FileTypeExt, MetadataExt};
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
        let f = Arc::clone(&self.file);
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
        let f = Arc::clone(&self.file);
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

    fn write_at_now(&self, b: &[u8], offset: u64) -> io::Result<()> {
        self.file.write_all_at(b, offset)
    }

    async fn sync_data(&self) -> io::Result<()> {
        let f = Arc::clone(&self.file);
        blocking(move || f.sync_data()).await
    }

    async fn set_len(&self, len: u64) -> io::Result<()> {
        let f = Arc::clone(&self.file);
        blocking(move || f.set_len(len)).await
    }

    async fn size(&self) -> io::Result<u64> {
        let f = Arc::clone(&self.file);
        blocking(move || {
            let m = f.metadata()?;
            if m.file_type().is_block_device() {
                return (&*f).seek(SeekFrom::End(0));
            }
            Ok(m.len())
        })
        .await
    }

    async fn start_writeback(&self) {
        #[cfg(target_os = "linux")]
        {
            let f = Arc::clone(&self.file);
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

fn copy_range(src: &File, dst: &File, start: u64, end: u64) -> io::Result<()> {
    let mut buf = vec![0u8; usize::try_from(end - start).map_or(1 << 20, |r| r.min(1 << 20))];
    let mut at = start;
    while at < end {
        let n = usize::try_from(end - at).map_or(buf.len(), |r| r.min(buf.len()));
        src.read_exact_at(&mut buf[..n], at)?;
        dst.write_all_at(&buf[..n], at)?;
        at += n as u64;
    }
    Ok(())
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

    async fn clone_ranges(&self, from: &Path, to: &Path, ranges: &[Range<u64>]) -> io::Result<()> {
        let (from, to, ranges) = (from.to_path_buf(), to.to_path_buf(), ranges.to_vec());
        blocking(move || {
            let src = File::open(&from)?;
            let len = src.metadata()?.len();
            let dst = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&to)?;
            if dst.metadata()?.len() < len {
                dst.set_len(len)?;
            }
            #[cfg(target_os = "linux")]
            let mut share = true;
            for r in ranges {
                let end = r.end.min(len);
                if end <= r.start {
                    continue;
                }
                #[cfg(target_os = "linux")]
                if share {
                    if crate::linux::reflink_range(&src, &dst, r.start, end - r.start)? {
                        continue;
                    }
                    share = false;
                }
                copy_range(&src, &dst, r.start, end)?;
            }
            Ok(())
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

    #[tokio::test(flavor = "multi_thread")]
    async fn a_range_clone_holds_only_its_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (dir.path().join("a"), dir.path().join("b"));
        let mut want = vec![0u8; (3 << 16) + 100];
        let src: Vec<u8> = (0..want.len())
            .map(|i| u8::try_from(i % 251).unwrap() + 1)
            .collect();
        let f = HostFs.open(&a, Open::CreateNew).await.unwrap();
        f.write_at(vec![Bytes::from(src.clone())], 0).await.unwrap();
        f.sync_data().await.unwrap();
        HostFs.open(&b, Open::CreateNew).await.unwrap();
        let ranges = [0..1 << 16, 2 << 16..4 << 16];
        HostFs.clone_ranges(&a, &b, &ranges).await.unwrap();
        want[..1 << 16].copy_from_slice(&src[..1 << 16]);
        want[2 << 16..].copy_from_slice(&src[2 << 16..]);
        let got = HostFs
            .open(&b, Open::Read)
            .await
            .unwrap()
            .read_at(src.len() + 10, 0)
            .await
            .unwrap();
        assert_eq!(&got[..], &want[..]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_range_clone_into_a_file_keeps_its_other_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (dir.path().join("a"), dir.path().join("b"));
        let f = HostFs.open(&a, Open::CreateNew).await.unwrap();
        f.write_at(vec![Bytes::from(vec![1u8; 3 << 16])], 0)
            .await
            .unwrap();
        HostFs
            .clone_ranges(&a, &b, &[0..1 << 16, 1 << 16..3 << 16])
            .await
            .unwrap();
        f.write_at(vec![Bytes::from(vec![2u8; 4 << 16])], 0)
            .await
            .unwrap();
        f.sync_data().await.unwrap();
        HostFs
            .clone_ranges(&a, &b, &[1 << 16..2 << 16, 3 << 16..4 << 16])
            .await
            .unwrap();
        let got = HostFs
            .open(&b, Open::Read)
            .await
            .unwrap()
            .read_at(5 << 16, 0)
            .await
            .unwrap();
        let mut want = vec![1u8; 4 << 16];
        want[1 << 16..2 << 16].fill(2);
        want[3 << 16..].fill(2);
        assert_eq!(&got[..], &want[..]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_file_reports_its_length() {
        let dir = tempfile::tempdir().unwrap();
        let f = HostFs.open(&dir.path().join("a"), Open::CreateNew).await.unwrap();
        f.set_len(5 << 20).await.unwrap();
        assert_eq!(f.size().await.unwrap(), 5 << 20);
        f.write_at(vec![Bytes::from_static(b"x")], 6 << 20).await.unwrap();
        assert_eq!(f.size().await.unwrap(), (6 << 20) + 1);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a block device named in PANDO_TEST_BLOCK_DEVICE, such as /dev/loop0"]
    async fn a_block_device_reports_its_size() {
        let dev = PathBuf::from(std::env::var("PANDO_TEST_BLOCK_DEVICE").unwrap());
        let name = dev.file_name().unwrap().to_str().unwrap();
        let sectors: u64 = std::fs::read_to_string(format!("/sys/class/block/{name}/size"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let f = HostFs.open(&dev, Open::Read).await.unwrap();
        assert_eq!(f.size().await.unwrap(), sectors * 512);
        assert_eq!(f.read_at(512, 0).await.unwrap().len(), 512);
    }
}
