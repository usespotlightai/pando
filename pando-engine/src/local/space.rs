use parking_lot::Mutex;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::time::{Duration, Instant};

use crate::ports::{LocalFs, Open};
use crate::{Error, Result};

/// How often a full [`Space`] looks for room again.
const CHECK_EVERY: Duration = Duration::from_secs(1);

/// Headroom on the disk of a directory, so a commit always finds room for what it writes.
///
/// A reserve file holds `reserve` bytes while the disk has room. When a write finds the disk
/// full, [`Space::release`] removes the file: the commits that follow write into its space,
/// and writes fail at once until the disk has twice `reserve` bytes free, when the file is
/// allocated again.
#[derive(Debug)]
pub(crate) struct Space {
    fs: Arc<dyn LocalFs>,
    dir: PathBuf,
    path: PathBuf,
    reserve: u64,
    full: AtomicBool,
    released: AtomicU64,
    turn: tokio::sync::Mutex<()>,
    next_check: Mutex<Option<Instant>>,
}

/// Whether `e` says that a disk or a quota has no room.
pub(crate) fn no_room(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded
    )
}

impl Space {
    /// Allocates the reserve file `name` of `reserve` bytes in `dir`. A disk without room for
    /// it starts full.
    pub(crate) async fn open(fs: Arc<dyn LocalFs>, dir: &Path, name: &str, reserve: u64) -> Result<Space> {
        let s = Space {
            fs,
            dir: dir.to_path_buf(),
            path: dir.join(name),
            reserve,
            full: AtomicBool::new(false),
            released: AtomicU64::new(0),
            turn: tokio::sync::Mutex::new(()),
            next_check: Mutex::new(None),
        };
        match s.hold().await {
            Ok(()) => {}
            Err(e) if no_room(&e) => {
                let _ = s.fs.remove(&s.path).await;
                s.full.store(true, Ordering::Release);
            }
            Err(e) => return Err(e.into()),
        }
        Ok(s)
    }

    async fn hold(&self) -> io::Result<()> {
        self.fs
            .open(&self.path, Open::Create)
            .await?
            .allocate(self.reserve)
            .await
    }

    /// Whether writes fail for want of room.
    pub(crate) fn is_full(&self) -> bool {
        self.full.load(Ordering::Acquire)
    }

    /// How many times the reserve has been freed.
    pub(crate) fn released(&self) -> u64 {
        self.released.load(Ordering::Acquire)
    }

    /// Frees the reserve for the commits to come and fails writes from now on. It returns
    /// [`Space::released`] once the reserve is gone, whether this call or another freed it.
    pub(crate) async fn release(&self) -> u64 {
        let _turn = self.turn.lock().await;
        if !self.full.swap(true, Ordering::AcqRel) {
            *self.next_check.lock() = Some(Instant::now() + CHECK_EVERY);
            let _ = self.fs.remove(&self.path).await;
            self.released.fetch_add(1, Ordering::AcqRel);
        }
        self.released()
    }

    /// Succeeds while writes may go on: always while the space has room, and once a full
    /// space finds twice its reserve free again and holds the reserve anew. It looks at most
    /// once a [`CHECK_EVERY`].
    ///
    /// # Errors
    ///
    /// [`Space::full_error`] while the space is full.
    pub(crate) async fn admit(&self) -> Result<()> {
        if self.is_full() && !self.try_restore().await {
            return Err(self.full_error());
        }
        Ok(())
    }

    async fn try_restore(&self) -> bool {
        {
            let mut next = self.next_check.lock();
            let now = Instant::now();
            if next.is_some_and(|n| now < n) {
                return false;
            }
            *next = Some(now + CHECK_EVERY);
        }
        let room = self.fs.available(&self.dir).await;
        if !room.is_ok_and(|n| n >= self.reserve.saturating_mul(2)) {
            return false;
        }
        let _turn = self.turn.lock().await;
        if !self.is_full() {
            return true;
        }
        if self.hold().await.is_err() {
            let _ = self.fs.remove(&self.path).await;
            return false;
        }
        self.full.store(false, Ordering::Release);
        true
    }

    /// The error of a write refused while the space is full: `StorageFull`, which a block
    /// device reports as ENOSPC.
    pub(crate) fn full_error(&self) -> Error {
        Error::Io(io::Error::new(
            io::ErrorKind::StorageFull,
            format!(
                "the disk of {} is full: writes fail until {} MiB are free",
                self.dir.display(),
                self.reserve.saturating_mul(2) >> 20
            ),
        ))
    }
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use super::*;
    use crate::sim::SimFs;

    #[tokio::test(start_paused = true)]
    async fn a_released_reserve_returns_once_twice_its_size_is_free() {
        let fs = SimFs::default();
        let dir = Path::new("/c");
        fs.create_dir_all(dir).await.unwrap();
        fs.set_capacity(Some(1000));
        let s = Space::open(Arc::new(fs.clone()), dir, "reserve", 100)
            .await
            .unwrap();
        s.admit().await.unwrap();
        let other = fs.open(Path::new("/c/other"), Open::Create).await.unwrap();
        other.allocate(900).await.unwrap();
        assert!(other.allocate(901).await.is_err());

        assert_eq!(s.released(), 0);
        assert_eq!(s.release().await, 1);
        assert_eq!(s.release().await, 1, "a second release frees nothing");
        assert!(s.is_full());
        other.allocate(950).await.unwrap();
        assert!(s.admit().await.is_err());

        other.set_len(810).await.unwrap();
        tokio::time::advance(CHECK_EVERY).await;
        assert!(
            s.admit().await.is_err(),
            "190 bytes free is less than twice the reserve"
        );
        other.set_len(800).await.unwrap();
        assert!(s.admit().await.is_err(), "it looks again only after CHECK_EVERY");
        tokio::time::advance(CHECK_EVERY).await;
        s.admit().await.unwrap();
        assert!(!s.is_full());
        assert_eq!(fs.available(dir).await.unwrap(), 100);
    }

    #[tokio::test]
    async fn a_disk_without_room_for_the_reserve_starts_full() {
        let fs = SimFs::default();
        let dir = Path::new("/c");
        fs.create_dir_all(dir).await.unwrap();
        fs.set_capacity(Some(50));
        let s = Space::open(Arc::new(fs.clone()), dir, "reserve", 100)
            .await
            .unwrap();
        assert!(s.is_full());
        let e = s.admit().await.unwrap_err();
        assert!(e.is_no_room(), "{e}");
    }
}
