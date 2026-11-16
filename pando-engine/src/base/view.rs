//! Reads of a branch's unchanged chunks from the base image of the root it opened at.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::ports::{LocalFile, LocalFs, Open};

/// The base image a branch reads its unchanged chunks from.
#[derive(Debug)]
pub(crate) struct BaseView {
    file: Arc<dyn LocalFile>,
    served: AtomicU64,
}

impl BaseView {
    pub(crate) async fn open(fs: &dyn LocalFs, image: &Path) -> io::Result<BaseView> {
        Ok(BaseView {
            file: fs.open(image, Open::Read).await?,
            served: AtomicU64::new(0),
        })
    }

    /// The `n` bytes at `off`, or `None` when the image has fewer.
    pub(crate) async fn read(&self, off: u64, n: usize) -> Option<Bytes> {
        let b = self.file.read_at(n, off).await.ok()?;
        (b.len() == n).then(|| {
            self.served.fetch_add(n as u64, Ordering::Relaxed);
            b
        })
    }

    /// Fills `dst` from `off` when the bytes are in the page cache.
    pub(crate) fn read_nowait(&self, off: u64, dst: &mut [u8]) -> bool {
        let hit = matches!(self.file.read_at_nowait(dst, off), Ok(Some(k)) if k == dst.len());
        if hit {
            self.served.fetch_add(dst.len() as u64, Ordering::Relaxed);
        }
        hit
    }

    /// The bytes reads took from the image.
    pub(crate) fn served(&self) -> u64 {
        self.served.load(Ordering::Relaxed)
    }
}
