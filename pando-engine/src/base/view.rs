//! Reads of a branch's unchanged chunks from the base image of the root it opened at.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::ports::{LocalFile, LocalFs, Open};

fn short(off: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!("the base image ends before {off}"),
    )
}

/// The base image a branch reads its unchanged chunks from.
#[derive(Debug)]
pub(crate) struct BaseView {
    file: Arc<dyn LocalFile>,
    size: u64,
    served: AtomicU64,
}

impl BaseView {
    pub(crate) async fn open(fs: &dyn LocalFs, image: &Path) -> io::Result<BaseView> {
        let file = fs.open(image, Open::Read).await?;
        Ok(BaseView {
            size: file.size().await?,
            file,
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

    /// The `n` bytes at `off`, with zeros past the end of the image.
    pub(crate) async fn read_padded(&self, off: u64, n: usize) -> io::Result<Bytes> {
        let have = self.within(off, n);
        if have == n {
            return self.read(off, n).await.ok_or_else(|| short(off));
        }
        let mut b = vec![0; n];
        if have > 0 {
            b[..have].copy_from_slice(&self.read(off, have).await.ok_or_else(|| short(off))?);
        }
        Ok(b.into())
    }

    /// [`Self::read_padded`] into `dst` from the page cache only: `false` when it would wait.
    pub(crate) fn read_padded_nowait(&self, off: u64, dst: &mut [u8]) -> bool {
        let have = self.within(off, dst.len());
        dst[have..].fill(0);
        have == 0 || self.read_nowait(off, &mut dst[..have])
    }

    /// How many of the `n` bytes at `off` the image has.
    fn within(&self, off: u64, n: usize) -> usize {
        usize::try_from(self.size.saturating_sub(off)).map_or(n, |left| left.min(n))
    }

    /// The bytes reads took from the image.
    pub(crate) fn served(&self) -> u64 {
        self.served.load(Ordering::Relaxed)
    }
}
