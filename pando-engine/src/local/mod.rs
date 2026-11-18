//! This host's own files: the object log, the journal, small state files.

mod journal;
mod object_log;
mod space;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
pub(crate) use journal::{Journal, Record};
pub(crate) use object_log::{ObjectLog, WritebackHold};
use pando_format::usize_from;
pub(crate) use space::{Space, no_room};

use crate::ports::{LocalFs, Open};

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

fn tmp_path(p: &Path) -> PathBuf {
    let mut s = p.as_os_str().to_owned();
    s.push(format!(".{}.tmp", TMP_SEQ.fetch_add(1, Ordering::Relaxed)));
    PathBuf::from(s)
}

/// Replaces `p` with `b` so that a crash leaves either the old or the new content, and the new
/// content is durable when this returns.
pub(crate) async fn write_durable(fs: &dyn LocalFs, p: &Path, b: Bytes) -> io::Result<()> {
    let dir = p.parent().unwrap_or_else(|| Path::new(""));
    fs.create_dir_all(dir).await?;
    let tmp = tmp_path(p);
    let f = fs.open(&tmp, Open::CreateNew).await?;
    f.write_at(vec![b], 0).await?;
    f.sync_data().await?;
    fs.rename(&tmp, p).await?;
    fs.sync_dir(dir).await
}

/// Replaces `p` with `b` atomically, without waiting for the disk.
pub(crate) async fn write_atomic(fs: &dyn LocalFs, p: &Path, b: Bytes) -> io::Result<()> {
    let dir = p.parent().unwrap_or_else(|| Path::new(""));
    fs.create_dir_all(dir).await?;
    let tmp = tmp_path(p);
    let f = fs.open(&tmp, Open::CreateNew).await?;
    f.write_at(vec![b], 0).await?;
    fs.rename(&tmp, p).await
}

/// The whole content of `p`, or `None` when it does not exist.
pub(crate) async fn read_file(fs: &dyn LocalFs, p: &Path) -> io::Result<Option<Bytes>> {
    match fs.open(p, Open::Read).await {
        Ok(f) => {
            let n = f.size().await?;
            Ok(Some(f.read_at(usize_from(n), 0).await?))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// The first `N` bytes of `b`, which then starts after them; `None` when `b` is shorter.
pub(crate) fn take<const N: usize>(b: &mut &[u8]) -> Option<[u8; N]> {
    let (head, rest) = b.split_first_chunk::<N>()?;
    *b = rest;
    Some(*head)
}
