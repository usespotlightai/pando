//! This host's own files: the object log, the journal, small state files.

mod journal;
mod object_log;
mod space;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
pub(crate) use journal::{Journal, Record};
pub(crate) use object_log::{ObjectLog, WritebackHold};
use pando_format::usize_from;
pub(crate) use space::{Space, no_room};

use crate::ports::{LocalFile, LocalFs, Open};

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// How many names in a row [`create_tmp`] tries when each one exists already, before it fails.
const TMP_TRIES: u64 = 1 << 16;

/// Creates a new temporary file next to `p` and returns its path. The name takes the next value
/// of `seq`, and a name that exists already is skipped, never reused: a process killed before its
/// rename leaves its file behind, and the counter of a restarted process starts again at the
/// names it used. The counter only goes up, so a process skips each such name once.
async fn create_tmp(
    fs: &dyn LocalFs,
    p: &Path,
    seq: &AtomicU64,
) -> io::Result<(PathBuf, Arc<dyn LocalFile>)> {
    let mut taken = 0;
    loop {
        let mut s = p.as_os_str().to_owned();
        s.push(format!(".{}.tmp", seq.fetch_add(1, Ordering::Relaxed)));
        let tmp = PathBuf::from(s);
        match fs.open(&tmp, Open::CreateNew).await {
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists && taken + 1 < TMP_TRIES => {
                taken += 1;
            }
            opened => return opened.map(|f| (tmp, f)),
        }
    }
}

/// Replaces `p` with `b` so that a crash leaves either the old or the new content, and the new
/// content is durable when this returns.
pub(crate) async fn write_durable(fs: &dyn LocalFs, p: &Path, b: Bytes) -> io::Result<()> {
    let dir = p.parent().unwrap_or_else(|| Path::new(""));
    fs.create_dir_all(dir).await?;
    let (tmp, f) = create_tmp(fs, p, &TMP_SEQ).await?;
    f.write_at(vec![b], 0).await?;
    f.sync_data().await?;
    fs.rename(&tmp, p).await?;
    fs.sync_dir(dir).await
}

/// Replaces `p` with `b` atomically, without waiting for the disk.
pub(crate) async fn write_atomic(fs: &dyn LocalFs, p: &Path, b: Bytes) -> io::Result<()> {
    let dir = p.parent().unwrap_or_else(|| Path::new(""));
    fs.create_dir_all(dir).await?;
    let (tmp, f) = create_tmp(fs, p, &TMP_SEQ).await?;
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

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use super::*;
    use crate::sim::SimFs;

    async fn leave_stale(fs: &SimFs, seqs: std::ops::Range<u64>) {
        fs.create_dir_all(Path::new("/c")).await.unwrap();
        for seq in seqs {
            let stale = fs
                .open(
                    &PathBuf::from(format!("/c/objects.synced.{seq}.tmp")),
                    Open::CreateNew,
                )
                .await
                .unwrap();
            stale
                .write_at(vec![Bytes::from_static(b"stale")], 0)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn a_restarted_process_skips_the_temporary_files_a_killed_one_left() {
        let fs = SimFs::default();
        let p = Path::new("/c/objects.synced");
        leave_stale(&fs, 0..1000).await;
        let restarted = AtomicU64::new(0);
        let (tmp, _) = create_tmp(&fs, p, &restarted).await.unwrap();
        assert_eq!(tmp, Path::new("/c/objects.synced.1000.tmp"));
        assert_eq!(
            read_file(&fs, Path::new("/c/objects.synced.999.tmp"))
                .await
                .unwrap(),
            Some(Bytes::from_static(b"stale"))
        );
        let (tmp, _) = create_tmp(&fs, p, &restarted).await.unwrap();
        assert_eq!(tmp, Path::new("/c/objects.synced.1001.tmp"));
    }

    #[tokio::test]
    async fn a_temporary_name_is_not_searched_for_without_end() {
        let fs = SimFs::default();
        leave_stale(&fs, 0..TMP_TRIES).await;
        let err = create_tmp(&fs, Path::new("/c/objects.synced"), &AtomicU64::new(0))
            .await
            .err()
            .unwrap();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[tokio::test]
    async fn replaces_write_the_new_content() {
        let fs = SimFs::default();
        let p = Path::new("/c/objects.synced");
        write_atomic(&fs, p, Bytes::from_static(b"42")).await.unwrap();
        assert_eq!(read_file(&fs, p).await.unwrap(), Some(Bytes::from_static(b"42")));
        write_durable(&fs, p, Bytes::from_static(b"43")).await.unwrap();
        assert_eq!(read_file(&fs, p).await.unwrap(), Some(Bytes::from_static(b"43")));
    }
}
