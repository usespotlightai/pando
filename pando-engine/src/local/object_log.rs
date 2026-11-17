use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use bytes::Bytes;
use pando_format::{Hash, u64_from, usize_from};
use tokio::sync::Notify;

use super::{read_file, take, write_atomic};
use crate::ports::{Advice, FileLock, LocalFile, LocalFs, Open};
use crate::{Error, Result};

const HEADER: usize = 32 + 4;
const HEADER_LEN: u64 = u64_from(HEADER);
const REF_FLAG: u32 = 1 << 31;
const SHARDS: usize = 256;
/// After this many appended bytes the log starts its write-back, so a flush's sync does not
/// also write every object fetched since the last one.
const WRITEBACK_BYTES: u64 = 8 << 20;

/// The local tier: one append-only file of records `[hash][length][data]`, with an index in
/// memory. A record may instead name another file that holds the object (a clone of a
/// handed-off image): `[hash][REF_FLAG | length][object length u32][offset u64][path]`. Such a
/// file must not change while records name it; once it is dropped, its records read as misses.
///
/// `<log>.synced` says how much of the log is durable, so an open after a crash checks the
/// hashes of the records beyond it only. A record with the zero hash is padding: the space of
/// an append whose write failed. One process at a time opens the log: `<log>.lock`.
#[derive(Debug)]
pub(crate) struct ObjectLog {
    fs: Arc<dyn LocalFs>,
    file: Arc<dyn LocalFile>,
    path: PathBuf,
    end: AtomicU64,
    shards: Vec<RwLock<HashMap<Hash, Arc<Loc>>>>,
    /// Where appends are still writing. An append that could not write its record or its
    /// padding stays here, and the log is poisoned.
    in_flight: Arc<Mutex<BTreeSet<u64>>>,
    landed: Arc<Notify>,
    poisoned: Arc<AtomicBool>,
    _lock: Box<dyn FileLock>,
    files: Mutex<BTreeMap<PathBuf, Arc<ExtFile>>>,
    unwritten: AtomicU64,
    writing_back: Arc<AtomicBool>,
    holds: AtomicU32,
}

#[derive(Debug)]
struct Loc {
    off: u64,
    len: u32,
    touched: AtomicBool,
    ext: Option<(Arc<ExtFile>, u64)>,
}

impl Loc {
    fn dead(&self) -> bool {
        self.ext
            .as_ref()
            .is_some_and(|(e, _)| e.gone.load(Ordering::Acquire))
    }
}

#[derive(Debug)]
struct ExtFile {
    path: PathBuf,
    file: Mutex<Option<Arc<dyn LocalFile>>>,
    gone: AtomicBool,
    needs_sync: AtomicBool,
}

impl ExtFile {
    async fn open(&self, fs: &dyn LocalFs) -> Option<Arc<dyn LocalFile>> {
        if self.gone.load(Ordering::Acquire) {
            return None;
        }
        let opened = self.file.lock().clone();
        if let Some(f) = opened {
            return Some(f);
        }
        if let Ok(f) = fs.open(&self.path, Open::Read).await {
            let mut slot = self.file.lock();
            (!self.gone.load(Ordering::Acquire)).then(|| Arc::clone(slot.get_or_insert(f)))
        } else {
            self.gone.store(true, Ordering::Release);
            None
        }
    }

    fn opened(&self) -> Option<Arc<dyn LocalFile>> {
        if self.gone.load(Ordering::Acquire) {
            return None;
        }
        self.file.lock().clone()
    }
}

/// Holds the log's write-back until dropped; then it starts one.
#[derive(Debug)]
pub(crate) struct WritebackHold<'a>(&'a ObjectLog);

impl Drop for WritebackHold<'_> {
    fn drop(&mut self) {
        if self.0.holds.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.write_back();
        }
    }
}

/// How much of the log a scan reads at once.
const SCAN_WINDOW: usize = 4 << 20;

/// The part of the log a scan read last, so it reads the log in large blocks.
#[derive(Default)]
struct Window {
    start: u64,
    buf: Bytes,
}

impl Window {
    /// `len` bytes at `at`, fewer at the end of the file.
    async fn read(&mut self, f: &dyn LocalFile, at: u64, len: usize) -> io::Result<Bytes> {
        let end = self.start + u64_from(self.buf.len());
        if at < self.start || at + u64_from(len) > end {
            if len > SCAN_WINDOW {
                return f.read_at(len, at).await;
            }
            self.buf = f.read_at(SCAN_WINDOW, at).await?;
            self.start = at;
        }
        let from = usize_from(at - self.start);
        Ok(self.buf.slice(from..(from + len).min(self.buf.len())))
    }

    /// [`Self::read`] for a few bytes: from the page cache when it holds them, so a scan of
    /// headers does not read the objects between them.
    async fn read_small(&mut self, f: &dyn LocalFile, at: u64, len: usize) -> io::Result<Bytes> {
        let end = self.start + u64_from(self.buf.len());
        if at < self.start || at + u64_from(len) > end {
            let mut b = vec![0; len];
            if f.read_at_nowait(&mut b, at)? == Some(len) {
                return Ok(Bytes::from(b));
            }
        }
        self.read(f, at, len).await
    }
}

fn header(h: Hash, len: u32) -> Bytes {
    let mut b = Vec::with_capacity(HEADER);
    b.extend_from_slice(&h.0);
    b.extend_from_slice(&len.to_le_bytes());
    Bytes::from(b)
}

impl ObjectLog {
    pub(crate) async fn open(fs: Arc<dyn LocalFs>, path: &Path) -> Result<ObjectLog> {
        let dir = path.parent().unwrap_or_else(|| Path::new(""));
        fs.create_dir_all(dir).await?;
        let mut lock_path = path.as_os_str().to_owned();
        lock_path.push(".lock");
        let lock = fs.try_lock(Path::new(&lock_path)).await.map_err(|e| {
            Error::Io(io::Error::new(
                e.kind(),
                format!("the object log {} is in use: {e}", path.display()),
            ))
        })?;
        let file = fs.open(path, Open::Create).await?;
        fs.sync_dir(dir).await?;
        let log = ObjectLog {
            fs: Arc::clone(&fs),
            file,
            path: path.to_path_buf(),
            end: AtomicU64::new(0),
            shards: (0..SHARDS).map(|_| RwLock::default()).collect(),
            in_flight: Arc::default(),
            landed: Arc::default(),
            poisoned: Arc::default(),
            _lock: lock,
            files: Mutex::default(),
            unwritten: AtomicU64::new(0),
            writing_back: Arc::default(),
            holds: AtomicU32::new(0),
        };
        let synced = match read_file(fs.as_ref(), &log.synced_path()).await? {
            Some(b) => std::str::from_utf8(&b)
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0),
            None => 0,
        };
        let end = log.scan(synced).await?;
        if end < log.file.size().await? {
            log.file.set_len(end).await?;
        }
        log.end.store(end, Ordering::Release);
        Ok(log)
    }

    fn synced_path(&self) -> PathBuf {
        let mut s = self.path.as_os_str().to_owned();
        s.push(".synced");
        PathBuf::from(s)
    }

    /// Indexes the records, checking those at or past `synced`; returns where the valid log ends.
    async fn scan(&self, synced: u64) -> Result<u64> {
        let size = self.file.size().await?;
        let mut w = Window::default();
        let mut off = 0u64;
        while off + HEADER_LEN <= size {
            let hdr = w.read_small(self.file.as_ref(), off, HEADER).await?;
            let mut rest: &[u8] = &hdr;
            let (Some(h), Some(n)) = (take::<32>(&mut rest), take::<4>(&mut rest)) else {
                break;
            };
            let (h, n) = (Hash(h), u32::from_le_bytes(n));
            let plen = u64::from(n & !REF_FLAG);
            let body = off + HEADER_LEN;
            if body + plen > size {
                break;
            }
            if h == Hash::ZERO {
                if off >= synced {
                    break;
                }
                off = body + plen;
                continue;
            }
            if n & REF_FLAG != 0 {
                let payload = w.read_small(self.file.as_ref(), body, usize_from(plen)).await?;
                let Some((len, ext, ext_off)) = self.read_ref(&payload) else {
                    break;
                };
                if let Some(f) = ext.open(self.fs.as_ref()).await {
                    if off >= synced {
                        let data = f.read_at(len as usize, ext_off).await?;
                        if Hash::of(&data) != h {
                            break;
                        }
                    }
                    self.index(
                        h,
                        Arc::new(Loc {
                            off,
                            len,
                            touched: AtomicBool::new(false),
                            ext: Some((ext, ext_off)),
                        }),
                    );
                }
            } else {
                if off >= synced {
                    let data = w.read(self.file.as_ref(), body, n as usize).await?;
                    if data.len() != n as usize || Hash::of(&data) != h {
                        break;
                    }
                }
                self.index(
                    h,
                    Arc::new(Loc {
                        off,
                        len: n,
                        touched: AtomicBool::new(false),
                        ext: None,
                    }),
                );
            }
            off = body + plen;
        }
        Ok(off)
    }

    /// The object length, file and offset in that file that a reference record's payload names.
    fn read_ref(&self, mut b: &[u8]) -> Option<(u32, Arc<ExtFile>, u64)> {
        let len = u32::from_le_bytes(take(&mut b)?);
        let ext_off = u64::from_le_bytes(take(&mut b)?);
        let path = std::str::from_utf8(b).ok()?;
        Some((len, self.ext_file(Path::new(path)), ext_off))
    }

    fn ext_file(&self, path: &Path) -> Arc<ExtFile> {
        Arc::clone(self.files.lock().entry(path.to_path_buf()).or_insert_with(|| {
            Arc::new(ExtFile {
                path: path.to_path_buf(),
                file: Mutex::default(),
                gone: AtomicBool::new(false),
                needs_sync: AtomicBool::new(false),
            })
        }))
    }

    fn shard(&self, h: &Hash) -> &RwLock<HashMap<Hash, Arc<Loc>>> {
        &self.shards[usize::from(h.0[0])]
    }

    fn index(&self, h: Hash, loc: Arc<Loc>) {
        let mut m = self.shard(&h).write();
        if m.get(&h).is_none_or(|old| old.dead()) {
            m.insert(h, loc);
        }
    }

    fn lookup(&self, h: &Hash) -> Option<Arc<Loc>> {
        self.shard(h).read().get(h).filter(|l| !l.dead()).cloned()
    }

    pub(crate) fn has(&self, h: &Hash) -> bool {
        self.lookup(h).is_some()
    }

    /// Whether `h` is kept in the log itself, not as a reference to another file.
    pub(crate) fn has_own(&self, h: &Hash) -> bool {
        self.lookup(h).is_some_and(|l| l.ext.is_none())
    }

    /// Writes a record at the end of the log. The write runs on its own task, so a caller that
    /// stops waiting leaves no hole: a failed write leaves padding instead.
    async fn append(&self, h: Hash, hdr: Bytes, body: Bytes) -> Result<u64> {
        let pad_len = u32::try_from(body.len())
            .map_err(|_| Error::Corrupt(format!("object {h} has a record of {} bytes", body.len())))?;
        let n = u64_from(hdr.len() + body.len());
        let at = {
            let mut f = self.in_flight.lock();
            let at = self.end.fetch_add(n, Ordering::AcqRel);
            f.insert(at);
            at
        };
        let (file, in_flight, landed, poisoned) = (
            Arc::clone(&self.file),
            Arc::clone(&self.in_flight),
            Arc::clone(&self.landed),
            Arc::clone(&self.poisoned),
        );
        let task = tokio::spawn(async move {
            let r = file.write_at(vec![hdr, body], at).await;
            let settled = if let Ok(()) = r {
                true
            } else {
                let pad = header(Hash::ZERO, pad_len);
                file.write_at(vec![pad], at).await.is_ok()
            };
            if settled {
                in_flight.lock().remove(&at);
            } else {
                poisoned.store(true, Ordering::Release);
            }
            landed.notify_waiters();
            r
        });
        let r = task.await.map_err(io::Error::other)?;
        r.map_err(|e| Error::Io(io::Error::new(e.kind(), format!("write object {h}: {e}"))))?;
        Ok(at)
    }

    /// Appends object `h`. Readers see it only once it is written. Durable after [`Self::sync`].
    pub(crate) async fn put(&self, h: Hash, b: Bytes) -> Result<()> {
        if self.has(&h) {
            return Ok(());
        }
        let len =
            u32::try_from(b.len()).map_err(|_| Error::Corrupt(format!("object {h} of {} bytes", b.len())))?;
        let at = self.append(h, header(h, len), b).await?;
        self.index(
            h,
            Arc::new(Loc {
                off: at,
                len,
                touched: AtomicBool::new(false),
                ext: None,
            }),
        );
        let n = self
            .unwritten
            .fetch_add(u64::from(len) + HEADER_LEN, Ordering::AcqRel);
        if n + u64::from(len) >= WRITEBACK_BYTES && self.holds.load(Ordering::Acquire) == 0 {
            self.write_back();
        }
        Ok(())
    }

    /// Records that object `h` is the `len` bytes at `offset` of `path`, which must not change
    /// until [`Self::drop_file`].
    pub(crate) async fn put_ref(&self, h: Hash, path: &Path, offset: u64, len: u32) -> Result<()> {
        if self.has(&h) {
            return Ok(());
        }
        let ext = self.ext_file(path);
        if ext.open(self.fs.as_ref()).await.is_none() {
            return Err(Error::Corrupt(format!(
                "reference object {h}: {} is gone",
                path.display()
            )));
        }
        ext.needs_sync.store(true, Ordering::Release);
        let p = path.as_os_str().as_encoded_bytes();
        let mut payload = Vec::with_capacity(12 + p.len());
        payload.extend_from_slice(&len.to_le_bytes());
        payload.extend_from_slice(&offset.to_le_bytes());
        payload.extend_from_slice(p);
        let plen = u32::try_from(payload.len())
            .ok()
            .filter(|n| n & REF_FLAG == 0)
            .ok_or_else(|| {
                Error::Corrupt(format!(
                    "reference object {h}: the path {} is too long",
                    path.display()
                ))
            })?;
        let at = self
            .append(h, header(h, REF_FLAG | plen), Bytes::from(payload))
            .await?;
        self.index(
            h,
            Arc::new(Loc {
                off: at,
                len,
                touched: AtomicBool::new(false),
                ext: Some((ext, offset)),
            }),
        );
        Ok(())
    }

    /// Turns every record that names `path` into a miss; call it before removing the file.
    pub(crate) fn drop_file(&self, path: &Path) {
        let e = self.ext_file(path);
        e.gone.store(true, Ordering::Release);
        e.file.lock().take();
    }

    async fn source(&self, loc: &Loc) -> Option<(Arc<dyn LocalFile>, u64)> {
        match &loc.ext {
            None => Some((Arc::clone(&self.file), loc.off + HEADER_LEN)),
            Some((e, off)) => e.open(self.fs.as_ref()).await.map(|f| (f, *off)),
        }
    }

    /// Bytes `[inner, inner + len)` of object `h`, or `None` when the log does not have it.
    pub(crate) async fn read_at(&self, h: &Hash, inner: usize, len: usize) -> Result<Option<Bytes>> {
        let Some(loc) = self.lookup(h) else {
            return Ok(None);
        };
        if inner + len > loc.len as usize {
            return Err(Error::Corrupt(format!(
                "read {inner}+{len} beyond object {h} of {} bytes",
                loc.len
            )));
        }
        let Some((f, at)) = self.source(&loc).await else {
            return Ok(None);
        };
        if len < loc.len as usize && !loc.touched.swap(true, Ordering::AcqRel) {
            f.advise(at, u64::from(loc.len), Advice::WillNeed);
        }
        Ok(Some(f.read_at(len, at + u64_from(inner)).await?))
    }

    /// Reads bytes of `h` into `dst` only from the page cache; `false` when that is not enough.
    pub(crate) fn read_nowait(&self, h: &Hash, inner: usize, dst: &mut [u8]) -> Result<bool> {
        let Some(loc) = self.lookup(h) else {
            return Ok(false);
        };
        if inner + dst.len() > loc.len as usize {
            return Err(Error::Corrupt(format!(
                "read {inner}+{} beyond object {h} of {} bytes",
                dst.len(),
                loc.len
            )));
        }
        let (f, at) = match &loc.ext {
            None => (Arc::clone(&self.file), loc.off + HEADER_LEN),
            Some((e, off)) => match e.opened() {
                Some(f) => (f, *off),
                None => return Ok(false),
            },
        };
        Ok(f.read_at_nowait(dst, at + u64_from(inner))? == Some(dst.len()))
    }

    /// The whole object `h`. An object kept in another file is checked against its hash, since
    /// that file may have changed.
    pub(crate) async fn get(&self, h: &Hash) -> Result<Option<Bytes>> {
        let Some(loc) = self.lookup(h) else {
            return Ok(None);
        };
        let Some(b) = self.read_at(h, 0, loc.len as usize).await? else {
            return Ok(None);
        };
        if loc.ext.is_some() && Hash::of(&b) != *h {
            return Err(Error::ChunkLost { hash: *h });
        }
        Ok(Some(b))
    }

    pub(crate) fn hold_writeback(&self) -> WritebackHold<'_> {
        self.holds.fetch_add(1, Ordering::AcqRel);
        WritebackHold(self)
    }

    fn write_back(&self) {
        if self.writing_back.swap(true, Ordering::AcqRel) {
            return;
        }
        self.unwritten.store(0, Ordering::Release);
        let (file, flag) = (Arc::clone(&self.file), Arc::clone(&self.writing_back));
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                rt.spawn(async move {
                    file.start_writeback().await;
                    flag.store(false, Ordering::Release);
                });
            }
            Err(_) => flag.store(false, Ordering::Release),
        }
    }

    /// Waits for every put that started before it, makes them durable, then records that the
    /// log is durable up to them.
    pub(crate) async fn sync(&self) -> Result<()> {
        let end = {
            let _f = self.in_flight.lock();
            self.end.load(Ordering::Acquire)
        };
        loop {
            let landed = self.landed.notified();
            tokio::pin!(landed);
            landed.as_mut().enable();
            if self.poisoned.load(Ordering::Acquire) {
                return Err(Error::Io(io::Error::other(format!(
                    "the object log {} lost a write",
                    self.path.display()
                ))));
            }
            let first = self.in_flight.lock().first().copied();
            if first.is_none_or(|f| f >= end) {
                break;
            }
            landed.await;
        }
        let ext: Vec<Arc<ExtFile>> = self
            .files
            .lock()
            .values()
            .filter(|e| e.needs_sync.swap(false, Ordering::AcqRel))
            .cloned()
            .collect();
        for e in ext {
            if let Some(f) = e.open(self.fs.as_ref()).await
                && let Err(err) = f.sync_data().await
            {
                e.needs_sync.store(true, Ordering::Release);
                return Err(err.into());
            }
        }
        self.file.sync_data().await?;
        write_atomic(
            self.fs.as_ref(),
            &self.synced_path(),
            Bytes::from(end.to_string()),
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use super::*;
    use crate::sim::SimFs;

    fn obj(i: u8) -> (Hash, Bytes) {
        let b = Bytes::from(vec![i; 1000 + usize::from(i)]);
        (Hash::of(&b), b)
    }

    #[tokio::test]
    async fn objects_survive_reopen_and_a_crash_keeps_only_synced_ones() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let (h1, b1) = obj(1);
        let (h2, b2) = obj(2);
        log.put(h1, b1.clone()).await.unwrap();
        log.sync().await.unwrap();
        log.put(h2, b2.clone()).await.unwrap();
        assert_eq!(log.read_at(&h2, 10, 5).await.unwrap().unwrap(), b2.slice(10..15));
        drop(log);

        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert_eq!(
            again.get(&h2).await.unwrap(),
            Some(b2),
            "a daemon restart keeps unsynced records"
        );
        drop(again);

        fs.crash();
        let after = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert_eq!(after.get(&h1).await.unwrap(), Some(b1));
        assert!(!after.has(&h2));
    }

    #[tokio::test]
    async fn one_process_at_a_time_opens_the_log() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let err = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap_err();
        assert!(err.to_string().contains("in use"), "{err}");
        drop(log);
        ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
    }

    #[tokio::test]
    async fn padding_is_skipped_below_the_synced_mark_and_ends_the_log_above_it() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let ((h1, b1), (h2, b2)) = (obj(1), obj(2));
        log.put(h1, b1).await.unwrap();
        drop(log);
        let f = fs.open(path, Open::ReadWrite).await.unwrap();
        let at = f.size().await.unwrap();
        let tail = vec![
            header(Hash::ZERO, 100),
            Bytes::from(vec![0; 100]),
            header(h2, u32::try_from(b2.len()).unwrap()),
            b2.clone(),
        ];
        f.write_at(tail.clone(), at).await.unwrap();
        let end = f.size().await.unwrap();

        let unsynced = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert!(unsynced.has(&h1) && !unsynced.has(&h2));
        drop(unsynced);
        assert_eq!(f.size().await.unwrap(), at, "the open cut the log at the padding");
        f.write_at(tail, at).await.unwrap();
        write_atomic(&fs, Path::new("/c/objects.synced"), Bytes::from(end.to_string()))
            .await
            .unwrap();
        let synced = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert!(synced.has(&h1) && !synced.has(&Hash::ZERO));
        assert_eq!(synced.get(&h2).await.unwrap(), Some(b2));
    }

    #[tokio::test]
    async fn a_torn_record_ends_the_log() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let (h1, b1) = obj(1);
        log.put(h1, b1).await.unwrap();
        let f = fs.open(path, Open::ReadWrite).await.unwrap();
        let size = f.size().await.unwrap();
        f.write_at(vec![Bytes::from(vec![9; 50])], size).await.unwrap();
        drop(log);
        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert!(again.has(&h1));
        assert_eq!(f.size().await.unwrap(), size);
    }

    #[tokio::test]
    async fn references_read_through_and_drop() {
        let fs = SimFs::default();
        let log = ObjectLog::open(Arc::new(fs.clone()), Path::new("/c/objects"))
            .await
            .unwrap();
        let img = fs.open(Path::new("/c/img"), Open::CreateNew).await.unwrap();
        let (h, b) = obj(3);
        img.write_at(vec![Bytes::from(vec![0; 100]), b.clone()], 0)
            .await
            .unwrap();
        log.put_ref(h, Path::new("/c/img"), 100, u32::try_from(b.len()).unwrap())
            .await
            .unwrap();
        assert_eq!(log.get(&h).await.unwrap(), Some(b));
        log.drop_file(Path::new("/c/img"));
        assert!(!log.has(&h));
    }
}
