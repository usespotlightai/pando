use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use bytes::Bytes;
use pando_format::{Hash, Loc as StoreLoc, u64_from, usize_from};
use tokio::sync::Notify;

use super::{read_file, take, write_atomic, write_durable};
use crate::ports::{Advice, FileLock, LocalFile, LocalFs, Open};
use crate::{Error, Result};

const HEADER: usize = 32 + 4;
const HEADER_LEN: u64 = u64_from(HEADER);
const REF_FLAG: u32 = 1 << 31;
const SHARDS: usize = 256;
/// After this many appended bytes the log starts its write-back, so a flush's sync does not
/// also write every object fetched since the last one.
const WRITEBACK_BYTES: u64 = 8 << 20;
/// The most bytes at the log's end that may wait for the disk. Past it a put waits for the
/// disk: the daemon serves the device, so the kernel does not slow its writes when the dirty
/// pages fill its memory cgroup.
const BEHIND_BYTES: u64 = 16 << 20;
/// The fewest appended bytes between two sweeps.
const SWEEP_MIN: u64 = if cfg!(test) { 1 << 20 } else { 1 << 30 };
/// The most bytes `<log>.pads` lists before a sweep syncs the log itself to empty it.
const PADS_MAX: usize = 1 << 20;
/// The longest padding a sweep writes over the records it drops.
const PAD_MAX: u64 = 1 << 30;
const PAGE: u64 = 4096;
/// An offset in the log is `segment << SEG_SHIFT | offset in the segment's file`.
const SEG_SHIFT: u32 = 40;
/// A segment takes no record that would end past this, unless it is empty.
const SEG_ROLL: u64 = if cfg!(test) { 1 << 20 } else { 16 << 30 };

fn seg_of(off: u64) -> u64 {
    off >> SEG_SHIFT
}

fn seg_pos(off: u64) -> u64 {
    off & ((1 << SEG_SHIFT) - 1)
}

fn seg_base(id: u64) -> u64 {
    id << SEG_SHIFT
}

/// The local tier: an append-only log of records `[hash][length][data]`, with an index in
/// memory. The log is a directory of segment files, each named by its number; appends go to
/// the last, and [`ObjectLog::compact`] moves the few records left in an earlier one to the
/// end and removes its file, so the log's files stay near the size of what it keeps. A record may instead name another file that holds the object (a clone of a
/// handed-off image): `[hash][REF_FLAG | length][object length u32][offset u64][path]`. Such a
/// file must not change while records name it; once it is dropped, its records read as misses.
///
/// `<log>.synced` says how much of the log is durable, so an open after a crash checks the
/// hashes of the records beyond it only. Padding fills the space of an append whose write
/// failed: a header whose hash is [`pad_hash`] of its offset and length, which a scan checks
/// and skips wherever it is. A record with the zero hash is padding of earlier releases, which
/// a scan trusts only below `<log>.synced`. When the padding cannot be written either, as on
/// a full disk, [`ObjectLog::sync`] writes it before it reports the log durable, and fails
/// until it can. One process at a time opens the log: `<log>.lock`.
///
/// A sweep drops the records nothing marked since an epoch: it writes padding over them and
/// frees their pages, so the offsets of the others hold and the log takes no more disk than
/// they do. With a budget, a sweep also drops records it was asked to keep only while they
/// fit: those read longest ago go first.
#[derive(Debug)]
pub(crate) struct ObjectLog {
    fs: Arc<dyn LocalFs>,
    segs: Arc<Segments>,
    path: PathBuf,
    end: AtomicU64,
    shards: Vec<RwLock<HashMap<Hash, Arc<Loc>>>>,
    /// Where appends are still writing. An append that could not write its record or its
    /// padding stays here until its padding is written.
    in_flight: Arc<Mutex<BTreeSet<u64>>>,
    landed: Arc<Notify>,
    /// Where appends that could write neither their record nor their padding start, with
    /// the length of the padding they need.
    unpadded: Arc<Mutex<BTreeMap<u64, u32>>>,
    /// Where appends started whose records are not indexed yet. [`ObjectLog::compact`] keeps
    /// their segments.
    unindexed: Mutex<BTreeSet<u64>>,
    _lock: Box<dyn FileLock>,
    files: Mutex<BTreeMap<PathBuf, Arc<ExtFile>>>,
    unwritten: AtomicU64,
    writing_back: Arc<AtomicBool>,
    behind: tokio::sync::Mutex<()>,
    holds: AtomicU32,
    epoch: AtomicU64,
    /// Counts the objects appended. A read stamps its object with it, so the order of reads
    /// between two sweeps shows apart from the reads between two appends.
    clock: AtomicU64,
    /// Bytes appended since the last sweep, and how many make the next one due.
    appended: AtomicU64,
    sweep_after: AtomicU64,
    due: Arc<Notify>,
    /// The bytes of the records in the log itself, as of the last sweep or open, with the
    /// appends since.
    live: AtomicU64,
    /// The most bytes the log keeps of objects a sweep may drop; zero for no limit.
    budget: AtomicU64,
    /// Where the log ended when the last sweep began, or when it opened: a sweep with a budget
    /// keeps the wanted records from there on, so a read that just fetched its object finds it.
    fresh_from: AtomicU64,
    /// Records of objects the log had already when they landed, which the next sweep drops.
    orphans: Mutex<Vec<(u64, u64)>>,
    /// The runs of padding a sweep wrote or a scan found, by start, with their ends. A sweep
    /// pads the records it drops together with the padding next to them, so the page of each
    /// earlier run's header is freed with them.
    padded: Mutex<BTreeMap<u64, u64>>,
    /// The runs of `<log>.pads` whose padding may not be durable yet.
    pads: tokio::sync::Mutex<Pads>,
}

/// The lines of `<log>.pads`, one run of padding each. A sweep appends its runs and makes
/// them durable before it writes their headers, then frees their pages without a sync of the
/// log: an open writes the padding of every listed run again. [`ObjectLog::sync`] drops the
/// runs whose headers its sync covered.
#[derive(Debug, Default)]
struct Pads {
    file: Option<Arc<dyn LocalFile>>,
    text: String,
    /// How many leading bytes of `text` list runs whose headers are written.
    headed: usize,
    /// How many bytes of runs were dropped from the start of the file so far.
    dropped: usize,
}

/// The files of the log's segments, by number.
#[derive(Debug)]
struct Segments {
    fs: Arc<dyn LocalFs>,
    dir: PathBuf,
    files: RwLock<BTreeMap<u64, Arc<dyn LocalFile>>>,
    creating: tokio::sync::Mutex<()>,
    /// The segments written since a sync, each with the count of writes at its last write. An
    /// open lists the segments that reach past the synced marker, since the process before it
    /// may not have synced them.
    dirty: Mutex<BTreeMap<u64, u64>>,
    writes: AtomicU64,
}

impl Segments {
    fn get(&self, id: u64) -> Option<Arc<dyn LocalFile>> {
        self.files.read().get(&id).cloned()
    }

    fn path(&self, id: u64) -> PathBuf {
        self.dir.join(id.to_string())
    }

    /// The file of segment `id`, created when there is none yet.
    async fn open(&self, id: u64) -> io::Result<Arc<dyn LocalFile>> {
        if let Some(f) = self.get(id) {
            return Ok(f);
        }
        let _c = self.creating.lock().await;
        if let Some(f) = self.get(id) {
            return Ok(f);
        }
        let f = self.fs.open(&self.path(id), Open::Create).await?;
        self.fs.sync_dir(&self.dir).await?;
        self.files.write().insert(id, Arc::clone(&f));
        Ok(f)
    }

    /// Records that a write to segment `id` landed, so the next [`Self::sync`] covers it.
    fn written(&self, id: u64) {
        let n = self.writes.fetch_add(1, Ordering::AcqRel);
        self.dirty.lock().insert(id, n);
    }

    /// Makes every write that landed before it durable.
    async fn sync(&self) -> io::Result<()> {
        let dirty: Vec<(u64, u64)> = self.dirty.lock().iter().map(|(a, b)| (*a, *b)).collect();
        for (id, n) in dirty {
            if let Some(f) = self.get(id) {
                f.sync_data().await?;
            }
            let mut d = self.dirty.lock();
            if d.get(&id) == Some(&n) {
                d.remove(&id);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Loc {
    off: u64,
    /// Where the object was first appended since the log opened; a move by
    /// [`ObjectLog::compact`] keeps it, so a sweep tells old records from new ones by it.
    born: u64,
    len: u32,
    touched: AtomicBool,
    ext: Option<(Arc<ExtFile>, u64)>,
    /// The last epoch that marked the record or put its object again.
    mark: AtomicU64,
    /// The log's clock at the last read of the object or put of it.
    used: AtomicU64,
    /// Where the store has the object, once an upload put it there, and whose upload.
    stored: Mutex<Option<(u64, StoreLoc)>>,
}

impl Loc {
    fn new(off: u64, len: u32, ext: Option<(Arc<ExtFile>, u64)>, used: u64) -> Arc<Loc> {
        Arc::new(Loc {
            off,
            born: off,
            len,
            touched: AtomicBool::new(false),
            ext,
            mark: AtomicU64::new(0),
            used: AtomicU64::new(used),
            stored: Mutex::new(None),
        })
    }

    /// The end of the record in the log.
    fn end(&self) -> u64 {
        let body = match &self.ext {
            None => u64::from(self.len),
            Some((e, _)) => 12 + u64_from(e.path.as_os_str().as_encoded_bytes().len()),
        };
        self.off + HEADER_LEN + body
    }

    /// Records a read or put of the object at `tick` of the log's clock. Most reads find it
    /// there already, so they only load it, and a chunk many readers share is not written by
    /// each of them.
    fn used_at(&self, tick: u64) {
        if self.used.load(Ordering::Relaxed) < tick {
            self.used.fetch_max(tick, Ordering::AcqRel);
        }
    }

    /// The bytes of the record in the log itself; a reference record counts none.
    fn own_bytes(&self) -> u64 {
        if self.ext.is_some() {
            0
        } else {
            u64::from(self.len) + HEADER_LEN
        }
    }

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

/// What [`ObjectLog::sweep`] dropped: the objects, and the bytes of disk it freed. `kept` is
/// the bytes of the records left in the log itself.
#[derive(Debug, Default)]
pub(crate) struct Swept {
    pub(crate) dropped: Vec<Hash>,
    pub(crate) freed: u64,
    pub(crate) kept: u64,
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

/// An append whose record is not indexed yet; drop it once the record is indexed or given up.
#[derive(Debug)]
struct Unindexed<'a> {
    log: &'a ObjectLog,
    at: u64,
}

impl Drop for Unindexed<'_> {
    fn drop(&mut self) {
        self.log.unindexed.lock().remove(&self.at);
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

/// The hash in the header of padding of `len` bytes at `off`: bound to both, so a stale or
/// torn header does not pass for padding.
fn pad_hash(off: u64, len: u32) -> Hash {
    let mut b = Vec::with_capacity(36);
    b.extend_from_slice(b"pando object log pad");
    b.extend_from_slice(&off.to_le_bytes());
    b.extend_from_slice(&len.to_le_bytes());
    Hash::of(&b)
}

/// How many bytes to append before the next sweep, after one that kept `kept` bytes: as many
/// as it kept, at least [`SWEEP_MIN`], and with a budget no more than what is left of it, or
/// an eighth of it when nothing is.
fn next_sweep(kept: u64, budget: u64) -> u64 {
    let n = kept.max(SWEEP_MIN);
    if budget == 0 {
        return n;
    }
    n.min(budget.saturating_sub(kept).max(budget / 8).max(1))
}

/// The records of a segment [`ObjectLog::compact`] may move: their bytes, whether one is from
/// below the floor, and each with its object.
#[derive(Default)]
struct Held {
    bytes: u64,
    old: bool,
    locs: Vec<(Hash, Arc<Loc>)>,
}

/// Gives `new`, the copy of the record at `old`, the marks, reads and store location of `old`.
fn take_uses(old: &Loc, new: &Loc) {
    new.mark
        .fetch_max(old.mark.load(Ordering::Acquire), Ordering::AcqRel);
    new.used
        .fetch_max(old.used.load(Ordering::Acquire), Ordering::AcqRel);
    if old.touched.load(Ordering::Acquire) {
        new.touched.store(true, Ordering::Release);
    }
    let stored = *old.stored.lock();
    let mut s = new.stored.lock();
    if s.is_none() {
        *s = stored;
    }
}

/// The payload of a reference record of object `h`, the `len` bytes at `offset` of `path`,
/// with its length.
fn ref_payload(h: Hash, path: &Path, offset: u64, len: u32) -> Result<(u32, Bytes)> {
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
    Ok((plen, Bytes::from(payload)))
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
        fs.create_dir_all(path).await?;
        fs.sync_dir(dir).await?;
        let segs = Segments {
            fs: Arc::clone(&fs),
            dir: path.to_path_buf(),
            files: RwLock::default(),
            creating: tokio::sync::Mutex::default(),
            dirty: Mutex::default(),
            writes: AtomicU64::new(0),
        };
        for e in fs.list(path).await? {
            let id = e
                .name
                .parse::<u64>()
                .ok()
                .filter(|&id| !e.is_dir && id < 1 << (64 - SEG_SHIFT))
                .ok_or_else(|| {
                    Error::Corrupt(format!(
                        "{} is not a segment of the object log",
                        path.join(&e.name).display()
                    ))
                })?;
            let f = fs.open(&segs.path(id), Open::ReadWrite).await?;
            segs.files.write().insert(id, f);
        }
        let log = ObjectLog {
            fs: Arc::clone(&fs),
            segs: Arc::new(segs),
            path: path.to_path_buf(),
            end: AtomicU64::new(0),
            shards: (0..SHARDS).map(|_| RwLock::default()).collect(),
            in_flight: Arc::default(),
            landed: Arc::default(),
            unpadded: Arc::default(),
            unindexed: Mutex::default(),
            _lock: lock,
            files: Mutex::default(),
            unwritten: AtomicU64::new(0),
            writing_back: Arc::default(),
            behind: tokio::sync::Mutex::new(()),
            holds: AtomicU32::new(0),
            epoch: AtomicU64::new(1),
            clock: AtomicU64::new(0),
            appended: AtomicU64::new(0),
            sweep_after: AtomicU64::new(SWEEP_MIN),
            due: Arc::default(),
            live: AtomicU64::new(0),
            budget: AtomicU64::new(0),
            fresh_from: AtomicU64::new(0),
            orphans: Mutex::default(),
            padded: Mutex::default(),
            pads: tokio::sync::Mutex::default(),
        };
        let synced = match read_file(fs.as_ref(), &log.synced_path()).await? {
            Some(b) => std::str::from_utf8(&b)
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0),
            None => 0,
        };
        log.redo_pads().await?;
        let end = log.scan(synced).await?;
        let unsynced: Vec<u64> = log
            .segs
            .files
            .read()
            .range(seg_of(synced)..)
            .map(|(&id, _)| id)
            .collect();
        for id in unsynced {
            log.segs.written(id);
        }
        log.end.store(end, Ordering::Release);
        log.fresh_from.store(end, Ordering::Release);
        Ok(log)
    }

    /// `<log>.pads`: the padding a sweep writes over dropped records, recorded before it does,
    /// so an open writes it again in full when a crash tore a header.
    fn pads_path(&self) -> PathBuf {
        let mut s = self.path.as_os_str().to_owned();
        s.push(".pads");
        PathBuf::from(s)
    }

    /// Writes padding over each run `[a, b)` of records and makes it durable.
    async fn pad_runs(&self, runs: &[(u64, u64)]) -> Result<()> {
        self.write_pads(runs).await?;
        self.segs.sync().await?;
        Ok(())
    }

    /// Writes padding over each run `[a, b)` of records, without waiting for the disk. A run in
    /// a segment that is gone needs none.
    async fn write_pads(&self, runs: &[(u64, u64)]) -> Result<()> {
        for &(a, b) in runs {
            let len = u32::try_from(b - a - HEADER_LEN)
                .ok()
                .filter(|_| seg_of(a) == seg_of(b))
                .ok_or_else(|| Error::Corrupt(format!("padding of {} bytes at {a}", b - a)))?;
            let Some(f) = self.segs.get(seg_of(a)) else {
                continue;
            };
            f.write_at(vec![header(pad_hash(a, len), len)], seg_pos(a))
                .await?;
            self.segs.written(seg_of(a));
        }
        Ok(())
    }

    /// Writes again the padding a sweep recorded and may not have finished.
    async fn redo_pads(&self) -> Result<()> {
        let Some(b) = read_file(self.fs.as_ref(), &self.pads_path()).await? else {
            return Ok(());
        };
        let corrupt = || Error::Corrupt(format!("{} does not parse", self.pads_path().display()));
        let whole = b.iter().rposition(|&c| c == b'\n').map_or(0, |i| i + 1);
        let text = std::str::from_utf8(&b[..whole]).map_err(|_| corrupt())?;
        let mut runs = Vec::new();
        for l in text.lines() {
            let (a, b) = l.split_once(' ').ok_or_else(corrupt)?;
            let (a, b): (u64, u64) = (
                a.parse().map_err(|_| corrupt())?,
                b.parse().map_err(|_| corrupt())?,
            );
            if b < a + HEADER_LEN {
                return Err(corrupt());
            }
            runs.push((a, b));
        }
        if !runs.is_empty() {
            self.pad_runs(&runs).await?;
        }
        if !b.is_empty() {
            write_durable(self.fs.as_ref(), &self.pads_path(), Bytes::new()).await?;
        }
        Ok(())
    }

    /// Appends `lines` to `<log>.pads` and makes them durable, cutting off what a failed append
    /// left past them.
    async fn list_pads(&self, p: &mut Pads, lines: &str) -> Result<()> {
        let f = if let Some(f) = &p.file {
            Arc::clone(f)
        } else {
            let f = self.fs.open(&self.pads_path(), Open::Create).await?;
            if f.size().await? != u64_from(p.text.len()) {
                f.set_len(u64_from(p.text.len())).await?;
            }
            self.fs
                .sync_dir(self.path.parent().unwrap_or_else(|| Path::new("")))
                .await?;
            p.file = Some(Arc::clone(&f));
            f
        };
        f.write_at(
            vec![Bytes::copy_from_slice(lines.as_bytes())],
            u64_from(p.text.len()),
        )
        .await?;
        let end = u64_from(p.text.len() + lines.len());
        if f.size().await? > end {
            f.set_len(end).await?;
        }
        f.sync_data().await?;
        p.text.push_str(lines);
        Ok(())
    }

    /// Drops from `<log>.pads` the first `n` bytes of runs, whose headers a sync made durable.
    async fn unlist_pads(&self, p: &mut Pads, n: usize) -> Result<()> {
        if n == 0 {
            return Ok(());
        }
        let rest = p.text.split_off(n);
        write_durable(
            self.fs.as_ref(),
            &self.pads_path(),
            Bytes::from(rest.clone().into_bytes()),
        )
        .await?;
        p.file = None;
        p.text = rest;
        p.headed -= n;
        p.dropped += n;
        Ok(())
    }

    fn synced_path(&self) -> PathBuf {
        let mut s = self.path.as_os_str().to_owned();
        s.push(".synced");
        PathBuf::from(s)
    }

    /// Indexes the records, checking those at or past `synced`; returns where the valid log
    /// ends. The segment with the first record that is not valid is cut there, and the
    /// segments after it are removed: they hold only appends a sync did not cover.
    async fn scan(&self, synced: u64) -> Result<u64> {
        let ids: Vec<u64> = self.segs.files.read().keys().copied().collect();
        let mut end = 0;
        let mut cut = false;
        for id in ids {
            if cut {
                self.segs.files.write().remove(&id);
                self.fs.remove(&self.segs.path(id)).await?;
                continue;
            }
            let Some(f) = self.segs.get(id) else {
                continue;
            };
            let size = f.size().await?;
            let valid = self.scan_segment(f.as_ref(), seg_base(id), size, synced).await?;
            if valid < size {
                f.set_len(valid).await?;
                cut = true;
            }
            end = seg_base(id) + valid;
        }
        if cut {
            self.fs.sync_dir(&self.path).await?;
        }
        Ok(end)
    }

    /// [`Self::scan`] of the `size` bytes of the segment that starts at `base` of the log;
    /// returns where its valid records end in its file.
    async fn scan_segment(&self, f: &dyn LocalFile, base: u64, size: u64, synced: u64) -> Result<u64> {
        let mut w = Window::default();
        let mut pos = 0u64;
        while pos + HEADER_LEN <= size {
            let off = base + pos;
            let hdr = w.read_small(f, pos, HEADER).await?;
            let mut rest: &[u8] = &hdr;
            let (Some(h), Some(n)) = (take::<32>(&mut rest), take::<4>(&mut rest)) else {
                break;
            };
            let (h, n) = (Hash(h), u32::from_le_bytes(n));
            let plen = u64::from(n & !REF_FLAG);
            let body = pos + HEADER_LEN;
            if body + plen > size {
                break;
            }
            let next = body + plen;
            if n & REF_FLAG == 0 && h == pad_hash(off, n) {
                self.padded.lock().insert(off, base + next);
                pos = next;
                continue;
            }
            if h == Hash::ZERO {
                if off >= synced {
                    break;
                }
                pos = next;
                continue;
            }
            if n & REF_FLAG != 0 {
                let payload = w.read_small(f, body, usize_from(plen)).await?;
                let Some((len, ext, ext_off)) = self.read_ref(&payload) else {
                    break;
                };
                if let Some(x) = ext.open(self.fs.as_ref()).await {
                    if off >= synced {
                        let data = x.read_at(len as usize, ext_off).await?;
                        if Hash::of(&data) != h {
                            break;
                        }
                        ext.needs_sync.store(true, Ordering::Release);
                    }
                    self.index(h, Loc::new(off, len, Some((ext, ext_off)), 0));
                }
            } else {
                if off >= synced {
                    let data = w.read(f, body, n as usize).await?;
                    if data.len() != n as usize || Hash::of(&data) != h {
                        break;
                    }
                }
                self.index(h, Loc::new(off, n, None, 0));
                self.live.fetch_add(HEADER_LEN + u64::from(n), Ordering::AcqRel);
            }
            pos = next;
        }
        Ok(pos)
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
        match m.get(&h).filter(|old| !old.dead()) {
            None => {
                m.insert(h, loc);
            }
            Some(old) => {
                old.mark.fetch_max(self.epoch(), Ordering::AcqRel);
                self.orphans.lock().push((loc.off, loc.end()));
            }
        }
    }

    fn lookup(&self, h: &Hash) -> Option<Arc<Loc>> {
        self.shard(h).read().get(h).filter(|l| !l.dead()).cloned()
    }

    /// What the log knows of object `h`, for an error report.
    pub(crate) fn describe(&self, h: &Hash) -> String {
        let epoch = self.epoch();
        let Some(l) = self.shard(h).read().get(h).cloned() else {
            return format!("the log has no record of it (epoch {epoch})");
        };
        let file = match &l.ext {
            None if self.segs.get(seg_of(l.off)).is_some() => "present",
            None => "absent",
            Some((e, _)) if e.gone.load(Ordering::Acquire) => "gone",
            Some(_) => "a reference",
        };
        format!(
            "the log has it at {} (segment {}, file {file}), {} bytes, born {}, marked {}, epoch {epoch}",
            l.off,
            seg_of(l.off),
            l.len,
            l.born,
            l.mark.load(Ordering::Acquire),
        )
    }

    pub(crate) fn has(&self, h: &Hash) -> bool {
        self.lookup(h).is_some()
    }

    /// Whether `h` is kept in the log itself, not as a reference to another file.
    pub(crate) fn has_own(&self, h: &Hash) -> bool {
        self.lookup(h).is_some_and(|l| l.ext.is_none())
    }

    /// Writes a record at the end of the log, in a new segment when it would end past
    /// [`SEG_ROLL`] of the last. The write runs on its own task, so a caller that stops waiting
    /// leaves no hole: a failed write leaves padding instead. Returns the record's offset and
    /// an [`Unindexed`] that keeps its segment from [`Self::compact`]; drop it after indexing.
    async fn append(&self, h: Hash, hdr: Bytes, body: Bytes) -> Result<(u64, Unindexed<'_>)> {
        let pad_len = u32::try_from(body.len())
            .map_err(|_| Error::Corrupt(format!("object {h} has a record of {} bytes", body.len())))?;
        let n = u64_from(hdr.len() + body.len());
        let at = {
            let mut f = self.in_flight.lock();
            let mut at = self.end.load(Ordering::Acquire);
            if seg_pos(at) > 0 && seg_pos(at) + n > SEG_ROLL {
                at = seg_base(seg_of(at) + 1);
            }
            self.end.store(at + n, Ordering::Release);
            f.insert(at);
            self.unindexed.lock().insert(at);
            at
        };
        let pending = Unindexed { log: self, at };
        let (segs, in_flight, landed, unpadded) = (
            Arc::clone(&self.segs),
            Arc::clone(&self.in_flight),
            Arc::clone(&self.landed),
            Arc::clone(&self.unpadded),
        );
        let task = tokio::spawn(async move {
            let (id, pos) = (seg_of(at), seg_pos(at));
            let r = match segs.open(id).await {
                Ok(file) => file.write_at(vec![hdr, body], pos).await,
                Err(e) => Err(e),
            };
            let settled = if let Ok(()) = r {
                true
            } else {
                let pad = header(pad_hash(at, pad_len), pad_len);
                match segs.open(id).await {
                    Ok(file) => file.write_at(vec![pad], pos).await.is_ok(),
                    Err(_) => false,
                }
            };
            if settled {
                segs.written(id);
                in_flight.lock().remove(&at);
            } else {
                unpadded.lock().insert(at, pad_len);
            }
            landed.notify_waiters();
            r
        });
        let r = task.await.map_err(io::Error::other)?;
        r.map_err(|e| Error::Io(io::Error::new(e.kind(), format!("write object {h}: {e}"))))?;
        Ok((at, pending))
    }

    /// Appends object `h`. Readers see it only once it is written. Durable after [`Self::sync`].
    pub(crate) async fn put(&self, h: Hash, b: Bytes) -> Result<()> {
        if self.touch(&h) {
            return Ok(());
        }
        let len =
            u32::try_from(b.len()).map_err(|_| Error::Corrupt(format!("object {h} of {} bytes", b.len())))?;
        let (at, pending) = self.append(h, header(h, len), b).await?;
        self.index(h, Loc::new(at, len, None, self.tick()));
        drop(pending);
        self.live.fetch_add(u64::from(len) + HEADER_LEN, Ordering::AcqRel);
        self.appended(u64::from(len) + HEADER_LEN);
        let n = self
            .unwritten
            .fetch_add(u64::from(len) + HEADER_LEN, Ordering::AcqRel);
        if n + u64::from(len) >= WRITEBACK_BYTES && self.holds.load(Ordering::Acquire) == 0 {
            self.write_behind().await;
        }
        Ok(())
    }

    /// Records that object `h` is the `len` bytes at `offset` of `path`, which must not change
    /// until [`Self::drop_file`].
    pub(crate) async fn put_ref(&self, h: Hash, path: &Path, offset: u64, len: u32) -> Result<()> {
        if self.touch(&h) {
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
        let (plen, payload) = ref_payload(h, path, offset, len)?;
        let (at, pending) = self.append(h, header(h, REF_FLAG | plen), payload).await?;
        let n = u64::from(plen) + HEADER_LEN;
        self.index(h, Loc::new(at, len, Some((ext, offset)), self.tick()));
        drop(pending);
        self.appended(n);
        Ok(())
    }

    /// The current epoch.
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// The log's clock now.
    fn now(&self) -> u64 {
        self.clock.load(Ordering::Acquire)
    }

    /// Advances the log's clock for an object appended, and returns the new reading.
    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Marks object `h` in the current epoch when the log has it.
    fn touch(&self, h: &Hash) -> bool {
        let m = self.shard(h).read();
        match m.get(h).filter(|l| !l.dead()) {
            Some(l) => {
                l.mark.fetch_max(self.epoch(), Ordering::AcqRel);
                l.used_at(self.now());
                true
            }
            None => false,
        }
    }

    fn appended(&self, n: u64) {
        if self.appended.fetch_add(n, Ordering::AcqRel) + n >= self.sweep_after.load(Ordering::Acquire) {
            self.due.notify_one();
        }
    }

    /// Notified once enough was appended since the last sweep that another is due.
    pub(crate) fn sweep_due(&self) -> Arc<Notify> {
        Arc::clone(&self.due)
    }

    /// Whether enough was appended since the last sweep that another is due.
    pub(crate) fn sweep_is_due(&self) -> bool {
        self.appended.load(Ordering::Acquire) >= self.sweep_after.load(Ordering::Acquire)
    }

    /// Limits the bytes the log keeps of objects a sweep may drop to `bytes`, or lifts the
    /// limit with zero. A log over it makes a sweep due at once.
    pub(crate) fn set_budget(&self, bytes: u64) {
        self.budget.store(bytes, Ordering::Release);
        let live = self.live.load(Ordering::Acquire);
        let after = next_sweep(live, bytes);
        self.sweep_after.fetch_min(after, Ordering::AcqRel);
        if bytes > 0 && live > bytes {
            self.sweep_after.store(0, Ordering::Release);
            self.due.notify_one();
        }
    }

    /// The most bytes the log keeps of objects a sweep may drop; zero for no limit.
    pub(crate) fn budget(&self) -> u64 {
        self.budget.load(Ordering::Acquire)
    }

    /// The bytes of the records in the log itself, as far as it knows: what the last sweep
    /// kept and what was appended since.
    pub(crate) fn live(&self) -> u64 {
        self.live.load(Ordering::Acquire)
    }

    /// The log's clock at the last read or put of object `h`, when the log has it; a later read
    /// has a reading at least as high.
    pub(crate) fn used(&self, h: &Hash) -> Option<u64> {
        self.lookup(h).map(|l| l.used.load(Ordering::Acquire))
    }

    /// Where the log ends; appends land at or past it.
    pub(crate) fn end(&self) -> u64 {
        self.end.load(Ordering::Acquire)
    }

    /// Starts a new epoch and returns it. A put of an object the log has marks it in the
    /// current epoch.
    pub(crate) fn next_epoch(&self) -> u64 {
        self.epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Marks object `h` in `epoch`; `false` when the log does not have it.
    pub(crate) fn mark(&self, h: &Hash, epoch: u64) -> bool {
        self.lookup(h)
            .map(|l| l.mark.fetch_max(epoch, Ordering::AcqRel))
            .is_some()
    }

    /// Records where the store has object `h`, and whose upload put it there.
    pub(crate) fn set_stored(&self, h: &Hash, l: (u64, StoreLoc)) {
        if let Some(x) = self.lookup(h) {
            *x.stored.lock() = Some(l);
        }
    }

    /// Where the store has object `h`, and whose upload put it there, when one recorded it.
    pub(crate) fn stored(&self, h: &Hash) -> Option<(u64, StoreLoc)> {
        self.lookup(h).and_then(|l| *l.stored.lock())
    }

    /// Drops every record that starts from `floor` and below `below` and was last marked
    /// before epoch `keep_from`, except the tree nodes of `nodes` and the chunks of `chunks`
    /// that fit: all of them without a budget, and with one every node, the chunks appended
    /// since the last sweep began, then the longest run of the rest of `chunks` from its start
    /// that leaves the log within seven eighths of the budget.
    /// Each run of dropped records becomes padding, and its pages are freed once the padding
    /// is durable and no read of a dropped record is left. When that fails, the next sweep pads
    /// the runs.
    #[expect(
        clippy::iter_over_hash_type,
        reason = "the sums and sets it builds do not depend on the order"
    )]
    pub(crate) async fn sweep(
        &self,
        floor: u64,
        below: u64,
        keep_from: u64,
        nodes: &[Hash],
        chunks: &[Hash],
    ) -> Result<Swept> {
        let fresh = self.fresh_from.swap(below, Ordering::AcqRel);
        let droppable = |l: &Loc| {
            (floor..below).contains(&l.born) && (l.mark.load(Ordering::Acquire) < keep_from || l.dead())
        };
        let budget = self.budget.load(Ordering::Acquire);
        let keep: HashSet<Hash> = if budget == 0 {
            nodes.iter().chain(chunks).copied().collect()
        } else {
            let room = budget - budget / 8;
            let mut cost: HashMap<Hash, u64> = HashMap::new();
            let mut base = 0;
            let mut keep = HashSet::new();
            let nodes: HashSet<&Hash> = nodes.iter().collect();
            let wanted: HashSet<&Hash> = chunks.iter().collect();
            for s in &self.shards {
                for (h, l) in s.read().iter() {
                    if !droppable(l) {
                        base += l.own_bytes();
                    } else if nodes.contains(h) || (wanted.contains(h) && l.born >= fresh) {
                        base += l.own_bytes();
                        keep.insert(*h);
                    } else {
                        cost.insert(*h, l.own_bytes());
                    }
                }
            }
            for h in chunks {
                let Some(&c) = cost.get(h) else {
                    continue;
                };
                if base + c > room {
                    break;
                }
                base += c;
                keep.insert(*h);
            }
            keep
        };
        let mut gone = Vec::new();
        let mut kept = 0;
        for s in &self.shards {
            s.write().retain(|h, l| {
                let dropped = droppable(l) && !keep.contains(h);
                if dropped {
                    gone.push((*h, Arc::clone(l)));
                } else {
                    kept += l.own_bytes();
                }
                !dropped
            });
        }
        self.appended.store(0, Ordering::Release);
        self.live.store(kept, Ordering::Release);
        self.sweep_after
            .store(next_sweep(kept, budget), Ordering::Release);
        let mut runs: Vec<(u64, u64)> = Vec::new();
        let mut recs: Vec<(u64, u64)> = gone.iter().map(|(_, l)| (l.off, l.end())).collect();
        recs.extend(std::mem::take(&mut *self.orphans.lock()));
        recs.sort_unstable();
        for (a, b) in recs {
            match runs.last_mut() {
                Some(r) if r.1 == a && b - r.0 <= PAD_MAX => r.1 = b,
                _ => runs.push((a, b)),
            }
        }
        let runs = self.join_padded(runs);
        for (_, l) in &gone {
            while Arc::strong_count(l) > 1 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }
        let dropped = gone.into_iter().map(|(h, _)| h).collect();
        match self.pad_and_free(&runs).await {
            Ok(freed) => {
                self.padded.lock().extend(runs.iter().copied());
                Ok(Swept { dropped, freed, kept })
            }
            Err(e) => {
                self.orphans.lock().extend(runs);
                Err(e)
            }
        }
    }

    /// `runs`, sorted and apart, each extended over the padding that ends where it starts and
    /// the padding that starts where it ends, and joined with the next run it then meets, while
    /// a run stays within [`PAD_MAX`]. The padding a run took in is forgotten: the run's own
    /// padding covers it.
    fn join_padded(&self, runs: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
        let mut padded = self.padded.lock();
        let mut out: Vec<(u64, u64)> = Vec::with_capacity(runs.len());
        for mut r in runs {
            if let Some((&a, &b)) = padded.range(..r.0).next_back()
                && b == r.0
                && r.1 - a <= PAD_MAX
            {
                padded.remove(&a);
                r.0 = a;
            }
            if let Some(&b) = padded.get(&r.1)
                && b - r.0 <= PAD_MAX
            {
                padded.remove(&r.1);
                r.1 = b;
            }
            match out.last_mut() {
                Some(l) if l.1 == r.0 && r.1 - l.0 <= PAD_MAX => l.1 = r.1,
                _ => out.push(r),
            }
        }
        out
    }

    /// Writes padding over `runs` of dropped records and frees their pages; returns the bytes
    /// freed. After a failure the runs may be padded in part, and padding them again is safe.
    async fn pad_and_free(&self, runs: &[(u64, u64)]) -> Result<u64> {
        let mut planned = String::new();
        for &(a, b) in runs {
            let _ = writeln!(planned, "{a} {b}");
        }
        if !runs.is_empty() {
            let mut p = self.pads.lock().await;
            self.list_pads(&mut p, &planned).await?;
            self.write_pads(runs).await?;
            p.headed = p.text.len();
            if p.text.len() > PADS_MAX {
                self.segs.sync().await?;
                let n = p.headed;
                self.unlist_pads(&mut p, n).await?;
            }
        }
        let mut freed = 0;
        for &(a, b) in runs {
            let (from, to) = ((a + HEADER_LEN).next_multiple_of(PAGE), b / PAGE * PAGE);
            if from < to
                && let Some(f) = self.segs.get(seg_of(a))
            {
                f.punch(seg_pos(from), to - from).await?;
                freed += to - from;
            }
        }
        Ok(freed)
    }

    /// Removes each segment before the last two whose records take at most a quarter of
    /// [`SEG_ROLL`], once it moved them to the end of the log and a sync made them durable
    /// there. The segment just before the last stays, so its records have a whole segment of
    /// appends to be written again first: under churn few are left to move, and a compaction
    /// right after a roll would copy what the next writes replace. A segment with a record
    /// from below `floor`, with an append not settled yet, or with a record not indexed yet,
    /// stays. Callers must not run it beside [`Self::sweep`] or another compaction. A crash
    /// before the removal leaves both copies, and the next sweep drops the later one.
    #[expect(
        clippy::iter_over_hash_type,
        reason = "the records of each segment are moved in any order"
    )]
    pub(crate) async fn compact(&self, floor: u64) -> Result<()> {
        let (last, mut busy) = {
            let f = self.in_flight.lock();
            let mut busy: HashSet<u64> = f.iter().map(|&a| seg_of(a)).collect();
            busy.extend(self.unindexed.lock().iter().map(|&a| seg_of(a)));
            (seg_of(self.end()), busy)
        };
        busy.extend(self.unpadded.lock().keys().map(|&a| seg_of(a)));
        let ids: Vec<u64> = self
            .segs
            .files
            .read()
            .keys()
            .copied()
            .filter(|id| *id + 1 < last && !busy.contains(id))
            .collect();
        if ids.is_empty() {
            return Ok(());
        }
        let mut held: HashMap<u64, Held> = HashMap::new();
        for s in &self.shards {
            for (h, l) in s.read().iter() {
                if seg_of(l.off) < last {
                    let e = held.entry(seg_of(l.off)).or_default();
                    e.bytes += l.end() - l.off;
                    e.old |= l.born < floor;
                    e.locs.push((*h, Arc::clone(l)));
                }
            }
        }
        let mut moved = Vec::new();
        let mut gone = Vec::new();
        for id in ids {
            let Held { bytes, old, locs } = held.remove(&id).unwrap_or_default();
            if old || bytes > SEG_ROLL / 4 {
                continue;
            }
            moved.extend(self.relocate(locs).await?);
            gone.push(id);
        }
        drop(held);
        if gone.is_empty() {
            return Ok(());
        }
        self.sync().await?;
        for (old, new) in &moved {
            while Arc::strong_count(old) > 1 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            take_uses(old, new);
        }
        for id in gone {
            let span = seg_base(id)..seg_base(id + 1);
            self.padded.lock().retain(|a, _| !span.contains(a));
            self.orphans.lock().retain(|(a, _)| !span.contains(a));
            self.segs.files.write().remove(&id);
            self.segs.dirty.lock().remove(&id);
            self.fs.remove(&self.segs.path(id)).await?;
        }
        self.fs.sync_dir(&self.path).await?;
        Ok(())
    }

    /// Appends each record of `locs` again and indexes the copy in its place; returns each old
    /// location with its copy. The record of a file that is gone is dropped instead.
    async fn relocate(&self, locs: Vec<(Hash, Arc<Loc>)>) -> Result<Vec<(Arc<Loc>, Arc<Loc>)>> {
        let mut out = Vec::with_capacity(locs.len());
        for (h, old) in locs {
            if old.dead() {
                let mut m = self.shard(&h).write();
                if m.get(&h).is_some_and(|l| Arc::ptr_eq(l, &old)) {
                    m.remove(&h);
                }
                continue;
            }
            let (hdr, body) = match &old.ext {
                None => {
                    let f = self
                        .segs
                        .get(seg_of(old.off))
                        .ok_or_else(|| Error::Corrupt(format!("object {h} at {} has no segment", old.off)))?;
                    let b = f.read_at(old.len as usize, seg_pos(old.off) + HEADER_LEN).await?;
                    if b.len() != old.len as usize || Hash::of(&b) != h {
                        return Err(Error::Corrupt(format!(
                            "object {h} at {} of the object log {} does not match its hash",
                            old.off,
                            self.path.display()
                        )));
                    }
                    (header(h, old.len), b)
                }
                Some((e, off)) => {
                    let (plen, p) = ref_payload(h, &e.path, *off, old.len)?;
                    (header(h, REF_FLAG | plen), p)
                }
            };
            let (at, pending) = self.append(h, hdr, body).await?;
            let new = Arc::new(Loc {
                off: at,
                born: old.born,
                len: old.len,
                touched: AtomicBool::new(false),
                ext: old.ext.clone(),
                mark: AtomicU64::new(0),
                used: AtomicU64::new(0),
                stored: Mutex::new(None),
            });
            let mut m = self.shard(&h).write();
            if m.get(&h).is_some_and(|l| Arc::ptr_eq(l, &old)) {
                take_uses(&old, &new);
                m.insert(h, Arc::clone(&new));
                drop(m);
                out.push((old, new));
            } else {
                drop(m);
                self.orphans.lock().push((at, new.end()));
            }
            drop(pending);
        }
        Ok(out)
    }

    /// Turns every record that names `path` into a miss; call it before removing the file.
    pub(crate) fn drop_file(&self, path: &Path) {
        let e = self.ext_file(path);
        e.gone.store(true, Ordering::Release);
        e.file.lock().take();
    }

    async fn source(&self, loc: &Loc) -> Option<(Arc<dyn LocalFile>, u64)> {
        match &loc.ext {
            None => self
                .segs
                .get(seg_of(loc.off))
                .map(|f| (f, seg_pos(loc.off) + HEADER_LEN)),
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
        loc.used_at(self.now());
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
            None => match self.segs.get(seg_of(loc.off)) {
                Some(f) => (f, seg_pos(loc.off) + HEADER_LEN),
                None => return Ok(false),
            },
            Some((e, off)) => match e.opened() {
                Some(f) => (f, *off),
                None => return Ok(false),
            },
        };
        loc.used_at(self.now());
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

    /// Writes the padding that failed appends could not write.
    async fn pad(&self) -> Result<()> {
        let pending: Vec<(u64, u32)> = self.unpadded.lock().iter().map(|(a, n)| (*a, *n)).collect();
        for (at, len) in pending {
            let r = match self.segs.open(seg_of(at)).await {
                Ok(f) => {
                    f.write_at(vec![header(pad_hash(at, len), len)], seg_pos(at))
                        .await
                }
                Err(e) => Err(e),
            };
            if let Err(e) = r {
                return Err(Error::Io(io::Error::new(
                    e.kind(),
                    format!(
                        "the object log {} cannot pad a failed write at {at}: {e}",
                        self.path.display()
                    ),
                )));
            }
            self.segs.written(seg_of(at));
            self.unpadded.lock().remove(&at);
            self.in_flight.lock().remove(&at);
        }
        Ok(())
    }

    pub(crate) fn hold_writeback(&self) -> WritebackHold<'_> {
        self.holds.fetch_add(1, Ordering::AcqRel);
        WritebackHold(self)
    }

    /// Starts the write-back, then waits until all but the last [`BEHIND_BYTES`] are on the
    /// disk.
    async fn write_behind(&self) {
        self.unwritten.store(0, Ordering::Release);
        let _behind = self.behind.lock().await;
        let end = self.end.load(Ordering::Acquire);
        let last = self.segs.get(seg_of(end));
        if let Some(f) = &last {
            f.start_writeback().await;
        }
        let below = end.saturating_sub(BEHIND_BYTES);
        if seg_of(below) == seg_of(end) {
            if let Some(f) = &last {
                f.write_behind(seg_pos(below)).await;
            }
        } else if let Some(f) = self.segs.get(seg_of(below)) {
            f.start_writeback().await;
            f.write_behind(seg_pos(below)).await;
        }
    }

    fn write_back(&self) {
        if self.writing_back.swap(true, Ordering::AcqRel) {
            return;
        }
        self.unwritten.store(0, Ordering::Release);
        let (file, flag) = (self.segs.get(seg_of(self.end())), Arc::clone(&self.writing_back));
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                rt.spawn(async move {
                    if let Some(file) = file {
                        file.start_writeback().await;
                    }
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
            self.pad().await?;
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
        let headed = {
            let p = self.pads.lock().await;
            p.dropped + p.headed
        };
        self.segs.sync().await?;
        write_atomic(
            self.fs.as_ref(),
            &self.synced_path(),
            Bytes::from(end.to_string()),
        )
        .await?;
        let mut p = self.pads.lock().await;
        let n = headed.saturating_sub(p.dropped);
        self.unlist_pads(&mut p, n).await?;
        Ok(())
    }
}

/// The disk the files of the log at `log` take.
#[cfg(all(test, feature = "sim"))]
pub(crate) async fn allocated(fs: &crate::sim::SimFs, log: &Path) -> io::Result<u64> {
    use crate::ports::LocalImage;
    let mut n = 0;
    for e in fs.list(log).await? {
        n += fs.allocated(&log.join(&e.name)).await?;
    }
    Ok(n)
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use std::collections::HashSet;

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
    async fn a_sync_after_a_restart_makes_the_records_before_it_durable() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let (h, b) = obj(1);
        log.put(h, b.clone()).await.unwrap();
        drop(log);

        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        again.sync().await.unwrap();
        drop(again);

        fs.crash();
        let after = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert_eq!(after.get(&h).await.unwrap(), Some(b));
    }

    #[tokio::test]
    async fn a_sync_after_a_restart_makes_the_referenced_bytes_before_it_durable() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let img = fs.open(Path::new("/c/img"), Open::CreateNew).await.unwrap();
        fs.sync_dir(Path::new("/c")).await.unwrap();
        let (h, b) = obj(3);
        img.write_at(vec![b.clone()], 0).await.unwrap();
        log.put_ref(h, Path::new("/c/img"), 0, u32::try_from(b.len()).unwrap())
            .await
            .unwrap();
        drop(log);

        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        again.sync().await.unwrap();
        drop(again);

        fs.crash();
        let after = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert_eq!(after.get(&h).await.unwrap(), Some(b));
    }

    #[tokio::test]
    async fn an_open_syncs_again_only_the_segments_past_the_synced_marker() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        for i in 0..5u8 {
            let b = Bytes::from(vec![i; 300 << 10]);
            log.put(Hash::of(&b), b).await.unwrap();
        }
        log.sync().await.unwrap();
        assert_eq!(seg_of(log.end()), 1);
        drop(log);

        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert_eq!(
            again.segs.dirty.lock().keys().copied().collect::<Vec<_>>(),
            vec![1]
        );
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
    async fn zero_hash_padding_is_skipped_below_the_synced_mark_and_ends_the_log_above_it() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let ((h1, b1), (h2, b2)) = (obj(1), obj(2));
        log.put(h1, b1).await.unwrap();
        drop(log);
        let f = fs.open(&path.join("0"), Open::ReadWrite).await.unwrap();
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
    async fn a_put_on_a_full_disk_fails_and_the_log_syncs_once_there_is_room() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let ((h1, b1), (h2, b2), (h3, b3)) = (obj(1), obj(2), obj(3));
        log.put(h1, b1.clone()).await.unwrap();
        log.sync().await.unwrap();
        let big = u64::MAX / 2;
        fs.set_capacity(Some(big));
        let used = big - fs.available(Path::new("/c")).await.unwrap();
        fs.set_capacity(Some(used + 20));
        let e = log.put(h2, b2).await.unwrap_err();
        assert!(e.is_no_room(), "{e}");
        assert!(!log.has(&h2));
        let e = log.sync().await.unwrap_err();
        assert!(e.is_no_room(), "the padding does not fit either: {e}");

        fs.set_capacity(None);
        log.put(h3, b3.clone()).await.unwrap();
        log.sync().await.unwrap();
        drop(log);
        fs.crash();
        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert_eq!(again.get(&h1).await.unwrap(), Some(b1));
        assert_eq!(
            again.get(&h3).await.unwrap(),
            Some(b3),
            "the padding is skipped although the crash lost the synced mark"
        );
        assert!(!again.has(&h2));
    }

    #[tokio::test]
    async fn a_torn_record_ends_the_log() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let (h1, b1) = obj(1);
        log.put(h1, b1).await.unwrap();
        let f = fs.open(&path.join("0"), Open::ReadWrite).await.unwrap();
        let size = f.size().await.unwrap();
        f.write_at(vec![Bytes::from(vec![9; 50])], size).await.unwrap();
        drop(log);
        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert!(again.has(&h1));
        assert_eq!(f.size().await.unwrap(), size);
    }

    #[tokio::test]
    async fn a_sweep_drops_what_no_epoch_marked_frees_its_disk_and_survives_a_reopen() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let big = |i: u8| {
            let b = Bytes::from(vec![i; 20_000]);
            (Hash::of(&b), b)
        };
        let objs: Vec<(Hash, Bytes)> = (1..=6).map(big).collect();
        for (h, b) in &objs {
            log.put(*h, b.clone()).await.unwrap();
        }
        log.sync().await.unwrap();
        let epoch = log.next_epoch();
        assert!(log.mark(&objs[1].0, epoch));
        log.put(objs[4].0, objs[4].1.clone()).await.unwrap();
        let below = log.end();
        let (late_h, late_b) = big(7);
        log.put(late_h, late_b.clone()).await.unwrap();
        let before = allocated(&fs, path).await.unwrap();
        let swept = log.sweep(0, below, epoch, &[], &[]).await.unwrap();
        let dropped: HashSet<Hash> = swept.dropped.into_iter().collect();
        let want: Vec<Hash> = [0, 2, 3, 5].iter().map(|&i| objs[i].0).collect();
        assert_eq!(
            dropped,
            want.iter().copied().collect(),
            "the marked, the put again and the appended after stay"
        );
        let after = allocated(&fs, path).await.unwrap();
        assert!(
            before - after >= 3 * 20_000,
            "{before} -> {after}, freed {}",
            swept.freed
        );
        for i in [1, 4] {
            assert_eq!(log.get(&objs[i].0).await.unwrap(), Some(objs[i].1.clone()));
        }
        assert!(!log.has(&objs[0].0));
        log.sync().await.unwrap();
        drop(log);

        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        for i in [1, 4] {
            assert_eq!(again.get(&objs[i].0).await.unwrap(), Some(objs[i].1.clone()));
        }
        assert_eq!(again.get(&late_h).await.unwrap(), Some(late_b));
        for h in &want {
            assert!(!again.has(h), "the padding over a dropped record is skipped");
        }
        let (h, b) = big(9);
        again.put(h, b.clone()).await.unwrap();
        assert_eq!(again.get(&h).await.unwrap(), Some(b));
    }

    #[tokio::test]
    async fn an_epoch_held_open_keeps_what_was_put_again_not_all_it_appended() {
        let log = ObjectLog::open(Arc::new(SimFs::default()), Path::new("/c/objects"))
            .await
            .unwrap();
        let ((h1, b1), (h2, b2)) = (obj(1), obj(2));
        log.put(h1, b1).await.unwrap();
        log.put(h2, b2.clone()).await.unwrap();
        let (below, keep_from) = (log.end(), log.epoch());
        log.put(h2, b2.clone()).await.unwrap();
        log.next_epoch();
        let swept = log.sweep(0, below, keep_from, &[], &[]).await.unwrap();
        assert_eq!(swept.dropped, vec![h1], "an append alone marks nothing");
        assert_eq!(log.get(&h2).await.unwrap(), Some(b2));
    }

    #[tokio::test]
    async fn sweeps_that_each_drop_the_record_before_leave_no_disk_behind() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let rec = |i: u32| {
            let b = Bytes::from(
                (0..65_536u32)
                    .map(|j| u8::try_from((i * 7 + j) % 251).unwrap() | 1)
                    .collect::<Vec<u8>>(),
            );
            (Hash::of(&b), b)
        };
        for i in 0..200 {
            let (h, b) = rec(i);
            log.put(h, b).await.unwrap();
            let epoch = log.next_epoch();
            assert!(log.mark(&h, epoch));
            log.sweep(0, log.end(), epoch, &[], &[]).await.unwrap();
            log.compact(0).await.unwrap();
        }
        log.sync().await.unwrap();
        let allocated = allocated(&fs, path).await.unwrap();
        assert!(
            allocated <= 2 * (65_536 + 2 * PAGE),
            "{allocated} bytes allocated for one record of 64 KiB, the log ending at {}",
            log.end()
        );
        drop(log);
        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let (h, b) = rec(199);
        assert_eq!(again.get(&h).await.unwrap(), Some(b));
        assert!(!again.has(&rec(198).0));
    }

    #[tokio::test]
    async fn padding_between_two_runs_a_sweep_drops_joins_them_into_one() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let rec = |i: u8| {
            let b = Bytes::from(vec![i; 30_000]);
            (Hash::of(&b), b)
        };
        let recs: Vec<(Hash, Bytes)> = (1..=3).map(rec).collect();
        for (h, b) in &recs {
            log.put(*h, b.clone()).await.unwrap();
        }
        let epoch = log.next_epoch();
        assert!(log.mark(&recs[0].0, epoch) && log.mark(&recs[2].0, epoch));
        log.sweep(0, log.end(), epoch, &[], &[]).await.unwrap();
        let epoch = log.next_epoch();
        log.sweep(0, log.end(), epoch, &[], &[]).await.unwrap();
        log.sync().await.unwrap();
        assert_eq!(
            allocated(&fs, path).await.unwrap(),
            2 * PAGE,
            "one run of padding over all three, which keeps only the page of its header and the \
             page it ends in"
        );
        drop(log);
        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert!(recs.iter().all(|(h, _)| !again.has(h)));
        assert_eq!(again.end(), 3 * (HEADER_LEN + 30_000));
    }

    async fn segments(fs: &SimFs, path: &Path) -> Vec<(u64, u64)> {
        let mut v: Vec<(u64, u64)> = fs
            .list(path)
            .await
            .unwrap()
            .into_iter()
            .map(|e| (e.name.parse().unwrap(), e.len))
            .collect();
        v.sort_unstable();
        v
    }

    fn rec(i: u32) -> (Hash, Bytes) {
        let b = Bytes::from(
            (0..65_536u32)
                .map(|j| u8::try_from((i * 7 + j) % 251).unwrap() | 1)
                .collect::<Vec<u8>>(),
        );
        (Hash::of(&b), b)
    }

    #[tokio::test]
    async fn a_log_that_keeps_one_record_through_much_churn_keeps_its_files_small() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let (kh, kb) = obj(1);
        log.put(kh, kb.clone()).await.unwrap();
        for i in 0..400 {
            let (h, b) = rec(i);
            log.put(h, b).await.unwrap();
            let epoch = log.next_epoch();
            assert!(log.mark(&h, epoch) && log.mark(&kh, epoch));
            log.sweep(0, log.end(), epoch, &[], &[]).await.unwrap();
            log.compact(0).await.unwrap();
        }
        assert!(seg_of(log.end()) > 20, "the log ends at {}", log.end());
        let segs = segments(&fs, path).await;
        assert!(segs.len() <= 2, "{segs:?}");
        let apparent: u64 = segs.iter().map(|s| s.1).sum();
        assert!(apparent <= 2 * SEG_ROLL, "{apparent} bytes in {segs:?}");
        assert_eq!(log.get(&kh).await.unwrap(), Some(kb.clone()));
        drop(log);
        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert_eq!(again.get(&kh).await.unwrap(), Some(kb));
        assert_eq!(again.get(&rec(399).0).await.unwrap(), Some(rec(399).1));
        assert!(!again.has(&rec(398).0));
    }

    #[tokio::test]
    async fn a_moved_record_keeps_its_marks_and_age_and_survives_a_crash() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let img = fs.open(Path::new("/c/img"), Open::CreateNew).await.unwrap();
        let ((kh, kb), (rh, rb), (oh, ob)) = (obj(1), obj(2), obj(3));
        img.write_at(vec![rb.clone()], 0).await.unwrap();
        fs.sync_dir(Path::new("/c")).await.unwrap();
        log.put(kh, kb.clone()).await.unwrap();
        log.put(oh, ob.clone()).await.unwrap();
        log.put_ref(rh, Path::new("/c/img"), 0, u32::try_from(rb.len()).unwrap())
            .await
            .unwrap();
        let before = log.end();
        for i in 0..36 {
            let (h, b) = rec(i);
            log.put(h, b).await.unwrap();
        }
        assert!(seg_of(log.end()) > 1);
        let epoch = log.next_epoch();
        for h in [kh, rh, oh] {
            assert!(log.mark(&h, epoch));
        }
        log.sweep(0, log.end(), epoch, &[], &[]).await.unwrap();
        log.compact(0).await.unwrap();
        assert!(
            segments(&fs, path).await.iter().all(|s| s.0 > 0),
            "the first segment is gone"
        );
        assert!(log.lookup(&kh).unwrap().off >= before);
        assert_eq!(log.lookup(&kh).unwrap().born, 0);
        assert_eq!(log.get(&kh).await.unwrap(), Some(kb.clone()));
        assert_eq!(log.get(&rh).await.unwrap(), Some(rb.clone()));

        let older = log.next_epoch();
        assert!(log.mark(&kh, older) && log.mark(&rh, older));
        let swept = log.sweep(0, before, older, &[], &[]).await.unwrap();
        assert_eq!(
            swept.dropped,
            vec![oh],
            "a moved record is as old as where it was first appended"
        );
        fs.crash();
        drop(log);
        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert_eq!(again.get(&kh).await.unwrap(), Some(kb));
        assert_eq!(again.get(&rh).await.unwrap(), Some(rb));
    }

    #[tokio::test]
    async fn the_segment_before_the_last_waits_a_segment_before_its_compaction() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let (kh, kb) = obj(1);
        log.put(kh, kb.clone()).await.unwrap();
        let mut i = 0;
        let sweep = async |log: &ObjectLog, until: u64, i: &mut u32| {
            while seg_of(log.end()) < until {
                let (h, b) = rec(*i);
                log.put(h, b).await.unwrap();
                *i += 1;
            }
            let epoch = log.next_epoch();
            assert!(log.mark(&kh, epoch));
            log.sweep(0, log.end(), epoch, &[], &[]).await.unwrap();
            log.compact(0).await.unwrap();
        };
        sweep(&log, 1, &mut i).await;
        assert_eq!(
            segments(&fs, path).await[0].0,
            0,
            "the segment just rolled from stays"
        );
        assert_eq!(seg_of(log.lookup(&kh).unwrap().off), 0);
        sweep(&log, 2, &mut i).await;
        assert_ne!(segments(&fs, path).await[0].0, 0, "a segment later it goes");
        assert_eq!(seg_of(log.lookup(&kh).unwrap().off), 2);
        assert_eq!(log.get(&kh).await.unwrap(), Some(kb));
    }

    #[tokio::test]
    async fn a_segment_that_holds_more_than_a_quarter_stays() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        for i in 0..36 {
            let (h, b) = rec(i);
            log.put(h, b).await.unwrap();
        }
        assert!(seg_of(log.end()) > 1);
        let epoch = log.next_epoch();
        for i in 0..6 {
            assert!(log.mark(&rec(i).0, epoch));
        }
        log.sweep(0, log.end(), epoch, &[], &[]).await.unwrap();
        log.compact(0).await.unwrap();
        assert_eq!(
            segments(&fs, path).await[0].0,
            0,
            "six records of 64 KiB are kept in place"
        );
        let epoch = log.next_epoch();
        assert!(log.mark(&rec(0).0, epoch));
        log.sweep(0, log.end(), epoch, &[], &[]).await.unwrap();
        log.compact(1).await.unwrap();
        assert_eq!(
            segments(&fs, path).await[0].0,
            0,
            "a record below the floor stays in place"
        );
        log.compact(0).await.unwrap();
        assert_ne!(segments(&fs, path).await[0].0, 0);
        assert_eq!(log.get(&rec(0).0).await.unwrap(), Some(rec(0).1));
    }

    #[tokio::test]
    async fn a_record_that_landed_but_is_not_indexed_keeps_its_segment() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        for i in 0..14 {
            let (h, b) = rec(i);
            log.put(h, b).await.unwrap();
        }
        let (h, b) = rec(100);
        let len = u32::try_from(b.len()).unwrap();
        let (at, pending) = log.append(h, header(h, len), b.clone()).await.unwrap();
        assert_eq!(seg_of(at), 0);
        for i in 14..36 {
            let (h, b) = rec(i);
            log.put(h, b).await.unwrap();
        }
        assert!(seg_of(log.end()) > 1);
        let epoch = log.next_epoch();
        log.sweep(0, log.end(), epoch, &[], &[]).await.unwrap();
        log.compact(0).await.unwrap();
        assert_eq!(segments(&fs, path).await[0].0, 0);
        log.index(h, Loc::new(at, len, None, log.tick()));
        drop(pending);
        assert_eq!(log.get(&h).await.unwrap(), Some(b.clone()));
        log.compact(0).await.unwrap();
        assert_ne!(segments(&fs, path).await[0].0, 0);
        assert_eq!(log.get(&h).await.unwrap(), Some(b));
    }

    #[tokio::test]
    async fn a_torn_record_removes_the_segments_after_it() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        for i in 0..20 {
            let (h, b) = rec(i);
            log.put(h, b).await.unwrap();
        }
        log.sync().await.unwrap();
        let last = seg_of(log.end());
        assert!(last > 0);
        assert_eq!(seg_of(log.lookup(&rec(0).0).unwrap().off), 0);
        drop(log);
        let f = fs.open(&path.join("0"), Open::ReadWrite).await.unwrap();
        let size = f.size().await.unwrap();
        f.write_at(vec![Bytes::from(vec![9; 50])], size).await.unwrap();
        fs.remove(Path::new("/c/objects.synced")).await.unwrap();
        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert!(again.has(&rec(0).0));
        assert!(!again.has(&rec(19).0), "the record in a later segment is gone");
        assert_eq!(segments(&fs, path).await.len(), 1);
        assert_eq!(again.end(), size);
    }

    #[tokio::test]
    async fn a_budget_keeps_the_later_of_two_reads_in_one_epoch() {
        let fs = SimFs::default();
        let log = ObjectLog::open(Arc::new(fs.clone()), Path::new("/c/objects"))
            .await
            .unwrap();
        let obj = |i: u8| {
            let b = Bytes::from(vec![i; 20_000]);
            (Hash::of(&b), b)
        };
        let objs: Vec<(Hash, Bytes)> = (1..=3).map(obj).collect();
        for (h, b) in &objs[..2] {
            log.put(*h, b.clone()).await.unwrap();
        }
        log.sync().await.unwrap();
        let all: Vec<Hash> = objs.iter().map(|(h, _)| *h).collect();
        let epoch = log.next_epoch();
        log.sweep(0, log.end(), epoch, &[], &all).await.unwrap();

        let epoch = log.next_epoch();
        log.get(&objs[0].0).await.unwrap();
        log.put(objs[2].0, objs[2].1.clone()).await.unwrap();
        log.get(&objs[1].0).await.unwrap();
        assert_eq!(log.epoch(), epoch);
        assert!(
            log.used(&objs[1].0).unwrap() > log.used(&objs[0].0).unwrap(),
            "the later read stamps later"
        );
        let mut wanted: Vec<(u64, Hash)> = all.iter().map(|h| (log.used(h).unwrap(), *h)).collect();
        wanted.sort_by_key(|&(used, _)| std::cmp::Reverse(used));
        let wanted: Vec<Hash> = wanted.into_iter().map(|(_, h)| h).collect();
        let record = 20_000 + HEADER_LEN;
        log.set_budget(2 * record + record / 2);
        let swept = log
            .sweep(0, log.end(), log.next_epoch(), &[], &wanted)
            .await
            .unwrap();
        assert_eq!(swept.dropped, vec![objs[0].0], "the earlier read goes");
    }

    #[tokio::test]
    async fn a_sweep_that_fails_to_pad_leaves_its_runs_to_the_next() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        for i in 0..4u8 {
            let b = Bytes::from(vec![i; 20_000]);
            log.put(Hash::of(&b), b).await.unwrap();
        }
        log.sync().await.unwrap();
        let before = allocated(&fs, path).await.unwrap();
        fs.stop_after(Some(0));
        let epoch = log.next_epoch();
        assert!(log.sweep(0, log.end(), epoch, &[], &[]).await.is_err());
        fs.stop_after(None);
        let epoch = log.next_epoch();
        let swept = log.sweep(0, log.end(), epoch, &[], &[]).await.unwrap();
        assert!(swept.dropped.is_empty(), "the failed sweep dropped them");
        assert!(swept.freed > 0, "the next sweep frees their pages");
        assert!(allocated(&fs, path).await.unwrap() < before);
        drop(log);
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert_eq!(log.live(), 0, "an open finds padding only");
    }

    #[tokio::test]
    async fn a_budget_keeps_the_wanted_records_read_last_and_never_the_marked_ones() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let big = |i: u8| {
            let b = Bytes::from(vec![i; 20_000]);
            (Hash::of(&b), b)
        };
        let objs: Vec<(Hash, Bytes)> = (1..=8).map(big).collect();
        for (h, b) in &objs {
            log.put(*h, b.clone()).await.unwrap();
        }
        log.sync().await.unwrap();
        assert_eq!(
            log.sweep(0, log.end(), 0, &[], &[]).await.unwrap().dropped,
            vec![]
        );
        let record = 20_000 + HEADER_LEN;
        log.set_budget(4 * record + record / 2);
        assert!(log.sweep_is_due(), "the log is over its budget");
        log.next_epoch();
        for i in [5, 2] {
            log.get(&objs[i].0).await.unwrap();
        }
        let epoch = log.next_epoch();
        assert!(log.mark(&objs[7].0, epoch));
        let mut wanted: Vec<(u64, Hash)> = objs[..7]
            .iter()
            .map(|(h, _)| (log.used(h).unwrap(), *h))
            .collect();
        wanted.sort_by_key(|&(used, _)| std::cmp::Reverse(used));
        let wanted: Vec<Hash> = wanted.into_iter().map(|(_, h)| h).collect();
        let swept = log.sweep(0, log.end(), epoch, &[], &wanted).await.unwrap();
        assert_eq!(swept.kept, 3 * record, "the marked one and two read last fit");
        assert!(!log.sweep_is_due());
        for i in [2, 5, 7] {
            assert_eq!(log.get(&objs[i].0).await.unwrap(), Some(objs[i].1.clone()));
        }
        for i in [0, 1, 3, 4, 6] {
            assert!(!log.has(&objs[i].0), "object {i} goes");
        }
        assert!(allocated(&fs, path).await.unwrap() <= 4 * record + 8 * PAGE);

        log.set_budget(0);
        let epoch = log.next_epoch();
        let swept = log
            .sweep(0, log.end(), epoch, &[], &[objs[2].0, objs[5].0])
            .await
            .unwrap();
        assert_eq!(
            swept.dropped,
            vec![objs[7].0],
            "without a budget every wanted record stays"
        );

        log.set_budget(record);
        let epoch = log.next_epoch();
        let swept = log
            .sweep(0, log.end(), epoch, &[objs[5].0], &[objs[2].0])
            .await
            .unwrap();
        assert_eq!(swept.dropped, vec![objs[2].0], "a node stays past the budget");
        assert_eq!(swept.kept, record);
    }

    #[tokio::test]
    async fn a_sweep_lists_its_padding_until_a_sync_so_a_crash_before_one_loses_nothing() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let pads = Path::new("/c/objects.pads");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let big = |i: u8| {
            let b = Bytes::from(vec![i; 20_000]);
            (Hash::of(&b), b)
        };
        let objs: Vec<(Hash, Bytes)> = (1..=6).map(big).collect();
        for (h, b) in &objs {
            log.put(*h, b.clone()).await.unwrap();
        }
        log.sync().await.unwrap();
        let epoch = log.next_epoch();
        assert!(log.mark(&objs[1].0, epoch));
        let swept = log.sweep(0, log.end(), epoch, &[], &[]).await.unwrap();
        assert_eq!(swept.dropped.len(), 5);
        assert!(!read_file(&fs, pads).await.unwrap().unwrap().is_empty());
        fs.crash();
        drop(log);

        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert_eq!(log.get(&objs[1].0).await.unwrap(), Some(objs[1].1.clone()));
        for i in [0, 2, 3, 4, 5] {
            assert!(!log.has(&objs[i].0), "object {i} stays dropped");
        }
        assert_eq!(read_file(&fs, pads).await.unwrap(), Some(Bytes::new()));

        let (h, b) = big(9);
        log.put(h, b.clone()).await.unwrap();
        let epoch = log.next_epoch();
        assert!(log.mark(&h, epoch));
        assert_eq!(
            log.sweep(0, log.end(), epoch, &[], &[]).await.unwrap().dropped,
            vec![objs[1].0]
        );
        log.sync().await.unwrap();
        assert_eq!(
            read_file(&fs, pads).await.unwrap(),
            Some(Bytes::new()),
            "a sync made the padding durable"
        );
        drop(log);

        let (a, end) = {
            let l = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
            let x = l.lookup(&h).unwrap();
            (x.off, x.end())
        };
        write_durable(&fs, pads, Bytes::from(format!("{a} {end}\n{a} 9")))
            .await
            .unwrap();
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert!(!log.has(&h), "the whole line is padded, the torn one is not read");
        assert_eq!(read_file(&fs, pads).await.unwrap(), Some(Bytes::new()));
    }

    #[tokio::test]
    async fn an_open_finishes_the_padding_a_sweep_recorded() {
        let fs = SimFs::default();
        let path = Path::new("/c/objects");
        let log = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        let ((h1, b1), (h2, b2), (h3, b3)) = (obj(1), obj(2), obj(3));
        for (h, b) in [(h1, b1.clone()), (h2, b2), (h3, b3.clone())] {
            log.put(h, b).await.unwrap();
        }
        log.sync().await.unwrap();
        let (a, b) = {
            let l = log.lookup(&h2).unwrap();
            (l.off, l.end())
        };
        drop(log);
        write_durable(
            &fs,
            Path::new("/c/objects.pads"),
            Bytes::from(format!("{a} {b}\n")),
        )
        .await
        .unwrap();
        let again = ObjectLog::open(Arc::new(fs.clone()), path).await.unwrap();
        assert!(!again.has(&h2), "the recorded run is padding");
        assert_eq!(again.get(&h1).await.unwrap(), Some(b1));
        assert_eq!(again.get(&h3).await.unwrap(), Some(b3));
        assert_eq!(
            read_file(&fs, Path::new("/c/objects.pads")).await.unwrap(),
            Some(Bytes::new())
        );
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
