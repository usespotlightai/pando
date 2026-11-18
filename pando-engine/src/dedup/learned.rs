//! Learned chunks: whole chunks that runs on this host uploaded, kept as recipe sources for later
//! runs.
//!
//! A tool that runs again on the same host (an install, a build, a test run) writes much of what
//! it wrote last time, at new places. The base does not hold those blocks, so without help every
//! run uploads them again.
//!
//! A learned source is a chunk an earlier run uploaded as itself, never a recipe, so a reader
//! rebuilds a chunk with one level of sources. A host learns only chunks whose upload was
//! published. Each learned chunk carries the time it was last published: as itself, or as the
//! source of a published recipe or reuse. The host forgets a chunk [`MAX_AGE`] after that time,
//! and garbage collection keeps every segment younger than that, so the segment of a chunk
//! published or used within [`MAX_AGE`] still exists.
//!
//! Files, per run: `<stem>.chunks` holds the chunk bytes by slot, `<stem>.lrn` the records.

use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use pando_format::{
    BLOCK_SIZE, BLOCKS_PER_CHUNK, BlockKey, BlockKeys, CHUNK_SIZE, ENTRY_SIZE, Entry, Hash, Loc, is_zero,
    u64_from,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::BlockAt;
use crate::local::{read_file, take, write_atomic};
use crate::ports::{Clock, Cpu, CpuClass, LocalFile, LocalFs, Open, on_cpu};
use crate::{Error, Result};

const MAGIC: &[u8; 4] = b"lrn2";
/// A host forgets a learned chunk this long after it was last published.
pub(crate) const MAX_AGE: Duration = Duration::from_hours(12);
/// Learned chunks a run loads at most, the most recently published first.
const MAX_CHUNKS: usize = 16384;
/// Chunks waiting for the writer; past it, chunks are not learned, so learning never slows an
/// upload.
const QUEUE: usize = 256;
const RECORD: usize = 4 + ENTRY_SIZE + 2 + BLOCKS_PER_CHUNK * 8 + 8;
const CHUNK: u64 = u64_from(CHUNK_SIZE);

/// One learned chunk: its published entry, its slot in the run's chunk file, the keys of its
/// non-zero blocks, and when it was last published (Unix ns).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct LearnedRec {
    pub(crate) entry: Entry,
    pub(crate) slot: u32,
    pub(crate) mask: u16,
    pub(crate) keys: [BlockKey; BLOCKS_PER_CHUNK],
    pub(crate) used: i64,
}

fn encode(rs: &[LearnedRec]) -> Result<Bytes> {
    let n = u32::try_from(rs.len())
        .map_err(|_| Error::Corrupt(format!("{} learned chunks in one record file", rs.len())))?;
    let mut b = Vec::with_capacity(8 + rs.len() * RECORD);
    b.extend_from_slice(MAGIC);
    b.extend_from_slice(&n.to_le_bytes());
    for r in rs {
        b.extend_from_slice(&r.slot.to_le_bytes());
        b.extend_from_slice(&r.entry.to_bytes());
        b.extend_from_slice(&r.mask.to_le_bytes());
        for k in &r.keys {
            b.extend_from_slice(&k.0.to_le_bytes());
        }
        b.extend_from_slice(&r.used.to_le_bytes());
    }
    Ok(Bytes::from(b))
}

fn decode(b: &[u8]) -> Option<Vec<LearnedRec>> {
    let rest = b.strip_prefix(MAGIC)?;
    let (n, rest) = rest.split_first_chunk::<4>()?;
    let n = u32::from_le_bytes(*n) as usize;
    let (records, tail) = rest.as_chunks::<RECORD>();
    if records.len() != n || !tail.is_empty() {
        return None;
    }
    records
        .iter()
        .map(|r| {
            let mut r: &[u8] = r;
            let slot = u32::from_le_bytes(take(&mut r)?);
            let entry = Entry::from_bytes(&take(&mut r)?);
            let mask = u16::from_le_bytes(take(&mut r)?);
            let mut keys = [BlockKey::default(); BLOCKS_PER_CHUNK];
            for k in &mut keys {
                *k = BlockKey(u64::from_le_bytes(take(&mut r)?));
            }
            let used = i64::from_le_bytes(take(&mut r)?);
            Some(LearnedRec {
                entry,
                slot,
                mask,
                keys,
                used,
            })
        })
        .collect()
}

fn nanos(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

fn unix_nanos(t: SystemTime) -> i64 {
    t.duration_since(SystemTime::UNIX_EPOCH).map_or(0, nanos)
}

fn with_suffix(stem: &Path, suffix: &str) -> PathBuf {
    let mut s = stem.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

#[derive(Debug, Default)]
struct LearnerState {
    open: bool,
    next: u32,
    skipped: usize,
    done: Vec<LearnedRec>,
    queue: Option<mpsc::Sender<(u32, Bytes)>>,
}

/// Keeps the chunks one run uploads as themselves. A background writer puts their bytes in the
/// run's chunk file. A record counts only once the upload that carried it is published.
#[derive(Debug)]
pub(crate) struct Learner {
    fs: Arc<dyn LocalFs>,
    clock: Arc<dyn Clock>,
    stem: PathBuf,
    state: Mutex<LearnerState>,
    writer: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    /// Slots below this are on disk: the writer goes in slot order.
    written: Arc<AtomicU32>,
}

impl Learner {
    pub(crate) async fn new(
        fs: Arc<dyn LocalFs>,
        clock: Arc<dyn Clock>,
        dir: &Path,
        name: &str,
    ) -> Result<Learner> {
        fs.create_dir_all(dir).await?;
        let stem = dir.join(format!("{}-{name}", unix_nanos(clock.wall())));
        let f = fs
            .open(&with_suffix(&stem, ".chunks.tmp"), Open::CreateNew)
            .await?;
        let (tx, mut rx) = mpsc::channel::<(u32, Bytes)>(QUEUE);
        let written = Arc::new(AtomicU32::new(0));
        let w = Arc::clone(&written);
        let writer = tokio::spawn(async move {
            while let Some((slot, b)) = rx.recv().await {
                if f.write_at(vec![b], u64::from(slot) * CHUNK).await.is_err() {
                    rx.close();
                    return;
                }
                w.store(slot + 1, Ordering::Release);
            }
        });
        Ok(Learner {
            fs,
            clock,
            stem,
            state: Mutex::new(LearnerState {
                open: true,
                queue: Some(tx),
                ..LearnerState::default()
            }),
            writer: tokio::sync::Mutex::new(Some(writer)),
            written,
        })
    }

    fn chunks_tmp(&self) -> PathBuf {
        with_suffix(&self.stem, ".chunks.tmp")
    }

    /// Queues full chunk `h`, uploaded as itself at `loc`. The record counts once its upload is
    /// published.
    pub(crate) fn keep(&self, h: Hash, loc: Loc, b: &Bytes, keys: &BlockKeys) -> Option<LearnedRec> {
        let mut s = self.state.lock();
        if !s.open || s.next as usize >= MAX_CHUNKS || b.len() != CHUNK_SIZE {
            return None;
        }
        let slot = s.next;
        if s.queue.as_ref()?.try_send((slot, b.clone())).is_err() {
            s.skipped += 1;
            return None;
        }
        s.next += 1;
        let mut r = LearnedRec {
            entry: Entry { hash: h, loc },
            slot,
            mask: 0,
            keys: [BlockKey::default(); BLOCKS_PER_CHUNK],
            used: 0,
        };
        for (blk, part) in b.as_chunks::<BLOCK_SIZE>().0.iter().enumerate() {
            if !is_zero(part) {
                r.mask |= 1 << blk;
                r.keys[blk] = keys.0[blk];
            }
        }
        Some(r)
    }

    /// Chunks not learned because the writer was behind.
    pub(crate) fn skipped(&self) -> usize {
        self.state.lock().skipped
    }

    /// Counts the records of an upload whose ref moved.
    pub(crate) fn published(&self, rs: &[LearnedRec]) {
        self.state.lock().done.extend_from_slice(rs);
    }

    async fn stop(&self) {
        drop(self.state.lock().queue.take());
        let mut writer = self.writer.lock().await;
        if let Some(w) = writer.take() {
            let _ = w.await;
        }
    }

    /// Makes this run's published chunks available to later runs on the host: the chunk file
    /// first, then the record file, each by rename, so a reader never sees a partial file.
    pub(crate) async fn save(&self) -> Result<()> {
        self.stop().await;
        let done = {
            let mut s = self.state.lock();
            if !std::mem::take(&mut s.open) {
                return Ok(());
            }
            std::mem::take(&mut s.done)
        };
        if done.is_empty() {
            return Ok(self.fs.remove(&self.chunks_tmp()).await?);
        }
        let written = self.written.load(Ordering::Acquire);
        let now = unix_nanos(self.clock.wall());
        let rs: Vec<LearnedRec> = done
            .into_iter()
            .filter(|r| r.slot < written)
            .map(|r| LearnedRec { used: now, ..r })
            .collect();
        self.fs
            .rename(&self.chunks_tmp(), &with_suffix(&self.stem, ".chunks"))
            .await?;
        Ok(write_atomic(self.fs.as_ref(), &with_suffix(&self.stem, ".lrn"), encode(&rs)?).await?)
    }

    /// Drops what this run kept, after an upload that never published.
    pub(crate) async fn discard(&self) {
        self.stop().await;
        if std::mem::take(&mut self.state.lock().open) {
            let _ = self.fs.remove(&self.chunks_tmp()).await;
        }
    }
}

const UNCHECKED: u8 = 0;
const GOOD: u8 = 1;
const BAD: u8 = 2;

#[derive(Debug, Default)]
struct Index {
    map: HashMap<BlockKey, BlockAt>,
    slot_of: HashMap<Hash, u32>,
    sources: Vec<Entry>,
    file_of: Vec<usize>,
    local: Vec<u32>,
    checked: Vec<AtomicU8>,
    files: Vec<Arc<dyn LocalFile>>,
    stems: Vec<PathBuf>,
    used_at: BTreeMap<u32, i64>,
    /// The file of this run's own chunks, once a published upload added some; its slots below
    /// `own_written` are on disk.
    own: Option<usize>,
    own_written: Option<Arc<AtomicU32>>,
}

impl Index {
    fn add(&mut self, file: usize, r: &LearnedRec) {
        let Ok(slot) = u32::try_from(self.sources.len()) else {
            return;
        };
        self.slot_of.entry(r.entry.hash).or_insert(slot);
        self.sources.push(r.entry);
        self.file_of.push(file);
        self.local.push(r.slot);
        self.checked.push(AtomicU8::new(UNCHECKED));
        for (blk, k) in r.keys.iter().enumerate() {
            if r.mask & (1 << blk) != 0
                && let Some(at) = BlockAt::new(u64::from(slot), blk)
            {
                self.map.entry(*k).or_insert(at);
            }
        }
    }

    fn at(&self, slot: u32) -> (Arc<dyn LocalFile>, u64) {
        let s = slot as usize;
        (
            Arc::clone(&self.files[self.file_of[s]]),
            u64::from(self.local[s]) * CHUNK,
        )
    }
}

/// Maps 4 KiB blocks to the learned chunks that hold them: those of earlier runs on this host,
/// and those this run already published. Slots count over all files.
#[derive(Debug)]
pub(crate) struct LearnedIndex {
    fs: Arc<dyn LocalFs>,
    cpu: Arc<dyn Cpu>,
    clock: Arc<dyn Clock>,
    dir: PathBuf,
    index: RwLock<Index>,
}

impl LearnedIndex {
    /// Loads the learned chunks in `dir` that were published within [`MAX_AGE`], the most
    /// recently published first, up to [`MAX_CHUNKS`]. It removes the file pairs that keep no
    /// chunk, record files of another format, and chunk files of runs that never saved.
    pub(crate) async fn load(
        fs: Arc<dyn LocalFs>,
        cpu: Arc<dyn Cpu>,
        clock: Arc<dyn Clock>,
        dir: &Path,
    ) -> Result<LearnedIndex> {
        let now = clock.wall();
        let entries = match fs.list(dir).await {
            Ok(es) => es,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        let mut stems = Vec::new();
        for de in &entries {
            let p = dir.join(&de.name);
            if Path::new(&de.name)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("tmp"))
            {
                if now.duration_since(de.modified).is_ok_and(|age| age > MAX_AGE) {
                    let _ = fs.remove(&p).await;
                }
            } else if let Some(stem) = de.name.strip_suffix(".lrn") {
                stems.push(dir.join(stem));
            }
        }
        stems.sort_unstable_by(|a, b| b.cmp(a));
        let now_ns = unix_nanos(now);
        let max_age = nanos(MAX_AGE);
        let mut all: Vec<(usize, LearnedRec)> = Vec::new();
        for (i, stem) in stems.iter().enumerate() {
            let Some(b) = read_file(fs.as_ref(), &with_suffix(stem, ".lrn")).await? else {
                continue;
            };
            let Some(rs) = decode(&b) else {
                let _ = fs.remove(&with_suffix(stem, ".lrn")).await;
                let _ = fs.remove(&with_suffix(stem, ".chunks")).await;
                continue;
            };
            let fresh = rs
                .into_iter()
                .filter(|r| now_ns.saturating_sub(r.used) <= max_age);
            all.extend(fresh.map(|r| (i, r)));
        }
        all.sort_by_key(|(_, r)| std::cmp::Reverse(r.used));
        all.truncate(MAX_CHUNKS);
        let mut file_idx: HashMap<usize, usize> = HashMap::new();
        let mut ix = Index::default();
        for (i, stem) in stems.iter().enumerate() {
            if !all.iter().any(|(f, _)| *f == i) {
                let _ = fs.remove(&with_suffix(stem, ".lrn")).await;
                let _ = fs.remove(&with_suffix(stem, ".chunks")).await;
                continue;
            }
            let Ok(f) = fs.open(&with_suffix(stem, ".chunks"), Open::Read).await else {
                continue;
            };
            file_idx.insert(i, ix.files.len());
            ix.files.push(f);
            ix.stems.push(stem.clone());
        }
        for (i, r) in &all {
            if let Some(&f) = file_idx.get(i) {
                ix.add(f, r);
            }
        }
        Ok(LearnedIndex {
            fs,
            cpu,
            clock,
            dir: dir.to_owned(),
            index: RwLock::new(ix),
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.index.read().sources.len()
    }

    pub(crate) fn lookup(&self, k: BlockKey) -> Option<BlockAt> {
        self.index.read().map.get(&k).copied()
    }

    /// Makes chunks this run published sources for its later uploads.
    pub(crate) async fn add_published(&self, l: &Learner, rs: &[LearnedRec]) {
        if rs.is_empty() {
            return;
        }
        let own = self.index.read().own;
        let opened = match own {
            Some(_) => None,
            None => match self.fs.open(&l.chunks_tmp(), Open::Read).await {
                Ok(f) => Some(f),
                Err(_) => return,
            },
        };
        let mut ix = self.index.write();
        let own = match (ix.own, opened) {
            (Some(o), _) => o,
            (None, Some(f)) => {
                let o = ix.files.len();
                ix.files.push(f);
                ix.stems.push(l.stem.clone());
                ix.own = Some(o);
                ix.own_written = Some(Arc::clone(&l.written));
                o
            }
            (None, None) => return,
        };
        for r in rs {
            ix.add(own, r);
        }
    }

    /// Adds the learned slots behind `es` to `out`, except this run's own.
    pub(crate) fn slots_of(&self, out: &mut Vec<u32>, es: &[Entry]) {
        let ix = self.index.read();
        for e in es {
            if let Some(&slot) = ix.slot_of.get(&e.hash)
                && ix.sources[slot as usize] == *e
                && Some(ix.file_of[slot as usize]) != ix.own
            {
                out.push(slot);
            }
        }
    }

    /// Notes that a published upload used `slots`.
    pub(crate) fn mark_used(&self, slots: &[u32]) {
        if slots.is_empty() {
            return;
        }
        let now = unix_nanos(self.clock.wall());
        let mut ix = self.index.write();
        for s in slots {
            ix.used_at.insert(*s, now);
        }
    }

    /// Writes the times of this run's uses into the record files of earlier runs. Returns how
    /// many records moved.
    pub(crate) async fn save_uses(&self) -> Result<usize> {
        let by_file = {
            let mut ix = self.index.write();
            let uses = std::mem::take(&mut ix.used_at);
            let mut by_file: BTreeMap<usize, HashMap<u32, i64>> = BTreeMap::new();
            for (slot, t) in uses {
                let s = slot as usize;
                by_file.entry(ix.file_of[s]).or_default().insert(ix.local[s], t);
            }
            by_file
                .into_iter()
                .map(|(f, m)| (ix.stems[f].clone(), m))
                .collect::<Vec<_>>()
        };
        if by_file.is_empty() {
            return Ok(0);
        }
        let _lock = self.fs.lock(&self.dir.join(".lock")).await?;
        let mut n = 0;
        for (stem, m) in by_file {
            let path = with_suffix(&stem, ".lrn");
            let Some(b) = read_file(self.fs.as_ref(), &path).await? else {
                continue;
            };
            let Some(mut rs) = decode(&b) else { continue };
            for r in &mut rs {
                if let Some(&t) = m.get(&r.slot)
                    && t > r.used
                {
                    r.used = t;
                    n += 1;
                }
            }
            write_atomic(self.fs.as_ref(), &path, encode(&rs)?).await?;
        }
        Ok(n)
    }

    /// The entry of learned chunk `slot` once its bytes on this host match its hash.
    pub(crate) async fn source(&self, slot: u32) -> Option<Entry> {
        let (file, off, entry) = {
            let ix = self.index.read();
            let s = slot as usize;
            if Some(ix.file_of[s]) == ix.own
                && ix.local[s] >= ix.own_written.as_ref().map_or(0, |w| w.load(Ordering::Acquire))
            {
                return None;
            }
            match ix.checked[s].load(Ordering::Acquire) {
                GOOD => return Some(ix.sources[s]),
                BAD => return None,
                _ => {}
            }
            let (f, off) = ix.at(slot);
            (f, off, ix.sources[s])
        };
        let good = match file.read_at(CHUNK_SIZE, off).await {
            Ok(b) if b.len() == CHUNK_SIZE => {
                on_cpu(self.cpu.as_ref(), CpuClass::Background, move || {
                    Hash::of(&b) == entry.hash
                })
                .await
            }
            _ => false,
        };
        let ix = self.index.read();
        ix.checked[slot as usize].store(if good { GOOD } else { BAD }, Ordering::Release);
        good.then_some(entry)
    }

    /// `n` blocks of learned chunk `slot` from block `blk` on.
    pub(crate) async fn read(&self, slot: u32, blk: usize, n: usize) -> Result<Bytes> {
        let (f, off) = self.index.read().at(slot);
        let b = f
            .read_at(n * BLOCK_SIZE, off + u64_from(blk * BLOCK_SIZE))
            .await?;
        if b.len() != n * BLOCK_SIZE {
            return Err(Error::BeyondEnd);
        }
        Ok(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip() {
        let r = LearnedRec {
            entry: Entry {
                hash: Hash::of(b"x"),
                ..Entry::EMPTY
            },
            slot: 3,
            mask: 0b101,
            keys: [BlockKey(9); BLOCKS_PER_CHUNK],
            used: 1_700_000_000_000_000_000,
        };
        assert_eq!(decode(&encode(&[r, r]).unwrap()).unwrap(), vec![r, r]);
        assert!(decode(&encode(&[r]).unwrap()[..20]).is_none());
        assert!(decode(b"lrn1\0\0\0\0").is_none());
    }
}
