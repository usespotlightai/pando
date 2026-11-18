//! A volume: one writable branch, with write-back to the store.
//!
//! A flush commits locally: chunks and nodes go to the cache, one sync, then the local root is
//! recorded. A shipper then puts the flush's chunks in the branch's write log in the store,
//! and an uploader packs pending objects into segments, uploads them and moves the branch's ref
//! with a conditional update. A lost host loses only what neither has in the store yet; with
//! [`LogMode::Strict`], a flush returns only once the log or an upload has it.

use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use pando_format::{
    BLOCK_SIZE, BlockKeys, CHUNK_SIZE, Entry, Hash, Loc, Ref, Writer, WriterId, depth_for, is_zero, u64_from,
    usize_from,
};
use tokio::sync::{Mutex as AsyncMutex, Notify, RwLock, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::base::{BaseView, Bases};
use crate::cache::Cache;
use crate::dedup::learned::Learner;
use crate::dedup::{Dedup, PreparedChunks};
use crate::flight::bounded;
use crate::gate::Gate;
use crate::local::{Journal, Record, read_file, write_durable};
use crate::packer::{PackStats, Packer, SEG_MAX};
use crate::ports::{Cpu, CpuClass, FileLock, LocalFs, Version, on_cpu};
use crate::remote::Remote;
use crate::tree::Tree;
use crate::{Error, Ports, Result};

mod image;
mod log;

pub use log::LogMode;

const STRIPES: usize = 64;
const CHUNK: u64 = u64_from(CHUNK_SIZE);
const CHUNK_LEN: u32 = 64 << 10;
const _: () = assert!(CHUNK_LEN as usize == CHUNK_SIZE);
/// The uploader looks for new commits at least this often.
const UPLOAD_EVERY: Duration = Duration::from_secs(2);
/// After a failed upload the uploader waits this long before the next try.
const RETRY_AFTER: Duration = Duration::from_secs(1);
/// How often an open tries to take the writer generation when the store fails transiently.
const GENERATION_TRIES: usize = 10;
/// Chunks a flush hashes and stores at once.
const STORE_PARALLEL: usize = 16;
/// Segments a change list walk fetches nodes from at once.
const LIST_PARALLEL: usize = 16;
static ZEROS: [u8; CHUNK_SIZE] = [0; CHUNK_SIZE];

/// A chunk written since the last flush.
#[derive(Debug)]
enum Dirty {
    Bytes(Vec<u8>),
    /// All zeros, held without a buffer, so clearing a large range costs no memory.
    Zero,
}

impl Dirty {
    fn of(b: &[u8]) -> Dirty {
        if is_zero(b) {
            Dirty::Zero
        } else {
            Dirty::Bytes(b.to_vec())
        }
    }

    fn bytes(self) -> Bytes {
        match self {
            Dirty::Bytes(v) => Bytes::from(v),
            Dirty::Zero => Bytes::from_static(&ZEROS),
        }
    }

    fn copy_to(&self, at: usize, dst: &mut [u8]) {
        match self {
            Dirty::Bytes(d) => dst.copy_from_slice(&d[at..at + dst.len()]),
            Dirty::Zero => dst.fill(0),
        }
    }
}

#[derive(Debug)]
struct Mem {
    root: Entry,
    /// The tree `root` is a root of, whose depth follows `size`.
    tree: Arc<Tree>,
    size: u64,
    /// The root the branch opened at, as a root of `tree`.
    opened_now: Entry,
    dirty: BTreeMap<u64, Dirty>,
    /// Every chunk written since the branch opened, so not the base image's any more.
    touched: HashSet<u64>,
    flushing: Option<Arc<BTreeMap<u64, Bytes>>>,
    jfloor: u64,
    /// Dirty chunks whose bytes are also in a file that stays until they are published.
    dirty_ref: BTreeMap<u64, ChunkRef>,
    /// The chunks written since tracking started, while a handoff copies the branch.
    track: Option<HashSet<u64>>,
    /// Flushes committed so far.
    flush_seq: u64,
    /// Per file, the last flush that recorded a chunk as a reference into it.
    ref_seq: HashMap<PathBuf, u64>,
}

impl Mem {
    /// Records that chunk `idx` is dirty now: not the base image's, and not a reference.
    fn written(&mut self, idx: u64) {
        self.touched.insert(idx);
        self.dirty_ref.remove(&idx);
        if let Some(t) = &mut self.track {
            t.insert(idx);
        }
    }
}

/// A chunk whose bytes are `CHUNK_SIZE` bytes at `offset` of the file at `path`.
#[derive(Clone, Debug)]
pub(crate) struct ChunkRef {
    pub(crate) path: PathBuf,
    pub(crate) offset: u64,
    pub(crate) hash: Hash,
}

#[derive(Debug)]
struct Up {
    version: Version,
    published: Entry,
    /// The size the store has for the branch, as this writer last published it.
    published_size: u64,
    uploaded: Entry,
    last_from: Hash,
    publishing: Entry,
    known: Arc<HashMap<Hash, Loc>>,
    fenced: bool,
    gen_taken: bool,
    error: Option<Arc<Error>>,
    last: Option<PackStats>,
    /// Upload passes that moved the ref since the branch opened, and the segment bytes they put.
    passes: u64,
    put_bytes: u64,
    last_times: Option<PassTimes>,
}

/// How long the parts of one upload pass took.
#[derive(Clone, Copy, Debug, Default)]
pub struct PassTimes {
    /// Packing the pending objects and putting their segments and the change list.
    pub pack: Duration,
    /// Recording the packed root on this host and moving the branch's ref.
    pub publish: Duration,
}

/// What [`Volume::checkpoint_report`] did.
#[derive(Clone, Copy, Debug, Default)]
pub struct CheckpointReport {
    /// Committing the completed writes on this host.
    pub flush: Duration,
    /// From the end of the flush until the store had everything.
    pub wait: Duration,
    /// Upload passes that moved the ref before the checkpoint started.
    pub passes_before: u64,
    /// The segment bytes those passes put.
    pub bytes_before: u64,
    /// Upload passes that moved the ref during the checkpoint.
    pub passes: u64,
    /// The segment bytes those passes put.
    pub bytes: u64,
    /// The last of those passes; `None` when there was none.
    pub last: Option<PassTimes>,
}

#[derive(Debug)]
struct Inner {
    name: String,
    /// The size reads and writes are checked against; a grow raises it once `Mem` has the
    /// new size.
    size: AtomicU64,
    generation: u64,
    from: Option<String>,
    remote: Remote,
    cache: Arc<Cache>,
    opened_tree: Arc<Tree>,
    opened_size: u64,
    fs: Arc<dyn LocalFs>,
    cpu: Arc<dyn Cpu>,
    writer: Arc<Writer>,
    journal: Journal,
    state_path: PathBuf,
    state_lock: Mutex<Option<Box<dyn FileLock>>>,
    opened: Entry,
    stripes: Vec<RwLock<()>>,
    mem: Mutex<Mem>,
    commit: AsyncMutex<()>,
    up: Mutex<Up>,
    changed: watch::Sender<u64>,
    kick: Notify,
    stop: watch::Sender<bool>,
    dedup: Mutex<Option<Arc<Dedup>>>,
    learner: Mutex<Option<Arc<Learner>>>,
    /// The block keys of the chunks in the latest change list that earlier uploads packed.
    chunk_keys: Mutex<HashMap<Hash, BlockKeys>>,
    base: Mutex<Option<Arc<BaseView>>>,
    prepared: Mutex<Option<Arc<PreparedChunks>>>,
    gate: Arc<Gate>,
    /// The last flush an upload published.
    uploaded_seq: watch::Sender<u64>,
    log: log::Log,
    /// The generation the local state names: the ref's when the branch opened, this writer's
    /// once it took the branch.
    state_generation: AtomicU64,
}

/// One writable branch.
#[derive(Debug)]
pub struct Volume {
    inner: Arc<Inner>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

/// The local state of a branch. `size` is the size `root` belongs to, and `generation` the
/// writer that wrote it; states written before sizes could grow, or before the write log, have
/// none.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct VolState {
    root: Entry,
    published: Entry,
    publishing: Entry,
    jfloor: u64,
    size: Option<u64>,
    generation: Option<u64>,
}

impl VolState {
    fn encode(&self) -> Bytes {
        let tail = match (self.size, self.generation) {
            (Some(n), Some(g)) => format!("{n}\n{g}\n"),
            (Some(n), None) => format!("{n}\n"),
            (None, _) => String::new(),
        };
        Bytes::from(format!(
            "{}\n{}\n{}\n{}\n{tail}",
            self.root, self.published, self.publishing, self.jfloor
        ))
    }

    fn decode(b: &[u8]) -> Option<VolState> {
        let s = std::str::from_utf8(b).ok()?;
        let lines: Vec<&str> = s.lines().collect();
        let (root, published, publishing, jfloor, size, generation) = match lines.as_slice() {
            [r, p, q, j] => (r, p, q, j, None, None),
            [r, p, q, j, n] => (r, p, q, j, Some(n.parse().ok()?), None),
            [r, p, q, j, n, g] => (r, p, q, j, Some(n.parse().ok()?), Some(g.parse().ok()?)),
            _ => return None,
        };
        Some(VolState {
            root: Entry::parse_text(root).ok()?,
            published: Entry::parse_text(published).ok()?,
            publishing: Entry::parse_text(publishing).ok()?,
            jfloor: jfloor.parse().ok()?,
            size,
            generation,
        })
    }

    /// Whether a writer may resume this state over ref `r`: the ref holds the root this state
    /// published or was publishing, and the last writer to take the branch is the one this
    /// state names.
    fn resumes(&self, r: &Ref) -> bool {
        (self.published.hash == r.root.hash || self.publishing.hash == r.root.hash)
            && self.generation.is_none_or(|g| g == r.generation)
    }
}

impl Volume {
    /// Creates branch `name` of `size` bytes, all zero.
    pub async fn create(ports: &Ports, name: &str, size: u64) -> Result<()> {
        let r = Ref {
            size,
            root: Entry::EMPTY,
            generation: 0,
            from: None,
            log: None,
        };
        Remote::new(Arc::clone(&ports.store))
            .create_ref(name, &r)
            .await
            .map(drop)
    }

    /// Opens branch `name` for writing and takes its next writer generation. Its local state
    /// lives under `dir`, next to the cache.
    pub async fn open(ports: &Ports, cache: Arc<Cache>, dir: &Path, name: &str) -> Result<Volume> {
        let v = Volume::open_then(ports, cache, dir, name).await?;
        if let Err(e) = v.wait_generation().await {
            v.close().await;
            return Err(e);
        }
        Ok(v)
    }

    /// [`Self::open`] that returns before the writer generation is taken: reads need no
    /// generation, and uploads wait for it.
    pub async fn open_then(ports: &Ports, cache: Arc<Cache>, dir: &Path, name: &str) -> Result<Volume> {
        Volume::open_logged(ports, cache, dir, name, LogMode::default()).await
    }

    /// [`Self::open_then`] that logs flushes as `mode` says. Writes in the log that the ref
    /// does not hold are applied before it returns, unless this host resumes its own state.
    pub async fn open_logged(
        ports: &Ports,
        cache: Arc<Cache>,
        dir: &Path,
        name: &str,
        mode: LogMode,
    ) -> Result<Volume> {
        let remote = cache.remote().clone();
        let (r, version) = remote
            .get_ref(name)
            .await?
            .ok_or_else(|| Error::Corrupt(format!("open {name}: no such branch")))?;
        let generation = r.generation + 1;
        let state_path = dir.join("state").join(name);
        let (state_lock, state) = open_state(ports.fs.as_ref(), &state_path).await?;
        let journal = Journal::open(Arc::clone(&ports.fs), &dir.join("journal").join(name)).await?;
        let resume = state.filter(|s| s.resumes(&r));
        let scan = match resume {
            Some(_) => log::Scan::resumed(&r),
            None => log::scan(&remote, &ports.cpu, name, &r).await?,
        };
        let size = resume.and_then(|s| s.size).map_or(r.size, |n| n.max(r.size));
        let tree = Arc::new(Tree::new(Arc::clone(&cache), size.div_ceil(CHUNK)));
        let root = resume.map_or(r.root, |s| s.root);
        let mem = Mem {
            root,
            tree: Arc::clone(&tree),
            size,
            opened_now: root,
            dirty: BTreeMap::new(),
            touched: HashSet::new(),
            flushing: None,
            jfloor: resume.map_or(0, |s| s.jfloor),
            dirty_ref: BTreeMap::new(),
            track: None,
            flush_seq: 0,
            ref_seq: HashMap::new(),
        };
        let inner = Arc::new(Inner {
            name: name.to_owned(),
            size: AtomicU64::new(size),
            generation,
            from: r.from.clone(),
            remote,
            opened_tree: tree,
            opened_size: size,
            cache,
            fs: Arc::clone(&ports.fs),
            cpu: Arc::clone(&ports.cpu),
            writer: Arc::new(Writer::new(WriterId::new(name, generation), ports.rng.next_u32())),
            journal,
            state_path,
            state_lock: Mutex::new(Some(state_lock)),
            opened: mem.root,
            stripes: (0..STRIPES).map(|_| RwLock::new(())).collect(),
            mem: Mutex::new(mem),
            commit: AsyncMutex::new(()),
            up: Mutex::new(Up {
                version: version.clone(),
                published: r.root,
                published_size: r.size,
                uploaded: r.root,
                last_from: Hash::ZERO,
                publishing: Entry::EMPTY,
                known: Arc::default(),
                fenced: false,
                gen_taken: false,
                error: None,
                last: None,
                passes: 0,
                put_bytes: 0,
                last_times: None,
            }),
            changed: watch::Sender::new(0),
            kick: Notify::new(),
            stop: watch::Sender::new(false),
            dedup: Mutex::default(),
            learner: Mutex::default(),
            chunk_keys: Mutex::default(),
            base: Mutex::default(),
            prepared: Mutex::default(),
            gate: Arc::default(),
            uploaded_seq: watch::Sender::new(0),
            log: log::Log::new(mode, scan.start),
            state_generation: AtomicU64::new(r.generation),
        });
        inner.start_journal(resume).await?;
        let grown = size != r.size;
        let taker = tokio::spawn(Arc::clone(&inner).take_generation(r, version));
        let uploader = tokio::spawn(Arc::clone(&inner).uploader());
        let shipper = tokio::spawn(Arc::clone(&inner).shipper());
        let v = Volume {
            inner,
            tasks: Mutex::new(vec![taker, uploader, shipper]),
        };
        if let Err(e) = v.inner.replay_log(scan.writes).await {
            v.close().await;
            return Err(e);
        }
        let uploaded = v.inner.up.lock().uploaded.hash;
        if v.inner.root().hash != uploaded || grown {
            v.inner.kick.notify_one();
        }
        Ok(v)
    }

    /// The name of the branch.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The branch this one was copied from.
    pub fn copied_from(&self) -> Option<&str> {
        self.inner.from.as_deref()
    }

    /// The tree of the current root; a grow past its depth replaces it.
    pub(crate) fn tree(&self) -> Arc<Tree> {
        Arc::clone(&self.inner.mem.lock().tree)
    }

    /// The current root and the tree it is a root of.
    pub(crate) fn tree_and_root(&self) -> (Arc<Tree>, Entry) {
        let m = self.inner.mem.lock();
        (Arc::clone(&m.tree), m.root)
    }

    /// The root the branch opened at.
    pub(crate) fn opened(&self) -> Entry {
        self.inner.opened
    }

    /// The tree of the root the branch opened at, which a grow leaves as it is.
    pub(crate) fn opened_tree(&self) -> &Arc<Tree> {
        &self.inner.opened_tree
    }

    /// The size the branch had when it opened.
    pub(crate) fn opened_size(&self) -> u64 {
        self.inner.opened_size
    }

    /// Lets later uploads send recipes of the blocks `dedup` finds.
    pub(crate) fn set_dedup(&self, dedup: Option<Arc<Dedup>>) {
        *self.inner.dedup.lock() = dedup;
    }

    pub(crate) fn dedup(&self) -> Option<Arc<Dedup>> {
        self.inner.dedup.lock().clone()
    }

    /// Keeps what later uploads send as themselves for later runs on this host.
    pub(crate) fn set_learner(&self, learner: Option<Arc<Learner>>) {
        *self.inner.learner.lock() = learner;
    }

    /// Reads unchanged chunks from the base image of the root the branch opened at, when this
    /// host has it. Returns whether it has.
    pub(crate) async fn use_base(&self, bases: &Bases) -> Result<bool> {
        let opened = self.inner.opened.hash;
        if !bases.has(&opened).await {
            return Ok(false);
        }
        let view = BaseView::open(self.inner.fs.as_ref(), &bases.path(&opened)).await?;
        *self.inner.base.lock() = Some(Arc::new(view));
        Ok(true)
    }

    /// Stops reading from the base image; returns the bytes reads took from it.
    pub(crate) fn drop_base(&self) -> u64 {
        self.inner.base.lock().take().map_or(0, |b| b.served())
    }

    /// The size of the branch in bytes; reads and writes must end at or below it.
    pub fn size(&self) -> u64 {
        self.inner.size()
    }

    /// Grows the branch to `size` bytes, a multiple of 4 KiB; the added bytes read as zero.
    /// Reads and writes up to `size` work once this returns, the new size is durable on this
    /// host, and the next upload publishes it. A size below the current one is refused.
    pub async fn grow(&self, size: u64) -> Result<()> {
        self.inner.grow(size).await
    }

    /// The current local root.
    pub fn root(&self) -> Entry {
        self.inner.root()
    }

    /// The root the store has for this branch, as this writer last published it.
    pub fn published(&self) -> Entry {
        self.inner.up.lock().published
    }

    /// Waits until the writer generation is taken; an error when another writer took it.
    pub async fn wait_generation(&self) -> Result<()> {
        let mut rx = self.inner.changed.subscribe();
        loop {
            {
                let up = self.inner.up.lock();
                if up.gen_taken {
                    return match (&up.error, up.fenced) {
                        (Some(e), true) => Err(Error::Shared(Arc::clone(e))),
                        _ => Ok(()),
                    };
                }
            }
            if rx.changed().await.is_err() {
                return Err(Error::Cancelled);
            }
        }
    }

    /// Fills `p` with the bytes at `off`: what was written, and zeros elsewhere.
    pub async fn read_at(&self, p: &mut [u8], off: u64) -> Result<()> {
        if beyond(off, u64_from(p.len()), self.inner.size()) {
            return Err(Error::BeyondEnd);
        }
        let mut misses = Vec::new();
        for (idx, at, pos, n) in parts(off, p.len()) {
            if !self.inner.try_read_chunk(idx, at, &mut p[pos..pos + n])? {
                misses.push((idx, at, pos, n));
            }
        }
        if misses.is_empty() {
            return Ok(());
        }
        let futs = misses
            .iter()
            .map(|&(idx, at, _, n)| self.inner.read_chunk(idx, at, n));
        let got = futures::future::join_all(futs).await;
        for ((_, _, pos, n), b) in misses.into_iter().zip(got) {
            p[pos..pos + n].copy_from_slice(&b?);
        }
        Ok(())
    }

    /// Reads only from memory and the page cache, without waiting: `false` when it would wait.
    pub fn try_read_at(&self, p: &mut [u8], off: u64) -> Result<bool> {
        if beyond(off, u64_from(p.len()), self.inner.size()) {
            return Err(Error::BeyondEnd);
        }
        for (idx, at, pos, n) in parts(off, p.len()) {
            if !self.inner.try_read_chunk(idx, at, &mut p[pos..pos + n])? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Writes `p` at `off`; reads see it once this returns. [`Self::flush`] makes it durable
    /// on this host.
    pub async fn write_at(&self, p: &[u8], off: u64) -> Result<()> {
        if beyond(off, u64_from(p.len()), self.inner.size()) {
            return Err(Error::BeyondEnd);
        }
        let writes = parts(off, p.len())
            .into_iter()
            .map(|(idx, at, pos, n)| self.inner.write_chunk(idx, at, &p[pos..pos + n], true));
        futures::future::join_all(writes).await.into_iter().collect()
    }

    /// Clears a range. Whole chunks that hold no data stay sparse.
    pub async fn zero(&self, off: u64, len: u64) -> Result<()> {
        if beyond(off, len, self.inner.size()) {
            return Err(Error::BeyondEnd);
        }
        let mut pos = off;
        while pos < off + len {
            let (idx, at) = (pos / CHUNK, usize_from(pos % CHUNK));
            let n = (CHUNK_SIZE - at).min(usize_from(off + len - pos));
            if n < CHUNK_SIZE {
                self.inner.write_chunk(idx, at, &ZEROS[..n], true).await?;
            } else if !self.inner.is_sparse(idx).await? {
                self.inner.zero_chunk(idx, true).await?;
            }
            pos += u64_from(n);
        }
        Ok(())
    }

    /// Commits completed writes locally and durably, then nudges the uploader.
    pub async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }

    /// Flushes and waits until everything is published in the store.
    pub async fn checkpoint(&self) -> Result<()> {
        self.checkpoint_report().await.map(|_| ())
    }

    /// [`Self::checkpoint`], reporting how long its parts took and what the uploads put.
    pub async fn checkpoint_report(&self) -> Result<CheckpointReport> {
        let _hurry = self.inner.gate.hurry();
        let t0 = Instant::now();
        let (passes_before, bytes_before) = {
            let up = self.inner.up.lock();
            (up.passes, up.put_bytes)
        };
        self.inner.flush().await?;
        let flushed = Instant::now();
        let (target, size) = {
            let m = self.inner.mem.lock();
            (m.root.hash, m.size)
        };
        let mut rx = self.inner.changed.subscribe();
        self.inner.kick.notify_one();
        loop {
            {
                let up = self.inner.up.lock();
                if (up.last_from == target || up.uploaded.hash == target) && up.published_size >= size {
                    let passes = up.passes - passes_before;
                    return Ok(CheckpointReport {
                        flush: flushed - t0,
                        wait: flushed.elapsed(),
                        passes_before,
                        bytes_before,
                        passes,
                        bytes: up.put_bytes - bytes_before,
                        last: up.last_times.filter(|_| passes > 0),
                    });
                }
                if let Some(e) = &up.error {
                    return Err(Error::Shared(Arc::clone(e)));
                }
            }
            if rx.changed().await.is_err() {
                return Err(Error::Cancelled);
            }
        }
    }

    /// What the last upload packed.
    pub fn last_upload(&self) -> Option<PackStats> {
        self.inner.up.lock().last
    }

    /// How this volume logs its flushes.
    pub fn log_mode(&self) -> LogMode {
        self.inner.log.mode()
    }

    /// Records in the ref that this writer adds no more records to the log, so the next open
    /// reads none. Call it after [`Self::checkpoint`], when no more writes come; without every
    /// flush published it does nothing.
    pub async fn close_log(&self) -> Result<()> {
        self.inner.close_log().await
    }

    /// Stops the uploader after any upload in flight finishes, then releases the local state
    /// for another open.
    pub async fn close(&self) {
        self.inner.stop.send_replace(true);
        let tasks = std::mem::take(&mut *self.tasks.lock());
        for t in tasks {
            let _ = t.await;
        }
        drop(self.inner.state_lock.lock().take());
    }
}

/// Locks the local state at `state_path`, so one process at a time writes the branch from it
/// and a writer that resumes the state knows the one before it is gone, then reads it.
async fn open_state(fs: &dyn LocalFs, state_path: &Path) -> Result<(Box<dyn FileLock>, Option<VolState>)> {
    if let Some(dir) = state_path.parent() {
        fs.create_dir_all(dir).await?;
    }
    let mut lock_path = state_path.as_os_str().to_owned();
    lock_path.push(".lock");
    let lock = fs.try_lock(Path::new(&lock_path)).await.map_err(|e| {
        Error::Io(std::io::Error::new(
            e.kind(),
            format!("the branch state {} is in use: {e}", state_path.display()),
        ))
    })?;
    let state = read_file(fs, state_path)
        .await?
        .and_then(|b| VolState::decode(&b));
    Ok((lock, state))
}

/// The chunk pieces of `len` bytes at `off`: chunk, offset in it, offset in the buffer, length.
fn parts(off: u64, len: usize) -> Vec<(u64, usize, usize, usize)> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < len {
        let at = off + u64_from(pos);
        let (idx, inner) = (at / CHUNK, usize_from(at % CHUNK));
        let n = (CHUNK_SIZE - inner).min(len - pos);
        out.push((idx, inner, pos, n));
        pos += n;
    }
    out
}

fn beyond(off: u64, len: u64, size: u64) -> bool {
    off.checked_add(len).is_none_or(|end| end > size)
}

fn is_transient(e: &Error) -> bool {
    matches!(e.kind(), Error::Store(crate::ports::StoreError::Transient { .. }))
}

fn fail_with(up: &mut Up, e: Error) -> Arc<Error> {
    let e = match e {
        Error::Shared(e) => e,
        e @ (Error::Fenced { .. }
        | Error::ChunkLost { .. }
        | Error::Store(_)
        | Error::Format(_)
        | Error::Io(_)
        | Error::BeyondEnd
        | Error::Shrink { .. }
        | Error::Corrupt(_)
        | Error::SegmentExists(_)
        | Error::RefExists(_)
        | Error::CheckpointExists(_)
        | Error::Cancelled) => Arc::new(e),
    };
    up.error = Some(Arc::clone(&e));
    e
}

impl Inner {
    /// Replays the journal above the floor of `resume`, or starts an empty one without it.
    async fn start_journal(&self, resume: Option<VolState>) -> Result<()> {
        let Some(s) = resume else {
            self.journal.reset().await?;
            self.journal.start(0).await?;
            return self.persist().await;
        };
        let mut writes = Vec::new();
        self.journal
            .replay(s.jfloor, |idx, r| {
                writes.push((idx, r));
                Ok(())
            })
            .await?;
        for (idx, r) in writes {
            match r {
                Record::Data(at, data) => self.write_chunk(idx, at, &data, false).await?,
                Record::ZeroChunk => self.zero_chunk(idx, false).await?,
            }
        }
        let next = self.journal.last().await?.max(s.jfloor) + 1;
        self.journal.start(next).await
    }

    fn root(&self) -> Entry {
        self.mem.lock().root
    }

    fn size(&self) -> u64 {
        self.size.load(Ordering::Acquire)
    }

    async fn grow(&self, size: u64) -> Result<()> {
        let _c = self.commit.lock().await;
        let (old, from) = {
            let m = self.mem.lock();
            (m.size, Arc::clone(&m.tree))
        };
        if size == old {
            if self.size() < old {
                self.persist().await?;
                self.size.store(old, Ordering::Release);
                self.kick.notify_one();
            }
            return Ok(());
        }
        if size < old {
            return Err(Error::Shrink { size: old, to: size });
        }
        if !size.is_multiple_of(BLOCK_SIZE as u64) {
            return Err(Error::Corrupt(format!(
                "size {size} is not a multiple of {BLOCK_SIZE}"
            )));
        }
        let depth = depth_for(size.div_ceil(CHUNK));
        let to = if depth == from.depth() {
            Arc::clone(&from)
        } else {
            Arc::new(from.at_depth(depth))
        };
        loop {
            let (root, opened) = {
                let m = self.mem.lock();
                (m.root, m.opened_now)
            };
            let lifted = to.lift(&root, from.depth()).await?;
            let opened = to.lift(&opened, from.depth()).await?;
            if depth != from.depth() {
                self.cache.sync_local().await?;
            }
            let mut m = self.mem.lock();
            if m.root.hash != root.hash {
                continue;
            }
            m.root = lifted;
            m.opened_now = opened;
            m.tree = Arc::clone(&to);
            m.size = size;
            break;
        }
        self.persist().await?;
        self.size.store(size, Ordering::Release);
        self.kick.notify_one();
        Ok(())
    }

    fn stripe(&self, idx: u64) -> &RwLock<()> {
        &self.stripes[usize_from(idx % u64_from(STRIPES))]
    }

    fn notify(&self) {
        self.changed.send_modify(|n| *n += 1);
    }

    async fn persist(&self) -> Result<()> {
        let (root, jfloor, size) = {
            let m = self.mem.lock();
            (m.root, m.jfloor, m.size)
        };
        let (published, publishing) = {
            let u = self.up.lock();
            (u.published, u.publishing)
        };
        let st = VolState {
            root,
            published,
            publishing,
            jfloor,
            size: Some(size),
            generation: Some(self.state_generation.load(Ordering::Acquire)),
        };
        Ok(write_durable(self.fs.as_ref(), &self.state_path, st.encode()).await?)
    }

    async fn take_generation(self: Arc<Self>, mut r: Ref, version: Version) {
        r.generation = self.generation;
        r.log = Some(self.log.start);
        let mut res = self.remote.put_ref(&self.name, &r, &version).await;
        for _ in 1..GENERATION_TRIES {
            if !matches!(&res, Err(e) if is_transient(e)) {
                break;
            }
            tokio::time::sleep(RETRY_AFTER).await;
            res = self.remote.put_ref(&self.name, &r, &version).await;
        }
        let taken = res.is_ok();
        {
            let mut up = self.up.lock();
            match res {
                Ok(v) => up.version = v,
                Err(e) => {
                    up.fenced = true;
                    fail_with(&mut up, e);
                }
            }
        }
        if taken {
            self.state_generation.store(self.generation, Ordering::Release);
            let _ = self.persist().await;
        }
        self.up.lock().gen_taken = true;
        self.notify();
    }

    async fn read_committed(
        &self,
        tree: &Tree,
        idx: u64,
        root: &Entry,
        at: usize,
        n: usize,
    ) -> Result<Bytes> {
        let e = tree.lookup(root, idx, false).await?;
        if e.is_empty() {
            return Ok(Bytes::from(vec![0; n]));
        }
        self.cache.read(&e, at, n).await
    }

    async fn read_chunk(&self, idx: u64, at: usize, n: usize) -> Result<Bytes> {
        let _g = self.stripe(idx).read().await;
        let (tree, root) = {
            let m = self.mem.lock();
            match m.dirty.get(&idx) {
                Some(Dirty::Bytes(d)) => return Ok(Bytes::copy_from_slice(&d[at..at + n])),
                Some(Dirty::Zero) => return Ok(Bytes::from_static(&ZEROS[at..at + n])),
                None => {}
            }
            if let Some(f) = m.flushing.as_ref().and_then(|f| f.get(&idx)) {
                return Ok(f.slice(at..at + n));
            }
            (Arc::clone(&m.tree), m.root)
        };
        if let Some(b) = self.read_base(idx, at, n).await {
            return Ok(b);
        }
        self.read_committed(&tree, idx, &root, at, n).await
    }

    /// The base image's bytes of chunk `idx`, when no write since open changed it.
    fn base_of(&self, idx: u64) -> Option<Arc<BaseView>> {
        if self.mem.lock().touched.contains(&idx) {
            return None;
        }
        self.base.lock().clone()
    }

    async fn read_base(&self, idx: u64, at: usize, n: usize) -> Option<Bytes> {
        self.base_of(idx)?.read(idx * CHUNK + at as u64, n).await
    }

    fn try_read_chunk(&self, idx: u64, at: usize, dst: &mut [u8]) -> Result<bool> {
        let Ok(_g) = self.stripe(idx).try_read() else {
            return Ok(false);
        };
        let (tree, root) = {
            let m = self.mem.lock();
            if let Some(d) = m.dirty.get(&idx) {
                d.copy_to(at, dst);
                return Ok(true);
            }
            if let Some(f) = m.flushing.as_ref().and_then(|f| f.get(&idx)) {
                dst.copy_from_slice(&f[at..at + dst.len()]);
                return Ok(true);
            }
            (Arc::clone(&m.tree), m.root)
        };
        self.try_read_stored(&tree, idx, &root, at, dst)
    }

    /// [`Self::read_base`], then [`Self::read_committed`], from memory and the page cache only.
    fn try_read_stored(
        &self,
        tree: &Tree,
        idx: u64,
        root: &Entry,
        at: usize,
        dst: &mut [u8],
    ) -> Result<bool> {
        if let Some(base) = self.base_of(idx)
            && base.read_nowait(idx * CHUNK + at as u64, dst)
        {
            return Ok(true);
        }
        let Some(e) = tree.lookup_nowait(root, idx) else {
            return Ok(false);
        };
        if e.is_empty() {
            dst.fill(0);
            return Ok(true);
        }
        self.cache.read_nowait(&e, at, dst)
    }

    async fn is_sparse(&self, idx: u64) -> Result<bool> {
        let (tree, root) = {
            let m = self.mem.lock();
            if let Some(d) = m.dirty.get(&idx) {
                return Ok(matches!(d, Dirty::Zero));
            }
            if m.flushing.as_ref().is_some_and(|f| f.contains_key(&idx)) {
                return Ok(false);
            }
            (Arc::clone(&m.tree), m.root)
        };
        Ok(tree.lookup(&root, idx, false).await?.is_empty())
    }

    /// Applies one write to a chunk. The chunk's stripe lock orders writes to the same chunk,
    /// in the buffer and in the journal; writes to other chunks run in parallel. A flush takes
    /// every stripe, so no write is half applied when it takes the dirty set.
    async fn write_chunk(&self, idx: u64, at: usize, data: &[u8], journaled: bool) -> Result<()> {
        let _g = self.stripe(idx).write().await;
        let (dirty, flushing, tree, root, size) = {
            let m = self.mem.lock();
            let f = m.flushing.as_ref().and_then(|f| f.get(&idx)).cloned();
            let zero = m.dirty.get(&idx).map(|d| matches!(d, Dirty::Zero));
            (zero, f, Arc::clone(&m.tree), m.root, m.size)
        };
        let fresh = if dirty == Some(false) {
            None
        } else if dirty == Some(true) {
            Some(vec![0; CHUNK_SIZE])
        } else if let Some(f) = flushing {
            Some(f.to_vec())
        } else if data.len() < CHUNK_SIZE {
            let n = usize_from(CHUNK.min(size - idx * CHUNK));
            let mut v = vec![0; CHUNK_SIZE];
            if !self.try_read_stored(&tree, idx, &root, 0, &mut v[..n])? {
                let b = match self.read_base(idx, 0, n).await {
                    Some(b) => b,
                    None => self.read_committed(&tree, idx, &root, 0, n).await?,
                };
                v[..b.len()].copy_from_slice(&b);
            }
            Some(v)
        } else {
            Some(vec![0; CHUNK_SIZE])
        };
        if journaled {
            self.journal.append(idx, at, Bytes::copy_from_slice(data)).await?;
        }
        let mut m = self.mem.lock();
        m.written(idx);
        match fresh {
            None => {
                let Some(Dirty::Bytes(d)) = m.dirty.get_mut(&idx) else {
                    return Err(Error::Corrupt(format!(
                        "chunk {idx} left the dirty set under its stripe lock"
                    )));
                };
                d[at..at + data.len()].copy_from_slice(data);
            }
            Some(mut buf) => {
                buf[at..at + data.len()].copy_from_slice(data);
                m.dirty.insert(idx, Dirty::Bytes(buf));
            }
        }
        Ok(())
    }

    /// Makes chunk `idx` all zeros, as [`Self::write_chunk`] would with a chunk of zeros.
    async fn zero_chunk(&self, idx: u64, journaled: bool) -> Result<()> {
        let _g = self.stripe(idx).write().await;
        if journaled {
            self.journal.append_zero_chunk(idx).await?;
        }
        let mut m = self.mem.lock();
        m.written(idx);
        m.dirty.insert(idx, Dirty::Zero);
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let seq = self.commit_flush().await?;
        if self.log.mode() == LogMode::Strict {
            self.wait_logged(seq).await?;
        }
        Ok(())
    }

    /// Commits completed writes locally and durably; returns the number of the last flush.
    async fn commit_flush(&self) -> Result<u64> {
        let _c = self.commit.lock().await;
        if self.up.lock().fenced {
            return Err(Error::Fenced {
                branch: self.name.clone(),
            });
        }
        let mut guards = Vec::with_capacity(STRIPES);
        for s in &self.stripes {
            guards.push(s.write().await);
        }
        {
            let m = self.mem.lock();
            if m.dirty.is_empty() {
                return Ok(m.flush_seq);
            }
        }
        let generation = self.journal.rotate().await?;
        let (snapshot, refs, tree, root) = {
            let mut m = self.mem.lock();
            let snap: BTreeMap<u64, Bytes> = std::mem::take(&mut m.dirty)
                .into_iter()
                .map(|(i, d)| (i, d.bytes()))
                .collect();
            let snap = Arc::new(snap);
            m.flushing = Some(Arc::clone(&snap));
            (
                snap,
                std::mem::take(&mut m.dirty_ref),
                Arc::clone(&m.tree),
                m.root,
            )
        };
        drop(guards);
        match self.commit_snapshot(&tree, &snapshot, &refs, &root).await {
            Ok((new_root, updates)) => {
                let (seq, size) = {
                    let mut m = self.mem.lock();
                    m.root = new_root;
                    m.flushing = None;
                    m.jfloor = generation;
                    m.flush_seq += 1;
                    let seq = m.flush_seq;
                    for r in refs.into_values() {
                        m.ref_seq.insert(r.path, seq);
                    }
                    (seq, m.size)
                };
                self.persist().await?;
                self.journal.drop_below(generation).await;
                let chunks = updates
                    .into_iter()
                    .map(|(idx, e)| (idx, (!e.is_empty()).then(|| (e.hash, snapshot[&idx].clone()))))
                    .collect();
                self.queue_flush(log::Flushed { seq, size, chunks });
                self.kick.notify_one();
                Ok(seq)
            }
            Err(e) => {
                let mut m = self.mem.lock();
                m.flushing = None;
                for (idx, b) in snapshot.iter() {
                    if !m.dirty.contains_key(idx) {
                        m.dirty.insert(*idx, Dirty::of(b));
                        if let Some(r) = refs.get(idx) {
                            m.dirty_ref.insert(*idx, r.clone());
                        }
                    }
                }
                Err(e)
            }
        }
    }

    async fn commit_snapshot(
        &self,
        tree: &Tree,
        snapshot: &BTreeMap<u64, Bytes>,
        refs: &BTreeMap<u64, ChunkRef>,
        root: &Entry,
    ) -> Result<(Entry, Vec<(u64, Entry)>)> {
        let mut futs = Vec::with_capacity(snapshot.len());
        for (idx, b) in snapshot {
            futs.push(self.store_chunk(*idx, b.clone(), refs.get(idx)));
        }
        let mut updates = bounded(futs, STORE_PARALLEL)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        {
            let up = self.up.lock();
            for (_, e) in &mut updates {
                if let Some(l) = up.known.get(&e.hash) {
                    e.loc = *l;
                }
            }
        }
        updates.sort_by_key(|u| u.0);
        let new_root = tree.update(root, &updates).await?;
        self.cache.sync_local().await?;
        Ok((new_root, updates))
    }

    async fn store_chunk(&self, idx: u64, b: Bytes, r: Option<&ChunkRef>) -> Result<(u64, Entry)> {
        if let Some(r) = r {
            self.cache.put_ref(r.hash, &r.path, r.offset, CHUNK_LEN).await?;
            return Ok((
                idx,
                Entry {
                    hash: r.hash,
                    ..Entry::EMPTY
                },
            ));
        }
        let hashed = b.clone();
        let h = on_cpu(self.cpu.as_ref(), CpuClass::Foreground, move || {
            (!is_zero(&hashed)).then(|| Hash::of(&hashed))
        })
        .await;
        let Some(h) = h else {
            return Ok((idx, Entry::EMPTY));
        };
        self.cache.put_local(h, b).await?;
        Ok((
            idx,
            Entry {
                hash: h,
                ..Entry::EMPTY
            },
        ))
    }

    async fn uploader(self: Arc<Self>) {
        let mut stop = self.stop.subscribe();
        let mut changed = self.changed.subscribe();
        loop {
            let (taken, fenced) = {
                let up = self.up.lock();
                (up.gen_taken, up.fenced)
            };
            if fenced {
                return;
            }
            if taken {
                break;
            }
            tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                _ = changed.changed() => {}
            }
        }
        loop {
            tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                () = self.kick.notified() => {}
                () = tokio::time::sleep(UPLOAD_EVERY) => {}
            }
            if let Err(e) = self.upload().await {
                if matches!(e.kind(), Error::Fenced { .. } | Error::ChunkLost { .. }) {
                    return;
                }
                tokio::select! {
                    _ = stop.wait_for(|s| *s) => return,
                    () = tokio::time::sleep(RETRY_AFTER) => {}
                }
            }
        }
    }

    /// Packs everything pending under the current local root and publishes it.
    async fn upload(&self) -> Result<()> {
        let (root, seq, tree, size) = {
            let m = self.mem.lock();
            (m.root, m.flush_seq, Arc::clone(&m.tree), m.size)
        };
        let (known, version) = {
            let up = self.up.lock();
            if (root.hash == up.uploaded.hash && size == up.published_size) || up.fenced {
                return Ok(());
            }
            (Arc::clone(&up.known), up.version.clone())
        };
        let t0 = Instant::now();
        let dedup = self.dedup.lock().clone();
        let learner = self.learner.lock().clone();
        let prepared = self.prepared.lock().clone();
        let mut p = Packer::new(
            self.remote.clone(),
            Arc::clone(&self.cpu),
            Arc::clone(&self.writer),
            SEG_MAX,
            known,
        )
        .with_dedup(dedup.clone(), prepared.clone())
        .with_gate(Arc::clone(&self.gate))
        .with_learner(learner.clone());
        let packed = match tree.pack(&root, &mut p).await {
            Ok(e) => e,
            Err(e) => {
                if matches!(e.kind(), Error::ChunkLost { .. }) {
                    let e = fail_with(&mut self.up.lock(), e);
                    self.notify();
                    return Err(Error::Shared(e));
                }
                return Err(e);
            }
        };
        let keys = p.keys().clone();
        let (done, ()) = futures::join!(p.finish(), self.put_change_list(&tree, size, &packed, &keys));
        done?;
        let packed_at = Instant::now();
        self.up.lock().publishing = packed;
        self.persist().await?;
        let pos = self.log_pos(seq);
        let r = Ref {
            size,
            root: packed,
            generation: self.generation,
            from: self.from.clone(),
            log: Some(pos),
        };
        let new_version = match self.remote.put_ref(&self.name, &r, &version).await {
            Ok(v) => v,
            Err(e) => {
                if matches!(e.kind(), Error::Fenced { .. }) {
                    let mut up = self.up.lock();
                    up.fenced = true;
                    let e = fail_with(&mut up, e);
                    drop(up);
                    self.notify();
                    return Err(Error::Shared(e));
                }
                return Err(e);
            }
        };
        let stats = p.stats();
        let added = p.added().clone();
        record_learned(dedup.as_ref(), learner.as_ref(), &p).await;
        drop(p);
        {
            let mut up = self.up.lock();
            Arc::make_mut(&mut up.known).extend(added);
            up.version = new_version;
            up.published = packed;
            up.published_size = size;
            up.publishing = Entry::EMPTY;
            up.uploaded = root;
            up.last_from = root.hash;
            up.last = Some(stats);
            up.passes += 1;
            up.put_bytes += stats.bytes;
            up.last_times = Some(PassTimes {
                pack: packed_at - t0,
                publish: packed_at.elapsed(),
            });
            let mut m = self.mem.lock();
            if m.root.hash == root.hash {
                m.root = packed;
                up.uploaded = packed;
            }
        }
        if let Some(p) = &prepared {
            p.drop_through(seq);
        }
        self.log_published(&pos);
        self.uploaded_seq.send_modify(|s| *s = (*s).max(seq));
        self.notify();
        let _ = self.persist().await;
        Ok(())
    }

    /// Puts the change list of `root`, a root of `tree` for a branch of `size` bytes, from the
    /// root the branch opened at, while the segments upload. Only a branch of the size it
    /// opened at has one. Best effort: without it, another host walks the tree.
    async fn put_change_list(&self, tree: &Tree, size: u64, root: &Entry, keys: &HashMap<Hash, BlockKeys>) {
        let from = self.opened;
        if from.is_empty() || from.loc.is_pending() || from.hash == root.hash || size != self.opened_size {
            return;
        }
        let earlier = self.chunk_keys.lock().clone();
        let key_of = |h: &Hash| keys.get(h).or_else(|| earlier.get(h)).copied();
        let Ok(cl) = tree.change_list(&from, root, LIST_PARALLEL, key_of).await else {
            return;
        };
        let kept = cl
            .changes
            .iter()
            .filter_map(|c| Some((c.entry.hash, key_of(&c.entry.hash)?)))
            .collect();
        *self.chunk_keys.lock() = kept;
        let _ = self
            .remote
            .put_change_list(root.hash, from.hash, Bytes::from(cl.encode()))
            .await;
    }
}

async fn record_learned(dedup: Option<&Arc<Dedup>>, learner: Option<&Arc<Learner>>, p: &Packer) {
    if let Some(learned) = dedup.and_then(|d| d.learned.as_ref()) {
        learned.mark_used(p.used_learned());
        if let Some(l) = learner {
            l.published(p.learned());
            learned.add_published(l, p.learned()).await;
        }
    } else if let Some(l) = learner {
        l.published(p.learned());
    }
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use super::*;
    use crate::ports::Open;
    use crate::sim::{self, SimFs, SimStore};

    const SIZE: u64 = 64 << 20;

    async fn host(ports: &Ports, dir: &str) -> Arc<Cache> {
        Arc::new(Cache::open(ports, Path::new(dir)).await.unwrap())
    }

    async fn branch(ports: &Ports, name: &str) {
        Volume::create(ports, name, SIZE).await.unwrap();
    }

    fn data(seed: u8, n: usize) -> Vec<u8> {
        (0..=u8::MAX)
            .cycle()
            .take(n)
            .map(|i| i.wrapping_mul(31).wrapping_add(seed))
            .collect()
    }

    async fn read(v: &Volume, off: u64, n: usize) -> Vec<u8> {
        let mut b = vec![0; n];
        v.read_at(&mut b, off).await.unwrap();
        b
    }

    #[tokio::test(start_paused = true)]
    async fn a_checkpoint_publishes_what_another_host_reads() {
        let (ports, _, _) = sim::ports(1);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        let w = data(1, 200_000);
        v.write_at(&w, 30_000).await.unwrap();
        v.write_at(&data(2, 10), 100).await.unwrap();
        assert_eq!(read(&v, 30_000, w.len()).await, w);
        v.checkpoint().await.unwrap();
        assert!(!v.root().loc.is_pending());
        assert_eq!(v.published(), v.root());
        v.write_at(&data(3, 5), 64 << 10).await.unwrap();
        assert_eq!(read(&v, 64 << 10, 5).await, data(3, 5));
        v.close().await;

        let other = Volume::open(&ports, host(&ports, "/b").await, Path::new("/b"), "main")
            .await
            .unwrap();
        assert_eq!(
            read(&other, 30_000, w.len()).await[..(64 << 10) - 30_000],
            w[..(64 << 10) - 30_000]
        );
        assert_eq!(read(&other, 100, 10).await, data(2, 10));
        assert_eq!(
            read(&other, (64 << 10) + 5, 10).await,
            w[(64 << 10) - 30_000 + 5..][..10]
        );
        assert_eq!(read(&other, SIZE - 10, 10).await, vec![0; 10]);
        other.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_checkpoint_reports_the_passes_before_it_and_its_own() {
        let (ports, _, _) = sim::ports(3);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        v.write_at(&data(1, 300_000), 0).await.unwrap();
        let first = v.checkpoint_report().await.unwrap();
        assert_eq!((first.passes_before, first.bytes_before), (0, 0));
        assert!(first.passes >= 1 && first.bytes > 0);
        assert!(first.last.is_some());

        let again = v.checkpoint_report().await.unwrap();
        assert_eq!(again.passes, 0, "nothing new to publish");
        assert!(again.last.is_none());
        assert_eq!(again.bytes_before, first.bytes);

        v.write_at(&data(2, 100_000), 1 << 20).await.unwrap();
        v.flush().await.unwrap();
        v.checkpoint().await.unwrap();
        v.write_at(&data(3, 10), 0).await.unwrap();
        let third = v.checkpoint_report().await.unwrap();
        assert!(third.passes_before > first.passes);
        assert!(third.bytes_before > first.bytes);
        assert!(third.passes >= 1 && third.bytes > 0);
        v.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_restart_replays_journaled_writes_and_a_crash_keeps_flushed_ones() {
        let (ports, _, fs) = sim::ports(2);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        v.write_at(&data(1, 1000), 0).await.unwrap();
        v.flush().await.unwrap();
        v.write_at(&data(2, 1000), 500).await.unwrap();
        v.close().await;
        drop(v);

        let again = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        let mut want = data(1, 1000);
        want.extend_from_slice(&[0; 500]);
        want[500..].copy_from_slice(&data(2, 1000));
        assert_eq!(
            read(&again, 0, 1500).await,
            want,
            "the daemon restart replays the journal"
        );
        again.close().await;
        drop(again);

        crash(&fs);
        let after = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        let mut flushed = data(1, 1000);
        flushed.extend_from_slice(&[0; 500]);
        let got = read(&after, 0, 1500).await;
        assert_eq!(
            got, flushed,
            "a crash keeps the flushed writes and loses the journaled ones"
        );
        after.checkpoint().await.unwrap();
        after.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_cleared_range_reads_as_zeros_without_a_buffer_per_chunk() {
        let (ports, _, _) = sim::ports(5);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        let w = data(4, 4 * CHUNK_SIZE);
        v.write_at(&w, 0).await.unwrap();
        v.checkpoint().await.unwrap();

        v.zero(1000, 3 * CHUNK - 1000).await.unwrap();
        {
            let m = v.inner.mem.lock();
            assert!(matches!(m.dirty.get(&0), Some(Dirty::Bytes(_))));
            assert!(matches!(m.dirty.get(&1), Some(Dirty::Zero)));
            assert!(matches!(m.dirty.get(&2), Some(Dirty::Zero)));
            assert!(!m.dirty.contains_key(&3));
        }
        v.write_at(&data(7, 10), CHUNK + 50).await.unwrap();
        let mut want = w.clone();
        want[1000..3 * CHUNK_SIZE].fill(0);
        want[CHUNK_SIZE + 50..][..10].copy_from_slice(&data(7, 10));
        assert_eq!(read(&v, 0, want.len()).await, want);
        v.close().await;
        drop(v);

        let again = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        assert_eq!(
            read(&again, 0, want.len()).await,
            want,
            "the restart replays the cleared chunks"
        );
        again.checkpoint().await.unwrap();
        again.close().await;

        let other = Volume::open(&ports, host(&ports, "/b").await, Path::new("/b"), "main")
            .await
            .unwrap();
        assert_eq!(read(&other, 0, want.len()).await, want);
        other.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn writes_after_a_crash_survive_the_next_restart() {
        let (ports, _, fs) = sim::ports(4);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        for i in 0..3 {
            v.write_at(&data(i, 10), 0).await.unwrap();
            v.flush().await.unwrap();
        }
        v.close().await;
        drop(v);
        crash(&fs);
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        v.write_at(&data(9, 10), 0).await.unwrap();
        v.close().await;
        drop(v);
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        assert_eq!(
            read(&v, 0, 10).await,
            data(9, 10),
            "the journal starts above the floor the state names"
        );
        v.close().await;
        drop(v);
    }

    fn crash(fs: &SimFs) {
        fs.crash();
    }

    #[test]
    fn a_state_without_a_size_or_generation_still_reads() {
        let s = VolState {
            root: Entry::EMPTY,
            published: Entry::EMPTY,
            publishing: Entry::EMPTY,
            jfloor: 7,
            size: Some(8 << 30),
            generation: Some(4),
        };
        assert_eq!(VolState::decode(&s.encode()), Some(s));
        let sized = VolState {
            generation: None,
            ..s
        };
        assert_eq!(VolState::decode(&sized.encode()), Some(sized));
        let old = VolState {
            size: None,
            generation: None,
            ..s
        };
        let four = String::from_utf8(old.encode().to_vec()).unwrap();
        assert_eq!(four.lines().count(), 4);
        assert_eq!(VolState::decode(four.as_bytes()), Some(old));
    }

    #[tokio::test(start_paused = true)]
    async fn a_grow_past_the_tree_depth_keeps_every_chunk_and_publishes_the_size() {
        let (ports, _, _) = sim::ports(6);
        Volume::create(&ports, "main", 8 << 20).await.unwrap();
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        assert_eq!(v.tree().depth(), 1);
        v.write_at(&data(1, 1000), 4096).await.unwrap();
        v.checkpoint().await.unwrap();
        v.write_at(&data(2, 100), 70_000).await.unwrap();
        assert!(matches!(
            v.write_at(&data(3, 10), 8 << 20).await.unwrap_err(),
            Error::BeyondEnd
        ));

        let size = 64 << 20;
        v.grow(size).await.unwrap();
        assert_eq!((v.size(), v.tree().depth()), (size, 2));
        assert_eq!(read(&v, 4096, 1000).await, data(1, 1000));
        assert_eq!(read(&v, 70_000, 100).await, data(2, 100));
        assert_eq!(
            read(&v, 8 << 20, 10).await,
            vec![0; 10],
            "the new bytes read as zero"
        );
        v.write_at(&data(3, 5000), size - 5000).await.unwrap();
        v.checkpoint().await.unwrap();
        v.close().await;

        let other = Volume::open(&ports, host(&ports, "/b").await, Path::new("/b"), "main")
            .await
            .unwrap();
        assert_eq!(other.size(), size, "the ref has the new size");
        assert_eq!(read(&other, 4096, 1000).await, data(1, 1000));
        assert_eq!(read(&other, 70_000, 100).await, data(2, 100));
        assert_eq!(read(&other, size - 5000, 5000).await, data(3, 5000));
        other.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_grow_with_no_writes_still_publishes_the_size() {
        let (ports, _, _) = sim::ports(7);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        v.write_at(&data(1, 100), 0).await.unwrap();
        v.checkpoint().await.unwrap();
        let root = v.root();
        v.grow(2 * SIZE).await.unwrap();
        assert_eq!(v.root(), root, "a grow within the depth keeps the root");
        v.checkpoint().await.unwrap();
        v.close().await;
        let other = Volume::open(&ports, host(&ports, "/b").await, Path::new("/b"), "main")
            .await
            .unwrap();
        assert_eq!(other.size(), 2 * SIZE);
        assert_eq!(read(&other, 2 * SIZE - 10, 10).await, vec![0; 10]);
        other.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_grow_whose_state_write_failed_can_be_repeated() {
        let (ports, _, fs) = sim::ports(9);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        v.write_at(&data(1, 100), 0).await.unwrap();
        v.checkpoint().await.unwrap();
        let state = Path::new("/a/state");
        fs.remove(state).await.unwrap();
        fs.open(state, Open::Create).await.unwrap();
        assert!(v.grow(2 * SIZE).await.is_err());
        assert_eq!(v.size(), SIZE);
        fs.remove(state).await.unwrap();
        fs.create_dir_all(state).await.unwrap();
        v.grow(2 * SIZE).await.unwrap();
        assert_eq!(v.size(), 2 * SIZE);
        v.write_at(&data(2, 100), 2 * SIZE - 100).await.unwrap();
        v.checkpoint().await.unwrap();
        v.close().await;
        let other = Volume::open(&ports, host(&ports, "/b").await, Path::new("/b"), "main")
            .await
            .unwrap();
        assert_eq!(other.size(), 2 * SIZE);
        assert_eq!(read(&other, 2 * SIZE - 100, 100).await, data(2, 100));
        other.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_crash_after_a_grow_keeps_the_size_and_the_flushed_writes() {
        let (ports, store, fs) = sim::ports(8);
        Volume::create(&ports, "main", 8 << 20).await.unwrap();
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        v.write_at(&data(1, 100), 0).await.unwrap();
        v.checkpoint().await.unwrap();
        store.inject(sim::Fault {
            op: sim::StoreOp::Update,
            applied: false,
        });
        v.grow(64 << 20).await.unwrap();
        v.write_at(&data(2, 100), 40 << 20).await.unwrap();
        v.flush().await.unwrap();
        v.close().await;
        drop(v);
        crash(&fs);
        let (r, _) = Remote::new(Arc::clone(&ports.store))
            .get_ref("main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.size, 8 << 20);

        let again = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        assert_eq!(
            again.size(),
            64 << 20,
            "the local state has the size the store lacks"
        );
        assert_eq!(read(&again, 0, 100).await, data(1, 100));
        assert_eq!(read(&again, 40 << 20, 100).await, data(2, 100));
        again.checkpoint().await.unwrap();
        again.close().await;
        let other = Volume::open(&ports, host(&ports, "/b").await, Path::new("/b"), "main")
            .await
            .unwrap();
        assert_eq!(other.size(), 64 << 20);
        assert_eq!(read(&other, 40 << 20, 100).await, data(2, 100));
        other.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_shrink_and_a_size_off_the_block_size_are_refused() {
        let (ports, _, _) = sim::ports(9);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        assert!(matches!(
            v.grow(SIZE - 4096).await.unwrap_err(),
            Error::Shrink { .. }
        ));
        assert!(v.grow(SIZE + 100).await.is_err());
        v.grow(SIZE).await.unwrap();
        assert_eq!(v.size(), SIZE);
        v.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_second_writer_fences_the_first() {
        let (ports, store, _) = sim::ports(3);
        branch(&ports, "main").await;
        let a = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        a.write_at(&data(1, 100), 0).await.unwrap();
        a.checkpoint().await.unwrap();
        let b = Volume::open(&ports, host(&ports, "/b").await, Path::new("/b"), "main")
            .await
            .unwrap();
        a.write_at(&data(2, 100), 0).await.unwrap();
        let e = a.checkpoint().await.unwrap_err();
        assert!(matches!(e.kind(), Error::Fenced { .. }), "{e}");
        assert!(matches!(
            a.flush().await.unwrap_err().kind(),
            Error::Fenced { .. }
        ));
        assert_eq!(
            read(&b, 0, 100).await,
            data(1, 100),
            "the store keeps the first writer's last publish"
        );
        a.close().await;
        b.close().await;
        let _: &SimStore = &store;
    }
}
