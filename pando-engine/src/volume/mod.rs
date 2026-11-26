//! A volume: one writable branch, with write-back to the store.
//!
//! A flush commits locally: chunks and nodes go to the cache, one sync, then the local root is
//! recorded. A shipper then puts the flush's chunks in the branch's write log in the store,
//! and an uploader packs pending objects into segments, uploads them and moves the branch's ref
//! with a conditional update. A lost host loses only what neither has in the store yet; with
//! [`LogMode::Strict`], a flush returns only once the log or an upload has it.

use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use pando_format::{
    BLOCK_SIZE, BlockKeys, BranchId, CHUNK_SIZE, Entry, Hash, Ref, Writer, WriterId, depth_for, is_zero,
    u64_from, usize_from,
};
use tokio::sync::{Mutex as AsyncMutex, Notify, RwLock, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::base::{BaseView, Bases};
use crate::cache::{Cache, Root, Roots};
use crate::dedup::learned::Learner;
use crate::dedup::{Dedup, PreparedChunks};
use crate::flight::bounded;
use crate::gate::Gate;
use crate::local::{Extent, Journal, Record, VolState, read_file, write_durable};
use crate::packer::{PackStats, Packer, SEG_MAX};
use crate::ports::{Cpu, CpuClass, FileLock, LocalFs, Rng, StoreError, Version, on_cpu};
use crate::remote::Remote;
use crate::tree::Tree;
use crate::{Error, Ports, Result};

mod dirty;
mod image;
mod log;

use dirty::{Dirty, DirtySet, Under, WRITES_MAX};

pub use log::LogMode;
pub(crate) use log::seal as seal_log;

const STRIPES: usize = 64;
const CHUNK: u64 = u64_from(CHUNK_SIZE);
const CHUNK_LEN: u32 = 64 << 10;
const _: () = assert!(CHUNK_LEN as usize == CHUNK_SIZE);
/// The uploader looks for new commits at least this often.
const UPLOAD_EVERY: Duration = Duration::from_secs(2);
/// After a failed upload the uploader waits this long before the next try.
const RETRY_AFTER: Duration = Duration::from_secs(1);
/// The longest wait between tries while the store fails transiently; see [`backoff`].
const RETRY_MAX: Duration = Duration::from_secs(30);
/// How long a read, write or strict flush waits on the store by default before it fails.
pub const STORE_WAIT: Duration = Duration::from_secs(30);
/// Uploads in a row the store may refuse before the uploader stops. The wait before each next
/// try doubles from [`RETRY_AFTER`], so a refusal that passes within about two minutes, such
/// as a policy that is still propagating, does not stop it.
const REFUSED_TRIES: u32 = 8;
/// How often an open tries to take the writer generation when the store fails transiently.
const GENERATION_TRIES: usize = 10;
/// Chunks a flush hashes and stores at once.
const STORE_PARALLEL: usize = 16;
/// Segments a change list walk fetches nodes from at once.
const LIST_PARALLEL: usize = 16;
/// Journal and buffer bytes of the dirty set past which a commit starts in the background.
#[cfg(not(test))]
const DIRTY_COMMIT: usize = 256 << 20;
#[cfg(test)]
const DIRTY_COMMIT: usize = 2 << 20;
/// Journal and buffer bytes of the dirty set past which a write commits first.
#[cfg(not(test))]
const DIRTY_MAX: usize = 1 << 30;
#[cfg(test)]
const DIRTY_MAX: usize = 4 << 20;
/// The fewest dirty bytes a commit waits for under a limit of [`Cache::set_dirty_memory`].
const DIRTY_COMMIT_MIN: usize = 256 << 10;

/// The dirty bytes past which a commit starts, and past which a write commits first, for a
/// volume whose uncommitted writes may take `memory` bytes of page cache, or zero for no limit.
/// The journal holds the dirty set and, while a commit stores it, the one before, so the
/// limits are a quarter and a half of `memory`, and never above [`DIRTY_COMMIT`] and
/// [`DIRTY_MAX`].
fn dirty_limits(memory: u64) -> (usize, usize) {
    if memory == 0 {
        return (DIRTY_COMMIT, DIRTY_MAX);
    }
    let quarter = usize::try_from(memory / 4)
        .unwrap_or(usize::MAX)
        .max(DIRTY_COMMIT_MIN);
    (
        quarter.min(DIRTY_COMMIT),
        quarter.saturating_mul(2).min(DIRTY_MAX),
    )
}
static ZEROS: [u8; CHUNK_SIZE] = [0; CHUNK_SIZE];
static UPLOADS: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct Mem {
    root: Entry,
    /// The tree `root` is a root of, whose depth follows `size`.
    tree: Arc<Tree>,
    size: u64,
    /// The root the branch opened at, as a root of `tree`.
    opened_now: Entry,
    dirty: DirtySet,
    /// Every chunk written since the branch opened, so not the base image's any more.
    touched: HashSet<u64>,
    flushing: Option<Arc<BTreeMap<u64, Dirty>>>,
    /// The last commit's chunks: a write that started from one may still hold what lay under
    /// it, until the next commit takes the stripes.
    flushed: Option<Arc<BTreeMap<u64, Dirty>>>,
    jfloor: u64,
    /// Dirty chunks whose bytes are also in a file that stays until they are published.
    dirty_ref: BTreeMap<u64, ChunkRef>,
    /// The chunks written since tracking started, while a handoff copies the branch.
    track: Option<HashSet<u64>>,
    /// Flushes committed so far.
    flush_seq: u64,
    /// The journal generation of a commit whose state write failed: the state on disk still
    /// names an older root, and the journal below this generation is still needed.
    unpersisted: Option<u64>,
    /// Per file, the last flush that recorded a chunk as a reference into it.
    ref_seq: HashMap<PathBuf, u64>,
    /// The roots the state on disk may name, with their trees' depths: the last one written,
    /// and those whose writes have not finished or failed since.
    persisted: Vec<(Entry, usize)>,
}

impl Mem {
    /// The state of a branch that opened at `root`, with nothing written since.
    fn opened(root: Entry, tree: Arc<Tree>, size: u64, jfloor: u64) -> Mem {
        let persisted = vec![(root, tree.depth())];
        Mem {
            persisted,
            root,
            tree,
            size,
            opened_now: root,
            dirty: DirtySet::default(),
            touched: HashSet::new(),
            flushing: None,
            flushed: None,
            jfloor,
            dirty_ref: BTreeMap::new(),
            track: None,
            flush_seq: 0,
            unpersisted: None,
            ref_seq: HashMap::new(),
        }
    }

    /// Records that chunk `idx` is dirty now: not the base image's, and not a reference.
    fn written(&mut self, idx: u64) {
        self.touched.insert(idx);
        self.dirty_ref.remove(&idx);
        if let Some(t) = &mut self.track {
            t.insert(idx);
        }
    }
}

/// What a write puts in a chunk.
enum Write<'a> {
    /// New bytes, which go to the journal first.
    New(&'a [u8]),
    /// A write the journal holds already.
    Logged(Extent),
}

/// What a chunk starts from when a write makes it dirty.
enum Start {
    /// It is dirty already.
    Dirty,
    From(Dirty),
    /// What lies under the write is not on this host.
    Fetch,
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
    id: BranchId,
    from: Option<String>,
    remote: Remote,
    cache: Arc<Cache>,
    opened_tree: Arc<Tree>,
    opened_size: u64,
    fs: Arc<dyn LocalFs>,
    cpu: Arc<dyn Cpu>,
    writer: Arc<Writer>,
    /// Tells this open's uploads apart from every other's in the cache.
    uploads: u64,
    journal: Journal,
    state_path: PathBuf,
    state_lock: Mutex<Option<Box<dyn FileLock>>>,
    opened: Entry,
    stripes: Vec<RwLock<()>>,
    mem: Mutex<Mem>,
    commit: AsyncMutex<()>,
    persisting: AsyncMutex<()>,
    up: Mutex<Up>,
    changed: watch::Sender<u64>,
    kick: Notify,
    /// Wakes the committer: the dirty set passed the first of [`dirty_limits`].
    full: Notify,
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
    /// How long device I/O waits on the store, in milliseconds; zero waits for good.
    store_wait: AtomicU64,
    rng: Arc<dyn Rng>,
    /// When committed flushes not yet uploaded were committed: the earliest time of each run
    /// of flushes, keyed by the last flush of the run.
    unuploaded: Mutex<VecDeque<(u64, Instant)>>,
}

/// One writable branch.
#[derive(Debug)]
pub struct Volume {
    inner: Arc<Inner>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl VolState {
    /// Whether a writer may resume this state over ref `r`: the ref is of the life of the
    /// branch this state wrote, holds the root this state published or was publishing, and the
    /// last writer to take the branch is the one this state names.
    fn resumes(&self, r: &Ref) -> bool {
        self.id == Some(r.id)
            && (self.published.hash == r.root.hash || self.publishing.hash == r.root.hash)
            && self.generation.is_none_or(|g| g == r.generation)
    }
}

/// A root an operation starts from, of a tree of `depth`: what it has not uploaded stays.
fn pinned(entry: Entry, depth: usize) -> Root {
    Root {
        entry,
        height: depth,
        all: false,
    }
}

impl Roots for Inner {
    fn roots(&self, out: &mut Vec<Root>) {
        let m = self.mem.lock();
        out.push(Root {
            entry: m.root,
            height: m.tree.depth(),
            all: true,
        });
        out.extend(m.persisted.iter().map(|&(e, depth)| pinned(e, depth)));
        out.push(pinned(m.opened_now, m.tree.depth()));
        let flushing =
            (m.flushing.iter().chain(&m.flushed)).flat_map(|f| f.values().filter_map(Dirty::stored));
        out.extend(m.dirty.stored().chain(flushing).map(|&entry| Root {
            entry,
            height: 0,
            all: true,
        }));
    }

    fn forget(&self, dropped: &HashSet<Hash>) {
        let tree = Arc::clone(&self.mem.lock().tree);
        tree.forget(dropped);
    }
}

impl Volume {
    /// Creates branch `name` of `size` bytes, all zero.
    pub async fn create(ports: &Ports, name: &str, size: u64) -> Result<()> {
        Remote::new(Arc::clone(&ports.store))
            .create_ref(ports.rng.as_ref(), name, |id| Ref {
                size,
                id,
                root: Entry::EMPTY,
                ..Ref::default()
            })
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
        let mem = Mem::opened(root, Arc::clone(&tree), size, resume.map_or(0, |s| s.jfloor));
        let inner = Arc::new(Inner {
            name: name.to_owned(),
            size: AtomicU64::new(size),
            generation,
            id: r.id,
            from: r.from.clone(),
            remote,
            opened_tree: tree,
            opened_size: size,
            cache,
            fs: Arc::clone(&ports.fs),
            cpu: Arc::clone(&ports.cpu),
            writer: Arc::new(Writer::new(WriterId::new(name, generation), ports.rng.next_u32())),
            uploads: UPLOADS.fetch_add(1, Ordering::Relaxed),
            journal,
            state_path,
            state_lock: Mutex::new(Some(state_lock)),
            opened: mem.root,
            stripes: (0..STRIPES).map(|_| RwLock::new(())).collect(),
            mem: Mutex::new(mem),
            commit: AsyncMutex::new(()),
            persisting: AsyncMutex::new(()),
            up: Mutex::new(Up {
                version: version.clone(),
                published: r.root,
                published_size: r.size,
                uploaded: r.root,
                last_from: Hash::ZERO,
                publishing: Entry::EMPTY,
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
            full: Notify::new(),
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
            store_wait: AtomicU64::new(millis(STORE_WAIT)),
            rng: Arc::clone(&ports.rng),
            unuploaded: Mutex::default(),
        });
        let weak: Weak<Inner> = Arc::downgrade(&inner);
        let roots: Weak<dyn Roots> = weak;
        inner.cache.register(&inner.state_path, roots).await?;
        inner.cache.start_sweeps();
        inner.start_journal(resume).await?;
        if resume.is_some_and(|s| s.root.hash != r.root.hash) {
            inner.resumed_ahead();
        }
        let grown = size != r.size;
        let v = Volume {
            tasks: Mutex::new(inner.start(r, version)),
            inner,
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

    /// The bytes of chunks under the current root that no upload has put in the store yet, as
    /// 64 KiB chunks; zero once every flush is uploaded.
    pub async fn unuploaded_bytes(&self) -> Result<u64> {
        let (tree, root) = self.tree_and_root();
        Ok(tree.pending_count(&root).await? * CHUNK)
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
        let Some(until) = self.inner.deadline() else {
            let futs = misses
                .iter()
                .map(|&(idx, at, _, n)| self.inner.read_chunk(idx, at, n));
            let got = futures::future::join_all(futs).await;
            for ((_, _, pos, n), b) in misses.into_iter().zip(got) {
                p[pos..pos + n].copy_from_slice(&b?);
            }
            return Ok(());
        };
        loop {
            let futs = misses
                .iter()
                .map(|&(idx, at, _, n)| self.inner.read_chunk_held(idx, at, n));
            let got = futures::future::join_all(futs).await;
            let mut again = Vec::new();
            for (m, b) in misses.into_iter().zip(got) {
                let (_, _, pos, n) = m;
                match b? {
                    Some(b) => p[pos..pos + n].copy_from_slice(&b),
                    None => again.push(m),
                }
            }
            if again.is_empty() {
                return Ok(());
            }
            let needs = again.iter().map(|&(idx, ..)| (idx, true)).collect();
            self.inner.ready(needs, Some(until)).await?;
            misses = again;
        }
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
    /// on this host. While the cache's disk is full it fails at once, changing nothing, with an
    /// error that [`Error::is_no_room`] reports. A write that fills the disk part-way fails
    /// with that error too, and may have applied some of its chunks, as a failed write to a
    /// disk may have written some of its sectors.
    pub async fn write_at(&self, p: &[u8], off: u64) -> Result<()> {
        if beyond(off, u64_from(p.len()), self.inner.size()) {
            return Err(Error::BeyondEnd);
        }
        self.inner.cache.space().admit().await?;
        let r = self.write_parts(p, off).await;
        self.inner.noted(r).await
    }

    async fn write_parts(&self, p: &[u8], off: u64) -> Result<()> {
        let until = self.inner.deadline();
        let mut parts = parts(off, p.len());
        let needs = parts.iter().map(|&(idx, .., n)| (idx, n < CHUNK_SIZE)).collect();
        self.inner.ready(needs, until).await?;
        loop {
            let writes = parts.iter().map(|&(idx, at, pos, n)| {
                self.inner
                    .write_chunk(idx, at, Write::New(&p[pos..pos + n]), until.is_some())
            });
            let done = futures::future::join_all(writes).await;
            let mut again = Vec::new();
            for (part, applied) in parts.into_iter().zip(done) {
                if !applied? {
                    again.push(part);
                }
            }
            if again.is_empty() {
                return Ok(());
            }
            self.inner
                .ready(again.iter().map(|&(idx, ..)| (idx, true)).collect(), until)
                .await?;
            parts = again;
        }
    }

    /// Clears a range. Whole chunks that hold no data stay sparse. It fails as
    /// [`Self::write_at`] does while the cache's disk is full.
    pub async fn zero(&self, off: u64, len: u64) -> Result<()> {
        if beyond(off, len, self.inner.size()) {
            return Err(Error::BeyondEnd);
        }
        self.inner.cache.space().admit().await?;
        let r = self.zero_parts(off, len).await;
        self.inner.noted(r).await
    }

    async fn zero_parts(&self, off: u64, len: u64) -> Result<()> {
        let mut needs = Vec::new();
        let mut pos = off;
        while pos < off + len {
            let (idx, at) = (pos / CHUNK, usize_from(pos % CHUNK));
            let n = (CHUNK_SIZE - at).min(usize_from(off + len - pos));
            needs.push((idx, n < CHUNK_SIZE));
            pos += u64_from(n);
        }
        let until = self.inner.deadline();
        self.inner.ready(needs, until).await?;
        let mut pos = off;
        while pos < off + len {
            let (idx, at) = (pos / CHUNK, usize_from(pos % CHUNK));
            let n = (CHUNK_SIZE - at).min(usize_from(off + len - pos));
            if n < CHUNK_SIZE {
                while !self
                    .inner
                    .write_chunk(idx, at, Write::New(&ZEROS[..n]), until.is_some())
                    .await?
                {
                    self.inner.ready(vec![(idx, true)], until).await?;
                }
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

    /// Waits until the uploader stops for good, and returns why: no later upload can publish
    /// the branch.
    pub async fn upload_stopped(&self) -> Arc<Error> {
        let mut rx = self.inner.changed.subscribe();
        loop {
            if let Some(e) = &self.inner.up.lock().error {
                return Arc::clone(e);
            }
            if rx.changed().await.is_err() {
                return Arc::new(Error::Cancelled);
            }
        }
    }

    /// Why the uploader stopped for good, once it has: no later upload can publish the branch.
    pub fn upload_error(&self) -> Option<Arc<Error>> {
        self.inner.up.lock().error.clone()
    }

    /// What the last upload packed.
    pub fn last_upload(&self) -> Option<PackStats> {
        self.inner.up.lock().last
    }

    /// Sets how long a read, write, zero or strict flush waits on the store before it fails
    /// with a transient store error; zero waits for good. A fetch it gave up on goes on, so
    /// no write is half applied and a later try may find the bytes here. Opens start at
    /// [`STORE_WAIT`].
    pub fn set_store_wait(&self, wait: Duration) {
        let ms = if wait.is_zero() { 0 } else { millis(wait).max(1) };
        self.inner.store_wait.store(ms, Ordering::Relaxed);
    }

    /// How long ago the oldest committed flush that no upload has published yet committed,
    /// to within two seconds; zero when every flush is published. It grows while the store
    /// is unreachable.
    pub fn upload_lag(&self) -> Duration {
        self.inner.upload_lag()
    }

    /// Whether writes fail because the cache's disk is full; it looks for room again first, at
    /// most once a second. Flushes and reads go on while it is full: the cache keeps 128 MiB
    /// for the commits, and writes resume once twice that is free.
    pub async fn disk_full(&self) -> bool {
        self.inner.cache.space().admit().await.is_err()
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

    /// Ends the volume's tasks where they are, as a killed process does: a store call in flight
    /// may or may not have happened. Then releases the local state for another open.
    #[cfg(feature = "sim")]
    pub async fn kill(&self) {
        self.inner.stop.send_replace(true);
        let tasks = std::mem::take(&mut *self.tasks.lock());
        for t in &tasks {
            t.abort();
        }
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

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// The wait before the next try after `failed` tries in a row failed: it doubles from
/// [`RETRY_AFTER`] up to [`RETRY_MAX`], and a random part in its upper half keeps hosts that
/// failed together from trying again together.
fn backoff(failed: u32, rng: &dyn Rng) -> Duration {
    let full = millis(
        RETRY_AFTER
            .saturating_mul(1 << failed.saturating_sub(1).min(16))
            .min(RETRY_MAX),
    );
    let half = full / 2;
    Duration::from_millis(half + rng.next_u64() % (full - half + 1))
}

/// The error of device I/O that waited `wait` on the store for `what`.
fn unanswered(what: String, wait: Duration) -> Error {
    Error::Store(StoreError::Transient {
        key: what,
        source: format!("the store did not answer within {wait:?}").into(),
    })
}

fn is_transient(e: &Error) -> bool {
    matches!(e.kind(), Error::Store(StoreError::Transient { .. }))
}

fn is_permanent(e: &Error) -> bool {
    matches!(e.kind(), Error::Store(StoreError::Permanent { .. }))
}

fn fail_with(up: &mut Up, e: Error) -> Arc<Error> {
    let e = match e {
        Error::Shared(e) => e,
        e @ (Error::Fenced { .. }
        | Error::Remade { .. }
        | Error::ChunkLost { .. }
        | Error::Store(_)
        | Error::Format(_)
        | Error::UnknownFormat { .. }
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
    /// Starts the tasks that run until the volume closes: the generation take on ref `r`, the
    /// uploader, the shipper and the committer.
    fn start(self: &Arc<Self>, r: Ref, version: Version) -> Vec<JoinHandle<()>> {
        vec![
            tokio::spawn(Arc::clone(self).take_generation(r, version)),
            tokio::spawn(Arc::clone(self).uploader()),
            tokio::spawn(Arc::clone(self).shipper()),
            tokio::spawn(Arc::clone(self).committer()),
        ]
    }

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
                Record::Data(e) => {
                    self.write_chunk(idx, e.at(), Write::Logged(e), false).await?;
                }
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
            let ((root, opened), pin) = self.cache.pin(|| {
                let m = self.mem.lock();
                ((m.root, m.opened_now), vec![pinned(m.root, from.depth())])
            });
            let lifted = to.lift(&root, from.depth()).await?;
            let opened = to.lift(&opened, from.depth()).await?;
            if depth != from.depth() {
                self.cache.sync_local().await?;
            }
            let grown = pin.finish(|| {
                let mut m = self.mem.lock();
                if m.root.hash != root.hash {
                    return false;
                }
                m.root = lifted;
                m.opened_now = opened;
                m.tree = Arc::clone(&to);
                m.size = size;
                true
            });
            if grown {
                break;
            }
        }
        self.persist().await?;
        self.size.store(size, Ordering::Release);
        self.kick.notify_one();
        Ok(())
    }

    fn store_wait(&self) -> Option<Duration> {
        match self.store_wait.load(Ordering::Relaxed) {
            0 => None,
            ms => Some(Duration::from_millis(ms)),
        }
    }

    /// When device I/O that starts now must stop waiting on the store; `None` waits for good.
    fn deadline(&self) -> Option<Instant> {
        self.store_wait().map(|w| Instant::now() + w)
    }

    /// Whether chunk `idx` is readable without the store: its tree nodes, and with `data` its
    /// bytes, are on this host.
    fn is_local(&self, idx: u64, data: bool) -> bool {
        let (tree, root) = match self.dirty_stored(idx) {
            Ok(e) => return !data || e.is_none_or(|e| self.cache.holds(&e.hash)),
            Err(t) => t,
        };
        if data && self.base_of(idx).is_some() {
            return true;
        }
        match tree.lookup_nowait(&root, idx) {
            None => false,
            Some(e) if !data || e.is_empty() => true,
            Some(e) => self.cache.holds(&e.hash),
        }
    }

    /// Fetches what [`Self::is_local`] needs of chunk `idx`. It takes no stripe lock, so a
    /// fetch the store does not answer holds up no write or flush.
    async fn warm(&self, idx: u64, data: bool) -> Result<()> {
        let (tree, root) = match self.dirty_stored(idx) {
            Ok(Some(e)) if data => return self.cache.warm(&e).await,
            Ok(_) => return Ok(()),
            Err(t) => t,
        };
        if data && self.base_of(idx).is_some() {
            return Ok(());
        }
        let e = tree.lookup(&root, idx, false).await?;
        if data && !e.is_empty() {
            self.cache.warm(&e).await?;
        }
        Ok(())
    }

    /// For a dirty chunk `idx`, the committed chunk its reads may still need; for any other,
    /// the tree and root to read it from.
    fn dirty_stored(&self, idx: u64) -> std::result::Result<Option<Entry>, (Arc<Tree>, Entry)> {
        let m = self.mem.lock();
        match m.dirty.get(idx).or_else(|| m.flushing.as_ref()?.get(&idx)) {
            Some(d) => Ok(d.stored().copied()),
            None => Err((Arc::clone(&m.tree), m.root)),
        }
    }

    /// Fetches what each `(chunk, data)` of `needs` lacks, as [`Self::warm`] does, waiting
    /// until `until` at most: device I/O then fails instead of hanging while the store does not
    /// answer. The fetches go on after it gives up, so the next try may find them done, and a
    /// try that finds a fetch of the same frame running waits for it instead of fetching again.
    async fn ready(self: &Arc<Self>, mut needs: Vec<(u64, bool)>, until: Option<Instant>) -> Result<()> {
        needs.retain(|&(idx, data)| !self.is_local(idx, data));
        let Some(&(first, _)) = needs.first() else {
            return Ok(());
        };
        let inner = Arc::clone(self);
        let fetch = tokio::spawn(async move {
            let futs = needs.iter().map(|&(idx, data)| inner.warm(idx, data)).collect();
            bounded(futs, STORE_PARALLEL)
                .await
                .into_iter()
                .collect::<Result<()>>()
        });
        let joined = match until {
            None => fetch.await,
            Some(until) => tokio::time::timeout_at(until, fetch).await.map_err(|_| {
                let wait = self.store_wait().unwrap_or_default();
                unanswered(format!("chunk {first} of {}", self.name), wait)
            })?,
        };
        joined.map_err(|_| Error::Cancelled)?
    }

    /// Records that flush `seq` committed now.
    fn committed(&self, seq: u64) {
        let now = Instant::now();
        let uploaded = *self.uploaded_seq.borrow();
        let mut q = self.unuploaded.lock();
        while q.front().is_some_and(|&(s, _)| s <= uploaded) {
            q.pop_front();
        }
        match q.back_mut() {
            Some((last, at)) if now.duration_since(*at) < UPLOAD_EVERY => *last = seq,
            _ => q.push_back((seq, now)),
        }
    }

    fn upload_lag(&self) -> Duration {
        let uploaded = *self.uploaded_seq.borrow();
        let mut q = self.unuploaded.lock();
        while q.front().is_some_and(|&(s, _)| s <= uploaded) {
            q.pop_front();
        }
        q.front().map_or(Duration::ZERO, |&(_, at)| at.elapsed())
    }

    fn stripe(&self, idx: u64) -> &RwLock<()> {
        &self.stripes[usize_from(idx % u64_from(STRIPES))]
    }

    fn notify(&self) {
        self.changed.send_modify(|n| *n += 1);
    }

    async fn persist(&self) -> Result<()> {
        let _one = self.persisting.lock().await;
        let (root, jfloor, size) = {
            let mut m = self.mem.lock();
            let state = (m.root, m.tree.depth());
            m.persisted.push(state);
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
            id: Some(self.id),
        };
        write_durable(self.fs.as_ref(), &self.state_path, st.encode()).await?;
        self.mem.lock().persisted.retain(|p| p.0.hash == root.hash);
        Ok(())
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

    /// What a read of `[at, at + n)` of a dirty chunk `idx` needs, or else the tree and root
    /// to read the chunk from.
    fn find(&self, idx: u64, at: usize, n: usize) -> std::result::Result<Dirty, (Arc<Tree>, Entry)> {
        let m = self.mem.lock();
        match m.dirty.get(idx).or_else(|| m.flushing.as_ref()?.get(&idx)) {
            Some(d) => Ok(d.part(at, n)),
            None => Err((Arc::clone(&m.tree), m.root)),
        }
    }

    async fn read_chunk(&self, idx: u64, at: usize, n: usize) -> Result<Bytes> {
        let _g = self.stripe(idx).read().await;
        let (tree, root) = match self.find(idx, at, n) {
            Ok(d) => return d.read(&self.cache, idx, at, n).await,
            Err(t) => t,
        };
        if let Some(b) = self.read_base(idx, at, n).await {
            return Ok(b);
        }
        self.read_committed(&tree, idx, &root, at, n).await
    }

    /// [`Self::read_chunk`] from this host only: `None` when it would wait on the store, which
    /// it never does while it holds the stripe lock.
    async fn read_chunk_held(&self, idx: u64, at: usize, n: usize) -> Result<Option<Bytes>> {
        let _g = self.stripe(idx).read().await;
        let (tree, root) = match self.find(idx, at, n) {
            Ok(d) => return d.read_held(&self.cache, idx, at, n).await,
            Err(t) => t,
        };
        if let Some(b) = self.read_base(idx, at, n).await {
            return Ok(Some(b));
        }
        self.read_held(&tree, idx, &root, at, n).await
    }

    /// [`Self::read_committed`] from this host only: `None` when it needs the store.
    async fn read_held(
        &self,
        tree: &Tree,
        idx: u64,
        root: &Entry,
        at: usize,
        n: usize,
    ) -> Result<Option<Bytes>> {
        match tree.lookup_nowait(root, idx) {
            None => Ok(None),
            Some(e) if e.is_empty() => Ok(Some(Bytes::from(vec![0; n]))),
            Some(e) => self.cache.read_held(&e, at, n).await,
        }
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
        match self.find(idx, at, dst.len()) {
            Ok(d) => d.read_nowait(&self.cache, idx, at, dst),
            Err((tree, root)) => self.try_read_stored(&tree, idx, &root, at, dst),
        }
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
            if let Some(d) = m.dirty.get(idx).or_else(|| m.flushing.as_ref()?.get(&idx)) {
                return Ok(d.is_zero());
            }
            (Arc::clone(&m.tree), m.root)
        };
        Ok(tree.lookup(&root, idx, false).await?.is_empty())
    }

    /// Applies one write to a chunk. The chunk's stripe lock orders writes to the same chunk,
    /// in the dirty set and in the journal; writes to other chunks run in parallel. A flush
    /// takes every stripe, so no write is half applied when it takes the dirty set. With
    /// `held_only` it never waits on the store under the lock: it applies nothing and returns
    /// `false` when what lies under a partial write is not on this host.
    async fn write_chunk(&self, idx: u64, at: usize, w: Write<'_>, held_only: bool) -> Result<bool> {
        if matches!(w, Write::New(_)) {
            self.make_room().await?;
        }
        let _g = self.stripe(idx).write().await;
        let n = match &w {
            Write::New(data) => data.len(),
            Write::Logged(e) => e.end() - e.at(),
        };
        let from = match self
            .starts_from(idx, at == 0 && n == CHUNK_SIZE, held_only)
            .await?
        {
            Start::Dirty => None,
            Start::From(d) => Some(d),
            Start::Fetch => return Ok(false),
        };
        let (e, new) = match w {
            Write::New(data) => (self.journal.append(idx, at, data).await?, true),
            Write::Logged(e) => (e, false),
        };
        let kept = {
            let mut m = self.mem.lock();
            m.written(idx);
            m.dirty.push(idx, from, e)
        };
        match kept {
            None => Err(Error::Corrupt(format!(
                "chunk {idx} left the dirty set under its stripe lock"
            ))),
            Some(k) if new && k > WRITES_MAX => self.fold(idx).await.map(|()| true),
            Some(_) => Ok(true),
        }
    }

    /// What chunk `idx` starts from when a write makes it dirty. A write of the whole chunk
    /// needs nothing under it; a partial one keeps what lies under it, to read later.
    async fn starts_from(&self, idx: u64, whole: bool, held_only: bool) -> Result<Start> {
        let (tree, root) = {
            let m = self.mem.lock();
            if m.dirty.contains_key(idx) {
                return Ok(Start::Dirty);
            }
            if let Some(f) = m.flushing.as_ref().and_then(|f| f.get(&idx)) {
                return Ok(Start::From(f.clone()));
            }
            if whole {
                return Ok(Start::From(Dirty::new(Under::Zero)));
            }
            (Arc::clone(&m.tree), m.root)
        };
        if let Some(v) = self.base_of(idx) {
            return Ok(Start::From(Dirty::new(Under::Base(v))));
        }
        let e = match tree.lookup_nowait(&root, idx) {
            Some(e) => e,
            None if held_only => return Ok(Start::Fetch),
            None => tree.lookup(&root, idx, false).await?,
        };
        if held_only && !e.is_empty() && !self.cache.holds(&e.hash) {
            return Ok(Start::Fetch);
        }
        Ok(Start::From(Dirty::new(Under::Stored(e))))
    }

    /// Replaces the writes of chunk `idx` with one record of the whole chunk, so its reads stay
    /// short. Call it holding the chunk's stripe lock.
    async fn fold(&self, idx: u64) -> Result<()> {
        let Some(d) = self.mem.lock().dirty.get(idx).cloned() else {
            return Ok(());
        };
        let b = d.read(&self.cache, idx, 0, CHUNK_SIZE).await?;
        let e = self.journal.append(idx, 0, &b).await?;
        self.mem.lock().dirty.push(idx, None, e);
        Ok(())
    }

    /// Keeps the dirty set near its most, the second of [`dirty_limits`]: past the first it
    /// wakes the committer, and past the most it commits before the write. Call it holding no
    /// stripe lock.
    async fn make_room(&self) -> Result<()> {
        let (commit, most) = dirty_limits(self.cache.dirty_memory());
        loop {
            let bytes = self.mem.lock().dirty.bytes;
            if bytes < commit {
                return Ok(());
            }
            self.full.notify_one();
            if bytes < most {
                return Ok(());
            }
            self.commit_at(most).await?;
        }
    }

    /// Commits the dirty set whenever it passes the first of [`dirty_limits`], until the
    /// volume stops. A commit that fails leaves its writes dirty, or its state unwritten, for
    /// the next flush to repeat and report.
    async fn committer(self: Arc<Self>) {
        let mut stop = self.stop.subscribe();
        loop {
            tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                () = self.full.notified() => {}
            }
            let _ = self.commit_at(dirty_limits(self.cache.dirty_memory()).0).await;
        }
    }

    /// Makes chunk `idx` all zeros, as [`Self::write_chunk`] would with a chunk of zeros.
    async fn zero_chunk(&self, idx: u64, journaled: bool) -> Result<()> {
        let _g = self.stripe(idx).write().await;
        if journaled {
            self.journal.append_zero_chunk(idx).await?;
        }
        let mut m = self.mem.lock();
        m.written(idx);
        m.dirty.insert(idx, Dirty::new(Under::Zero));
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let seq = self.commit_flush().await?;
        if self.log.mode() == LogMode::Strict {
            match self.store_wait() {
                None => self.wait_logged(seq).await?,
                Some(wait) => tokio::time::timeout(wait, self.wait_logged(seq))
                    .await
                    .map_err(|_| unanswered(format!("the write log of {}", self.name), wait))??,
            }
        }
        Ok(())
    }

    /// Commits completed writes locally and durably; returns the number of the last flush.
    async fn commit_flush(&self) -> Result<u64> {
        self.commit_at(0).await
    }

    /// Frees the cache's reserve when `r` failed for want of room, so the commits that follow
    /// find it, and writes fail until the disk has room again.
    async fn noted<T>(&self, r: Result<T>) -> Result<T> {
        if let Err(e) = &r
            && e.is_no_room()
        {
            self.cache.space().release().await;
        }
        r
    }

    /// Commits the dirty set when it is not empty and holds at least `min` bytes.
    /// A commit with `min` above 0 only bounds memory: no caller waits for it, so it leaves
    /// the log to the uploader's next pass and does not start one. A commit that finds the
    /// disk full frees the cache's reserve and tries once more, when the reserve was freed
    /// since it began, by it or by a write.
    async fn commit_at(&self, min: usize) -> Result<u64> {
        let space = self.cache.space();
        let before = space.released();
        match self.commit_once(min).await {
            Err(e) if e.is_no_room() && space.release().await != before => self.commit_once(min).await,
            r => r,
        }
    }

    async fn commit_once(&self, min: usize) -> Result<u64> {
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
        let unpersisted = {
            let m = self.mem.lock();
            if !m.dirty.is_empty() && m.dirty.bytes >= min {
                None
            } else if let Some(g) = m.unpersisted {
                Some((g, m.flush_seq, m.size))
            } else {
                return Ok(m.flush_seq);
            }
        };
        if let Some((generation, seq, size)) = unpersisted {
            drop(guards);
            self.persist_commit(generation).await?;
            let chunks = Vec::new();
            self.queue_flush(log::Flushed { seq, size, chunks }, false);
            self.kick.notify_one();
            return Ok(seq);
        }
        let generation = self.journal.rotate().await?;
        let ((snapshot, refs, tree, root), pin) = self.cache.pin(|| {
            let mut m = self.mem.lock();
            let snap = Arc::new(m.dirty.take());
            m.flushing = Some(Arc::clone(&snap));
            m.flushed = None;
            let root = pinned(m.root, m.tree.depth());
            (
                (
                    snap,
                    std::mem::take(&mut m.dirty_ref),
                    Arc::clone(&m.tree),
                    m.root,
                ),
                vec![root],
            )
        });
        drop(guards);
        let keep = min == 0 && snapshot.len() <= log::KEPT_MAX;
        match self.commit_snapshot(&tree, &snapshot, &refs, &root, keep).await {
            Ok((new_root, updates)) => {
                let (seq, size, after_unpersisted) = pin.finish(|| {
                    let mut m = self.mem.lock();
                    m.root = new_root;
                    m.flushed = m.flushing.take();
                    m.jfloor = generation;
                    m.flush_seq += 1;
                    let seq = m.flush_seq;
                    for r in refs.into_values() {
                        m.ref_seq.insert(r.path, seq);
                    }
                    (seq, m.size, m.unpersisted.replace(generation).is_some())
                });
                self.committed(seq);
                self.persist_commit(generation).await?;
                let chunks = updates
                    .into_iter()
                    .map(|(idx, e, b)| (idx, b.filter(|_| !e.is_empty()).map(|b| (e.hash, b))))
                    .collect();
                self.queue_flush(log::Flushed { seq, size, chunks }, keep && !after_unpersisted);
                if min == 0 {
                    self.kick.notify_one();
                }
                Ok(seq)
            }
            Err(e) => {
                let mut m = self.mem.lock();
                m.flushing = None;
                for (idx, d) in snapshot.iter() {
                    if !m.dirty.contains_key(*idx) {
                        m.dirty.insert(*idx, d.clone());
                        if let Some(r) = refs.get(idx) {
                            m.dirty_ref.insert(*idx, r.clone());
                        }
                    }
                }
                Err(e)
            }
        }
    }

    /// Writes the state of a commit at journal generation `generation`, then drops the journal
    /// below it. Until the state write succeeds, every later commit and flush repeats it first.
    async fn persist_commit(&self, generation: u64) -> Result<()> {
        self.persist().await?;
        self.mem.lock().unpersisted = None;
        self.journal.drop_below(generation).await;
        Ok(())
    }

    /// Stores the chunks of `snapshot` and moves `root` to them. Returns the new root and each
    /// chunk's entry, with its bytes when `keep`.
    async fn commit_snapshot(
        &self,
        tree: &Tree,
        snapshot: &BTreeMap<u64, Dirty>,
        refs: &BTreeMap<u64, ChunkRef>,
        root: &Entry,
        keep: bool,
    ) -> Result<(Entry, Vec<(u64, Entry, Option<Bytes>)>)> {
        let mut futs = Vec::with_capacity(snapshot.len());
        for (&idx, d) in snapshot {
            futs.push(async move {
                let b = d.read(&self.cache, idx, 0, CHUNK_SIZE).await?;
                let e = self.store_chunk(b.clone(), refs.get(&idx)).await?;
                Ok((idx, e, keep.then_some(b)))
            });
        }
        let mut done = bounded(futs, STORE_PARALLEL)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        for (_, e, _) in &mut done {
            if let Some(l) = self.cache.stored(&e.hash, self.uploads) {
                e.loc = l;
            }
        }
        done.sort_by_key(|u| u.0);
        let updates: Vec<(u64, Entry)> = done.iter().map(|&(idx, e, _)| (idx, e)).collect();
        let new_root = tree.update(root, &updates).await?;
        self.cache.sync_local().await?;
        Ok((new_root, done))
    }

    async fn store_chunk(&self, b: Bytes, r: Option<&ChunkRef>) -> Result<Entry> {
        if let Some(r) = r {
            self.cache.put_ref(r.hash, &r.path, r.offset, CHUNK_LEN).await?;
            return Ok(Entry {
                hash: r.hash,
                ..Entry::EMPTY
            });
        }
        let hashed = b.clone();
        let h = on_cpu(self.cpu.as_ref(), CpuClass::Foreground, move || {
            (!is_zero(&hashed)).then(|| Hash::of(&hashed))
        })
        .await;
        let Some(h) = h else {
            return Ok(Entry::EMPTY);
        };
        self.cache.put_local(h, b).await?;
        Ok(Entry {
            hash: h,
            ..Entry::EMPTY
        })
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
        let mut refused = 0;
        let mut failed = 0;
        loop {
            tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                () = self.kick.notified() => {}
                () = tokio::time::sleep(UPLOAD_EVERY) => {}
            }
            let e = match self.upload().await {
                Ok(()) => {
                    refused = 0;
                    failed = 0;
                    continue;
                }
                Err(e) => e,
            };
            if matches!(e.kind(), Error::Fenced { .. } | Error::ChunkLost { .. }) {
                return;
            }
            let wait = if is_permanent(&e) {
                refused += 1;
                if refused >= REFUSED_TRIES {
                    fail_with(&mut self.up.lock(), e);
                    self.notify();
                    return;
                }
                RETRY_AFTER * (1 << (refused - 1))
            } else {
                failed += 1;
                backoff(failed, self.rng.as_ref())
            };
            tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                () = tokio::time::sleep(wait) => {}
            }
        }
    }

    /// Packs everything pending under the current local root and publishes it.
    async fn upload(&self) -> Result<()> {
        let ((root, seq, tree, size), pin) = self.cache.pin_roots(|| {
            let m = self.mem.lock();
            (
                (m.root, m.flush_seq, Arc::clone(&m.tree), m.size),
                vec![pinned(m.root, m.tree.depth())],
            )
        });
        let version = {
            let up = self.up.lock();
            if (root.hash == up.uploaded.hash && size == up.published_size) || up.fenced {
                return Ok(());
            }
            up.version.clone()
        };
        let t0 = Instant::now();
        let dedup = self.dedup.lock().clone();
        let learner = self.learner.lock().clone();
        let prepared = self.prepared.lock().clone();
        let mut p = self.packer(dedup.clone(), prepared.clone(), learner.clone());
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
            id: self.id,
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
        self.cache.set_stored_all(p.added(), self.uploads);
        record_learned(dedup.as_ref(), learner.as_ref(), &p).await;
        drop(p);
        pin.finish(|| {
            let mut up = self.up.lock();
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
        });
        if let Some(p) = &prepared {
            p.drop_through(seq);
        }
        self.log_published(&pos);
        self.uploaded_seq.send_modify(|s| *s = (*s).max(seq));
        self.notify();
        let _ = self.persist().await;
        Ok(())
    }

    fn packer(
        &self,
        dedup: Option<Arc<Dedup>>,
        prepared: Option<Arc<PreparedChunks>>,
        learner: Option<Arc<Learner>>,
    ) -> Packer {
        Packer::new(
            self.remote.clone(),
            Arc::clone(&self.cpu),
            Arc::clone(&self.writer),
            SEG_MAX,
        )
        .with_stored(Arc::clone(&self.cache), self.uploads)
        .with_dedup(dedup, prepared)
        .with_gate(Arc::clone(&self.gate))
        .with_learner(learner)
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
    use crate::ports::{Open, StoreError};
    use crate::sim::{self, Fault, SimFs, SimStore, StoreOp};

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
    async fn unuploaded_bytes_count_the_flushed_chunks_until_an_upload_puts_them() {
        let (ports, _, _) = sim::ports(1);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        assert_eq!(v.generation(), 1);
        assert_eq!(v.unuploaded_bytes().await.unwrap(), 0);
        v.write_at(&data(1, 200_000), 30_000).await.unwrap();
        v.flush().await.unwrap();
        let pending = v.unuploaded_bytes().await.unwrap();
        assert!(
            pending > 0 && pending <= 4 * CHUNK && pending % CHUNK == 0,
            "{pending}"
        );
        v.checkpoint().await.unwrap();
        assert_eq!(v.unuploaded_bytes().await.unwrap(), 0);
        v.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_disk_fails_writes_lets_flushes_commit_and_loses_no_acknowledged_write() {
        let (ports, _, fs) = sim::ports(1);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        let big = u64::MAX / 2;
        fs.set_capacity(Some(big));
        let used = big - fs.available(Path::new("/a")).await.unwrap();
        fs.set_capacity(Some(used + (6 << 20)));
        let mut acked = Vec::new();
        for i in 0..SIZE >> 20 {
            match v
                .write_at(&data(u8::try_from(i).unwrap(), 1 << 20), i << 20)
                .await
            {
                Ok(()) => acked.push(i),
                Err(e) => {
                    assert!(e.is_no_room(), "{e}");
                    break;
                }
            }
        }
        assert!(
            !acked.is_empty() && u64_from(acked.len()) < SIZE >> 20,
            "{acked:?}"
        );
        assert!(v.disk_full().await);
        v.flush().await.unwrap();
        let e = v.write_at(&data(99, 10), 0).await.unwrap_err();
        assert!(e.is_no_room(), "{e}");
        let e = v.zero(0, 4096).await.unwrap_err();
        assert!(e.is_no_room(), "{e}");
        for &i in &acked {
            assert_eq!(
                read(&v, i << 20, 1 << 20).await,
                data(u8::try_from(i).unwrap(), 1 << 20)
            );
        }

        fs.set_capacity(None);
        tokio::time::advance(Duration::from_secs(1)).await;
        v.write_at(&data(100, 10), SIZE - 10).await.unwrap();
        assert!(!v.disk_full().await);
        v.checkpoint().await.unwrap();
        v.close().await;

        let other = Volume::open(&ports, host(&ports, "/b").await, Path::new("/b"), "main")
            .await
            .unwrap();
        for &i in &acked {
            assert_eq!(
                read(&other, i << 20, 1 << 20).await,
                data(u8::try_from(i).unwrap(), 1 << 20)
            );
        }
        assert_eq!(read(&other, SIZE - 10, 10).await, data(100, 10));
        other.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_flush_that_fills_the_disk_survives_a_crash_before_any_upload() {
        let (ports, store, fs) = sim::ports(4);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        store.refuse(StoreOp::Create);
        let n = 8;
        for i in 0..n {
            v.write_at(&data(i, 1 << 20), u64::from(i) << 20).await.unwrap();
        }
        let big = u64::MAX / 2;
        fs.set_capacity(Some(big));
        let used = big - fs.available(Path::new("/a")).await.unwrap();
        fs.set_capacity(Some(used + (3 << 20) + 12_345));
        v.flush().await.unwrap();
        assert!(v.disk_full().await, "the flush took the reserve");
        let _ = v.upload_stopped().await;
        v.close().await;
        drop(v);

        crash(&fs);
        store.accept();
        fs.set_capacity(None);
        let after = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        for i in 0..n {
            assert_eq!(
                read(&after, u64::from(i) << 20, 1 << 20).await,
                data(i, 1 << 20),
                "flushed write {i}"
            );
        }
        after.checkpoint().await.unwrap();
        after.close().await;
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
            assert!(m.dirty.get(0).is_some_and(|d| !d.is_zero() && d.writes() == 1));
            assert!(m.dirty.get(1).is_some_and(Dirty::is_zero));
            assert!(m.dirty.get(2).is_some_and(Dirty::is_zero));
            assert!(!m.dirty.contains_key(3));
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
    async fn overlapping_small_writes_read_back_fold_and_replay() {
        let (ports, _, _) = sim::ports(11);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        let mut want = data(1, 4 * CHUNK_SIZE);
        v.write_at(&want, 0).await.unwrap();
        v.checkpoint().await.unwrap();
        let mut rng = 7u64;
        for i in 0..400u32 {
            rng = rng
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let n = 1 + usize_from(rng >> 52) % 3000;
            let off = usize_from(rng >> 20) % (want.len() - n);
            let w = data(u8::try_from(i % 251).unwrap(), n);
            v.write_at(&w, u64_from(off)).await.unwrap();
            want[off..off + n].copy_from_slice(&w);
            if i % 50 == 0 {
                assert_eq!(read(&v, 0, want.len()).await, want, "after write {i}");
            }
        }
        {
            let m = v.inner.mem.lock();
            for idx in 0..4 {
                let d = m.dirty.get(idx).unwrap();
                assert!(
                    d.writes() <= WRITES_MAX,
                    "chunk {idx} keeps {} writes",
                    d.writes()
                );
            }
        }
        assert_eq!(read(&v, 0, want.len()).await, want);
        let mut part = vec![0; 100];
        assert!(v.try_read_at(&mut part, 5000).unwrap());
        assert_eq!(part, want[5000..5100]);
        v.close().await;
        drop(v);

        let again = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        assert_eq!(
            read(&again, 0, want.len()).await,
            want,
            "the restart replays the journal"
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
    async fn writes_without_a_flush_keep_the_dirty_set_bounded_and_read_back() {
        let (ports, _, fs) = sim::ports(8);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        let step = 1 << 20;
        let w = data(5, 3 * DIRTY_MAX / 2);
        for (i, part) in w.chunks(step).enumerate() {
            v.write_at(part, u64_from(i * step)).await.unwrap();
            let bytes = v.inner.mem.lock().dirty.bytes;
            assert!(
                bytes <= DIRTY_MAX + 2 * step,
                "{bytes} dirty bytes after write {i}"
            );
        }
        assert!(v.inner.mem.lock().flush_seq > 0, "a commit bounded the dirty set");
        assert_eq!(read(&v, 0, w.len()).await, w);
        v.flush().await.unwrap();
        v.close().await;
        drop(v);

        crash(&fs);
        let again = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        assert_eq!(read(&again, 0, w.len()).await, w, "the flush kept every write");
        again.checkpoint().await.unwrap();
        again.close().await;

        let other = Volume::open(&ports, host(&ports, "/b").await, Path::new("/b"), "main")
            .await
            .unwrap();
        assert_eq!(read(&other, 0, w.len()).await, w, "the upload has every write");
        other.close().await;
    }

    #[test]
    fn dirty_limits_follow_the_memory_up_to_the_defaults() {
        assert_eq!(dirty_limits(0), (DIRTY_COMMIT, DIRTY_MAX));
        assert_eq!(dirty_limits(u64::MAX), (DIRTY_COMMIT, DIRTY_MAX));
        assert_eq!(
            dirty_limits(u64_from(4 * DIRTY_COMMIT_MIN)),
            (DIRTY_COMMIT_MIN, 2 * DIRTY_COMMIT_MIN)
        );
        assert_eq!(dirty_limits(1), (DIRTY_COMMIT_MIN, 2 * DIRTY_COMMIT_MIN));
    }

    /// The bytes of the journal files of branch `name` in `dir`.
    async fn journal_bytes(fs: &SimFs, dir: &str, name: &str) -> u64 {
        let jdir = Path::new(dir).join("journal").join(name);
        let mut n = 0;
        for e in fs.list(&jdir).await.unwrap() {
            n += fs
                .open(&jdir.join(&e.name), Open::Read)
                .await
                .unwrap()
                .size()
                .await
                .unwrap();
        }
        n
    }

    #[tokio::test(start_paused = true)]
    async fn a_dirty_memory_limit_keeps_the_journal_within_it() {
        let (ports, _, fs) = sim::ports(8);
        branch(&ports, "main").await;
        let memory = 2 << 20;
        let cache = host(&ports, "/a").await;
        cache.set_dirty_memory(u64_from(memory));
        let v = Volume::open(&ports, cache, Path::new("/a"), "main")
            .await
            .unwrap();
        let (_, most) = dirty_limits(u64_from(memory));
        assert_eq!(most, memory / 2);
        let step = 128 << 10;
        let w = data(9, 4 * DIRTY_MAX);
        let mut journal = 0;
        for (i, part) in w.chunks(step).enumerate() {
            v.write_at(part, u64_from(i * step)).await.unwrap();
            let bytes = v.inner.mem.lock().dirty.bytes;
            assert!(bytes <= most + 2 * step, "{bytes} dirty bytes after write {i}");
            journal = journal.max(journal_bytes(&fs, "/a", "main").await);
        }
        assert!(
            journal <= u64_from(2 * (most + 2 * step)),
            "the journal reached {journal} bytes under a limit of {memory}"
        );
        assert!(v.inner.mem.lock().flush_seq > 4, "commits kept the journal small");
        assert_eq!(read(&v, 0, w.len()).await, w);
        v.close().await;
        drop(v);

        let again = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        assert_eq!(
            read(&again, 0, w.len()).await,
            w,
            "a restart replays what no commit took"
        );
        again.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_flush_repeats_the_state_write_a_commit_could_not_make() {
        let (ports, _, fs) = sim::ports(10);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        v.write_at(&data(1, 100), 0).await.unwrap();
        v.checkpoint().await.unwrap();
        let state = Path::new("/a/state");
        fs.remove(state).await.unwrap();
        fs.open(state, Open::Create).await.unwrap();
        v.write_at(&data(2, 100), 0).await.unwrap();
        assert!(v.flush().await.is_err());
        assert!(v.flush().await.is_err(), "the state is still unwritten");
        fs.remove(state).await.unwrap();
        fs.create_dir_all(state).await.unwrap();
        v.flush().await.unwrap();
        assert_eq!(v.inner.mem.lock().unpersisted, None);
        v.checkpoint().await.unwrap();
        v.close().await;
        let other = Volume::open(&ports, host(&ports, "/b").await, Path::new("/b"), "main")
            .await
            .unwrap();
        assert_eq!(read(&other, 0, 100).await, data(2, 100));
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

    #[tokio::test(start_paused = true)]
    async fn sweeps_drop_old_chunk_versions_and_keep_what_any_branch_still_needs() {
        let (ports, store, fs) = sim::ports(6);
        branch(&ports, "main").await;
        branch(&ports, "other").await;
        let cache = host(&ports, "/a").await;
        store.refuse(StoreOp::Create);
        let other = Volume::open(&ports, Arc::clone(&cache), Path::new("/a"), "other")
            .await
            .unwrap();
        other.write_at(&data(9, 3 * CHUNK_SIZE), 0).await.unwrap();
        other.flush().await.unwrap();
        other.close().await;
        drop(other);
        store.accept();

        let v = Volume::open(&ports, Arc::clone(&cache), Path::new("/a"), "main")
            .await
            .unwrap();
        let chunks = 8;
        let version = |round: u8| {
            let mut b = Vec::with_capacity(chunks * CHUNK_SIZE);
            for idx in 0..u8::try_from(chunks).unwrap() {
                let seed = if idx < 4 {
                    round.wrapping_mul(8).wrapping_add(idx)
                } else {
                    round
                };
                b.extend_from_slice(&data(seed, CHUNK_SIZE));
            }
            b
        };
        for round in 0..40u8 {
            v.write_at(&version(round), 0).await.unwrap();
            v.flush().await.unwrap();
            if round % 10 == 9 {
                v.checkpoint().await.unwrap();
            }
        }
        cache.sweep().await.unwrap();
        let used = crate::local::log_allocated(&fs, Path::new("/a/objects"))
            .await
            .unwrap();
        assert!(
            used < 2 * 11 * u64_from(CHUNK_SIZE),
            "40 versions of {chunks} chunks went in; the log keeps {used} bytes"
        );
        assert_eq!(read(&v, 0, chunks * CHUNK_SIZE).await, version(39));
        v.write_at(&data(77, CHUNK_SIZE), 0).await.unwrap();
        v.flush().await.unwrap();
        drop(v);
        drop(cache);
        crash(&fs);

        let cache = host(&ports, "/a").await;
        let v = Volume::open(&ports, Arc::clone(&cache), Path::new("/a"), "main")
            .await
            .unwrap();
        let mut want = data(77, CHUNK_SIZE);
        want.extend_from_slice(&version(39)[CHUNK_SIZE..]);
        assert_eq!(read(&v, 0, chunks * CHUNK_SIZE).await, want);
        let other = Volume::open(&ports, Arc::clone(&cache), Path::new("/a"), "other")
            .await
            .unwrap();
        assert_eq!(
            read(&other, 0, 3 * CHUNK_SIZE).await,
            data(9, 3 * CHUNK_SIZE),
            "a branch closed before its upload keeps its writes"
        );
        other.checkpoint().await.unwrap();
        v.checkpoint().await.unwrap();
        other.close().await;
        v.close().await;
    }

    fn chunks(from: u8, n: u8) -> Vec<u8> {
        (from..from + n).flat_map(|i| data(i, CHUNK_SIZE)).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn a_cache_budget_keeps_the_chunks_read_last_and_fetches_the_rest_again() {
        let (ports, store, fs) = sim::ports(7);
        branch(&ports, "main").await;
        let cache = host(&ports, "/a").await;
        let v = Volume::open(&ports, Arc::clone(&cache), Path::new("/a"), "main")
            .await
            .unwrap();
        let all = chunks(0, 96);
        v.write_at(&all, 0).await.unwrap();
        v.checkpoint().await.unwrap();
        cache.sweep().await.unwrap();
        assert!(cache.log_bytes() >= 96 * u64_from(CHUNK_SIZE));

        let budget = 48 * u64_from(CHUNK_SIZE);
        cache.set_budget(budget);
        let hot = 4 * CHUNK_SIZE;
        assert_eq!(read(&v, 0, hot).await, all[..hot]);
        cache.sweep().await.unwrap();
        assert!(cache.log_bytes() <= budget, "{} bytes kept", cache.log_bytes());
        assert!(
            crate::local::log_allocated(&fs, Path::new("/a/objects"))
                .await
                .unwrap()
                <= budget + 64 * 4096
        );

        store.refuse(StoreOp::Get);
        assert_eq!(read(&v, 0, hot).await, all[..hot], "the chunks read last stay");
        store.accept();
        assert_eq!(read(&v, 0, all.len()).await, all, "the rest come from the store");
        v.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_cache_budget_never_drops_what_is_not_uploaded() {
        let (ports, store, fs) = sim::ports(8);
        branch(&ports, "main").await;
        let cache = host(&ports, "/a").await;
        cache.set_budget(4 * u64_from(CHUNK_SIZE));
        store.refuse(StoreOp::Create);
        let v = Volume::open(&ports, Arc::clone(&cache), Path::new("/a"), "main")
            .await
            .unwrap();
        let all = chunks(0, 24);
        v.write_at(&all, 0).await.unwrap();
        v.flush().await.unwrap();
        cache.sweep().await.unwrap();
        assert!(cache.log_bytes() >= 24 * u64_from(CHUNK_SIZE));
        drop(v);
        drop(cache);
        crash(&fs);

        let cache = host(&ports, "/a").await;
        cache.set_budget(4 * u64_from(CHUNK_SIZE));
        let v = Volume::open(&ports, Arc::clone(&cache), Path::new("/a"), "main")
            .await
            .unwrap();
        cache.sweep().await.unwrap();
        store.refuse(StoreOp::Get);
        assert_eq!(read(&v, 0, all.len()).await, all, "every flushed write stays");
        store.accept();
        v.checkpoint().await.unwrap();
        v.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_crash_at_any_step_of_a_sweep_within_a_budget_loses_nothing() {
        let mut steps = 0;
        loop {
            let (ports, store, fs) = sim::ports(9);
            branch(&ports, "main").await;
            let cache = host(&ports, "/a").await;
            let v = Volume::open(&ports, Arc::clone(&cache), Path::new("/a"), "main")
                .await
                .unwrap();
            let uploaded = chunks(0, 16);
            v.write_at(&uploaded, 0).await.unwrap();
            v.checkpoint().await.unwrap();
            store.refuse(StoreOp::Create);
            let pending = chunks(100, 4);
            let at = u64_from(uploaded.len());
            v.write_at(&pending, at).await.unwrap();
            v.flush().await.unwrap();
            cache.sweep().await.unwrap();

            fs.stop_after(Some(steps));
            cache.set_budget(4 * u64_from(CHUNK_SIZE));
            let swept = cache.sweep().await;
            let stopped = fs.stopped();
            drop(v);
            drop(cache);
            crash(&fs);

            let cache = host(&ports, "/a").await;
            let v = Volume::open(&ports, Arc::clone(&cache), Path::new("/a"), "main")
                .await
                .unwrap();
            assert_eq!(read(&v, 0, uploaded.len()).await, uploaded, "step {steps}");
            assert_eq!(read(&v, at, pending.len()).await, pending, "step {steps}");
            store.accept();
            v.checkpoint().await.unwrap();
            v.close().await;
            if !stopped {
                let dropped = swept.unwrap().dropped.len();
                assert!(dropped >= 12, "the sweep dropped {dropped} objects");
                break;
            }
            steps += 1;
            assert!(steps < 200, "a sweep takes fewer steps");
        }
        assert!(steps > 3, "the sweep stopped at {steps} steps");
    }

    #[test]
    fn a_state_without_a_size_generation_or_id_still_reads() {
        let s = VolState {
            root: Entry::EMPTY,
            published: Entry::EMPTY,
            publishing: Entry::EMPTY,
            jfloor: 7,
            size: Some(8 << 30),
            generation: Some(4),
            id: Some(BranchId::from_parts(3, 4)),
        };
        assert_eq!(VolState::decode(&s.encode()), Some(s));
        let no_id = VolState { id: None, ..s };
        assert_eq!(VolState::decode(&no_id.encode()), Some(no_id));
        let sized = VolState {
            generation: None,
            id: None,
            ..s
        };
        assert_eq!(VolState::decode(&sized.encode()), Some(sized));
        let old = VolState {
            size: None,
            generation: None,
            id: None,
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
        store.inject(Fault {
            op: StoreOp::Update,
            applied: false,
            permanent: false,
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
    async fn a_lasting_refusal_stops_the_uploader_and_fails_the_checkpoint() {
        let (ports, store, _) = sim::ports(5);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        v.write_at(&data(1, 100), 0).await.unwrap();
        v.checkpoint().await.unwrap();
        let published = v.published();
        store.refuse(StoreOp::Create);
        v.write_at(&data(2, 100), 0).await.unwrap();
        let refused = |e: &Error| matches!(e.kind(), Error::Store(StoreError::Permanent { .. }));
        let e = v.checkpoint().await.unwrap_err();
        assert!(refused(&e), "{e}");
        let stopped = v.upload_stopped().await;
        assert!(refused(&stopped), "{stopped}");
        store.accept();
        tokio::time::sleep(UPLOAD_EVERY * 30).await;
        assert_eq!(
            v.published(),
            published,
            "no upload ran after the uploader stopped"
        );
        let again = v.checkpoint().await.unwrap_err();
        assert!(refused(&again), "{again}");
        v.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_refusal_that_passes_does_not_stop_the_uploader() {
        let (ports, store, _) = sim::ports(6);
        branch(&ports, "main").await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        v.write_at(&data(1, 100), 0).await.unwrap();
        v.checkpoint().await.unwrap();
        for _ in 1..REFUSED_TRIES {
            store.inject(Fault {
                op: StoreOp::Update,
                applied: false,
                permanent: true,
            });
        }
        v.write_at(&data(2, 100), 0).await.unwrap();
        v.checkpoint().await.unwrap();
        assert_eq!(v.published(), v.root());
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
