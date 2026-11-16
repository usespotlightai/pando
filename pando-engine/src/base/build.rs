//! Building a base image, once per host and root.
//!
//! A build writes the chunks of the root straight from their frames into the image: each frame
//! is fetched once, and its chunks go from the decoded frame to their places without passing
//! through the cache. While it runs, the image is served by chunk hash to reads and to recipes
//! that need a chunk as a source. A chunk the fetch could not hand over (another fetch had its
//! frame, or the queue was full) is read back from the cache afterwards; recipe chunks are
//! rebuilt last, from sources in the image where it has them.
//!
//! When the host has the image of another root of the same size, and few enough chunks differ,
//! the build starts from a clone of it and writes only the chunks that differ: from blocks of
//! other bases on the host where the change list's block keys find them all, else from the
//! store.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use pando_format::{BlockKeys, CHUNK_SIZE, ChangeList, Entry, Hash, Recipe, is_zero};
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};

use super::host_blocks::{HostBlocks, Plan};
use super::{Bases, node_pack, with_suffix};
use crate::cache::{BaseKey, Cache, FetchOpts, PartialBase, Sink};
use crate::dedup::BlockMap;
use crate::flight::bounded;
use crate::local::write_durable;
use crate::ports::{CpuClass, FileLock, LocalFile, LocalFs, Open, on_cpu};
use crate::tree::Tree;
use crate::{Error, Result};

const CHUNK: u64 = CHUNK_SIZE as u64;
/// A base is seeded from another only when at most a quarter of its chunks differ; past that,
/// the build from the frames is as fast.
const SEED_SHARE: u64 = 4;
/// Fetched chunks waiting for the writers take at most this much memory.
const QUEUE_MAX: usize = 256 << 20;
const WRITERS: usize = 16;
/// The build starts the write-back of the image each time it wrote this much more.
const WRITEBACK_STEP: u64 = 64 << 20;
/// A build waits at most this long for the first reads of an attach to go first.
const FOREGROUND_FIRST_MAX: Duration = Duration::from_secs(2);

pub(crate) struct BuildOptions {
    /// Segments fetched and recipe chunks rebuilt at once.
    pub(crate) parallel: usize,
    /// Keep serving the image's chunks by hash after the build, until a handoff stops it.
    pub(crate) serve: bool,
    /// When set, chunks are fetched and written only once it turns true (or after
    /// [`FOREGROUND_FIRST_MAX`]): an attach sets it when its first reads are local, so the
    /// build does not take the link from them.
    pub(crate) after: Option<watch::Receiver<bool>>,
}

/// What one base build did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BuildReport {
    /// False when the image was there already.
    pub built: bool,
    /// The root of the image the build started from.
    pub seed: Option<Hash>,
    /// Distinct chunks written, each at every place it has.
    pub chunks: usize,
    /// Frames fetched for them.
    pub frames: usize,
    /// Chunks written straight from their decoded frames.
    pub from_fetch: usize,
    /// Chunks read back from the cache after the fetch.
    pub read_back: usize,
    /// Chunks built from blocks of other bases on this host.
    pub from_local: usize,
    /// Chunks rebuilt from their recipes.
    pub recipes: usize,
    /// Chunks the new root emptied, punched out of the seed's clone.
    pub holes: usize,
}

async fn lock(fs: &dyn LocalFs, path: &Path) -> io::Result<Box<dyn FileLock>> {
    loop {
        match fs.lock(path).await {
            Err(e) if e.kind() == io::ErrorKind::ResourceBusy => {
                tokio::time::sleep(Duration::from_millis(50)).await
            }
            r => return r,
        }
    }
}

async fn wait_foreground(after: &Option<watch::Receiver<bool>>) {
    if let Some(rx) = after {
        let mut rx = rx.clone();
        let _ = tokio::time::timeout(FOREGROUND_FIRST_MAX, rx.wait_for(|v| *v)).await;
    }
}

/// Builds the image of `root` (`size` bytes) unless this host has it.
pub(crate) async fn build(
    bases: &Bases,
    cache: &Arc<Cache>,
    tree: &Tree,
    root: Entry,
    size: u64,
    opts: BuildOptions,
) -> Result<BuildReport> {
    let path = bases.path(&root.hash);
    if bases.exists(&path).await {
        return Ok(BuildReport::default());
    }
    bases.fs.create_dir_all(&bases.dir).await?;
    let _lock = lock(bases.fs.as_ref(), &with_suffix(&path, ".lock")).await?;
    if bases.exists(&path).await {
        return Ok(BuildReport::default());
    }
    let _hold = cache.hold_writeback();
    let tmp = with_suffix(&path, ".tmp");
    let key = BaseKey::Building(root.hash);
    let mut done = None;
    if let Some((seed, seed_root)) = bases.nearest(size, &root.hash).await {
        let _ = bases.fs.remove(&tmp).await;
        match seeded(bases, cache, tree, root, size, &tmp, &seed, seed_root, &opts).await {
            Ok(d) => done = Some(d),
            Err(_) => cache.stop_serving_base(key),
        }
    }
    let (mut report, blocks, file) = match done {
        Some(d) => d,
        None => {
            let _ = bases.fs.remove(&tmp).await;
            let file = bases.fs.open(&tmp, Open::Create).await?;
            file.set_len(size).await?;
            let _ = bases.image.prepare(&tmp, None).await;
            match from_frames(bases, cache, tree, root, size, file.clone(), &opts).await {
                Ok((r, b)) => (r, b, file),
                Err(e) => {
                    cache.stop_serving_base(key);
                    let _ = bases.fs.remove(&tmp).await;
                    return Err(e);
                }
            }
        }
    };
    if let Err(e) = save(bases, tree, &root, file.as_ref(), &tmp, &path, &blocks).await {
        cache.stop_serving_base(key);
        let _ = bases.fs.remove(&tmp).await;
        return Err(e);
    }
    if !opts.serve {
        cache.stop_serving_base(key);
    }
    report.built = true;
    Ok(report)
}

/// Makes the image at `tmp` durable with its block map and node pack, then moves it to `path`.
async fn save(
    bases: &Bases,
    tree: &Tree,
    root: &Entry,
    file: &dyn LocalFile,
    tmp: &Path,
    path: &Path,
    blocks: &BlockMap,
) -> Result<()> {
    file.sync_data().await?;
    blocks.save(bases.fs.as_ref(), &Bases::blocks_path(path)).await?;
    let nodes = tree.nodes_of(root).await?;
    write_durable(
        bases.fs.as_ref(),
        &Bases::nodes_path(path),
        node_pack::encode(&nodes),
    )
    .await?;
    bases.fs.rename(tmp, path).await?;
    bases.fs.sync_dir(&bases.dir).await?;
    bases.touch(path).await?;
    Ok(())
}

async fn from_frames(
    bases: &Bases,
    cache: &Arc<Cache>,
    tree: &Tree,
    root: Entry,
    size: u64,
    file: Arc<dyn LocalFile>,
    opts: &BuildOptions,
) -> Result<(BuildReport, BlockMap)> {
    tree.prefetch_nodes(&root, opts.parallel).await?;
    let mut places: HashMap<Hash, Vec<u64>> = HashMap::new();
    let mut order = Vec::new();
    let mut pending = None;
    tree.leaves(&root, |idx, e| {
        if idx * CHUNK >= size {
            return;
        }
        if e.loc.is_pending() {
            pending.get_or_insert(idx);
            return;
        }
        places
            .entry(e.hash)
            .or_insert_with(|| {
                order.push(e);
                Vec::new()
            })
            .push(idx);
    })
    .await?;
    if let Some(idx) = pending {
        return Err(Error::Corrupt(format!(
            "chunk {idx} of root {} is not uploaded",
            root.hash.short()
        )));
    }
    let fill = Fill::new(
        bases,
        cache,
        file,
        size,
        places,
        BlockMap::default(),
        opts.parallel,
    );
    cache.serve_base(BaseKey::Building(root.hash), fill.built.clone());
    wait_foreground(&opts.after).await;
    let report = fill.run(&order, None).await?;
    Ok((report, fill.blocks.into_inner().expect("blocks lock")))
}

#[allow(clippy::too_many_arguments, reason = "one build's inputs, each used once")]
async fn seeded(
    bases: &Bases,
    cache: &Arc<Cache>,
    tree: &Tree,
    root: Entry,
    size: u64,
    tmp: &Path,
    seed: &Path,
    seed_root: Hash,
    opts: &BuildOptions,
) -> Result<(BuildReport, BlockMap, Arc<dyn LocalFile>)> {
    bases.image.clone_file(seed, tmp).await?;
    let file = bases.fs.open(tmp, Open::ReadWrite).await?;
    bases.load_nodes(tree, seed).await?;
    let from = Entry {
        hash: seed_root,
        ..Entry::EMPTY
    };
    let seed_file = bases.fs.open(seed, Open::Read).await?;
    let mut seen = HashSet::new();
    let mut seed_places = Vec::new();
    tree.leaves(&from, |idx, e| {
        if idx * CHUNK < size && seen.insert(e.hash) {
            seed_places.push((e.hash, idx));
        }
    })
    .await?;
    let served = Arc::new(PartialBase::new(seed_file.clone(), seed_places.iter().copied()));
    for (h, idx) in &seed_places {
        served.written(h, CHUNK.min(size - idx * CHUNK) as usize);
    }
    let key = BaseKey::Seed(seed_root);
    cache.serve_base(key, served);
    let r = seed_into(
        bases, cache, tree, root, size, tmp, seed, seed_root, file, seed_file, opts,
    )
    .await;
    cache.stop_serving_base(key);
    r
}

#[allow(clippy::too_many_arguments, reason = "one build's inputs, each used once")]
async fn seed_into(
    bases: &Bases,
    cache: &Arc<Cache>,
    tree: &Tree,
    root: Entry,
    size: u64,
    tmp: &Path,
    seed: &Path,
    seed_root: Hash,
    file: Arc<dyn LocalFile>,
    seed_file: Arc<dyn LocalFile>,
    opts: &BuildOptions,
) -> Result<(BuildReport, BlockMap, Arc<dyn LocalFile>)> {
    let from = Entry {
        hash: seed_root,
        ..Entry::EMPTY
    };
    let listed = match cache.remote().get_change_list(root.hash, seed_root).await {
        Ok(Some(b)) => match ChangeList::decode(&b) {
            Ok(cl) => tree.apply_change_list(&from, &root, cl).await.ok(),
            Err(_) => None,
        },
        _ => None,
    };
    let changes: Vec<(u64, Entry, Option<BlockKeys>)> = match listed {
        Some(cs) => cs.into_iter().map(|c| (c.idx, c.entry, c.keys)).collect(),
        None => tree
            .changed_leaves(&from, &root, opts.parallel)
            .await?
            .into_iter()
            .map(|(i, e)| (i, e, None))
            .collect(),
    };
    if changes.len() as u64 * SEED_SHARE > size.div_ceil(CHUNK) {
        return Err(Error::Corrupt(format!(
            "{} chunks differ from the seed base",
            changes.len()
        )));
    }
    let mut blocks = BlockMap::load(bases.fs.as_ref(), &Bases::blocks_path(seed))
        .await?
        .ok_or_else(|| Error::Corrupt(format!("{} has no block map", seed.display())))?;
    let mut host = bases.other_blocks(size, &[seed_root, root.hash]).await;
    host.add_first(seed_file, blocks.clone());
    let stale: HashSet<u64> = changes.iter().map(|c| c.0).collect();
    blocks.drop_chunks(|c| stale.contains(&c));

    let mut places: HashMap<Hash, Vec<u64>> = HashMap::new();
    let mut holes = Vec::new();
    let mut planned: HashMap<Hash, (Entry, Plan)> = HashMap::new();
    let mut fetch = Vec::new();
    for (idx, e, keys) in &changes {
        if idx * CHUNK >= size {
            continue;
        }
        if e.is_empty() {
            holes.push(*idx);
            continue;
        }
        let first = !places.contains_key(&e.hash);
        places.entry(e.hash).or_default().push(*idx);
        if !first {
            continue;
        }
        let plan = match keys {
            Some(k) if host.len() >= 2 && (idx + 1) * CHUNK <= size => host.plan(k),
            _ => None,
        };
        match plan {
            Some(p) => {
                planned.insert(e.hash, (*e, p));
            }
            None => fetch.push(*e),
        }
    }
    let fill = Fill::new(bases, cache, file.clone(), size, places, blocks, opts.parallel);
    cache.serve_base(BaseKey::Building(root.hash), fill.built.clone());
    wait_foreground(&opts.after).await;
    for idx in &holes {
        let n = CHUNK.min(size - idx * CHUNK);
        bases.image.punch_hole(tmp, idx * CHUNK, n).await?;
    }
    let local: Vec<(Entry, Plan)> = planned.into_values().collect();
    let mut report = fill.run(&fetch, Some((&host, &local))).await?;
    report.seed = Some(seed_root);
    report.holes = holes.len();
    Ok((report, fill.blocks.into_inner().expect("blocks lock"), file))
}

/// Writes the chunks of `places` into one image.
struct Fill<'a> {
    bases: &'a Bases,
    cache: &'a Arc<Cache>,
    file: Arc<dyn LocalFile>,
    size: u64,
    places: HashMap<Hash, Vec<u64>>,
    claimed: HashMap<Hash, AtomicBool>,
    built: Arc<PartialBase>,
    blocks: Mutex<BlockMap>,
    written: AtomicU64,
    parallel: usize,
}

struct FillSink<'f, 'a> {
    fill: &'f Fill<'a>,
    queue: mpsc::UnboundedSender<(Hash, Bytes)>,
    queued: &'f AtomicUsize,
    recipes: &'f Mutex<HashMap<Hash, Bytes>>,
}

impl Sink for FillSink<'_, '_> {
    fn object(&self, h: &Hash, b: &Bytes) -> bool {
        if !self.fill.wants(h) {
            return true;
        }
        let n = b.len();
        if self.queued.fetch_add(n, Ordering::AcqRel) + n > QUEUE_MAX {
            self.queued.fetch_sub(n, Ordering::AcqRel);
            return false;
        }
        if !self.fill.claim(h) || self.queue.send((*h, b.clone())).is_err() {
            self.queued.fetch_sub(n, Ordering::AcqRel);
        }
        true
    }

    fn recipe(&self, h: &Hash, enc: &Bytes) -> bool {
        if !self.fill.wants(h) {
            return true;
        }
        self.recipes
            .lock()
            .expect("recipes lock")
            .entry(*h)
            .or_insert_with(|| enc.clone());
        true
    }
}

impl<'a> Fill<'a> {
    fn new(
        bases: &'a Bases,
        cache: &'a Arc<Cache>,
        file: Arc<dyn LocalFile>,
        size: u64,
        places: HashMap<Hash, Vec<u64>>,
        blocks: BlockMap,
        parallel: usize,
    ) -> Fill<'a> {
        let built = Arc::new(PartialBase::new(
            file.clone(),
            places.iter().map(|(h, at)| (*h, at[0])),
        ));
        let claimed = places.keys().map(|h| (*h, AtomicBool::new(false))).collect();
        Fill {
            bases,
            cache,
            file,
            size,
            places,
            claimed,
            built,
            blocks: Mutex::new(blocks),
            written: AtomicU64::new(0),
            parallel: parallel.max(1),
        }
    }

    fn wants(&self, h: &Hash) -> bool {
        self.claimed.get(h).is_some_and(|c| !c.load(Ordering::Acquire))
    }

    fn claim(&self, h: &Hash) -> bool {
        self.claimed
            .get(h)
            .is_some_and(|c| !c.swap(true, Ordering::AcqRel))
    }

    /// Writes chunk `h` at every place it has.
    async fn put(&self, h: &Hash, b: &Bytes) -> Result<()> {
        let mut local = BlockMap::default();
        for (i, idx) in self.places[h].iter().enumerate() {
            let off = idx * CHUNK;
            let n = CHUNK.min(self.size - off) as usize;
            let part = if b.len() >= n {
                b.slice(..n)
            } else {
                let mut v = b.to_vec();
                v.resize(n, 0);
                Bytes::from(v)
            };
            self.file.write_at(vec![part.clone()], off).await?;
            if i == 0 {
                self.built.written(h, b.len().min(n));
            }
            if !is_zero(&part) {
                local.add_chunk(*idx, &part);
            }
            let w = self.written.fetch_add(n as u64, Ordering::Relaxed) + n as u64;
            if w / WRITEBACK_STEP != (w - n as u64) / WRITEBACK_STEP {
                self.file.start_writeback().await;
            }
        }
        self.blocks.lock().expect("blocks lock").merge(local);
        Ok(())
    }

    /// The bytes of chunk `h` once this build wrote it.
    async fn written_chunk(&self, h: &Hash) -> Option<Bytes> {
        if !self.built.has(h) {
            return None;
        }
        let off = self.places.get(h)?[0] * CHUNK;
        let n = CHUNK.min(self.size - off) as usize;
        self.file.read_at(n, off).await.ok().filter(|b| b.len() == n)
    }

    /// Fetches `fetch` into the image, builds `local` from blocks on this host, then reads back
    /// what the fetch did not hand over and rebuilds the recipe chunks.
    async fn run(
        &self,
        fetch: &[Entry],
        local: Option<(&HostBlocks, &[(Entry, Plan)])>,
    ) -> Result<BuildReport> {
        let mut report = BuildReport {
            chunks: self.places.len(),
            ..BuildReport::default()
        };
        let queued = AtomicUsize::new(0);
        let recipes: Mutex<HashMap<Hash, Bytes>> = Mutex::default();
        let (tx, rx) = mpsc::unbounded_channel::<(Hash, Bytes)>();
        let rx = AsyncMutex::new(rx);
        let from_fetch = AtomicUsize::new(0);
        let from_local = AtomicUsize::new(0);
        let fetching = async {
            let sink = FillSink {
                fill: self,
                queue: tx,
                queued: &queued,
                recipes: &recipes,
            };
            let opts = FetchOpts {
                parallel: self.parallel,
                sink: Some(&sink),
                ..FetchOpts::default()
            };
            self.cache.fetch_with(fetch, &opts).await
        };
        let writing = async {
            let mut ws = Vec::with_capacity(WRITERS);
            for _ in 0..WRITERS {
                ws.push(async {
                    loop {
                        let next = rx.lock().await.recv().await;
                        let Some((h, b)) = next else {
                            return Ok::<_, Error>(());
                        };
                        queued.fetch_sub(b.len(), Ordering::AcqRel);
                        self.put(&h, &b).await?;
                        from_fetch.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
            futures::future::try_join_all(ws).await.map(drop)
        };
        let building = async {
            let Some((host, plans)) = local else { return Ok(()) };
            let from_local = &from_local;
            let mut futs = Vec::with_capacity(plans.len());
            for (e, plan) in plans {
                futs.push(async move {
                    let Ok(b) = host.build(plan).await else {
                        return Ok(());
                    };
                    let want = e.hash;
                    let check = b.clone();
                    let good = on_cpu(self.bases.cpu.as_ref(), CpuClass::Background, move || {
                        Hash::of(&check) == want
                    })
                    .await;
                    if good && self.claim(&e.hash) {
                        self.put(&e.hash, &b).await?;
                        from_local.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok::<_, Error>(())
                });
            }
            bounded(futs, self.parallel)
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
                .map(drop)
        };
        let (frames, wrote, built) = futures::join!(fetching, writing, building);
        wrote?;
        built?;
        report.frames = frames;
        report.from_fetch = from_fetch.into_inner();
        report.from_local = from_local.into_inner();

        let all: Vec<Entry> = fetch
            .iter()
            .copied()
            .chain(local.into_iter().flat_map(|(_, p)| p.iter().map(|(e, _)| *e)))
            .collect();
        let mut futs = Vec::new();
        for e in &all {
            if self.claim(&e.hash) {
                futs.push(async move {
                    let b = self.cache.get_background(e).await?;
                    self.put(&e.hash, &b).await
                });
            }
        }
        report.read_back = futs.len();
        bounded(futs, self.parallel)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let recipes = recipes.into_inner().expect("recipes lock");
        report.recipes = self.rebuild(&recipes).await?;
        for h in self.places.keys() {
            if !self.built.has(h) {
                return Err(Error::Corrupt(format!("chunk {} is not in its frame", h.short())));
            }
        }
        Ok(report)
    }

    /// Rebuilds the recipe chunks no frame held as themselves. A source this build wrote is read
    /// from the image; the others are fetched first, as one fetch.
    async fn rebuild(&self, recipes: &HashMap<Hash, Bytes>) -> Result<usize> {
        let todo: Vec<(&Hash, &Bytes)> = recipes.iter().filter(|(h, _)| self.wants(h)).collect();
        if todo.is_empty() {
            return Ok(0);
        }
        let mut missing = Vec::new();
        let mut seen = HashSet::new();
        for (_, enc) in &todo {
            for s in Recipe::sources_of(enc) {
                if !self.built.has(&s.hash) && seen.insert(s.hash) {
                    missing.push(s);
                }
            }
        }
        self.cache.fetch_entries(&missing, self.parallel).await;
        let mut futs = Vec::with_capacity(todo.len());
        for (h, enc) in todo {
            futs.push(async move {
                let mut got = HashMap::new();
                for s in Recipe::sources_of(enc) {
                    if got.contains_key(&s.hash) {
                        continue;
                    }
                    let b = match self.written_chunk(&s.hash).await {
                        Some(b) => b,
                        None => self.cache.get_background(&s).await?,
                    };
                    got.insert(s.hash, b);
                }
                let (h, enc) = (*h, enc.clone());
                let b = on_cpu(self.bases.cpu.as_ref(), CpuClass::Background, move || {
                    let r = Recipe::parse(&enc)
                        .map_err(|why| Error::Corrupt(format!("recipe for {}: {why}", h.short())))?;
                    r.rebuild(h, |s| got.get(&s.hash).cloned().ok_or("not fetched"))
                        .map(|v| v.into_bytes())
                        .map_err(|e| Error::Corrupt(e.to_string()))
                })
                .await?;
                if self.claim(&h) {
                    self.put(&h, &b).await?;
                }
                Ok::<_, Error>(())
            });
        }
        let n = futs.len();
        bounded(futs, self.parallel)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        Ok(n)
    }
}
