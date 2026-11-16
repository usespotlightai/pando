//! A volume: one writable branch, with write-back to the store.
//!
//! A flush commits locally: chunks and nodes go to the cache, one sync, then the local root is
//! recorded. An uploader packs pending objects into segments, uploads them and moves the
//! branch's ref with a conditional update. A lost host loses only what was not uploaded yet.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use pando_format::{BlockKeys, CHUNK_SIZE, Entry, Hash, Loc, Ref, Writer, WriterId, is_zero};
use tokio::sync::{Mutex as AsyncMutex, Notify, RwLock, watch};
use tokio::task::JoinHandle;

use crate::base::{BaseView, Bases};
use crate::cache::Cache;
use crate::dedup::Dedup;
use crate::dedup::learned::Learner;
use crate::flight::bounded;
use crate::local::{Journal, read_file, write_durable};
use crate::packer::{PackStats, Packer, SEG_MAX};
use crate::ports::{Cpu, CpuClass, LocalFs, Version, on_cpu};
use crate::remote::Remote;
use crate::tree::Tree;
use crate::{Error, Ports, Result};

const STRIPES: usize = 64;
const CHUNK: u64 = CHUNK_SIZE as u64;
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

#[derive(Debug, Default)]
struct Mem {
    root: Entry,
    dirty: HashMap<u64, Vec<u8>>,
    /// Every chunk written since the branch opened, so not the base image's any more.
    touched: HashSet<u64>,
    flushing: Option<Arc<HashMap<u64, Bytes>>>,
    jfloor: u64,
}

#[derive(Debug)]
struct Up {
    version: Version,
    published: Entry,
    uploaded: Entry,
    last_from: Hash,
    publishing: Entry,
    known: Arc<HashMap<Hash, Loc>>,
    fenced: bool,
    gen_taken: bool,
    error: Option<Arc<Error>>,
    last: Option<PackStats>,
}

#[derive(Debug)]
struct Inner {
    name: String,
    size: u64,
    generation: u64,
    from: Option<String>,
    remote: Remote,
    cache: Arc<Cache>,
    tree: Arc<Tree>,
    fs: Arc<dyn LocalFs>,
    cpu: Arc<dyn Cpu>,
    writer: Arc<Writer>,
    journal: Journal,
    state_path: PathBuf,
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
}

/// One writable branch.
#[derive(Debug)]
pub struct Volume {
    inner: Arc<Inner>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct VolState {
    root: Entry,
    published: Entry,
    publishing: Entry,
    jfloor: u64,
}

impl VolState {
    fn encode(&self) -> Bytes {
        Bytes::from(format!(
            "{}\n{}\n{}\n{}\n",
            self.root, self.published, self.publishing, self.jfloor
        ))
    }

    fn decode(b: &[u8]) -> Option<VolState> {
        let s = std::str::from_utf8(b).ok()?;
        let lines: Vec<&str> = s.trim().split('\n').collect();
        let [root, published, publishing, jfloor] = lines.as_slice() else {
            return None;
        };
        Some(VolState {
            root: Entry::parse_text(root).ok()?,
            published: Entry::parse_text(published).ok()?,
            publishing: Entry::parse_text(publishing).ok()?,
            jfloor: jfloor.parse().ok()?,
        })
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
        };
        Remote::new(ports.store.clone())
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
        let remote = cache.remote().clone();
        let (r, version) = remote
            .get_ref(name)
            .await?
            .ok_or_else(|| Error::Corrupt(format!("open {name}: no such branch")))?;
        let generation = r.generation + 1;
        let journal = Journal::open(ports.fs.clone(), &dir.join("journal").join(name)).await?;
        let state_path = dir.join("state").join(name);
        let state = read_file(ports.fs.as_ref(), &state_path)
            .await?
            .and_then(|b| VolState::decode(&b));
        let resume = state.filter(|s| s.published.hash == r.root.hash || s.publishing.hash == r.root.hash);
        let mem = Mem {
            root: resume.map_or(r.root, |s| s.root),
            jfloor: resume.map_or(0, |s| s.jfloor),
            ..Mem::default()
        };
        let inner = Arc::new(Inner {
            name: name.to_owned(),
            size: r.size,
            generation,
            from: r.from.clone(),
            remote,
            tree: Arc::new(Tree::new(cache.clone(), r.size.div_ceil(CHUNK))),
            cache,
            fs: ports.fs.clone(),
            cpu: ports.cpu.clone(),
            writer: Arc::new(Writer::new(WriterId::new(name, generation), ports.rng.next_u32())),
            journal,
            state_path,
            opened: mem.root,
            stripes: (0..STRIPES).map(|_| RwLock::new(())).collect(),
            mem: Mutex::new(mem),
            commit: AsyncMutex::new(()),
            up: Mutex::new(Up {
                version: version.clone(),
                published: r.root,
                uploaded: r.root,
                last_from: Hash::ZERO,
                publishing: Entry::EMPTY,
                known: Arc::default(),
                fenced: false,
                gen_taken: false,
                error: None,
                last: None,
            }),
            changed: watch::Sender::new(0),
            kick: Notify::new(),
            stop: watch::Sender::new(false),
            dedup: Mutex::default(),
            learner: Mutex::default(),
            chunk_keys: Mutex::default(),
            base: Mutex::default(),
        });
        if let Some(s) = resume {
            let mut writes = Vec::new();
            inner
                .journal
                .replay(s.jfloor, |idx, at, data| {
                    writes.push((idx, at, data));
                    Ok(())
                })
                .await?;
            for (idx, at, data) in writes {
                inner.write_chunk(idx, at, &data, false).await?;
            }
            let next = inner.journal.last().await?.max(s.jfloor) + 1;
            inner.journal.start(next).await?;
        } else {
            inner.journal.reset().await?;
            inner.journal.start(0).await?;
            inner.persist().await?;
        }
        let taker = tokio::spawn(inner.clone().take_generation(r, version));
        let uploader = tokio::spawn(inner.clone().uploader());
        let uploaded = inner.up.lock().expect("up lock").uploaded.hash;
        if inner.root().hash != uploaded {
            inner.kick.notify_one();
        }
        Ok(Volume {
            inner,
            tasks: Mutex::new(vec![taker, uploader]),
        })
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub(crate) fn tree(&self) -> &Arc<Tree> {
        &self.inner.tree
    }

    /// The root the branch opened at.
    pub(crate) fn opened(&self) -> Entry {
        self.inner.opened
    }

    /// Lets later uploads send recipes of the blocks `dedup` finds.
    pub(crate) fn set_dedup(&self, dedup: Option<Arc<Dedup>>) {
        *self.inner.dedup.lock().expect("dedup lock") = dedup;
    }

    pub(crate) fn dedup(&self) -> Option<Arc<Dedup>> {
        self.inner.dedup.lock().expect("dedup lock").clone()
    }

    /// Keeps what later uploads send as themselves for later runs on this host.
    pub(crate) fn set_learner(&self, learner: Option<Arc<Learner>>) {
        *self.inner.learner.lock().expect("learner lock") = learner;
    }

    /// Reads unchanged chunks from the base image of the root the branch opened at, when this
    /// host has it. Returns whether it has.
    pub(crate) async fn use_base(&self, bases: &Bases) -> Result<bool> {
        let opened = self.inner.opened.hash;
        if !bases.has(&opened).await {
            return Ok(false);
        }
        let view = BaseView::open(self.inner.fs.as_ref(), &bases.path(&opened)).await?;
        *self.inner.base.lock().expect("base lock") = Some(Arc::new(view));
        Ok(true)
    }

    /// Stops reading from the base image; returns the bytes reads took from it.
    pub(crate) fn drop_base(&self) -> u64 {
        self.inner
            .base
            .lock()
            .expect("base lock")
            .take()
            .map_or(0, |b| b.served())
    }

    pub fn size(&self) -> u64 {
        self.inner.size
    }

    /// The current local root.
    pub fn root(&self) -> Entry {
        self.inner.root()
    }

    /// The root the store has for this branch, as this writer last published it.
    pub fn published(&self) -> Entry {
        self.inner.up.lock().expect("up lock").published
    }

    /// Waits until the writer generation is taken; an error when another writer took it.
    pub async fn wait_generation(&self) -> Result<()> {
        let mut rx = self.inner.changed.subscribe();
        loop {
            {
                let up = self.inner.up.lock().expect("up lock");
                if up.gen_taken {
                    return match (&up.error, up.fenced) {
                        (Some(e), true) => Err(Error::Shared(e.clone())),
                        _ => Ok(()),
                    };
                }
            }
            if rx.changed().await.is_err() {
                return Err(Error::Cancelled);
            }
        }
    }

    pub async fn read_at(&self, p: &mut [u8], off: u64) -> Result<()> {
        if off + p.len() as u64 > self.inner.size {
            return Err(Error::BeyondEnd);
        }
        let parts = parts(off, p.len());
        let mut futs = Vec::with_capacity(parts.len());
        for &(idx, at, _, n) in &parts {
            futs.push(self.inner.read_chunk(idx, at, n));
        }
        let got = futures::future::join_all(futs).await;
        for ((_, _, pos, n), b) in parts.into_iter().zip(got) {
            p[pos..pos + n].copy_from_slice(&b?);
        }
        Ok(())
    }

    /// Reads only from memory and the page cache, without waiting: `false` when it would wait.
    pub fn try_read_at(&self, p: &mut [u8], off: u64) -> Result<bool> {
        if off + p.len() as u64 > self.inner.size {
            return Err(Error::BeyondEnd);
        }
        for (idx, at, pos, n) in parts(off, p.len()) {
            if !self.inner.try_read_chunk(idx, at, &mut p[pos..pos + n])? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub async fn write_at(&self, p: &[u8], off: u64) -> Result<()> {
        if off + p.len() as u64 > self.inner.size {
            return Err(Error::BeyondEnd);
        }
        for (idx, at, pos, n) in parts(off, p.len()) {
            self.inner.write_chunk(idx, at, &p[pos..pos + n], true).await?;
        }
        Ok(())
    }

    /// Clears a range. Whole chunks that hold no data stay sparse.
    pub async fn zero(&self, off: u64, len: u64) -> Result<()> {
        if off + len > self.inner.size {
            return Err(Error::BeyondEnd);
        }
        let zeros = vec![0; CHUNK_SIZE];
        let mut pos = off;
        while pos < off + len {
            let (idx, at) = (pos / CHUNK, (pos % CHUNK) as usize);
            let n = (CHUNK_SIZE - at).min((off + len - pos) as usize);
            if n == CHUNK_SIZE && self.inner.is_sparse(idx).await? {
                pos += n as u64;
                continue;
            }
            self.inner.write_chunk(idx, at, &zeros[..n], true).await?;
            pos += n as u64;
        }
        Ok(())
    }

    /// Commits completed writes locally and durably, then nudges the uploader.
    pub async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }

    /// Flushes and waits until everything is published in the store.
    pub async fn checkpoint(&self) -> Result<()> {
        self.inner.flush().await?;
        let target = self.inner.root().hash;
        let mut rx = self.inner.changed.subscribe();
        self.inner.kick.notify_one();
        loop {
            {
                let up = self.inner.up.lock().expect("up lock");
                if up.last_from == target || up.uploaded.hash == target {
                    return Ok(());
                }
                if let Some(e) = &up.error {
                    return Err(Error::Shared(e.clone()));
                }
            }
            if rx.changed().await.is_err() {
                return Err(Error::Cancelled);
            }
        }
    }

    /// What the last upload packed.
    pub fn last_upload(&self) -> Option<PackStats> {
        self.inner.up.lock().expect("up lock").last
    }

    /// Stops the uploader after any upload in flight finishes.
    pub async fn close(&self) {
        self.inner.stop.send_replace(true);
        let tasks = std::mem::take(&mut *self.tasks.lock().expect("tasks lock"));
        for t in tasks {
            let _ = t.await;
        }
    }
}

/// The chunk pieces of `len` bytes at `off`: chunk, offset in it, offset in the buffer, length.
fn parts(off: u64, len: usize) -> Vec<(u64, usize, usize, usize)> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < len {
        let at = off + pos as u64;
        let (idx, inner) = (at / CHUNK, (at % CHUNK) as usize);
        let n = (CHUNK_SIZE - inner).min(len - pos);
        out.push((idx, inner, pos, n));
        pos += n;
    }
    out
}

fn is_transient(e: &Error) -> bool {
    matches!(e.kind(), Error::Store(crate::ports::StoreError::Transient { .. }))
}

fn fail_with(up: &mut Up, e: Error) -> Arc<Error> {
    let e = match e {
        Error::Shared(e) => e,
        e => Arc::new(e),
    };
    up.error = Some(e.clone());
    e
}

impl Inner {
    fn root(&self) -> Entry {
        self.mem.lock().expect("mem lock").root
    }

    fn stripe(&self, idx: u64) -> &RwLock<()> {
        &self.stripes[(idx % STRIPES as u64) as usize]
    }

    fn notify(&self) {
        self.changed.send_modify(|n| *n += 1);
    }

    async fn persist(&self) -> Result<()> {
        let (root, jfloor) = {
            let m = self.mem.lock().expect("mem lock");
            (m.root, m.jfloor)
        };
        let (published, publishing) = {
            let u = self.up.lock().expect("up lock");
            (u.published, u.publishing)
        };
        let st = VolState {
            root,
            published,
            publishing,
            jfloor,
        };
        Ok(write_durable(self.fs.as_ref(), &self.state_path, st.encode()).await?)
    }

    async fn take_generation(self: Arc<Self>, mut r: Ref, version: Version) {
        r.generation = self.generation;
        let mut res = self.remote.put_ref(&self.name, &r, &version).await;
        for _ in 1..GENERATION_TRIES {
            if !matches!(&res, Err(e) if is_transient(e)) {
                break;
            }
            tokio::time::sleep(RETRY_AFTER).await;
            res = self.remote.put_ref(&self.name, &r, &version).await;
        }
        {
            let mut up = self.up.lock().expect("up lock");
            match res {
                Ok(v) => up.version = v,
                Err(e) => {
                    up.fenced = true;
                    fail_with(&mut up, e);
                }
            }
            up.gen_taken = true;
        }
        self.notify();
    }

    async fn read_committed(&self, idx: u64, root: &Entry, at: usize, n: usize) -> Result<Bytes> {
        let e = self.tree.lookup(root, idx, false).await?;
        if e.is_empty() {
            return Ok(Bytes::from(vec![0; n]));
        }
        self.cache.read(&e, at, n).await
    }

    async fn read_chunk(&self, idx: u64, at: usize, n: usize) -> Result<Bytes> {
        let _g = self.stripe(idx).read().await;
        let root = {
            let m = self.mem.lock().expect("mem lock");
            if let Some(d) = m.dirty.get(&idx) {
                return Ok(Bytes::copy_from_slice(&d[at..at + n]));
            }
            if let Some(f) = m.flushing.as_ref().and_then(|f| f.get(&idx)) {
                return Ok(f.slice(at..at + n));
            }
            m.root
        };
        if let Some(b) = self.read_base(idx, at, n).await {
            return Ok(b);
        }
        self.read_committed(idx, &root, at, n).await
    }

    /// The base image's bytes of chunk `idx`, when no write since open changed it.
    fn base_of(&self, idx: u64) -> Option<Arc<BaseView>> {
        if self.mem.lock().expect("mem lock").touched.contains(&idx) {
            return None;
        }
        self.base.lock().expect("base lock").clone()
    }

    async fn read_base(&self, idx: u64, at: usize, n: usize) -> Option<Bytes> {
        self.base_of(idx)?.read(idx * CHUNK + at as u64, n).await
    }

    fn try_read_chunk(&self, idx: u64, at: usize, dst: &mut [u8]) -> Result<bool> {
        let Ok(_g) = self.stripe(idx).try_read() else {
            return Ok(false);
        };
        let root = {
            let m = self.mem.lock().expect("mem lock");
            if let Some(d) = m.dirty.get(&idx) {
                dst.copy_from_slice(&d[at..at + dst.len()]);
                return Ok(true);
            }
            if let Some(f) = m.flushing.as_ref().and_then(|f| f.get(&idx)) {
                dst.copy_from_slice(&f[at..at + dst.len()]);
                return Ok(true);
            }
            m.root
        };
        if let Some(base) = self.base_of(idx)
            && base.read_nowait(idx * CHUNK + at as u64, dst)
        {
            return Ok(true);
        }
        let Some(e) = self.tree.lookup_nowait(&root, idx) else {
            return Ok(false);
        };
        if e.is_empty() {
            dst.fill(0);
            return Ok(true);
        }
        self.cache.read_nowait(&e, at, dst)
    }

    async fn is_sparse(&self, idx: u64) -> Result<bool> {
        let root = {
            let m = self.mem.lock().expect("mem lock");
            if m.dirty.contains_key(&idx) || m.flushing.as_ref().is_some_and(|f| f.contains_key(&idx)) {
                return Ok(false);
            }
            m.root
        };
        Ok(self.tree.lookup(&root, idx, false).await?.is_empty())
    }

    /// Applies one write to a chunk. The chunk's stripe lock orders writes to the same chunk,
    /// in the buffer and in the journal; writes to other chunks run in parallel. A flush takes
    /// every stripe, so no write is half applied when it takes the dirty set.
    async fn write_chunk(&self, idx: u64, at: usize, data: &[u8], journaled: bool) -> Result<()> {
        let _g = self.stripe(idx).write().await;
        let (dirty, flushing, root) = {
            let m = self.mem.lock().expect("mem lock");
            let f = m.flushing.as_ref().and_then(|f| f.get(&idx)).cloned();
            (m.dirty.contains_key(&idx), f, m.root)
        };
        let fresh = if dirty {
            None
        } else if let Some(f) = flushing {
            Some(f.to_vec())
        } else if data.len() < CHUNK_SIZE {
            let n = CHUNK.min(self.size - idx * CHUNK) as usize;
            let b = match self.read_base(idx, 0, n).await {
                Some(b) => b,
                None => self.read_committed(idx, &root, 0, CHUNK_SIZE).await?,
            };
            let mut v = b.to_vec();
            v.resize(CHUNK_SIZE, 0);
            Some(v)
        } else {
            Some(vec![0; CHUNK_SIZE])
        };
        if journaled {
            self.journal.append(idx, at, Bytes::copy_from_slice(data)).await?;
        }
        let mut m = self.mem.lock().expect("mem lock");
        m.touched.insert(idx);
        match fresh {
            None => m.dirty.get_mut(&idx).expect("dirty under the stripe lock")[at..at + data.len()]
                .copy_from_slice(data),
            Some(mut buf) => {
                buf[at..at + data.len()].copy_from_slice(data);
                m.dirty.insert(idx, buf);
            }
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let _c = self.commit.lock().await;
        if self.up.lock().expect("up lock").fenced {
            return Err(Error::Fenced {
                branch: self.name.clone(),
            });
        }
        let mut guards = Vec::with_capacity(STRIPES);
        for s in &self.stripes {
            guards.push(s.write().await);
        }
        if self.mem.lock().expect("mem lock").dirty.is_empty() {
            return Ok(());
        }
        let generation = self.journal.rotate().await?;
        let (snapshot, root) = {
            let mut m = self.mem.lock().expect("mem lock");
            let snap: HashMap<u64, Bytes> = std::mem::take(&mut m.dirty)
                .into_iter()
                .map(|(i, b)| (i, Bytes::from(b)))
                .collect();
            let snap = Arc::new(snap);
            m.flushing = Some(snap.clone());
            (snap, m.root)
        };
        drop(guards);
        match self.commit_snapshot(&snapshot, &root).await {
            Ok(new_root) => {
                {
                    let mut m = self.mem.lock().expect("mem lock");
                    m.root = new_root;
                    m.flushing = None;
                    m.jfloor = generation;
                }
                self.persist().await?;
                self.journal.drop_below(generation).await;
                self.kick.notify_one();
                Ok(())
            }
            Err(e) => {
                let mut m = self.mem.lock().expect("mem lock");
                m.flushing = None;
                for (idx, b) in snapshot.iter() {
                    m.dirty.entry(*idx).or_insert_with(|| b.to_vec());
                }
                Err(e)
            }
        }
    }

    async fn commit_snapshot(&self, snapshot: &HashMap<u64, Bytes>, root: &Entry) -> Result<Entry> {
        let mut futs = Vec::with_capacity(snapshot.len());
        for (idx, b) in snapshot {
            futs.push(self.store_chunk(*idx, b.clone()));
        }
        let mut updates = bounded(futs, STORE_PARALLEL)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        {
            let up = self.up.lock().expect("up lock");
            for (_, e) in &mut updates {
                if let Some(l) = up.known.get(&e.hash) {
                    e.loc = *l;
                }
            }
        }
        updates.sort_by_key(|u| u.0);
        let new_root = self.tree.update(root, &updates).await?;
        self.cache.sync_local().await?;
        Ok(new_root)
    }

    async fn store_chunk(&self, idx: u64, b: Bytes) -> Result<(u64, Entry)> {
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
                let up = self.up.lock().expect("up lock");
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
        let root = self.root();
        let (known, version) = {
            let up = self.up.lock().expect("up lock");
            if root.hash == up.uploaded.hash || up.fenced {
                return Ok(());
            }
            (up.known.clone(), up.version.clone())
        };
        let dedup = self.dedup.lock().expect("dedup lock").clone();
        let learner = self.learner.lock().expect("learner lock").clone();
        let mut p = Packer::new(
            self.remote.clone(),
            self.cpu.clone(),
            self.writer.clone(),
            SEG_MAX,
            known,
        )
        .with_dedup(dedup.clone(), None)
        .with_learner(learner.clone());
        let packed = match self.tree.pack(&root, &mut p).await {
            Ok(e) => e,
            Err(e) => {
                if matches!(e.kind(), Error::ChunkLost { .. }) {
                    let e = fail_with(&mut self.up.lock().expect("up lock"), e);
                    self.notify();
                    return Err(Error::Shared(e));
                }
                return Err(e);
            }
        };
        let keys = p.keys().clone();
        let (done, _) = futures::join!(p.finish(), self.put_change_list(&packed, &keys));
        done?;
        self.up.lock().expect("up lock").publishing = packed;
        self.persist().await?;
        let r = Ref {
            size: self.size,
            root: packed,
            generation: self.generation,
            from: self.from.clone(),
        };
        let new_version = match self.remote.put_ref(&self.name, &r, &version).await {
            Ok(v) => v,
            Err(e) => {
                if matches!(e.kind(), Error::Fenced { .. }) {
                    let mut up = self.up.lock().expect("up lock");
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
        if let Some(learned) = dedup.as_ref().and_then(|d| d.learned.as_ref()) {
            learned.mark_used(p.used_learned());
            if let Some(l) = &learner {
                l.published(p.learned());
                learned.add_published(l, p.learned()).await;
            }
        } else if let Some(l) = &learner {
            l.published(p.learned());
        }
        drop(p);
        {
            let mut up = self.up.lock().expect("up lock");
            Arc::make_mut(&mut up.known).extend(added);
            up.version = new_version;
            up.published = packed;
            up.publishing = Entry::EMPTY;
            up.uploaded = root;
            up.last_from = root.hash;
            up.last = Some(stats);
            let mut m = self.mem.lock().expect("mem lock");
            if m.root.hash == root.hash {
                m.root = packed;
                up.uploaded = packed;
            }
        }
        self.notify();
        let _ = self.persist().await;
        Ok(())
    }

    /// Puts the change list of `root` from the root the branch opened at, while the segments
    /// upload. Best effort: without it, another host walks the tree.
    async fn put_change_list(&self, root: &Entry, keys: &HashMap<Hash, pando_format::BlockKeys>) {
        let from = self.opened;
        if from.is_empty() || from.loc.is_pending() || from.hash == root.hash {
            return;
        }
        let earlier = self.chunk_keys.lock().expect("keys lock").clone();
        let key_of = |h: &Hash| keys.get(h).or_else(|| earlier.get(h)).copied();
        let Ok(cl) = self.tree.change_list(&from, root, LIST_PARALLEL, key_of).await else {
            return;
        };
        let kept = cl
            .changes
            .iter()
            .filter_map(|c| Some((c.entry.hash, key_of(&c.entry.hash)?)))
            .collect();
        *self.chunk_keys.lock().expect("keys lock") = kept;
        let _ = self
            .remote
            .put_change_list(root.hash, from.hash, Bytes::from(cl.encode()))
            .await;
    }
}

#[cfg(all(test, feature = "sim"))]
mod tests {
    use super::*;
    use crate::sim::{self, SimFs, SimStore};

    const SIZE: u64 = 64 << 20;

    async fn host(ports: &Ports, dir: &str) -> Arc<Cache> {
        Arc::new(Cache::open(ports, Path::new(dir)).await.unwrap())
    }

    async fn branch(ports: &Ports, name: &str) {
        Volume::create(ports, name, SIZE).await.unwrap();
    }

    fn data(seed: u8, n: usize) -> Vec<u8> {
        (0..n)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
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
        crash(&fs);
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        v.write_at(&data(9, 10), 0).await.unwrap();
        v.close().await;
        let v = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "main")
            .await
            .unwrap();
        assert_eq!(
            read(&v, 0, 10).await,
            data(9, 10),
            "the journal starts above the floor the state names"
        );
        v.close().await;
    }

    fn crash(fs: &SimFs) {
        fs.crash();
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
