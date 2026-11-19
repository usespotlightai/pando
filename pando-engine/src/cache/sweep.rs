//! Sweeps of the object log: what the live roots on this host reach stays, the rest goes.
//!
//! Each volume on the cache tells it its roots ([`Roots`]), and each operation that builds a
//! root from another holds a [`Pin`] on the root it starts from. A sweep marks every object
//! those reach, and the objects not uploaded under the state of each branch closed on this
//! host, then drops the records older than every pin that nothing marked since the oldest pin
//! began. A put of an object the log has marks it, so an operation that finds its object there
//! keeps it.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use bytes::Bytes;
use pando_format::{CHUNK_SIZE, Entry, Hash, Node, depth_for, u64_from};
use parking_lot::{Mutex, RwLock};

use super::Cache;
use crate::local::{Swept, VolState, read_file, write_durable};
use crate::ports::LocalFs;
use crate::{Error, Result};

/// A root whose objects stay on this host: a tree of `height` levels of nodes, or a chunk when
/// it is 0. Only the objects not uploaded yet, unless `all`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Root {
    pub(crate) entry: Entry,
    pub(crate) height: usize,
    pub(crate) all: bool,
}

/// A user of the cache with roots of its own.
pub(crate) trait Roots: Send + Sync {
    /// Adds the roots whose objects must stay on this host to `out`.
    fn roots(&self, out: &mut Vec<Root>);
    /// Forgets what it keeps in memory of the objects a sweep dropped.
    fn forget(&self, dropped: &HashSet<Hash>);
}

struct Held {
    /// Where the log ended and the epoch when the pin began, when it keeps what the
    /// operation appends.
    from: Option<(u64, u64)>,
    roots: Vec<Root>,
}

pub(super) struct Sweeper {
    fs: Arc<dyn LocalFs>,
    /// Records below it are kept: they were in the log before it had the list of states.
    floor: u64,
    /// Pins and roots change under its read lock; a sweep takes them under its write lock.
    gate: RwLock<()>,
    pins: Mutex<(u64, HashMap<u64, Held>)>,
    users: Mutex<Vec<(PathBuf, Weak<dyn Roots>)>>,
    states_path: PathBuf,
    states: Mutex<BTreeSet<PathBuf>>,
    states_write: tokio::sync::Mutex<()>,
    started: AtomicBool,
    running: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for Sweeper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sweeper")
            .field("states", &self.states)
            .finish_non_exhaustive()
    }
}

impl Sweeper {
    /// The sweeper of the cache in `dir`, with the branch states it was told of there. A
    /// cache without that list yet keeps every record below `end`, where its log ends: the
    /// branches that wrote them are not on it.
    pub(super) async fn open(fs: Arc<dyn LocalFs>, dir: &Path, end: u64) -> Result<Sweeper> {
        let states_path = dir.join("states");
        let corrupt = || Error::Corrupt(format!("{} does not parse", states_path.display()));
        let (floor, states, new) = match read_file(fs.as_ref(), &states_path).await? {
            Some(b) => {
                let text = std::str::from_utf8(&b).map_err(|_| corrupt())?;
                let mut lines = text.lines();
                let floor = lines
                    .next()
                    .and_then(|l| l.strip_prefix("floor "))
                    .and_then(|n| n.parse().ok())
                    .ok_or_else(corrupt)?;
                let states = lines.filter(|l| !l.is_empty()).map(PathBuf::from).collect();
                (floor, states, false)
            }
            None => (end, BTreeSet::new(), true),
        };
        let s = Sweeper {
            fs,
            floor,
            gate: RwLock::default(),
            pins: Mutex::default(),
            users: Mutex::default(),
            states_path,
            states: Mutex::new(states),
            states_write: tokio::sync::Mutex::default(),
            started: AtomicBool::new(false),
            running: tokio::sync::Mutex::default(),
        };
        if new {
            s.write_states().await?;
        }
        Ok(s)
    }

    async fn write_states(&self) -> Result<()> {
        let _w = self.states_write.lock().await;
        let mut b = format!("floor {}\n", self.floor);
        for p in self.states.lock().iter() {
            b.push_str(&p.to_string_lossy());
            b.push('\n');
        }
        Ok(write_durable(self.fs.as_ref(), &self.states_path, Bytes::from(b)).await?)
    }
}

/// Keeps what an operation appends to the log, and the objects under the roots it starts
/// from, until [`Pin::finish`] or drop.
#[derive(Debug)]
pub(crate) struct Pin {
    cache: Arc<Cache>,
    id: u64,
}

impl Pin {
    /// Runs `f`, which puts the operation's result where a [`Roots`] reports it, and ends the
    /// pin, so no sweep falls between the two.
    pub(crate) fn finish<T>(mut self, f: impl FnOnce() -> T) -> T {
        let _g = self.cache.sweeper.gate.read();
        let t = f();
        self.cache.sweeper.pins.lock().1.remove(&self.id);
        self.id = 0;
        t
    }
}

impl Drop for Pin {
    fn drop(&mut self) {
        if self.id != 0 {
            let _g = self.cache.sweeper.gate.read();
            self.cache.sweeper.pins.lock().1.remove(&self.id);
        }
    }
}

/// The root a branch's state names. A state that does not parse, or has no size, resumes
/// nothing that needs it.
fn state_root(b: &[u8]) -> Option<Root> {
    let s = VolState::decode(b)?;
    Some(Root {
        entry: s.root,
        height: depth_for(s.size?.div_ceil(u64_from(CHUNK_SIZE))),
        all: false,
    })
}

impl Cache {
    /// Runs `f`, which reads the roots an operation starts from, and pins them with what the
    /// operation appends from now on.
    pub(crate) fn pin<T>(self: &Arc<Self>, f: impl FnOnce() -> (T, Vec<Root>)) -> (T, Pin) {
        self.pin_with(f, true)
    }

    /// [`Self::pin`] of the roots only, for an operation whose appends the store gets before
    /// anything reads them from this host: an upload.
    pub(crate) fn pin_roots<T>(self: &Arc<Self>, f: impl FnOnce() -> (T, Vec<Root>)) -> (T, Pin) {
        self.pin_with(f, false)
    }

    fn pin_with<T>(self: &Arc<Self>, f: impl FnOnce() -> (T, Vec<Root>), appends: bool) -> (T, Pin) {
        let _g = self.sweeper.gate.read();
        let (t, roots) = f();
        let held = Held {
            from: appends.then(|| (self.log.end(), self.log.epoch())),
            roots,
        };
        let mut pins = self.sweeper.pins.lock();
        pins.0 += 1;
        let id = pins.0;
        pins.1.insert(id, held);
        (
            t,
            Pin {
                cache: Arc::clone(self),
                id,
            },
        )
    }

    /// Adds `roots`, a branch whose state is at `state`, to what sweeps keep. Once it is gone,
    /// sweeps keep what its state names that is not uploaded.
    pub(crate) async fn register(&self, state: &Path, roots: Weak<dyn Roots>) -> Result<()> {
        self.sweeper.users.lock().push((state.to_path_buf(), roots));
        if self.sweeper.states.lock().insert(state.to_path_buf()) {
            self.sweeper.write_states().await?;
        }
        Ok(())
    }

    /// Starts the task that sweeps the object log whenever enough was appended since the last
    /// sweep.
    pub(crate) fn start_sweeps(self: &Arc<Self>) {
        if self.sweeper.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let due = self.log.sweep_due();
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                due.notified().await;
                let Some(c) = weak.upgrade() else {
                    return;
                };
                let _ = c.sweep().await;
            }
        });
    }

    /// Drops the records of the object log that no root, pin or closed branch on this host
    /// reaches.
    pub(crate) async fn sweep(&self) -> Result<Swept> {
        let _one = self.sweeper.running.lock().await;
        let (epoch, below, keep_from, mut roots, users, closed) = {
            let _g = self.sweeper.gate.write();
            let epoch = self.log.next_epoch();
            let end = self.log.end();
            let pins = self.sweeper.pins.lock();
            let from = || pins.1.values().filter_map(|p| p.from);
            let below = from().map(|(at, _)| at).min().unwrap_or(end).min(end);
            let keep_from = from().map(|(_, e)| e).min().unwrap_or(epoch).min(epoch);
            let mut roots: Vec<Root> = pins.1.values().flat_map(|p| p.roots.iter().copied()).collect();
            drop(pins);
            let mut users = Vec::new();
            let mut open = HashSet::new();
            self.sweeper.users.lock().retain(|(path, w)| match w.upgrade() {
                Some(u) => {
                    open.insert(path.clone());
                    users.push(u);
                    true
                }
                None => false,
            });
            for u in &users {
                u.roots(&mut roots);
            }
            let closed: Vec<PathBuf> = self
                .sweeper
                .states
                .lock()
                .iter()
                .filter(|p| !open.contains(*p))
                .cloned()
                .collect();
            (epoch, below, keep_from, roots, users, closed)
        };
        let mut gone = Vec::new();
        for p in closed {
            match read_file(self.sweeper.fs.as_ref(), &p).await? {
                Some(b) => roots.extend(state_root(&b)),
                None => gone.push(p),
            }
        }
        self.mark(roots, epoch).await?;
        let swept = self.log.sweep(self.sweeper.floor, below, keep_from).await?;
        let dropped: HashSet<Hash> = swept.dropped.iter().copied().collect();
        for u in &users {
            u.forget(&dropped);
        }
        if !gone.is_empty() {
            {
                let mut s = self.sweeper.states.lock();
                let open: HashSet<PathBuf> = self
                    .sweeper
                    .users
                    .lock()
                    .iter()
                    .filter(|(_, w)| w.strong_count() > 0)
                    .map(|(p, _)| p.clone())
                    .collect();
                for p in gone.iter().filter(|p| !open.contains(*p)) {
                    s.remove(p);
                }
            }
            self.sweeper.write_states().await?;
        }
        Ok(swept)
    }

    /// Marks every object under `roots` that this host has, through the nodes it has.
    async fn mark(&self, mut stack: Vec<Root>, epoch: u64) -> Result<()> {
        let mut seen: HashMap<Hash, bool> = HashMap::new();
        while let Some(r) = stack.pop() {
            let e = r.entry;
            if e.is_empty()
                || (!r.all && !e.loc.is_pending())
                || !self.log.mark(&e.hash, epoch)
                || r.height == 0
            {
                continue;
            }
            if seen.get(&e.hash).is_some_and(|&all| all || !r.all) {
                continue;
            }
            seen.insert(e.hash, r.all);
            let Some(b) = self.log.get(&e.hash).await? else {
                continue;
            };
            let n = Node::decode(&b)?;
            stack.extend(n.children().map(|(_, c)| Root {
                entry: *c,
                height: r.height - 1,
                all: r.all,
            }));
        }
        Ok(())
    }

    /// Where the store has object `h`, when the uploads of `uploads` recorded it.
    pub(crate) fn stored(&self, h: &Hash, uploads: u64) -> Option<pando_format::Loc> {
        self.log.stored(h).and_then(|(u, l)| (u == uploads).then_some(l))
    }

    /// Records where the store has each object an upload of `uploads` put.
    #[expect(
        clippy::iter_over_hash_type,
        reason = "each object gets its own location, so the order does not matter"
    )]
    pub(crate) fn set_stored_all(&self, added: &HashMap<Hash, pando_format::Loc>, uploads: u64) {
        for (h, l) in added {
            self.log.set_stored(h, (uploads, *l));
        }
    }
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use super::*;
    use crate::remote::Remote;
    use crate::sim;

    #[tokio::test]
    async fn a_cache_from_before_the_list_of_states_keeps_its_records() {
        let (ports, _, _) = sim::ports(3);
        let open = || async {
            Arc::new(
                Cache::open_with(
                    Arc::clone(&ports.fs),
                    Arc::clone(&ports.cpu),
                    Remote::new(Arc::clone(&ports.store)),
                    Path::new("/c"),
                )
                .await
                .unwrap(),
            )
        };
        let c = open().await;
        let (old, new) = (Bytes::from_static(b"old"), Bytes::from_static(b"new"));
        c.put_local(Hash::of(&old), old.clone()).await.unwrap();
        c.sync_local().await.unwrap();
        drop(c);
        ports.fs.remove(Path::new("/c/states")).await.unwrap();

        let c = open().await;
        c.put_local(Hash::of(&new), new.clone()).await.unwrap();
        let swept = c.sweep().await.unwrap();
        assert_eq!(
            swept.dropped,
            vec![Hash::of(&new)],
            "only what came after the list goes"
        );
        assert_eq!(c.local(&Hash::of(&old)).await.unwrap(), Some(old));
        drop(c);
        let c = open().await;
        assert!(
            c.sweep().await.unwrap().dropped.is_empty(),
            "the floor stays across opens"
        );
    }
}
