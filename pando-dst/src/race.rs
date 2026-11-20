//! Hosts that race for one branch. Each opens it, flushes, publishes, detaches, stops or is lost
//! at random while the others' background tasks run, and now and then the branch is advanced to
//! an empty branch or to a copy of itself. Every open must hold a prefix of some writer's
//! flushes, every strict flush that returned since the last advance, and no flush an advance
//! dropped. A fenced writer's last write, whose flush failed, may follow its flushes: its
//! journal may keep it, and an open that resumes its state then holds it. A write whose
//! `write_at` failed is in no journal: each write has a chunk of its own, so it fails before the
//! journal append if at all. The calls on the branch's ref and log, and each write and flush,
//! are traced for `specs/WriteLogTrace.tla`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pando_engine::ports::{Rng, Store};
use pando_engine::sim::{self, SimRng};
use pando_engine::{Cache, LogMode, Ports, Volume, branch};
use pando_format::{keys, u64_from, usize_from};

use crate::trace::{self, Event, Kind, Step, TracedStore, Tracer};

const CHUNK: u64 = 64 << 10;
const FLUSHES: u64 = 40;
const BRANCH: &str = "main";
/// The hosts, by name.
pub const HOSTS: [&str; 2] = ["a", "b"];
/// The host that runs `pando advance`.
pub const ADVANCER: &str = "x";
const WAIT: Duration = Duration::from_secs(60);

/// What one race did, and its trace.
#[derive(Debug, Default)]
pub struct Race {
    /// Opens, including those another host fenced before they took the branch.
    pub opens: usize,
    /// Opens that took the branch.
    pub taken: usize,
    /// Flushes some writer committed.
    pub flushes: usize,
    /// Strict flushes that returned.
    pub strict: usize,
    /// Opens that read the log.
    pub scans: usize,
    /// Seals an open created.
    pub seals: usize,
    /// Appends a seal or another record refused.
    pub refused_appends: usize,
    /// Generation takes another writer beat.
    pub lost_takes: usize,
    /// Advances that moved the branch.
    pub advances: usize,
    /// Seals an advance created.
    pub advance_seals: usize,
    /// The trace as module `TraceData`, unless a call had no step in the spec.
    pub module: Option<String>,
    /// Why there is no module.
    pub unsupported: Option<String>,
}

struct Host {
    name: &'static str,
    store: Arc<TracedStore>,
    ports: Ports,
    lives: usize,
    open: Option<Writer>,
}

struct Writer {
    host: &'static str,
    v: Volume,
    mode: LogMode,
    /// Index into the race's histories of what this writer holds.
    history: usize,
    fenced: bool,
}

struct World {
    rng: SimRng,
    tracer: Arc<Tracer>,
    /// The store without a trace, for the branches an advance reads from.
    ports: Ports,
    size: u64,
    advancer: Arc<TracedStore>,
    advancer_ports: Ports,
    sources: usize,
    /// The flushes an advance dropped.
    dropped: BTreeSet<u64>,
    /// Each committed flush's number in the trace: the flushes the trace has, counted from 1.
    traced: BTreeMap<u64, u64>,
    /// Each writer's flushes in order, from the open it took the branch with.
    histories: Vec<Vec<u64>>,
    /// The write each history's writer could not flush, being fenced.
    unflushed: BTreeMap<usize, u64>,
    acked: BTreeSet<u64>,
    next: u64,
    race: Race,
}

impl World {
    fn below(&self, n: u64) -> u64 {
        self.rng.next_u64() % n
    }

    /// The flushes `v` holds: each flush writes its id at the start of its own chunk.
    async fn held(v: &Volume) -> Result<BTreeSet<u64>, String> {
        let mut held = BTreeSet::new();
        for id in 1..=FLUSHES {
            let mut b = [0u8; 8];
            v.read_at(&mut b, id * CHUNK)
                .await
                .map_err(|e| format!("read flush {id}: {e}"))?;
            match u64::from_le_bytes(b) {
                0 => {}
                x if x == id => {
                    held.insert(id);
                }
                x => return Err(format!("chunk of flush {id} holds {x}")),
            }
        }
        Ok(held)
    }

    /// The prefix of a writer history that `held` is, in order: some of the writer's flushes,
    /// or all of them and the write it could not flush.
    fn prefix_of(&self, held: &BTreeSet<u64>) -> Option<Vec<u64>> {
        let k = held.len();
        self.histories.iter().enumerate().find_map(|(i, h)| {
            let mut ids = h.clone();
            ids.extend(self.unflushed.get(&i));
            (ids.len() >= k && ids[..k].iter().copied().collect::<BTreeSet<_>>() == *held)
                .then(|| ids[..k].to_vec())
        })
    }

    async fn open(&mut self, host: &mut Host, at: &str) -> Result<(), String> {
        let mode = if self.below(2) == 0 {
            LogMode::Strict
        } else {
            LogMode::Behind
        };
        host.store.set_mode(mode);
        let dir = PathBuf::from(format!("/{}{}", host.name, host.lives));
        let cache = Arc::new(
            Cache::open(&host.ports, &dir.join("cache"))
                .await
                .map_err(|e| format!("{at}: open cache: {e}"))?,
        );
        let acked = self.acked.clone();
        self.race.opens += 1;
        let v = Volume::open_logged(&host.ports, cache, &dir, BRANCH, mode)
            .await
            .map_err(|e| format!("{at}: open: {e}"))?;
        if v.wait_generation().await.is_err() {
            v.close().await;
            return Ok(());
        }
        self.race.taken += 1;
        let held = Self::held(&v).await.map_err(|e| format!("{at}: {e}"))?;
        let Some(prefix) = self.prefix_of(&held) else {
            return Err(format!(
                "{at}: the open holds flushes {held:?}, a prefix of no writer's"
            ));
        };
        if let Some(back) = held.intersection(&self.dropped).next() {
            return Err(format!(
                "{at}: the open holds flush {back}, which an advance dropped"
            ));
        }
        if let Some(lost) = acked.difference(&held).next() {
            return Err(format!(
                "{at}: strict flush {lost} returned, and the open does not hold it"
            ));
        }
        self.histories.push(prefix);
        host.open = Some(Writer {
            host: host.name,
            v,
            mode,
            history: self.histories.len() - 1,
            fenced: false,
        });
        Ok(())
    }

    async fn flush(&mut self, w: &mut Writer) {
        if w.fenced || self.next > FLUSHES {
            return;
        }
        let id = self.next;
        self.next += 1;
        let wrote = self.tracer.len();
        if w.v.write_at(&id.to_le_bytes(), id * CHUNK).await.is_err() {
            w.fenced = true;
            return;
        }
        self.tracer.insert(wrote, w.host, Step::Write);
        self.traced.insert(id, u64_from(self.traced.len()) + 1);
        let before = w.v.root();
        let at = self.tracer.len();
        let r = tokio::time::timeout(WAIT, w.v.flush()).await;
        if w.v.root() == before {
            w.fenced = true;
            self.unflushed.insert(w.history, id);
            return;
        }
        self.tracer.insert(at, w.host, Step::Flush);
        self.histories[w.history].push(id);
        self.race.flushes += 1;
        if w.mode == LogMode::Strict && matches!(r, Ok(Ok(()))) {
            self.acked.insert(id);
            self.race.strict += 1;
        }
    }

    /// Ends the host's process: a close lets an upload in flight finish, a kill does not.
    async fn stop(&mut self, host: &mut Host, kill: bool) {
        if let Some(w) = host.open.take() {
            if kill {
                w.v.kill().await;
            } else {
                w.v.close().await;
            }
            self.tracer.push(host.name, Step::Stop);
        }
    }

    /// The flushes branch `name` holds, read through a host of its own.
    async fn read_branch(&self, name: &str) -> Result<BTreeSet<u64>, String> {
        let dir = PathBuf::from(format!("/{name}"));
        let cache = Arc::new(
            Cache::open(&self.ports, &dir.join("cache"))
                .await
                .map_err(|e| e.to_string())?,
        );
        let v = Volume::open_logged(&self.ports, cache, &dir, name, LogMode::Off)
            .await
            .map_err(|e| e.to_string())?;
        let held = Self::held(&v).await;
        v.close().await;
        held
    }

    /// Advances the branch to a new empty branch or to a copy of what it published.
    async fn advance(&mut self, at: &str) -> Result<(), String> {
        let src = format!("src{}", self.sources);
        self.sources += 1;
        let root = if self.below(2) == 0 {
            Volume::create(&self.ports, &src, self.size)
                .await
                .map_err(|e| format!("{at}: create {src}: {e}"))?;
            Vec::new()
        } else {
            branch::copy(&self.ports, BRANCH, &src, None)
                .await
                .map_err(|e| format!("{at}: copy to {src}: {e}"))?;
            let held = self
                .read_branch(&src)
                .await
                .map_err(|e| format!("{at}: read {src}: {e}"))?;
            self.prefix_of(&held)
                .ok_or_else(|| format!("{at}: {src} holds flushes {held:?}, a prefix of no writer's"))?
        };
        self.advancer
            .set_root(root.iter().map(|id| self.traced[id]).collect());
        let r = tokio::time::timeout(WAIT, branch::advance(&self.advancer_ports, BRANCH, &src)).await;
        if matches!(r, Ok(Ok(()))) {
            self.race.advances += 1;
            let kept: BTreeSet<u64> = root.iter().copied().collect();
            self.dropped
                .extend((1..self.next).filter(|id| !kept.contains(id)));
            self.acked.clear();
        }
        Ok(())
    }

    async fn step(&mut self, host: &mut Host, at: &str) -> Result<(), String> {
        let Some(mut w) = host.open.take() else {
            return self.open(host, at).await;
        };
        match self.below(100) {
            0..45 => self.flush(&mut w).await,
            45..55 => {
                let _ = tokio::time::timeout(WAIT, w.v.checkpoint()).await;
            }
            55..62 => {
                if matches!(tokio::time::timeout(WAIT, w.v.checkpoint()).await, Ok(Ok(()))) {
                    let _ = w.v.close_log().await;
                }
                host.open = Some(w);
                self.stop(host, false).await;
                return Ok(());
            }
            62..70 => {
                let kill = self.below(2) == 0;
                host.open = Some(w);
                self.stop(host, kill).await;
                return Ok(());
            }
            70..75 => {
                self.flush(&mut w).await;
                host.open = Some(w);
                self.stop(host, true).await;
                return Ok(());
            }
            75..80 => {
                host.open = Some(w);
                self.stop(host, true).await;
                self.tracer.push(host.name, Step::Lose);
                host.lives += 1;
                return Ok(());
            }
            _ => tokio::time::sleep(Duration::from_millis(1 + self.below(150))).await,
        }
        host.open = Some(w);
        Ok(())
    }
}

/// Runs `steps` random steps of the hosts with `seed`.
pub async fn race(seed: u64, steps: usize) -> Result<Race, String> {
    let (ports, store, _) = sim::ports(seed);
    store.set_latency(Duration::from_millis(1 + seed % 7));
    if seed % 2 == 1 {
        store.set_latency_under(keys::SEGMENTS, Duration::from_millis(20 + 40 * (seed % 5)));
    }
    let size = (FLUSHES + 1) * CHUNK;
    Volume::create(&ports, BRANCH, size)
        .await
        .map_err(|e| e.to_string())?;
    let tracer = Tracer::new(BRANCH);
    let advancer = TracedStore::new(Arc::clone(&ports.store), ADVANCER, Arc::clone(&tracer));
    let mut advancer_ports = ports.clone();
    advancer_ports.store = Arc::<TracedStore>::clone(&advancer);
    let mut hosts: Vec<Host> = HOSTS
        .iter()
        .map(|&name| {
            let store = TracedStore::new(Arc::clone(&ports.store), name, Arc::clone(&tracer));
            let mut p = ports.clone();
            let traced: Arc<dyn Store> = Arc::<TracedStore>::clone(&store);
            p.store = traced;
            Host {
                name,
                store,
                ports: p,
                lives: 0,
                open: None,
            }
        })
        .collect();
    let mut w = World {
        rng: SimRng::new(seed ^ 0x7ace),
        tracer: Arc::clone(&tracer),
        ports: ports.clone(),
        size,
        advancer,
        advancer_ports,
        sources: 0,
        dropped: BTreeSet::new(),
        traced: BTreeMap::new(),
        histories: vec![Vec::new()],
        unflushed: BTreeMap::new(),
        acked: BTreeSet::new(),
        next: 1,
        race: Race::default(),
    };
    for step in 0..steps {
        if w.below(25) == 0 {
            w.advance(&format!("seed {seed} step {step} advance")).await?;
            continue;
        }
        let i = usize_from(w.below(u64_from(hosts.len())));
        let at = format!("seed {seed} step {step} host {}", hosts[i].name);
        w.step(&mut hosts[i], &at).await?;
    }
    for h in &mut hosts {
        w.stop(h, false).await;
    }
    let mut race = std::mem::take(&mut w.race);
    race.unsupported = tracer.unsupported();
    if race.unsupported.is_none() {
        race.module = Some(trace::module(
            &HOSTS,
            ADVANCER,
            (race.opens, usize_from(w.next - 1), w.sources),
            &tracer.events(),
        ));
    }
    count(&mut race, &tracer.events());
    Ok(race)
}

fn count(race: &mut Race, events: &[Event]) {
    let mut opening: Vec<(&str, bool)> = Vec::new();
    for e in events {
        let host = e.host.as_str();
        if host == ADVANCER {
            race.advance_seals += usize::from(matches!(e.step, Step::CreateLog(_, _, Kind::Seal, true)));
            continue;
        }
        match e.step {
            Step::GetRef(_) => opening.push((host, false)),
            Step::GetLog(..) => {
                if let Some(o) = opening.iter_mut().find(|o| o.0 == host) {
                    o.1 = true;
                }
            }
            Step::UpdateRef(_, _, ok, _) => {
                if let Some(i) = opening.iter().position(|o| o.0 == host) {
                    let (_, scanned) = opening.remove(i);
                    race.scans += usize::from(scanned);
                    race.lost_takes += usize::from(!ok);
                }
            }
            Step::CreateLog(_, _, Kind::Seal, true) => race.seals += 1,
            Step::CreateLog(_, _, Kind::Writes { .. }, false) => race.refused_appends += 1,
            Step::CreateLog(..)
            | Step::DeleteLog(..)
            | Step::ListLog
            | Step::Write
            | Step::Flush
            | Step::Stop
            | Step::Lose => {}
        }
    }
}
