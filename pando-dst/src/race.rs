//! Hosts that race for one branch. Each opens it, flushes, publishes, detaches, stops or is lost
//! at random while the others' background tasks run. Every open must hold a prefix of some
//! writer's flushes, and every strict flush that returned; the calls on the branch's ref and log
//! are traced for `specs/WriteLogTrace.tla`.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pando_engine::ports::{Rng, Store};
use pando_engine::sim::{self, SimRng};
use pando_engine::{Cache, LogMode, Ports, Volume};
use pando_format::{keys, u64_from, usize_from};

use crate::trace::{self, Event, Kind, Step, TracedStore, Tracer};

const CHUNK: u64 = 64 << 10;
const FLUSHES: u64 = 40;
const BRANCH: &str = "main";
/// The hosts, by name.
pub const HOSTS: [&str; 2] = ["a", "b"];
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
    /// Each writer's flushes in order, from the open it took the branch with.
    histories: Vec<Vec<u64>>,
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

    /// The writer history `held` is a prefix of, and the length of that prefix.
    fn prefix_of(&self, held: &BTreeSet<u64>) -> Option<Vec<u64>> {
        self.histories.iter().find_map(|h| {
            let k = held.len();
            (h.len() >= k && h[..k].iter().copied().collect::<BTreeSet<_>>() == *held)
                .then(|| h[..k].to_vec())
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
        if w.v.write_at(&id.to_le_bytes(), id * CHUNK).await.is_err() {
            w.fenced = true;
            return;
        }
        let before = w.v.root();
        let at = self.tracer.len();
        let r = tokio::time::timeout(WAIT, w.v.flush()).await;
        if w.v.root() == before {
            w.fenced = true;
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
        histories: vec![Vec::new()],
        acked: BTreeSet::new(),
        next: 1,
        race: Race::default(),
    };
    for step in 0..steps {
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
            race.opens,
            usize_from(w.next - 1),
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
        match e.step {
            Step::GetRef(_) => opening.push((host, false)),
            Step::GetLog(..) => {
                if let Some(o) = opening.iter_mut().find(|o| o.0 == host) {
                    o.1 = true;
                }
            }
            Step::UpdateRef(_, _, ok) => {
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
            | Step::Flush
            | Step::Stop
            | Step::Lose => {}
        }
    }
}
