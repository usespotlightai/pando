//! GC racing what makes segments reachable: branch copies the store holds up for less or more
//! than GC's validate delay, checkpoints and copies of them, fast-forwards, branch and checkpoint
//! deletes, a writer whose publish the store holds up past GC's validate delay, and writers a
//! writer on another host fences. GC keeps no segment for being young, so only what reaches a
//! segment and its writer being current keep it. After every GC that deleted a segment, every
//! branch and checkpoint reads whole from a fresh host, and the writer's branch holds what it
//! last published.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use pando_engine::branch::{self, Checkpoints};
use pando_engine::gc::{self, GcOptions, GcReport};
use pando_engine::ports::Rng;
use pando_engine::sim::{self, SimRng, SimStore, StoreOp};
use pando_engine::{Cache, Ports, Volume};
use pando_format::{keys, u64_from, usize_from};

const CHUNK: usize = 64 << 10;
const SIZE: usize = 8 * CHUNK;
const MAIN: &str = "main";
const CHECK: &str = "check";
/// GC's validate delay in [`gc_races`]: a copy must finish within it.
pub const VALIDATE: Duration = Duration::from_secs(30);
const OPTS: GcOptions = GcOptions {
    delete: true,
    keep_young: Duration::ZERO,
    validate: VALIDATE,
};
const ROUND_MAX: Duration = Duration::from_hours(6);

/// What [`gc_races`] did over its rounds.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GcRaces {
    /// GC runs, each of which deletes what it finds dead.
    pub collections: usize,
    /// Segments GC deleted.
    pub deleted: usize,
    /// Segments dead at a GC's first mark that a branch reached at its second.
    pub revived: usize,
    /// Unreached segments GC kept because their writer is current.
    pub current_writer: usize,
    /// Copies of a branch or a checkpoint the store held up while GC ran.
    pub held_copies: usize,
    /// Held copies that outlasted their max age and were undone.
    pub undone: usize,
    /// Publishes the store held up while GC ran.
    pub held_publishes: usize,
    /// Writers a writer on another host fenced.
    pub fenced: usize,
    /// Branches and checkpoints read whole from a fresh host.
    pub checked: usize,
}

impl GcRaces {
    /// Adds the counts of `r` to these.
    pub fn add(&mut self, r: &GcRaces) {
        self.collections += r.collections;
        self.deleted += r.deleted;
        self.revived += r.revived;
        self.current_writer += r.current_writer;
        self.held_copies += r.held_copies;
        self.undone += r.undone;
        self.held_publishes += r.held_publishes;
        self.fenced += r.fenced;
        self.checked += r.checked;
    }

    fn collected(&mut self, r: &GcReport) {
        self.collections += 1;
        self.deleted += r.dead;
        self.revived += r.revived;
        self.current_writer += r.current_writer;
    }
}

#[derive(Debug)]
enum Op {
    Nothing,
    Checkpoint(String),
    DeleteCheckpoint(String),
    FromCheckpoint(String, String),
    Advance(String),
    Delete(String),
}

/// What one round runs at once, and when.
struct Plan {
    writes: Vec<(usize, Vec<u8>)>,
    hold_publish: bool,
    publish_for: Duration,
    copy: Option<(String, String)>,
    copy_for: Duration,
    gc_after: Duration,
    op: Op,
    op_after: Duration,
}

#[derive(Clone, Copy)]
enum Source<'a> {
    Branch(&'a str),
    Checkpoint(&'a str),
}

struct Sim {
    ports: Ports,
    store: Arc<SimStore>,
    rng: SimRng,
    /// What the writer last published.
    model: Vec<u8>,
    writer: Volume,
    gc: Arc<Cache>,
    kept: Checkpoints,
    /// What each branch and checkpoint other than the writer's read at its first check.
    seen: HashMap<String, Vec<u8>>,
    names: usize,
    max_age: Option<Duration>,
    races: GcRaces,
}

async fn open(ports: &Ports, dir: &str, name: &str) -> Result<Volume, String> {
    let cache = Arc::new(
        Cache::open(ports, Path::new(dir))
            .await
            .map_err(|e| format!("open cache {dir}: {e}"))?,
    );
    Volume::open(ports, cache, Path::new(dir), name)
        .await
        .map_err(|e| format!("open {name} in {dir}: {e}"))
}

fn undone(e: &pando_engine::Error) -> bool {
    e.to_string().contains("took longer than")
}

/// Runs `rounds` rounds of GC racing branch operations and a writer, with seed `seed`. Copies
/// and checkpoints are undone past `max_age`; with `None` they never are, and a slow copy can
/// name what GC deleted.
pub async fn gc_races(seed: u64, rounds: usize, max_age: Option<Duration>) -> Result<GcRaces, String> {
    let (ports, store, _) = sim::ports(seed);
    let rng = SimRng::new(seed ^ 0x6c);
    store.set_latency(Duration::from_millis(rng.next_u64() % 40));
    Volume::create(&ports, MAIN, u64_from(SIZE))
        .await
        .map_err(|e| e.to_string())?;
    let writer = open(&ports, "/w0", MAIN).await?;
    let gc = Arc::new(
        Cache::open(&ports, Path::new("/gc"))
            .await
            .map_err(|e| e.to_string())?,
    );
    let mut s = Sim {
        ports,
        store,
        rng,
        model: vec![0; SIZE],
        writer,
        gc,
        kept: Checkpoints::default(),
        seen: HashMap::new(),
        names: 0,
        max_age,
        races: GcRaces::default(),
    };
    for round in 0..rounds {
        let at = format!("seed {seed} round {round}");
        tokio::time::timeout(ROUND_MAX, s.round(&at))
            .await
            .map_err(|_| format!("{at}: hung"))??;
    }
    s.check_all(&format!("seed {seed} at the end")).await?;
    s.writer.close().await;
    Ok(s.races)
}

impl Sim {
    fn below(&self, n: usize) -> usize {
        usize_from(self.rng.next_u64() % u64_from(n))
    }

    fn secs_below(&self, d: Duration) -> Duration {
        Duration::from_secs(self.rng.next_u64() % d.as_secs().max(1))
    }

    fn name(&mut self, prefix: &str) -> String {
        self.names += 1;
        format!("{prefix}{}", self.names)
    }

    fn noise(&self, n: usize) -> Vec<u8> {
        let seed = self.rng.next_u64();
        if self.below(4) == 0 {
            return vec![seed.to_le_bytes()[0] | 1; n];
        }
        let mut x = seed | 1;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x.to_le_bytes()[0]
            })
            .collect()
    }

    /// Writes of random bytes, and of whole chunks the branch holds elsewhere; or a rewrite
    /// of the whole branch, which leaves no segment the branch reached before.
    fn plan_writes(&self) -> Vec<(usize, Vec<u8>)> {
        if self.below(4) == 0 {
            return vec![(0, self.noise(SIZE))];
        }
        let mut after = self.model.clone();
        (0..=self.below(6))
            .map(|_| {
                let chunk = self.below(SIZE / CHUNK) * CHUNK;
                let (off, b) = if self.below(3) == 0 {
                    let from = self.below(SIZE / CHUNK) * CHUNK;
                    (chunk, after[from..from + CHUNK].to_vec())
                } else {
                    let off = chunk + if self.below(2) == 0 { 0 } else { self.below(CHUNK) };
                    let len = (1 + self.below(2 * CHUNK)).min(SIZE - off);
                    (off, self.noise(len))
                };
                after[off..off + b.len()].copy_from_slice(&b);
                (off, b)
            })
            .collect()
    }

    async fn branches(&self) -> Result<Vec<String>, String> {
        let mut out = branch::list(&self.ports).await.map_err(|e| e.to_string())?;
        out.retain(|n| n != MAIN);
        Ok(out)
    }

    async fn checkpoints(&self) -> Result<Vec<String>, String> {
        Ok(branch::list_checkpoints(&self.ports)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|(n, _)| n)
            .collect())
    }

    fn pick(&self, names: &[String]) -> Option<String> {
        (!names.is_empty()).then(|| names[self.below(names.len())].clone())
    }

    async fn plan_op(&mut self) -> Result<Op, String> {
        let branches = self.branches().await?;
        let checkpoints = self.checkpoints().await?;
        Ok(match self.below(6) {
            0 if checkpoints.len() < 3 => Op::Checkpoint(self.name("c")),
            1 => self.pick(&checkpoints).map_or(Op::Nothing, Op::DeleteCheckpoint),
            2 if branches.len() < 4 => match self.pick(&checkpoints) {
                Some(c) => Op::FromCheckpoint(c, self.name("b")),
                None => Op::Nothing,
            },
            3 => self.pick(&branches).map_or(Op::Nothing, Op::Advance),
            4 | 5 if branches.len() > 1 => self.pick(&branches).map_or(Op::Nothing, Op::Delete),
            _ => Op::Nothing,
        })
    }

    async fn run_op(&self, op: &Op) -> pando_engine::Result<()> {
        let p = &self.ports;
        match op {
            Op::Nothing => Ok(()),
            Op::Checkpoint(name) => branch::checkpoint(p, &self.kept, MAIN, name, self.max_age)
                .await
                .map(drop),
            Op::DeleteCheckpoint(name) => branch::delete_checkpoint(p, &self.kept, name).await,
            Op::FromCheckpoint(c, dst) => branch::from_checkpoint(p, &self.kept, c, dst, self.max_age).await,
            Op::Advance(dst) => branch::advance(p, dst, MAIN).await,
            Op::Delete(name) => branch::delete(p, name).await,
        }
    }

    /// Opens the branch on another host, which fences the writer before it: that writer's
    /// next upload must fail to publish.
    async fn fence(&mut self, at: &str) -> Result<(), String> {
        let dir = self.name("/w");
        let v = open(&self.ports, &dir, MAIN).await?;
        let old = std::mem::replace(&mut self.writer, v);
        old.write_at(&[0xee; 100], 0)
            .await
            .map_err(|e| format!("{at}: fenced write: {e}"))?;
        if old.checkpoint().await.is_ok() {
            return Err(format!("{at}: a fenced writer published"));
        }
        old.close().await;
        self.races.fenced += 1;
        Ok(())
    }

    async fn plan(&mut self) -> Result<Plan, String> {
        let writes = if self.below(5) == 0 {
            Vec::new()
        } else {
            self.plan_writes()
        };
        let hold_publish = !writes.is_empty() && self.below(3) == 0;
        let publish_for = if self.below(2) == 0 {
            2 * VALIDATE
        } else {
            20 * VALIDATE
        };
        let copy = if self.below(2) == 0 {
            let branches = self.branches().await?;
            let src = if self.below(3) == 0 {
                self.pick(&branches)
            } else {
                None
            };
            Some((src.unwrap_or_else(|| MAIN.to_owned()), self.name("b")))
        } else {
            None
        };
        let copy_for = if self.below(2) == 0 {
            VALIDATE / 3
        } else {
            3 * VALIDATE
        };
        let gc_after = match self.below(3) {
            0 if copy.is_some() => Duration::from_secs(1) + self.secs_below(copy_for),
            1 if hold_publish => self.secs_below(publish_for),
            _ => self.secs_below(2 * VALIDATE),
        };
        let op = self.plan_op().await?;
        let op_after = self.secs_below(2 * VALIDATE);
        Ok(Plan {
            writes,
            hold_publish,
            publish_for,
            copy,
            copy_for,
            gc_after,
            op,
            op_after,
        })
    }

    async fn round(&mut self, at: &str) -> Result<(), String> {
        let pause = match self.below(2) {
            0 => Duration::ZERO,
            _ => self.secs_below(Duration::from_mins(10)),
        };
        tokio::time::sleep(pause).await;
        if self.below(6) == 0 {
            self.fence(at).await?;
        }
        let Plan {
            writes,
            hold_publish,
            publish_for,
            copy,
            copy_for,
            gc_after,
            op,
            op_after,
        } = self.plan().await?;
        let refs = format!("{}/", keys::REFS);
        let main_ref = keys::reference(MAIN);
        if copy.is_some() {
            self.store.hold(StoreOp::Create, &refs);
        }
        if hold_publish {
            self.store.hold(StoreOp::Update, &main_ref);
        }
        let (copied, published, collected, done, (), ()) = tokio::join!(
            async {
                match &copy {
                    Some((src, dst)) => Some(branch::copy(&self.ports, src, dst, self.max_age).await),
                    None => None,
                }
            },
            async {
                for (off, b) in &writes {
                    self.writer.write_at(b, u64_from(*off)).await?;
                }
                self.writer.checkpoint().await
            },
            async {
                tokio::time::sleep(gc_after).await;
                gc::collect(&self.gc, self.ports.clock.as_ref(), &OPTS).await
            },
            async {
                tokio::time::sleep(op_after).await;
                self.run_op(&op).await
            },
            async {
                if copy.is_some() {
                    tokio::time::sleep(copy_for).await;
                    self.store.release(StoreOp::Create, &refs);
                }
            },
            async {
                if hold_publish {
                    tokio::time::sleep(publish_for).await;
                    self.store.release(StoreOp::Update, &main_ref);
                }
            },
        );

        published.map_err(|e| format!("{at}: write and publish: {e}"))?;
        for (off, b) in &writes {
            self.model[*off..*off + b.len()].copy_from_slice(b);
        }
        self.races.held_publishes += usize::from(hold_publish);
        let branches = self.branches().await?;
        if let (Some((src, dst)), Some(r)) = (&copy, copied) {
            self.races.held_copies += 1;
            match r {
                Ok(()) => {}
                Err(e) if undone(&e) || e.to_string().contains("no branch") => {
                    self.races.undone += usize::from(undone(&e));
                    if branches.contains(dst) {
                        return Err(format!("{at}: the failed copy of {src} to {dst} left {dst}: {e}"));
                    }
                }
                Err(e) => return Err(format!("{at}: copy {src} to {dst}: {e}")),
            }
        }
        let report = collected.map_err(|e| format!("{at}: gc: {e}"))?;
        self.races.collected(&report);
        match done {
            Ok(()) => {
                if let Op::Advance(dst) = &op {
                    self.seen.remove(dst);
                }
            }
            Err(e) if undone(&e) && copy.is_some() => self.races.undone += 1,
            Err(e) => return Err(format!("{at}: {op:?}: {e}")),
        }
        if report.dead > 0 || self.below(4) == 0 {
            self.check_all(at).await?;
        }
        Ok(())
    }

    /// Reads every branch and checkpoint whole from a fresh host.
    async fn check_all(&mut self, at: &str) -> Result<(), String> {
        for name in branch::list(&self.ports).await.map_err(|e| e.to_string())? {
            if name == CHECK {
                return Err(format!("{at}: a check left its branch"));
            }
            let got = self.read_whole(Source::Branch(&name), at).await?;
            if name == MAIN {
                if got != self.model {
                    return Err(format!("{at}: {MAIN} differs from what its writer published"));
                }
            } else {
                self.saw(name, got, at)?;
            }
        }
        for name in self.checkpoints().await? {
            let got = self.read_whole(Source::Checkpoint(&name), at).await?;
            self.saw(format!("checkpoint {name}"), got, at)?;
        }
        Ok(())
    }

    fn saw(&mut self, what: String, got: Vec<u8>, at: &str) -> Result<(), String> {
        match self.seen.entry(what) {
            Entry::Occupied(e) if *e.get() != got => Err(format!("{at}: {} changed", e.key())),
            Entry::Occupied(_) => Ok(()),
            Entry::Vacant(e) => {
                e.insert(got);
                Ok(())
            }
        }
    }

    /// What `src` holds, read on a host with an empty cache through a copy of it, so the read
    /// fences no writer of `src`.
    async fn read_whole(&mut self, src: Source<'_>, at: &str) -> Result<Vec<u8>, String> {
        let p = &self.ports;
        let what = match src {
            Source::Branch(n) => {
                branch::copy(p, n, CHECK, None)
                    .await
                    .map_err(|e| format!("{at}: copy {n}: {e}"))?;
                format!("branch {n}")
            }
            Source::Checkpoint(n) => {
                branch::from_checkpoint(p, &self.kept, n, CHECK, None)
                    .await
                    .map_err(|e| format!("{at}: copy checkpoint {n}: {e}"))?;
                format!("checkpoint {n}")
            }
        };
        let dir = self.name("/check");
        let v = open(&self.ports, &dir, CHECK).await?;
        let mut got = vec![0; SIZE];
        let read = v.read_at(&mut got, 0).await;
        v.close().await;
        branch::delete(&self.ports, CHECK)
            .await
            .map_err(|e| format!("{at}: delete the copy of {what}: {e}"))?;
        read.map_err(|e| format!("{at}: {what} does not read whole from a fresh host: {e}"))?;
        self.races.checked += 1;
        Ok(got)
    }
}
