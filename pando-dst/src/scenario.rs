//! Seeded scenarios: random writes, flushes, checkpoints, daemon restarts, host crashes and
//! store faults on one branch, checked against a model of what the volume must hold.

use std::path::Path;
use std::sync::Arc;

use pando_engine::ports::Rng;
use pando_engine::sim::{self, Fault, SimFs, SimRng, SimStore, StoreOp};
use pando_engine::{Cache, Ports, Volume};
use pando_format::{u64_from, usize_from};

const SIZE: usize = 4 << 20;
const CHUNK: usize = 64 << 10;
const BRANCH: &str = "main";

/// What one scenario did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    /// Writes and zeroings of a random range.
    pub writes: usize,
    /// Flushes; a host crash keeps what the last one made durable.
    pub flushes: usize,
    /// Checkpoints, each a publish to the store.
    pub checkpoints: usize,
    /// Closes and reopens of the volume on the same host.
    pub restarts: usize,
    /// Host crashes, which lose what was written since the last flush.
    pub crashes: usize,
    /// Store faults injected into a later create or update of a ref.
    pub faults: usize,
    /// Reads checked against the model.
    pub reads: usize,
}

struct World {
    ports: Ports,
    store: Arc<SimStore>,
    fs: Arc<SimFs>,
    rng: SimRng,
    /// What the volume holds now, and what it holds after a host crash: the last flush.
    model: Vec<u8>,
    flushed: Vec<u8>,
    report: Report,
}

async fn open(ports: &Ports, dir: &str) -> Result<Volume, String> {
    let cache = Arc::new(
        Cache::open(ports, Path::new(dir))
            .await
            .map_err(|e| format!("open cache: {e}"))?,
    );
    Volume::open(ports, cache, Path::new(dir), BRANCH)
        .await
        .map_err(|e| format!("open volume: {e}"))
}

impl World {
    fn below(&self, n: usize) -> usize {
        usize_from(self.rng.next_u64() % u64_from(n))
    }

    fn range(&self) -> (usize, usize) {
        let off = if self.below(2) == 0 {
            self.below(SIZE / CHUNK) * CHUNK
        } else {
            self.below(SIZE)
        };
        let len = 1 + self.below(3 * CHUNK).min(SIZE - off - 1);
        (off, len)
    }

    fn bytes(&self, n: usize) -> Vec<u8> {
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

    fn fault(&mut self) {
        let op = if self.below(2) == 0 {
            StoreOp::Create
        } else {
            StoreOp::Update
        };
        self.store.inject(Fault {
            op,
            applied: self.below(2) == 0,
        });
        self.report.faults += 1;
    }

    async fn check(&mut self, v: &Volume, off: usize, len: usize, when: &str) -> Result<(), String> {
        let mut got = vec![0; len];
        v.read_at(&mut got, u64_from(off))
            .await
            .map_err(|e| format!("{when}: read {len} at {off}: {e}"))?;
        self.report.reads += 1;
        if got != self.model[off..off + len] {
            let at = got
                .iter()
                .zip(&self.model[off..])
                .position(|(a, b)| a != b)
                .unwrap_or(0);
            return Err(format!(
                "{when}: read {len} at {off} differs from the model at byte {}",
                off + at
            ));
        }
        Ok(())
    }
}

/// Runs `steps` random steps with `seed`, then checks the whole volume from a fresh host.
pub async fn crash_and_restart(seed: u64, steps: usize) -> Result<Report, String> {
    let (ports, store, fs) = sim::ports(seed);
    Volume::create(&ports, BRANCH, u64_from(SIZE))
        .await
        .map_err(|e| e.to_string())?;
    let mut w = World {
        ports,
        store,
        fs,
        rng: SimRng::new(seed ^ 0x5eed),
        model: vec![0; SIZE],
        flushed: vec![0; SIZE],
        report: Report::default(),
    };
    let mut v = open(&w.ports, "/host").await?;
    let trace = std::env::var_os("PANDO_DST_TRACE").is_some();
    for step in 0..steps {
        let at = format!("seed {seed} step {step}");
        let op = w.below(100);
        if trace {
            eprintln!("{at}: op {op} root {}", v.root());
        }
        match op {
            0..50 => {
                let (off, len) = w.range();
                let b = w.bytes(len);
                trace_range(trace, "write", off, len);
                v.write_at(&b, u64_from(off))
                    .await
                    .map_err(|e| format!("{at}: write: {e}"))?;
                w.model[off..off + len].copy_from_slice(&b);
                w.report.writes += 1;
            }
            50..55 => {
                let (off, len) = w.range();
                trace_range(trace, "zero", off, len);
                v.zero(u64_from(off), u64_from(len))
                    .await
                    .map_err(|e| format!("{at}: zero: {e}"))?;
                w.model[off..off + len].fill(0);
                w.report.writes += 1;
            }
            55..67 => {
                v.flush().await.map_err(|e| format!("{at}: flush: {e}"))?;
                w.flushed.clone_from(&w.model);
                w.report.flushes += 1;
            }
            67..72 => {
                v.checkpoint()
                    .await
                    .map_err(|e| format!("{at}: checkpoint: {e}"))?;
                w.flushed.clone_from(&w.model);
                w.report.checkpoints += 1;
            }
            72..77 => {
                v.close().await;
                drop(v);
                v = open(&w.ports, "/host")
                    .await
                    .map_err(|e| format!("{at}: restart: {e}"))?;
                w.report.restarts += 1;
            }
            77..82 => {
                v.close().await;
                drop(v);
                w.fs.crash();
                w.model.clone_from(&w.flushed);
                v = open(&w.ports, "/host")
                    .await
                    .map_err(|e| format!("{at}: after crash: {e}"))?;
                w.report.crashes += 1;
            }
            82..87 => w.fault(),
            _ => {
                let (off, len) = w.range();
                w.check(&v, off, len, &at).await?;
            }
        }
    }
    v.checkpoint()
        .await
        .map_err(|e| format!("seed {seed} final checkpoint: {e}"))?;
    v.close().await;
    let fresh = open(&w.ports, "/fresh").await?;
    for off in (0..SIZE).step_by(4 * CHUNK) {
        w.check(&fresh, off, 4 * CHUNK, &format!("seed {seed} fresh host"))
            .await?;
    }
    fresh.close().await;
    Ok(w.report)
}

fn trace_range(on: bool, what: &str, off: usize, len: usize) {
    if on {
        eprintln!(
            "  {what} {len} at {off} (chunks {}..={})",
            off / CHUNK,
            (off + len - 1) / CHUNK
        );
    }
}

/// Two hosts take the branch in turns. Each open fences the writer before it, which must then
/// fail to publish; the store keeps what the fenced writer published before.
pub async fn writers_take_turns(seed: u64, turns: usize) -> Result<(), String> {
    let (ports, _, _) = sim::ports(seed);
    Volume::create(&ports, BRANCH, u64_from(SIZE))
        .await
        .map_err(|e| e.to_string())?;
    let rng = SimRng::new(seed);
    let mut model = vec![0u8; SIZE];
    let mut prev: Option<Volume> = None;
    for turn in 0..turns {
        let dir = if turn % 2 == 0 { "/a" } else { "/b" };
        let v = open(&ports, dir).await.map_err(|e| format!("turn {turn}: {e}"))?;
        if let Some(old) = prev.take() {
            old.write_at(&[0xee; 100], 0)
                .await
                .map_err(|e| format!("turn {turn}: fenced write: {e}"))?;
            if old.checkpoint().await.is_ok() {
                return Err(format!("turn {turn}: a fenced writer published"));
            }
            old.close().await;
        }
        let mut got = vec![0; SIZE];
        v.read_at(&mut got, 0)
            .await
            .map_err(|e| format!("turn {turn}: read: {e}"))?;
        if got != model {
            return Err(format!(
                "turn {turn}: the new writer does not see the last publish"
            ));
        }
        let off = usize_from(rng.next_u64() % (u64_from(SIZE) - 1000));
        let fill = u8::try_from(turn + 1).map_err(|e| format!("turn {turn}: {e}"))?;
        let b = vec![fill; 1000];
        v.write_at(&b, u64_from(off))
            .await
            .map_err(|e| format!("turn {turn}: write: {e}"))?;
        v.checkpoint()
            .await
            .map_err(|e| format!("turn {turn}: checkpoint: {e}"))?;
        model[off..off + 1000].copy_from_slice(&b);
        prev = Some(v);
    }
    if let Some(v) = prev {
        v.close().await;
    }
    Ok(())
}
