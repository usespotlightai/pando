//! Seeded scenarios: random writes, flushes, checkpoints, daemon restarts, host crashes, lost
//! hosts, store faults and a cache over its budget on one branch, checked against a model of
//! what the volume must hold.

use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use pando_engine::ports::{Rng, StoreError};
use pando_engine::sim::{self, Fault, SimFs, SimRng, SimStore, StoreOp};
use pando_engine::{Cache, Error, LogMode, Ports, Volume};
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
    /// Reads, writes and zeroings that failed because the store did not answer in time.
    pub unanswered: usize,
    /// Writes and zeroings during which a commit that bounds memory moved the root.
    pub commits: usize,
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
    open_with(ports, dir, 0).await
}

/// [`open`] with a cache whose volumes keep their uncommitted writes within `dirty_memory`.
async fn open_with(ports: &Ports, dir: &str, dirty_memory: u64) -> Result<Volume, String> {
    let cache = Cache::open(ports, Path::new(dir))
        .await
        .map_err(|e| format!("open cache: {e}"))?;
    cache.set_dirty_memory(dirty_memory);
    Volume::open(ports, Arc::new(cache), Path::new(dir), BRANCH)
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
            permanent: false,
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

    /// Checks the volume after a host crash and makes the model what it holds. It holds what
    /// the last flush made durable, then the writes `since` in order, up to the one a commit
    /// that bounded memory took last, and of the write after it the chunks that commit took.
    /// Without such commits, `strict`, it holds what the last flush made durable.
    async fn after_crash(
        &mut self,
        v: &Volume,
        since: &[(usize, Vec<u8>)],
        strict: bool,
        at: &str,
    ) -> Result<(), String> {
        let mut got = vec![0; SIZE];
        v.read_at(&mut got, 0)
            .await
            .map_err(|e| format!("{at}: read after the crash: {e}"))?;
        self.report.reads += 1;
        let chunk = |b: &[u8], c: usize| b[c * CHUNK..(c + 1) * CHUNK].to_vec();
        let mut m = self.flushed.clone();
        let upto = if strict { 0 } else { since.len() };
        for k in 0..=upto {
            let differs: Vec<usize> = (0..SIZE / CHUNK)
                .filter(|&c| chunk(&got, c) != chunk(&m, c))
                .collect();
            if differs.is_empty() {
                break;
            }
            let Some((off, b)) = since.get(k).filter(|_| k < upto) else {
                return Err(format!(
                    "{at}: after the crash chunk {} holds neither the last flush nor a later commit",
                    differs[0]
                ));
            };
            let mut next = m.clone();
            next[*off..off + b.len()].copy_from_slice(b);
            if differs.iter().all(|&c| chunk(&got, c) == chunk(&next, c)) {
                m.clone_from(&got);
                break;
            }
            m = next;
        }
        self.model.clone_from(&m);
        self.flushed = m;
        Ok(())
    }
}

/// Runs `steps` random steps with `seed`, then checks the whole volume from a fresh host.
/// Seeds that are not a multiple of 3 keep each volume's uncommitted writes within 1 or 2 MiB,
/// so commits that bound memory come between flushes, and a crash keeps what they took.
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
    let dirty = u64::from(u8::try_from(seed % 3).unwrap_or(0)) << 20;
    let mut since: Vec<(usize, Vec<u8>)> = Vec::new();
    let mut v = open_with(&w.ports, "/host", dirty).await?;
    let trace = std::env::var_os("PANDO_DST_TRACE").is_some();
    for step in 0..steps {
        let root = v.root();
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
                since.push((off, b));
                w.report.writes += 1;
            }
            50..55 => {
                let (off, len) = w.range();
                trace_range(trace, "zero", off, len);
                v.zero(u64_from(off), u64_from(len))
                    .await
                    .map_err(|e| format!("{at}: zero: {e}"))?;
                w.model[off..off + len].fill(0);
                since.push((off, vec![0; len]));
                w.report.writes += 1;
            }
            55..67 => {
                v.flush().await.map_err(|e| format!("{at}: flush: {e}"))?;
                w.flushed.clone_from(&w.model);
                since.clear();
                w.report.flushes += 1;
            }
            67..72 => {
                v.checkpoint()
                    .await
                    .map_err(|e| format!("{at}: checkpoint: {e}"))?;
                w.flushed.clone_from(&w.model);
                since.clear();
                w.report.checkpoints += 1;
            }
            72..77 => {
                v.close().await;
                drop(v);
                v = open_with(&w.ports, "/host", dirty)
                    .await
                    .map_err(|e| format!("{at}: restart: {e}"))?;
                w.report.restarts += 1;
            }
            77..82 => {
                v.close().await;
                drop(v);
                w.fs.crash();
                v = open_with(&w.ports, "/host", dirty)
                    .await
                    .map_err(|e| format!("{at}: after crash: {e}"))?;
                w.after_crash(&v, &since, dirty == 0, &at).await?;
                since.clear();
                w.report.crashes += 1;
            }
            82..87 => w.fault(),
            _ => {
                let (off, len) = w.range();
                w.check(&v, off, len, &at).await?;
            }
        }
        w.report.commits += usize::from(op < 55 && v.root() != root);
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

fn refused(e: &Error) -> bool {
    matches!(e.kind(), Error::Store(StoreError::Permanent { .. }))
}

/// From a random step on, the store refuses every segment create or every ref update, after a
/// write that must be uploaded. Every checkpoint from then on fails with the refusal, and the
/// uploader stops. Nothing is lost: once the store accepts again, a restart publishes every
/// write.
pub async fn a_refusing_store(seed: u64, steps: usize) -> Result<Report, String> {
    let (ports, store, fs) = sim::ports(seed);
    Volume::create(&ports, BRANCH, u64_from(SIZE))
        .await
        .map_err(|e| e.to_string())?;
    let mut w = World {
        ports,
        store,
        fs,
        rng: SimRng::new(seed ^ 0x4e0),
        model: vec![0; SIZE],
        flushed: vec![0; SIZE],
        report: Report::default(),
    };
    let v = open(&w.ports, "/host").await?;
    let refuse_at = w.below(steps);
    for step in 0..steps {
        let at = format!("seed {seed} step {step}");
        if step == refuse_at {
            let op = if w.below(2) == 0 {
                StoreOp::Create
            } else {
                StoreOp::Update
            };
            w.store.refuse(op);
            w.report.faults += 1;
            let b = [!w.model[0]];
            v.write_at(&b, 0).await.map_err(|e| format!("{at}: write: {e}"))?;
            w.model[0] = b[0];
            w.report.writes += 1;
        }
        match w.below(100) {
            0..60 => {
                let (off, len) = w.range();
                let b = w.bytes(len);
                v.write_at(&b, u64_from(off))
                    .await
                    .map_err(|e| format!("{at}: write: {e}"))?;
                w.model[off..off + len].copy_from_slice(&b);
                w.report.writes += 1;
            }
            60..80 => {
                v.flush().await.map_err(|e| format!("{at}: flush: {e}"))?;
                w.report.flushes += 1;
            }
            80..90 => {
                match v.checkpoint().await {
                    Ok(()) if step < refuse_at => {}
                    Ok(()) => return Err(format!("{at}: a checkpoint after the refusal passed")),
                    Err(e) if step >= refuse_at && refused(&e) => {}
                    Err(e) => return Err(format!("{at}: checkpoint: {e}")),
                }
                w.report.checkpoints += 1;
            }
            _ => {
                let (off, len) = w.range();
                w.check(&v, off, len, &at).await?;
            }
        }
    }
    match v.checkpoint().await {
        Err(e) if refused(&e) => {}
        r => return Err(format!("seed {seed}: the last checkpoint ended with {r:?}")),
    }
    let stopped = v.upload_stopped().await;
    if !refused(&stopped) {
        return Err(format!(
            "seed {seed}: the uploader stopped for another reason: {stopped}"
        ));
    }
    v.close().await;
    drop(v);
    w.store.accept();
    let v = open(&w.ports, "/host")
        .await
        .map_err(|e| format!("seed {seed}: restart: {e}"))?;
    v.checkpoint()
        .await
        .map_err(|e| format!("seed {seed}: checkpoint after the restart: {e}"))?;
    v.close().await;
    let fresh = open(&w.ports, "/fresh").await?;
    for off in (0..SIZE).step_by(4 * CHUNK) {
        w.check(&fresh, off, 4 * CHUNK, &format!("seed {seed} fresh host"))
            .await?;
    }
    fresh.close().await;
    Ok(w.report)
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

/// Writers on hosts lost one after another, each with a strict log and with store faults: each
/// next host holds every flush a writer returned from, though no upload need have published it.
pub async fn hosts_lost_with_a_strict_log(seed: u64, hosts: usize, steps: usize) -> Result<Report, String> {
    let (ports, store, fs) = sim::ports(seed);
    Volume::create(&ports, BRANCH, SIZE as u64)
        .await
        .map_err(|e| e.to_string())?;
    let mut w = World {
        ports,
        store,
        fs,
        rng: SimRng::new(seed ^ 0x1057),
        model: vec![0; SIZE],
        flushed: vec![0; SIZE],
        report: Report::default(),
    };
    for host in 0..=hosts {
        let dir = format!("/host{host}");
        let at = format!("seed {seed} host {host}");
        let cache = Arc::new(
            Cache::open(&w.ports, Path::new(&dir))
                .await
                .map_err(|e| format!("{at}: open cache: {e}"))?,
        );
        let v = Volume::open_logged(&w.ports, cache, Path::new(&dir), BRANCH, LogMode::Strict)
            .await
            .map_err(|e| format!("{at}: open: {e}"))?;
        v.wait_generation()
            .await
            .map_err(|e| format!("{at}: generation: {e}"))?;
        w.model.clone_from(&w.flushed);
        for off in (0..SIZE).step_by(4 * CHUNK) {
            w.check(&v, off, 4 * CHUNK, &format!("{at} opened")).await?;
        }
        if host == hosts {
            v.close().await;
            break;
        }
        for step in 0..steps {
            let at = format!("{at} step {step}");
            match w.below(100) {
                0..50 => {
                    let (off, len) = w.range();
                    let b = w.bytes(len);
                    v.write_at(&b, off as u64)
                        .await
                        .map_err(|e| format!("{at}: write: {e}"))?;
                    w.model[off..off + len].copy_from_slice(&b);
                    w.report.writes += 1;
                }
                50..70 => {
                    v.flush().await.map_err(|e| format!("{at}: flush: {e}"))?;
                    w.flushed.clone_from(&w.model);
                    w.report.flushes += 1;
                }
                70..80 => w.fault(),
                _ => {
                    let (off, len) = w.range();
                    w.check(&v, off, len, &at).await?;
                }
            }
        }
        v.close().await;
        w.report.crashes += 1;
    }
    Ok(w.report)
}

/// How long device I/O waits on the store in [`a_store_outage`].
const STORE_WAIT: Duration = Duration::from_secs(10);
/// The chunks of one frame of random bytes: a read of one fetches them all.
const FRAME_CHUNKS: usize = pando_format::FRAME_MAX / CHUNK;

/// The store stops answering after a host read part of a published branch. Reads, writes and
/// zeroings that need the store fail within the store wait instead of hanging, and fail whole;
/// those on what the host holds, and every flush, go on, and the uploads fall behind. Once the
/// store answers again, nothing written is lost: the host and a fresh host read the model.
pub async fn a_store_outage(seed: u64, steps: usize) -> Result<Report, String> {
    let (mut w, v, mut local) = a_host_of_a_published_branch(seed).await?;
    w.store.set_answering(false);
    let mut flushed_writes = false;
    let mut written = false;
    for step in 0..steps {
        let at = format!("seed {seed} step {step}");
        let (off, len) = w.range();
        let chunks = off / CHUNK..=(off + len - 1) / CHUNK;
        let partial = |c: usize| off > c * CHUNK || off + len < (c + 1) * CHUNK;
        let needs_store = chunks.clone().any(|c| !local[c] && partial(c));
        let t0 = Instant::now();
        match w.below(100) {
            0..40 => {
                let b = w.bytes(len);
                let r = bounded(v.write_at(&b, u64_from(off)), &at).await?;
                if w.settled(r, needs_store, t0, &at, "write")? {
                    w.model[off..off + len].copy_from_slice(&b);
                    mark(&mut local, chunks);
                    written = true;
                }
                w.report.writes += 1;
            }
            40..50 => {
                let r = bounded(v.zero(u64_from(off), u64_from(len)), &at).await?;
                if w.settled(r, needs_store, t0, &at, "zero")? {
                    w.model[off..off + len].fill(0);
                    mark(&mut local, chunks);
                    written = true;
                }
                w.report.writes += 1;
            }
            50..65 => {
                bounded(v.flush(), &at)
                    .await?
                    .map_err(|e| format!("{at}: flush: {e}"))?;
                flushed_writes |= written;
                w.report.flushes += 1;
            }
            _ => {
                let needs_store = chunks.clone().any(|c| !local[c]);
                let mut got = vec![0; len];
                let r = bounded(v.read_at(&mut got, u64_from(off)), &at).await?;
                if w.settled(r, needs_store, t0, &at, "read")? {
                    if got != w.model[off..off + len] {
                        return Err(format!("{at}: read {len} at {off} differs from the model"));
                    }
                    mark(&mut local, chunks);
                    w.report.reads += 1;
                }
            }
        }
    }
    tokio::time::sleep(STORE_WAIT).await;
    if flushed_writes && v.upload_lag() < STORE_WAIT {
        return Err(format!(
            "seed {seed}: uploads are {:?} behind flushes the store never saw",
            v.upload_lag()
        ));
    }
    w.store.set_answering(true);
    for off in (0..SIZE).step_by(4 * CHUNK) {
        w.check(&v, off, 4 * CHUNK, &format!("seed {seed} after the outage"))
            .await?;
    }
    v.checkpoint()
        .await
        .map_err(|e| format!("seed {seed}: checkpoint after the outage: {e}"))?;
    w.report.checkpoints += 1;
    if !v.upload_lag().is_zero() {
        return Err(format!(
            "seed {seed}: uploads are {:?} behind after a checkpoint",
            v.upload_lag()
        ));
    }
    v.close().await;
    let fresh = open(&w.ports, "/fresh").await?;
    for off in (0..SIZE).step_by(4 * CHUNK) {
        w.check(&fresh, off, 4 * CHUNK, &format!("seed {seed} fresh host"))
            .await?;
    }
    fresh.close().await;
    Ok(w.report)
}

/// A branch of random bytes another host published, open on a host that read only some chunks
/// of its first frame, which `local` marks, with the store wait at [`STORE_WAIT`].
async fn a_host_of_a_published_branch(seed: u64) -> Result<(World, Volume, Vec<bool>), String> {
    let (ports, store, fs) = sim::ports(seed);
    Volume::create(&ports, BRANCH, u64_from(SIZE))
        .await
        .map_err(|e| e.to_string())?;
    let mut w = World {
        ports,
        store,
        fs,
        rng: SimRng::new(seed ^ 0x0a7),
        model: vec![0; SIZE],
        flushed: vec![0; SIZE],
        report: Report::default(),
    };
    let mut x = seed | 1;
    for b in &mut w.model {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = x.to_le_bytes()[0];
    }
    let writer = open(&w.ports, "/writer").await?;
    writer
        .write_at(&w.model, 0)
        .await
        .map_err(|e| format!("seed {seed}: write the branch: {e}"))?;
    writer
        .checkpoint()
        .await
        .map_err(|e| format!("seed {seed}: publish the branch: {e}"))?;
    writer.close().await;
    let v = open(&w.ports, "/host").await?;
    v.set_store_wait(STORE_WAIT);
    let mut local = vec![false; SIZE / CHUNK];
    for (c, l) in local.iter_mut().enumerate().take(FRAME_CHUNKS) {
        if c == 0 || w.below(2) == 0 {
            w.check(&v, c * CHUNK, CHUNK, &format!("seed {seed} before the outage"))
                .await?;
            *l = true;
        }
    }
    Ok((w, v, local))
}

/// `op`, failed when it outlasts twice the store wait: device I/O must not hang.
async fn bounded<T>(op: impl Future<Output = T>, at: &str) -> Result<T, String> {
    tokio::time::timeout(2 * STORE_WAIT, op)
        .await
        .map_err(|_| format!("{at}: hung while the store did not answer"))
}

fn mark(local: &mut [bool], chunks: RangeInclusive<usize>) {
    for c in chunks {
        local[c] = true;
    }
}

impl World {
    /// Whether device I/O that `needs_store`, or not, and started at `t0`, did what it must
    /// during an outage: done when it needs nothing from the store, and otherwise either done
    /// or failed as unanswered within the store wait.
    fn settled(
        &mut self,
        r: Result<(), Error>,
        needs_store: bool,
        t0: Instant,
        at: &str,
        what: &str,
    ) -> Result<bool, String> {
        match r {
            Ok(()) => Ok(true),
            Err(e) if needs_store && matches!(e.kind(), Error::Store(StoreError::Transient { .. })) => {
                if t0.elapsed() > STORE_WAIT {
                    return Err(format!("{at}: {what} failed after {:?}", t0.elapsed()));
                }
                self.report.unanswered += 1;
                Ok(false)
            }
            Err(e) => Err(format!("{at}: {what}: {e}")),
        }
    }
}

/// The size of the branch [`a_cache_over_its_budget`] reads, eight times its cache budget.
const BUDGETED_SIZE: usize = 32 << 20;
const BUDGET: u64 = 4 << 20;
/// The end of the branch that [`a_cache_over_its_budget`] rewrites: half the budget.
const REWRITTEN: usize = 2 << 20;
/// The most reads [`a_cache_over_its_budget`] sends at once.
const READS_AT_ONCE: usize = 8;

async fn open_budgeted(ports: &Ports, dir: &str) -> Result<Volume, String> {
    let cache = Cache::open(ports, Path::new(dir))
        .await
        .map_err(|e| format!("open cache: {e}"))?;
    cache.set_budget(BUDGET);
    Volume::open(ports, Arc::new(cache), Path::new(dir), BRANCH)
        .await
        .map_err(|e| format!("open volume: {e}"))
}

impl World {
    /// A range of up to two chunks anywhere in the branch of [`a_cache_over_its_budget`].
    fn budgeted_range(&self) -> (usize, usize) {
        let off = if self.below(2) == 0 {
            self.below(BUDGETED_SIZE / CHUNK) * CHUNK
        } else {
            self.below(BUDGETED_SIZE)
        };
        (off, 1 + self.below(2 * CHUNK).min(BUDGETED_SIZE - off - 1))
    }

    /// Up to [`READS_AT_ONCE`] reads at once, each checked against the model.
    async fn check_at_once(&mut self, v: &Volume, at: &str) -> Result<(), String> {
        let ranges: Vec<(usize, usize)> = (0..=self.below(READS_AT_ONCE))
            .map(|_| self.budgeted_range())
            .collect();
        let reads = ranges.iter().map(|&(off, len)| async move {
            let mut got = vec![0; len];
            v.read_at(&mut got, u64_from(off)).await.map(|()| got)
        });
        for (got, &(off, len)) in futures::future::join_all(reads).await.into_iter().zip(&ranges) {
            let got = got.map_err(|e| format!("{at}: read {len} at {off}: {e}"))?;
            self.report.reads += 1;
            if got != self.model[off..off + len] {
                return Err(format!("{at}: read {len} at {off} differs from the model"));
            }
        }
        Ok(())
    }
}

/// A host writes and publishes a branch eight times its cache budget, then runs `steps`
/// random steps on it as the soak does: reads at once from anywhere in the branch, checked
/// rewrites of its end, checkpoints that let sweeps drop what was rewritten, restarts and host
/// crashes. A fresh host then reads the branch whole.
pub async fn a_cache_over_its_budget(seed: u64, steps: usize) -> Result<Report, String> {
    let (ports, store, fs) = sim::ports(seed);
    Volume::create(&ports, BRANCH, u64_from(BUDGETED_SIZE))
        .await
        .map_err(|e| e.to_string())?;
    let mut w = World {
        ports,
        store,
        fs,
        rng: SimRng::new(seed ^ 0xb0d6),
        model: Vec::new(),
        flushed: Vec::new(),
        report: Report::default(),
    };
    w.model = w.bytes(BUDGETED_SIZE);
    let writer = open(&w.ports, "/host").await?;
    writer
        .write_at(&w.model, 0)
        .await
        .map_err(|e| format!("seed {seed}: write the branch: {e}"))?;
    writer
        .checkpoint()
        .await
        .map_err(|e| format!("seed {seed}: publish the branch: {e}"))?;
    writer.close().await;
    drop(writer);
    w.flushed.clone_from(&w.model);
    let mut v = open_budgeted(&w.ports, "/host").await?;
    for step in 0..steps {
        let at = format!("seed {seed} step {step}");
        match w.below(100) {
            0..50 => w.check_at_once(&v, &at).await?,
            50..70 => {
                let off = BUDGETED_SIZE - REWRITTEN + w.below(REWRITTEN - 1);
                let len = 1 + w.below(2 * CHUNK).min(BUDGETED_SIZE - off - 1);
                let b = w.bytes(len);
                v.write_at(&b, u64_from(off))
                    .await
                    .map_err(|e| format!("{at}: write: {e}"))?;
                w.model[off..off + len].copy_from_slice(&b);
                w.report.writes += 1;
            }
            70..80 => {
                v.checkpoint()
                    .await
                    .map_err(|e| format!("{at}: checkpoint: {e}"))?;
                w.flushed.clone_from(&w.model);
                w.report.checkpoints += 1;
            }
            80..85 => {
                v.flush().await.map_err(|e| format!("{at}: flush: {e}"))?;
                w.flushed.clone_from(&w.model);
                w.report.flushes += 1;
            }
            85..95 => {
                v.close().await;
                drop(v);
                v = open_budgeted(&w.ports, "/host")
                    .await
                    .map_err(|e| format!("{at}: restart: {e}"))?;
                w.report.restarts += 1;
            }
            _ => {
                v.close().await;
                drop(v);
                w.fs.crash();
                w.model.clone_from(&w.flushed);
                v = open_budgeted(&w.ports, "/host")
                    .await
                    .map_err(|e| format!("{at}: after crash: {e}"))?;
                w.report.crashes += 1;
            }
        }
    }
    v.checkpoint()
        .await
        .map_err(|e| format!("seed {seed} final checkpoint: {e}"))?;
    v.close().await;
    let fresh = open(&w.ports, "/fresh").await?;
    for off in (0..BUDGETED_SIZE).step_by(16 * CHUNK) {
        let mut got = vec![0; 16 * CHUNK];
        fresh
            .read_at(&mut got, u64_from(off))
            .await
            .map_err(|e| format!("seed {seed} fresh host: read at {off}: {e}"))?;
        if got != w.model[off..off + 16 * CHUNK] {
            return Err(format!(
                "seed {seed} fresh host: read at {off} differs from the model"
            ));
        }
    }
    fresh.close().await;
    Ok(w.report)
}
