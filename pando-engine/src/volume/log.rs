//! The write log: each committed flush, shipped to the store as soon as it is local.
//!
//! A shipper merges the flushes that wait into one record and creates it at the writer's next
//! record number, one record at a time. An upload that publishes a root records in the ref how
//! far the log is covered, and the shipper then deletes the covered records. An open that does
//! not resume local state reads the records its ref does not cover, from the ref's log position
//! up to the last writer, applies them as writes and seals each writer's log at its first free
//! number, so a writer still running fails its next append. A record that would leave a gap is
//! not applied, nor anything after it, so what an open applies is a prefix of the flushes.

use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use pando_format::{
    CHUNK_SIZE, Hash, LOG_CHUNKS_MAX, LOG_DATA_CHUNKS_MAX, LOG_SEAL_LEN, LogBody, LogPos, LogRecord,
    LogWrites, Ref, u64_from,
};
use tokio::sync::{Notify, watch};

use super::{Inner, RETRY_AFTER, UPLOAD_EVERY, backoff, fail_with, is_transient};
use crate::ports::{Cpu, CpuClass, on_cpu};
use crate::remote::Remote;
use crate::{Error, Result};

/// How a volume logs its flushes to the store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LogMode {
    /// No log: what no upload has published yet is only on this host.
    Off,
    /// A flush returns once it is durable on this host; the log in the store has it moments
    /// later.
    #[default]
    Behind,
    /// A flush returns once its writes are in the log in the store, or published.
    Strict,
}

/// How long a write-behind shipper gathers flushes into one record.
const GATHER: Duration = Duration::from_millis(50);
/// The chunk bytes one record holds at most; a larger flush is left to the uploader.
const RECORD_MAX: usize = LOG_DATA_CHUNKS_MAX * CHUNK_SIZE;
/// The chunk bytes that may wait for the log; past it, waiting flushes are left to the uploader.
const PENDING_MAX: usize = 32 << 20;
/// Records an open reads at most.
const REPLAY_MAX: u64 = 100_000;
/// How often an open tries each read and seal of the log when the store fails transiently.
const READ_TRIES: usize = 10;

/// A committed flush waiting for the log: its number, the branch's size, and its chunks.
#[derive(Debug)]
pub(super) struct Flushed {
    pub(super) seq: u64,
    pub(super) size: u64,
    pub(super) chunks: Vec<(u64, Option<(Hash, Bytes)>)>,
}

#[derive(Debug, Default)]
struct Queue {
    pending: VecDeque<Flushed>,
    bytes: usize,
    /// Flushes through this one left the queue unlogged; only an upload covers them.
    dropped: u64,
    /// The last flush of each record this writer created or is creating, by record number. A
    /// number is taken before its create starts, so a log position counts a record in flight.
    records: Vec<u64>,
    /// Records below this number are deleted.
    trimmed: u64,
    /// Records below this number hold only published flushes.
    covered: u64,
    published: bool,
    older_trimmed: bool,
}

/// The log of one writer.
#[derive(Debug)]
pub(super) struct Log {
    mode: LogMode,
    queue: Mutex<Queue>,
    wake: Notify,
    /// The last flush the log holds, with every flush before it.
    logged: watch::Sender<u64>,
    /// The log takes no more flushes. It changes only under the queue's lock.
    stopped: watch::Sender<bool>,
    /// The shipper is creating a record. It turns on only under the queue's lock while the log
    /// is not stopped, so once the log stops and it is off, no record is created any more.
    shipping: watch::Sender<bool>,
    /// Where this open's read of the log started; the generation take writes it to the ref.
    pub(super) start: LogPos,
}

impl Log {
    pub(super) fn new(mode: LogMode, start: LogPos) -> Log {
        Log {
            mode,
            queue: Mutex::default(),
            wake: Notify::new(),
            logged: watch::Sender::new(0),
            stopped: watch::Sender::new(false),
            shipping: watch::Sender::new(false),
            start,
        }
    }

    pub(super) fn mode(&self) -> LogMode {
        self.mode
    }
}

/// Marks a record in flight in [`Log::shipping`] until it drops, so a shipper that ends in any
/// way does not leave a close waiting.
struct Shipping<'a>(&'a watch::Sender<bool>);

impl<'a> Shipping<'a> {
    fn start(flag: &'a watch::Sender<bool>) -> Shipping<'a> {
        flag.send_replace(true);
        Shipping(flag)
    }
}

impl Drop for Shipping<'_> {
    fn drop(&mut self) {
        self.0.send_replace(false);
    }
}

fn chunk_bytes(f: &Flushed) -> usize {
    f.chunks.iter().filter(|c| c.1.is_some()).count() * CHUNK_SIZE
}

impl Queue {
    /// Takes the flushes after `base` that fit one record, dropping those at or below it.
    /// `None` when flushes after `base` left the queue unlogged.
    fn take_batch(&mut self, base: u64) -> Option<Vec<Flushed>> {
        if self.dropped > base {
            return None;
        }
        let mut batch = Vec::new();
        let (mut bytes, mut count) = (0, 0);
        while let Some(f) = self.pending.front() {
            let n = chunk_bytes(f);
            if f.seq > base
                && !batch.is_empty()
                && (bytes + n > RECORD_MAX || count + f.chunks.len() > LOG_CHUNKS_MAX)
            {
                break;
            }
            let Some(f) = self.pending.pop_front() else {
                break;
            };
            self.bytes -= n;
            if f.seq > base {
                bytes += n;
                count += f.chunks.len();
                batch.push(f);
            }
        }
        Some(batch)
    }
}

/// The chunks `batch` changed, each as of the last flush that changed it, in index order.
fn merged(batch: Vec<Flushed>) -> Vec<(u64, Option<(Hash, Bytes)>)> {
    let mut merged: HashMap<u64, Option<(Hash, Bytes)>> = HashMap::new();
    for f in batch {
        merged.extend(f.chunks);
    }
    let mut chunks: Vec<_> = merged.into_iter().collect();
    chunks.sort_by_key(|c| c.0);
    chunks
}

/// What an open applies from the log: where its read started, and the writes in order.
#[derive(Debug, Default)]
pub(super) struct Scan {
    pub(super) start: LogPos,
    pub(super) writes: Vec<LogWrites>,
}

/// Where ref `r` says the log stands; a ref from before the log has read none of it.
fn log_start(r: &Ref) -> LogPos {
    r.log.unwrap_or_else(|| LogPos {
        generation: r.generation,
        ..LogPos::default()
    })
}

impl Scan {
    /// The scan of a writer that resumes its local state over ref `r`, which reads nothing.
    pub(super) fn resumed(r: &Ref) -> Scan {
        Scan {
            start: log_start(r),
            writes: Vec::new(),
        }
    }
}

/// Reads the records of branch `name` that its ref `r` does not cover and seals every writer's
/// log it read.
pub(super) async fn scan(remote: &Remote, cpu: &Arc<dyn Cpu>, name: &str, r: &Ref) -> Result<Scan> {
    let start = log_start(r);
    let mut out = Scan {
        start,
        writes: Vec::new(),
    };
    if r.generation == 0 || (start.closed && start.generation == r.generation) {
        return Ok(out);
    }
    let mut gap = false;
    let mut read = 0;
    for g in start.generation.max(1)..=r.generation {
        if g == start.generation && start.closed {
            continue;
        }
        let (mut n, mut have) = if g == start.generation {
            (start.next, start.flush)
        } else {
            (0, 0)
        };
        loop {
            read += 1;
            if read > REPLAY_MAX {
                return Err(Error::Corrupt(format!(
                    "{name}: more than {REPLAY_MAX} log records to read"
                )));
            }
            let Some(b) = retried(|| remote.get_log_record(name, g, n)).await? else {
                let seal = Bytes::from(LogRecord::encode_seal(g, n));
                if retried(|| remote.create_log_record(name, g, n, seal.clone())).await? {
                    break;
                }
                continue;
            };
            let rec = on_cpu(cpu.as_ref(), CpuClass::Foreground, move || LogRecord::decode(&b)).await?;
            if (rec.generation, rec.n) != (g, n) {
                return Err(Error::Corrupt(format!(
                    "{name}: log record {g}/{n} names {}/{}",
                    rec.generation, rec.n
                )));
            }
            let LogBody::Writes(w) = rec.body else {
                break;
            };
            n += 1;
            if w.to <= have {
                continue;
            }
            if w.from > have {
                gap = true;
            }
            if !gap {
                have = w.to;
                out.writes.push(w);
            }
        }
    }
    Ok(out)
}

/// `f`, tried again while the store fails transiently, a few times.
async fn retried<T, F: Future<Output = Result<T>>>(mut f: impl FnMut() -> F) -> Result<T> {
    let mut tries = 1;
    loop {
        match f().await {
            Err(e) if tries < READ_TRIES && is_transient(&e) => {
                tries += 1;
                tokio::time::sleep(RETRY_AFTER).await;
            }
            r => return r,
        }
    }
}

impl Inner {
    /// Hands a committed flush to the shipper; without `logged`, it and the flushes waiting
    /// before it are left to the uploader.
    pub(super) fn queue_flush(&self, f: Flushed, logged: bool) {
        if self.log.mode == LogMode::Off {
            return;
        }
        let n = chunk_bytes(&f);
        let mut q = self.log.queue.lock();
        if !logged || n > RECORD_MAX || f.chunks.len() > LOG_CHUNKS_MAX || q.bytes + n > PENDING_MAX {
            q.pending.clear();
            q.bytes = 0;
            q.dropped = f.seq;
        } else {
            q.bytes += n;
            q.pending.push_back(f);
        }
        drop(q);
        self.log.wake.notify_one();
    }

    /// Waits until flush `seq` is in the log or published; an error when this writer was
    /// fenced or stopped first.
    pub(super) async fn wait_logged(&self, seq: u64) -> Result<()> {
        let mut logged = self.log.logged.subscribe();
        let mut uploaded = self.uploaded_seq.subscribe();
        let mut changed = self.changed.subscribe();
        let mut stop = self.stop.subscribe();
        loop {
            if *logged.borrow_and_update() >= seq || *uploaded.borrow_and_update() >= seq {
                return Ok(());
            }
            if self.up.lock().fenced {
                return Err(Error::Fenced {
                    branch: self.name.clone(),
                });
            }
            tokio::select! {
                _ = logged.changed() => {}
                _ = uploaded.changed() => {}
                _ = changed.changed() => {}
                _ = stop.wait_for(|s| *s) => return Err(Error::Cancelled),
            }
        }
    }

    /// The log position of a root that holds flushes up to `seq`.
    pub(super) fn log_pos(&self, seq: u64) -> LogPos {
        let q = self.log.queue.lock();
        LogPos {
            generation: self.generation,
            flush: seq,
            next: q.records.partition_point(|to| *to <= seq) as u64,
            closed: self.log.mode == LogMode::Off || *self.log.stopped.borrow(),
        }
    }

    /// Notes that the store's ref now has log position `pos`, so the records it covers can go.
    pub(super) fn log_published(&self, pos: &LogPos) {
        let mut q = self.log.queue.lock();
        q.covered = q.covered.max(pos.next);
        q.published = true;
        drop(q);
        self.log.wake.notify_one();
    }

    /// Stops the log, waits for a record in flight, and records in the ref that this writer
    /// appends no more records. Only once every flush is published; otherwise it does nothing.
    pub(super) async fn close_log(&self) -> Result<()> {
        let seq = self.mem.lock().flush_seq;
        if *self.uploaded_seq.borrow() < seq {
            return Ok(());
        }
        {
            let _q = self.log.queue.lock();
            self.log.stopped.send_replace(true);
        }
        self.log.wake.notify_one();
        let _ = self.log.shipping.subscribe().wait_for(|s| !*s).await;
        let pos = self.log_pos(seq);
        let (r, version) = {
            let up = self.up.lock();
            (
                Ref {
                    size: up.published_size,
                    root: up.published,
                    generation: self.generation,
                    from: self.from.clone(),
                    log: Some(pos),
                },
                up.version.clone(),
            )
        };
        let v = self.remote.put_ref(&self.name, &r, &version).await?;
        self.up.lock().version = v;
        self.log_published(&pos);
        self.trim_log().await;
        Ok(())
    }

    /// Applies writes read from the log, in order, then commits them as one flush.
    pub(super) async fn replay_log(&self, writes: Vec<LogWrites>) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let zeros = vec![0; CHUNK_SIZE];
        for w in writes {
            if w.size > self.size() {
                self.grow(w.size).await?;
            }
            for (idx, c) in w.chunks {
                let data: &[u8] = c.as_ref().map_or(&zeros, |v| v.bytes());
                self.write_chunk(idx, 0, data, true, false).await?;
            }
        }
        self.commit_flush().await.map(drop)
    }

    /// Waits until this writer has taken the branch; false when it is fenced or stopping.
    async fn taken(&self, stop: &mut watch::Receiver<bool>) -> bool {
        let mut changed = self.changed.subscribe();
        loop {
            let (taken, fenced) = {
                let up = self.up.lock();
                (up.gen_taken, up.fenced)
            };
            if fenced {
                return false;
            }
            if taken {
                return true;
            }
            tokio::select! {
                _ = stop.wait_for(|s| *s) => return false,
                _ = changed.changed() => {}
            }
        }
    }

    /// Creates record `n` of this writer with `body`, retrying until the store answers; false
    /// when the writer is fenced or stopping, or the log stopped after a failed try.
    async fn put_record(&self, n: u64, body: Bytes, stop: &mut watch::Receiver<bool>) -> bool {
        let mut stopped = self.log.stopped.subscribe();
        let mut failed = 0;
        loop {
            match self
                .remote
                .create_log_record(&self.name, self.generation, n, body.clone())
                .await
            {
                Ok(true) => return true,
                Ok(false) => {
                    let mut up = self.up.lock();
                    up.fenced = true;
                    fail_with(
                        &mut up,
                        Error::Fenced {
                            branch: self.name.clone(),
                        },
                    );
                    drop(up);
                    self.notify();
                    return false;
                }
                Err(_) => {
                    failed += 1;
                    tokio::select! {
                        _ = stop.wait_for(|s| *s) => return false,
                        _ = stopped.wait_for(|s| *s) => return false,
                        () = tokio::time::sleep(backoff(failed, self.rng.as_ref())) => {}
                    }
                }
            }
        }
    }

    pub(super) async fn shipper(self: Arc<Self>) {
        let mut stop = self.stop.subscribe();
        if !self.taken(&mut stop).await {
            return;
        }
        let mut uploaded = self.uploaded_seq.subscribe();
        loop {
            self.trim_log().await;
            let (waiting, stopped) = {
                let q = self.log.queue.lock();
                (
                    !q.pending.is_empty() || q.dropped > *self.log.logged.borrow(),
                    *self.log.stopped.borrow(),
                )
            };
            if !waiting || stopped {
                tokio::select! {
                    _ = stop.wait_for(|s| *s) => return,
                    () = self.log.wake.notified() => {}
                    () = tokio::time::sleep(UPLOAD_EVERY) => {}
                }
                continue;
            }
            if self.log.mode == LogMode::Behind {
                tokio::select! {
                    _ = stop.wait_for(|s| *s) => return,
                    () = tokio::time::sleep(GATHER) => {}
                }
            }
            let base = (*self.log.logged.borrow()).max(*uploaded.borrow_and_update());
            let batch = self.log.queue.lock().take_batch(base);
            let Some(batch) = batch else {
                tokio::select! {
                    _ = stop.wait_for(|s| *s) => return,
                    _ = uploaded.changed() => {}
                }
                continue;
            };
            let Some(last) = batch.last() else {
                self.log.logged.send_if_modified(|l| {
                    let more = base > *l;
                    *l = (*l).max(base);
                    more
                });
                continue;
            };
            let (to, size) = (last.seq, last.size);
            let chunks = merged(batch);
            let n = u64_from(self.log.queue.lock().records.len());
            let generation = self.generation;
            let class = if self.log.mode == LogMode::Strict {
                CpuClass::Foreground
            } else {
                CpuClass::Background
            };
            let body = match on_cpu(self.cpu.as_ref(), class, move || {
                LogRecord::encode_writes(generation, n, base, to, size, &chunks)
            })
            .await
            {
                Ok(b) => Bytes::from(b),
                Err(e) => {
                    let mut up = self.up.lock();
                    fail_with(&mut up, e.into());
                    return;
                }
            };
            let shipping = {
                let mut q = self.log.queue.lock();
                if *self.log.stopped.borrow() {
                    continue;
                }
                q.records.push(to);
                Shipping::start(&self.log.shipping)
            };
            let created = self.put_record(n, body, &mut stop).await;
            drop(shipping);
            if !created {
                return;
            }
            self.log.logged.send_modify(|l| *l = (*l).max(to));
        }
    }

    /// Deletes the records the published root covers, and once this writer has published,
    /// what earlier writers left in the log except the seals that fence them. Best effort.
    async fn trim_log(&self) {
        let (from, to, older) = {
            let mut q = self.log.queue.lock();
            let older = q.published && !q.older_trimmed;
            q.older_trimmed |= older;
            (q.trimmed, q.covered, older)
        };
        let mut done = from;
        for n in from..to {
            if self
                .remote
                .delete_log_record(&self.name, self.generation, n)
                .await
                .is_err()
            {
                break;
            }
            done = n + 1;
        }
        self.log.queue.lock().trimmed = done;
        if older && let Ok(all) = self.remote.list_log(&self.name).await {
            let mut last: BTreeMap<u64, u64> = BTreeMap::new();
            for &(g, n, _) in &all {
                let l = last.entry(g).or_insert(n);
                *l = (*l).max(n);
            }
            for (g, n, size) in all.into_iter().filter(|r| r.0 < self.generation) {
                if last.get(&g) == Some(&n) && size == u64_from(LOG_SEAL_LEN) {
                    continue;
                }
                let _ = self.remote.delete_log_record(&self.name, g, n).await;
            }
        }
    }
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use std::path::Path;

    use pando_format::{Entry, keys};

    use super::*;
    use crate::Volume;
    use crate::cache::Cache;
    use crate::ports::Ports;
    use crate::sim::{self, Fault, SimStore, StoreOp};

    const SIZE: u64 = 16 << 20;

    async fn open(ports: &Ports, dir: &str, mode: LogMode) -> Volume {
        let cache = Arc::new(Cache::open(ports, &Path::new(dir).join("cache")).await.unwrap());
        let v = Volume::open_logged(ports, cache, Path::new(dir), "main", mode)
            .await
            .unwrap();
        v.wait_generation().await.unwrap();
        v
    }

    fn data(seed: u8) -> Vec<u8> {
        (0..CHUNK_SIZE + 100)
            .map(|i| u8::try_from(i % 256).unwrap().wrapping_mul(7).wrapping_add(seed))
            .collect()
    }

    async fn read(v: &Volume, n: usize) -> Vec<u8> {
        let mut b = vec![0; n];
        v.read_at(&mut b, 1000).await.unwrap();
        b
    }

    fn block_uploads(store: &SimStore) {
        for _ in 0..10_000 {
            store.inject(Fault {
                op: StoreOp::Update,
                applied: false,
                permanent: false,
            });
        }
    }

    fn log_keys(store: &SimStore) -> Vec<String> {
        store.keys(&keys::log_branch("main"))
    }

    async fn reference(ports: &Ports) -> Ref {
        Remote::new(Arc::clone(&ports.store))
            .get_ref("main")
            .await
            .unwrap()
            .unwrap()
            .0
    }

    #[tokio::test(start_paused = true)]
    async fn another_host_gets_logged_writes_no_upload_published() {
        let (ports, store, _) = sim::ports(1);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let a = open(&ports, "/a", LogMode::Behind).await;
        block_uploads(&store);
        a.write_at(&data(1), 1000).await.unwrap();
        a.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        a.close().await;
        store.clear_faults();
        assert_eq!(reference(&ports).await.root, Entry::EMPTY);

        let b = open(&ports, "/b", LogMode::Behind).await;
        assert_eq!(read(&b, data(1).len()).await, data(1));
        b.checkpoint().await.unwrap();
        b.close_log().await.unwrap();
        assert_eq!(log_keys(&store), [keys::log_record("main", 1, 1)], "a's seal");
        b.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_strict_flush_returns_once_the_store_has_it() {
        let (ports, store, _) = sim::ports(2);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let a = open(&ports, "/a", LogMode::Strict).await;
        block_uploads(&store);
        store.set_latency(Duration::from_millis(10));
        a.write_at(&data(2), 1000).await.unwrap();
        let t0 = tokio::time::Instant::now();
        a.flush().await.unwrap();
        assert!(t0.elapsed() >= Duration::from_millis(10));
        assert!(t0.elapsed() < Duration::from_millis(100), "{:?}", t0.elapsed());
        a.close().await;
        store.clear_faults();

        let b = open(&ports, "/b", LogMode::Strict).await;
        assert_eq!(read(&b, data(2).len()).await, data(2));
        b.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_strict_flush_after_commits_that_bound_memory_returns_once_uploaded() {
        let (ports, store, _) = sim::ports(9);
        Volume::create(&ports, "main", 4 * SIZE).await.unwrap();
        let a = open(&ports, "/a", LogMode::Strict).await;
        let w: Vec<u8> = (0..3 * super::super::DIRTY_MAX / 2)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        for (i, part) in w.chunks(1 << 20).enumerate() {
            a.write_at(part, u64_from(i << 20)).await.unwrap();
        }
        a.write_at(&data(4), 1000).await.unwrap();
        a.flush().await.unwrap();
        assert_eq!(a.published(), a.root(), "an upload, not the log, has the flush");
        assert_eq!(log_keys(&store), Vec::<String>::new());
        a.close().await;

        let b = open(&ports, "/b", LogMode::Strict).await;
        assert_eq!(read(&b, data(4).len()).await, data(4));
        let mut got = vec![0; w.len() - 2 * CHUNK_SIZE];
        b.read_at(&mut got, u64_from(2 * CHUNK_SIZE)).await.unwrap();
        assert!(got == w[2 * CHUNK_SIZE..], "the writes before the flush");
        b.close().await;
    }

    async fn a_strict_flush_fails_in_time_while_the_store_does_not_answer() {
        let (ports, store, _) = sim::ports(9);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let a = open(&ports, "/a", LogMode::Strict).await;
        a.set_store_wait(Duration::from_secs(5));
        a.write_at(&data(9), 1000).await.unwrap();
        store.set_answering(false);
        let t0 = tokio::time::Instant::now();
        let e = a.flush().await.unwrap_err();
        assert!(is_transient(&e), "{e}");
        assert_eq!(t0.elapsed(), Duration::from_secs(5));
        assert_eq!(read(&a, data(9).len()).await, data(9));
        store.set_answering(true);
        a.flush().await.unwrap();
        a.close().await;

        let b = open(&ports, "/b", LogMode::Strict).await;
        assert_eq!(read(&b, data(9).len()).await, data(9));
        b.close().await;
    }

    #[test]
    fn retries_back_off_with_jitter_up_to_a_cap() {
        let rng = sim::SimRng::new(5);
        for failed in 1..40 {
            let full = RETRY_AFTER
                .saturating_mul(1 << (failed - 1).min(16))
                .min(super::super::RETRY_MAX);
            for _ in 0..50 {
                let d = backoff(failed, &rng);
                assert!(d >= full / 2 && d <= full, "{failed}: {d:?} for {full:?}");
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn writers_that_die_one_after_another_lose_nothing_logged() {
        let (ports, store, _) = sim::ports(3);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let a = open(&ports, "/a", LogMode::Strict).await;
        block_uploads(&store);
        a.write_at(&data(3), 1000).await.unwrap();
        a.flush().await.unwrap();
        a.close().await;
        store.clear_faults();

        let b = open(&ports, "/b", LogMode::Strict).await;
        block_uploads(&store);
        b.write_at(&data(4)[..100], 1000 + CHUNK_SIZE as u64 * 3)
            .await
            .unwrap();
        b.flush().await.unwrap();
        b.close().await;
        store.clear_faults();

        let c = open(&ports, "/c", LogMode::Behind).await;
        assert_eq!(read(&c, data(3).len()).await, data(3));
        let mut got = vec![0; 100];
        c.read_at(&mut got, 1000 + CHUNK_SIZE as u64 * 3).await.unwrap();
        assert_eq!(got, data(4)[..100]);
        c.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_writer_whose_log_was_sealed_is_fenced() {
        let (ports, _, _) = sim::ports(4);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let a = open(&ports, "/a", LogMode::Strict).await;
        a.write_at(&data(5), 1000).await.unwrap();
        a.flush().await.unwrap();

        let b = open(&ports, "/b", LogMode::Strict).await;
        a.write_at(&data(6), 1000).await.unwrap();
        assert!(a.flush().await.is_err());
        assert_eq!(read(&b, data(5).len()).await, data(5));
        a.close().await;
        b.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn one_open_at_a_time_writes_from_a_local_state() {
        let (ports, _, _) = sim::ports(11);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let a = open(&ports, "/a", LogMode::Behind).await;
        let cache = Arc::new(Cache::open(&ports, Path::new("/x/cache")).await.unwrap());
        let again = Volume::open_logged(&ports, cache, Path::new("/a"), "main", LogMode::Behind).await;
        assert!(again.is_err());
        a.close().await;
        drop(a);
        open(&ports, "/a", LogMode::Behind).await.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_seal_outlasts_the_trim_of_the_writer_it_fenced() {
        let (ports, store, _) = sim::ports(9);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let a = open(&ports, "/a", LogMode::Strict).await;
        block_uploads(&store);
        a.write_at(&data(5), 1000).await.unwrap();
        a.flush().await.unwrap();
        store.clear_faults();

        let b = open(&ports, "/b", LogMode::Strict).await;
        b.write_at(&data(6), 1000).await.unwrap();
        b.checkpoint().await.unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
        let a_log = format!("{}/{:020}/", keys::log_branch("main"), 1);
        assert_eq!(
            log_keys(&store).iter().filter(|k| k.starts_with(&a_log)).count(),
            1,
            "only the seal of a's log is left: {:?}",
            log_keys(&store)
        );
        a.write_at(&data(7), 1000).await.unwrap();
        assert!(a.flush().await.is_err());
        assert_eq!(read(&b, data(6).len()).await, data(6));
        a.close().await;
        b.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_record_whose_create_reply_was_lost_is_the_writers_own() {
        let (ports, store, _) = sim::ports(10);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let a = open(&ports, "/a", LogMode::Strict).await;
        block_uploads(&store);
        store.inject(Fault {
            op: StoreOp::Create,
            applied: true,
            permanent: false,
        });
        store.inject(Fault {
            op: StoreOp::Get,
            applied: false,
            permanent: false,
        });
        a.write_at(&data(8), 1000).await.unwrap();
        a.flush().await.unwrap();
        a.write_at(&data(9), 1000).await.unwrap();
        a.flush().await.unwrap();
        a.close().await;
        store.clear_faults();

        let b = open(&ports, "/b", LogMode::Strict).await;
        assert_eq!(read(&b, data(9).len()).await, data(9));
        b.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_clean_close_leaves_nothing_for_the_next_open_to_read() {
        let (ports, store, _) = sim::ports(5);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let a = open(&ports, "/a", LogMode::Behind).await;
        a.write_at(&data(7), 1000).await.unwrap();
        a.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        a.checkpoint().await.unwrap();
        a.close_log().await.unwrap();
        a.close().await;
        let r = reference(&ports).await;
        assert!(
            r.log.is_some_and(|l| l.closed && l.generation == r.generation),
            "{r:?}"
        );
        assert!(log_keys(&store).is_empty(), "{:?}", log_keys(&store));

        store.set_latency(Duration::from_millis(10));
        let t0 = tokio::time::Instant::now();
        let cache = Arc::new(Cache::open(&ports, Path::new("/b/cache")).await.unwrap());
        let b = Volume::open_then(&ports, cache, Path::new("/b"), "main")
            .await
            .unwrap();
        assert_eq!(t0.elapsed(), Duration::from_millis(10), "one GET of the ref");
        assert!(log_keys(&store).is_empty(), "no seal");
        assert_eq!(read(&b, data(7).len()).await, data(7));
        b.close().await;
    }

    async fn record_waits_while_published(ports: &Ports, store: &SimStore, seed: u8) -> Volume {
        Volume::create(ports, "main", SIZE).await.unwrap();
        let a = open(ports, "/a", LogMode::Behind).await;
        let reference = keys::reference("main");
        store.hold(StoreOp::Update, &reference);
        store.hold(StoreOp::Create, &keys::log_branch("main"));
        a.write_at(&data(seed), 1000).await.unwrap();
        a.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        store.release(StoreOp::Update, &reference);
        a.checkpoint().await.unwrap();
        a
    }

    async fn assert_closed_clean(ports: &Ports, store: &SimStore, seed: u8) {
        assert!(log_keys(store).is_empty(), "{:?}", log_keys(store));
        let r = reference(ports).await;
        assert!(
            r.log
                .is_some_and(|l| l.closed && l.generation == r.generation && l.next == 1),
            "{r:?}"
        );
        let b = open(ports, "/b", LogMode::Behind).await;
        assert_eq!(read(&b, data(seed).len()).await, data(seed));
        b.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_record_created_while_the_log_closes_is_trimmed() {
        let (ports, store, _) = sim::ports(12);
        let a = record_waits_while_published(&ports, &store, 12).await;
        let (closed, ()) = tokio::join!(a.close_log(), async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            store.release(StoreOp::Create, &keys::log_branch("main"));
        });
        closed.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        a.close().await;
        assert_closed_clean(&ports, &store, 12).await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_close_ends_the_retries_of_a_record() {
        let (ports, store, _) = sim::ports(13);
        let a = record_waits_while_published(&ports, &store, 13).await;
        store.inject(Fault {
            op: StoreOp::Create,
            applied: false,
            permanent: false,
        });
        let t0 = tokio::time::Instant::now();
        let (closed, ()) = tokio::join!(a.close_log(), async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            store.release(StoreOp::Create, &keys::log_branch("main"));
        });
        closed.unwrap();
        assert!(t0.elapsed() < RETRY_AFTER / 2, "{:?}", t0.elapsed());
        tokio::time::sleep(Duration::from_secs(5)).await;
        a.close().await;
        assert_closed_clean(&ports, &store, 13).await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_volume_without_a_log_writes_no_records() {
        let (ports, store, _) = sim::ports(6);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let a = open(&ports, "/a", LogMode::Off).await;
        a.write_at(&data(8), 1000).await.unwrap();
        a.checkpoint().await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(log_keys(&store), Vec::<String>::new());
        a.close().await;
        let r = reference(&ports).await;
        assert!(r.log.is_some_and(|l| l.closed));
    }

    #[tokio::test(start_paused = true)]
    async fn a_log_gap_stops_the_replay_at_the_writes_before_it() {
        let (ports, store, _) = sim::ports(7);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let a = open(&ports, "/a", LogMode::Strict).await;
        block_uploads(&store);
        a.write_at(&data(9), 1000).await.unwrap();
        a.flush().await.unwrap();
        a.write_at(&data(10), 1000).await.unwrap();
        a.flush().await.unwrap();
        a.write_at(&data(11), 1000).await.unwrap();
        a.flush().await.unwrap();
        a.close().await;
        store.clear_faults();
        let generation = reference(&ports).await.generation;
        Remote::new(Arc::clone(&ports.store))
            .delete_log_record("main", generation, 1)
            .await
            .unwrap();

        let b = open(&ports, "/b", LogMode::Behind).await;
        assert_eq!(read(&b, data(9).len()).await, data(9));
        b.close().await;
    }
}
