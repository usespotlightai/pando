use parking_lot::{Mutex, RwLock};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use pando_format::{CHUNK_SIZE, u64_from, usize_from};
use tokio::sync::oneshot;

use super::take;
use crate::ports::{LocalFile, LocalFs, Open};
use crate::{Error, Result};

/// What one journal record does to its chunk.
#[derive(Clone, Debug)]
pub(crate) enum Record {
    /// The bytes of one write, where the journal keeps them.
    Data(Extent),
    /// The whole chunk becomes zeros.
    ZeroChunk,
}

/// Where the journal keeps the bytes of one write: `len` bytes at `pos` of `file`, written at
/// offset `at` of the chunk. The file stays readable while an extent holds it, also after
/// [`Journal::drop_below`] removed it.
#[derive(Clone, Debug)]
pub(crate) struct Extent {
    file: Arc<dyn LocalFile>,
    pos: u64,
    at: u32,
    len: u32,
}

impl Extent {
    /// The first byte of the chunk it writes.
    pub(crate) fn at(&self) -> usize {
        self.at as usize
    }

    /// The byte of the chunk after the last one it writes.
    pub(crate) fn end(&self) -> usize {
        (self.at + self.len) as usize
    }

    /// The journal bytes its record takes.
    pub(crate) fn logged(&self) -> usize {
        HEADER + self.len as usize
    }

    /// The `n` bytes it wrote from offset `from` of the chunk, a range inside it.
    pub(crate) async fn read(&self, from: usize, n: usize) -> Result<Bytes> {
        let b = self
            .file
            .read_at(n, self.pos + u64_from(from - self.at()))
            .await?;
        if b.len() != n {
            return Err(Error::Corrupt(format!(
                "a journal record at {} is shorter than its write",
                self.pos
            )));
        }
        Ok(b)
    }

    /// [`Self::read`] into `dst` from the page cache only: `false` when it would wait.
    pub(crate) fn read_nowait(&self, from: usize, dst: &mut [u8]) -> Result<bool> {
        let at = self.pos + u64_from(from - self.at());
        Ok(self.file.read_at_nowait(dst, at)? == Some(dst.len()))
    }
}

const HEADER: usize = 16;
const SHARDS: usize = 16;
/// The bytes a replay reads at once.
const WINDOW: usize = 4 << 20;
/// Set in a record's length field: the record zeroes a whole chunk and carries no data.
const ZERO_CHUNK: u32 = 1 << 31;

/// Acknowledged writes that no flush has committed yet.
///
/// Records go to a local file without a sync before the write is acknowledged: a daemon crash
/// keeps them in the page cache, so the next daemon replays them. A host crash loses them, as
/// it would lose a disk's volatile write cache. Nothing starts their writeback: a commit
/// removes them while they are still in the page cache, so they never reach the disk, and
/// the volume bounds their memory by committing (see `Cache::set_dirty_memory`). Each flush
/// starts a new generation; a committed flush removes the generations below it. A generation has one file per shard of chunk
/// indexes, so writes to different chunks append in parallel and one chunk's records stay in
/// order. A record is `[chunk index u64][offset in chunk u32][length u32][data]`, or a length of
/// [`ZERO_CHUNK`] and no data for a chunk that became zeros. Appends to one
/// shard queue while a write is in flight and go out together in the next one, written on the
/// calling thread: the page cache takes them without a wait, so a hop to the blocking pool
/// would cost more than the write. An append whose write fails leaves nothing of itself in
/// the file.
#[derive(Debug)]
pub(crate) struct Journal {
    fs: Arc<dyn LocalFs>,
    dir: PathBuf,
    current: RwLock<Option<Arc<Generation>>>,
}

#[derive(Debug)]
struct Generation {
    number: u64,
    shards: Vec<Shard>,
}

#[derive(Debug)]
struct Shard {
    file: Arc<dyn LocalFile>,
    queue: Mutex<Queue>,
}

/// The records waiting for a shard's next write. While `writing`, one caller writes them.
/// `torn` says that a failed write may have left bytes past `end`.
#[derive(Debug, Default)]
struct Queue {
    end: u64,
    records: BytesMut,
    /// Each sender learns where its batch went, and finds its record from there.
    waiters: Vec<oneshot::Sender<Result<u64, Arc<Error>>>>,
    writing: bool,
    torn: bool,
}

impl Shard {
    /// Writes the queued records until none are left. It runs as a task of its own, so a
    /// cancelled append never leaves `writing` set with no writer. A write that fails, as on a full disk,
    /// may have written part of its records: the file is cut back to its end before anything
    /// else is written, so a replay never reads that part, or stops at it, in front of
    /// records written later.
    async fn drain(&self) {
        loop {
            let (batch, waiters, at, torn) = {
                let mut q = self.queue.lock();
                if q.records.is_empty() {
                    q.writing = false;
                    return;
                }
                (
                    q.records.split().freeze(),
                    std::mem::take(&mut q.waiters),
                    q.end,
                    q.torn,
                )
            };
            let cut = if torn { self.file.set_len(at).await } else { Ok(()) };
            let done = match cut.and_then(|()| self.file.write_at_now(&batch, at)) {
                Ok(()) => {
                    let mut q = self.queue.lock();
                    q.end += u64_from(batch.len());
                    q.torn = false;
                    Ok(at)
                }
                Err(e) => {
                    let cut = self.file.set_len(at).await;
                    self.queue.lock().torn = cut.is_err();
                    Err(Arc::new(Error::Io(e)))
                }
            };
            for w in waiters {
                let _ = w.send(done.clone());
            }
        }
    }
}

fn parse_name(name: &str) -> Option<u64> {
    name.split_once('.')?.0.parse().ok()
}

impl Journal {
    pub(crate) async fn open(fs: Arc<dyn LocalFs>, dir: &Path) -> Result<Journal> {
        fs.create_dir_all(dir).await?;
        Ok(Journal {
            fs,
            dir: dir.to_path_buf(),
            current: RwLock::default(),
        })
    }

    fn path(&self, number: u64, shard: usize) -> PathBuf {
        self.dir.join(format!("{number:020}.{shard:02}"))
    }

    async fn gens(&self) -> Result<Vec<u64>> {
        let mut gs: Vec<u64> = self
            .fs
            .list(&self.dir)
            .await?
            .iter()
            .filter_map(|e| parse_name(&e.name))
            .collect();
        gs.sort_unstable();
        gs.dedup();
        Ok(gs)
    }

    /// The newest generation on disk, or 0.
    pub(crate) async fn last(&self) -> Result<u64> {
        Ok(self.gens().await?.last().copied().unwrap_or(0))
    }

    /// Calls `apply` for every record of every generation from `floor` upward, in write order
    /// for each chunk; returns how many it applied. A record cut short ends its file.
    pub(crate) async fn replay<F>(&self, floor: u64, mut apply: F) -> Result<usize>
    where
        F: FnMut(u64, Record) -> Result<()>,
    {
        let mut n = 0;
        for g in self.gens().await?.into_iter().filter(|g| *g >= floor) {
            for s in 0..SHARDS {
                let file = match self.fs.open(&self.path(g, s), Open::Read).await {
                    Ok(f) => f,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                };
                n += replay_file(&file, &mut apply).await?;
            }
        }
        Ok(n)
    }

    /// Opens generation `number` for appends; the files of later generations stay as they are.
    pub(crate) async fn start(&self, number: u64) -> Result<()> {
        let mut shards = Vec::with_capacity(SHARDS);
        for s in 0..SHARDS {
            let file = self.fs.open(&self.path(number, s), Open::Create).await?;
            let end = file.size().await?;
            shards.push(Shard {
                file,
                queue: Mutex::new(Queue {
                    end,
                    ..Queue::default()
                }),
            });
        }
        *self.current.write() = Some(Arc::new(Generation { number, shards }));
        Ok(())
    }

    /// The generation that appends go to.
    pub(crate) fn generation(&self) -> u64 {
        self.current.read().as_ref().map_or(0, |g| g.number)
    }

    /// Records one write and returns where the journal keeps its bytes. Callers keep appends
    /// for one chunk in order; no append may run while [`Self::rotate`] does.
    pub(crate) async fn append(&self, idx: u64, inner: usize, data: &[u8]) -> Result<Extent> {
        let too_far = || Error::Corrupt(format!("a journal record of chunk {idx} past the chunk"));
        let at = u32::try_from(inner).map_err(|_| too_far())?;
        let len = u32::try_from(data.len()).map_err(|_| too_far())?;
        let (file, pos) = self.push(idx, at, len, data).await?;
        Ok(Extent {
            file,
            pos: pos + u64_from(HEADER),
            at,
            len,
        })
    }

    /// Records that chunk `idx` became zeros, as [`Self::append`] records a write.
    pub(crate) async fn append_zero_chunk(&self, idx: u64) -> Result<()> {
        self.push(idx, 0, ZERO_CHUNK, &[]).await.map(drop)
    }

    /// Queues one record and returns the shard file and where the record went in it.
    async fn push(&self, idx: u64, inner: u32, len: u32, data: &[u8]) -> Result<(Arc<dyn LocalFile>, u64)> {
        let Some(g) = self.current.read().clone() else {
            return Err(Error::Corrupt("an append to a journal not started".into()));
        };
        let n = usize_from(idx % u64_from(SHARDS));
        let shard = &g.shards[n];
        let (tx, rx) = oneshot::channel();
        let (off, lead) = {
            let mut q = shard.queue.lock();
            let off = u64_from(q.records.len());
            q.records.reserve(HEADER + data.len());
            q.records.put_u64_le(idx);
            q.records.put_u32_le(inner);
            q.records.put_u32_le(len);
            q.records.put_slice(data);
            q.waiters.push(tx);
            (off, !std::mem::replace(&mut q.writing, true))
        };
        if lead {
            let g = Arc::clone(&g);
            tokio::spawn(async move { g.shards[n].drain().await });
        }
        match rx.await {
            Ok(Ok(at)) => Ok((Arc::clone(&shard.file), at + off)),
            Ok(Err(e)) => Err(Error::Shared(e)),
            Err(_) => Err(Error::Cancelled),
        }
    }

    /// Starts the next generation and returns it.
    pub(crate) async fn rotate(&self) -> Result<u64> {
        let next = self.generation() + 1;
        self.start(next).await?;
        Ok(next)
    }

    /// Removes the generations below `number`, best effort: a file left behind is replayed again,
    /// which is harmless because replay is idempotent.
    pub(crate) async fn drop_below(&self, number: u64) {
        let Ok(entries) = self.fs.list(&self.dir).await else {
            return;
        };
        for e in entries {
            if parse_name(&e.name).is_some_and(|g| g < number) {
                let _ = self.fs.remove(&self.dir.join(&e.name)).await;
            }
        }
    }

    /// Removes every generation.
    pub(crate) async fn reset(&self) -> Result<()> {
        *self.current.write() = None;
        for e in self.fs.list(&self.dir).await? {
            self.fs.remove(&self.dir.join(&e.name)).await?;
        }
        Ok(())
    }
}

/// Calls `apply` for each whole record of `file`, in order; returns how many it applied.
async fn replay_file<F>(file: &Arc<dyn LocalFile>, apply: &mut F) -> Result<usize>
where
    F: FnMut(u64, Record) -> Result<()>,
{
    let size = file.size().await?;
    let header = u64_from(HEADER);
    let (mut pos, mut n) = (0, 0);
    while pos + header <= size {
        let window = file.read_at(WINDOW, pos).await?;
        let mut rest: &[u8] = &window;
        if rest.len() < HEADER {
            break;
        }
        while let (Some(idx), Some(inner), Some(len)) =
            (take::<8>(&mut rest), take::<4>(&mut rest), take::<4>(&mut rest))
        {
            let idx = u64::from_le_bytes(idx);
            let at = u32::from_le_bytes(inner);
            let len = u32::from_le_bytes(len);
            if len == ZERO_CHUNK && at == 0 {
                apply(idx, Record::ZeroChunk)?;
                n += 1;
                pos += header;
                continue;
            }
            let end = pos + header + u64::from(len);
            if at as usize + len as usize > CHUNK_SIZE || end > size {
                return Ok(n);
            }
            let file = Arc::clone(file);
            apply(
                idx,
                Record::Data(Extent {
                    file,
                    pos: pos + header,
                    at,
                    len,
                }),
            )?;
            n += 1;
            pos = end;
            rest = rest.get(len as usize..).unwrap_or_default();
        }
    }
    Ok(n)
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use super::*;

    use crate::sim::SimFs;

    /// What a record does, with its bytes read back.
    async fn read(r: &Record) -> Option<(usize, Bytes)> {
        let Record::Data(e) = r else {
            return None;
        };
        Some((e.at(), e.read(e.at(), e.end() - e.at()).await.unwrap()))
    }

    #[tokio::test]
    async fn replays_from_the_floor_in_write_order() {
        let fs = SimFs::default();
        let j = Journal::open(Arc::new(fs.clone()), Path::new("/j"))
            .await
            .unwrap();
        j.start(1).await.unwrap();
        j.append(3, 0, b"one").await.unwrap();
        let two = j.append(3, 1, b"two").await.unwrap();
        assert_eq!(two.read(2, 2).await.unwrap(), Bytes::from_static(b"wo"));
        assert_eq!(j.rotate().await.unwrap(), 2);
        j.append(19, 5, b"three").await.unwrap();
        j.append_zero_chunk(3).await.unwrap();

        let again = Journal::open(Arc::new(fs.clone()), Path::new("/j"))
            .await
            .unwrap();
        assert_eq!(again.last().await.unwrap(), 2);
        let mut seen = Vec::new();
        let n = again
            .replay(1, |i, r| {
                seen.push((i, r));
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(n, 4);
        let idxs: Vec<u64> = seen.iter().map(|s| s.0).collect();
        assert_eq!(idxs, [3, 3, 19, 3]);
        assert_eq!(read(&seen[0].1).await, Some((0, Bytes::from_static(b"one"))));
        assert_eq!(read(&seen[1].1).await, Some((1, Bytes::from_static(b"two"))));
        assert_eq!(read(&seen[2].1).await, Some((5, Bytes::from_static(b"three"))));
        assert!(matches!(seen[3].1, Record::ZeroChunk));

        j.drop_below(2).await;
        assert_eq!(again.replay(0, |_, _| Ok(())).await.unwrap(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_appends_to_one_shard_all_replay() {
        let fs = SimFs::default();
        let j = Arc::new(
            Journal::open(Arc::new(fs.clone()), Path::new("/j"))
                .await
                .unwrap(),
        );
        j.start(1).await.unwrap();
        let tasks: Vec<_> = (0..64u64)
            .map(|i| {
                let j = Arc::clone(&j);
                tokio::spawn(async move {
                    j.append(i * u64_from(SHARDS), 0, &[u8::try_from(i).unwrap(); 100])
                        .await
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        let mut records = Vec::new();
        let n = j
            .replay(0, |i, r| {
                records.push((i, r));
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(n, 64);
        let mut seen = Vec::new();
        for (i, r) in &records {
            let (_, d) = read(r).await.unwrap();
            assert!(d.iter().all(|&b| u64::from(b) * u64_from(SHARDS) == *i));
            seen.push(*i);
        }
        seen.sort_unstable();
        assert_eq!(seen, (0..64).map(|i| i * u64_from(SHARDS)).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn an_append_that_finds_the_disk_full_leaves_nothing_and_later_ones_replay() {
        let fs = SimFs::default();
        let j = Journal::open(Arc::new(fs.clone()), Path::new("/j"))
            .await
            .unwrap();
        j.start(1).await.unwrap();
        j.append(0, 0, b"kept").await.unwrap();
        let used = fs
            .open(&j.path(1, 0), Open::Read)
            .await
            .unwrap()
            .size()
            .await
            .unwrap();
        fs.set_capacity(Some(used + 30));
        let e = j.append(16, 0, &[7; 100]).await.unwrap_err();
        assert!(e.is_no_room(), "{e}");
        assert_eq!(
            fs.open(&j.path(1, 0), Open::Read)
                .await
                .unwrap()
                .size()
                .await
                .unwrap(),
            used,
            "the failed append was cut off"
        );
        fs.set_capacity(None);
        j.append(32, 0, b"after").await.unwrap();
        let mut seen = Vec::new();
        j.replay(0, |i, r| {
            seen.push((i, r));
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(seen.iter().map(|s| s.0).collect::<Vec<_>>(), [0, 32]);
        assert_eq!(read(&seen[0].1).await, Some((0, Bytes::from_static(b"kept"))));
        assert_eq!(read(&seen[1].1).await, Some((0, Bytes::from_static(b"after"))));
    }

    #[tokio::test]
    async fn a_replay_reads_records_past_one_window() {
        let fs = SimFs::default();
        let j = Journal::open(Arc::new(fs.clone()), Path::new("/j"))
            .await
            .unwrap();
        j.start(1).await.unwrap();
        let n = 2 * WINDOW / CHUNK_SIZE + 3;
        for i in 0..n {
            j.append(0, 0, &vec![u8::try_from(i % 251).unwrap(); CHUNK_SIZE])
                .await
                .unwrap();
        }
        let mut records = Vec::new();
        assert_eq!(
            j.replay(0, |_, r| {
                records.push(r);
                Ok(())
            })
            .await
            .unwrap(),
            n
        );
        for (i, r) in records.iter().enumerate() {
            let (_, d) = read(r).await.unwrap();
            assert!(d.iter().all(|&b| usize::from(b) == i % 251), "record {i}");
        }
    }

    #[tokio::test]
    async fn a_torn_record_ends_its_shard() {
        let fs = SimFs::default();
        let j = Journal::open(Arc::new(fs.clone()), Path::new("/j"))
            .await
            .unwrap();
        j.start(1).await.unwrap();
        j.append(0, 0, b"kept").await.unwrap();
        let f = fs.open(&j.path(1, 0), Open::ReadWrite).await.unwrap();
        let end = f.size().await.unwrap();
        f.write_at(vec![Bytes::from(vec![1; 20])], end).await.unwrap();
        assert_eq!(j.replay(0, |_, _| Ok(())).await.unwrap(), 1);
    }
}
