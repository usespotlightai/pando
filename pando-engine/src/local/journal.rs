use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use bytes::{BufMut, Bytes, BytesMut};
use pando_format::CHUNK_SIZE;
use tokio::sync::oneshot;

use super::read_file;
use crate::ports::{LocalFile, LocalFs, Open};
use crate::{Error, Result};

const HEADER: usize = 16;
const SHARDS: usize = 16;

/// Acknowledged writes that no flush has committed yet.
///
/// Records go to a local file without a sync before the write is acknowledged: a daemon crash
/// keeps them in the page cache, so the next daemon replays them. A host crash loses them, as
/// it would lose a disk's volatile write cache. Each flush starts a new generation; a committed
/// flush removes the generations below it. A generation has one file per shard of chunk
/// indexes, so writes to different chunks append in parallel and one chunk's records stay in
/// order. A record is `[chunk index u64][offset in chunk u32][length u32][data]`. Appends to one
/// shard queue while a write is in flight and go out together in the next one, written on the
/// calling thread: the page cache takes them without a wait, so a hop to the blocking pool
/// would cost more than the write.
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
#[derive(Debug, Default)]
struct Queue {
    end: u64,
    records: BytesMut,
    waiters: Vec<oneshot::Sender<Result<(), Arc<Error>>>>,
    writing: bool,
}

impl Shard {
    /// Writes the queued records until none are left.
    fn drain(&self) {
        loop {
            let (batch, waiters, at) = {
                let mut q = self.queue.lock().expect("journal queue lock");
                if q.records.is_empty() {
                    q.writing = false;
                    return;
                }
                (q.records.split().freeze(), std::mem::take(&mut q.waiters), q.end)
            };
            let n = batch.len() as u64;
            let done = self
                .file
                .write_at_now(&batch, at)
                .map_err(|e| Arc::new(Error::Io(e)));
            if done.is_ok() {
                self.queue.lock().expect("journal queue lock").end += n;
            }
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
        F: FnMut(u64, usize, Bytes) -> Result<()>,
    {
        let mut n = 0;
        for g in self.gens().await?.into_iter().filter(|g| *g >= floor) {
            for s in 0..SHARDS {
                let Some(b) = read_file(self.fs.as_ref(), &self.path(g, s)).await? else {
                    continue;
                };
                let mut off = 0;
                while off + HEADER <= b.len() {
                    let idx = u64::from_le_bytes(b[off..off + 8].try_into().expect("8 bytes"));
                    let inner =
                        u32::from_le_bytes(b[off + 8..off + 12].try_into().expect("4 bytes")) as usize;
                    let len = u32::from_le_bytes(b[off + 12..off + 16].try_into().expect("4 bytes")) as usize;
                    let body = off + HEADER;
                    if body + len > b.len() || inner + len > CHUNK_SIZE {
                        break;
                    }
                    apply(idx, inner, b.slice(body..body + len))?;
                    n += 1;
                    off = body + len;
                }
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
        *self.current.write().expect("journal lock") = Some(Arc::new(Generation { number, shards }));
        Ok(())
    }

    /// The generation that appends go to.
    pub(crate) fn generation(&self) -> u64 {
        self.current
            .read()
            .expect("journal lock")
            .as_ref()
            .map_or(0, |g| g.number)
    }

    /// Records one write. Callers keep appends for one chunk in order; no append may run while
    /// [`Self::rotate`] does.
    pub(crate) async fn append(&self, idx: u64, inner: usize, data: Bytes) -> Result<()> {
        let g = self
            .current
            .read()
            .expect("journal lock")
            .clone()
            .expect("journal started");
        let shard = &g.shards[(idx % SHARDS as u64) as usize];
        let (tx, rx) = oneshot::channel();
        let lead = {
            let mut q = shard.queue.lock().expect("journal queue lock");
            q.records.reserve(HEADER + data.len());
            q.records.put_u64_le(idx);
            q.records.put_u32_le(inner as u32);
            q.records.put_u32_le(data.len() as u32);
            q.records.put_slice(&data);
            q.waiters.push(tx);
            !std::mem::replace(&mut q.writing, true)
        };
        if lead {
            shard.drain();
        }
        match rx.await {
            Ok(done) => done.map_err(Error::Shared),
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
        *self.current.write().expect("journal lock") = None;
        for e in self.fs.list(&self.dir).await? {
            self.fs.remove(&self.dir.join(&e.name)).await?;
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "sim"))]
mod tests {
    use super::*;

    use crate::sim::SimFs;

    #[tokio::test]
    async fn replays_from_the_floor_in_write_order() {
        let fs = SimFs::default();
        let j = Journal::open(Arc::new(fs.clone()), Path::new("/j"))
            .await
            .unwrap();
        j.start(1).await.unwrap();
        j.append(3, 0, Bytes::from_static(b"one")).await.unwrap();
        j.append(3, 1, Bytes::from_static(b"two")).await.unwrap();
        assert_eq!(j.rotate().await.unwrap(), 2);
        j.append(19, 5, Bytes::from_static(b"three")).await.unwrap();

        let again = Journal::open(Arc::new(fs.clone()), Path::new("/j"))
            .await
            .unwrap();
        assert_eq!(again.last().await.unwrap(), 2);
        let mut seen = Vec::new();
        let n = again
            .replay(1, |i, at, d| {
                seen.push((i, at, d));
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(n, 3);
        assert_eq!(seen[0], (3, 0, Bytes::from_static(b"one")));
        assert_eq!(seen[1], (3, 1, Bytes::from_static(b"two")));
        assert_eq!(seen[2], (19, 5, Bytes::from_static(b"three")));

        j.drop_below(2).await;
        assert_eq!(again.replay(0, |_, _, _| Ok(())).await.unwrap(), 1);
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
                let j = j.clone();
                tokio::spawn(async move {
                    j.append(i * SHARDS as u64, 0, Bytes::from(vec![i as u8; 100]))
                        .await
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        let mut seen = Vec::new();
        let n = j
            .replay(0, |i, _, d| {
                assert!(d.iter().all(|&b| u64::from(b) * SHARDS as u64 == i));
                seen.push(i);
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(n, 64);
        seen.sort_unstable();
        assert_eq!(seen, (0..64).map(|i| i * SHARDS as u64).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_torn_record_ends_its_shard() {
        let fs = SimFs::default();
        let j = Journal::open(Arc::new(fs.clone()), Path::new("/j"))
            .await
            .unwrap();
        j.start(1).await.unwrap();
        j.append(0, 0, Bytes::from_static(b"kept")).await.unwrap();
        let f = fs.open(&j.path(1, 0), Open::ReadWrite).await.unwrap();
        let end = f.size().await.unwrap();
        f.write_at(vec![Bytes::from(vec![1; 20])], end).await.unwrap();
        assert_eq!(j.replay(0, |_, _, _| Ok(())).await.unwrap(), 1);
    }
}
