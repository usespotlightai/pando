use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use pando_format::CHUNK_SIZE;
use tokio::sync::Mutex;

use super::read_file;
use crate::Result;
use crate::ports::{LocalFile, LocalFs, Open};

const HEADER: usize = 16;
const SHARDS: usize = 16;

/// Acknowledged writes that no flush has committed yet.
///
/// Records go to a local file without a sync before the write is acknowledged: a daemon crash
/// keeps them in the page cache, so the next daemon replays them. A host crash loses them, as
/// it would lose a disk's volatile write cache. Each flush starts a new generation; a committed
/// flush removes the generations below it. A generation has one file per shard of chunk
/// indexes, so writes to different chunks append in parallel and one chunk's records stay in
/// order. A record is `[chunk index u64][offset in chunk u32][length u32][data]`.
#[derive(Debug)]
pub(crate) struct Journal {
    fs: Arc<dyn LocalFs>,
    dir: PathBuf,
    current: RwLock<Option<Arc<Generation>>>,
}

#[derive(Debug)]
struct Generation {
    number: u64,
    shards: Vec<Mutex<Shard>>,
}

#[derive(Debug)]
struct Shard {
    file: Arc<dyn LocalFile>,
    end: u64,
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
            shards.push(Mutex::new(Shard { file, end }));
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
        let mut hdr = Vec::with_capacity(HEADER);
        hdr.extend_from_slice(&idx.to_le_bytes());
        hdr.extend_from_slice(&(inner as u32).to_le_bytes());
        hdr.extend_from_slice(&(data.len() as u32).to_le_bytes());
        let n = (HEADER + data.len()) as u64;
        let bufs = vec![Bytes::from(hdr), data];
        let mut s = g.shards[(idx % SHARDS as u64) as usize].lock().await;
        let done = s.file.write_at_nowait(&bufs, s.end)?;
        if (done as u64) < n {
            s.file.write_at(skip(bufs, done), s.end + done as u64).await?;
        }
        s.end += n;
        Ok(())
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

/// `bufs` without its first `n` bytes.
fn skip(bufs: Vec<Bytes>, mut n: usize) -> Vec<Bytes> {
    let mut rest = Vec::with_capacity(bufs.len());
    for b in bufs {
        if n >= b.len() {
            n -= b.len();
        } else {
            rest.push(b.slice(n..));
            n = 0;
        }
    }
    rest
}

#[cfg(all(test, feature = "sim"))]
mod tests {
    use super::*;

    #[test]
    fn skip_drops_a_prefix_across_buffers() {
        let b = vec![Bytes::from_static(b"abc"), Bytes::from_static(b"defg")];
        assert_eq!(skip(b.clone(), 0), b);
        assert_eq!(skip(b.clone(), 3), [Bytes::from_static(b"defg")]);
        assert_eq!(skip(b.clone(), 5), [Bytes::from_static(b"fg")]);
        assert!(skip(b, 7).is_empty());
    }
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
