//! Importing a disk image as a new branch.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use futures::StreamExt;
use pando_format::{CHUNK_SIZE, Entry, Hash, Ref, Writer, WriterId, is_zero, u64_from, usize_from};

use crate::cache::Cache;
use crate::packer::{PackStats, Packer, SEG_MAX};
use crate::ports::{CpuClass, LocalFile, Open, on_cpu};
use crate::tree::Tree;
use crate::{Ports, Result, ext4};

const CHUNK: u64 = u64_from(CHUNK_SIZE);
const READ_AHEAD: usize = 16;

/// What an import uploaded.
#[derive(Clone, Copy, Debug, Default)]
pub struct ImportReport {
    /// The size of the branch in bytes: the size of the image.
    pub size: u64,
    /// Chunks that hold data; zero chunks stay holes.
    pub chunks: usize,
    /// Chunks of ext4 metadata, packed first so they share few frames.
    pub metadata: usize,
    /// What the upload of the chunks and the tree packed and sent.
    pub upload: PackStats,
}

struct ImageFile<'a> {
    f: &'a Arc<dyn LocalFile>,
    at: HashMap<u64, Hash>,
    order: Vec<Hash>,
}

impl ext4::Image for ImageFile<'_> {
    async fn read(&mut self, off: u64, len: u64) -> Result<Vec<u8>> {
        let mut b = self.f.read_at(usize_from(len), off).await?.to_vec();
        b.resize(usize_from(len), 0);
        Ok(b)
    }

    fn wave(&mut self, idxs: Vec<u64>) -> impl Future<Output = Result<()>> {
        self.order
            .extend(idxs.into_iter().filter_map(|i| self.at.get(&i).copied()));
        std::future::ready(Ok(()))
    }
}

/// Packs the chunks of an ext4 file system's metadata in the image `f` before the others.
/// An image without one packs in index order.
async fn pack_metadata(
    f: &Arc<dyn LocalFile>,
    cache: &Cache,
    updates: &[(u64, Entry)],
    p: &mut Packer,
) -> Result<usize> {
    let mut img = ImageFile {
        f,
        at: updates.iter().map(|(i, e)| (*i, e.hash)).collect(),
        order: Vec::new(),
    };
    if ext4::metadata(&mut img).await.is_err() {
        return Ok(0);
    }
    let mut packed = HashSet::new();
    for h in img.order {
        if packed.insert(h) {
            let b = cache
                .local(&h)
                .await?
                .ok_or(crate::Error::ChunkLost { hash: h })?;
            p.append_chunk(h, b).await?;
        }
    }
    Ok(packed.len())
}

/// Creates branch `name` with the bytes of the image file at `image`. The branch appears only
/// once everything is uploaded; a failed import leaves segments that GC collects.
pub async fn import(ports: &Ports, cache: &Arc<Cache>, name: &str, image: &Path) -> Result<ImportReport> {
    let f = ports.fs.open(image, Open::Read).await?;
    let size = f.size().await?;
    let remote = cache.remote();
    if remote.get_ref(name).await?.is_some() {
        return Err(crate::Error::RefExists(name.to_owned()));
    }
    let generation = 1;
    let tree = Tree::new(Arc::clone(cache), size.div_ceil(CHUNK));
    let cpu = Arc::clone(&ports.cpu);
    let mut chunks = futures::stream::iter(0..size.div_ceil(CHUNK))
        .map(|idx| {
            let (f, cpu) = (Arc::clone(&f), Arc::clone(&cpu));
            async move {
                let mut b = f.read_at(CHUNK_SIZE, idx * CHUNK).await?.to_vec();
                b.resize(CHUNK_SIZE, 0);
                let b = bytes::Bytes::from(b);
                let hashed = b.clone();
                let h = on_cpu(cpu.as_ref(), CpuClass::Background, move || {
                    (!is_zero(&hashed)).then(|| Hash::of(&hashed))
                })
                .await;
                Ok::<_, crate::Error>((idx, h, b))
            }
        })
        .buffered(READ_AHEAD);
    let mut updates = Vec::new();
    while let Some(r) = chunks.next().await {
        let (idx, h, b) = r?;
        if let Some(h) = h {
            cache.put_local(h, b).await?;
            updates.push((
                idx,
                Entry {
                    hash: h,
                    ..Entry::EMPTY
                },
            ));
        }
    }
    cache.sync_local().await?;
    let root = tree.update(&Entry::EMPTY, &updates).await?;
    let writer = Arc::new(Writer::new(WriterId::new(name, generation), ports.rng.next_u32()));
    let mut p = Packer::new(
        remote.clone(),
        Arc::clone(&ports.cpu),
        writer,
        SEG_MAX,
        Arc::default(),
    );
    let metadata = pack_metadata(&f, cache, &updates, &mut p).await?;
    let packed = tree.pack(&root, &mut p).await?;
    p.finish().await?;
    remote
        .create_ref(
            name,
            &Ref {
                size,
                root: packed,
                generation,
                from: None,
                log: Some(pando_format::LogPos {
                    generation,
                    closed: true,
                    ..pando_format::LogPos::default()
                }),
            },
        )
        .await?;
    Ok(ImportReport {
        size,
        chunks: updates.len(),
        metadata,
        upload: p.stats(),
    })
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::{Volume, sim};

    #[tokio::test(start_paused = true)]
    async fn an_imported_image_reads_back_on_another_host() {
        let (ports, _, _) = sim::ports(9);
        let mut img = vec![0u8; 5 * CHUNK_SIZE + 123];
        img[CHUNK_SIZE..CHUNK_SIZE + 10].fill(1);
        img[5 * CHUNK_SIZE..].fill(2);
        ports.fs.create_dir_all(Path::new("/img")).await.unwrap();
        let f = ports.fs.open(Path::new("/img/disk"), Open::Create).await.unwrap();
        f.write_at(vec![Bytes::from(img.clone())], 0).await.unwrap();
        let c = Arc::new(Cache::open(&ports, Path::new("/c")).await.unwrap());
        let r = import(&ports, &c, "main", Path::new("/img/disk")).await.unwrap();
        assert_eq!((r.size, r.chunks), (img.len() as u64, 2));

        let d = Arc::new(Cache::open(&ports, Path::new("/d")).await.unwrap());
        let v = Volume::open(&ports, d, Path::new("/v"), "main").await.unwrap();
        let mut got = vec![9; img.len()];
        v.read_at(&mut got, 0).await.unwrap();
        assert_eq!(got, img);
        v.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_import_leaves_no_branch() {
        let (ports, store, _) = sim::ports(11);
        let img = vec![3u8; 2 * CHUNK_SIZE];
        ports.fs.create_dir_all(Path::new("/img")).await.unwrap();
        let f = ports.fs.open(Path::new("/img/disk"), Open::Create).await.unwrap();
        f.write_at(vec![Bytes::from(img.clone())], 0).await.unwrap();
        let c = Arc::new(Cache::open(&ports, Path::new("/c")).await.unwrap());
        store.inject(sim::Fault {
            op: sim::StoreOp::Create,
            applied: false,
            permanent: false,
        });
        assert!(import(&ports, &c, "main", Path::new("/img/disk")).await.is_err());
        assert!(c.remote().get_ref("main").await.unwrap().is_none());
        import(&ports, &c, "main", Path::new("/img/disk")).await.unwrap();
        let again = import(&ports, &c, "main", Path::new("/img/disk")).await;
        assert!(matches!(again.unwrap_err().kind(), crate::Error::RefExists(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn ext4_metadata_is_packed_first() {
        let (ports, _, _) = sim::ports(10);
        let mut img = ext4::tests::image();
        img[100 * CHUNK_SIZE] = 7;
        ports.fs.create_dir_all(Path::new("/img")).await.unwrap();
        let f = ports.fs.open(Path::new("/img/disk"), Open::Create).await.unwrap();
        f.write_at(vec![Bytes::from(img.clone())], 0).await.unwrap();
        let c = Arc::new(Cache::open(&ports, Path::new("/c")).await.unwrap());
        let r = import(&ports, &c, "main", Path::new("/img/disk")).await.unwrap();
        assert_eq!((r.chunks, r.metadata), (3, 2));

        let d = Arc::new(Cache::open(&ports, Path::new("/d")).await.unwrap());
        let v = Volume::open(&ports, d, Path::new("/v"), "main").await.unwrap();
        let mut got = vec![9; img.len()];
        v.read_at(&mut got, 0).await.unwrap();
        assert!(got == img);
        v.close().await;
    }
}
