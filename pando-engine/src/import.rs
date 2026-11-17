//! Importing a disk image as a new branch.

use std::path::Path;
use std::sync::Arc;

use futures::StreamExt;
use pando_format::{CHUNK_SIZE, Entry, Hash, Ref, Writer, WriterId, is_zero};

use crate::cache::Cache;
use crate::packer::{PackStats, Packer, SEG_MAX};
use crate::ports::{CpuClass, Open, on_cpu};
use crate::tree::Tree;
use crate::{Ports, Result};

const CHUNK: u64 = CHUNK_SIZE as u64;
const READ_AHEAD: usize = 16;

/// What an import uploaded.
#[derive(Clone, Copy, Debug, Default)]
pub struct ImportReport {
    pub size: u64,
    /// Chunks that hold data; zero chunks stay holes.
    pub chunks: usize,
    pub upload: PackStats,
}

/// Creates branch `name` with the bytes of the image file at `image`.
pub async fn import(ports: &Ports, cache: &Arc<Cache>, name: &str, image: &Path) -> Result<ImportReport> {
    let f = ports.fs.open(image, Open::Read).await?;
    let size = f.size().await?;
    let remote = cache.remote();
    let generation = 1;
    let version = remote
        .create_ref(
            name,
            &Ref {
                size,
                root: Entry::EMPTY,
                generation,
                from: None,
            },
        )
        .await?;
    let tree = Tree::new(cache.clone(), size.div_ceil(CHUNK));
    let cpu = ports.cpu.clone();
    let mut chunks = futures::stream::iter(0..size.div_ceil(CHUNK))
        .map(|idx| {
            let (f, cpu) = (f.clone(), cpu.clone());
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
    let mut p = Packer::new(remote.clone(), ports.cpu.clone(), writer, SEG_MAX, Arc::default());
    let packed = tree.pack(&root, &mut p).await?;
    p.finish().await?;
    remote
        .put_ref(
            name,
            &Ref {
                size,
                root: packed,
                generation,
                from: None,
            },
            &version,
        )
        .await?;
    Ok(ImportReport {
        size,
        chunks: updates.len(),
        upload: p.stats(),
    })
}

#[cfg(all(test, feature = "sim"))]
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
}
