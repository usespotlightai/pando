use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use pando_format::{BLOCK_SIZE, CHUNK_SIZE, Entry, Hash, Writer, WriterId};

use super::learned::{LearnedIndex, Learner};
use super::*;
use crate::Ports;
use crate::cache::Cache;
use crate::packer::{PackStats, Packer, SEG_MAX};
use crate::ports::Open;
use crate::remote::Remote;
use crate::sim;
use crate::tree::Tree;

fn noise(seed: u64, n: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = Vec::with_capacity(n + 8);
    while out.len() < n {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(n);
    out
}

/// A chunk of `n` blocks of `from` (starting at block `at`) and new bytes after them.
fn rewrite(from: &[u8], at: usize, n: usize, seed: u64) -> Bytes {
    let mut b = from[at * BLOCK_SIZE..(at + n) * BLOCK_SIZE].to_vec();
    b.extend_from_slice(&noise(seed, CHUNK_SIZE - b.len()));
    Bytes::from(b)
}

async fn tree(ports: &Ports, dir: &str) -> Arc<Tree> {
    let c = Cache::open_with(
        Arc::clone(&ports.fs),
        Arc::clone(&ports.cpu),
        Remote::new(Arc::clone(&ports.store)),
        Path::new(dir),
    )
    .await
    .unwrap();
    Arc::new(Tree::new(Arc::new(c), 1 << 10))
}

fn writer() -> Arc<Writer> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Arc::new(Writer::new(WriterId::new("main", n), 0))
}

/// Publishes `chunks` at their indexes and writes them into an image with their block map.
async fn base(ports: &Ports, t: &Tree, chunks: &[(u64, Bytes)]) -> (Entry, BlockMap, Arc<dyn LocalFile>) {
    let mut ups = Vec::new();
    for (i, b) in chunks {
        let h = Hash::of(b);
        t.cache().put_local(h, b.clone()).await.unwrap();
        ups.push((
            *i,
            Entry {
                hash: h,
                ..Entry::EMPTY
            },
        ));
    }
    let root = t.update(&Entry::EMPTY, &ups).await.unwrap();
    let mut p = Packer::new(
        t.cache().remote().clone(),
        Arc::clone(&ports.cpu),
        writer(),
        SEG_MAX,
        Arc::default(),
    );
    let root = t.pack(&root, &mut p).await.unwrap();
    p.finish().await.unwrap();
    let image = ports.fs.open(Path::new("/base.img"), Open::Create).await.unwrap();
    let mut map = BlockMap::default();
    for (i, b) in chunks {
        image
            .write_at(vec![b.clone()], i * CHUNK_SIZE as u64)
            .await
            .unwrap();
        map.add_chunk(*i, b);
    }
    (root, map, image)
}

async fn pack_one(
    ports: &Ports,
    dedup: Option<Arc<Dedup>>,
    learner: Option<Arc<Learner>>,
    b: &Bytes,
) -> (Entry, Packer) {
    let h = Hash::of(b);
    let mut p = Packer::new(
        Remote::new(Arc::clone(&ports.store)),
        Arc::clone(&ports.cpu),
        writer(),
        SEG_MAX,
        Arc::default(),
    )
    .with_dedup(dedup, None)
    .with_learner(learner);
    let loc = p.append_chunk(h, b.clone()).await.unwrap();
    p.finish().await.unwrap();
    (Entry { hash: h, loc }, p)
}

async fn read_elsewhere(ports: &Ports, dir: &str, e: &Entry) -> Bytes {
    tree(ports, dir).await.cache().get(e).await.unwrap()
}

#[tokio::test]
async fn rewritten_base_blocks_go_up_as_a_recipe() {
    let (ports, _, _) = sim::ports(1);
    let t = tree(&ports, "/a").await;
    let a = Bytes::from(noise(1, CHUNK_SIZE));
    let (root, map, image) = base(&ports, &t, &[(3, a.clone())]).await;
    let dedup = Arc::new(Dedup {
        base: Some(BaseBlocks::new(Arc::clone(&t), root, map, image)),
        learned: None,
    });

    let b = rewrite(&a, 4, 8, 2);
    let (e, p) = pack_one(&ports, Some(Arc::clone(&dedup)), None, &b).await;
    let s = p.stats();
    assert_eq!((s.recipes, s.reused), (1, 0));
    assert!(
        s.recipe_saved >= (7 * BLOCK_SIZE) as u64,
        "saved {}",
        s.recipe_saved
    );
    assert_eq!(read_elsewhere(&ports, "/b", &e).await, b);

    let (same, p) = pack_one(&ports, Some(Arc::clone(&dedup)), None, &a).await;
    assert_eq!(p.stats().reused, 1);
    assert_eq!(
        same,
        t.lookup(&root, 3, false).await.unwrap(),
        "an equal chunk names the base chunk"
    );

    let few = rewrite(&a, 0, 3, 3);
    let (_, p) = pack_one(&ports, Some(dedup), None, &few).await;
    assert_eq!(
        p.stats(),
        PackStats {
            objects: 1,
            segments: 1,
            bytes: p.stats().bytes,
            raw_bytes: CHUNK_SIZE as u64,
            ..PackStats::default()
        },
        "three blocks are too few"
    );
}

#[tokio::test]
async fn a_base_block_that_changed_on_disk_is_not_used() {
    let (ports, _, _) = sim::ports(2);
    let t = tree(&ports, "/a").await;
    let a = Bytes::from(noise(1, CHUNK_SIZE));
    let (root, map, image) = base(&ports, &t, &[(0, a.clone())]).await;
    image
        .write_at(vec![Bytes::from(vec![9u8; CHUNK_SIZE])], 0)
        .await
        .unwrap();
    let dedup = Arc::new(Dedup {
        base: Some(BaseBlocks::new(t, root, map, image)),
        learned: None,
    });
    let b = rewrite(&a, 0, 8, 2);
    let (e, p) = pack_one(&ports, Some(dedup), None, &b).await;
    assert_eq!(p.stats().recipes, 0);
    assert_eq!(read_elsewhere(&ports, "/b", &e).await, b);
}

#[tokio::test]
async fn a_later_run_takes_blocks_from_chunks_an_earlier_run_published() {
    let (ports, _, _) = sim::ports(3);
    let dir = Path::new("/learned");
    let first = Arc::new(
        Learner::new(Arc::clone(&ports.fs), Arc::clone(&ports.clock), dir, "main")
            .await
            .unwrap(),
    );
    let a = Bytes::from(noise(5, CHUNK_SIZE));
    let (ea, p) = pack_one(&ports, None, Some(Arc::clone(&first)), &a).await;
    assert_eq!(p.learned().len(), 1);
    first.published(p.learned());
    first.save().await.unwrap();

    let learned = Arc::new(
        LearnedIndex::load(
            Arc::clone(&ports.fs),
            Arc::clone(&ports.cpu),
            Arc::clone(&ports.clock),
            dir,
        )
        .await
        .unwrap(),
    );
    assert_eq!(learned.len(), 1);
    let dedup = Arc::new(Dedup {
        base: None,
        learned: Some(Arc::clone(&learned)),
    });
    let b = rewrite(&a, 2, 10, 6);
    let (eb, p) = pack_one(&ports, Some(Arc::clone(&dedup)), None, &b).await;
    assert_eq!(p.stats().recipes, 1);
    assert_eq!(p.used_learned(), &[0]);
    assert_eq!(read_elsewhere(&ports, "/b", &eb).await, b);

    learned.mark_used(p.used_learned());
    assert_eq!(
        learned.save_uses().await.unwrap(),
        1,
        "the use moves the record forward"
    );
    assert_eq!(learned.save_uses().await.unwrap(), 0, "and is written once");
    let (same, p) = pack_one(&ports, Some(dedup), None, &a).await;
    assert_eq!((same, p.stats().reused), (ea, 1));
}

#[tokio::test]
async fn a_learned_chunk_with_bad_bytes_is_not_a_source() {
    let (ports, _, _) = sim::ports(4);
    let dir = Path::new("/learned");
    let first = Arc::new(
        Learner::new(Arc::clone(&ports.fs), Arc::clone(&ports.clock), dir, "main")
            .await
            .unwrap(),
    );
    let a = Bytes::from(noise(5, CHUNK_SIZE));
    let (_, p) = pack_one(&ports, None, Some(Arc::clone(&first)), &a).await;
    first.published(p.learned());
    first.save().await.unwrap();
    let chunks = ports
        .fs
        .list(dir)
        .await
        .unwrap()
        .into_iter()
        .find(|d| d.name.ends_with(".chunks"))
        .unwrap();
    let f = ports
        .fs
        .open(&dir.join(&chunks.name), Open::ReadWrite)
        .await
        .unwrap();
    f.write_at(vec![Bytes::from_static(&[1, 2, 3])], 100)
        .await
        .unwrap();

    let learned = LearnedIndex::load(
        Arc::clone(&ports.fs),
        Arc::clone(&ports.cpu),
        Arc::clone(&ports.clock),
        dir,
    )
    .await
    .unwrap();
    let dedup = Arc::new(Dedup {
        base: None,
        learned: Some(Arc::new(learned)),
    });
    let (_, p) = pack_one(&ports, Some(dedup), None, &rewrite(&a, 2, 10, 6)).await;
    assert_eq!(p.stats().recipes, 0);
}

#[tokio::test]
async fn an_unpublished_run_leaves_nothing_learned() {
    let (ports, _, _) = sim::ports(5);
    let dir = Path::new("/learned");
    let l = Learner::new(Arc::clone(&ports.fs), Arc::clone(&ports.clock), dir, "main")
        .await
        .unwrap();
    let a = Bytes::from(noise(5, CHUNK_SIZE));
    assert!(
        l.keep(Hash::of(&a), pando_format::Loc::default(), &a, &BlockKeys::of(&a))
            .is_some()
    );
    l.discard().await;
    assert_eq!(
        ports.fs.list(dir).await.unwrap(),
        Vec::<crate::ports::DirEntry>::new()
    );
    let learned = LearnedIndex::load(
        Arc::clone(&ports.fs),
        Arc::clone(&ports.cpu),
        Arc::clone(&ports.clock),
        dir,
    )
    .await
    .unwrap();
    assert_eq!(learned.len(), 0);
}
