use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use pando_format::{CHUNK_SIZE, Ref};

use super::follow::FOLLOW_MAX_AGE;
use super::*;
use crate::cache::{BaseKey, Cache};
use crate::{Ports, Volume, sim};

const SIZE: u64 = 4 << 20;

/// Noise, except that chunk 7 repeats chunk 2.
fn data(seed: u64) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = Vec::with_capacity(SIZE as usize);
    while out.len() < SIZE as usize {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.copy_within(2 * CHUNK_SIZE..3 * CHUNK_SIZE, 7 * CHUNK_SIZE);
    out
}

async fn host(ports: &Ports, dir: &str) -> Arc<Cache> {
    Arc::new(Cache::open(ports, Path::new(dir)).await.unwrap())
}

async fn write(ports: &Ports, c: &Arc<Cache>, b: &[u8], off: u64) -> Ref {
    let v = Volume::open(ports, c.clone(), Path::new("/v"), "main")
        .await
        .unwrap();
    v.write_at(b, off).await.unwrap();
    v.checkpoint().await.unwrap();
    v.close().await;
    c.remote().get_ref("main").await.unwrap().unwrap().0
}

fn opts(serve: bool) -> BuildOptions {
    BuildOptions {
        parallel: 8,
        serve,
        after: None,
    }
}

async fn image(ports: &Ports, bases: &Bases, r: &Ref) -> Vec<u8> {
    let f = ports
        .fs
        .open(&bases.path(&r.root.hash), Open::Read)
        .await
        .unwrap();
    f.read_at(r.size as usize, 0).await.unwrap().to_vec()
}

#[tokio::test(start_paused = true)]
async fn a_build_writes_the_root_and_a_branch_reads_it() {
    let (ports, _, _) = sim::ports(1);
    Volume::create(&ports, "main", SIZE).await.unwrap();
    let a = host(&ports, "/a").await;
    let want = data(1);
    let r = write(&ports, &a, &want, 0).await;

    let b = host(&ports, "/b").await;
    let bases = Bases::new(&ports, Path::new("/b/bases"));
    let tree = Tree::new(b.clone(), r.size.div_ceil(CHUNK_SIZE as u64));
    let rep = build(&bases, &b, &tree, r.root, r.size, opts(false))
        .await
        .unwrap();
    assert!(rep.built);
    assert_eq!(rep.chunks, SIZE as usize / CHUNK_SIZE - 1);
    assert_eq!(rep.from_fetch + rep.read_back + rep.recipes, rep.chunks);
    assert!(!b.serves_base(BaseKey::Building(r.root.hash)));
    assert_eq!(image(&ports, &bases, &r).await, want);
    assert!(
        !build(&bases, &b, &tree, r.root, r.size, opts(false))
            .await
            .unwrap()
            .built
    );

    let v = Volume::open(&ports, b.clone(), Path::new("/w"), "main")
        .await
        .unwrap();
    assert!(v.use_base(&bases).await.unwrap());
    let mut got = vec![0; 3 * CHUNK_SIZE];
    v.read_at(&mut got, CHUNK_SIZE as u64).await.unwrap();
    assert_eq!(got, want[CHUNK_SIZE..4 * CHUNK_SIZE]);
    v.write_at(&[9; 100], CHUNK_SIZE as u64 + 5).await.unwrap();
    v.read_at(&mut got, CHUNK_SIZE as u64).await.unwrap();
    assert_eq!(got[5..105], [9; 100]);
    assert_eq!(got[105..], want[CHUNK_SIZE + 105..4 * CHUNK_SIZE]);
    assert_eq!(v.drop_base(), 6 * CHUNK_SIZE as u64);
    v.close().await;
}

#[tokio::test(start_paused = true)]
async fn a_new_root_is_seeded_from_the_nearest_base() {
    let (ports, _, _) = sim::ports(2);
    Volume::create(&ports, "main", SIZE).await.unwrap();
    let a = host(&ports, "/a").await;
    let mut want = data(3);
    let r1 = write(&ports, &a, &want, 0).await;

    let b = host(&ports, "/b").await;
    let bases = Bases::new(&ports, Path::new("/b/bases"));
    let chunks = SIZE.div_ceil(CHUNK_SIZE as u64);
    let t1 = Tree::new(b.clone(), chunks);
    build(&bases, &b, &t1, r1.root, SIZE, opts(false)).await.unwrap();

    let change = vec![0x5a; CHUNK_SIZE + 10];
    want[2 * CHUNK_SIZE..3 * CHUNK_SIZE + 10].copy_from_slice(&change);
    let r2 = write(&ports, &a, &change, 2 * CHUNK_SIZE as u64).await;

    let fresh = host(&ports, "/c").await;
    let t2 = Tree::new(fresh.clone(), chunks);
    let bases_c = Bases::new(&ports, Path::new("/b/bases"));
    let rep = build(&bases_c, &fresh, &t2, r2.root, SIZE, opts(true))
        .await
        .unwrap();
    assert_eq!(rep.seed, Some(r1.root.hash));
    assert_eq!(rep.chunks, 2);
    assert!(fresh.serves_base(BaseKey::Building(r2.root.hash)));
    assert!(!fresh.serves_base(BaseKey::Seed(r1.root.hash)));
    assert_eq!(image(&ports, &bases, &r2).await, want);
}

#[tokio::test(start_paused = true)]
async fn a_served_base_answers_chunk_reads_and_gives_upload_blocks() {
    let (ports, _, _) = sim::ports(3);
    Volume::create(&ports, "main", SIZE).await.unwrap();
    let a = host(&ports, "/a").await;
    let want = data(5);
    let r = write(&ports, &a, &want, 0).await;

    let b = host(&ports, "/b").await;
    let bases = Bases::new(&ports, Path::new("/b/bases"));
    let tree = Arc::new(Tree::new(b.clone(), SIZE.div_ceil(CHUNK_SIZE as u64)));
    assert!(bases.blocks_for(&tree, r.root).await.unwrap().is_none());
    let rep = build(&bases, &b, &tree, r.root, SIZE, opts(true)).await.unwrap();
    assert!(rep.from_fetch > 0);

    let e = tree.lookup(&r.root, 0, false).await.unwrap();
    let before = b.stats().base_hits.load(Ordering::Relaxed);
    let got = b.read(&e, 0, CHUNK_SIZE).await.unwrap();
    assert_eq!(got, want[..CHUNK_SIZE]);
    assert!(b.stats().base_hits.load(Ordering::Relaxed) > before);

    let other = Arc::new(Tree::new(
        host(&ports, "/c").await,
        SIZE.div_ceil(CHUNK_SIZE as u64),
    ));
    let blocks = bases.blocks_for(&other, r.root).await.unwrap();
    assert!(blocks.is_some());
    assert!(other.has_node(&r.root.hash));
}

#[tokio::test(start_paused = true)]
async fn a_follower_builds_the_new_root_of_a_followed_branch_and_evicts_old_bases() {
    let (ports, _, _) = sim::ports(4);
    Volume::create(&ports, "main", SIZE).await.unwrap();
    let a = host(&ports, "/a").await;
    let r1 = write(&ports, &a, &data(7), 0).await;

    let b = host(&ports, "/b").await;
    let bases = Bases::new(&ports, Path::new("/b/bases"));
    bases.note_follow("main").await.unwrap();
    let mut f = Follower::new(&ports, b.clone(), Path::new("/b/bases"), 8, 1, 0);
    assert_eq!(f.once().await.unwrap(), 1);
    assert!(bases.has(&r1.root.hash).await);
    assert_eq!(f.once().await.unwrap(), 0);

    tokio::time::sleep(Duration::from_secs(1)).await;
    let r2 = write(&ports, &a, &[3; 100], 0).await;
    assert_eq!(f.once().await.unwrap(), 1);
    assert!(bases.has(&r2.root.hash).await);
    assert!(!bases.has(&r1.root.hash).await, "only the most recent base stays");

    tokio::time::sleep(FOLLOW_MAX_AGE + Duration::from_secs(1)).await;
    write(&ports, &a, &[4; 100], 0).await;
    assert_eq!(f.once().await.unwrap(), 0, "the note aged out");
}
