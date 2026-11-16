use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use pando_format::{
    BLOCKS_PER_CHUNK, CHUNK_SIZE, Entry, FrameBuilder, Hash, Loc, Pick, Recipe, SegId, SegmentIndex, WriterId,
};

use super::sched::{FetchOpts, URGENT_GETS, WINDOW, Window};
use super::*;
use crate::remote::Remote;
use crate::sim;

struct Seg {
    id: SegId,
    entries: Vec<Entry>,
}

async fn upload_as(remote: &Remote, n: u32, deps: Vec<SegId>, frames: Vec<Vec<(Bytes, bool, Hash)>>) -> Seg {
    let id = SegId::new(WriterId::new("main", 1), n);
    let (mut data, mut index, mut entries) = (Vec::new(), Vec::new(), Vec::new());
    for (f, objs) in frames.into_iter().enumerate() {
        let mut fb = FrameBuilder::default();
        for (b, recipe, h) in objs {
            let (inner, len) = fb.push(h, &b, recipe);
            entries.push(Entry {
                hash: h,
                loc: Loc {
                    seg: id,
                    frame: f as u32,
                    inner,
                    len,
                },
            });
        }
        let z = fb.finish().unwrap();
        index.push((data.len() as u32, z.len() as u32));
        data.extend_from_slice(&z);
    }
    let idx = SegmentIndex { frames: index, deps }.encode();
    remote
        .put_segment(id, Bytes::from(data), Bytes::from(idx))
        .await
        .unwrap();
    Seg { id, entries }
}

async fn upload(remote: &Remote, frames: Vec<Vec<(Bytes, bool, Hash)>>) -> Seg {
    upload_as(remote, 7, vec![], frames).await
}

fn chunk(seed: u8) -> Bytes {
    Bytes::from(
        (0..CHUNK_SIZE)
            .map(|i| (i as u8).wrapping_mul(seed).wrapping_add(seed))
            .collect::<Vec<_>>(),
    )
}

/// A chunk zstd cannot shrink, so frames have their real size.
fn noise(seed: u64) -> Bytes {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = Vec::with_capacity(CHUNK_SIZE);
    while out.len() < CHUNK_SIZE {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(CHUNK_SIZE);
    Bytes::from(out)
}

fn plain(b: Bytes) -> (Bytes, bool, Hash) {
    let h = Hash::of(&b);
    (b, false, h)
}

/// A recipe that takes the second half of `src` as its first half and zeroes the rest.
fn half_of(src: &Entry, src_bytes: &[u8]) -> (Bytes, bool, Hash) {
    let mut want = vec![0; CHUNK_SIZE];
    want[..CHUNK_SIZE / 2].copy_from_slice(&src_bytes[CHUNK_SIZE / 2..]);
    let mut picks = [Pick::Zero; BLOCKS_PER_CHUNK];
    for (i, p) in picks.iter_mut().enumerate().take(BLOCKS_PER_CHUNK / 2) {
        *p = Pick::Source {
            src: 0,
            block: (i + BLOCKS_PER_CHUNK / 2) as u8,
        };
    }
    let enc = Recipe::encode(std::slice::from_ref(src), &picks, &want);
    (Bytes::from(enc), true, Hash::of(&want))
}

fn want_of(src_bytes: &[u8]) -> Bytes {
    let mut want = vec![0; CHUNK_SIZE];
    want[..CHUNK_SIZE / 2].copy_from_slice(&src_bytes[CHUNK_SIZE / 2..]);
    Bytes::from(want)
}

async fn cache(ports: &crate::Ports, dir: &str) -> Cache {
    let remote = Remote::new(ports.store.clone());
    Cache::open_with(ports.fs.clone(), ports.cpu.clone(), remote, Path::new(dir))
        .await
        .unwrap()
}

fn gets(c: &Cache) -> u64 {
    c.remote().stats().gets.load(Ordering::Relaxed)
}

#[tokio::test]
async fn a_miss_fetches_its_frame_once_for_every_reader() {
    let (ports, _, _) = sim::ports(1);
    let c = cache(&ports, "/c").await;
    let seg = upload(
        c.remote(),
        vec![vec![plain(chunk(1)), plain(chunk(2))], vec![plain(chunk(3))]],
    )
    .await;
    let e = seg.entries[1];
    let (a, b) = futures::join!(c.get(&e), c.read(&e, 100, 10));
    assert_eq!(a.unwrap(), chunk(2));
    assert_eq!(b.unwrap(), chunk(2).slice(100..110));
    assert_eq!(c.stats().frames_fetched.load(Ordering::Relaxed), 1);
    assert!(c.has(&seg.entries[0].hash).await, "the whole frame is stored");
    assert!(!c.has(&seg.entries[2].hash).await);

    assert_eq!(c.fetch_entries(&seg.entries, 4).await, 1);
    assert_eq!(c.get(&seg.entries[2]).await.unwrap(), chunk(3));
    assert_eq!(gets(&c), 3, "one index and two ranges");
    assert_eq!(seg.id, e.loc.seg);
}

#[tokio::test]
async fn recipes_rebuild_from_their_sources() {
    let (ports, _, _) = sim::ports(2);
    let c = cache(&ports, "/c").await;
    let src = chunk(5);
    let first = upload(c.remote(), vec![vec![plain(src.clone())]]).await;
    let other = cache(&ports, "/d").await;
    let second = upload_as(
        other.remote(),
        8,
        vec![first.id],
        vec![vec![half_of(&first.entries[0], &src)]],
    )
    .await;
    assert_eq!(other.get(&second.entries[0]).await.unwrap(), want_of(&src));
    assert!(other.has(&first.entries[0].hash).await, "the source came along");
}

#[tokio::test]
async fn a_failed_fetch_leaves_nothing_behind() {
    let (ports, store, _) = sim::ports(3);
    let c = cache(&ports, "/c").await;
    let seg = upload(c.remote(), vec![vec![plain(chunk(9))], vec![plain(chunk(10))]]).await;
    assert_eq!(c.get(&seg.entries[0]).await.unwrap(), chunk(9));
    store.inject(sim::Fault {
        op: sim::StoreOp::Get,
        applied: false,
    });
    assert!(c.get(&seg.entries[1]).await.is_err());
    assert_eq!(c.get(&seg.entries[1]).await.unwrap(), chunk(10));
}

#[tokio::test]
async fn adjacent_frames_go_as_one_run_and_ready_fires_for_every_entry() {
    let (ports, _, _) = sim::ports(4);
    let c = cache(&ports, "/c").await;
    let frames = (0..6).map(|i| vec![plain(noise(i))]).collect();
    let seg = upload(c.remote(), frames).await;
    let seen = Mutex::new(Vec::new());
    let ready = |i: usize| seen.lock().unwrap().push(i);
    let es = [seg.entries[4], seg.entries[1], seg.entries[2], seg.entries[1]];
    let n = c
        .fetch_with(
            &es,
            &FetchOpts {
                parallel: 2,
                ready: Some(&ready),
                ..FetchOpts::default()
            },
        )
        .await;
    assert_eq!(n, 3);
    let mut seen = seen.into_inner().unwrap();
    seen.sort_unstable();
    assert_eq!(seen, vec![0, 1, 2, 3]);
    assert_eq!(
        gets(&c),
        1 + 2,
        "one index, frames 1..=2 as one run, frame 4 alone"
    );
    for e in &es {
        assert!(c.has(&e.hash).await);
    }
    assert!(!c.has(&seg.entries[3].hash).await);
}

#[tokio::test]
async fn a_failed_index_still_tells_every_entry() {
    let (ports, store, _) = sim::ports(5);
    let c = cache(&ports, "/c").await;
    let seg = upload(c.remote(), vec![vec![plain(noise(1))], vec![plain(noise(2))]]).await;
    store.inject(sim::Fault {
        op: sim::StoreOp::Get,
        applied: false,
    });
    let seen = Mutex::new(0);
    let ready = |_: usize| *seen.lock().unwrap() += 1;
    c.fetch_with(
        &seg.entries,
        &FetchOpts {
            parallel: 2,
            ready: Some(&ready),
            ..FetchOpts::default()
        },
    )
    .await;
    assert_eq!(*seen.lock().unwrap(), 2);
    assert!(!c.has(&seg.entries[0].hash).await);
    assert_eq!(
        c.fetch_entries(&seg.entries, 2).await,
        2,
        "the next fetch gets them"
    );
    assert!(c.has(&seg.entries[1].hash).await);
}

#[tokio::test]
async fn asked_only_keeps_the_rest_of_the_frame_for_the_next_fetch() {
    let (ports, _, _) = sim::ports(6);
    let c = cache(&ports, "/c").await;
    let seg = upload(
        c.remote(),
        vec![vec![plain(noise(1)), plain(noise(2)), plain(noise(3))]],
    )
    .await;
    assert_eq!(c.fetch_chunks(&seg.entries[..1], HashSet::new(), 2).await, 1);
    assert!(c.has(&seg.entries[0].hash).await);
    assert!(
        !c.has(&seg.entries[1].hash).await,
        "only the asked chunk is stored"
    );
    let before = gets(&c);
    assert_eq!(c.fetch_chunks(&seg.entries[1..2], HashSet::new(), 2).await, 1);
    assert!(c.has(&seg.entries[1].hash).await);
    assert_eq!(gets(&c), before, "the kept frame served the second fetch");
    assert_eq!(c.stats().kept_hits.load(Ordering::Relaxed), 1);
    assert_eq!(
        c.get(&seg.entries[2]).await.unwrap(),
        noise(3),
        "a read gets the frame again"
    );
}

#[tokio::test]
async fn a_frame_with_a_tree_node_decodes_whole() {
    let (ports, _, _) = sim::ports(7);
    let c = cache(&ports, "/c").await;
    let seg = upload(c.remote(), vec![vec![plain(noise(1)), plain(noise(2))]]).await;
    let nodes = HashSet::from([seg.entries[0].hash]);
    c.fetch_chunks(&seg.entries[..1], nodes, 2).await;
    assert!(c.has(&seg.entries[1].hash).await);
}

#[tokio::test]
async fn a_recipe_without_its_source_is_held_until_asked_for() {
    let (ports, _, _) = sim::ports(8);
    let c = cache(&ports, "/c").await;
    let src = noise(9);
    let first = upload(c.remote(), vec![vec![plain(src.clone())]]).await;
    let second = upload_as(
        c.remote(),
        8,
        vec![first.id],
        vec![vec![half_of(&first.entries[0], &src), plain(noise(10))]],
    )
    .await;
    c.fetch_entries(&second.entries[1..], 2).await;
    assert!(c.has(&second.entries[1].hash).await);
    assert!(
        !c.has(&second.entries[0].hash).await,
        "the recipe waits for its source"
    );
    assert!(
        !c.has(&first.entries[0].hash).await,
        "and its source was not fetched"
    );
    assert!(c.held_encoding(&second.entries[0]).is_some());

    c.fetch_entries(&second.entries[..1], 2).await;
    assert_eq!(
        c.local(&second.entries[0].hash).await.unwrap(),
        Some(want_of(&src))
    );
    assert!(c.held_encoding(&second.entries[0]).is_none());
    assert_eq!(c.stats().held_rebuilt.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn a_held_recipe_rebuilds_on_a_read() {
    let (ports, _, _) = sim::ports(9);
    let c = cache(&ports, "/c").await;
    let src = noise(11);
    let first = upload(c.remote(), vec![vec![plain(src.clone())]]).await;
    let second = upload_as(
        c.remote(),
        8,
        vec![first.id],
        vec![vec![half_of(&first.entries[0], &src), plain(noise(12))]],
    )
    .await;
    c.fetch_entries(&second.entries[1..], 2).await;
    assert_eq!(
        c.read(&second.entries[0], 10, 20).await.unwrap(),
        want_of(&src).slice(10..30)
    );
    assert!(c.has(&first.entries[0].hash).await);
}

#[tokio::test]
async fn the_window_lets_at_most_its_bytes_through() {
    let w = Window::default();
    let a = w.acquire(WINDOW - 10, false).await;
    let b = tokio::time::timeout(std::time::Duration::from_millis(1), w.acquire(20, false)).await;
    assert!(b.is_err(), "no room for more");
    let big = {
        drop(a);
        w.acquire(WINDOW * 2, false).await
    };
    assert_eq!(w.peak_gets(), 1, "a run larger than the window goes alone");
    drop(big);
}

#[tokio::test]
async fn background_gets_yield_while_a_fetch_is_urgent() {
    let w = Window::default();
    let mut held = Vec::new();
    {
        let _u = w.urgent();
        for _ in 0..URGENT_GETS {
            held.push(w.acquire(1, false).await);
        }
        let more = tokio::time::timeout(std::time::Duration::from_millis(1), w.acquire(1, false)).await;
        assert!(more.is_err());
        held.push(w.acquire(1, true).await);
    }
    held.push(w.acquire(1, false).await);
    assert_eq!(w.peak_gets(), URGENT_GETS + 2);
}
