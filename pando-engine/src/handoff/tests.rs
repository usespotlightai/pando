use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use pando_format::CHUNK_SIZE;

use super::adopt::{adopt_leftovers, rejoin};
use super::*;
use crate::cache::Cache;
use crate::sim::{self, SimBlock, SimDevices};
use crate::{Ports, Volume};

const SIZE: u64 = 2 << 20;
const DM: &str = "dm0";
const FRONT: &str = "/dev/ublkb0";

struct VolBlock(Arc<Volume>);

#[async_trait]
impl SimBlock for VolBlock {
    async fn read(&self, buf: &mut [u8], off: u64) -> io::Result<()> {
        self.0.read_at(buf, off).await.map_err(io::Error::other)
    }

    async fn write(&self, b: &[u8], off: u64) -> io::Result<()> {
        self.0.write_at(b, off).await.map_err(io::Error::other)
    }
}

struct Host {
    ports: Ports,
    devices: SimDevices,
    cache: Arc<Cache>,
}

impl Host {
    /// A host whose kernel has an era target when `eras`.
    async fn new(seed: u64, eras: bool) -> Host {
        let (ports, _, fs) = sim::ports(seed);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let cache = Arc::new(Cache::open(&ports, Path::new("/c")).await.unwrap());
        let fs = (*fs).clone();
        Host {
            devices: if eras {
                SimDevices::new(fs)
            } else {
                SimDevices::without_eras(fs)
            },
            ports,
            cache,
        }
    }

    fn env(&self, shared: bool) -> HostEnv {
        HostEnv {
            ports: self.ports.clone(),
            devices: Arc::new(self.devices.clone()),
            bases: shared.then(|| Bases::new(&self.ports, Path::new("/bases"))),
            dedup: shared,
            learned: None,
        }
    }

    async fn open(&self) -> Arc<Volume> {
        Arc::new(
            Volume::open(&self.ports, self.cache.clone(), Path::new("/v"), "main")
                .await
                .unwrap(),
        )
    }

    async fn serve(&self, v: &Arc<Volume>) {
        self.devices
            .add_block(Path::new(FRONT), Arc::new(VolBlock(v.clone())));
        if self.devices.live_target(DM).await.unwrap().is_none() {
            self.devices.create(DM, Path::new(FRONT), SIZE).await.unwrap();
        }
    }

    async fn write(&self, at_chunk: u64, fill: u8) {
        self.devices
            .write(DM, &vec![fill; CHUNK_SIZE + 100], at_chunk * CHUNK_SIZE as u64)
            .await
            .unwrap();
    }

    /// The image the device serves, through the era device when there is one.
    async fn served_image(&self) -> Option<PathBuf> {
        let mut live = self.devices.live_target(DM).await.unwrap()?;
        if let Some(e) = self.devices.era(&era_name(DM)).await.unwrap()
            && e.path == live
        {
            live = e.origin;
        }
        self.devices.loop_image(&live).await.unwrap()
    }

    async fn clones(&self) -> Vec<PathBuf> {
        let mut out: Vec<PathBuf> = self
            .ports
            .fs
            .list(Path::new("/img"))
            .await
            .unwrap()
            .into_iter()
            .filter(|d| d.name.contains(".snap"))
            .map(|d| Path::new("/img").join(d.name))
            .collect();
        out.sort_by_key(|p| {
            let s = p.to_string_lossy();
            s[s.rfind(".snap").unwrap() + 5..].parse::<u32>().unwrap()
        });
        out
    }

    /// The branch as a fresh host reads it from the store.
    async fn published(&self) -> Vec<u8> {
        let c = Arc::new(Cache::open(&self.ports, Path::new("/fresh")).await.unwrap());
        let v = Volume::open(&self.ports, c, Path::new("/fresh-v"), "main")
            .await
            .unwrap();
        let mut b = vec![0; SIZE as usize];
        v.read_at(&mut b, 0).await.unwrap();
        v.close().await;
        let _ = self.ports.fs.remove(Path::new("/fresh-v/state/main")).await;
        b
    }
}

fn expect(writes: &[(u64, u8)]) -> Vec<u8> {
    let mut b = vec![0; SIZE as usize];
    for &(at, fill) in writes {
        let off = at as usize * CHUNK_SIZE;
        b[off..off + CHUNK_SIZE + 100].fill(fill);
    }
    b
}

fn none() -> SnapshotOptions {
    SnapshotOptions::default()
}

fn every(secs: u64) -> SnapshotOptions {
    SnapshotOptions {
        every: Duration::from_secs(secs),
        ..SnapshotOptions::default()
    }
}

async fn moves_the_device_and_publishes_at_detach(seed: u64, eras: bool) {
    let host = Host::new(seed, eras).await;
    let v = host.open().await;
    host.serve(&v).await;
    host.write(1, 0x11).await;
    host.write(5, 0x55).await;
    let env = host.env(false);
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let (h, rep) = hand_off(&env, &v, DM, image.clone(), none()).await.unwrap();
    assert!(!rep.shared);
    assert_eq!(rep.tracked, eras);
    assert_eq!(rep.copied, 4);
    assert_eq!(host.served_image().await, Some(image.clone()));

    host.write(9, 0x99).await;
    host.write(1, 0x12).await;
    let want = expect(&[(1, 0x11), (5, 0x55), (9, 0x99), (1, 0x12)]);
    let mut got = vec![0; SIZE as usize];
    host.devices.read(DM, &mut got, 0).await.unwrap();
    assert_eq!(got, want);

    host.devices.remove(DM).await.unwrap();
    assert_eq!(h.absorb_final(&v).await.unwrap(), 4);
    v.checkpoint().await.unwrap();
    h.remove(&v).await;
    v.close().await;
    assert!(host.ports.fs.open(&image, Open::Read).await.is_err());
    assert_eq!(host.devices.era(&era_name(DM)).await.unwrap(), None);
    let left = host.ports.fs.list(Path::new("/img")).await.unwrap();
    assert!(left.is_empty(), "{left:?}");
    assert_eq!(host.published().await, want);
}

#[tokio::test(start_paused = true)]
async fn a_handoff_moves_the_device_to_the_image_and_detach_publishes_its_writes() {
    moves_the_device_and_publishes_at_detach(1, true).await;
}

#[tokio::test(start_paused = true)]
async fn without_eras_a_handoff_moves_the_device_and_detach_publishes_its_writes() {
    moves_the_device_and_publishes_at_detach(11, false).await;
}

async fn snapshots_a_shared_base_run(seed: u64, eras: bool) {
    let host = Host::new(seed, eras).await;
    let v = host.open().await;
    host.serve(&v).await;
    host.write(0, 0x01).await;
    host.write(20, 0x20).await;
    v.checkpoint().await.unwrap();
    v.close().await;
    let v = host.open().await;
    host.serve(&v).await;
    host.write(3, 0x03).await;

    let env = host.env(true);
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let (h, rep) = hand_off(&env, &v, DM, image.clone(), every(5)).await.unwrap();
    assert!(rep.shared);
    assert_eq!(rep.tracked, eras);
    assert_eq!(rep.copied, 2);

    host.write(12, 0x12).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(h.stats.taken.load(Ordering::Relaxed) >= 1);
    assert_eq!(h.stats.failed.load(Ordering::Relaxed), 0);
    host.write(14, 0x14).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let clones = host.clones().await;
    assert!(clones.len() <= 2, "{clones:?}");

    host.devices.remove(DM).await.unwrap();
    h.absorb_final(&v).await.unwrap();
    v.checkpoint().await.unwrap();
    h.remove(&v).await;
    v.close().await;
    let left = host.ports.fs.list(Path::new("/img")).await.unwrap();
    assert!(left.is_empty(), "{left:?}");
    assert_eq!(
        host.published().await,
        expect(&[(0, 1), (20, 0x20), (3, 3), (12, 0x12), (14, 0x14)])
    );
}

#[tokio::test(start_paused = true)]
async fn a_shared_base_handoff_snapshots_the_run_and_drops_old_clones() {
    snapshots_a_shared_base_run(2, true).await;
}

#[tokio::test(start_paused = true)]
async fn without_eras_a_shared_base_handoff_snapshots_the_run() {
    snapshots_a_shared_base_run(12, false).await;
}

#[tokio::test(start_paused = true)]
async fn an_era_snapshot_clones_only_the_chunks_written_since_the_last() {
    let host = Host::new(5, true).await;
    let v = host.open().await;
    host.serve(&v).await;
    for c in 0..SIZE / CHUNK_SIZE as u64 {
        host.devices
            .write(DM, &vec![0x5a; CHUNK_SIZE], c * CHUNK_SIZE as u64)
            .await
            .unwrap();
    }
    let env = host.env(false);
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let (h, rep) = hand_off(&env, &v, DM, image.clone(), every(5)).await.unwrap();
    assert!(rep.tracked);

    host.write(12, 0x12).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(h.stats.chunks.load(Ordering::Relaxed), 2, "chunks 12 and 13");
    let clones = host.clones().await;
    let latest = clones.last().expect("a clone");
    let image_bytes = host.ports.image.allocated(&image).await.unwrap();
    let clone_bytes = host.ports.image.allocated(latest).await.unwrap();
    assert_eq!(image_bytes, SIZE);
    assert!(clone_bytes <= 2 * CHUNK_SIZE as u64, "{clone_bytes}");

    let taken = h.stats.taken.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(h.stats.taken.load(Ordering::Relaxed) > taken);
    assert_eq!(h.stats.chunks.load(Ordering::Relaxed), 2);
    assert_eq!(&host.clones().await, &clones, "nothing written, no clone");

    host.write(20, 0x20).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(h.stats.chunks.load(Ordering::Relaxed), 4, "only chunks 20 and 21");

    host.write(30, 0x30).await;
    host.devices.remove(DM).await.unwrap();
    assert_eq!(h.absorb_final(&v).await.unwrap(), 2, "only chunks 30 and 31");
    v.checkpoint().await.unwrap();
    h.remove(&v).await;
    v.close().await;
    let mut want = vec![0x5a; SIZE as usize];
    for (at, fill) in [(12usize, 0x12u8), (20, 0x20), (30, 0x30)] {
        want[at * CHUNK_SIZE..at * CHUNK_SIZE + CHUNK_SIZE + 100].fill(fill);
    }
    assert_eq!(host.published().await, want);
}

fn copying(secs: u64, copy_max: u64) -> SnapshotOptions {
    SnapshotOptions {
        copy_max,
        ..every(secs)
    }
}

#[tokio::test(start_paused = true)]
async fn a_copy_snapshot_reads_the_written_chunks_without_a_clone() {
    let host = Host::new(11, true).await;
    let v = host.open().await;
    host.serve(&v).await;
    host.write(1, 0x11).await;
    let env = host.env(false);
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let (h, rep) = hand_off(&env, &v, DM, image, copying(5, 1 << 20)).await.unwrap();
    assert!(rep.tracked);

    host.write(12, 0x12).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(h.stats.chunks.load(Ordering::Relaxed), 2, "chunks 12 and 13");
    assert_eq!(h.stats.copied.load(Ordering::Relaxed), 1);
    assert!(host.clones().await.is_empty());

    host.write(20, 0x20).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(h.stats.chunks.load(Ordering::Relaxed), 4, "only chunks 20 and 21");
    assert!(host.clones().await.is_empty());

    host.write(30, 0x30).await;
    host.devices.remove(DM).await.unwrap();
    assert_eq!(h.absorb_final(&v).await.unwrap(), 2, "only chunks 30 and 31");
    v.checkpoint().await.unwrap();
    h.remove(&v).await;
    v.close().await;
    assert_eq!(
        host.published().await,
        expect(&[(1, 0x11), (12, 0x12), (20, 0x20), (30, 0x30)])
    );
}

fn chunk_writes(first: u64, n: u64, fill: u8) -> Vec<(u64, Vec<u8>)> {
    (first..first + n)
        .map(|c| (c * CHUNK_SIZE as u64, vec![fill; CHUNK_SIZE]))
        .collect()
}

fn chunks_filled(runs: &[(u64, u64, u8)]) -> Vec<u8> {
    let mut b = vec![0; SIZE as usize];
    for &(first, n, fill) in runs {
        b[first as usize * CHUNK_SIZE..(first + n) as usize * CHUNK_SIZE].fill(fill);
    }
    b
}

#[tokio::test(start_paused = true)]
async fn a_copy_snapshot_reads_again_what_was_written_while_it_read() {
    let host = Host::new(15, true).await;
    let v = host.open().await;
    host.serve(&v).await;
    let env = host.env(false);
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let (h, _) = hand_off(&env, &v, DM, image, copying(5, 1 << 20)).await.unwrap();
    for (off, b) in chunk_writes(0, 10, 0x01) {
        host.devices.write(DM, &b, off).await.unwrap();
    }
    host.devices
        .write_at_era_begins(&era_name(DM), vec![vec![], chunk_writes(0, 2, 0x02)]);
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(h.stats.copied.load(Ordering::Relaxed), 1);
    assert_eq!(h.stats.postponed.load(Ordering::Relaxed), 0);
    assert_eq!(h.stats.chunks.load(Ordering::Relaxed), 12, "ten, then two again");
    assert!(host.clones().await.is_empty());
    host.devices.remove(DM).await.unwrap();
    assert_eq!(h.absorb_final(&v).await.unwrap(), 0);
    v.checkpoint().await.unwrap();
    h.remove(&v).await;
    v.close().await;
    assert_eq!(
        host.published().await,
        chunks_filled(&[(0, 2, 0x02), (2, 8, 0x01)])
    );
}

#[tokio::test(start_paused = true)]
async fn a_clone_snapshot_clones_again_what_was_written_while_it_cloned() {
    let host = Host::new(17, true).await;
    let v = host.open().await;
    host.serve(&v).await;
    let env = host.env(false);
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let (h, _) = hand_off(&env, &v, DM, image, every(5)).await.unwrap();
    for (off, b) in chunk_writes(0, 10, 0x01) {
        host.devices.write(DM, &b, off).await.unwrap();
    }
    host.devices.write_at_era_begins(
        &era_name(DM),
        vec![vec![], chunk_writes(0, 2, 0x02), chunk_writes(3, 1, 0x03)],
    );
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(h.stats.taken.load(Ordering::Relaxed), 1);
    assert_eq!(h.stats.chunks.load(Ordering::Relaxed), 10);
    assert_eq!(host.clones().await.len(), 1);
    host.devices.remove(DM).await.unwrap();
    assert_eq!(h.absorb_final(&v).await.unwrap(), 0);
    v.checkpoint().await.unwrap();
    h.remove(&v).await;
    v.close().await;
    assert_eq!(
        host.published().await,
        chunks_filled(&[(0, 2, 0x02), (2, 1, 0x01), (3, 1, 0x03), (4, 6, 0x01)])
    );
}

#[tokio::test(start_paused = true)]
async fn writes_that_do_not_let_up_postpone_copy_snapshots_until_one_clones() {
    let host = Host::new(16, true).await;
    let v = host.open().await;
    host.serve(&v).await;
    let env = host.env(false);
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let (h, _) = hand_off(&env, &v, DM, image, copying(1, 1 << 20)).await.unwrap();
    let batches = (1..=2 * u8::try_from(POSTPONE_MAX).unwrap())
        .map(|fill| chunk_writes(20, 10, fill))
        .collect();
    host.devices.write_at_era_begins(&era_name(DM), batches);
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(h.stats.postponed.load(Ordering::Relaxed), POSTPONE_MAX as usize);
    assert_eq!(h.stats.copied.load(Ordering::Relaxed), 0);
    assert_eq!(host.clones().await.len(), 1);
    host.devices.remove(DM).await.unwrap();
    h.absorb_final(&v).await.unwrap();
    v.checkpoint().await.unwrap();
    h.remove(&v).await;
    v.close().await;
    let last = 2 * u8::try_from(POSTPONE_MAX).unwrap();
    assert_eq!(host.published().await, chunks_filled(&[(20, 10, last)]));
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_of_more_than_it_may_read_clones_the_chunks() {
    let host = Host::new(12, true).await;
    let v = host.open().await;
    host.serve(&v).await;
    let env = host.env(false);
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let (h, _) = hand_off(&env, &v, DM, image, copying(5, CHUNK_SIZE as u64))
        .await
        .unwrap();
    host.write(12, 0x12).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(h.stats.chunks.load(Ordering::Relaxed), 2);
    assert_eq!(h.stats.copied.load(Ordering::Relaxed), 0);
    assert_eq!(host.clones().await.len(), 1);
    host.devices.remove(DM).await.unwrap();
    h.absorb_final(&v).await.unwrap();
    v.checkpoint().await.unwrap();
    h.remove(&v).await;
    v.close().await;
    assert_eq!(host.published().await, expect(&[(12, 0x12)]));
}

#[tokio::test(start_paused = true)]
async fn a_chunk_read_again_as_it_was_drops_the_write_read_before() {
    let host = Host::new(13, true).await;
    let v = host.open().await;
    host.serve(&v).await;
    let env = host.env(false);
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let (h, _) = hand_off(&env, &v, DM, image, none()).await.unwrap();
    host.write(24, 0x24).await;
    assert_eq!(v.read_chunks(&h.file, &[24, 25]).await.unwrap(), 2);
    host.devices
        .write(DM, &vec![0; CHUNK_SIZE + 100], 24 * CHUNK_SIZE as u64)
        .await
        .unwrap();
    assert_eq!(
        v.read_chunks(&h.file, &[24, 25]).await.unwrap(),
        2,
        "both dropped"
    );
    assert_eq!(v.read_chunks(&h.file, &[24, 25]).await.unwrap(), 0);
    v.flush().await.unwrap();
    host.devices.remove(DM).await.unwrap();
    h.absorb_final(&v).await.unwrap();
    v.checkpoint().await.unwrap();
    h.remove(&v).await;
    v.close().await;
    assert_eq!(host.published().await, expect(&[]));
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_comes_once_the_writes_stop() {
    let host = Host::new(14, true).await;
    let v = host.open().await;
    host.serve(&v).await;
    let env = host.env(false);
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let opts = SnapshotOptions {
        idle: Duration::from_secs(1),
        ..copying(600, 1 << 20)
    };
    let (h, _) = hand_off(&env, &v, DM, image, opts).await.unwrap();
    for i in 0..5 {
        host.write(10 + 2 * i, 0x10).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert_eq!(h.stats.taken.load(Ordering::Relaxed), 0, "still writing");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(h.stats.taken.load(Ordering::Relaxed), 1);
    assert_eq!(h.stats.chunks.load(Ordering::Relaxed), 10);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(h.stats.taken.load(Ordering::Relaxed), 1, "nothing written since");
    host.devices.remove(DM).await.unwrap();
    h.absorb_final(&v).await.unwrap();
    h.remove(&v).await;
    v.close().await;
}

#[tokio::test(start_paused = true)]
async fn a_detach_without_the_era_device_takes_every_chunk_that_differs() {
    let host = Host::new(6, true).await;
    let v = host.open().await;
    host.serve(&v).await;
    host.write(2, 0x22).await;
    let env = host.env(false);
    let (h, rep) = hand_off(
        &env,
        &v,
        DM,
        image_path(&host.ports, Path::new("/img"), "main"),
        none(),
    )
    .await
    .unwrap();
    assert!(rep.tracked);
    host.write(7, 0x77).await;
    host.devices.remove(DM).await.unwrap();
    host.devices.remove(&era_name(DM)).await.unwrap();
    h.absorb_final(&v).await.unwrap();
    v.checkpoint().await.unwrap();
    h.remove(&v).await;
    v.close().await;
    assert_eq!(host.published().await, expect(&[(2, 0x22), (7, 0x77)]));
}

#[tokio::test(start_paused = true)]
async fn a_handoff_replaces_an_era_device_an_earlier_run_left() {
    let host = Host::new(7, true).await;
    let fs = host.ports.fs.as_ref();
    fs.create_dir_all(Path::new("/old")).await.unwrap();
    for p in ["/old/img", "/old/meta"] {
        fs.open(Path::new(p), Open::Create)
            .await
            .unwrap()
            .set_len(SIZE)
            .await
            .unwrap();
    }
    let olp = host.devices.attach_loop(Path::new("/old/img")).await.unwrap();
    let mlp = host.devices.attach_loop(Path::new("/old/meta")).await.unwrap();
    host.devices
        .create_era(&era_name(DM), &mlp, &olp, SIZE, CHUNK_SIZE as u64)
        .await
        .unwrap();

    let v = host.open().await;
    host.serve(&v).await;
    host.write(4, 0x44).await;
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let (h, rep) = hand_off(&host.env(false), &v, DM, image.clone(), none())
        .await
        .unwrap();
    assert!(rep.tracked);
    assert_eq!(host.served_image().await, Some(image));
    host.write(9, 0x99).await;
    host.devices.remove(DM).await.unwrap();
    h.absorb_final(&v).await.unwrap();
    v.checkpoint().await.unwrap();
    h.remove(&v).await;
    v.close().await;
    assert_eq!(host.published().await, expect(&[(4, 0x44), (9, 0x99)]));
}

async fn rejoins_a_run_the_old_writer_left(seed: u64, eras: bool, shared: bool) {
    let host = Host::new(seed, eras).await;
    let v = host.open().await;
    host.serve(&v).await;
    host.write(2, 0x22).await;
    if shared {
        v.checkpoint().await.unwrap();
        v.close().await;
        drop(v);
        let v = host.open().await;
        host.serve(&v).await;
        host.write(3, 0x33).await;
        return rejoin_after_handoff(&host, v, shared, &[(2, 0x22), (3, 0x33)]).await;
    }
    rejoin_after_handoff(&host, v, shared, &[(2, 0x22)]).await;
}

async fn rejoin_after_handoff(host: &Host, v: Arc<Volume>, shared: bool, before: &[(u64, u8)]) {
    let env = host.env(shared);
    let (h, _) = hand_off(
        &env,
        &v,
        DM,
        image_path(&host.ports, Path::new("/img"), "main"),
        every(5),
    )
    .await
    .unwrap();
    host.write(4, 0x44).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    host.write(5, 0x55).await;
    h.stop.send_replace(true);
    let task = h.task.lock().expect("task lock").take();
    if let Some(t) = task {
        t.await.unwrap();
    }
    drop(h);
    v.close().await;
    drop(v);

    let v = host.open().await;
    let h = rejoin(&env, &v, DM, Path::new("/img"), every(5))
        .await
        .unwrap()
        .expect("the device serves the image");
    host.write(6, 0x66).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    host.write(8, 0x88).await;
    host.devices.remove(DM).await.unwrap();
    h.absorb_final(&v).await.unwrap();
    v.checkpoint().await.unwrap();
    h.remove(&v).await;
    v.close().await;
    assert_eq!(host.devices.era(&era_name(DM)).await.unwrap(), None);
    let mut want: Vec<(u64, u8)> = before.to_vec();
    want.extend([(4, 0x44), (5, 0x55), (6, 0x66), (8, 0x88)]);
    assert_eq!(host.published().await, expect(&want));
}

#[tokio::test(start_paused = true)]
async fn a_new_writer_rejoins_a_run_the_old_one_left_on_the_image() {
    rejoins_a_run_the_old_writer_left(3, true, false).await;
}

#[tokio::test(start_paused = true)]
async fn a_new_writer_rejoins_a_shared_base_run_through_the_era_device() {
    rejoins_a_run_the_old_writer_left(13, true, true).await;
}

#[tokio::test(start_paused = true)]
async fn without_eras_a_new_writer_rejoins_a_run() {
    rejoins_a_run_the_old_writer_left(23, false, false).await;
}

#[tokio::test(start_paused = true)]
async fn without_eras_a_new_writer_rejoins_a_shared_base_run() {
    rejoins_a_run_the_old_writer_left(33, false, true).await;
}

#[tokio::test(start_paused = true)]
async fn after_a_restart_the_next_attach_adopts_the_image() {
    let host = Host::new(4, true).await;
    let v = host.open().await;
    host.serve(&v).await;
    host.write(7, 0x77).await;
    let env = host.env(false);
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let (h, _) = hand_off(&env, &v, DM, image.clone(), none()).await.unwrap();
    host.write(8, 0x88).await;
    host.devices.remove(DM).await.unwrap();
    host.devices.remove(&era_name(DM)).await.unwrap();
    drop(h);
    v.close().await;
    drop(v);

    let v = host.open().await;
    assert!(
        rejoin(&env, &v, DM, Path::new("/img"), none())
            .await
            .unwrap()
            .is_none()
    );
    assert!(adopt_leftovers(&env, &v, Path::new("/img")).await.unwrap() >= 1);
    v.close().await;
    assert!(host.ports.fs.open(&image, Open::Read).await.is_err());
    let left = host.ports.fs.list(Path::new("/img")).await.unwrap();
    assert!(left.is_empty(), "{left:?}");
    assert_eq!(host.published().await, expect(&[(7, 0x77), (8, 0x88)]));
}

#[test]
fn merged_ranges_are_sorted_and_disjoint() {
    assert_eq!(merge(&[4..6, 0..2], &[1..3, 6..7, 9..9]), vec![0..3, 4..7]);
    assert_eq!(merge(&[], &[]), Vec::<std::ops::Range<u64>>::new());
}
