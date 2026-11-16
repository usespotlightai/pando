use std::io;
use std::path::Path;
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
    async fn new(seed: u64) -> Host {
        let (ports, _, fs) = sim::ports(seed);
        Volume::create(&ports, "main", SIZE).await.unwrap();
        let cache = Arc::new(Cache::open(&ports, Path::new("/c")).await.unwrap());
        Host {
            devices: SimDevices::new((*fs).clone()),
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

#[tokio::test(start_paused = true)]
async fn a_handoff_moves_the_device_to_the_image_and_detach_publishes_its_writes() {
    let host = Host::new(1).await;
    let v = host.open().await;
    host.serve(&v).await;
    host.write(1, 0x11).await;
    host.write(5, 0x55).await;
    let env = host.env(false);
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let (h, rep) = hand_off(&env, &v, DM, image.clone(), none()).await.unwrap();
    assert!(!rep.shared);
    assert_eq!(rep.copied, 4);
    let live = host.devices.live_target(DM).await.unwrap().unwrap();
    assert_eq!(host.devices.loop_image(&live).await.unwrap(), Some(image.clone()));

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
    assert_eq!(host.published().await, want);
}

#[tokio::test(start_paused = true)]
async fn a_shared_base_handoff_snapshots_the_run_and_drops_old_clones() {
    let host = Host::new(2).await;
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
    let opts = SnapshotOptions {
        every: Duration::from_secs(5),
        burst: 0,
    };
    let (h, rep) = hand_off(&env, &v, DM, image.clone(), opts).await.unwrap();
    assert!(rep.shared);
    assert_eq!(rep.copied, 2);

    host.write(12, 0x12).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(h.stats.taken.load(Ordering::Relaxed) >= 1);
    assert_eq!(h.stats.failed.load(Ordering::Relaxed), 0);
    host.write(14, 0x14).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let clones = host.ports.fs.list(Path::new("/img")).await.unwrap();
    assert!(
        clones.iter().filter(|d| d.name.contains(".snap")).count() <= 2,
        "{clones:?}"
    );

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
async fn a_new_writer_rejoins_a_run_the_old_one_left_on_the_image() {
    let host = Host::new(3).await;
    let v = host.open().await;
    host.serve(&v).await;
    host.write(2, 0x22).await;
    let env = host.env(false);
    let (h, _) = hand_off(
        &env,
        &v,
        DM,
        image_path(&host.ports, Path::new("/img"), "main"),
        none(),
    )
    .await
    .unwrap();
    host.write(4, 0x44).await;
    drop(h);
    v.close().await;
    drop(v);

    let v = host.open().await;
    let h = rejoin(&env, &v, DM, Path::new("/img"), none())
        .await
        .unwrap()
        .expect("the device serves the image");
    host.write(6, 0x66).await;
    host.devices.remove(DM).await.unwrap();
    h.absorb_final(&v).await.unwrap();
    v.checkpoint().await.unwrap();
    h.remove(&v).await;
    v.close().await;
    assert_eq!(host.published().await, expect(&[(2, 0x22), (4, 0x44), (6, 0x66)]));
}

#[tokio::test(start_paused = true)]
async fn after_a_restart_the_next_attach_adopts_the_image() {
    let host = Host::new(4).await;
    let v = host.open().await;
    host.serve(&v).await;
    host.write(7, 0x77).await;
    let env = host.env(false);
    let image = image_path(&host.ports, Path::new("/img"), "main");
    let (h, _) = hand_off(&env, &v, DM, image.clone(), none()).await.unwrap();
    host.write(8, 0x88).await;
    host.devices.remove(DM).await.unwrap();
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
    assert_eq!(host.published().await, expect(&[(7, 0x77), (8, 0x88)]));
}
