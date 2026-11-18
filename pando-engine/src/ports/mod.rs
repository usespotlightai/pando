//! What the engine needs from the world. Adapters: `pando-s3` and `pando-host` in production,
//! `pando-linux` for the device, [`crate::sim`] in tests.

mod clock;
mod cpu;
mod device;
mod local;
mod rng;
mod store;

use std::sync::Arc;

pub use clock::Clock;
pub use cpu::{Cpu, CpuClass, CpuJob, on_cpu};
pub use device::{DeviceSwitch, EraDevice, EraWritten};
pub use local::{Advice, DirEntry, Extent, FileLock, LocalFile, LocalFs, LocalImage, Open};
pub use rng::Rng;
pub use store::{Object, ObjectInfo, Store, StoreError, Version};

/// The ports one engine instance runs on.
#[derive(Clone, Debug)]
pub struct Ports {
    pub store: Arc<dyn Store>,
    pub fs: Arc<dyn LocalFs>,
    pub image: Arc<dyn LocalImage>,
    pub clock: Arc<dyn Clock>,
    pub rng: Arc<dyn Rng>,
    pub cpu: Arc<dyn Cpu>,
}
