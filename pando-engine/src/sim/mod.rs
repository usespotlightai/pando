//! In-memory adapters for tests and the deterministic simulator. Each is deterministic given
//! its seed and the order of calls; the store and the file system inject faults on request.

mod clock;
mod cpu;
mod device;
mod fs;
mod rng;
mod store;

use std::sync::Arc;

pub use clock::SimClock;
pub use cpu::InlineCpu;
pub use device::{SimBlock, SimDevices};
pub use fs::SimFs;
pub use rng::SimRng;
pub use store::{Fault, SimStore, StoreOp};

use crate::Ports;

/// Ports on in-memory adapters, seeded with `seed`.
pub fn ports(seed: u64) -> (Ports, Arc<SimStore>, Arc<SimFs>) {
    let clock = Arc::new(SimClock::new());
    let store = Arc::new(SimStore::new(Arc::<SimClock>::clone(&clock)));
    let fs = Arc::new(SimFs::default());
    let ports = Ports {
        store: Arc::<SimStore>::clone(&store),
        fs: Arc::<SimFs>::clone(&fs),
        image: Arc::<SimFs>::clone(&fs),
        clock,
        rng: Arc::new(SimRng::new(seed)),
        cpu: Arc::new(InlineCpu),
    };
    (ports, store, fs)
}
