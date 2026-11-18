//! The host ports on a real machine. Linux gets reflinks, extent hints, hole punching and
//! page-cache-only reads; other systems (macOS for development) get plain fallbacks, so the
//! engine's tests and base builds run there too.

mod clock;
mod cpu;
mod fs;
#[cfg(target_os = "linux")]
mod linux;

use std::sync::Arc;

use clock::{SystemClock, SystemRng};
use cpu::PoolCpu;
pub use fs::HostFs;
use pando_engine::Ports;
use pando_engine::ports::Store;

/// Ports on this machine around `store`, with `cpu_threads` threads for foreground CPU work.
pub fn ports(store: Arc<dyn Store>, cpu_threads: usize) -> std::io::Result<Ports> {
    let fs = Arc::new(HostFs);
    Ok(Ports {
        store,
        fs: Arc::<HostFs>::clone(&fs),
        image: fs,
        clock: Arc::new(SystemClock),
        rng: Arc::new(SystemRng),
        cpu: Arc::new(PoolCpu::new(cpu_threads)?),
    })
}
