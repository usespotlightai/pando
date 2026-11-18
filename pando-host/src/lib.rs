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

/// Marks the calling thread as one that writes data out for the kernel (`PR_SET_IO_FLUSHER`):
/// its allocations never wait on I/O to free memory, so a thread serving a block device cannot
/// wait on writeback of that device. Needs `CAP_SYS_RESOURCE`; does nothing off Linux.
pub fn io_flusher() -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    rustix::process::configure_io_flusher_behavior(true)?;
    Ok(())
}

/// Ports on this machine around `store`, with `cpu_threads` threads for foreground CPU work.
/// With `flushers`, the CPU threads try [`io_flusher`], for a process that serves a device.
pub fn ports(store: Arc<dyn Store>, cpu_threads: usize, flushers: bool) -> std::io::Result<Ports> {
    let fs = Arc::new(HostFs);
    Ok(Ports {
        store,
        fs: Arc::<HostFs>::clone(&fs),
        image: fs,
        clock: Arc::new(SystemClock),
        rng: Arc::new(SystemRng),
        cpu: Arc::new(PoolCpu::new(cpu_threads, flushers)?),
    })
}
