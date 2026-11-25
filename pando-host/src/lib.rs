//! The host ports on a real machine. Linux gets reflinks, extent hints, hole punching and
//! page-cache-only reads; other systems (macOS for development) get plain fallbacks, so the
//! engine's tests and base builds run there too.

mod clock;
mod cpu;
mod fs;
#[cfg(target_os = "linux")]
mod linux;
mod memory;

use std::sync::Arc;

use clock::{SystemClock, SystemRng};
use cpu::PoolCpu;
pub use fs::HostFs;
pub use memory::memory_limit;
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

/// Has glibc's malloc map every allocation of 128 KiB or more on its own, so freeing one
/// returns its memory to the kernel at once. By default glibc raises that threshold to the size
/// of each such buffer freed, up to 32 MiB, and then keeps large buffers in its per-thread
/// arenas, which hold on to freed memory; a daemon whose large buffers come and go as data is
/// written would hold more memory the more it writes. For a daemon that serves a device; call
/// it before starting threads. Does nothing without glibc.
pub fn tune_malloc() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    linux::tune_malloc();
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
