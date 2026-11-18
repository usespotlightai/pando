use std::io;

use pando_engine::ports::{Cpu, CpuClass, CpuJob};

/// Two thread pools, so background hashing never queues ahead of a read the device waits for.
#[derive(Debug)]
pub(crate) struct PoolCpu {
    foreground: rayon::ThreadPool,
    background: rayon::ThreadPool,
}

impl PoolCpu {
    pub(crate) fn new(threads: usize, flushers: bool) -> io::Result<PoolCpu> {
        let pool = |n: usize, name: &'static str| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(n.max(1))
                .thread_name(move |i| format!("{name}-{i}"))
                .start_handler(move |_| {
                    if flushers {
                        let _ = crate::io_flusher();
                    }
                })
                .build()
                .map_err(io::Error::other)
        };
        Ok(PoolCpu {
            foreground: pool(threads, "cpu-fg")?,
            background: pool(threads / 2, "cpu-bg")?,
        })
    }
}

impl Cpu for PoolCpu {
    fn spawn(&self, class: CpuClass, job: CpuJob) {
        match class {
            CpuClass::Foreground => self.foreground.spawn(job),
            CpuClass::Background => self.background.spawn(job),
        }
    }
}
