use std::fmt;

/// Which work waits for a job: a read the device is blocked on, or background work.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CpuClass {
    /// A read the device is blocked on waits for it; it never queues behind background jobs.
    Foreground,
    /// No read waits for it: uploads, base builds, imports.
    Background,
}

/// A job for [`Cpu::spawn`].
pub type CpuJob = Box<dyn FnOnce() + Send + 'static>;

/// Runs CPU-heavy work (hashing, compression) off the I/O runtime. The simulator runs each job
/// at once, so a run stays deterministic.
pub trait Cpu: Send + Sync + fmt::Debug {
    /// Runs `job` once, on the pool for `class`. It may run it before returning, and never
    /// drops it unrun.
    fn spawn(&self, class: CpuClass, job: CpuJob);
}

/// Runs `f` on `cpu` and waits for its result.
///
/// # Panics
/// When `f` panics.
pub async fn on_cpu<T: Send + 'static>(
    cpu: &dyn Cpu,
    class: CpuClass,
    f: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (tx, rx) = tokio::sync::oneshot::channel();
    cpu.spawn(
        class,
        Box::new(move || {
            let _ = tx.send(f());
        }),
    );
    #[expect(
        clippy::expect_used,
        reason = "a Cpu runs every job, so the sender drops unsent only when `f` panicked, and that panic propagates"
    )]
    rx.await.expect("a CPU job panicked")
}
