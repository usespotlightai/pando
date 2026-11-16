use crate::ports::{Cpu, CpuClass, CpuJob};

/// Runs each job at once on the calling task.
#[derive(Debug, Default)]
pub struct InlineCpu;

impl Cpu for InlineCpu {
    fn spawn(&self, _class: CpuClass, job: CpuJob) {
        job();
    }
}
