//! Bounds how much of a volume's upload work (hashing, recipes, compression) runs at once. In
//! the background it admits few, so the workload on the branch keeps the CPU; while someone
//! waits for a checkpoint it admits many, because then the wait is the workload.

use std::sync::Mutex;

use tokio::sync::Notify;

const HURRY_PARALLEL: usize = 16;
const BACKGROUND_PARALLEL: usize = 2;

#[derive(Debug, Default)]
struct State {
    busy: usize,
    waiters: usize,
}

#[derive(Debug, Default)]
pub(crate) struct Gate {
    state: Mutex<State>,
    changed: Notify,
}

/// One admitted piece of work; dropping it lets the next one in.
#[derive(Debug)]
pub(crate) struct Pass<'a>(&'a Gate);

/// Raises the limit while it lives.
#[derive(Debug)]
pub(crate) struct Hurry<'a>(&'a Gate);

impl Gate {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("gate lock")
    }

    pub(crate) async fn enter(&self) -> Pass<'_> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut s = self.lock();
                let limit = if s.waiters > 0 {
                    HURRY_PARALLEL
                } else {
                    BACKGROUND_PARALLEL
                };
                if s.busy < limit {
                    s.busy += 1;
                    return Pass(self);
                }
            }
            changed.await;
        }
    }

    /// Admits [`HURRY_PARALLEL`] at once until the returned value is dropped.
    pub(crate) fn hurry(&self) -> Hurry<'_> {
        self.lock().waiters += 1;
        self.changed.notify_waiters();
        Hurry(self)
    }
}

impl Drop for Pass<'_> {
    fn drop(&mut self) {
        self.0.lock().busy -= 1;
        self.0.changed.notify_waiters();
    }
}

impl Drop for Hurry<'_> {
    fn drop(&mut self) {
        self.0.lock().waiters -= 1;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    async fn peak(g: Arc<Gate>) -> usize {
        let (now, peak) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let (g, now, peak) = (g.clone(), now.clone(), peak.clone());
            tasks.push(tokio::spawn(async move {
                let _p = g.enter().await;
                peak.fetch_max(now.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                now.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        peak.load(Ordering::SeqCst)
    }

    #[tokio::test(start_paused = true)]
    async fn a_gate_admits_few_in_the_background_and_many_in_a_hurry() {
        let g = Arc::new(Gate::default());
        assert_eq!(peak(g.clone()).await, BACKGROUND_PARALLEL);
        let _h = g.hurry();
        assert_eq!(peak(g.clone()).await, HURRY_PARALLEL);
    }
}
