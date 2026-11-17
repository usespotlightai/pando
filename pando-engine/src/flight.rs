//! Single flight: one caller does a piece of work, the others wait for its outcome.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tokio::sync::watch;

use crate::{Error, Result};

type Outcome = Option<Result<(), Arc<Error>>>;

#[derive(Debug)]
pub(crate) struct Call {
    done: watch::Sender<Outcome>,
    background: bool,
    waiters: AtomicU32,
}

impl Call {
    /// Whether someone waits for this call.
    pub(crate) fn awaited(&self) -> bool {
        self.waiters.load(Ordering::Acquire) > 0
    }

    pub(crate) async fn wait(&self) -> Result<()> {
        let mut rx = self.done.subscribe();
        let Ok(r) = rx.wait_for(Option::is_some).await else {
            return Err(Error::Cancelled);
        };
        match &*r {
            Some(Ok(())) => Ok(()),
            Some(Err(e)) => Err(Error::Shared(Arc::clone(e))),
            None => Err(Error::Cancelled),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Flights<K> {
    calls: Mutex<HashMap<K, Arc<Call>>>,
}

impl<K> Default for Flights<K> {
    fn default() -> Self {
        Flights {
            calls: Mutex::default(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum Joined<'a, K: Eq + Hash + Clone> {
    /// This caller does the work and must [`Claim::finish`] it.
    Lead(Claim<'a, K>),
    /// Another caller does it.
    Wait(Arc<Call>),
}

impl<K: Eq + Hash + Clone> Flights<K> {
    /// Joins the work on `k`. A foreground caller takes over work that a background caller
    /// leads: it does the work again rather than wait behind a long background job.
    pub(crate) fn join(&self, k: &K, background: bool) -> Joined<'_, K> {
        let mut calls = self.calls.lock();
        if let Some(c) = calls.get(k)
            && (background || !c.background)
        {
            c.waiters.fetch_add(1, Ordering::AcqRel);
            return Joined::Wait(Arc::clone(c));
        }
        let call = Arc::new(Call {
            done: watch::Sender::new(None),
            background,
            waiters: AtomicU32::new(0),
        });
        calls.insert(k.clone(), Arc::clone(&call));
        Joined::Lead(Claim {
            flights: self,
            key: k.clone(),
            call,
            finished: false,
        })
    }

    pub(crate) fn in_flight(&self, k: &K) -> bool {
        self.calls.lock().contains_key(k)
    }
}

/// The lead of a call. Dropping it unfinished fails the call, so no waiter waits forever.
#[derive(Debug)]
pub(crate) struct Claim<'a, K: Eq + Hash + Clone> {
    flights: &'a Flights<K>,
    key: K,
    call: Arc<Call>,
    finished: bool,
}

impl<K: Eq + Hash + Clone> Claim<'_, K> {
    pub(crate) fn call(&self) -> &Arc<Call> {
        &self.call
    }

    /// Ends the call with `r` and returns it, its error shared with the waiters.
    pub(crate) fn finish(mut self, r: Result<()>) -> Result<()> {
        self.finished = true;
        let outcome = r.map_err(|e| match e {
            Error::Shared(e) => e,
            e @ (Error::Fenced { .. }
            | Error::ChunkLost { .. }
            | Error::Store(_)
            | Error::Format(_)
            | Error::Io(_)
            | Error::BeyondEnd
            | Error::Shrink { .. }
            | Error::Corrupt(_)
            | Error::SegmentExists(_)
            | Error::RefExists(_)
            | Error::Cancelled) => Arc::new(e),
        });
        self.settle(outcome.clone());
        outcome.map_err(Error::Shared)
    }

    fn settle(&self, outcome: Result<(), Arc<Error>>) {
        {
            let mut calls = self.flights.calls.lock();
            if calls.get(&self.key).is_some_and(|c| Arc::ptr_eq(c, &self.call)) {
                calls.remove(&self.key);
            }
        }
        self.call.done.send_replace(Some(outcome));
    }
}

impl<K: Eq + Hash + Clone> Drop for Claim<'_, K> {
    fn drop(&mut self) {
        if !self.finished {
            self.settle(Err(Arc::new(Error::Cancelled)));
        }
    }
}

/// Runs `futs` with at most `limit` of them at once and returns their outputs as they finish.
pub(crate) async fn bounded<F: Future>(futs: Vec<F>, limit: usize) -> Vec<F::Output> {
    use futures::StreamExt;
    let mut it = futs.into_iter();
    let mut running: futures::stream::FuturesUnordered<F> = it.by_ref().take(limit.max(1)).collect();
    let mut out = Vec::new();
    while let Some(r) = running.next().await {
        out.push(r);
        if let Some(f) = it.next() {
            running.push(f);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn waiters_get_the_outcome_and_a_dropped_lead_cancels() {
        let f: Flights<u8> = Flights::default();
        let Joined::Lead(lead) = f.join(&1, true) else {
            panic!("first caller leads")
        };
        let Joined::Wait(w) = f.join(&1, true) else {
            panic!("second caller waits")
        };
        assert!(lead.call().awaited());
        let Joined::Lead(fg) = f.join(&1, false) else {
            panic!("a read takes over background work")
        };
        let r = lead.finish(Err(Error::BeyondEnd));
        assert!(matches!(r.unwrap_err().kind(), Error::BeyondEnd));
        assert!(matches!(w.wait().await.unwrap_err().kind(), Error::BeyondEnd));
        assert!(f.in_flight(&1), "the read's call stays");
        let Joined::Wait(w) = f.join(&1, false) else {
            panic!("waits for the read")
        };
        drop(fg);
        assert!(matches!(w.wait().await.unwrap_err().kind(), Error::Cancelled));
        assert!(!f.in_flight(&1));
    }
}
