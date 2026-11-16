//! Held recipes: recipe chunks of fetched frames whose sources are not on this host, kept as
//! their encodings until a read or a fetch asks for them.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::future::BoxFuture;
use pando_format::{Entry, Hash, Recipe};

use super::sched::FetchOpts;
use super::{Cache, FrameKey};
use crate::Result;
use crate::flight::{Flights, Joined, bounded};

/// Held encodings take at most this much memory; past it, frames rebuild recipes at once.
const HELD_MAX: u64 = 256 << 20;

/// A held recipe: its chunk and the frame that holds the recipe. A chunk can be stored in more
/// than one frame, as itself in one and as a recipe in another. A recipe names each source by
/// its frame, and a source frame is always older than the recipe, so a rebuild that follows the
/// frames ends; one that took any held recipe of a source could come back to a chunk it is
/// still rebuilding and wait for itself.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) struct HeldId {
    pub(super) hash: Hash,
    pub(super) at: FrameKey,
}

impl HeldId {
    fn of(e: &Entry) -> HeldId {
        HeldId {
            hash: e.hash,
            at: (e.loc.seg, e.loc.frame),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct Held {
    encodings: Mutex<HashMap<HeldId, Bytes>>,
    bytes: AtomicU64,
    flights: Flights<HeldId>,
}

impl Held {
    fn get(&self, id: &HeldId) -> Option<Bytes> {
        self.encodings.lock().expect("held lock").get(id).cloned()
    }

    fn remove(&self, id: &HeldId) {
        if let Some(b) = self.encodings.lock().expect("held lock").remove(id) {
            self.bytes.fetch_sub(b.len() as u64, Ordering::Relaxed);
        }
    }
}

impl Cache {
    /// Keeps the encoding of recipe chunk `id` until it is asked for; `false` when the budget
    /// is spent.
    pub(super) fn hold(&self, id: HeldId, enc: &Bytes) -> bool {
        let n = enc.len() as u64;
        if self.held.bytes.load(Ordering::Relaxed) + n > HELD_MAX {
            return false;
        }
        let mut m = self.held.encodings.lock().expect("held lock");
        if let std::collections::hash_map::Entry::Vacant(v) = m.entry(id) {
            v.insert(Bytes::copy_from_slice(enc));
            self.held.bytes.fetch_add(n, Ordering::Relaxed);
        }
        true
    }

    pub(super) fn held_encoding(&self, e: &Entry) -> Option<Bytes> {
        self.held.get(&HeldId::of(e))
    }

    /// Whether every source of a recipe is on this host.
    pub(super) async fn sources_here(&self, enc: &[u8]) -> bool {
        for s in Recipe::sources_of(enc) {
            if !self.has(&s.hash).await {
                return false;
            }
        }
        true
    }

    /// The sources of a recipe that are not on this host and can be fetched.
    pub(super) async fn missing_sources(&self, enc: &[u8]) -> Vec<Entry> {
        let mut out = Vec::new();
        for s in Recipe::sources_of(enc) {
            if !s.is_empty() && !s.loc.is_pending() && !self.has(&s.hash).await {
                out.push(s);
            }
        }
        out
    }

    /// Rebuilds the recipe chunk `e` if its frame's recipe is held, fetching its sources (as
    /// reads unless `background`), and stores it. One caller rebuilds a chunk; the others wait
    /// for it and read the stored chunk. The chunk stays held until it is stored.
    pub(super) async fn rebuild_if_held(&self, e: &Entry, background: bool) -> Result<Option<Bytes>> {
        let id = HeldId::of(e);
        let Some(enc) = self.held.get(&id) else {
            return Ok(None);
        };
        match self.held.flights.join(&id, background) {
            Joined::Wait(c) => {
                c.wait().await?;
                self.local(&e.hash).await
            }
            Joined::Lead(claim) => {
                let r = async {
                    let b = self.rebuild(e.hash, enc, background).await?;
                    self.put_local(e.hash, b.clone()).await?;
                    Ok(b)
                }
                .await;
                match r {
                    Ok(b) => {
                        self.held.remove(&id);
                        self.stats.held_rebuilt.fetch_add(1, Ordering::Relaxed);
                        claim.finish(Ok(()))?;
                        Ok(Some(b))
                    }
                    Err(err) => Err(claim.finish(Err(err)).expect_err("finished with an error")),
                }
            }
        }
    }

    /// Rebuilds the held recipe chunks among `es`: first their missing sources, as one fetch,
    /// then the chunks, `parallel` at a time.
    pub(super) fn rebuild_held<'a>(&'a self, es: &'a [Entry], parallel: usize) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut held = Vec::new();
            let mut seen = HashSet::new();
            let mut seen_src = HashSet::new();
            let mut sources = Vec::new();
            for e in es {
                let id = HeldId::of(e);
                let Some(enc) = self.held.get(&id) else { continue };
                if !seen.insert(id) {
                    continue;
                }
                held.push(*e);
                for s in self.missing_sources(&enc).await {
                    if seen_src.insert(s.hash) {
                        sources.push(s);
                    }
                }
            }
            if held.is_empty() {
                return;
            }
            self.fetch_with(
                &sources,
                &FetchOpts {
                    parallel,
                    ..FetchOpts::default()
                },
            )
            .await;
            let mut futs = Vec::with_capacity(held.len());
            for e in &held {
                futs.push(self.rebuild_if_held(e, true));
            }
            bounded(futs, parallel).await;
        })
    }
}
