//! Chunks an absorb read and hashed, made ready for the next upload, so the upload neither
//! reads nor hashes them again.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use pando_format::Hash;

use super::{Dedup, Prepared, prepare};

/// Prepared chunks take at most this much memory.
const BUDGET: usize = 256 << 20;

#[derive(Debug)]
struct Slot {
    chunk: Prepared,
    dedup: Option<Arc<Dedup>>,
    flush: u64,
}

/// Each chunk holds its own copy of the bytes its hash was computed over, or the recipe made
/// from them, so taking one is as safe as a read that checks the hash. A chunk is valid only
/// for the [`Dedup`] it was prepared with.
#[derive(Debug)]
pub(crate) struct PreparedChunks {
    chunks: Mutex<(HashMap<Hash, Slot>, usize)>,
    budget: usize,
}

impl Default for PreparedChunks {
    fn default() -> PreparedChunks {
        PreparedChunks {
            chunks: Mutex::default(),
            budget: BUDGET,
        }
    }
}

fn same(a: Option<&Arc<Dedup>>, b: Option<&Arc<Dedup>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}

impl PreparedChunks {
    /// Prepares chunk `h` from `b`, which hashes to `h`, for flush `flush` or a later one. It
    /// does nothing when the budget is spent.
    pub(crate) async fn add(&self, h: Hash, b: &[u8], dedup: Option<Arc<Dedup>>, flush: u64) {
        {
            let m = self.chunks.lock().expect("prepared lock");
            if m.0.contains_key(&h) || m.1 + b.len() > self.budget {
                return;
            }
        }
        let chunk = prepare(dedup.as_deref(), h, Bytes::copy_from_slice(b)).await;
        let n = chunk.held_bytes();
        let mut m = self.chunks.lock().expect("prepared lock");
        if m.0.contains_key(&h) || m.1 + n > self.budget {
            return;
        }
        m.0.insert(h, Slot { chunk, dedup, flush });
        m.1 += n;
    }

    /// Takes chunk `h` when it was prepared with `dedup`.
    pub(crate) fn take(&self, h: &Hash, dedup: Option<&Arc<Dedup>>) -> Option<Prepared> {
        let mut m = self.chunks.lock().expect("prepared lock");
        let s = m.0.remove(h)?;
        m.1 -= s.chunk.held_bytes();
        same(s.dedup.as_ref(), dedup).then_some(s.chunk)
    }

    /// Drops the chunks of flushes up to `flush`, which an upload has published.
    pub(crate) fn drop_through(&self, flush: u64) {
        let mut m = self.chunks.lock().expect("prepared lock");
        let (chunks, bytes) = &mut *m;
        chunks.retain(|_, s| {
            let keep = s.flush > flush;
            if !keep {
                *bytes -= s.chunk.held_bytes();
            }
            keep
        });
    }

    pub(crate) fn len(&self) -> usize {
        self.chunks.lock().expect("prepared lock").0.len()
    }
}
