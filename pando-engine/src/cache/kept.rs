//! Kept frames: compressed frames decoded in part, held so that the next GET of exactly that
//! frame is served from memory. Their bytes are checked when that fetch decodes them, as any
//! fetched bytes are.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use pando_format::SegId;

use super::Cache;

/// Kept frames take at most this much memory.
const KEPT_MAX: u64 = 256 << 20;
/// How long a frame is kept.
const KEPT_FOR: Duration = Duration::from_secs(60);

#[derive(Debug, Default)]
pub(super) struct Kept {
    frames: Mutex<HashMap<(SegId, u32), (u64, Bytes)>>,
    bytes: AtomicU64,
    next: AtomicU64,
}

impl Kept {
    /// The kept frame at `off` of `seg`, when it is exactly `n` bytes; it is kept no longer.
    pub(super) fn take(&self, seg: SegId, off: u32, n: u32) -> Option<Bytes> {
        let (_, b) = self.frames.lock().expect("kept lock").remove(&(seg, off))?;
        self.bytes.fetch_sub(b.len() as u64, Ordering::Relaxed);
        (b.len() == n as usize).then_some(b)
    }

    pub(super) fn has(&self, seg: SegId, off: u32) -> bool {
        self.frames.lock().expect("kept lock").contains_key(&(seg, off))
    }

    fn expire(&self, k: (SegId, u32), id: u64) {
        let mut m = self.frames.lock().expect("kept lock");
        if m.get(&k).is_some_and(|(i, _)| *i == id) {
            let (_, b) = m.remove(&k).expect("checked above");
            self.bytes.fetch_sub(b.len() as u64, Ordering::Relaxed);
        }
    }
}

impl Cache {
    pub(super) fn keep_frame(&self, seg: SegId, off: u32, z: &[u8]) {
        let n = z.len() as u64;
        if self.kept.bytes.fetch_add(n, Ordering::Relaxed) + n > KEPT_MAX {
            self.kept.bytes.fetch_sub(n, Ordering::Relaxed);
            return;
        }
        let id = self.kept.next.fetch_add(1, Ordering::Relaxed);
        {
            let mut m = self.kept.frames.lock().expect("kept lock");
            if m.contains_key(&(seg, off)) {
                self.kept.bytes.fetch_sub(n, Ordering::Relaxed);
                return;
            }
            m.insert((seg, off), (id, Bytes::copy_from_slice(z)));
        }
        let kept = Arc::clone(&self.kept);
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                tokio::time::sleep(KEPT_FOR).await;
                kept.expire((seg, off), id);
            });
        }
    }

    pub(super) fn is_kept(&self, seg: SegId, off: u32) -> bool {
        self.kept.has(seg, off)
    }
}
