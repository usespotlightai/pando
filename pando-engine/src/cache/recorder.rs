//! The chunks foreground reads needed, in the order they first needed them, for the recording
//! of the root the branch opened at.

use std::collections::HashSet;

use pando_format::{Entry, Hash, SegId, SegmentIndex};

use super::Cache;

#[derive(Debug, Default)]
pub(super) struct Recorder {
    seen: HashSet<Hash>,
    order: Vec<Entry>,
    nodes: HashSet<Hash>,
}

impl Cache {
    /// Records `e` the first time a foreground read needs it, whatever served it.
    pub(super) fn touched(&self, e: &Entry) {
        if e.loc.is_pending() {
            return;
        }
        let mut r = self.recorder.lock().expect("recorder lock");
        if r.seen.insert(e.hash) {
            r.order.push(*e);
        }
    }

    /// Marks a recorded object as a tree node.
    pub(crate) fn touched_node(&self, h: &Hash) {
        self.recorder.lock().expect("recorder lock").nodes.insert(*h);
    }

    /// The objects foreground reads needed, in first-touch order, and which of them are nodes.
    pub(crate) fn recorded(&self) -> (Vec<Entry>, HashSet<Hash>) {
        let r = self.recorder.lock().expect("recorder lock");
        (r.order.clone(), r.nodes.clone())
    }

    /// Takes segment indexes from a recording, so fetches need not get them first.
    pub(crate) fn seed_indexes(&self, indexes: &[(SegId, SegmentIndex)]) {
        let mut m = self.indexes.lock().expect("indexes lock");
        for (id, idx) in indexes {
            m.entry(*id).or_insert_with(|| std::sync::Arc::new(idx.clone()));
        }
    }

    /// The indexes this cache holds of the segments of `es`.
    pub(crate) fn known_indexes(&self, es: &[Entry]) -> Vec<(SegId, SegmentIndex)> {
        let m = self.indexes.lock().expect("indexes lock");
        let mut seen = HashSet::new();
        es.iter()
            .filter(|e| !e.loc.is_pending() && seen.insert(e.loc.seg))
            .filter_map(|e| Some((e.loc.seg, (**m.get(&e.loc.seg)?).clone())))
            .collect()
    }
}
