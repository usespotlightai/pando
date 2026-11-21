//! The local state of a branch, which a volume writes after each commit.

use bytes::Bytes;
use pando_format::{BranchId, Entry};

/// The local state of a branch. `size` is the size `root` belongs to, `generation` the writer
/// that wrote it and `id` the life of the branch it wrote; states written before sizes could
/// grow, before the write log, or before branch ids, have none.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VolState {
    pub(crate) root: Entry,
    pub(crate) published: Entry,
    pub(crate) publishing: Entry,
    pub(crate) jfloor: u64,
    pub(crate) size: Option<u64>,
    pub(crate) generation: Option<u64>,
    pub(crate) id: Option<BranchId>,
}

impl VolState {
    pub(crate) fn encode(&self) -> Bytes {
        let tail = match (self.size, self.generation, self.id) {
            (Some(n), Some(g), Some(i)) => format!("{n}\n{g}\n{i}\n"),
            (Some(n), Some(g), None) => format!("{n}\n{g}\n"),
            (Some(n), None, _) => format!("{n}\n"),
            (None, _, _) => String::new(),
        };
        Bytes::from(format!(
            "{}\n{}\n{}\n{}\n{tail}",
            self.root, self.published, self.publishing, self.jfloor
        ))
    }

    pub(crate) fn decode(b: &[u8]) -> Option<VolState> {
        let s = std::str::from_utf8(b).ok()?;
        let lines: Vec<&str> = s.lines().collect();
        let (root, published, publishing, jfloor, size, generation, id) = match lines.as_slice() {
            [r, p, q, j] => (r, p, q, j, None, None, None),
            [r, p, q, j, n] => (r, p, q, j, Some(n.parse().ok()?), None, None),
            [r, p, q, j, n, g] => (r, p, q, j, Some(n.parse().ok()?), Some(g.parse().ok()?), None),
            [r, p, q, j, n, g, i] => (
                r,
                p,
                q,
                j,
                Some(n.parse().ok()?),
                Some(g.parse().ok()?),
                Some(i.parse().ok()?),
            ),
            _ => return None,
        };
        Some(VolState {
            root: Entry::parse_text(root).ok()?,
            published: Entry::parse_text(published).ok()?,
            publishing: Entry::parse_text(publishing).ok()?,
            jfloor: jfloor.parse().ok()?,
            size,
            generation,
            id,
        })
    }
}
