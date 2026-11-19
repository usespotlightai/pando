//! The local state of a branch, which a volume writes after each commit.

use bytes::Bytes;
use pando_format::Entry;

/// The local state of a branch. `size` is the size `root` belongs to, and `generation` the
/// writer that wrote it; states written before sizes could grow, or before the write log, have
/// none.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VolState {
    pub(crate) root: Entry,
    pub(crate) published: Entry,
    pub(crate) publishing: Entry,
    pub(crate) jfloor: u64,
    pub(crate) size: Option<u64>,
    pub(crate) generation: Option<u64>,
}

impl VolState {
    pub(crate) fn encode(&self) -> Bytes {
        let tail = match (self.size, self.generation) {
            (Some(n), Some(g)) => format!("{n}\n{g}\n"),
            (Some(n), None) => format!("{n}\n"),
            (None, _) => String::new(),
        };
        Bytes::from(format!(
            "{}\n{}\n{}\n{}\n{tail}",
            self.root, self.published, self.publishing, self.jfloor
        ))
    }

    pub(crate) fn decode(b: &[u8]) -> Option<VolState> {
        let s = std::str::from_utf8(b).ok()?;
        let lines: Vec<&str> = s.lines().collect();
        let (root, published, publishing, jfloor, size, generation) = match lines.as_slice() {
            [r, p, q, j] => (r, p, q, j, None, None),
            [r, p, q, j, n] => (r, p, q, j, Some(n.parse().ok()?), None),
            [r, p, q, j, n, g] => (r, p, q, j, Some(n.parse().ok()?), Some(g.parse().ok()?)),
            _ => return None,
        };
        Some(VolState {
            root: Entry::parse_text(root).ok()?,
            published: Entry::parse_text(published).ok()?,
            publishing: Entry::parse_text(publishing).ok()?,
            jfloor: jfloor.parse().ok()?,
            size,
            generation,
        })
    }
}
