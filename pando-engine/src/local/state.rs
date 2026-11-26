//! The local state of a branch, which a volume writes after each commit.

use bytes::Bytes;
use pando_format::{BranchId, Entry};

/// The local state of a branch. `size` is the size `root` belongs to, `generation` the writer
/// that wrote it, `id` the life of the branch it wrote, `since` the first writer of the run of
/// writers on this host that this state carries on from, and `taking` the generation and token
/// of the take its writer started, which may have put the ref; states written before sizes
/// could grow, before the write log, before branch ids, before runs, or before takes were
/// noted, have none.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VolState {
    pub(crate) root: Entry,
    pub(crate) published: Entry,
    pub(crate) publishing: Entry,
    pub(crate) jfloor: u64,
    pub(crate) size: Option<u64>,
    pub(crate) generation: Option<u64>,
    pub(crate) id: Option<BranchId>,
    pub(crate) since: Option<u64>,
    pub(crate) taking: Option<(u64, u64)>,
}

impl VolState {
    pub(crate) fn encode(&self) -> Bytes {
        let tail = match (self.size, self.generation, self.id, self.since, self.taking) {
            (Some(n), Some(g), Some(i), Some(f), Some((tg, t))) => format!("{n}\n{g}\n{i}\n{f}\n{tg} {t}\n"),
            (Some(n), Some(g), Some(i), Some(f), None) => format!("{n}\n{g}\n{i}\n{f}\n"),
            (Some(n), Some(g), Some(i), None, _) => format!("{n}\n{g}\n{i}\n"),
            (Some(n), Some(g), None, _, _) => format!("{n}\n{g}\n"),
            (Some(n), None, _, _, _) => format!("{n}\n"),
            (None, _, _, _, _) => String::new(),
        };
        Bytes::from(format!(
            "{}\n{}\n{}\n{}\n{tail}",
            self.root, self.published, self.publishing, self.jfloor
        ))
    }

    pub(crate) fn decode(b: &[u8]) -> Option<VolState> {
        let s = std::str::from_utf8(b).ok()?;
        let lines: Vec<&str> = s.lines().collect();
        let (head, tail) = lines.split_at_checked(4)?;
        let [root, published, publishing, jfloor] = head else {
            return None;
        };
        let (size, generation, id, since) = match tail {
            [] => (None, None, None, None),
            [n] => (Some(n.parse().ok()?), None, None, None),
            [n, g] => (Some(n.parse().ok()?), Some(g.parse().ok()?), None, None),
            [n, g, i] => (
                Some(n.parse().ok()?),
                Some(g.parse().ok()?),
                Some(i.parse().ok()?),
                None,
            ),
            [n, g, i, f, ..] => (
                Some(n.parse().ok()?),
                Some(g.parse().ok()?),
                Some(i.parse().ok()?),
                Some(f.parse().ok()?),
            ),
        };
        let taking = match tail.get(4..).unwrap_or_default() {
            [] => None,
            [t] => {
                let (g, t) = t.split_once(' ')?;
                Some((g.parse().ok()?, t.parse().ok()?))
            }
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
            since,
            taking,
        })
    }
}
