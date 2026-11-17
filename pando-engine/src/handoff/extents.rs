//! Which chunks of an image changed between two clones of it, from their extents alone.

use std::time::Duration;

use pando_format::CHUNK_SIZE;

use crate::ports::Extent;

const CHUNK: u64 = CHUNK_SIZE as u64;

/// The least time between the end of one snapshot and the start of the next.
pub(crate) const SNAPSHOT_GAP: Duration = Duration::from_millis(500);

/// Whether to take a snapshot `since` after the last one ended, with `written` bytes (when
/// known) reaching the image since: every `every`, or early once `burst` bytes came. The
/// second value says whether it is early.
pub(crate) fn snapshot_due(
    since: Duration,
    written: Option<u64>,
    every: Duration,
    burst: u64,
) -> (bool, bool) {
    if burst > 0 && written.is_some_and(|w| w >= burst) && since >= SNAPSHOT_GAP {
        return (true, since < every);
    }
    (since >= every, false)
}

fn sorted(es: &[Extent]) -> Vec<Extent> {
    let mut v = es.to_vec();
    v.sort_by_key(|e| e.logical);
    v
}

/// The chunks, in order, whose bytes may differ between two clones of one image, and how many
/// bytes differ: a byte differs when only one clone maps it, or they map it to different places
/// or in different states.
/// Clones share every block nobody wrote since; a write moves a block to a new place. The walk
/// is linear in the extents, whatever the size of the image.
pub(crate) fn changed_chunks(prev: &[Extent], cur: &[Extent]) -> (Vec<u64>, u64) {
    let (prev, cur) = (sorted(prev), sorted(cur));
    let mut out: Vec<u64> = Vec::new();
    let mut bytes = 0;
    let mut mark = |from: u64, to: u64| {
        bytes += to - from;
        let (mut first, last) = (from / CHUNK, (to - 1) / CHUNK);
        if let Some(&l) = out.last()
            && l >= first
        {
            first = l + 1;
        }
        out.extend(first..=last);
    };
    let at = |es: &[Extent], i: &mut usize, pos: u64| -> (Option<Extent>, u64) {
        while *i < es.len() && es[*i].logical + es[*i].len <= pos {
            *i += 1;
        }
        match es.get(*i) {
            None => (None, u64::MAX),
            Some(x) if x.logical <= pos => (Some(*x), x.logical + x.len),
            Some(x) => (None, x.logical),
        }
    };
    let (mut i, mut j, mut pos) = (0, 0, 0);
    while pos < u64::MAX {
        let (a, na) = at(&prev, &mut i, pos);
        let (b, nb) = at(&cur, &mut j, pos);
        let next = na.min(nb);
        if a.is_none() && b.is_none() && next == u64::MAX {
            break;
        }
        let differs = match (a, b) {
            (Some(a), Some(b)) => {
                a.physical + pos - a.logical != b.physical + pos - b.logical || a.unwritten != b.unwritten
            }
            (None, None) => false,
            _ => true,
        };
        if differs {
            mark(pos, next);
        }
        pos = next;
    }
    (out, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn x(logical: u64, physical: u64, len: u64) -> Extent {
        Extent {
            logical,
            physical,
            len,
            unwritten: false,
        }
    }

    #[test]
    fn an_extent_written_in_place_is_a_change() {
        let unwritten = |e: Extent| Extent { unwritten: true, ..e };
        let prev = [unwritten(x(0, 1 << 30, 2 * CHUNK))];
        let cur = [
            x(0, 1 << 30, CHUNK),
            unwritten(x(CHUNK, (1 << 30) + CHUNK, CHUNK)),
        ];
        assert_eq!(changed_chunks(&prev, &cur), (vec![0], CHUNK));
    }

    #[test]
    fn moved_and_new_extents_mark_their_chunks_once() {
        let prev = [x(0, 1 << 30, 4 * CHUNK)];
        let cur = [
            x(0, 1 << 30, CHUNK),
            x(CHUNK, 7 << 30, 4096),
            x(CHUNK + 4096, (1 << 30) + CHUNK + 4096, 3 * CHUNK - 4096),
            x(9 * CHUNK, 3 << 30, 10),
        ];
        let (chunks, bytes) = changed_chunks(&prev, &cur);
        assert_eq!(chunks, vec![1, 9]);
        assert_eq!(bytes, 4096 + 10);
        assert_eq!(changed_chunks(&cur, &cur), (vec![], 0));
        assert_eq!(changed_chunks(&prev, &[]).0, vec![0, 1, 2, 3]);
    }

    #[test]
    fn snapshots_come_on_time_or_after_a_burst() {
        let every = Duration::from_secs(30);
        assert_eq!(
            snapshot_due(Duration::from_secs(31), None, every, 0),
            (true, false)
        );
        assert_eq!(
            snapshot_due(Duration::from_secs(1), Some(100), every, 64),
            (true, true)
        );
        assert_eq!(
            snapshot_due(Duration::from_millis(100), Some(100), every, 64),
            (false, false)
        );
        assert_eq!(
            snapshot_due(Duration::from_secs(1), Some(10), every, 64),
            (false, false)
        );
    }
}
