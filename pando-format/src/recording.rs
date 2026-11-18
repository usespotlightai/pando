use crate::{ENTRY_SIZE, Entry, FormatError, SegId, SegmentIndex};

const RECORDING_MAGIC: &[u8; 4] = b"rec1";

/// The chunks a root's earlier mounts read first, in that order, and the indexes of the
/// segments that hold them. With the indexes, one GET of the recording is enough to fetch those
/// chunks: no segment index has to be fetched first. Segments never change, so a recorded
/// index never goes stale. `ages[i]` counts the mounts since one last read `entries[i]`;
/// `nodes[i]` marks a tree node, whose frame a prefetch decodes whole for the other nodes in it.
///
/// Encoding: magic, count u32 LE, the entries, one age byte and one node byte per entry, then
/// `[segment id][index length u32][index]` per segment to the end.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Recording {
    /// The recorded objects, in first-read order.
    pub entries: Vec<Entry>,
    /// Per entry, the mounts since one last read it; a missing age encodes as 0.
    pub ages: Vec<u8>,
    /// Per entry, whether it is a tree node; a missing flag encodes as false.
    pub nodes: Vec<bool>,
    /// The indexes of the segments that hold the entries.
    pub indexes: Vec<(SegId, SegmentIndex)>,
}

impl Recording {
    /// The encoding described on [`Recording`]. `ages` and `nodes` are cut or padded to the
    /// length of `entries`.
    pub fn encode(&self) -> Vec<u8> {
        let n = self.entries.len();
        let mut b = Vec::with_capacity(8 + n * (ENTRY_SIZE + 2));
        b.extend_from_slice(RECORDING_MAGIC);
        #[expect(
            clippy::cast_possible_truncation,
            reason = "writers cap a recording at tens of thousands of entries, far below u32::MAX"
        )]
        let count = n as u32;
        b.extend_from_slice(&count.to_le_bytes());
        for e in &self.entries {
            b.extend_from_slice(&e.to_bytes());
        }
        b.extend((0..n).map(|i| self.ages.get(i).copied().unwrap_or(0)));
        b.extend((0..n).map(|i| u8::from(self.nodes.get(i).copied().unwrap_or(false))));
        for (id, idx) in &self.indexes {
            let enc = idx.encode();
            #[expect(
                clippy::cast_possible_truncation,
                reason = "a segment index is 8 bytes per frame of a segment of at most 4 GiB, far below \
                          u32::MAX bytes"
            )]
            let len = enc.len() as u32;
            b.extend_from_slice(&id.0);
            b.extend_from_slice(&len.to_le_bytes());
            b.extend_from_slice(&enc);
        }
        b
    }

    /// Decodes a recording; a node byte other than 1 decodes as false.
    pub fn decode(b: &[u8]) -> Result<Recording, FormatError> {
        let bad = |why: &str| FormatError::malformed("recording", why.to_string());
        let Some((&[m0, m1, m2, m3, n0, n1, n2, n3], mut rest)) = b.split_first_chunk::<8>() else {
            return Err(bad("bad magic"));
        };
        if [m0, m1, m2, m3] != *RECORDING_MAGIC {
            return Err(bad("bad magic"));
        }
        let n = u32::from_le_bytes([n0, n1, n2, n3]) as usize;
        if rest.len() < n * (ENTRY_SIZE + 2) {
            return Err(bad("short entries"));
        }
        let (es, r) = rest.split_at(n * ENTRY_SIZE);
        let (entries, _) = es.as_chunks::<ENTRY_SIZE>();
        let entries = entries.iter().map(Entry::from_bytes).collect();
        let (ages, r) = r.split_at(n);
        let (nodes, r) = r.split_at(n);
        rest = r;
        let mut indexes = Vec::new();
        while !rest.is_empty() {
            let Some((&[ref id @ .., l0, l1, l2, l3], r)) = rest.split_first_chunk::<20>() else {
                return Err(bad("short segment record"));
            };
            let len = u32::from_le_bytes([l0, l1, l2, l3]) as usize;
            let (enc, r) = r
                .split_at_checked(len)
                .ok_or_else(|| bad("short segment index"))?;
            indexes.push((SegId(*id), SegmentIndex::decode(enc)?));
            rest = r;
        }
        Ok(Recording {
            entries,
            ages: ages.to_vec(),
            nodes: nodes.iter().map(|f| *f == 1).collect(),
            indexes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Hash, Loc};

    #[test]
    fn a_recording_round_trips() {
        let e = |s: &[u8]| Entry {
            hash: Hash::of(s),
            loc: Loc {
                seg: SegId([3; 16]),
                frame: 1,
                inner: 9,
                len: 4,
            },
        };
        let r = Recording {
            entries: vec![e(b"a"), e(b"b")],
            ages: vec![0, 2],
            nodes: vec![true, false],
            indexes: vec![(
                SegId([3; 16]),
                SegmentIndex {
                    frames: vec![(0, 100), (100, 50)],
                    deps: vec![SegId([1; 16])],
                },
            )],
        };
        assert_eq!(Recording::decode(&r.encode()).unwrap(), r);
        assert!(Recording::decode(b"rec0\0\0\0\0").is_err());
    }
}
