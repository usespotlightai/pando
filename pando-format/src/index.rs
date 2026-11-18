use crate::{FormatError, SegId};

/// A segment's index, a small object next to it: the offset and length of each compressed
/// frame, then the segments whose objects its recipes read.
///
/// Encoding: frame count (u32 LE), `[offset u32][length u32]` per frame, then 16-byte segment ids.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct SegmentIndex {
    /// The offset and compressed length in the segment of each frame, by frame number.
    pub frames: Vec<(u32, u32)>,
    /// The segments that hold the sources of this segment's recipes.
    pub deps: Vec<SegId>,
}

impl SegmentIndex {
    /// The encoding described on [`SegmentIndex`].
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(4 + 8 * self.frames.len() + 16 * self.deps.len());
        #[expect(
            clippy::cast_possible_truncation,
            reason = "frame offsets are u32 and each frame takes bytes, so a segment has fewer than u32::MAX frames"
        )]
        let count = self.frames.len() as u32;
        b.extend_from_slice(&count.to_le_bytes());
        for (off, len) in &self.frames {
            b.extend_from_slice(&off.to_le_bytes());
            b.extend_from_slice(&len.to_le_bytes());
        }
        for d in &self.deps {
            b.extend_from_slice(&d.0);
        }
        b
    }

    /// Decodes an index; it fails when the frame table is cut or a segment id is partial.
    pub fn decode(b: &[u8]) -> Result<SegmentIndex, FormatError> {
        let bad = |why: &str| FormatError::malformed("segment index", why.to_string());
        let count = u32::from_le_bytes(*b.first_chunk().ok_or_else(|| bad("short"))?) as usize;
        let deps_at = count
            .checked_mul(8)
            .and_then(|n| n.checked_add(4))
            .filter(|&n| n <= b.len())
            .ok_or_else(|| bad("short frame table"))?;
        if !(b.len() - deps_at).is_multiple_of(16) {
            return Err(bad("partial segment id"));
        }
        let frames = b[4..deps_at]
            .as_chunks::<8>()
            .0
            .iter()
            .map(|&[o0, o1, o2, o3, l0, l1, l2, l3]| {
                (
                    u32::from_le_bytes([o0, o1, o2, o3]),
                    u32::from_le_bytes([l0, l1, l2, l3]),
                )
            })
            .collect();
        let deps = b[deps_at..]
            .as_chunks::<16>()
            .0
            .iter()
            .map(|s| SegId(*s))
            .collect();
        Ok(SegmentIndex { frames, deps })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_round_trips_and_rejects_a_cut_table() {
        let i = SegmentIndex {
            frames: vec![(0, 10), (10, 20)],
            deps: vec![SegId([7; 16])],
        };
        let b = i.encode();
        assert_eq!(SegmentIndex::decode(&b).unwrap(), i);
        assert!(SegmentIndex::decode(&b[..8]).is_err());
        assert!(SegmentIndex::decode(&b[..b.len() - 1]).is_err());
    }
}
