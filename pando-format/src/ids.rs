use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::{FormatError, Hash};

/// Names an uploaded segment: 8 bytes of the branch name's hash, the writer generation
/// (big-endian), then a sequence number unique to the writer.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct SegId(pub [u8; 16]);

/// The branch and generation of the writer that uploaded a segment. GC uses it to tell a
/// fenced writer's leftovers from an upload in flight.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct WriterId {
    /// The first 8 bytes of the SHA-256 of the branch name.
    pub branch: [u8; 8],
    /// The low 32 bits of the writer's generation.
    pub generation: u32,
}

impl SegId {
    /// No segment: an [`Entry`](crate::Entry) location in it is not uploaded yet.
    pub const ZERO: SegId = SegId([0; 16]);

    /// The id of segment `seq` of `writer`.
    pub fn new(writer: WriterId, seq: u32) -> SegId {
        let mut s = [0; 16];
        s[..8].copy_from_slice(&writer.branch);
        s[8..12].copy_from_slice(&writer.generation.to_be_bytes());
        s[12..].copy_from_slice(&seq.to_be_bytes());
        SegId(s)
    }

    /// Whether this is [`SegId::ZERO`].
    pub fn is_zero(&self) -> bool {
        *self == Self::ZERO
    }

    /// The writer the id names, as [`SegId::new`] put it in.
    pub fn writer(&self) -> WriterId {
        let mut branch = [0; 8];
        branch.copy_from_slice(&self.0[..8]);
        WriterId {
            branch,
            generation: u32::from_be_bytes([self.0[8], self.0[9], self.0[10], self.0[11]]),
        }
    }
}

impl WriterId {
    /// The writer of `branch` at `generation`; segment ids keep the low 32 bits of it.
    pub fn new(branch: &str, generation: u64) -> WriterId {
        let mut b = [0; 8];
        b.copy_from_slice(&Hash::of(branch.as_bytes()).0[..8]);
        let [g0, g1, g2, g3, ..] = generation.to_le_bytes();
        WriterId {
            branch: b,
            generation: u32::from_le_bytes([g0, g1, g2, g3]),
        }
    }
}

/// One writer of one branch. Its segment ids are numbered from a random start, so two writers
/// never share one, and a segment PUT never replaces another writer's segment.
#[derive(Debug)]
pub struct Writer {
    id: WriterId,
    seq: AtomicU32,
}

impl Writer {
    /// The writer `id`, whose first segment is numbered `start + 1` (wrapping).
    pub fn new(id: WriterId, start: u32) -> Writer {
        Writer {
            id,
            seq: AtomicU32::new(start),
        }
    }

    /// The writer's id, the same in every segment id it hands out.
    pub fn id(&self) -> WriterId {
        self.id
    }

    /// A segment id this writer has not handed out before, until its 2^32 numbers wrap.
    pub fn next_seg_id(&self) -> SegId {
        SegId::new(self.id, self.seq.fetch_add(1, Ordering::Relaxed).wrapping_add(1))
    }
}

/// One life of a branch, drawn at random when its ref is created, so a branch made again under
/// a deleted name has another. Its ref, the keys of its write log and a host's state of it all
/// carry it: nothing of an earlier life under the name is read, resumed or replaced as this
/// one's, and the ref's bytes differ from any earlier life's, so a write conditional on an
/// earlier life's version fails.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct BranchId(pub [u8; 16]);

impl BranchId {
    /// The id of a ref that has none, as the Go proof of concept writes them. A ref this build
    /// creates never has it.
    pub const NONE: BranchId = BranchId([0; 16]);

    /// The id whose bytes are `hi` then `lo`, big-endian.
    pub fn from_parts(hi: u64, lo: u64) -> BranchId {
        let mut b = [0; 16];
        b[..8].copy_from_slice(&hi.to_be_bytes());
        b[8..].copy_from_slice(&lo.to_be_bytes());
        BranchId(b)
    }
}

impl fmt::Display for BranchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for BranchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BranchId({self})")
    }
}

impl FromStr for BranchId {
    type Err = FormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut id = [0; 16];
        hex::decode_to_slice(s, &mut id).map_err(|e| FormatError::malformed("branch id", e.to_string()))?;
        Ok(BranchId(id))
    }
}

impl fmt::Display for SegId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for SegId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SegId({self})")
    }
}

impl FromStr for SegId {
    type Err = FormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut id = [0; 16];
        hex::decode_to_slice(s, &mut id).map_err(|e| FormatError::malformed("segment id", e.to_string()))?;
        Ok(SegId(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_ids_parse_back() {
        let id = BranchId::from_parts(1, u64::MAX);
        assert_eq!(id.to_string(), "0000000000000001ffffffffffffffff");
        assert_eq!(id.to_string().parse::<BranchId>().unwrap(), id);
        assert!("0001".parse::<BranchId>().is_err());
    }

    #[test]
    fn segment_ids_carry_their_writer_and_count_up() {
        let w = Writer::new(WriterId::new("main", 5), u32::MAX);
        let a = w.next_seg_id();
        let b = w.next_seg_id();
        assert_eq!(a.writer(), w.id());
        assert_eq!(&a.0[12..], &0u32.to_be_bytes());
        assert_eq!(&b.0[12..], &1u32.to_be_bytes());
    }
}
