use crate::{ENTRY_SIZE, Entry, FormatError, u64_from};

/// The tree's fanout: each level takes 8 bits of the chunk index.
pub const FANOUT: usize = 256;

/// A node of the block map, a persistent radix tree from chunk index to chunk entry. Nodes
/// are named by the hash of their encoding, so a branch is one root entry.
///
/// Encoding: child count (u16 LE), then `[slot u8][entry]` for each non-empty child in slot order.
/// Each entry holds its child's location, so nodes with the same hash have the same children in
/// the same places; GC's mark relies on this to walk a node's subtree once.
#[derive(Clone, PartialEq, Eq)]
pub struct Node {
    children: Box<[Entry; FANOUT]>,
}

impl Node {
    /// A node with every child empty.
    pub fn new() -> Node {
        Node {
            children: Box::new([Entry::EMPTY; FANOUT]),
        }
    }

    /// The child in `slot`; [`Entry::EMPTY`] when there is none.
    pub fn child(&self, slot: u8) -> &Entry {
        &self.children[usize::from(slot)]
    }

    /// Sets the child in `slot`; an empty `e` removes it.
    pub fn set(&mut self, slot: u8, e: Entry) {
        self.children[usize::from(slot)] = e;
    }

    /// Whether every child is empty.
    pub fn is_empty(&self) -> bool {
        self.children.iter().all(Entry::is_empty)
    }

    /// The non-empty children in slot order.
    pub fn children(&self) -> impl Iterator<Item = (u8, &Entry)> {
        (0..=u8::MAX)
            .map(|s| (s, self.child(s)))
            .filter(|(_, e)| !e.is_empty())
    }

    /// The encoding described on [`Node`]. Equal nodes encode, and so hash, the same.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(2 + FANOUT * (1 + ENTRY_SIZE));
        b.extend_from_slice(&[0, 0]);
        let mut count: u16 = 0;
        for (slot, e) in self.children() {
            b.push(slot);
            b.extend_from_slice(&e.to_bytes());
            count += 1;
        }
        b[..2].copy_from_slice(&count.to_le_bytes());
        b
    }

    /// Decodes a node; its slots must ascend strictly, no child may be empty, and its length must
    /// match its child count, so the only bytes that decode to a node are its encoding.
    pub fn decode(b: &[u8]) -> Result<Node, FormatError> {
        let bad = |why: &str| FormatError::malformed("node", why.to_string());
        let count = usize::from(u16::from_le_bytes([
            *b.first().ok_or_else(|| bad("short"))?,
            *b.get(1).ok_or_else(|| bad("short"))?,
        ]));
        if b.len() != 2 + count * (1 + ENTRY_SIZE) {
            return Err(bad("length does not match the child count"));
        }
        let mut n = Node::new();
        let mut last: Option<u8> = None;
        for [slot, entry @ ..] in b[2..].as_chunks::<{ 1 + ENTRY_SIZE }>().0 {
            let slot = *slot;
            if last.is_some_and(|l| l >= slot) {
                return Err(bad("slots out of order"));
            }
            last = Some(slot);
            let e = Entry::from_bytes(entry);
            if e.is_empty() {
                return Err(bad("empty child"));
            }
            n.set(slot, e);
        }
        Ok(n)
    }
}

impl Default for Node {
    fn default() -> Self {
        Node::new()
    }
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.children()).finish()
    }
}

/// The tree depth that addresses `chunks` chunks.
pub fn depth_for(chunks: u64) -> usize {
    let mut d = 1;
    let mut cap = u64_from(FANOUT);
    while cap < chunks {
        d += 1;
        cap = cap.saturating_mul(u64_from(FANOUT));
    }
    d
}

/// The slot of chunk `idx` in a node at `level` (0 = leaf level); 0 above the top byte.
pub fn digit(idx: u64, level: usize) -> u8 {
    idx.to_le_bytes().get(level).copied().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Hash;

    #[test]
    fn node_round_trips_and_rejects_bad_lengths() {
        let mut n = Node::new();
        n.set(
            3,
            Entry {
                hash: Hash::of(b"a"),
                ..Entry::EMPTY
            },
        );
        n.set(
            200,
            Entry {
                hash: Hash::of(b"b"),
                ..Entry::EMPTY
            },
        );
        let b = n.encode();
        assert_eq!(Node::decode(&b).unwrap(), n);
        assert!(Node::decode(&b[..b.len() - 1]).is_err());
        assert!(Node::decode(&[]).is_err());
    }

    #[test]
    fn the_location_of_a_child_is_part_of_the_encoding() {
        let at = |frame| {
            let mut n = Node::new();
            n.set(
                7,
                Entry {
                    hash: Hash::of(b"a"),
                    loc: crate::Loc {
                        frame,
                        ..Entry::EMPTY.loc
                    },
                },
            );
            n.encode()
        };
        assert_ne!(at(1), at(2));
    }

    #[test]
    fn digit_takes_eight_bits_per_level() {
        assert_eq!(digit(0x01_02_03, 0), 3);
        assert_eq!(digit(0x01_02_03, 2), 1);
        assert_eq!(digit(u64::MAX, 8), 0);
    }
}
