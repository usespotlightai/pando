use crate::{ENTRY_SIZE, Entry, FormatError};

/// The tree's fanout: each level takes 8 bits of the chunk index.
pub const FANOUT: usize = 256;

/// A node of the block map, a persistent radix tree from chunk index to chunk entry. Nodes
/// are named by the hash of their encoding, so a branch is one root entry.
///
/// Encoding: child count (u16 LE), then `[slot u8][entry]` for each non-empty child in slot order.
#[derive(Clone, PartialEq, Eq)]
pub struct Node {
    children: Box<[Entry; FANOUT]>,
}

impl Node {
    pub fn new() -> Node {
        Node {
            children: Box::new([Entry::EMPTY; FANOUT]),
        }
    }

    pub fn child(&self, slot: u8) -> &Entry {
        &self.children[usize::from(slot)]
    }

    pub fn set(&mut self, slot: u8, e: Entry) {
        self.children[usize::from(slot)] = e;
    }

    pub fn is_empty(&self) -> bool {
        self.children.iter().all(Entry::is_empty)
    }

    /// The non-empty children in slot order.
    pub fn children(&self) -> impl Iterator<Item = (u8, &Entry)> {
        (0..=u8::MAX)
            .map(|s| (s, self.child(s)))
            .filter(|(_, e)| !e.is_empty())
    }

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
        for rec in b[2..].as_chunks::<{ 1 + ENTRY_SIZE }>().0 {
            let slot = rec[0];
            if last.is_some_and(|l| l >= slot) {
                return Err(bad("slots out of order"));
            }
            last = Some(slot);
            n.set(slot, Entry::read(rec, 1).expect("record holds an entry"));
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
    let mut cap = FANOUT as u64;
    while cap < chunks {
        d += 1;
        cap = cap.saturating_mul(FANOUT as u64);
    }
    d
}

/// The slot of chunk `idx` in a node at `level` (0 = leaf level).
pub fn digit(idx: u64, level: usize) -> u8 {
    (idx >> (8 * level)) as u8
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
    fn digit_takes_eight_bits_per_level() {
        assert_eq!(digit(0x01_02_03, 0), 3);
        assert_eq!(digit(0x01_02_03, 2), 1);
    }
}
