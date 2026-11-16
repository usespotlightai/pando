use crate::{BLOCKS_PER_CHUNK, BlockKey, BlockKeys, ENTRY_SIZE, Entry, FormatError};

pub const CHANGE_LIST_MAGIC: &[u8; 4] = b"pcl2";

/// A tree node by its level (0 = leaf level) and the chunk index bits above it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct NodeAt {
    pub level: u8,
    pub prefix: u64,
}

/// A chunk the new root changed, with the keys of its blocks when the writer had them.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Change {
    pub idx: u64,
    pub entry: Entry,
    pub keys: Option<BlockKeys>,
}

/// The chunks a root changed from the root its writer opened at, with the entries of the
/// tree nodes on their paths. A host that has the earlier root's nodes rebuilds the new
/// root's nodes from it and checks that they hash to the new root.
///
/// Encoding: magic, depth u8, node count u32 LE, `[level u8][prefix u64][entry]` per node, then
/// `[index u64][entry][has keys u8][key u64 × 16 if it has]` per change to the end.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct ChangeList {
    pub depth: u8,
    pub nodes: Vec<(NodeAt, Entry)>,
    pub changes: Vec<Change>,
}

const NODE_REC: usize = 1 + 8 + ENTRY_SIZE;
const CHANGE_REC: usize = 8 + ENTRY_SIZE + 1;

impl ChangeList {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(9 + self.nodes.len() * NODE_REC + self.changes.len() * CHANGE_REC);
        b.extend_from_slice(CHANGE_LIST_MAGIC);
        b.push(self.depth);
        b.extend_from_slice(&(self.nodes.len() as u32).to_le_bytes());
        for (at, e) in &self.nodes {
            b.push(at.level);
            b.extend_from_slice(&at.prefix.to_le_bytes());
            b.extend_from_slice(&e.to_bytes());
        }
        for c in &self.changes {
            b.extend_from_slice(&c.idx.to_le_bytes());
            b.extend_from_slice(&c.entry.to_bytes());
            match &c.keys {
                None => b.push(0),
                Some(k) => {
                    b.push(1);
                    for key in &k.0 {
                        b.extend_from_slice(&key.0.to_le_bytes());
                    }
                }
            }
        }
        b
    }

    /// Decodes a change list; changes come out sorted by chunk index.
    pub fn decode(b: &[u8]) -> Result<ChangeList, FormatError> {
        let bad = || FormatError::malformed("change list", "bad length");
        if b.len() < 9 || &b[..4] != CHANGE_LIST_MAGIC {
            return Err(FormatError::OtherFormat { what: "change list" });
        }
        let u64_at = |at: usize| u64::from_le_bytes(b[at..at + 8].try_into().expect("8 bytes"));
        let count = u32::from_le_bytes(b[5..9].try_into().expect("4 bytes")) as usize;
        let end = count
            .checked_mul(NODE_REC)
            .map(|n| n + 9)
            .filter(|&n| n <= b.len())
            .ok_or_else(bad)?;
        let nodes = b[9..end]
            .as_chunks::<NODE_REC>()
            .0
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let at = 9 + i * NODE_REC;
                (
                    NodeAt {
                        level: r[0],
                        prefix: u64_at(at + 1),
                    },
                    Entry::read(b, at + 9).expect("in range"),
                )
            })
            .collect();
        let mut changes = Vec::new();
        let mut off = end;
        while off < b.len() {
            if b.len() - off < CHANGE_REC {
                return Err(bad());
            }
            let idx = u64_at(off);
            let entry = Entry::read(b, off + 8).expect("in range");
            off += CHANGE_REC;
            let keys = match b[off - 1] {
                0 => None,
                1 => {
                    let raw = b.get(off..off + 8 * BLOCKS_PER_CHUNK).ok_or_else(bad)?;
                    off += 8 * BLOCKS_PER_CHUNK;
                    let mut k = [BlockKey::default(); BLOCKS_PER_CHUNK];
                    for (key, v) in k.iter_mut().zip(raw.as_chunks::<8>().0) {
                        *key = BlockKey(u64::from_le_bytes(*v));
                    }
                    Some(BlockKeys(k))
                }
                _ => return Err(FormatError::malformed("change list", "bad key flag")),
            };
            changes.push(Change { idx, entry, keys });
        }
        changes.sort_by_key(|c| c.idx);
        Ok(ChangeList {
            depth: b[4],
            nodes,
            changes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Hash;

    #[test]
    fn change_list_round_trips_and_rejects_cuts() {
        let e = Entry {
            hash: Hash::of(b"n"),
            ..Entry::EMPTY
        };
        let cl = ChangeList {
            depth: 3,
            nodes: vec![(NodeAt { level: 2, prefix: 0 }, e)],
            changes: vec![
                Change {
                    idx: 5,
                    entry: e,
                    keys: Some(BlockKeys([BlockKey(9); BLOCKS_PER_CHUNK])),
                },
                Change {
                    idx: 9,
                    entry: Entry::EMPTY,
                    keys: None,
                },
            ],
        };
        let b = cl.encode();
        assert_eq!(ChangeList::decode(&b).unwrap(), cl);
        assert!(ChangeList::decode(&b[..b.len() - 1]).is_err());
        assert!(matches!(
            ChangeList::decode(b"pcl1xxxxxx"),
            Err(FormatError::OtherFormat { .. })
        ));
    }
}
