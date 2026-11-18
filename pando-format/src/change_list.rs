use crate::{BLOCKS_PER_CHUNK, BlockKey, BlockKeys, ENTRY_SIZE, Entry, FormatError};

const CHANGE_LIST_MAGIC: &[u8; 4] = b"pcl2";

/// A tree node by its level (0 = leaf level) and the chunk index bits above it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct NodeAt {
    /// The node's level; 0 is the leaf level.
    pub level: u8,
    /// The index of any chunk under the node, shifted right by `8 × (level + 1)` bits.
    pub prefix: u64,
}

/// A chunk the new root changed, with the keys of its blocks when the writer had them.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Change {
    /// The chunk index.
    pub idx: u64,
    /// The chunk's entry under the new root; empty when the chunk was removed.
    pub entry: Entry,
    /// The keys of the chunk's blocks, when the writer had them.
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
    /// The depth of the new root's tree.
    pub depth: u8,
    /// The new root's nodes on the paths of the changes, with their entries.
    pub nodes: Vec<(NodeAt, Entry)>,
    /// The changed chunks; [`ChangeList::decode`] returns them sorted by index.
    pub changes: Vec<Change>,
}

const NODE_REC: usize = 1 + 8 + ENTRY_SIZE;
const CHANGE_REC: usize = 8 + ENTRY_SIZE + 1;

impl ChangeList {
    /// The encoding described on [`ChangeList`], with nodes and changes in their order here.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(9 + self.nodes.len() * NODE_REC + self.changes.len() * CHANGE_REC);
        b.extend_from_slice(CHANGE_LIST_MAGIC);
        b.push(self.depth);
        #[expect(
            clippy::cast_possible_truncation,
            reason = "a change list holds at most depth nodes per change, far below u32::MAX"
        )]
        let count = self.nodes.len() as u32;
        b.extend_from_slice(&count.to_le_bytes());
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
        let other = || FormatError::OtherFormat { what: "change list" };
        let Some((&[m0, m1, m2, m3, depth, c0, c1, c2, c3], _)) = b.split_first_chunk::<9>() else {
            return Err(other());
        };
        if [m0, m1, m2, m3] != *CHANGE_LIST_MAGIC {
            return Err(other());
        }
        let u64_at = |at: usize| {
            b.get(at..)
                .and_then(<[u8]>::first_chunk)
                .map(|v| u64::from_le_bytes(*v))
        };
        let count = u32::from_le_bytes([c0, c1, c2, c3]) as usize;
        let end = count
            .checked_mul(NODE_REC)
            .map(|n| n + 9)
            .filter(|&n| n <= b.len())
            .ok_or_else(bad)?;
        let nodes = (0..count)
            .map(|i| {
                let at = 9 + i * NODE_REC;
                let node = NodeAt {
                    level: *b.get(at)?,
                    prefix: u64_at(at + 1)?,
                };
                Some((node, Entry::read(b, at + 9)?))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(bad)?;
        let mut changes = Vec::new();
        let mut off = end;
        while off < b.len() {
            let (Some(idx), Some(entry), Some(&flag)) =
                (u64_at(off), Entry::read(b, off + 8), b.get(off + CHANGE_REC - 1))
            else {
                return Err(bad());
            };
            off += CHANGE_REC;
            let keys = match flag {
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
            depth,
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
