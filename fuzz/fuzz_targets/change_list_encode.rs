#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use pando_format::{
    BLOCKS_PER_CHUNK, BlockKey, BlockKeys, Change, ChangeList, ENTRY_SIZE, Entry, Hash, Loc, NodeAt, SegId,
};

#[derive(Arbitrary, Debug)]
struct ArbEntry {
    hash: [u8; 32],
    seg: [u8; 16],
    frame: u32,
    inner: u32,
    len: u32,
}

impl ArbEntry {
    fn entry(&self) -> Entry {
        Entry {
            hash: Hash(self.hash),
            loc: Loc {
                seg: SegId(self.seg),
                frame: self.frame,
                inner: self.inner,
                len: self.len,
            },
        }
    }
}

#[derive(Arbitrary, Debug)]
struct Input {
    depth: u8,
    nodes: Vec<(u8, u64, ArbEntry)>,
    changes: Vec<(u64, ArbEntry, Option<[u64; BLOCKS_PER_CHUNK]>)>,
}

fuzz_target!(|input: Input| {
    let cl = ChangeList {
        depth: input.depth,
        nodes: input
            .nodes
            .iter()
            .map(|(level, prefix, e)| {
                (
                    NodeAt {
                        level: *level,
                        prefix: *prefix,
                    },
                    e.entry(),
                )
            })
            .collect(),
        changes: input
            .changes
            .iter()
            .map(|(idx, e, keys)| Change {
                idx: *idx,
                entry: e.entry(),
                keys: keys.map(|k| BlockKeys(k.map(BlockKey))),
            })
            .collect(),
    };
    let enc = cl.encode();
    let keyed = cl.changes.iter().filter(|c| c.keys.is_some()).count();
    assert_eq!(
        enc.len(),
        9 + cl.nodes.len() * (1 + 8 + ENTRY_SIZE)
            + cl.changes.len() * (8 + ENTRY_SIZE + 1)
            + keyed * 8 * BLOCKS_PER_CHUNK
    );

    let mut sorted = cl.clone();
    sorted.changes.sort_by_key(|c| c.idx);
    let dec = ChangeList::decode(&enc).expect("an encoded change list decodes");
    assert_eq!(dec, sorted);
    let again = dec.encode();
    assert_eq!(again, sorted.encode());
    if cl.changes.is_sorted_by_key(|c| c.idx) {
        assert_eq!(again, enc);
    }
});
