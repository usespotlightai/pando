//! A node pack keeps the tree nodes of a base's root next to the base image. A branch that opens
//! at that root loads them instead of fetching each node, which a lookup or a flush would
//! otherwise do one level after another.

use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use pando_format::Hash;

use crate::{Error, Result};

const MAGIC: &[u8; 4] = b"nod1";

/// Encoding: the magic, then `[hash][length u32 LE][node]` per node.
pub(crate) fn encode(nodes: &HashMap<Hash, Bytes>) -> Bytes {
    let mut out = BytesMut::with_capacity(MAGIC.len() + nodes.values().map(|b| 36 + b.len()).sum::<usize>());
    out.put_slice(MAGIC);
    for (h, b) in nodes {
        out.put_slice(&h.0);
        out.put_u32_le(b.len() as u32);
        out.put_slice(b);
    }
    out.freeze()
}

/// The nodes of a pack. Their bytes are not checked here: the tree checks each against its hash.
pub(crate) fn decode(b: &Bytes) -> Result<Vec<(Hash, Bytes)>> {
    if !b.starts_with(MAGIC) {
        return Err(Error::Corrupt("not a node pack".into()));
    }
    let mut out = Vec::new();
    let mut at = MAGIC.len();
    while at < b.len() {
        let head = b
            .get(at..at + 36)
            .ok_or_else(|| Error::Corrupt("node pack cut short".into()))?;
        let h = Hash(head[..32].try_into().expect("32 bytes"));
        let n = u32::from_le_bytes(head[32..].try_into().expect("4 bytes")) as usize;
        let end = at + 36 + n;
        if end > b.len() {
            return Err(Error::Corrupt("node pack cut short".into()));
        }
        out.push((h, b.slice(at + 36..end)));
        at = end;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_node_pack_round_trips() {
        let nodes = HashMap::from([
            (Hash::of(b"a"), Bytes::from_static(b"a")),
            (Hash::of(b"bc"), Bytes::from_static(b"bc")),
        ]);
        let mut got: Vec<_> = decode(&encode(&nodes)).unwrap();
        got.sort();
        let mut want: Vec<_> = nodes.into_iter().collect();
        want.sort();
        assert_eq!(got, want);
        assert!(decode(&Bytes::from_static(b"nod1\0")).is_err());
        assert!(decode(&Bytes::from_static(b"nod0")).is_err());
    }
}
