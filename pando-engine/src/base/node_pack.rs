//! A node pack keeps the tree nodes of a base's root next to the base image. A branch that opens
//! at that root loads them instead of fetching each node, which a lookup or a flush would
//! otherwise do one level after another.

use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use pando_format::Hash;

use crate::local::take;
use crate::{Error, Result};

const MAGIC: &[u8; 4] = b"nod1";

/// Encoding: the magic, then `[hash][length u32 LE][node]` per node, by hash.
pub(crate) fn encode(nodes: &HashMap<Hash, Bytes>) -> Result<Bytes> {
    let mut sorted: Vec<(&Hash, &Bytes)> = nodes.iter().collect();
    sorted.sort_unstable_by_key(|(h, _)| h.0);
    let mut out = BytesMut::with_capacity(MAGIC.len() + nodes.values().map(|b| 36 + b.len()).sum::<usize>());
    out.put_slice(MAGIC);
    for (h, b) in sorted {
        let n =
            u32::try_from(b.len()).map_err(|_| Error::Corrupt(format!("node {h} of {} bytes", b.len())))?;
        out.put_slice(&h.0);
        out.put_u32_le(n);
        out.put_slice(b);
    }
    Ok(out.freeze())
}

/// The nodes of a pack. Their bytes are not checked here: the tree checks each against its hash.
pub(crate) fn decode(b: &Bytes) -> Result<Vec<(Hash, Bytes)>> {
    let mut rest = b
        .strip_prefix(MAGIC)
        .ok_or_else(|| Error::Corrupt("not a node pack".into()))?;
    let cut = || Error::Corrupt("node pack cut short".into());
    let mut out = Vec::new();
    while !rest.is_empty() {
        let h = Hash(take(&mut rest).ok_or_else(cut)?);
        let n = u32::from_le_bytes(take(&mut rest).ok_or_else(cut)?) as usize;
        let (node, after) = rest.split_at_checked(n).ok_or_else(cut)?;
        out.push((h, b.slice_ref(node)));
        rest = after;
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
        let mut got: Vec<_> = decode(&encode(&nodes).unwrap()).unwrap();
        got.sort();
        let mut want: Vec<_> = nodes.into_iter().collect();
        want.sort();
        assert_eq!(got, want);
        assert!(decode(&Bytes::from_static(b"nod1\0")).is_err());
        assert!(decode(&Bytes::from_static(b"nod0")).is_err());
    }
}
