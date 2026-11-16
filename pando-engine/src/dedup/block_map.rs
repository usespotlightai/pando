//! The block map of a base image: the 4 KiB blocks of a root, by key, and the file that stores
//! it next to the image.

use std::collections::HashMap;
use std::path::Path;

use bytes::Bytes;
use pando_format::{BLOCK_SIZE, BLOCKS_PER_CHUNK, BlockKey, is_zero};

use crate::local::{read_file, write_durable};
use crate::ports::LocalFs;
use crate::{Error, Result};

const MAGIC: &[u8; 4] = b"bxi3";
const RECORD: usize = 12;

/// Where a block is: `chunk << 4 | block`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct BlockAt(pub(crate) u32);

impl BlockAt {
    pub(crate) fn new(chunk: u64, block: usize) -> BlockAt {
        debug_assert!(chunk < 1 << 28 && block < BLOCKS_PER_CHUNK);
        BlockAt((chunk as u32) << 4 | block as u32)
    }

    pub(crate) fn chunk(self) -> u64 {
        u64::from(self.0 >> 4)
    }

    pub(crate) fn block(self) -> usize {
        (self.0 & 15) as usize
    }
}

/// The first place of every non-zero block of a root.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct BlockMap(HashMap<BlockKey, BlockAt>);

impl BlockMap {
    pub(crate) fn get(&self, k: &BlockKey) -> Option<BlockAt> {
        self.0.get(k).copied()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    /// Adds the non-zero blocks of chunk `idx`; a block already mapped keeps its first place.
    pub(crate) fn add_chunk(&mut self, idx: u64, b: &[u8]) {
        for (blk, part) in b
            .as_chunks::<BLOCK_SIZE>()
            .0
            .iter()
            .take(BLOCKS_PER_CHUNK)
            .enumerate()
        {
            if !is_zero(part) {
                self.0.entry(BlockKey::of(part)).or_insert(BlockAt::new(idx, blk));
            }
        }
    }

    /// Drops the blocks of the chunks `stale` names.
    pub(crate) fn drop_chunks(&mut self, stale: impl Fn(u64) -> bool) {
        self.0.retain(|_, at| !stale(at.chunk()));
    }

    /// Adds every block of `other` that this map does not have.
    pub(crate) fn merge(&mut self, other: BlockMap) {
        for (k, at) in other.0 {
            self.0.entry(k).or_insert(at);
        }
    }

    /// Adds block `blk` of chunk `idx` under key `k`, unless `k` is mapped already.
    pub(crate) fn add(&mut self, k: BlockKey, idx: u64, blk: usize) {
        self.0.entry(k).or_insert(BlockAt::new(idx, blk));
    }

    /// Encoding: the magic, then `[key u64][place u32]` per block, little endian.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(MAGIC.len() + self.0.len() * RECORD);
        out.extend_from_slice(MAGIC);
        for (k, at) in &self.0 {
            out.extend_from_slice(&k.0.to_le_bytes());
            out.extend_from_slice(&at.0.to_le_bytes());
        }
        out
    }

    pub(crate) fn decode(b: &[u8]) -> Result<BlockMap> {
        let body = b
            .strip_prefix(MAGIC)
            .ok_or_else(|| Error::Corrupt("the block map has another format".into()))?;
        let (records, _) = body.as_chunks::<RECORD>();
        let mut m = HashMap::with_capacity(records.len());
        for r in records {
            let (k, at) = r.split_at(8);
            m.insert(
                BlockKey(u64::from_le_bytes(k.try_into().expect("8 bytes"))),
                BlockAt(u32::from_le_bytes(at.try_into().expect("4 bytes"))),
            );
        }
        Ok(BlockMap(m))
    }

    pub(crate) async fn save(&self, fs: &dyn LocalFs, path: &Path) -> Result<()> {
        Ok(write_durable(fs, path, Bytes::from(self.encode())).await?)
    }

    pub(crate) async fn load(fs: &dyn LocalFs, path: &Path) -> Result<Option<BlockMap>> {
        match read_file(fs, path).await? {
            Some(b) => Ok(Some(BlockMap::decode(&b)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use pando_format::CHUNK_SIZE;

    use super::*;

    #[test]
    fn a_block_map_round_trips_and_keeps_first_places() {
        let mut chunk = vec![0u8; CHUNK_SIZE];
        chunk[BLOCK_SIZE..2 * BLOCK_SIZE].fill(7);
        chunk[3 * BLOCK_SIZE..4 * BLOCK_SIZE].fill(7);
        let mut m = BlockMap::default();
        m.add_chunk(5, &chunk);
        m.add_chunk(6, &chunk);
        assert_eq!(
            m.len(),
            1,
            "zero blocks are left out and a repeat keeps its first place"
        );
        let at = m.get(&BlockKey::of(&chunk[BLOCK_SIZE..2 * BLOCK_SIZE])).unwrap();
        assert_eq!((at.chunk(), at.block()), (5, 1));
        assert_eq!(BlockMap::decode(&m.encode()).unwrap(), m);
        assert!(BlockMap::decode(b"bxi2").is_err());
    }
}
