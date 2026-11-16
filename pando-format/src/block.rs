use xxhash_rust::xxh3::xxh3_64;

/// The unit of the block map: the tree maps chunk indexes to chunks of this size.
pub const CHUNK_SIZE: usize = 64 << 10;
/// The file system block size. Recipes and block keys work in blocks of this size.
pub const BLOCK_SIZE: usize = 4 << 10;
pub const BLOCKS_PER_CHUNK: usize = CHUNK_SIZE / BLOCK_SIZE;

/// A fast, non-cryptographic fingerprint (XXH3-64) of a block. It only finds candidates:
/// every match is compared byte for byte or checked by hash before it is used.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default)]
pub struct BlockKey(pub u64);

impl BlockKey {
    pub fn of(block: &[u8]) -> BlockKey {
        BlockKey(xxh3_64(block))
    }

    pub fn zero() -> BlockKey {
        BlockKey::of(&[0; BLOCK_SIZE])
    }
}

/// The keys of the blocks of one full chunk.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct BlockKeys(pub [BlockKey; BLOCKS_PER_CHUNK]);

impl BlockKeys {
    /// # Panics
    /// When `chunk` is not [`CHUNK_SIZE`] bytes.
    pub fn of(chunk: &[u8]) -> BlockKeys {
        assert_eq!(chunk.len(), CHUNK_SIZE, "block keys need a full chunk");
        let mut k = [BlockKey::default(); BLOCKS_PER_CHUNK];
        for (key, block) in k.iter_mut().zip(chunk.as_chunks::<BLOCK_SIZE>().0) {
            *key = BlockKey::of(block);
        }
        BlockKeys(k)
    }
}

pub fn is_zero(b: &[u8]) -> bool {
    b.iter().all(|&x| x == 0)
}
