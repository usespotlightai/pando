//! The 4 KiB blocks of the base images on this host, by key.
//!
//! The writer of a root sends, in its change list, the keys of the blocks of each changed
//! chunk. A host that builds the root's base finds those blocks in base images it has; a chunk
//! whose blocks are all there is built from them and checked against its hash, instead of being
//! fetched.

use std::sync::Arc;

use bytes::Bytes;
use pando_format::{BLOCK_SIZE, BLOCKS_PER_CHUNK, BlockKey, BlockKeys, CHUNK_SIZE, u64_from};

use crate::Result;
use crate::dedup::BlockMap;
use crate::ports::LocalFile;

/// Where one block of a planned chunk comes from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum BlockSource {
    Zero,
    /// Image `base` at byte `off`.
    At {
        base: usize,
        off: u64,
    },
}

pub(crate) type Plan = [BlockSource; BLOCKS_PER_CHUNK];

#[derive(Debug, Default)]
pub(crate) struct HostBlocks {
    bases: Vec<(Arc<dyn LocalFile>, BlockMap)>,
}

impl HostBlocks {
    /// Puts a base image first, so its blocks are found before those of the others.
    pub(crate) fn add_first(&mut self, image: Arc<dyn LocalFile>, map: BlockMap) {
        self.bases.insert(0, (image, map));
    }

    pub(crate) fn push(&mut self, image: Arc<dyn LocalFile>, map: BlockMap) {
        self.bases.push((image, map));
    }

    pub(crate) fn len(&self) -> usize {
        self.bases.len()
    }

    /// Finds every block of a chunk with keys `k` on this host.
    pub(crate) fn plan(&self, k: &BlockKeys) -> Option<Plan> {
        let zero = BlockKey::zero();
        let mut p = [BlockSource::Zero; BLOCKS_PER_CHUNK];
        for (src, key) in p.iter_mut().zip(&k.0) {
            if *key == zero {
                continue;
            }
            *src = self.bases.iter().enumerate().find_map(|(i, (_, m))| {
                m.get(*key).map(|at| BlockSource::At {
                    base: i,
                    off: at.chunk() * u64_from(CHUNK_SIZE) + u64_from(at.block() * BLOCK_SIZE),
                })
            })?;
        }
        Some(p)
    }

    /// Reads the blocks of plan `p`, one read per run of adjacent blocks.
    pub(crate) async fn build(&self, p: &Plan) -> Result<Bytes> {
        let mut out = vec![0; CHUNK_SIZE];
        let mut i = 0;
        while i < BLOCKS_PER_CHUNK {
            let mut j = i + 1;
            match p[i] {
                BlockSource::Zero => {
                    while j < BLOCKS_PER_CHUNK && p[j] == BlockSource::Zero {
                        j += 1;
                    }
                }
                BlockSource::At { base, off } => {
                    while j < BLOCKS_PER_CHUNK
                        && p[j]
                            == (BlockSource::At {
                                base,
                                off: off + u64_from((j - i) * BLOCK_SIZE),
                            })
                    {
                        j += 1;
                    }
                    let n = (j - i) * BLOCK_SIZE;
                    let b = self.bases[base].0.read_at(n, off).await?;
                    out[i * BLOCK_SIZE..i * BLOCK_SIZE + b.len()].copy_from_slice(&b);
                }
            }
            i = j;
        }
        Ok(Bytes::from(out))
    }
}
