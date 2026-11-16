//! Recipes on upload.
//!
//! A file system writes file data in blocks of 4 KiB. A tool that rewrites files it already had
//! (an install, a checkout, a build that copies) puts the same blocks at new places, so most
//! chunks it changes consist of blocks that exist elsewhere: in the base the branch opened at,
//! or in chunks earlier runs on this host uploaded ([`learned`]). Such a chunk goes up as a
//! recipe: the chunks it takes blocks from, and only the blocks that are really new. Its
//! identity does not change: the tree still names it by the hash of its full content, and a
//! reader checks the rebuilt chunk against that hash.
//!
//! Block keys only find candidates: every candidate block is compared byte for byte with its
//! source before a recipe uses it, so a key collision costs a missed match, never wrong data.

mod block_map;
pub(crate) mod learned;
mod prepared;

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};

pub(crate) use block_map::{BlockAt, BlockMap};
use bytes::Bytes;
use pando_format::{
    BLOCK_SIZE, BLOCKS_PER_CHUNK, BlockKeys, CHUNK_SIZE, Entry, Hash, Pick, RECIPE_SOURCES_MAX, Recipe,
    SegId, is_zero,
};
pub(crate) use prepared::PreparedChunks;

use crate::Result;
use crate::ports::{LocalFile, LocalFs, Open};
use crate::tree::Tree;
use learned::LearnedIndex;

/// How many blocks a chunk must take from other chunks to go up as a recipe.
const RECIPE_MIN_SHARED: usize = 4;

/// A chunk made ready to append.
#[derive(Clone, Debug)]
pub(crate) enum Prepared {
    /// The chunk goes up as itself. A full chunk carries its block keys, for the change list.
    Plain { bytes: Bytes, keys: Option<BlockKeys> },
    /// The chunk equals one that is in the store already.
    Same(Entry),
    /// The chunk goes up as a recipe that reads the segments `deps`.
    Recipe {
        enc: Bytes,
        deps: Vec<SegId>,
        keys: BlockKeys,
    },
}

impl Prepared {
    /// The bytes this keeps in memory.
    pub(crate) fn held_bytes(&self) -> usize {
        match self {
            Prepared::Plain { bytes, .. } => bytes.len(),
            Prepared::Same(_) => 0,
            Prepared::Recipe { enc, .. } => enc.len(),
        }
    }
}

/// The blocks of the base a branch opened at: its block map, its image on this host, and its
/// root, which names each chunk as a recipe source.
#[derive(Debug)]
pub(crate) struct BaseBlocks {
    tree: Arc<Tree>,
    root: Entry,
    map: BlockMap,
    image: Arc<dyn LocalFile>,
    sources: Mutex<HashMap<u64, Entry>>,
}

impl BaseBlocks {
    pub(crate) fn new(tree: Arc<Tree>, root: Entry, map: BlockMap, image: Arc<dyn LocalFile>) -> BaseBlocks {
        BaseBlocks {
            tree,
            root,
            map,
            image,
            sources: Mutex::default(),
        }
    }

    /// The base blocks of `root` from the map saved at `map` and the image at `image`.
    pub(crate) async fn open(
        fs: &dyn LocalFs,
        map: &Path,
        image: &Path,
        tree: Arc<Tree>,
        root: Entry,
    ) -> Result<Option<BaseBlocks>> {
        let Some(m) = BlockMap::load(fs, map).await? else {
            return Ok(None);
        };
        let image = fs.open(image, Open::Read).await?;
        Ok(Some(BaseBlocks::new(tree, root, m, image)))
    }

    /// The published entry of chunk `idx` of the root, when it has one.
    async fn source(&self, idx: u64) -> Option<Entry> {
        if let Some(e) = self.sources.lock().expect("sources lock").get(&idx) {
            return (!e.is_empty()).then_some(*e);
        }
        let e = match self.tree.lookup(&self.root, idx, true).await {
            Ok(e) if !e.loc.is_pending() => e,
            _ => Entry::EMPTY,
        };
        self.sources.lock().expect("sources lock").insert(idx, e);
        (!e.is_empty()).then_some(e)
    }

    /// `n` blocks of chunk `idx` from block `blk` on; zeros past the end of the image.
    async fn read(&self, idx: u64, blk: usize, n: usize) -> Result<Bytes> {
        let off = idx * CHUNK_SIZE as u64 + (blk * BLOCK_SIZE) as u64;
        let b = self.image.read_at(n * BLOCK_SIZE, off).await?;
        if b.len() == n * BLOCK_SIZE {
            return Ok(b);
        }
        let mut v = b.to_vec();
        v.resize(n * BLOCK_SIZE, 0);
        Ok(Bytes::from(v))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Origin {
    Base,
    Learned,
}

#[derive(Clone, Copy)]
struct Candidate {
    origin: Origin,
    at: BlockAt,
}

enum Found {
    Nothing,
    Same(Entry),
    Recipe(Bytes, Vec<SegId>),
}

/// Where uploads of one branch take blocks from.
#[derive(Debug, Default)]
pub(crate) struct Dedup {
    pub(crate) base: Option<BaseBlocks>,
    pub(crate) learned: Option<Arc<LearnedIndex>>,
}

impl Dedup {
    /// Makes chunk `h` with bytes `b` ready to append: as the chunk it equals, as a recipe, or
    /// as itself.
    pub(crate) async fn prepare(&self, h: Hash, b: Bytes) -> Prepared {
        if b.len() != CHUNK_SIZE {
            return Prepared::Plain { bytes: b, keys: None };
        }
        let keys = BlockKeys::of(&b);
        match self.find(h, &b, &keys).await {
            Found::Nothing => Prepared::Plain {
                bytes: b,
                keys: Some(keys),
            },
            Found::Same(e) => Prepared::Same(e),
            Found::Recipe(enc, deps) => Prepared::Recipe { enc, deps, keys },
        }
    }

    fn candidate(&self, keys: &BlockKeys, blk: usize) -> Option<Candidate> {
        let k = &keys.0[blk];
        if let Some(at) = self.base.as_ref().and_then(|b| b.map.get(k)) {
            return Some(Candidate {
                origin: Origin::Base,
                at,
            });
        }
        self.learned
            .as_ref()
            .and_then(|l| l.lookup(k))
            .map(|at| Candidate {
                origin: Origin::Learned,
                at,
            })
    }

    /// The source of candidate `c` and `n` of its blocks, when it can be a source.
    async fn source(&self, c: Candidate, n: usize) -> Option<(Entry, Bytes)> {
        match c.origin {
            Origin::Base => {
                let b = self.base.as_ref()?;
                let e = b.source(c.at.chunk()).await?;
                Some((e, b.read(c.at.chunk(), c.at.block(), n).await.ok()?))
            }
            Origin::Learned => {
                let l = self.learned.as_ref()?;
                let slot = c.at.chunk() as u32;
                let e = l.source(slot).await?;
                Some((e, l.read(slot, c.at.block(), n).await.ok()?))
            }
        }
    }

    async fn find(&self, h: Hash, b: &[u8], keys: &BlockKeys) -> Found {
        let mut picks = [Pick::Literal; BLOCKS_PER_CHUNK];
        let mut cands: [Option<Candidate>; BLOCKS_PER_CHUNK] = [None; BLOCKS_PER_CHUNK];
        for (blk, part) in b.as_chunks::<BLOCK_SIZE>().0.iter().enumerate() {
            if is_zero(part) {
                picks[blk] = Pick::Zero;
            } else {
                cands[blk] = self.candidate(keys, blk);
            }
        }
        let mut sources: Vec<Entry> = Vec::new();
        let mut source_of: HashMap<Hash, u8> = HashMap::new();
        let mut shared = 0;
        let mut blk = 0;
        while blk < BLOCKS_PER_CHUNK {
            let Some(c) = cands[blk] else {
                blk += 1;
                continue;
            };
            let mut n = 1;
            while blk + n < BLOCKS_PER_CHUNK
                && cands[blk + n].is_some_and(|d| {
                    d.origin == c.origin && d.at.chunk() == c.at.chunk() && d.at.block() == c.at.block() + n
                })
            {
                n += 1;
            }
            let run = blk;
            blk += n;
            let Some((e, got)) = self.source(c, n).await else {
                continue;
            };
            for k in 0..n {
                let at = (run + k) * BLOCK_SIZE;
                if got[k * BLOCK_SIZE..(k + 1) * BLOCK_SIZE] != b[at..at + BLOCK_SIZE] {
                    continue;
                }
                if e.hash == h {
                    return Found::Same(e);
                }
                let src = match source_of.get(&e.hash) {
                    Some(&s) => s,
                    None if sources.len() == RECIPE_SOURCES_MAX => break,
                    None => {
                        let s = sources.len() as u8;
                        source_of.insert(e.hash, s);
                        sources.push(e);
                        s
                    }
                };
                picks[run + k] = Pick::Source {
                    src,
                    block: (c.at.block() + k) as u8,
                };
                shared += 1;
            }
        }
        if shared < RECIPE_MIN_SHARED {
            return Found::Nothing;
        }
        let enc = Recipe::encode(&sources, &picks, b);
        let mut seen = BTreeSet::new();
        let deps = sources
            .iter()
            .map(|s| s.loc.seg)
            .filter(|s| seen.insert(*s))
            .collect();
        Found::Recipe(Bytes::from(enc), deps)
    }
}

/// Makes chunk `h` ready to append with `dedup`, or as itself without one.
pub(crate) async fn prepare(dedup: Option<&Dedup>, h: Hash, b: Bytes) -> Prepared {
    match dedup {
        Some(d) => d.prepare(h, b).await,
        None if b.len() == CHUNK_SIZE => {
            let keys = Some(BlockKeys::of(&b));
            Prepared::Plain { bytes: b, keys }
        }
        None => Prepared::Plain { bytes: b, keys: None },
    }
}

#[cfg(all(test, feature = "sim"))]
mod tests;
