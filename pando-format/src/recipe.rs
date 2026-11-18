use bytes::Bytes;

use crate::{BLOCK_SIZE, BLOCKS_PER_CHUNK, CHUNK_SIZE, ENTRY_SIZE, Entry, Hash, IntegrityError, Verified};

const RUN_LITERAL: u8 = 0xff;
const RUN_ZERO: u8 = 0xfe;
/// The most sources one recipe can name; the source numbers above it mark literal and zero runs.
pub const RECIPE_SOURCES_MAX: usize = RUN_ZERO as usize;

/// Where the bytes of one block of a chunk come from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Pick {
    /// The block is stored in the recipe itself.
    Literal,
    /// The block is all zeros and stored nowhere.
    Zero,
    /// Block `block` of source chunk `src`.
    Source {
        /// The source's position in the recipe's source list.
        src: u8,
        /// The block's number in the source chunk.
        block: u8,
    },
}

/// A run of blocks of the chunk: `n` blocks from block `dst` on, taken from source `src` at
/// block `src_block`, or literal or zero.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Run {
    /// The first block of the run in the chunk.
    pub dst: u8,
    /// The number of blocks; `dst + n` is at most [`BLOCKS_PER_CHUNK`].
    pub n: u8,
    /// The source's position in the source list, or 0xff for a literal run and 0xfe for a zero run.
    pub src: u8,
    /// The first block taken from the source; 0 for literal and zero runs.
    pub src_block: u8,
}

/// A chunk described by blocks of other chunks plus the blocks that are really new. Its
/// identity does not change: the tree still names it by the hash of its full content, and
/// [`Recipe::rebuild`] checks the rebuilt chunk against that hash.
///
/// Encoding: source count u8, the source entries, run count u8, `[dst][n][src][src block]` per
/// run, then the literal blocks in run order.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Recipe<'a> {
    /// The chunks the runs read, numbered by their position.
    pub sources: Vec<Entry>,
    /// The runs in chunk order. [`Recipe::parse`] checks every run against the chunk, the
    /// sources and the literal blocks.
    pub runs: Vec<Run>,
    literals: &'a [u8],
}

/// Why [`Recipe::rebuild`] failed; `E` is the error of the source fetch.
#[derive(Debug, thiserror::Error)]
pub enum RecipeError<E> {
    /// A source is shorter than a run reads from it.
    #[error("recipe for {hash} is malformed: {why}")]
    Malformed {
        /// The chunk the recipe rebuilds.
        hash: Hash,
        /// What is wrong.
        why: &'static str,
    },
    /// Fetching a source failed.
    #[error("recipe source {source_hash}: {error}")]
    Source {
        /// The source that could not be fetched.
        source_hash: Hash,
        /// The fetch's error.
        error: E,
    },
    /// The rebuilt chunk does not hash to the hash that names it.
    #[error("chunk {} rebuilt from its recipe does not match its hash", .0.expected)]
    Integrity(IntegrityError),
}

impl<'a> Recipe<'a> {
    /// Parses and checks recipe `enc`: every run stays in the chunk and in its source, and the
    /// literal blocks are exactly the ones the literal runs need. The error says what is wrong.
    pub fn parse(enc: &'a [u8]) -> Result<Recipe<'a>, &'static str> {
        let nsrc = usize::from(*enc.first().ok_or("empty")?);
        if nsrc > RECIPE_SOURCES_MAX {
            return Err("more sources than a run can name");
        }
        let mut p = 1;
        let sources = (0..nsrc)
            .map(|i| Entry::read(enc, p + i * ENTRY_SIZE).ok_or("short source list"))
            .collect::<Result<Vec<_>, _>>()?;
        p += nsrc * ENTRY_SIZE;
        let nrun = usize::from(*enc.get(p).ok_or("no run count")?);
        p += 1;
        let table = enc.get(p..p + 4 * nrun).ok_or("short run table")?;
        let runs: Vec<Run> = table
            .as_chunks::<4>()
            .0
            .iter()
            .map(|r| Run {
                dst: r[0],
                n: r[1],
                src: r[2],
                src_block: r[3],
            })
            .collect();
        p += 4 * nrun;
        let mut literal = 0;
        for r in &runs {
            if usize::from(r.dst) + usize::from(r.n) > BLOCKS_PER_CHUNK {
                return Err("run past the chunk end");
            }
            match r.src {
                RUN_ZERO => {}
                RUN_LITERAL => literal += usize::from(r.n) * BLOCK_SIZE,
                s if usize::from(s) < nsrc => {
                    if usize::from(r.src_block) + usize::from(r.n) > BLOCKS_PER_CHUNK {
                        return Err("run past the source end");
                    }
                }
                _ => return Err("unknown source"),
            }
        }
        if enc.len() - p != literal {
            return Err("literal blocks do not match the runs");
        }
        Ok(Recipe {
            sources,
            runs,
            literals: &enc[p..],
        })
    }

    /// The sources a recipe reads, or none when it is malformed.
    pub fn sources_of(enc: &[u8]) -> Vec<Entry> {
        let Some((&n, rest)) = enc.split_first() else {
            return Vec::new();
        };
        rest.get(..usize::from(n) * ENTRY_SIZE).map_or_default(|s| {
            s.as_chunks::<ENTRY_SIZE>()
                .0
                .iter()
                .map(Entry::from_bytes)
                .collect()
        })
    }

    /// Encodes the recipe of `chunk` from one pick per block. Neighbouring picks merge into runs.
    ///
    /// # Panics
    /// When `chunk` is not a full chunk or there are more than [`RECIPE_SOURCES_MAX`] sources.
    pub fn encode(sources: &[Entry], picks: &[Pick; BLOCKS_PER_CHUNK], chunk: &[u8]) -> Vec<u8> {
        assert_eq!(chunk.len(), CHUNK_SIZE);
        assert!(sources.len() <= RECIPE_SOURCES_MAX);
        let mut runs: Vec<Run> = Vec::new();
        for (blk, pick) in (0u8..).zip(picks) {
            let (src, src_block) = match *pick {
                Pick::Literal => (RUN_LITERAL, 0),
                Pick::Zero => (RUN_ZERO, 0),
                Pick::Source { src, block } => (src, block),
            };
            if let Some(r) = runs.last_mut()
                && r.src == src
                && (src >= RUN_ZERO || r.src_block + r.n == src_block)
            {
                r.n += 1;
                continue;
            }
            runs.push(Run {
                dst: blk,
                n: 1,
                src,
                src_block,
            });
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "at most RECIPE_SOURCES_MAX sources, asserted above, and at most one run per block"
        )]
        let (nsrc, nrun) = (sources.len() as u8, runs.len() as u8);
        let mut enc = vec![nsrc];
        for s in sources {
            enc.extend_from_slice(&s.to_bytes());
        }
        enc.push(nrun);
        for r in &runs {
            enc.extend_from_slice(&[r.dst, r.n, r.src, r.src_block]);
        }
        for r in runs.iter().filter(|r| r.src == RUN_LITERAL) {
            enc.extend_from_slice(
                &chunk[usize::from(r.dst) * BLOCK_SIZE..usize::from(r.dst + r.n) * BLOCK_SIZE],
            );
        }
        enc
    }

    /// Rebuilds chunk `hash`, reading each source once through `get`, and checks the result.
    pub fn rebuild<E>(
        &self,
        hash: Hash,
        mut get: impl FnMut(&Entry) -> Result<Bytes, E>,
    ) -> Result<Verified, RecipeError<E>> {
        let mut out = vec![0; CHUNK_SIZE];
        let mut got: Vec<Option<Bytes>> = vec![None; self.sources.len()];
        let mut lit = self.literals;
        for r in &self.runs {
            let dst = usize::from(r.dst) * BLOCK_SIZE..usize::from(r.dst + r.n) * BLOCK_SIZE;
            match r.src {
                RUN_ZERO => {}
                RUN_LITERAL => {
                    let (now, rest) = lit.split_at(dst.len());
                    out[dst].copy_from_slice(now);
                    lit = rest;
                }
                s => {
                    let s = usize::from(s);
                    let b = match &mut got[s] {
                        Some(b) => b,
                        none => {
                            let e = &self.sources[s];
                            none.insert(get(e).map_err(|error| RecipeError::Source {
                                source_hash: e.hash,
                                error,
                            })?)
                        }
                    };
                    let from = usize::from(r.src_block) * BLOCK_SIZE;
                    let src = b.get(from..from + dst.len()).ok_or(RecipeError::Malformed {
                        hash,
                        why: "source too short",
                    })?;
                    out[dst].copy_from_slice(src);
                }
            }
        }
        Verified::check(hash, Bytes::from(out)).map_err(RecipeError::Integrity)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    fn chunks() -> (Vec<u8>, Vec<u8>, [Pick; BLOCKS_PER_CHUNK]) {
        let src: Vec<u8> = (0..CHUNK_SIZE)
            .map(|i| (i * 7 + i / 4096).to_le_bytes()[0])
            .collect();
        let mut chunk = vec![0; CHUNK_SIZE];
        let mut picks = [Pick::Zero; BLOCKS_PER_CHUNK];
        for (blk, pick) in (0u8..).zip(&mut picks).take(8) {
            let b = usize::from(blk);
            chunk[b * BLOCK_SIZE..(b + 1) * BLOCK_SIZE]
                .copy_from_slice(&src[(b + 4) * BLOCK_SIZE..(b + 5) * BLOCK_SIZE]);
            *pick = Pick::Source {
                src: 0,
                block: blk + 4,
            };
        }
        for (blk, pick) in (0u8..).zip(&mut picks).skip(12) {
            let b = usize::from(blk);
            chunk[b * BLOCK_SIZE..(b + 1) * BLOCK_SIZE].fill(blk);
            *pick = Pick::Literal;
        }
        (src, chunk, picks)
    }

    #[test]
    fn recipe_rebuilds_its_chunk_and_merges_runs() {
        let (src, chunk, picks) = chunks();
        let source = Entry {
            hash: Hash::of(&src),
            ..Entry::EMPTY
        };
        let enc = Recipe::encode(&[source], &picks, &chunk);
        let r = Recipe::parse(&enc).unwrap();
        assert_eq!(r.runs.len(), 3);
        assert_eq!(Recipe::sources_of(&enc), vec![source]);
        let got = r
            .rebuild(Hash::of(&chunk), |_| {
                Ok::<_, Infallible>(Bytes::from(src.clone()))
            })
            .unwrap();
        assert_eq!(&got[..], &chunk[..]);
    }

    #[test]
    fn a_wrong_source_fails_the_hash_check() {
        let (src, chunk, picks) = chunks();
        let enc = Recipe::encode(
            &[Entry {
                hash: Hash::of(&src),
                ..Entry::EMPTY
            }],
            &picks,
            &chunk,
        );
        let r = Recipe::parse(&enc).unwrap();
        let wrong = r.rebuild(Hash::of(&chunk), |_| {
            Ok::<_, Infallible>(Bytes::from(vec![1; CHUNK_SIZE]))
        });
        assert!(matches!(wrong, Err(RecipeError::Integrity(_))));
    }

    #[test]
    fn cut_recipes_are_rejected() {
        let (src, chunk, picks) = chunks();
        let enc = Recipe::encode(
            &[Entry {
                hash: Hash::of(&src),
                ..Entry::EMPTY
            }],
            &picks,
            &chunk,
        );
        for n in [0, 1, 30, enc.len() - 1] {
            assert!(Recipe::parse(&enc[..n]).is_err(), "cut at {n}");
        }
    }
}
