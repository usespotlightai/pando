#![no_main]

use std::convert::Infallible;

use arbitrary::Arbitrary;
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use pando_format::{
    BLOCK_SIZE, BLOCKS_PER_CHUNK, CHUNK_SIZE, ENTRY_SIZE, Entry, Hash, Loc, Pick, RECIPE_SOURCES_MAX, Recipe,
};

const RUN_LITERAL: u8 = 0xff;
const RUN_ZERO: u8 = 0xfe;

#[derive(Arbitrary, Debug, Clone, Copy)]
enum ArbPick {
    Literal(u8),
    Zero,
    Source(u8, u8),
}

#[derive(Arbitrary, Debug)]
struct Input {
    sources: u8,
    picks: [ArbPick; BLOCKS_PER_CHUNK],
}

fn source(e: &Entry) -> Bytes {
    let seed = e.to_bytes();
    (0..CHUNK_SIZE)
        .map(|k| seed[k % ENTRY_SIZE] ^ (k / BLOCK_SIZE) as u8 ^ k as u8)
        .collect::<Vec<u8>>()
        .into()
}

fuzz_target!(|input: Input| {
    let n = usize::from(input.sources) % (RECIPE_SOURCES_MAX + 1);
    let sources: Vec<Entry> = (0..n)
        .map(|i| Entry {
            hash: Hash([i as u8 ^ 0xa5; 32]),
            loc: Loc {
                frame: i as u32,
                ..Loc::default()
            },
        })
        .collect();

    let mut fetched: Vec<Option<Bytes>> = vec![None; n];
    let mut chunk = vec![0; CHUNK_SIZE];
    let mut picks = [Pick::Zero; BLOCKS_PER_CHUNK];
    let mut literal_blocks = 0;
    for (blk, p) in input.picks.iter().enumerate() {
        let dst = &mut chunk[blk * BLOCK_SIZE..(blk + 1) * BLOCK_SIZE];
        picks[blk] = match *p {
            ArbPick::Zero => Pick::Zero,
            ArbPick::Source(..) if n == 0 => Pick::Zero,
            ArbPick::Literal(fill) => {
                for (k, b) in dst.iter_mut().enumerate() {
                    *b = fill.wrapping_add((k % 251) as u8);
                }
                literal_blocks += 1;
                Pick::Literal
            }
            ArbPick::Source(src, block) => {
                let src = usize::from(src) % n;
                let block = usize::from(block) % BLOCKS_PER_CHUNK;
                let b = fetched[src].get_or_insert_with(|| source(&sources[src]));
                dst.copy_from_slice(&b[block * BLOCK_SIZE..(block + 1) * BLOCK_SIZE]);
                Pick::Source {
                    src: src as u8,
                    block: block as u8,
                }
            }
        };
    }

    let enc = Recipe::encode(&sources, &picks, &chunk);
    let r = Recipe::parse(&enc).expect("an encoded recipe parses");
    assert_eq!(r.sources, sources);
    assert_eq!(Recipe::sources_of(&enc), sources);
    assert_eq!(
        enc.len(),
        1 + n * ENTRY_SIZE + 1 + 4 * r.runs.len() + literal_blocks * BLOCK_SIZE
    );

    let mut next = 0;
    for run in &r.runs {
        assert_eq!(usize::from(run.dst), next, "runs do not tile the chunk");
        assert!(run.n > 0, "empty run");
        next += usize::from(run.n);
        for k in 0..run.n {
            let want = match run.src {
                RUN_LITERAL => Pick::Literal,
                RUN_ZERO => Pick::Zero,
                src => Pick::Source {
                    src,
                    block: run.src_block + k,
                },
            };
            assert_eq!(picks[usize::from(run.dst + k)], want);
        }
    }
    assert_eq!(next, BLOCKS_PER_CHUNK);
    for w in r.runs.windows(2) {
        let (a, b) = (w[0], w[1]);
        let mergeable = a.src == b.src && (a.src >= RUN_ZERO || a.src_block + a.n == b.src_block);
        assert!(!mergeable, "neighbouring runs were not merged: {a:?} {b:?}");
    }

    let got = r
        .rebuild(Hash::of(&chunk), |e| Ok::<_, Infallible>(source(e)))
        .expect("an encoded recipe rebuilds its chunk");
    assert_eq!(&got[..], &chunk[..]);
});
