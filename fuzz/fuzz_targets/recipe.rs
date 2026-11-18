#![no_main]

use std::convert::Infallible;

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use pando_format::{
    BLOCK_SIZE, BLOCKS_PER_CHUNK, CHUNK_SIZE, ENTRY_SIZE, Entry, Hash, Pick, Recipe, RecipeError,
};

const RUN_LITERAL: u8 = 0xff;
const RUN_ZERO: u8 = 0xfe;

fn source(e: &Entry) -> Bytes {
    let seed = e.to_bytes();
    (0..CHUNK_SIZE)
        .map(|k| seed[k % ENTRY_SIZE] ^ (k / BLOCK_SIZE) as u8 ^ k as u8)
        .collect::<Vec<u8>>()
        .into()
}

fn picks_of(r: &Recipe<'_>) -> [Pick; BLOCKS_PER_CHUNK] {
    let mut picks = [Pick::Zero; BLOCKS_PER_CHUNK];
    for run in &r.runs {
        for k in 0..run.n {
            let blk = usize::from(run.dst + k);
            match run.src {
                RUN_ZERO => {}
                RUN_LITERAL => picks[blk] = Pick::Literal,
                src => {
                    picks[blk] = Pick::Source {
                        src,
                        block: run.src_block + k,
                    }
                }
            }
        }
    }
    picks
}

fuzz_target!(|data: &[u8]| {
    let Ok(r) = Recipe::parse(data) else {
        return;
    };
    assert_eq!(
        Recipe::sources_of(data),
        r.sources,
        "sources_of disagrees with parse"
    );

    let mut lit = &data[1 + r.sources.len() * ENTRY_SIZE + 1 + 4 * r.runs.len()..];
    let mut fetched: Vec<Option<Bytes>> = vec![None; r.sources.len()];
    let mut chunk = vec![0; CHUNK_SIZE];
    for run in &r.runs {
        for k in 0..run.n {
            let dst = usize::from(run.dst + k) * BLOCK_SIZE;
            let dst = dst..dst + BLOCK_SIZE;
            match run.src {
                RUN_ZERO => {}
                RUN_LITERAL => {
                    let (now, rest) = lit.split_at(BLOCK_SIZE);
                    chunk[dst].copy_from_slice(now);
                    lit = rest;
                }
                src => {
                    let src = usize::from(src);
                    let b = fetched[src].get_or_insert_with(|| source(&r.sources[src]));
                    let from = usize::from(run.src_block + k) * BLOCK_SIZE;
                    chunk[dst].copy_from_slice(&b[from..from + BLOCK_SIZE]);
                }
            }
        }
    }
    assert!(lit.is_empty(), "literal blocks left over");
    let hash = Hash::of(&chunk);

    let got = r
        .rebuild(hash, |e| Ok::<_, Infallible>(source(e)))
        .expect("the recipe rebuilds the chunk its runs describe");
    assert_eq!(&got[..], &chunk[..]);

    let reads_a_source = r.runs.iter().any(|run| run.src < RUN_ZERO);
    let failed = r.rebuild(hash, |_| Err(()));
    assert_eq!(reads_a_source, matches!(failed, Err(RecipeError::Source { .. })));
    let copies_a_block = r.runs.iter().any(|run| run.src < RUN_ZERO && run.n > 0);
    let short = r.rebuild(hash, |_| Ok::<_, Infallible>(Bytes::new()));
    assert!(!copies_a_block || short.is_err());

    let picks = picks_of(&r);
    let enc = Recipe::encode(&r.sources, &picks, &chunk);
    let again = Recipe::parse(&enc).expect("a re-encoded recipe parses");
    assert_eq!(again.sources, r.sources);
    assert_eq!(picks_of(&again), picks);
    let got = again
        .rebuild(hash, |e| Ok::<_, Infallible>(source(e)))
        .expect("a re-encoded recipe rebuilds its chunk");
    assert_eq!(&got[..], &chunk[..]);
    assert_eq!(
        Recipe::encode(&again.sources, &picks, &chunk),
        enc,
        "re-encoding is not a fixpoint"
    );
});
