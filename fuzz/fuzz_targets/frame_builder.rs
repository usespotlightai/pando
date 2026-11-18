#![no_main]

use std::collections::{HashMap, HashSet};

use arbitrary::Arbitrary;
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use pando_format::{DecodedFrame, Entry, FormatError, FrameBuilder, Hash, Recipe, Verified, decode_frame};

#[derive(Arbitrary, Debug)]
struct Object {
    bytes: Vec<u8>,
    recipe: bool,
    claimed: Option<[u8; 32]>,
    sources: Vec<u8>,
    keep: bool,
}

struct Pushed {
    hash: Hash,
    bytes: Vec<u8>,
    recipe: bool,
    keep: bool,
}

fn build(input: &[Object]) -> Vec<Pushed> {
    let plain: Vec<Hash> = input
        .iter()
        .map(|o| o.claimed.map_or_else(|| Hash::of(&o.bytes), Hash))
        .collect();
    input
        .iter()
        .zip(&plain)
        .map(|(o, &hash)| {
            if !o.recipe {
                return Pushed {
                    hash,
                    bytes: o.bytes.clone(),
                    recipe: false,
                    keep: o.keep,
                };
            }
            let named: Vec<Hash> = o
                .sources
                .iter()
                .map(|&i| plain[usize::from(i) % plain.len()])
                .collect();
            let mut bytes = vec![u8::try_from(named.len().min(255)).expect("clamped")];
            for h in named.iter().take(255) {
                bytes.extend_from_slice(
                    &Entry {
                        hash: *h,
                        ..Entry::EMPTY
                    }
                    .to_bytes(),
                );
            }
            bytes.extend_from_slice(&o.bytes);
            Pushed {
                hash,
                bytes,
                recipe: true,
                keep: o.keep,
            }
        })
        .collect()
}

fn expect(pushed: &[Pushed], want: &dyn Fn(&Hash) -> bool) -> Option<DecodedFrame> {
    let mut objects = HashMap::new();
    let mut recipes = HashMap::new();
    for p in pushed.iter().filter(|p| want(&p.hash)) {
        if p.recipe {
            recipes.insert(p.hash, Bytes::from(p.bytes.clone()));
        } else {
            objects.insert(
                p.hash,
                Verified::check(p.hash, Bytes::from(p.bytes.clone())).ok()?,
            );
        }
    }
    Some(DecodedFrame { objects, recipes })
}

fn check(got: Result<DecodedFrame, FormatError>, want: Option<DecodedFrame>) {
    match (got, want) {
        (Ok(got), Some(want)) => {
            assert_eq!(got.objects, want.objects);
            assert_eq!(got.recipes, want.recipes);
        }
        (Err(FormatError::Integrity(_)), None) => {}
        (got, want) => panic!("decoded {got:?}, expected {want:?}"),
    }
}

fuzz_target!(|input: Vec<Object>| {
    let pushed = build(&input);
    let mut f = FrameBuilder::default();
    let mut at = 0u32;
    for p in &pushed {
        assert_eq!(
            f.push(p.hash, &p.bytes, p.recipe),
            (at, u32::try_from(p.bytes.len()).expect("small"))
        );
        at += u32::try_from(p.bytes.len()).expect("small");
    }
    assert_eq!(f.is_empty(), pushed.is_empty());
    assert_eq!(f.payload_len(), usize::try_from(at).expect("small"));
    let z = f.finish().expect("a frame encodes");

    check(decode_frame(&z, None), expect(&pushed, &|_| true));

    let kept: HashSet<Hash> = pushed.iter().filter(|p| p.keep).map(|p| p.hash).collect();
    let sources: HashSet<Hash> = pushed
        .iter()
        .filter(|p| p.recipe && kept.contains(&p.hash))
        .flat_map(|p| Recipe::sources_of(&p.bytes))
        .map(|e| e.hash)
        .collect();
    let keep = |h: &Hash| kept.contains(h);
    check(
        decode_frame(&z, Some(&keep)),
        expect(&pushed, &|h| kept.contains(h) || sources.contains(h)),
    );
});
