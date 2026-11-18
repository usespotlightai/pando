#![no_main]

use libfuzzer_sys::fuzz_target;
use pando_format::{DecodedFrame, FrameBuilder, Hash, decode_frame};

fn rebuild(d: &DecodedFrame) -> Vec<u8> {
    let mut f = FrameBuilder::default();
    let mut objects: Vec<_> = d.objects.iter().collect();
    objects.sort_by_key(|(h, _)| **h);
    for (h, v) in objects {
        f.push(*h, v, false);
    }
    let mut recipes: Vec<_> = d.recipes.iter().collect();
    recipes.sort_by_key(|(h, _)| **h);
    for (h, r) in recipes {
        f.push(*h, r, true);
    }
    f.finish().expect("a decoded frame encodes")
}

fuzz_target!(|data: &[u8]| {
    let Some((&sel, z)) = data.split_first() else {
        return;
    };
    let keep = |h: &Hash| h.0[0] < sel;
    let part = decode_frame(z, Some(&keep));
    let Ok(whole) = decode_frame(z, None) else {
        return;
    };
    let part = part.expect("a frame that decodes whole decodes in part");

    for (h, v) in &part.objects {
        assert_eq!(whole.objects.get(h), Some(v), "partial object differs from whole");
    }
    for (h, r) in &part.recipes {
        assert_eq!(whole.recipes.get(h), Some(r), "partial recipe differs from whole");
    }
    for h in whole.objects.keys().filter(|h| keep(h)) {
        assert!(
            part.objects.contains_key(h),
            "kept object missing from partial decode"
        );
    }
    for h in whole.recipes.keys().filter(|h| keep(h)) {
        assert!(
            part.recipes.contains_key(h),
            "kept recipe missing from partial decode"
        );
    }

    let again = decode_frame(&rebuild(&whole), None).expect("a re-encoded frame decodes");
    assert_eq!(again.objects, whole.objects);
    assert_eq!(again.recipes, whole.recipes);
});
