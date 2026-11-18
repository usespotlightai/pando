#![no_main]

use libfuzzer_sys::fuzz_target;
use pando_format::{Hash, SegId, keys};

fuzz_target!(|s: &str| {
    if let Some(id) = keys::parse_segment(s) {
        let seg = keys::segment(id);
        let idx = keys::segment_index(id);
        let under = |k: &str| {
            k.strip_prefix(keys::SEGMENTS)
                .and_then(|k| k.strip_prefix('/'))
                .map(str::to_owned)
        };
        assert_eq!(keys::parse_segment(&under(&seg).expect("segment key")), Some(id));
        assert_eq!(
            keys::parse_segment(&under(&idx).expect("segment index key")),
            Some(id)
        );
    }
    if let Some((root, from)) = keys::parse_change_list(s) {
        let k = keys::change_list(root, from);
        let name = k
            .strip_prefix(keys::CHANGES)
            .and_then(|k| k.strip_prefix('/'))
            .expect("change list key");
        assert_eq!(keys::parse_change_list(name), Some((root, from)));
    }
    if let Ok(h) = s.parse::<Hash>() {
        assert_eq!(h.to_string().parse::<Hash>().expect("a printed hash parses"), h);
    }
    if let Ok(id) = s.parse::<SegId>() {
        assert_eq!(
            id.to_string()
                .parse::<SegId>()
                .expect("a printed segment id parses"),
            id
        );
    }
});
