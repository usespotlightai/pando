#![no_main]

use libfuzzer_sys::fuzz_target;
use pando_format::SegmentIndex;

fuzz_target!(|data: &[u8]| {
    let Ok(i) = SegmentIndex::decode(data) else {
        return;
    };
    assert_eq!(
        i.encode(),
        data,
        "a segment index decoded from bytes other than its encoding"
    );
});
