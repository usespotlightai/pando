#![no_main]

use libfuzzer_sys::fuzz_target;
use pando_format::{ENTRY_SIZE, Recording};

fuzz_target!(|data: &[u8]| {
    let Ok(r) = Recording::decode(data) else {
        return;
    };
    let n = r.entries.len();
    assert_eq!((r.ages.len(), r.nodes.len()), (n, n));
    let enc = r.encode();
    assert_eq!(enc.len(), data.len(), "re-encoding changed the length");
    assert_eq!(Recording::decode(&enc).expect("an encoded recording decodes"), r);
    let flags = 8 + n * (ENTRY_SIZE + 1);
    if data[flags..flags + n].iter().all(|&f| f <= 1) {
        assert_eq!(
            enc, data,
            "a recording decoded from bytes other than its encoding"
        );
    }
});
