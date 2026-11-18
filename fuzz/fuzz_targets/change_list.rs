#![no_main]

use libfuzzer_sys::fuzz_target;
use pando_format::{ChangeList, ENTRY_SIZE};

fuzz_target!(|data: &[u8]| {
    let Ok(cl) = ChangeList::decode(data) else {
        return;
    };
    assert!(
        cl.changes.is_sorted_by_key(|c| c.idx),
        "changes not sorted by chunk index"
    );
    let enc = cl.encode();
    assert_eq!(enc.len(), data.len(), "re-encoding changed the length");
    let head = 9 + cl.nodes.len() * (1 + 8 + ENTRY_SIZE);
    assert_eq!(
        enc[..head],
        data[..head],
        "re-encoding changed the header or the nodes"
    );
    assert_eq!(
        ChangeList::decode(&enc).expect("an encoded change list decodes"),
        cl
    );
});
