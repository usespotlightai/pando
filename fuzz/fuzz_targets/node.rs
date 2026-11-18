#![no_main]

use libfuzzer_sys::fuzz_target;
use pando_format::Node;

fuzz_target!(|data: &[u8]| {
    let Ok(n) = Node::decode(data) else {
        return;
    };
    let enc = n.encode();
    assert_eq!(
        Node::decode(&enc).expect("an encoded node decodes"),
        n,
        "decode, encode, decode changed the node"
    );
    assert_eq!(enc, data, "a node decoded from bytes other than its encoding");
});
