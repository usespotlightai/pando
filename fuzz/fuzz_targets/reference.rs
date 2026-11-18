#![no_main]

use libfuzzer_sys::fuzz_target;
use pando_format::Ref;

fuzz_target!(|data: &[u8]| {
    let Ok(r) = Ref::decode(data) else {
        return;
    };
    let enc = r.encode();
    assert_eq!(Ref::decode(&enc).expect("an encoded ref decodes"), r);
});
