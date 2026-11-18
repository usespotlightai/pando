#![no_main]

use libfuzzer_sys::fuzz_target;
use pando_format::{ENTRY_SIZE, Entry};

fuzz_target!(|data: &[u8]| {
    if let Some(b) = data.first_chunk::<ENTRY_SIZE>() {
        assert_eq!(&Entry::from_bytes(b).to_bytes(), b);
    }
    if let Ok(s) = std::str::from_utf8(data)
        && let Ok(e) = Entry::parse_text(s)
    {
        assert_eq!(
            Entry::parse_text(&e.to_string()).expect("a printed entry parses"),
            e
        );
        assert_eq!(Entry::from_bytes(&e.to_bytes()), e);
    }
});
