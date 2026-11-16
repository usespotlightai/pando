//! Object keys in the store, relative to the store's prefix.

use crate::{Hash, SegId};

pub const SEGMENTS: &str = "segments";
pub const REFS: &str = "refs";
pub const CHANGES: &str = "changes";
pub const HINTS: &str = "hints";
/// A key no object has; a GET of it opens connections before a fetch needs them.
pub const WARM: &str = "warm";

pub fn segment(id: SegId) -> String {
    format!("{SEGMENTS}/{id}")
}

pub fn segment_index(id: SegId) -> String {
    format!("{SEGMENTS}/{id}.idx")
}

pub fn reference(name: &str) -> String {
    format!("{REFS}/{name}")
}

pub fn change_list(root: Hash, from: Hash) -> String {
    format!("{CHANGES}/{root}.{from}")
}

pub fn hints(key: &str) -> String {
    format!("{HINTS}/{key}")
}

/// The recording of the first reads of mounts at `root`. A new branch starts at its parent's
/// root, so it inherits the parent's recording.
pub fn recording(root: Hash) -> String {
    hints(&format!("recording-{root}"))
}

/// The segment a key under [`SEGMENTS`] names (its data or its index).
pub fn parse_segment(name: &str) -> Option<SegId> {
    name.strip_suffix(".idx").unwrap_or(name).parse().ok()
}

/// The root and the earlier root a change list key names.
pub fn parse_change_list(name: &str) -> Option<(Hash, Hash)> {
    let (root, from) = name.split_once('.')?;
    Some((root.parse().ok()?, from.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_parse_back() {
        let (a, b) = (Hash::of(b"a"), Hash::of(b"b"));
        let name = change_list(a, b);
        assert_eq!(
            parse_change_list(name.strip_prefix("changes/").unwrap()),
            Some((a, b))
        );
        let id = SegId([3; 16]);
        assert_eq!(
            parse_segment(segment_index(id).strip_prefix("segments/").unwrap()),
            Some(id)
        );
    }
}
