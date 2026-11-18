//! Object keys in the store, relative to the store's prefix.

use crate::{Hash, SegId};

/// The prefix, before a `/`, of the keys of segments and their indexes.
pub const SEGMENTS: &str = "segments";
/// The prefix, before a `/`, of the keys of refs.
pub const REFS: &str = "refs";
/// The prefix, before a `/`, of the keys of checkpoints.
pub const CHECKPOINTS: &str = "checkpoints";
/// The prefix, before a `/`, of the keys of write log records.
pub const LOG: &str = "log";
/// The prefix, before a `/`, of the keys of change lists.
pub const CHANGES: &str = "changes";
const HINTS: &str = "hints";
/// A key no object has; a GET of it opens connections before a fetch needs them.
pub const WARM: &str = "warm";

/// The key of segment `id`'s data.
pub fn segment(id: SegId) -> String {
    format!("{SEGMENTS}/{id}")
}

/// The key of segment `id`'s [`SegmentIndex`](crate::SegmentIndex).
pub fn segment_index(id: SegId) -> String {
    format!("{SEGMENTS}/{id}.idx")
}

/// The key of the [`Ref`](crate::Ref) of branch `name`.
pub fn reference(name: &str) -> String {
    format!("{REFS}/{name}")
}

/// The key of checkpoint `name`.
pub fn checkpoint(name: &str) -> String {
    format!("{CHECKPOINTS}/{name}")
}

/// The folder of branch `branch`'s write log, named by the hash of the branch's name.
pub fn log_branch(branch: &str) -> String {
    format!("{LOG}/{}", Hash::of(branch.as_bytes()))
}

/// Record `n` of writer `generation` of branch `branch`'s write log.
pub fn log_record(branch: &str, generation: u64, n: u64) -> String {
    format!("{}/{generation:020}/{n:020}", log_branch(branch))
}

/// The generation and number a key under [`log_branch`] names.
pub fn parse_log_record(name: &str) -> Option<(u64, u64)> {
    let (g, n) = name.split_once('/')?;
    Some((g.parse().ok()?, n.parse().ok()?))
}

/// The key of the [`ChangeList`](crate::ChangeList) from root `from` to root `root`.
pub fn change_list(root: Hash, from: Hash) -> String {
    format!("{CHANGES}/{root}.{from}")
}

fn hints(key: &str) -> String {
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
