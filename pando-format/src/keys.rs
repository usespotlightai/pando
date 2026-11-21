//! Object keys in the store, relative to the store's prefix.

use crate::{BranchId, Hash, SegId};

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
/// The marker that holds the store's [`crate::StoreFormat`].
pub const FORMAT: &str = "format";
/// An empty object a put tells from whether the store keeps earlier versions of objects.
pub const VERSIONS: &str = "versions";

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

/// The folder of the write logs of every life of branch `branch`, named by the hash of the
/// branch's name.
pub fn log_name(branch: &str) -> String {
    format!("{LOG}/{}", Hash::of(branch.as_bytes()))
}

/// The folder of the write log of life `id` of branch `branch`.
pub fn log_branch(branch: &str, id: BranchId) -> String {
    format!("{}/{id}", log_name(branch))
}

/// Record `n` of writer `generation` of life `id` of branch `branch`'s write log.
pub fn log_record(branch: &str, id: BranchId, generation: u64, n: u64) -> String {
    format!("{}/{generation:020}/{n:020}", log_branch(branch, id))
}

/// The generation and number a key under [`log_branch`] names.
pub fn parse_log_record(name: &str) -> Option<(u64, u64)> {
    let (g, n) = name.split_once('/')?;
    Some((g.parse().ok()?, n.parse().ok()?))
}

/// The hash of the branch name, the life, the generation and the number a key under [`LOG`]
/// names.
pub fn parse_log_key(name: &str) -> Option<(Hash, BranchId, u64, u64)> {
    let (branch, rest) = name.split_once('/')?;
    let (id, rest) = rest.split_once('/')?;
    let (g, n) = parse_log_record(rest)?;
    Some((branch.parse().ok()?, id.parse().ok()?, g, n))
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
        let life = BranchId::from_parts(4, 5);
        let key = log_record("main", life, 6, 7);
        assert_eq!(
            parse_log_key(key.strip_prefix("log/").unwrap()),
            Some((Hash::of(b"main"), life, 6, 7))
        );
        let id = SegId([3; 16]);
        assert_eq!(
            parse_segment(segment_index(id).strip_prefix("segments/").unwrap()),
            Some(id)
        );
    }
}
