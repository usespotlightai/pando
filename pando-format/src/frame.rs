use std::collections::{HashMap, HashSet};
use std::io::Read;

use bytes::Bytes;

use crate::{FormatError, Hash, Recipe, Verified, u64_from, usize_from};

/// The most payload a writer puts in one frame. One GET of a frame fills the cache with
/// every neighbour in it.
pub const FRAME_MAX: usize = 1 << 20;
/// The zstd level frames are written with.
pub const ZSTD_LEVEL: i32 = 3;

const RECIPE_FLAG: u32 = 1 << 31;
const HEADER_ENTRY: usize = 40;
const HEADER_MAX: usize = 16 << 20;
const PAYLOAD_MAX: usize = 16 * FRAME_MAX;

/// One object in a frame: its content hash, where it sits in the payload, and whether the
/// bytes are a recipe of the chunk (whose hash it still is) rather than the chunk itself.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct FrameObject {
    hash: Hash,
    inner: u32,
    len: u32,
    recipe: bool,
}

/// Collects objects into one frame.
///
/// A frame is one zstd frame (with its content size, without a checksum) of: object count
/// (u32 LE), `[hash][inner u32][len u32 | recipe flag]` per object, then the payload.
#[derive(Debug, Default)]
pub struct FrameBuilder {
    objects: Vec<FrameObject>,
    payload: Vec<u8>,
}

impl FrameBuilder {
    /// Whether no object has been pushed.
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// The uncompressed payload bytes pushed so far.
    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }

    /// Whether `n` more bytes would take a non-empty frame past [`FRAME_MAX`].
    pub fn is_full_for(&self, n: usize) -> bool {
        !self.payload.is_empty() && self.payload.len() + n > FRAME_MAX
    }

    /// Appends an object and returns its offset and length in the payload.
    pub fn push(&mut self, hash: Hash, bytes: &[u8], recipe: bool) -> (u32, u32) {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "writers close a frame once is_full_for says so, and objects are at most a chunk or \
                      its recipe, so a payload stays near FRAME_MAX (1 MiB)"
        )]
        let (inner, len) = (self.payload.len() as u32, bytes.len() as u32);
        self.objects.push(FrameObject {
            hash,
            inner,
            len,
            recipe,
        });
        self.payload.extend_from_slice(bytes);
        (inner, len)
    }

    /// The compressed frame of the pushed objects, in push order.
    pub fn finish(self) -> Result<Vec<u8>, FormatError> {
        encode_frame(&self.objects, &self.payload)
    }
}

fn encode_frame(objects: &[FrameObject], payload: &[u8]) -> Result<Vec<u8>, FormatError> {
    let count =
        u32::try_from(objects.len()).map_err(|_| FormatError::malformed("frame", "too many objects"))?;
    let mut raw = Vec::with_capacity(4 + objects.len() * HEADER_ENTRY + payload.len());
    raw.extend_from_slice(&count.to_le_bytes());
    for o in objects {
        raw.extend_from_slice(&o.hash.0);
        raw.extend_from_slice(&o.inner.to_le_bytes());
        raw.extend_from_slice(&(o.len | if o.recipe { RECIPE_FLAG } else { 0 }).to_le_bytes());
    }
    raw.extend_from_slice(payload);
    Ok(zstd::bulk::compress(&raw, ZSTD_LEVEL)?)
}

/// What a frame holds: objects checked against their hashes, and recipes (checked only once
/// rebuilt, by [`Recipe::rebuild`]).
#[derive(Debug, Default)]
pub struct DecodedFrame {
    /// The plain objects, each checked against its hash.
    pub objects: HashMap<Hash, Verified>,
    /// The recipes by the hash of the chunk they rebuild; not checked until rebuilt.
    pub recipes: HashMap<Hash, Bytes>,
}

/// Decodes frame `z`. With `keep`, it returns only the objects and recipes `keep` keeps, plus
/// the sources of those recipes that the frame holds, and decompresses the frame only up to
/// the last of them when the frame states its content size.
pub fn decode_frame(z: &[u8], keep: Option<&dyn Fn(&Hash) -> bool>) -> Result<DecodedFrame, FormatError> {
    let size = zstd::zstd_safe::get_frame_content_size(z).ok().flatten();
    let mut dec = zstd::stream::read::Decoder::with_buffer(z)?.single_frame();
    let mut count = [0; 4];
    dec.read_exact(&mut count)?;
    let header = (u32::from_le_bytes(count) as usize)
        .checked_mul(HEADER_ENTRY)
        .filter(|&n| n <= HEADER_MAX && size.is_none_or(|s| u64_from(4 + n) <= s))
        .ok_or_else(|| FormatError::malformed("frame", "header longer than the frame"))?;
    let mut entries = vec![0; header];
    dec.read_exact(&mut entries)?;
    let objects: Vec<FrameObject> = entries
        .as_chunks::<HEADER_ENTRY>()
        .0
        .iter()
        .map(header_entry)
        .collect();

    let mut payload = Vec::new();
    let mut sources = HashSet::new();
    if let (Some(keep), Some(size)) = (keep, size) {
        let limit = usize_from(size - 4 - u64_from(header));
        let end = payload_end(&objects, &|h| keep(h), limit)?;
        read_payload_to(&mut dec, &mut payload, end)?;
        sources = kept_recipe_sources(&objects, &payload, keep);
        if !sources.is_empty() {
            let end = payload_end(&objects, &|h| keep(h) || sources.contains(h), limit)?;
            read_payload_to(&mut dec, &mut payload, end)?;
        }
    } else {
        dec.by_ref()
            .take(u64_from(PAYLOAD_MAX) + 1)
            .read_to_end(&mut payload)?;
        if payload.len() > PAYLOAD_MAX {
            return Err(FormatError::malformed(
                "frame",
                "payload larger than any writer makes",
            ));
        }
        if let Some(keep) = keep {
            sources = kept_recipe_sources(&objects, &payload, keep);
        }
    }
    let keep = |h: &Hash| keep.is_none_or(|k| k(h)) || sources.contains(h);
    frame_objects(&objects, &Bytes::from(payload), &keep)
}

fn header_entry(&[ref hash @ .., i0, i1, i2, i3, l0, l1, l2, l3]: &[u8; HEADER_ENTRY]) -> FrameObject {
    let len = u32::from_le_bytes([l0, l1, l2, l3]);
    FrameObject {
        hash: Hash(*hash),
        inner: u32::from_le_bytes([i0, i1, i2, i3]),
        len: len & !RECIPE_FLAG,
        recipe: len & RECIPE_FLAG != 0,
    }
}

fn span(o: &FrameObject) -> std::ops::Range<usize> {
    o.inner as usize..o.inner as usize + o.len as usize
}

fn read_payload_to(dec: &mut impl Read, payload: &mut Vec<u8>, end: usize) -> Result<(), FormatError> {
    if end > PAYLOAD_MAX {
        return Err(FormatError::malformed(
            "frame",
            "payload larger than any writer makes",
        ));
    }
    let Some(more) = end.checked_sub(payload.len()) else {
        return Ok(());
    };
    dec.take(u64_from(more)).read_to_end(payload)?;
    if payload.len() == end {
        Ok(())
    } else {
        Err(FormatError::malformed(
            "frame",
            "payload shorter than its header says",
        ))
    }
}

fn payload_end(
    objects: &[FrameObject],
    keep: &dyn Fn(&Hash) -> bool,
    limit: usize,
) -> Result<usize, FormatError> {
    let mut end = 0;
    for o in objects.iter().filter(|o| keep(&o.hash)) {
        let s = span(o);
        if s.end > limit {
            return Err(FormatError::malformed("frame", "object out of range"));
        }
        end = end.max(s.end);
    }
    Ok(end)
}

fn kept_recipe_sources(
    objects: &[FrameObject],
    payload: &[u8],
    keep: &dyn Fn(&Hash) -> bool,
) -> HashSet<Hash> {
    objects
        .iter()
        .filter(|o| o.recipe && keep(&o.hash))
        .filter_map(|o| payload.get(span(o)))
        .flat_map(Recipe::sources_of)
        .map(|e| e.hash)
        .collect()
}

fn frame_objects(
    objects: &[FrameObject],
    payload: &Bytes,
    keep: &dyn Fn(&Hash) -> bool,
) -> Result<DecodedFrame, FormatError> {
    let mut out = DecodedFrame::default();
    for o in objects.iter().filter(|o| keep(&o.hash)) {
        let s = span(o);
        if s.end > payload.len() {
            return Err(FormatError::malformed("frame", "object out of range"));
        }
        let bytes = payload.slice(s);
        if o.recipe {
            out.recipes.insert(o.hash, bytes);
        } else {
            out.objects.insert(o.hash, Verified::check(o.hash, bytes)?);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> (Vec<u8>, Vec<Hash>) {
        let mut f = FrameBuilder::default();
        let objs: Vec<Vec<u8>> = (0..5u8).map(|i| vec![i; 1000 + usize::from(i)]).collect();
        let hashes = objs.iter().map(|o| Hash::of(o)).collect::<Vec<_>>();
        for (o, h) in objs.iter().zip(&hashes) {
            f.push(*h, o, false);
        }
        (f.finish().unwrap(), hashes)
    }

    #[test]
    fn whole_frame_round_trips() {
        let (z, hashes) = sample();
        let d = decode_frame(&z, None).unwrap();
        assert_eq!(d.objects.len(), 5);
        assert!(hashes.iter().all(|h| d.objects.contains_key(h)));
    }

    #[test]
    fn partial_decode_returns_only_kept_objects() {
        let (z, hashes) = sample();
        let want = hashes[1];
        let d = decode_frame(&z, Some(&|h: &Hash| *h == want)).unwrap();
        assert_eq!(d.objects.keys().collect::<Vec<_>>(), vec![&want]);
    }

    #[test]
    fn a_wrong_object_fails_verification() {
        let mut f = FrameBuilder::default();
        f.push(Hash::of(b"claimed"), b"actual", false);
        let z = f.finish().unwrap();
        assert!(matches!(decode_frame(&z, None), Err(FormatError::Integrity(_))));
    }

    #[test]
    fn garbage_is_an_error_not_a_panic() {
        assert!(decode_frame(b"not a frame", None).is_err());
    }
}
