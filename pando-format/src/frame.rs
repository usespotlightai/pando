use std::collections::{HashMap, HashSet};
use std::io::Read;

use bytes::Bytes;

use crate::{FormatError, Hash, Recipe, Verified};

/// The most payload a writer puts in one frame. One GET of a frame fills the cache with
/// every neighbour in it.
pub const FRAME_MAX: usize = 1 << 20;
/// The zstd level frames are written with.
pub const ZSTD_LEVEL: i32 = 3;

const RECIPE_FLAG: u32 = 1 << 31;
const HEADER_ENTRY: usize = 40;
const HEADER_MAX: usize = 16 << 20;

/// One object in a frame: its content hash, where it sits in the payload, and whether the
/// bytes are a recipe of the chunk (whose hash it still is) rather than the chunk itself.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FrameObject {
    pub hash: Hash,
    pub inner: u32,
    pub len: u32,
    pub recipe: bool,
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
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }

    /// Whether `n` more bytes would take a non-empty frame past [`FRAME_MAX`].
    pub fn is_full_for(&self, n: usize) -> bool {
        !self.payload.is_empty() && self.payload.len() + n > FRAME_MAX
    }

    /// Appends an object and returns its offset and length in the payload.
    pub fn push(&mut self, hash: Hash, bytes: &[u8], recipe: bool) -> (u32, u32) {
        let inner = self.payload.len() as u32;
        let len = bytes.len() as u32;
        self.objects.push(FrameObject {
            hash,
            inner,
            len,
            recipe,
        });
        self.payload.extend_from_slice(bytes);
        (inner, len)
    }

    pub fn finish(self) -> Result<Vec<u8>, FormatError> {
        encode_frame(&self.objects, &self.payload)
    }
}

pub fn encode_frame(objects: &[FrameObject], payload: &[u8]) -> Result<Vec<u8>, FormatError> {
    let mut raw = Vec::with_capacity(4 + objects.len() * HEADER_ENTRY + payload.len());
    raw.extend_from_slice(&(objects.len() as u32).to_le_bytes());
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
    pub objects: HashMap<Hash, Verified>,
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
        .filter(|&n| n <= HEADER_MAX && size.is_none_or(|s| (4 + n) as u64 <= s))
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
    match (keep, size) {
        (Some(keep), Some(size)) => {
            let limit = (size - 4 - header as u64) as usize;
            let end = payload_end(&objects, &|h| keep(h), limit)?;
            payload.resize(end, 0);
            dec.read_exact(&mut payload)?;
            sources = kept_recipe_sources(&objects, &payload, keep);
            if !sources.is_empty() {
                let end = payload_end(&objects, &|h| keep(h) || sources.contains(h), limit)?;
                let have = payload.len();
                if end > have {
                    payload.resize(end, 0);
                    dec.read_exact(&mut payload[have..])?;
                }
            }
        }
        _ => {
            dec.read_to_end(&mut payload)?;
            if let Some(keep) = keep {
                sources = kept_recipe_sources(&objects, &payload, keep);
            }
        }
    }
    let keep = |h: &Hash| keep.is_none_or(|k| k(h)) || sources.contains(h);
    frame_objects(&objects, Bytes::from(payload), &keep)
}

fn header_entry(e: &[u8; HEADER_ENTRY]) -> FrameObject {
    let u32_at = |at: usize| u32::from_le_bytes(e[at..at + 4].try_into().expect("4 bytes"));
    let len = u32_at(36);
    FrameObject {
        hash: Hash(e[..32].try_into().expect("32 bytes")),
        inner: u32_at(32),
        len: len & !RECIPE_FLAG,
        recipe: len & RECIPE_FLAG != 0,
    }
}

fn span(o: &FrameObject) -> std::ops::Range<usize> {
    o.inner as usize..o.inner as usize + o.len as usize
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
    payload: Bytes,
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
