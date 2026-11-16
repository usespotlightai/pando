use std::fmt;

use crate::{FormatError, Hash, SegId};

pub const ENTRY_SIZE: usize = 32 + 16 + 4 + 4 + 4;

/// Where an object lives remotely: in frame `frame` of segment `seg`, at `inner` in the
/// decompressed frame payload. A zero segment means the object is not uploaded yet and exists
/// only in the local cache of the host that wrote it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct Loc {
    pub seg: SegId,
    pub frame: u32,
    pub inner: u32,
    pub len: u32,
}

impl Loc {
    pub fn is_pending(&self) -> bool {
        self.seg.is_zero()
    }
}

/// An object's content hash and where to fetch it. A zero hash means no object.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct Entry {
    pub hash: Hash,
    pub loc: Loc,
}

impl Entry {
    pub const EMPTY: Entry = Entry {
        hash: Hash::ZERO,
        loc: Loc {
            seg: SegId::ZERO,
            frame: 0,
            inner: 0,
            len: 0,
        },
    };

    pub fn is_empty(&self) -> bool {
        self.hash.is_zero()
    }

    pub fn to_bytes(&self) -> [u8; ENTRY_SIZE] {
        let mut b = [0; ENTRY_SIZE];
        b[..32].copy_from_slice(&self.hash.0);
        b[32..48].copy_from_slice(&self.loc.seg.0);
        b[48..52].copy_from_slice(&self.loc.frame.to_le_bytes());
        b[52..56].copy_from_slice(&self.loc.inner.to_le_bytes());
        b[56..60].copy_from_slice(&self.loc.len.to_le_bytes());
        b
    }

    pub fn from_bytes(b: &[u8; ENTRY_SIZE]) -> Entry {
        let u32_at = |at: usize| u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]]);
        let mut hash = [0; 32];
        hash.copy_from_slice(&b[..32]);
        let mut seg = [0; 16];
        seg.copy_from_slice(&b[32..48]);
        Entry {
            hash: Hash(hash),
            loc: Loc {
                seg: SegId(seg),
                frame: u32_at(48),
                inner: u32_at(52),
                len: u32_at(56),
            },
        }
    }

    pub(crate) fn read(b: &[u8], at: usize) -> Option<Entry> {
        b.get(at..at + ENTRY_SIZE)
            .map(|s| Entry::from_bytes(s.try_into().expect("slice of ENTRY_SIZE")))
    }

    pub fn parse_text(s: &str) -> Result<Entry, FormatError> {
        let bad = |why: &str| FormatError::malformed("entry", why.to_string());
        let f: Vec<&str> = s.split_whitespace().collect();
        let [h, seg, frame, inner, len] = f[..] else {
            return Err(bad("want 5 fields"));
        };
        let num = |v: &str| v.parse::<u32>().map_err(|e| bad(&e.to_string()));
        Ok(Entry {
            hash: h.parse()?,
            loc: Loc {
                seg: seg.parse()?,
                frame: num(frame)?,
                inner: num(inner)?,
                len: num(len)?,
            },
        })
    }
}

/// The text form in refs: `<hash> <segment> <frame> <inner> <len>`.
impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let l = &self.loc;
        write!(f, "{} {} {} {} {}", self.hash, l.seg, l.frame, l.inner, l.len)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn entry() -> impl Strategy<Value = Entry> {
        (
            any::<[u8; 32]>(),
            any::<[u8; 16]>(),
            any::<u32>(),
            any::<u32>(),
            any::<u32>(),
        )
            .prop_map(|(h, s, frame, inner, len)| Entry {
                hash: Hash(h),
                loc: Loc {
                    seg: SegId(s),
                    frame,
                    inner,
                    len,
                },
            })
    }

    proptest! {
        #[test]
        fn binary_and_text_forms_round_trip(e in entry()) {
            prop_assert_eq!(Entry::from_bytes(&e.to_bytes()), e);
            prop_assert_eq!(Entry::parse_text(&e.to_string()).unwrap(), e);
        }
    }
}
