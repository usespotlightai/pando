use std::fmt;

use crate::FormatError;

/// The version of a store's format, held in the marker object at [`crate::keys::FORMAT`].
///
/// A build reads and writes every store with its own major version. A minor version only adds
/// objects, or lines after the first in text objects, that a build of an older minor version
/// ignores. Any other change to the encoding of a stored object takes a new major version.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct StoreFormat {
    pub major: u32,
    pub minor: u32,
}

impl StoreFormat {
    /// The format this build writes.
    pub const CURRENT: StoreFormat = StoreFormat { major: 1, minor: 0 };
    /// The format of a store with no marker: one written before markers existed.
    pub const UNMARKED: StoreFormat = StoreFormat { major: 1, minor: 0 };

    /// `pando-format <major>.<minor>\n`.
    pub fn encode(&self) -> Vec<u8> {
        format!("pando-format {self}\n").into_bytes()
    }

    /// Decodes the first line; later lines are left to later minor versions.
    pub fn decode(b: &[u8]) -> Result<StoreFormat, FormatError> {
        let bad = |why: &str| FormatError::malformed("store format", why);
        let s = std::str::from_utf8(b).map_err(|_| bad("not UTF-8"))?;
        let first = s.lines().next().unwrap_or_default().trim();
        let version = first
            .strip_prefix("pando-format ")
            .ok_or_else(|| bad("want pando-format <major>.<minor>"))?;
        let (major, minor) = version
            .split_once('.')
            .ok_or_else(|| bad("want <major>.<minor>"))?;
        Ok(StoreFormat {
            major: number(major).ok_or_else(|| bad("major"))?,
            minor: number(minor).ok_or_else(|| bad("minor"))?,
        })
    }

    /// Whether this build can read and write a store in format `store`.
    pub fn reads(&self, store: StoreFormat) -> bool {
        self.major == store.major
    }
}

fn number(s: &str) -> Option<u32> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

impl fmt::Display for StoreFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_format_round_trips() {
        let f = StoreFormat { major: 3, minor: 12 };
        assert_eq!(f.encode(), b"pando-format 3.12\n");
        assert_eq!(StoreFormat::decode(&f.encode()).unwrap(), f);
    }

    #[test]
    fn later_lines_are_ignored() {
        let f = StoreFormat::decode(b"pando-format 1.4\nfeature x\n").unwrap();
        assert_eq!(f, StoreFormat { major: 1, minor: 4 });
    }

    #[test]
    fn malformed_markers_are_rejected() {
        for b in [
            &b""[..],
            b"pando-format 1\n",
            b"pando 1.0\n",
            b"pando-format a.0\n",
            b"pando-format 1.0 x\n",
            b"pando-format 1.0.1\n",
            b"pando-format .0\n",
            b"pando-format 1.\n",
            b"pando-format +1.0\n",
            b"pando-format 1.+0\n",
            b"pando-format -1.0\n",
            b"pando-format 4294967296.0\n",
            b"\xff",
        ] {
            assert!(StoreFormat::decode(b).is_err(), "{b:?}");
        }
    }

    #[test]
    fn only_the_same_major_version_reads() {
        let at = |major, minor| StoreFormat { major, minor };
        assert!(at(1, 0).reads(at(1, 7)));
        assert!(at(1, 7).reads(at(1, 0)));
        assert!(!at(1, 0).reads(at(2, 0)));
        assert!(!at(2, 0).reads(at(1, 0)));
    }
}
