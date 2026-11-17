use std::fmt;
use std::str::FromStr;

use crate::FormatError;

/// The SHA-256 of an object's content. It names chunks and tree nodes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Hash(pub [u8; 32]);

impl Hash {
    /// The hash of nothing: an [`Entry`](crate::Entry) with it is empty.
    pub const ZERO: Hash = Hash([0; 32]);

    pub fn of(b: &[u8]) -> Hash {
        let d = ring::digest::digest(&ring::digest::SHA256, b);
        Hash(d.as_ref().try_into().expect("SHA-256 is 32 bytes"))
    }

    pub fn is_zero(&self) -> bool {
        *self == Self::ZERO
    }

    /// The first 8 hex digits, for logs.
    pub fn short(&self) -> String {
        hex::encode(&self.0[..4])
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({})", self.short())
    }
}

impl FromStr for Hash {
    type Err = FormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut h = [0; 32];
        hex::decode_to_slice(s.trim(), &mut h).map_err(|e| FormatError::malformed("hash", e.to_string()))?;
        Ok(Hash(h))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_of_empty_input_is_the_known_sha256() {
        assert_eq!(
            Hash::of(b"").to_string(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hex_round_trips_and_rejects_short_input() {
        let h = Hash::of(b"pando");
        assert_eq!(h.to_string().parse::<Hash>().unwrap(), h);
        assert!("abcd".parse::<Hash>().is_err());
    }
}
