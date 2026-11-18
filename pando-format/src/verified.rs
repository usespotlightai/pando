use std::fmt;
use std::ops::Deref;

use bytes::Bytes;

use crate::{Hash, IntegrityError};

/// Bytes known to hash to [`Verified::hash`]. Only a hash check creates one, so code that takes
/// a `Verified` cannot get unchecked bytes from the store.
#[derive(Clone, PartialEq, Eq)]
pub struct Verified {
    hash: Hash,
    bytes: Bytes,
}

impl Verified {
    /// `bytes` as verified, when they hash to `hash`.
    pub fn check(hash: Hash, bytes: Bytes) -> Result<Verified, IntegrityError> {
        if Hash::of(&bytes) == hash {
            Ok(Verified { hash, bytes })
        } else {
            Err(IntegrityError { expected: hash })
        }
    }

    /// Names bytes this host made by their hash.
    pub fn of(bytes: Bytes) -> Verified {
        Verified {
            hash: Hash::of(&bytes),
            bytes,
        }
    }

    /// The hash of the bytes.
    pub fn hash(&self) -> Hash {
        self.hash
    }

    /// The verified bytes.
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// The verified bytes, giving up the proof.
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

impl Deref for Verified {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

impl fmt::Debug for Verified {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Verified({}, {} bytes)", self.hash.short(), self.bytes.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_accepts_matching_bytes_only() {
        let b = Bytes::from_static(b"chunk");
        assert!(Verified::check(Hash::of(b"chunk"), b.clone()).is_ok());
        assert!(Verified::check(Hash::of(b"other"), b).is_err());
    }
}
