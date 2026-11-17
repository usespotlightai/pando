use crate::Hash;

/// A stored object that does not decode.
#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    /// The bytes are of the expected kind but cut short or inconsistent.
    #[error("malformed {what}: {why}")]
    Malformed {
        /// The kind of object.
        what: &'static str,
        /// What is wrong with it.
        why: String,
    },
    /// The bytes do not start with the kind's magic, so they are another format or version.
    #[error("{what} has another format")]
    OtherFormat {
        /// The kind of object expected.
        what: &'static str,
    },
    /// An object in the bytes does not hash to the hash that names it.
    #[error(transparent)]
    Integrity(#[from] IntegrityError),
    /// Compressing or decompressing a frame failed, or the decompressed frame ended early.
    #[error("zstd: {0}")]
    Compression(#[from] std::io::Error),
}

impl FormatError {
    pub(crate) fn malformed(what: &'static str, why: impl Into<String>) -> Self {
        Self::Malformed {
            what,
            why: why.into(),
        }
    }
}

/// Bytes that do not hash to the content hash that names them.
#[derive(Debug, thiserror::Error)]
#[error("object {expected} failed verification")]
pub struct IntegrityError {
    /// The hash the bytes were expected to have.
    pub expected: Hash,
}
