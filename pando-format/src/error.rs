use crate::Hash;

/// A stored object that does not decode.
#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    #[error("malformed {what}: {why}")]
    Malformed { what: &'static str, why: String },
    #[error("{what} has another format")]
    OtherFormat { what: &'static str },
    #[error(transparent)]
    Integrity(#[from] IntegrityError),
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
    pub expected: Hash,
}
