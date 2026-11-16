use std::sync::Arc;

use pando_format::{FormatError, Hash, SegId};

use crate::ports::StoreError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Another writer took the branch: this writer must stop.
    #[error("ref {branch} changed by another writer")]
    Fenced { branch: String },
    /// A chunk no upload published is gone from the cache or changed, so no later upload can
    /// publish the root that holds it.
    #[error("chunk {hash} lost from the local cache; this writer cannot publish the branch")]
    ChunkLost { hash: Hash },
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A write past the end of the volume.
    #[error("write beyond the end of the volume")]
    BeyondEnd,
    #[error("{0}")]
    Corrupt(String),
    #[error("segment {0} already exists")]
    SegmentExists(SegId),
    #[error("ref {0} already exists")]
    RefExists(String),
    /// The caller that led a fetch others waited for gave up.
    #[error("the fetch was cancelled")]
    Cancelled,
    /// The error of a fetch that several readers waited for.
    #[error(transparent)]
    Shared(Arc<Error>),
}

impl Error {
    /// The error itself, through any sharing.
    pub fn kind(&self) -> &Error {
        match self {
            Error::Shared(e) => e.kind(),
            e => e,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
