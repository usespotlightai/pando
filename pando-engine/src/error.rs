use std::sync::Arc;

use pando_format::{FormatError, Hash, SegId, StoreFormat};

use crate::ports::StoreError;

/// Every error the engine returns. Match on [`Error::kind`], which looks through
/// [`Error::Shared`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Another writer took the branch: this writer must stop.
    #[error("ref {branch} changed by another writer")]
    Fenced {
        /// The branch whose ref moved.
        branch: String,
    },
    /// A chunk no upload published is gone from the cache or changed, so no later upload can
    /// publish the root that holds it.
    #[error("chunk {hash} lost from the local cache; this writer cannot publish the branch")]
    ChunkLost {
        /// The hash of the lost chunk.
        hash: Hash,
    },
    /// The store failed a call.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Bytes from the store or a local file did not decode or verify.
    #[error(transparent)]
    Format(#[from] FormatError),
    /// The store's format marker names a format this build does not read.
    #[error("the store is in format {found}; this pando reads format {}.x", StoreFormat::CURRENT.major)]
    UnknownFormat {
        /// The format the marker names.
        found: StoreFormat,
    },
    /// A local file or device failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A write past the end of the volume.
    #[error("write beyond the end of the volume")]
    BeyondEnd,
    /// A grow to a size below the branch's size: branches only grow.
    #[error("the branch has {size} bytes and cannot shrink to {to}")]
    Shrink {
        /// The branch's size in bytes.
        size: u64,
        /// The size asked for.
        to: u64,
    },
    /// State that breaks an invariant the engine relies on, or a request it cannot serve.
    #[error("{0}")]
    Corrupt(String),
    /// A segment upload found the segment's key taken.
    #[error("segment {0} already exists")]
    SegmentExists(SegId),
    /// A branch creation found the branch's ref taken.
    #[error("ref {0} already exists")]
    RefExists(String),
    /// A checkpoint creation found the name taken.
    #[error("checkpoint {0} already exists")]
    CheckpointExists(String),
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
            e @ (Error::Fenced { .. }
            | Error::ChunkLost { .. }
            | Error::Store(_)
            | Error::Format(_)
            | Error::UnknownFormat { .. }
            | Error::Io(_)
            | Error::BeyondEnd
            | Error::Shrink { .. }
            | Error::Corrupt(_)
            | Error::SegmentExists(_)
            | Error::RefExists(_)
            | Error::CheckpointExists(_)
            | Error::Cancelled) => e,
        }
    }

    /// Whether a local disk, or a quota on it, had no room for a write: what a block device
    /// reports as ENOSPC.
    pub fn is_no_room(&self) -> bool {
        matches!(self.kind(), Error::Io(e) if crate::local::no_room(e))
    }
}

/// A result whose error is the engine's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;
