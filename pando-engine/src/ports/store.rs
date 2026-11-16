use std::fmt;
use std::ops::Range;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;

/// An object version as the store reports it (an ETag on S3). Conditional updates name the
/// version they replace.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Version(pub String);

#[derive(Clone, Debug)]
pub struct Object {
    pub body: Bytes,
    pub version: Version,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ObjectInfo {
    /// The key relative to the store's prefix.
    pub key: String,
    pub size: u64,
    pub modified: SystemTime,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("{key}: not found")]
    NotFound { key: String },
    /// A create found an object at the key; nothing was replaced.
    #[error("{key}: already exists")]
    AlreadyExists { key: String },
    /// A conditional update found another version: another writer moved the object.
    #[error("{key}: changed by another writer")]
    PreconditionFailed { key: String },
    /// May succeed if retried. A write that fails this way may still have taken effect.
    #[error("{key}: {source}")]
    Transient {
        key: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("{key}: {source}")]
    Other {
        key: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// The durable store: immutable segments and change lists, and small mutable refs that change
/// only by conditional update. Keys are relative to the store's prefix (see
/// [`pando_format::keys`]). Every S3-compatible store with conditional writes can serve it.
#[async_trait]
pub trait Store: Send + Sync + fmt::Debug {
    async fn get(&self, key: &str) -> Result<Object, StoreError>;
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StoreError>;
    /// The last `n` bytes of an object (all of it when shorter) and the object's size.
    async fn get_tail(&self, key: &str, n: u64) -> Result<(Bytes, u64), StoreError>;
    async fn put(&self, key: &str, body: Bytes) -> Result<Version, StoreError>;
    /// Puts only if no object exists at `key`.
    async fn create(&self, key: &str, body: Bytes) -> Result<Version, StoreError>;
    /// Puts only if the object still has version `expected`.
    async fn update(&self, key: &str, body: Bytes, expected: &Version) -> Result<Version, StoreError>;
    /// Every object in folder `dir` and below it (such as `segments`), in key order.
    async fn list(&self, dir: &str) -> Result<Vec<ObjectInfo>, StoreError>;
    /// Deleting a missing object is not an error.
    async fn delete(&self, key: &str) -> Result<(), StoreError>;
}
