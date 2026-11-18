use std::fmt;
use std::ops::Range;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;

/// An object version as the store reports it (an `ETag` on S3). Conditional updates name the
/// version they replace.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Version(pub String);

/// An object as [`Store::get`] returns it.
#[derive(Clone, Debug)]
pub struct Object {
    /// The whole object.
    pub body: Bytes,
    /// The version of this body, for a conditional update.
    pub version: Version,
}

/// An object as [`Store::list`] returns it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ObjectInfo {
    /// The key relative to the store's prefix.
    pub key: String,
    /// The size in bytes.
    pub size: u64,
    /// When the object was last written; GC compares it across hosts.
    pub modified: SystemTime,
}

/// How a [`Store`] call failed. Every variant names the key the call was for.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// No object at the key.
    #[error("{key}: not found")]
    NotFound {
        /// The key.
        key: String,
    },
    /// A create found an object at the key; nothing was replaced.
    #[error("{key}: already exists")]
    AlreadyExists {
        /// The key.
        key: String,
    },
    /// A conditional update found another version: another writer moved the object.
    #[error("{key}: changed by another writer")]
    PreconditionFailed {
        /// The key.
        key: String,
    },
    /// May succeed if retried. A write that fails this way may still have taken effect.
    #[error("{key}: {source}")]
    Transient {
        /// The key.
        key: String,
        /// What failed.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Fails again if retried: the store refused the request, or this client cannot make it.
    #[error("{key}: {source}")]
    Permanent {
        /// The key.
        key: String,
        /// What failed.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// The durable store: immutable segments and change lists, and small mutable refs that change
/// only by conditional update. Keys are relative to the store's prefix (see
/// [`pando_format::keys`]). Every S3-compatible store with conditional writes can serve it.
#[async_trait]
pub trait Store: Send + Sync + fmt::Debug {
    /// The whole object at `key`.
    async fn get(&self, key: &str) -> Result<Object, StoreError>;
    /// The bytes in `range` of the object at `key`.
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StoreError>;
    /// The last `n` bytes of an object (all of it when shorter) and the object's size.
    async fn get_tail(&self, key: &str, n: u64) -> Result<(Bytes, u64), StoreError>;
    /// Puts `body` at `key`, replacing any object there, and returns its version.
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
