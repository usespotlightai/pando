//! The store's format marker: checked when a command opens the store, written by the first
//! command that writes to it.

use bytes::Bytes;
use pando_format::{StoreFormat, keys};

use crate::ports::{Store, StoreError};
use crate::{Error, Result};

/// Whether a command may write to the store, and so may write its format marker.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Access {
    /// The command only reads the store.
    Read,
    /// The command may write the store.
    Write,
}

/// The store's format, once this build is known to read it. A store without a marker is in
/// [`StoreFormat::UNMARKED`]; with [`Access::Write`] the marker of [`StoreFormat::CURRENT`] is
/// written to it first. Fails with [`Error::UnknownFormat`] for a format this build does not read.
pub async fn check_format(store: &dyn Store, access: Access) -> Result<StoreFormat> {
    let unmarked = match store.get(keys::FORMAT).await {
        Ok(o) => return readable(StoreFormat::decode(&o.body)?),
        Err(StoreError::NotFound { .. }) => readable(StoreFormat::UNMARKED)?,
        Err(e) => return Err(e.into()),
    };
    if access == Access::Read {
        return Ok(unmarked);
    }
    let marker = Bytes::from(StoreFormat::CURRENT.encode());
    match store.create(keys::FORMAT, marker).await {
        Ok(_) => Ok(StoreFormat::CURRENT),
        Err(StoreError::AlreadyExists { .. }) => {
            readable(StoreFormat::decode(&store.get(keys::FORMAT).await?.body)?)
        }
        Err(e) => Err(e.into()),
    }
}

fn readable(found: StoreFormat) -> Result<StoreFormat> {
    if StoreFormat::CURRENT.reads(found) {
        Ok(found)
    } else {
        Err(Error::UnknownFormat { found })
    }
}

#[cfg(all(test, feature = "sim"))]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::ports::{Object, ObjectInfo, Version};
    use crate::sim::{SimClock, SimStore};

    fn store() -> Arc<SimStore> {
        Arc::new(SimStore::new(Arc::new(SimClock::new())))
    }

    async fn marker(s: &SimStore) -> Option<Bytes> {
        s.get(keys::FORMAT).await.ok().map(|o| o.body)
    }

    #[tokio::test]
    async fn a_write_marks_an_unmarked_store_and_a_read_does_not() {
        let s = store();
        assert_eq!(
            check_format(s.as_ref(), Access::Read).await.unwrap(),
            StoreFormat::UNMARKED
        );
        assert_eq!(marker(&s).await, None);
        assert_eq!(
            check_format(s.as_ref(), Access::Write).await.unwrap(),
            StoreFormat::CURRENT
        );
        assert_eq!(marker(&s).await.unwrap(), StoreFormat::CURRENT.encode());
        assert_eq!(
            check_format(s.as_ref(), Access::Read).await.unwrap(),
            StoreFormat::CURRENT
        );
    }

    #[tokio::test]
    async fn a_later_minor_version_is_read() {
        let s = store();
        let later = StoreFormat {
            minor: StoreFormat::CURRENT.minor + 1,
            ..StoreFormat::CURRENT
        };
        s.put(keys::FORMAT, later.encode().into()).await.unwrap();
        for access in [Access::Read, Access::Write] {
            assert_eq!(check_format(s.as_ref(), access).await.unwrap(), later);
        }
    }

    #[tokio::test]
    async fn another_major_version_is_refused_and_left_alone() {
        let s = store();
        let next = StoreFormat {
            major: StoreFormat::CURRENT.major + 1,
            minor: 0,
        };
        s.put(keys::FORMAT, next.encode().into()).await.unwrap();
        for access in [Access::Read, Access::Write] {
            let e = check_format(s.as_ref(), access).await.unwrap_err();
            assert!(
                matches!(e, Error::UnknownFormat { found } if found == next),
                "{e}"
            );
        }
        assert_eq!(marker(&s).await.unwrap(), next.encode());
    }

    #[tokio::test]
    async fn a_malformed_marker_is_an_error() {
        let s = store();
        s.put(keys::FORMAT, Bytes::from_static(b"junk\n")).await.unwrap();
        assert!(matches!(
            check_format(s.as_ref(), Access::Write).await,
            Err(Error::Format(_))
        ));
    }

    /// A store where another writer creates the marker between this writer's GET and create.
    #[derive(Debug)]
    struct Racing {
        inner: Arc<SimStore>,
        theirs: StoreFormat,
        raced: AtomicBool,
    }

    #[async_trait]
    impl Store for Racing {
        async fn get(&self, key: &str) -> Result<Object, StoreError> {
            if key == keys::FORMAT && !self.raced.swap(true, Ordering::SeqCst) {
                self.inner.put(key, self.theirs.encode().into()).await?;
                return Err(StoreError::NotFound { key: key.to_string() });
            }
            self.inner.get(key).await
        }
        async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StoreError> {
            self.inner.get_range(key, range).await
        }
        async fn get_tail(&self, key: &str, n: u64) -> Result<(Bytes, u64), StoreError> {
            self.inner.get_tail(key, n).await
        }
        async fn put(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
            self.inner.put(key, body).await
        }
        async fn create(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
            self.inner.create(key, body).await
        }
        async fn update(&self, key: &str, body: Bytes, expected: &Version) -> Result<Version, StoreError> {
            self.inner.update(key, body, expected).await
        }
        async fn list(&self, dir: &str) -> Result<Vec<ObjectInfo>, StoreError> {
            self.inner.list(dir).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
        async fn delete_if(&self, key: &str, expected: &Version) -> Result<(), StoreError> {
            self.inner.delete_if(key, expected).await
        }
        async fn keeps_versions(&self) -> Result<bool, StoreError> {
            self.inner.keeps_versions().await
        }
    }

    #[tokio::test]
    async fn a_writer_that_loses_the_race_to_mark_checks_the_winners_marker() {
        let next = StoreFormat {
            major: StoreFormat::CURRENT.major + 1,
            minor: 0,
        };
        let s = Racing {
            inner: store(),
            theirs: next,
            raced: AtomicBool::new(false),
        };
        let e = check_format(&s, Access::Write).await.unwrap_err();
        assert!(
            matches!(e, Error::UnknownFormat { found } if found == next),
            "{e}"
        );
        assert_eq!(marker(&s.inner).await.unwrap(), next.encode());
    }
}
