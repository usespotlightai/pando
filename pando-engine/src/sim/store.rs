use parking_lot::Mutex;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;

use pando_format::{u64_from, usize_from};

use crate::ports::{Clock, Object, ObjectInfo, Store, StoreError, Version};

/// The calls a [`Fault`] can fail.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreOp {
    /// [`Store::get`], [`Store::get_range`] and [`Store::get_tail`].
    Get,
    /// [`Store::put`].
    Put,
    /// [`Store::create`].
    Create,
    /// [`Store::update`].
    Update,
    /// [`Store::list`].
    List,
    /// [`Store::delete`].
    Delete,
}

/// A failure the next matching call returns. With `applied`, a write takes effect and still
/// reports the failure, as a store does when the reply is lost.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Fault {
    /// The call that fails.
    pub op: StoreOp,
    /// The write takes effect before the failure.
    pub applied: bool,
}

#[derive(Debug)]
struct Stored {
    body: Bytes,
    version: u64,
    modified: SystemTime,
}

#[derive(Debug, Default)]
struct State {
    objects: BTreeMap<String, Stored>,
    next_version: u64,
    faults: VecDeque<Fault>,
    latency: Duration,
}

/// An object store in memory, with S3's conditional write rules.
#[derive(Debug)]
pub struct SimStore {
    state: Mutex<State>,
    clock: Arc<dyn Clock>,
}

#[derive(Debug, thiserror::Error)]
#[error("injected fault")]
struct Injected;

impl SimStore {
    /// An empty store that stamps objects with `clock`'s wall time.
    pub fn new(clock: Arc<dyn Clock>) -> SimStore {
        SimStore {
            state: Mutex::default(),
            clock,
        }
    }

    /// Queues `fault`: the next call of its kind fails, once.
    pub fn inject(&self, fault: Fault) {
        self.state.lock().faults.push_back(fault);
    }

    /// Drops every injected fault no call has returned yet.
    pub fn clear_faults(&self) {
        self.state.lock().faults.clear();
    }

    /// The keys of every stored object under folder `dir`.
    pub fn keys(&self, dir: &str) -> Vec<String> {
        let prefix = format!("{}/", dir.trim_end_matches('/'));
        let s = self.state.lock();
        s.objects
            .range(prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Makes every call take `latency` of `tokio::time` before it acts, as a round trip does.
    pub fn set_latency(&self, latency: Duration) {
        self.state.lock().latency = latency;
    }

    async fn round_trip(&self) {
        let latency = self.state.lock().latency;
        if !latency.is_zero() {
            tokio::time::sleep(latency).await;
        }
    }

    /// Sets the modification time of `key`, for GC age tests.
    pub fn set_modified(&self, key: &str, at: SystemTime) {
        if let Some(o) = self.state.lock().objects.get_mut(key) {
            o.modified = at;
        }
    }

    fn fault(state: &mut State, op: StoreOp) -> Option<Fault> {
        let i = state.faults.iter().position(|f| f.op == op)?;
        state.faults.remove(i)
    }

    fn transient(key: &str) -> StoreError {
        StoreError::Transient {
            key: key.to_string(),
            source: Box::new(Injected),
        }
    }

    fn read(&self, key: &str) -> Result<(Bytes, u64), StoreError> {
        let mut s = self.state.lock();
        if Self::fault(&mut s, StoreOp::Get).is_some() {
            return Err(Self::transient(key));
        }
        s.objects
            .get(key)
            .map(|o| (o.body.clone(), o.version))
            .ok_or_else(|| StoreError::NotFound { key: key.to_string() })
    }

    fn write(
        &self,
        op: StoreOp,
        key: &str,
        body: Bytes,
        expected: Option<&Version>,
    ) -> Result<Version, StoreError> {
        let mut s = self.state.lock();
        let fault = Self::fault(&mut s, op);
        if fault.is_some_and(|f| !f.applied) {
            return Err(Self::transient(key));
        }
        let current = s.objects.get(key).map(|o| o.version);
        match (op, current, expected) {
            (StoreOp::Create, Some(_), _) => return Err(StoreError::AlreadyExists { key: key.to_string() }),
            (StoreOp::Update, None, _) => return Err(StoreError::NotFound { key: key.to_string() }),
            (StoreOp::Update, Some(v), Some(want)) if v.to_string() != want.0 => {
                return Err(StoreError::PreconditionFailed { key: key.to_string() });
            }
            _ => {}
        }
        s.next_version += 1;
        let version = s.next_version;
        s.objects.insert(
            key.to_string(),
            Stored {
                body,
                version,
                modified: self.clock.wall(),
            },
        );
        if fault.is_some() {
            return Err(Self::transient(key));
        }
        Ok(Version(version.to_string()))
    }
}

#[async_trait]
impl Store for SimStore {
    async fn get(&self, key: &str) -> Result<Object, StoreError> {
        self.round_trip().await;
        let (body, v) = self.read(key)?;
        Ok(Object {
            body,
            version: Version(v.to_string()),
        })
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StoreError> {
        self.round_trip().await;
        let (body, _) = self.read(key)?;
        let end = usize_from(range.end).min(body.len());
        let start = usize_from(range.start).min(end);
        Ok(body.slice(start..end))
    }

    async fn get_tail(&self, key: &str, n: u64) -> Result<(Bytes, u64), StoreError> {
        self.round_trip().await;
        let (body, _) = self.read(key)?;
        let len = body.len();
        Ok((body.slice(len - usize_from(n).min(len)..), u64_from(len)))
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
        self.round_trip().await;
        self.write(StoreOp::Put, key, body, None)
    }

    async fn create(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
        self.round_trip().await;
        self.write(StoreOp::Create, key, body, None)
    }

    async fn update(&self, key: &str, body: Bytes, expected: &Version) -> Result<Version, StoreError> {
        self.round_trip().await;
        self.write(StoreOp::Update, key, body, Some(expected))
    }

    async fn list(&self, dir: &str) -> Result<Vec<ObjectInfo>, StoreError> {
        self.round_trip().await;
        let mut s = self.state.lock();
        if Self::fault(&mut s, StoreOp::List).is_some() {
            return Err(Self::transient(dir));
        }
        let prefix = format!("{}/", dir.trim_end_matches('/'));
        Ok(s.objects
            .range(prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix))
            .map(|(k, o)| ObjectInfo {
                key: k.clone(),
                size: u64_from(o.body.len()),
                modified: o.modified,
            })
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.round_trip().await;
        let mut s = self.state.lock();
        let fault = Self::fault(&mut s, StoreOp::Delete);
        if fault.is_some_and(|f| !f.applied) {
            return Err(Self::transient(key));
        }
        s.objects.remove(key);
        if fault.is_some() {
            return Err(Self::transient(key));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::SimClock;

    #[tokio::test]
    async fn an_applied_fault_writes_and_still_fails() {
        let s = SimStore::new(Arc::new(SimClock::new()));
        s.inject(Fault {
            op: StoreOp::Create,
            applied: true,
        });
        assert!(matches!(
            s.create("k", Bytes::from_static(b"v")).await,
            Err(StoreError::Transient { .. })
        ));
        assert!(matches!(
            s.create("k", Bytes::new()).await,
            Err(StoreError::AlreadyExists { .. })
        ));
    }
}
