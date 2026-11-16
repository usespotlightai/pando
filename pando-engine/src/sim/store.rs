use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;

use crate::ports::{Clock, Object, ObjectInfo, Store, StoreError, Version};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreOp {
    Get,
    Put,
    Create,
    Update,
    List,
    Delete,
}

/// A failure the next matching call returns. With `applied`, a write takes effect and still
/// reports the failure, as a store does when the reply is lost.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Fault {
    pub op: StoreOp,
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
    pub fn new(clock: Arc<dyn Clock>) -> SimStore {
        SimStore {
            state: Mutex::default(),
            clock,
        }
    }

    pub fn inject(&self, fault: Fault) {
        self.state.lock().expect("sim store lock").faults.push_back(fault);
    }

    /// Sets the modification time of `key`, for GC age tests.
    pub fn set_modified(&self, key: &str, at: SystemTime) {
        if let Some(o) = self.state.lock().expect("sim store lock").objects.get_mut(key) {
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
        let mut s = self.state.lock().expect("sim store lock");
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
        let mut s = self.state.lock().expect("sim store lock");
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
        let (body, v) = self.read(key)?;
        Ok(Object {
            body,
            version: Version(v.to_string()),
        })
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StoreError> {
        let (body, _) = self.read(key)?;
        let end = range.end.min(body.len() as u64) as usize;
        let start = (range.start as usize).min(end);
        Ok(body.slice(start..end))
    }

    async fn get_tail(&self, key: &str, n: u64) -> Result<(Bytes, u64), StoreError> {
        let (body, _) = self.read(key)?;
        let size = body.len() as u64;
        Ok((body.slice((size - n.min(size)) as usize..), size))
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
        self.write(StoreOp::Put, key, body, None)
    }

    async fn create(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
        self.write(StoreOp::Create, key, body, None)
    }

    async fn update(&self, key: &str, body: Bytes, expected: &Version) -> Result<Version, StoreError> {
        self.write(StoreOp::Update, key, body, Some(expected))
    }

    async fn list(&self, dir: &str) -> Result<Vec<ObjectInfo>, StoreError> {
        let mut s = self.state.lock().expect("sim store lock");
        if Self::fault(&mut s, StoreOp::List).is_some() {
            return Err(Self::transient(dir));
        }
        let prefix = format!("{}/", dir.trim_end_matches('/'));
        Ok(s.objects
            .range(prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix))
            .map(|(k, o)| ObjectInfo {
                key: k.clone(),
                size: o.body.len() as u64,
                modified: o.modified,
            })
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let mut s = self.state.lock().expect("sim store lock");
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
