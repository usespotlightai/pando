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
    /// The failure is [`StoreError::Permanent`], as for a request the store refuses; otherwise
    /// it is [`StoreError::Transient`].
    pub permanent: bool,
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
    refused: Vec<StoreOp>,
    latency: Duration,
    slow: Vec<(String, Duration)>,
    versioned: bool,
}

/// An object store in memory, with S3's conditional write rules.
#[derive(Debug)]
pub struct SimStore {
    state: Mutex<State>,
    clock: Arc<dyn Clock>,
    answering: tokio::sync::watch::Sender<bool>,
    held: tokio::sync::watch::Sender<Vec<(StoreOp, String)>>,
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
            answering: tokio::sync::watch::Sender::new(true),
            held: tokio::sync::watch::Sender::new(Vec::new()),
        }
    }

    /// Queues `fault`: the next call of its kind fails, once.
    pub fn inject(&self, fault: Fault) {
        self.state.lock().faults.push_back(fault);
    }

    /// Fails every call of kind `op` with [`StoreError::Permanent`], as a store that refuses
    /// it does, until [`SimStore::accept`].
    pub fn refuse(&self, op: StoreOp) {
        self.state.lock().refused.push(op);
    }

    /// Ends every refusal of [`SimStore::refuse`].
    pub fn accept(&self) {
        self.state.lock().refused.clear();
    }

    /// Makes [`Store::keeps_versions`] answer `on`; a new store keeps no versions.
    pub fn set_versioned(&self, on: bool) {
        self.state.lock().versioned = on;
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

    /// Makes every call on a key that starts with `prefix` take `latency` instead, as calls on
    /// large objects over a slow link do. A key under several such prefixes takes the latency of
    /// the longest.
    pub fn set_latency_under(&self, prefix: &str, latency: Duration) {
        let mut s = self.state.lock();
        s.slow.retain(|(p, _)| p != prefix);
        s.slow.push((prefix.to_string(), latency));
    }

    /// Stops or resumes answering. While stopped, every call waits without failing, as a store
    /// that is down or cut off does; calls in flight finish once it answers again.
    pub fn set_answering(&self, on: bool) {
        self.answering.send_replace(on);
    }

    /// Holds every call of kind `op` on a key that starts with `prefix` and that starts after
    /// this: it waits without failing until [`SimStore::release`], as a call a slow link holds
    /// up does. A call that started before goes on.
    pub fn hold(&self, op: StoreOp, prefix: &str) {
        self.held.send_modify(|h| h.push((op, prefix.to_string())));
    }

    /// Ends the hold of [`SimStore::hold`] on `op` and `prefix`; the calls it held go on.
    pub fn release(&self, op: StoreOp, prefix: &str) {
        self.held
            .send_modify(|h| h.retain(|(o, p)| (*o, p.as_str()) != (op, prefix)));
    }

    async fn round_trip(&self, op: StoreOp, key: &str) {
        let mut answering = self.answering.subscribe();
        let _ = answering.wait_for(|on| *on).await;
        let mut held = self.held.subscribe();
        let _ = held
            .wait_for(|h| !h.iter().any(|(o, p)| *o == op && key.starts_with(p.as_str())))
            .await;
        let latency = {
            let s = self.state.lock();
            s.slow
                .iter()
                .filter(|(p, _)| key.starts_with(p.as_str()))
                .max_by_key(|(p, _)| p.len())
                .map_or(s.latency, |(_, l)| *l)
        };
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
        if state.refused.contains(&op) {
            return Some(Fault {
                op,
                applied: false,
                permanent: true,
            });
        }
        let i = state.faults.iter().position(|f| f.op == op)?;
        state.faults.remove(i)
    }

    fn failure(key: &str, f: Fault) -> StoreError {
        let (key, source) = (key.to_string(), Box::new(Injected));
        if f.permanent {
            StoreError::Permanent { key, source }
        } else {
            StoreError::Transient { key, source }
        }
    }

    fn read(&self, key: &str) -> Result<(Bytes, u64), StoreError> {
        let mut s = self.state.lock();
        if let Some(f) = Self::fault(&mut s, StoreOp::Get) {
            return Err(Self::failure(key, f));
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
        if let Some(f) = fault.filter(|f| !f.applied) {
            return Err(Self::failure(key, f));
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
        if let Some(f) = fault {
            return Err(Self::failure(key, f));
        }
        Ok(Version(version.to_string()))
    }
}

#[async_trait]
impl Store for SimStore {
    async fn get(&self, key: &str) -> Result<Object, StoreError> {
        self.round_trip(StoreOp::Get, key).await;
        let (body, v) = self.read(key)?;
        Ok(Object {
            body,
            version: Version(v.to_string()),
        })
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StoreError> {
        self.round_trip(StoreOp::Get, key).await;
        let (body, _) = self.read(key)?;
        let end = usize_from(range.end).min(body.len());
        let start = usize_from(range.start).min(end);
        Ok(body.slice(start..end))
    }

    async fn get_tail(&self, key: &str, n: u64) -> Result<(Bytes, u64), StoreError> {
        self.round_trip(StoreOp::Get, key).await;
        let (body, _) = self.read(key)?;
        let len = body.len();
        Ok((body.slice(len - usize_from(n).min(len)..), u64_from(len)))
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
        self.round_trip(StoreOp::Put, key).await;
        self.write(StoreOp::Put, key, body, None)
    }

    async fn create(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
        self.round_trip(StoreOp::Create, key).await;
        self.write(StoreOp::Create, key, body, None)
    }

    async fn update(&self, key: &str, body: Bytes, expected: &Version) -> Result<Version, StoreError> {
        self.round_trip(StoreOp::Update, key).await;
        self.write(StoreOp::Update, key, body, Some(expected))
    }

    async fn list(&self, dir: &str) -> Result<Vec<ObjectInfo>, StoreError> {
        self.round_trip(StoreOp::List, dir).await;
        let mut s = self.state.lock();
        if let Some(f) = Self::fault(&mut s, StoreOp::List) {
            return Err(Self::failure(dir, f));
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

    async fn keeps_versions(&self) -> Result<bool, StoreError> {
        Ok(self.state.lock().versioned)
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.round_trip(StoreOp::Delete, key).await;
        let mut s = self.state.lock();
        let fault = Self::fault(&mut s, StoreOp::Delete);
        if let Some(f) = fault.filter(|f| !f.applied) {
            return Err(Self::failure(key, f));
        }
        s.objects.remove(key);
        if let Some(f) = fault {
            return Err(Self::failure(key, f));
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
            permanent: false,
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

    #[tokio::test(start_paused = true)]
    async fn a_key_under_two_slow_prefixes_takes_the_longer_ones_latency() {
        let s = SimStore::new(Arc::new(SimClock::new()));
        s.set_latency(Duration::from_millis(1));
        s.set_latency_under("segments/", Duration::from_millis(500));
        s.set_latency_under("segments/b", Duration::from_millis(30));
        let took = |key: &'static str| {
            let s = &s;
            async move {
                let t0 = tokio::time::Instant::now();
                s.create(key, Bytes::new()).await.unwrap();
                t0.elapsed()
            }
        };
        assert_eq!(took("segments/b/1").await, Duration::from_millis(30));
        assert_eq!(took("segments/a/1").await, Duration::from_millis(500));
        assert_eq!(took("refs/main").await, Duration::from_millis(1));
    }
}
