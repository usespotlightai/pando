//! Pando's [`Store`] port over any S3-compatible object store, through `object_store`.
//!
//! Refs need conditional writes: `If-None-Match: *` for create and `If-Match: <etag>` for
//! update. AWS S3, Cloudflare R2 and MinIO support both.

#![forbid(unsafe_code)]

use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{
    ClientOptions, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    UpdateVersion,
};
use pando_engine::ports::{Object, ObjectInfo, Store, StoreError, Version};
use tokio::sync::{OnceCell, oneshot};

/// The longest one request may take. A segment PUT shares the uplink with the other PUTs in
/// flight, so on a slow link it takes minutes.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

/// Where the store is and how to sign in. Credentials come from the caller, never from a file
/// this crate reads.
#[derive(Clone, Default)]
pub struct S3Config {
    pub bucket: String,
    /// Keys go under this prefix; empty for the bucket root.
    pub prefix: String,
    /// The endpoint URL for a store other than AWS S3, such as R2 or MinIO.
    pub endpoint: Option<String>,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Allows a plain-HTTP endpoint, for a local MinIO.
    pub allow_http: bool,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct S3Store {
    inner: Client,
    prefix: String,
}

type Built = Result<Arc<dyn ObjectStore>, String>;

/// The `object_store` client. [`S3Store::new`] builds it on a thread of its own: the build
/// loads the system's root certificates, which takes tens of milliseconds the caller can spend
/// on other work, such as opening its cache. The first request waits for it.
#[derive(Debug)]
struct Client {
    ready: OnceCell<Built>,
    building: Mutex<Option<oneshot::Receiver<Built>>>,
}

impl Client {
    async fn get(&self, key: &str) -> Result<&Arc<dyn ObjectStore>, StoreError> {
        let built = self
            .ready
            .get_or_init(|| async {
                let rx = self.building.lock().expect("client lock").take();
                match rx {
                    Some(rx) => rx
                        .await
                        .unwrap_or_else(|_| Err("the store client build stopped".to_string())),
                    None => Err("the store client build stopped".to_string()),
                }
            })
            .await;
        built.as_ref().map_err(|e| StoreError::Other {
            key: key.to_string(),
            source: e.clone().into(),
        })
    }
}

impl S3Store {
    pub fn new(c: &S3Config) -> Result<S3Store, StoreError> {
        let mut b = AmazonS3Builder::new()
            .with_bucket_name(&c.bucket)
            .with_region(&c.region)
            .with_access_key_id(&c.access_key_id)
            .with_secret_access_key(&c.secret_access_key)
            .with_allow_http(c.allow_http)
            .with_client_options(ClientOptions::new().with_timeout(REQUEST_TIMEOUT))
            .with_conditional_put(S3ConditionalPut::ETagMatch);
        if let Some(e) = &c.endpoint {
            b = b.with_endpoint(e);
        }
        let (tx, rx) = oneshot::channel();
        let bucket = c.bucket.clone();
        std::thread::Builder::new()
            .name("s3-client".into())
            .spawn(move || {
                let built = b
                    .build()
                    .map(|s| Arc::new(s) as Arc<dyn ObjectStore>)
                    .map_err(|e| map_err(&bucket, e).to_string());
                let _ = tx.send(built);
            })
            .map_err(|e| StoreError::Other {
                key: c.bucket.clone(),
                source: Box::new(e),
            })?;
        Ok(S3Store {
            inner: Client {
                ready: OnceCell::new(),
                building: Mutex::new(Some(rx)),
            },
            prefix: c.prefix.trim_matches('/').to_string(),
        })
    }

    /// The port over another `object_store` backend, such as its in-memory store in tests.
    pub fn over(inner: Arc<dyn ObjectStore>, prefix: &str) -> S3Store {
        S3Store {
            inner: Client {
                ready: OnceCell::new_with(Some(Ok(inner))),
                building: Mutex::new(None),
            },
            prefix: prefix.trim_matches('/').to_string(),
        }
    }

    fn path(&self, key: &str) -> Path {
        match self.prefix.as_str() {
            "" => Path::from(key),
            p => Path::from(format!("{p}/{key}")),
        }
    }

    fn relative(&self, p: &Path) -> String {
        let p = p.as_ref();
        match self.prefix.as_str() {
            "" => p.to_string(),
            pre => p
                .strip_prefix(pre)
                .and_then(|r| r.strip_prefix('/'))
                .unwrap_or(p)
                .to_string(),
        }
    }

    async fn put_mode(&self, key: &str, body: Bytes, mode: PutMode) -> Result<Version, StoreError> {
        let opts = PutOptions {
            mode,
            ..Default::default()
        };
        let r = self
            .inner
            .get(key)
            .await?
            .put_opts(&self.path(key), PutPayload::from(body), opts)
            .await
            .map_err(|e| map_err(key, e))?;
        Ok(Version(r.e_tag.unwrap_or_default()))
    }
}

fn map_err(key: &str, e: object_store::Error) -> StoreError {
    let key = key.to_string();
    match e {
        object_store::Error::NotFound { .. } => StoreError::NotFound { key },
        object_store::Error::AlreadyExists { .. } => StoreError::AlreadyExists { key },
        object_store::Error::Precondition { .. } => StoreError::PreconditionFailed { key },
        e @ object_store::Error::Generic { .. } => StoreError::Transient {
            key,
            source: Box::new(e),
        },
        e => StoreError::Other {
            key,
            source: Box::new(e),
        },
    }
}

#[async_trait]
impl Store for S3Store {
    async fn get(&self, key: &str) -> Result<Object, StoreError> {
        let r = self
            .inner
            .get(key)
            .await?
            .get(&self.path(key))
            .await
            .map_err(|e| map_err(key, e))?;
        let version = Version(r.meta.e_tag.clone().unwrap_or_default());
        let body = r.bytes().await.map_err(|e| map_err(key, e))?;
        Ok(Object { body, version })
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StoreError> {
        self.inner
            .get(key)
            .await?
            .get_range(&self.path(key), range)
            .await
            .map_err(|e| map_err(key, e))
    }

    async fn get_tail(&self, key: &str, n: u64) -> Result<(Bytes, u64), StoreError> {
        let opts = GetOptions {
            range: Some(GetRange::Suffix(n)),
            ..Default::default()
        };
        let r = self
            .inner
            .get(key)
            .await?
            .get_opts(&self.path(key), opts)
            .await
            .map_err(|e| map_err(key, e))?;
        let size = r.meta.size;
        let body = r.bytes().await.map_err(|e| map_err(key, e))?;
        Ok((body, size))
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
        self.put_mode(key, body, PutMode::Overwrite).await
    }

    async fn create(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
        self.put_mode(key, body, PutMode::Create).await
    }

    async fn update(&self, key: &str, body: Bytes, expected: &Version) -> Result<Version, StoreError> {
        let v = UpdateVersion {
            e_tag: Some(expected.0.clone()),
            version: None,
        };
        self.put_mode(key, body, PutMode::Update(v)).await
    }

    async fn list(&self, dir: &str) -> Result<Vec<ObjectInfo>, StoreError> {
        let mut out: Vec<ObjectInfo> = self
            .inner
            .get(dir)
            .await?
            .list(Some(&self.path(dir.trim_end_matches('/'))))
            .map_ok(|m| ObjectInfo {
                key: self.relative(&m.location),
                size: m.size,
                modified: SystemTime::from(m.last_modified),
            })
            .try_collect()
            .await
            .map_err(|e| map_err(dir, e))?;
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        match self.inner.get(key).await?.delete(&self.path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(map_err(key, e)),
        }
    }
}
