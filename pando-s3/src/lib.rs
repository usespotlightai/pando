//! Pando's [`Store`] port over any S3-compatible object store, through `object_store`.
//!
//! Refs need conditional writes: `If-None-Match: *` for create and `If-Match: <etag>` for
//! update. AWS S3, Cloudflare R2 and `MinIO` support both.

#![forbid(unsafe_code)]

use parking_lot::Mutex;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService,
    ReqwestConnector,
};
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
    /// The bucket the store lives in.
    pub bucket: String,
    /// Keys go under this prefix; empty for the bucket root.
    pub prefix: String,
    /// The endpoint URL for a store other than AWS S3, such as R2 or `MinIO`.
    pub endpoint: Option<String>,
    /// The region requests are signed for.
    pub region: String,
    /// The access key ID requests are signed with.
    pub access_key_id: String,
    /// The secret for `access_key_id`. [`S3Config`]'s `Debug` leaves it out.
    pub secret_access_key: String,
    /// Allows a plain-HTTP endpoint, for a local `MinIO`.
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

/// The [`Store`] port over one bucket and key prefix. Refs use conditional writes, so the
/// backend must support `If-None-Match` and `If-Match` on PUT.
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
                let rx = self.building.lock().take();
                match rx {
                    Some(rx) => rx
                        .await
                        .unwrap_or_else(|_| Err("the store client build stopped".to_string())),
                    None => Err("the store client build stopped".to_string()),
                }
            })
            .await;
        built.as_ref().map_err(|e| StoreError::Permanent {
            key: key.to_string(),
            source: e.clone().into(),
        })
    }
}

impl S3Store {
    /// The port over the store `c` names. It returns before the client is built; the first
    /// request waits for the build and fails with [`StoreError::Other`] if the build failed.
    pub fn new(c: &S3Config) -> Result<S3Store, StoreError> {
        let mut b = AmazonS3Builder::new()
            .with_bucket_name(&c.bucket)
            .with_region(&c.region)
            .with_access_key_id(&c.access_key_id)
            .with_secret_access_key(&c.secret_access_key)
            .with_client_options(
                ClientOptions::new()
                    .with_timeout(REQUEST_TIMEOUT)
                    .with_allow_http(c.allow_http),
            )
            .with_http_connector(RefusalConnector)
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
                    .map(|s| -> Arc<dyn ObjectStore> { Arc::new(s) })
                    .map_err(|e| map_err(&bucket, e).to_string());
                let _ = tx.send(built);
            })
            .map_err(|e| StoreError::Permanent {
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

/// A client error the store answered that the same request gets again.
#[derive(Debug, thiserror::Error)]
#[error("the store refused the request: {status} {reason}: {body}")]
struct Refused {
    status: u16,
    reason: &'static str,
    body: String,
}

/// Error codes S3-compatible stores answer with a 4xx status for a condition that passes, as
/// the AWS SDKs list them.
const TRANSIENT_CODES: &[&str] = &[
    "RequestTimeout",
    "RequestTimeoutException",
    "PriorRequestNotComplete",
    "Throttling",
    "ThrottlingException",
    "ThrottledException",
    "RequestThrottled",
    "RequestThrottledException",
    "RequestLimitExceeded",
    "TooManyRequestsException",
    "SlowDown",
    "BandwidthLimitExceeded",
    "ProvisionedThroughputExceededException",
    "TransactionInProgressException",
    "EC2ThrottledException",
];

/// Whether an answer of `status` with `body` fails again if the request is repeated. Statuses
/// `object_store` maps to errors of its own keep that mapping.
fn refuses(status: u16, body: &str) -> bool {
    if !(400..500).contains(&status) || matches!(status, 401 | 403 | 404 | 408 | 409 | 412 | 429) {
        return false;
    }
    let code = body
        .split_once("<Code>")
        .and_then(|(_, rest)| rest.split_once("</Code>"))
        .map(|(code, _)| code.trim());
    !code.is_some_and(|c| TRANSIENT_CODES.contains(&c))
}

/// Serves requests over `reqwest` and answers a status that [`refuses`] with a [`Refused`]
/// error, which `object_store` passes on without a retry and [`map_err`] finds in the source
/// chain.
#[derive(Debug)]
struct RefusalConnector;

impl HttpConnector for RefusalConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(RefusalService(
            ReqwestConnector::default().connect(options)?,
        )))
    }
}

#[derive(Debug)]
struct RefusalService(HttpClient);

#[async_trait]
impl HttpService for RefusalService {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let r = self.0.execute(req).await?;
        let status = r.status();
        if !status.is_client_error() {
            return Ok(r);
        }
        let (parts, body) = r.into_parts();
        let body = body.bytes().await?;
        let text = String::from_utf8_lossy(&body).into_owned();
        if !refuses(status.as_u16(), &text) {
            return Ok(HttpResponse::from_parts(parts, body.into()));
        }
        Err(HttpError::new(
            HttpErrorKind::Unknown,
            Refused {
                status: status.as_u16(),
                reason: status.canonical_reason().unwrap_or(""),
                body: text,
            },
        ))
    }
}

fn refused(e: &object_store::Error) -> bool {
    let mut at: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(e) = at {
        if e.is::<Refused>() {
            return true;
        }
        at = e.source();
    }
    false
}

fn map_err(key: &str, e: object_store::Error) -> StoreError {
    let key = key.to_string();
    match e {
        object_store::Error::NotFound { .. } => StoreError::NotFound { key },
        object_store::Error::AlreadyExists { .. } => StoreError::AlreadyExists { key },
        object_store::Error::Precondition { .. } => StoreError::PreconditionFailed { key },
        e @ object_store::Error::Generic { .. } if !refused(&e) => StoreError::Transient {
            key,
            source: Box::new(e),
        },
        e @ (object_store::Error::Generic { .. }
        | object_store::Error::InvalidPath { .. }
        | object_store::Error::JoinError { .. }
        | object_store::Error::NotSupported { .. }
        | object_store::Error::NotModified { .. }
        | object_store::Error::NotImplemented { .. }
        | object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. }
        | object_store::Error::UnknownConfigurationKey { .. }
        | _) => StoreError::Permanent {
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    async fn answer(mut c: TcpStream, status: &str, body: &str) {
        let mut req = Vec::new();
        let mut buf = [0; 4096];
        let head_end = loop {
            let n = c.read(&mut buf).await.unwrap();
            assert!(n > 0, "the request ended early");
            req.extend_from_slice(&buf[..n]);
            if let Some(i) = req.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let head = String::from_utf8_lossy(&req[..head_end]).to_ascii_lowercase();
        let len: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .map_or(0, |v| v.trim().parse().unwrap());
        while req.len() < head_end + len {
            let n = c.read(&mut buf).await.unwrap();
            assert!(n > 0, "the request body ended early");
            req.extend_from_slice(&buf[..n]);
        }
        let reply = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        c.write_all(reply.as_bytes()).await.unwrap();
        c.shutdown().await.unwrap();
    }

    async fn answering(status: &'static str, body: &'static str) -> (S3Store, Arc<AtomicUsize>) {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let seen = Arc::new(AtomicUsize::new(0));
        let n = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let (c, _) = l.accept().await.unwrap();
                n.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(answer(c, status, body));
            }
        });
        let s = S3Store::new(&S3Config {
            bucket: "b".into(),
            endpoint: Some(format!("http://{addr}")),
            region: "us-east-1".into(),
            access_key_id: "id".into(),
            secret_access_key: "secret".into(),
            allow_http: true,
            ..S3Config::default()
        })
        .unwrap();
        (s, seen)
    }

    #[tokio::test]
    async fn a_refused_put_is_permanent_and_not_retried() {
        let (s, seen) = answering(
            "400 Bad Request",
            "<Error><Code>ValidationException</Code><Message>bucket over 64 MiB</Message></Error>",
        )
        .await;
        let e = s.put("k", Bytes::from_static(b"v")).await.unwrap_err();
        assert!(matches!(e, StoreError::Permanent { .. }), "{e}");
        assert!(e.to_string().contains("ValidationException"), "{e}");
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_too_large_put_is_permanent() {
        let (s, _) = answering("413 Payload Too Large", "").await;
        let e = s.create("k", Bytes::from_static(b"v")).await.unwrap_err();
        assert!(matches!(e, StoreError::Permanent { .. }), "{e}");
    }

    #[tokio::test]
    async fn a_request_timeout_answered_with_400_stays_transient() {
        let (s, _) = answering(
            "400 Bad Request",
            "<Error><Code>RequestTimeout</Code><Message>idle</Message></Error>",
        )
        .await;
        let e = s.put("k", Bytes::from_static(b"v")).await.unwrap_err();
        assert!(matches!(e, StoreError::Transient { .. }), "{e}");
    }

    #[tokio::test]
    async fn statuses_object_store_maps_keep_their_errors() {
        let (s, _) = answering("404 Not Found", "<Error><Code>NoSuchKey</Code></Error>").await;
        let e = s.get("k").await.unwrap_err();
        assert!(matches!(e, StoreError::NotFound { .. }), "{e}");
        let (s, _) = answering("412 Precondition Failed", "").await;
        let e = s
            .update("k", Bytes::from_static(b"v"), &Version("\"e\"".into()))
            .await
            .unwrap_err();
        assert!(matches!(e, StoreError::PreconditionFailed { .. }), "{e}");
    }

    #[test]
    fn only_lasting_client_errors_refuse() {
        assert!(refuses(400, "<Error><Code>InvalidArgument</Code></Error>"));
        assert!(refuses(400, ""));
        assert!(refuses(413, ""));
        assert!(!refuses(400, "<Error><Code>SlowDown</Code></Error>"));
        for s in [401, 403, 404, 408, 409, 412, 429, 500, 503] {
            assert!(!refuses(s, ""), "{s}");
        }
    }
}
