//! Pando's [`Store`] port over any S3-compatible object store, through `object_store`.
//!
//! Refs need conditional writes: `If-None-Match: *` for create and `If-Match: <etag>` for
//! update. AWS S3, Cloudflare R2 and `MinIO` support both.

#![forbid(unsafe_code)]

use parking_lot::Mutex;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredential, S3ConditionalPut};
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpResponseBody,
    HttpService, ReqwestConnector,
};
use object_store::path::Path;
use object_store::{
    ClientOptions, CredentialProvider, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode,
    PutOptions, PutPayload, UpdateVersion,
};
use pando_engine::ports::{Object, ObjectInfo, Store, StoreError, Version};
use tokio::sync::{OnceCell, oneshot};

/// The longest one request may take. A segment PUT shares the uplink with the other PUTs in
/// flight, so on a slow link it takes minutes.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
/// The longest a connection to the store may take to open, so a store that does not answer
/// fails each try quickly and the engine's retries and waits apply.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Where the store is and how to sign in.
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
    /// What requests are signed with. [`S3Config`]'s `Debug` leaves it out.
    pub credentials: Credentials,
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
            .field("credentials", &self.credentials)
            .finish_non_exhaustive()
    }
}

/// What requests to the store are signed with.
#[derive(Clone)]
pub enum Credentials {
    /// An access key, with the session token of a temporary key.
    Keys {
        /// The access key ID.
        access_key_id: String,
        /// The secret for `access_key_id`.
        secret_access_key: String,
        /// The session token of a temporary key.
        session_token: Option<String>,
    },
    /// The role of this instance, or of an `AgentCore` microVM, from the instance metadata
    /// service. Requests take a session token first (`IMDSv2`; `IMDSv1` is never tried), and the
    /// credentials are fetched again five minutes before they expire, on the next request.
    Instance {
        /// The metadata service, `http://169.254.169.254` when `None`.
        endpoint: Option<String>,
    },
    /// The AWS credential chain after static keys, as `vars` configure it: web identity
    /// (`AWS_WEB_IDENTITY_TOKEN_FILE` with `AWS_ROLE_ARN`, as EKS sets them), else the container
    /// credentials of ECS (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`) or of EKS Pod Identity
    /// (`AWS_CONTAINER_CREDENTIALS_FULL_URI` with `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`), else
    /// the instance's role. Each is fetched again before it expires.
    Aws {
        /// Values of the variables [`AWS_CHAIN_VARS`] names, by name; any other is ignored.
        vars: Vec<(String, String)>,
    },
    /// A key in a JSON file another process keeps current, in the form `credential_process`
    /// prints: `AccessKeyId`, `SecretAccessKey`, and for a temporary key `SessionToken` and
    /// `Expiration` (RFC 3339). Each request reads the file again if its size or modification
    /// time changed, so a replacement written before the old key expires takes over without a
    /// failed request. A key past its `Expiration` fails the request as transient, and the
    /// engine's retries wait for the file to change.
    File {
        /// The file.
        path: PathBuf,
    },
}

/// The variables of the AWS credential chain [`Credentials::Aws`] reads.
pub const AWS_CHAIN_VARS: &[&str] = &[
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME",
    "AWS_ENDPOINT_URL_STS",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    "AWS_EC2_METADATA_SERVICE_ENDPOINT",
];

fn chain_key(var: &str) -> Option<AmazonS3ConfigKey> {
    Some(match var {
        "AWS_WEB_IDENTITY_TOKEN_FILE" => AmazonS3ConfigKey::WebIdentityTokenFile,
        "AWS_ROLE_ARN" => AmazonS3ConfigKey::RoleArn,
        "AWS_ROLE_SESSION_NAME" => AmazonS3ConfigKey::RoleSessionName,
        "AWS_ENDPOINT_URL_STS" => AmazonS3ConfigKey::StsEndpoint,
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" => AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
        "AWS_CONTAINER_CREDENTIALS_FULL_URI" => AmazonS3ConfigKey::ContainerCredentialsFullUri,
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE" => AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
        "AWS_EC2_METADATA_SERVICE_ENDPOINT" => AmazonS3ConfigKey::MetadataEndpoint,
        _ => return None,
    })
}

impl Default for Credentials {
    fn default() -> Credentials {
        Credentials::Keys {
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: None,
        }
    }
}

/// Names the source only: no key, secret or token.
impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Credentials::Keys { session_token, .. } => f
                .debug_struct("Keys")
                .field("temporary", &session_token.is_some())
                .finish_non_exhaustive(),
            Credentials::Instance { endpoint } => {
                f.debug_struct("Instance").field("endpoint", endpoint).finish()
            }
            Credentials::Aws { vars } => f
                .debug_struct("Aws")
                .field("vars", &vars.iter().map(|(k, _)| k).collect::<Vec<_>>())
                .finish(),
            Credentials::File { path } => f.debug_struct("File").field("path", path).finish(),
        }
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
    /// request waits for the build and fails with [`StoreError::Permanent`] if the build failed.
    pub fn new(c: &S3Config) -> Result<S3Store, StoreError> {
        let mut b = AmazonS3Builder::new()
            .with_bucket_name(&c.bucket)
            .with_region(&c.region)
            .with_client_options(
                ClientOptions::new()
                    .with_timeout(REQUEST_TIMEOUT)
                    .with_connect_timeout(CONNECT_TIMEOUT)
                    .with_allow_http(c.allow_http),
            )
            .with_http_connector(RefusalConnector)
            .with_conditional_put(S3ConditionalPut::ETagMatch);
        if let Some(e) = &c.endpoint {
            b = b.with_endpoint(e);
        }
        match &c.credentials {
            Credentials::Keys {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                b = b
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key);
                if let Some(t) = session_token {
                    b = b.with_token(t);
                }
            }
            Credentials::Instance { endpoint } => {
                if let Some(e) = endpoint {
                    b = b.with_metadata_endpoint(e);
                }
            }
            Credentials::Aws { vars } => {
                for (var, value) in vars {
                    if let Some(k) = chain_key(var) {
                        b = b.with_config(k, value);
                    }
                }
            }
            Credentials::File { path } => {
                b = b.with_credentials(Arc::new(FileCredentials {
                    path: path.clone(),
                    last: Mutex::new(None),
                }));
            }
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

/// The key of [`Credentials::File`], read again when the file changes.
struct FileCredentials {
    path: PathBuf,
    last: Mutex<Option<FromFile>>,
}

struct FromFile {
    stamp: (Option<SystemTime>, u64),
    credential: Arc<AwsCredential>,
    expires: Option<SystemTime>,
}

impl std::fmt::Debug for FileCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileCredentials")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl FileCredentials {
    fn error(&self, what: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: format!("the credentials in {}: {what}", self.path.display()).into(),
        }
    }

    fn current(&self, f: &FromFile) -> object_store::Result<Arc<AwsCredential>> {
        match f.expires {
            Some(t) if t <= SystemTime::now() => Err(self.error(&format!(
                "expired at {}, and the file has not changed since",
                chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339()
            ))),
            _ => Ok(Arc::clone(&f.credential)),
        }
    }
}

#[async_trait]
impl CredentialProvider for FileCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let meta = tokio::fs::metadata(&self.path)
            .await
            .map_err(|e| self.error(&e.to_string()))?;
        let stamp = (meta.modified().ok(), meta.len());
        if let Some(f) = self.last.lock().as_ref().filter(|f| f.stamp == stamp) {
            return self.current(f);
        }
        let text = tokio::fs::read(&self.path)
            .await
            .map_err(|e| self.error(&e.to_string()))?;
        let (credential, expires) = parse_credentials(&text).map_err(|e| self.error(&e))?;
        let f = FromFile {
            stamp,
            credential: Arc::new(credential),
            expires,
        };
        let c = self.current(&f);
        *self.last.lock() = Some(f);
        c
    }
}

/// The key in a `credential_process` document, and when it expires. An error names what is
/// wrong without any of the file's text.
fn parse_credentials(text: &[u8]) -> Result<(AwsCredential, Option<SystemTime>), String> {
    let doc: serde_json::Value = serde_json::from_slice(text)
        .map_err(|e| format!("not JSON at line {} column {}", e.line(), e.column()))?;
    let field = |name: &str| -> Result<Option<String>, String> {
        match doc.get(name) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(serde_json::Value::String(s)) if !s.is_empty() => Ok(Some(s.clone())),
            Some(_) => Err(format!("{name} is not a string")),
        }
    };
    let (Some(key_id), Some(secret_key)) = (field("AccessKeyId")?, field("SecretAccessKey")?) else {
        return Err("it needs AccessKeyId and SecretAccessKey".into());
    };
    let token = match field("SessionToken")? {
        Some(t) => Some(t),
        None => field("Token")?,
    };
    let expires = field("Expiration")?
        .map(|t| {
            chrono::DateTime::parse_from_rfc3339(&t)
                .map(SystemTime::from)
                .map_err(|_| "Expiration is not an RFC 3339 time".to_string())
        })
        .transpose()?;
    Ok((
        AwsCredential {
            key_id,
            secret_key,
            token,
        },
        expires,
    ))
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
        let body = head(body).await?;
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

/// The most of a client error's body that is read and kept.
const BODY_MAX: usize = 4 << 10;

/// The first [`BODY_MAX`] bytes of `body`.
async fn head(body: HttpResponseBody) -> Result<Bytes, HttpError> {
    let mut chunks = body.bytes_stream();
    let mut head = Vec::new();
    while head.len() < BODY_MAX {
        match chunks.try_next().await? {
            Some(c) => head.extend_from_slice(&c),
            None => break,
        }
    }
    head.truncate(BODY_MAX);
    Ok(Bytes::from(head))
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
        if c.write_all(reply.as_bytes()).await.is_ok() {
            let _ = c.shutdown().await;
        }
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
            credentials: Credentials::Keys {
                access_key_id: "id".into(),
                secret_access_key: "secret".into(),
                session_token: None,
            },
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
    async fn a_refusal_keeps_only_the_head_of_a_long_body() {
        let body: &'static str = Box::leak(
            format!(
                "<Error><Code>InvalidArgument</Code>{}</Error>",
                "x".repeat(1 << 20)
            )
            .into_boxed_str(),
        );
        let (s, _) = answering("400 Bad Request", body).await;
        let e = s.put("k", Bytes::from_static(b"v")).await.unwrap_err();
        assert!(matches!(e, StoreError::Permanent { .. }), "{e}");
        assert!(
            e.to_string().len() < 2 * BODY_MAX,
            "{} bytes",
            e.to_string().len()
        );
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

    async fn read_head(c: &mut TcpStream) -> String {
        let mut req = Vec::new();
        let mut buf = [0; 4096];
        loop {
            let n = c.read(&mut buf).await.unwrap();
            assert!(n > 0, "the request ended early");
            req.extend_from_slice(&buf[..n]);
            if req.windows(4).any(|w| w == b"\r\n\r\n") {
                return String::from_utf8_lossy(&req).to_ascii_lowercase();
            }
        }
    }

    async fn metadata_and_store(mut c: TcpStream, heads: Arc<std::sync::Mutex<Vec<String>>>) {
        let head = read_head(&mut c).await;
        heads.lock().unwrap().push(head.clone());
        let token = head.contains("x-aws-ec2-metadata-token: tok\r\n");
        let (status, body) = match head.split_whitespace().take(2).collect::<Vec<_>>()[..] {
            ["put", "/latest/api/token"] if head.contains("x-aws-ec2-metadata-token-ttl-seconds:") => {
                ("200 OK", "tok")
            }
            ["get", "/latest/meta-data/iam/security-credentials/"] if token => ("200 OK", "role"),
            ["get", "/latest/meta-data/iam/security-credentials/role"] if token => (
                "200 OK",
                r#"{"Code":"Success","AccessKeyId":"ASIAEXAMPLE","SecretAccessKey":"s3cr3t","Token":"session","Expiration":"2099-01-01T00:00:00Z"}"#,
            ),
            ["get", "/b/k"] => ("200 OK", "v"),
            _ => ("401 Unauthorized", ""),
        };
        let reply = format!(
            "HTTP/1.1 {status}\r\netag: \"e\"\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        if c.write_all(reply.as_bytes()).await.is_ok() {
            let _ = c.shutdown().await;
        }
    }

    #[tokio::test]
    async fn instance_credentials_take_a_metadata_token_and_sign_with_the_session() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let heads = Arc::new(std::sync::Mutex::new(Vec::new()));
        let h = Arc::clone(&heads);
        tokio::spawn(async move {
            loop {
                let (c, _) = l.accept().await.unwrap();
                tokio::spawn(metadata_and_store(c, Arc::clone(&h)));
            }
        });
        let config = S3Config {
            bucket: "b".into(),
            endpoint: Some(format!("http://{addr}")),
            region: "us-east-1".into(),
            credentials: Credentials::Instance {
                endpoint: Some(format!("http://{addr}")),
            },
            allow_http: true,
            ..S3Config::default()
        };
        assert!(!format!("{config:?}").contains("s3cr3t"));
        let s = S3Store::new(&config).unwrap();
        assert_eq!(&s.get("k").await.unwrap().body[..], b"v");
        assert_eq!(&s.get("k").await.unwrap().body[..], b"v");
        let heads = heads.lock().unwrap();
        let paths: Vec<&str> = heads.iter().filter_map(|h| h.split_whitespace().nth(1)).collect();
        assert_eq!(
            paths,
            [
                "/latest/api/token",
                "/latest/meta-data/iam/security-credentials/",
                "/latest/meta-data/iam/security-credentials/role",
                "/b/k",
                "/b/k"
            ]
        );
        for get in &heads[3..] {
            assert!(get.contains("x-amz-security-token: session\r\n"), "{get}");
            assert!(get.contains("credential=asiaexample/"), "{get}");
        }
    }

    type Heads = Arc<std::sync::Mutex<Vec<String>>>;
    type Reply = Box<dyn Fn(&str) -> (&'static str, String) + Send + Sync>;

    async fn serving(reply: Reply) -> (std::net::SocketAddr, Heads) {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let heads: Heads = Arc::default();
        let h = Arc::clone(&heads);
        let reply = Arc::new(reply);
        tokio::spawn(async move {
            loop {
                let (mut c, _) = l.accept().await.unwrap();
                let h = Arc::clone(&h);
                let reply = Arc::clone(&reply);
                tokio::spawn(async move {
                    let head = read_head(&mut c).await;
                    h.lock().unwrap().push(head.clone());
                    let (status, body) = reply(&head);
                    let r = format!(
                        "HTTP/1.1 {status}\r\netag: \"e\"\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    if c.write_all(r.as_bytes()).await.is_ok() {
                        let _ = c.shutdown().await;
                    }
                });
            }
        });
        (addr, heads)
    }

    fn signed_by(heads: &Heads, path: &str) -> Vec<String> {
        heads
            .lock()
            .unwrap()
            .iter()
            .filter(|h| h.split_whitespace().nth(1) == Some(path))
            .filter_map(|h| {
                h.split_once("credential=")
                    .map(|(_, r)| r.split('/').next().unwrap_or("").to_string())
            })
            .collect()
    }

    fn store_at(addr: std::net::SocketAddr, credentials: Credentials) -> S3Store {
        S3Store::new(&S3Config {
            bucket: "b".into(),
            endpoint: Some(format!("http://{addr}")),
            region: "us-east-1".into(),
            credentials,
            allow_http: true,
            ..S3Config::default()
        })
        .unwrap()
    }

    fn rfc3339(t: SystemTime) -> String {
        chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339()
    }

    fn write(path: &std::path::Path, text: &str) {
        let tmp = path.with_extension("new");
        std::fs::write(&tmp, text).unwrap();
        std::fs::rename(&tmp, path).unwrap();
    }

    #[tokio::test]
    async fn a_credentials_file_replaced_before_it_expires_signs_the_next_request() {
        let (addr, heads) = serving(Box::new(|_| ("200 OK", "v".into()))).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        let soon = rfc3339(SystemTime::now() + Duration::from_secs(600));
        write(
            &path,
            &format!(
                r#"{{"Version":1,"AccessKeyId":"ASIAONE","SecretAccessKey":"s3cr3t1","SessionToken":"t1","Expiration":"{soon}"}}"#
            ),
        );
        let s = store_at(addr, Credentials::File { path: path.clone() });
        s.get("k").await.unwrap();
        s.put("k", Bytes::from_static(b"v")).await.unwrap();
        write(
            &path,
            &format!(
                r#"{{"Version":1,"AccessKeyId":"ASIATWOTWO","SecretAccessKey":"s3cr3t2","SessionToken":"t2","Expiration":"{soon}"}}"#
            ),
        );
        s.put("k", Bytes::from_static(b"v")).await.unwrap();
        assert_eq!(signed_by(&heads, "/b/k"), ["asiaone", "asiaone", "asiatwotwo"]);
        let last = heads.lock().unwrap().last().unwrap().clone();
        assert!(last.contains("x-amz-security-token: t2\r\n"), "{last}");
    }

    #[tokio::test]
    async fn an_expired_credentials_file_fails_as_transient_without_its_secret() {
        let (addr, heads) = serving(Box::new(|_| ("200 OK", "v".into()))).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        let past = rfc3339(SystemTime::now() - Duration::from_secs(1));
        write(
            &path,
            &format!(
                r#"{{"AccessKeyId":"ASIAOLD","SecretAccessKey":"s3cr3t","SessionToken":"tok","Expiration":"{past}"}}"#
            ),
        );
        let s = store_at(addr, Credentials::File { path: path.clone() });
        let e = s.get("k").await.unwrap_err();
        assert!(matches!(e, StoreError::Transient { .. }), "{e}");
        let text = format!("{e} {e:?}");
        assert!(text.contains("expired"), "{text}");
        for secret in ["ASIAOLD", "s3cr3t", "tok\""] {
            assert!(!text.contains(secret), "{text}");
        }
        assert!(heads.lock().unwrap().is_empty());
        write(&path, r#"{"AccessKeyId":"ASIANEW","SecretAccessKey":"s3cr3t"}"#);
        s.get("k").await.unwrap();
        assert_eq!(signed_by(&heads, "/b/k"), ["asianew"]);
    }

    #[test]
    fn a_malformed_credentials_file_names_the_fault_and_no_value() {
        for (text, fault) in [
            (
                r#"{"AccessKeyId":5,"SecretAccessKey":"s3cr3t"}"#,
                "AccessKeyId is not a string",
            ),
            (
                r#"{"AccessKeyId":"AKID"}"#,
                "needs AccessKeyId and SecretAccessKey",
            ),
            (
                r#"{"AccessKeyId":"AKID","SecretAccessKey":"s3cr3t","Expiration":"s3cr3t"}"#,
                "not an RFC 3339",
            ),
            (r#"{"AccessKeyId":"AKID","SecretAccessKey":"s3cr3t""#, "not JSON"),
        ] {
            let e = parse_credentials(text.as_bytes()).err().unwrap();
            assert!(e.contains(fault), "{e}");
            assert!(!e.contains("s3cr3t") && !e.contains("AKID\""), "{e}");
        }
    }

    #[tokio::test]
    async fn short_instance_sessions_are_fetched_again_and_uploads_go_on() {
        let fetched = Arc::new(AtomicUsize::new(0));
        let n = Arc::clone(&fetched);
        let (addr, heads) = serving(Box::new(move |head| {
            let token = head.contains("x-aws-ec2-metadata-token: tok\r\n");
            match head.split_whitespace().take(2).collect::<Vec<_>>()[..] {
                ["put", "/latest/api/token"] => ("200 OK", "tok".into()),
                ["get", "/latest/meta-data/iam/security-credentials/"] if token => ("200 OK", "role".into()),
                ["get", "/latest/meta-data/iam/security-credentials/role"] if token => {
                    let i = n.fetch_add(1, Ordering::SeqCst);
                    let expires = rfc3339(SystemTime::now() + Duration::from_secs(120));
                    (
                        "200 OK",
                        format!(
                            r#"{{"Code":"Success","AccessKeyId":"ASIA{i}","SecretAccessKey":"s{i}","Token":"t{i}","Expiration":"{expires}"}}"#
                        ),
                    )
                }
                [_, "/b/k"] => ("200 OK", "v".into()),
                _ => ("401 Unauthorized", String::new()),
            }
        }))
        .await;
        let s = store_at(
            addr,
            Credentials::Instance {
                endpoint: Some(format!("http://{addr}")),
            },
        );
        s.put("k", Bytes::from_static(b"v")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        s.put("k", Bytes::from_static(b"v")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        s.get("k").await.unwrap();
        assert_eq!(signed_by(&heads, "/b/k"), ["asia0", "asia1", "asia2"]);
    }

    #[tokio::test]
    async fn the_aws_chain_takes_eks_pod_identity_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, "pod-token").unwrap();
        let (addr, heads) = serving(Box::new(|head| {
            match head.split_whitespace().take(2).collect::<Vec<_>>()[..] {
                ["get", "/v1/credentials"] if head.contains("authorization: pod-token\r\n") => (
                    "200 OK",
                    r#"{"AccessKeyId":"ASIAPOD","SecretAccessKey":"s3cr3t","Token":"session","Expiration":"2099-01-01T00:00:00Z"}"#.into(),
                ),
                ["get", "/b/k"] => ("200 OK", "v".into()),
                _ => ("401 Unauthorized", String::new()),
            }
        }))
        .await;
        let vars = vec![
            (
                "AWS_CONTAINER_CREDENTIALS_FULL_URI".to_string(),
                format!("http://{addr}/v1/credentials"),
            ),
            (
                "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE".to_string(),
                token.display().to_string(),
            ),
            ("AWS_SECRET_ACCESS_KEY".to_string(), "ignored".to_string()),
        ];
        let c = Credentials::Aws { vars };
        assert!(!format!("{c:?}").contains("pod-token"));
        let s = store_at(addr, c);
        s.get("k").await.unwrap();
        assert_eq!(signed_by(&heads, "/b/k"), ["asiapod"]);
    }

    #[test]
    fn every_chain_variable_configures_the_client() {
        for v in AWS_CHAIN_VARS {
            assert!(chain_key(v).is_some(), "{v}");
        }
    }

    #[test]
    fn debug_shows_no_credential() {
        let c = S3Config {
            credentials: Credentials::Keys {
                access_key_id: "AKIDEXAMPLE".into(),
                secret_access_key: "s3cr3t".into(),
                session_token: Some("session".into()),
            },
            ..S3Config::default()
        };
        let d = format!("{c:?}");
        for secret in ["AKIDEXAMPLE", "s3cr3t", "session"] {
            assert!(!d.contains(secret), "{d}");
        }
        assert!(d.contains("temporary: true"), "{d}");
        let f = FileCredentials {
            path: "/run/pando/credentials.json".into(),
            last: Mutex::new(Some(FromFile {
                stamp: (None, 0),
                credential: Arc::new(AwsCredential {
                    key_id: "AKIDEXAMPLE".into(),
                    secret_key: "s3cr3t".into(),
                    token: Some("session".into()),
                }),
                expires: None,
            })),
        };
        let d = format!("{f:?}");
        assert!(d.contains("credentials.json"), "{d}");
        for secret in ["AKIDEXAMPLE", "s3cr3t", "session"] {
            assert!(!d.contains(secret), "{d}");
        }
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
