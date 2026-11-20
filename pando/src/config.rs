use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, bail};
use clap::Args;
use pando_engine::ports::Store;
use pando_s3::{AWS_CHAIN_VARS, Credentials, S3Config, S3Store};

/// Which store to use. S3 settings come from flags or the S3_* variables; their values never
/// show in help or errors.
#[derive(Args, Debug)]
pub(crate) struct StoreArgs {
    /// The store: s3://<prefix> [default: s3://].
    #[expect(
        rustdoc::invalid_html_tags,
        reason = "the comment is the help text, where <...> marks a placeholder"
    )]
    #[arg(long, global = true)]
    remote: Option<String>,
    #[arg(long, env = "S3_BUCKET", hide_env_values = true, global = true)]
    bucket: Option<String>,
    #[arg(long, env = "S3_ENDPOINT", hide_env_values = true, global = true)]
    endpoint: Option<String>,
    #[arg(
        long,
        env = "S3_REGION",
        default_value = "auto",
        hide_env_values = true,
        global = true
    )]
    region: String,
    /// Where S3 credentials come from: keys, the S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY and
    /// S3_SESSION_TOKEN variables (never flags); instance, the role of this instance or AgentCore
    /// microVM from the instance metadata service; aws, the AWS chain: web identity
    /// (AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN), ECS or EKS Pod Identity container
    /// credentials, then the instance; file, a JSON file another process keeps current
    /// (--credentials-file). All but keys are fetched again before they expire.
    #[expect(
        clippy::doc_markdown,
        reason = "the comment is the help text, where backticks would show"
    )]
    #[arg(
        long,
        env = "S3_CREDENTIALS",
        value_enum,
        default_value = "keys",
        global = true
    )]
    credentials: Source,
    /// With --credentials file: the file, in the form credential_process prints (AccessKeyId,
    /// SecretAccessKey, SessionToken, Expiration). Replace it, by a rename, before the key in it
    /// expires.
    #[expect(
        clippy::doc_markdown,
        reason = "the comment is the help text, where backticks would show"
    )]
    #[arg(long, env = "S3_CREDENTIALS_FILE", global = true)]
    credentials_file: Option<PathBuf>,
    /// The instance metadata service [default: http://169.254.169.254].
    #[expect(
        clippy::doc_markdown,
        reason = "the comment is the help text, where angle brackets would show"
    )]
    #[arg(long, env = "S3_METADATA_ENDPOINT", global = true)]
    metadata_endpoint: Option<String>,
}

/// Where S3 credentials come from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Source {
    Keys,
    Instance,
    Aws,
    File,
}

impl StoreArgs {
    /// The store this names: `s3://<prefix>`.
    pub(crate) fn remote(&self) -> &str {
        self.remote.as_deref().unwrap_or("s3://")
    }

    pub(crate) fn open(&self) -> Result<Arc<dyn Store>> {
        let Some(prefix) = self.remote().strip_prefix("s3://") else {
            bail!("--remote must be s3://<prefix>");
        };
        let Some(bucket) = &self.bucket else {
            bail!("an s3:// store needs S3_BUCKET");
        };
        let config = S3Config {
            bucket: bucket.clone(),
            prefix: prefix.to_string(),
            endpoint: self.endpoint.clone().filter(|e| !e.is_empty()),
            region: self.region.clone(),
            credentials: self.credentials()?,
            allow_http: false,
        };
        Ok(Arc::new(S3Store::new(&config)?))
    }

    fn credentials(&self) -> Result<Credentials> {
        self.credentials_from(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
    }

    /// The credentials, with `var` reading the environment.
    pub(crate) fn credentials_from(&self, var: impl Fn(&str) -> Option<String>) -> Result<Credentials> {
        Ok(match self.credentials {
            Source::Instance => Credentials::Instance {
                endpoint: self.metadata_endpoint.clone().filter(|e| !e.is_empty()),
            },
            Source::Aws => Credentials::Aws {
                vars: AWS_CHAIN_VARS
                    .iter()
                    .filter_map(|k| Some(((*k).to_string(), var(k)?)))
                    .collect(),
            },
            Source::File => {
                let Some(path) = &self.credentials_file else {
                    bail!("S3_CREDENTIALS=file needs S3_CREDENTIALS_FILE or --credentials-file");
                };
                Credentials::File { path: path.clone() }
            }
            Source::Keys => {
                let (Some(access_key_id), Some(secret_access_key)) =
                    (var("S3_ACCESS_KEY_ID"), var("S3_SECRET_ACCESS_KEY"))
                else {
                    bail!(
                        "an s3:// store needs S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY, \
                         or S3_CREDENTIALS=instance, aws or file"
                    );
                };
                Credentials::Keys {
                    access_key_id,
                    secret_access_key,
                    session_token: var("S3_SESSION_TOKEN"),
                }
            }
        })
    }

    /// Makes the credentials file absolute, against `cwd` where it is relative.
    pub(crate) fn resolve(&mut self, cwd: &std::path::Path) {
        if let Some(f) = &mut self.credentials_file {
            *f = cwd.join(&*f);
        }
    }

    /// A name of the store that tells it apart from other stores and holds no secret.
    pub(crate) fn identity(&self) -> String {
        self.identity_or(self.remote())
    }

    /// [`Self::identity`], with `remote` as the store when none was given.
    pub(crate) fn identity_or(&self, remote: &str) -> String {
        format!(
            "{}|{}|{}",
            self.endpoint.as_deref().unwrap_or_default(),
            self.bucket.as_deref().unwrap_or_default(),
            normal(self.remote.as_deref().unwrap_or(remote))
        )
    }
}

/// `remote` with the slashes around its prefix trimmed, so `s3://a/` and `s3://a` name one store.
fn normal(remote: &str) -> String {
    match remote.strip_prefix("s3://") {
        Some(p) => format!("s3://{}", p.trim_matches('/')),
        None => remote.to_owned(),
    }
}
