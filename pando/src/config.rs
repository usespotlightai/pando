use std::sync::Arc;

use anyhow::{Result, bail};
use clap::Args;
use pando_engine::ports::Store;
use pando_s3::{S3Config, S3Store};

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
    #[arg(
        long,
        env = "S3_ACCESS_KEY_ID",
        hide_env_values = true,
        hide = true,
        global = true
    )]
    access_key_id: Option<String>,
    #[arg(
        long,
        env = "S3_SECRET_ACCESS_KEY",
        hide_env_values = true,
        hide = true,
        global = true
    )]
    secret_access_key: Option<String>,
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
        let (Some(bucket), Some(access_key_id), Some(secret_access_key)) =
            (&self.bucket, &self.access_key_id, &self.secret_access_key)
        else {
            bail!("an s3:// store needs S3_BUCKET, S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY");
        };
        let config = S3Config {
            bucket: bucket.clone(),
            prefix: prefix.to_string(),
            endpoint: self.endpoint.clone().filter(|e| !e.is_empty()),
            region: self.region.clone(),
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            allow_http: false,
        };
        Ok(Arc::new(S3Store::new(&config)?))
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
            self.remote.as_deref().unwrap_or(remote)
        )
    }
}
