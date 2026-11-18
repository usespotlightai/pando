//! The store conformance suite against each [`Store`](pando_engine::ports::Store) adapter.

use std::sync::Arc;

use object_store::memory::InMemory;
use pando_dst::store_conformance;
use pando_engine::sim::{SimClock, SimStore};
use pando_s3::S3Store;

#[tokio::test]
async fn sim_store_conforms() {
    store_conformance::check(&SimStore::new(Arc::new(SimClock::new()))).await;
}

#[tokio::test]
async fn s3_adapter_conforms_over_an_in_memory_backend() {
    store_conformance::check(&S3Store::over(Arc::new(InMemory::new()), "prefix/a")).await;
}

/// Runs against a real store named by `S3_BUCKET`, `S3_REGION`, `S3_ENDPOINT`, `S3_ACCESS_KEY_ID`,
/// `S3_SECRET_ACCESS_KEY` and, for a temporary key, `S3_SESSION_TOKEN`, under a fresh prefix it removes afterwards.
#[tokio::test]
#[ignore = "needs S3_* variables for a real store"]
async fn s3_store_conforms() {
    let var = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"));
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let config = pando_s3::S3Config {
        bucket: var("S3_BUCKET"),
        prefix: format!("pando-rs-conformance/{nanos}"),
        endpoint: std::env::var("S3_ENDPOINT").ok().filter(|e| !e.is_empty()),
        region: std::env::var("S3_REGION").unwrap_or_else(|_| "auto".into()),
        credentials: pando_s3::Credentials::Keys {
            access_key_id: var("S3_ACCESS_KEY_ID"),
            secret_access_key: var("S3_SECRET_ACCESS_KEY"),
            session_token: std::env::var("S3_SESSION_TOKEN").ok().filter(|t| !t.is_empty()),
        },
        allow_http: false,
    };
    let store = S3Store::new(&config).unwrap();
    store_conformance::check(&store).await;
    store_conformance::clean_up(&store).await;
}
