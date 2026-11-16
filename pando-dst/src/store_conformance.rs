//! The behaviour the engine relies on from a [`Store`]. Run [`check`] against each adapter.

use bytes::Bytes;
use pando_engine::ports::{Store, StoreError};

/// Runs every check against `store`, using keys under `test/`. Panics on the first failure.
pub async fn check(store: &dyn Store) {
    missing_objects_are_not_found(store).await;
    create_does_not_replace(store).await;
    update_needs_the_current_version(store).await;
    put_replaces(store).await;
    ranges_and_tails(store).await;
    list_returns_one_folder_in_key_order(store).await;
    delete_is_idempotent(store).await;
}

/// Deletes what [`check`] left under `test/`.
pub async fn clean_up(store: &dyn Store) {
    for o in store.list("test").await.unwrap() {
        store.delete(&o.key).await.unwrap();
    }
}

fn b(s: &'static str) -> Bytes {
    Bytes::from_static(s.as_bytes())
}

async fn missing_objects_are_not_found(store: &dyn Store) {
    assert!(matches!(
        store.get("test/missing").await,
        Err(StoreError::NotFound { .. })
    ));
    assert!(matches!(
        store.get_range("test/missing", 0..1).await,
        Err(StoreError::NotFound { .. })
    ));
}

async fn create_does_not_replace(store: &dyn Store) {
    store.create("test/create", b("first")).await.unwrap();
    assert!(matches!(
        store.create("test/create", b("second")).await,
        Err(StoreError::AlreadyExists { .. })
    ));
    assert_eq!(store.get("test/create").await.unwrap().body, b("first"));
}

async fn update_needs_the_current_version(store: &dyn Store) {
    let v1 = store.create("test/ref", b("gen 1")).await.unwrap();
    let got = store.get("test/ref").await.unwrap();
    assert_eq!(got.version, v1, "get reports the version create returned");
    let v2 = store.update("test/ref", b("gen 2"), &v1).await.unwrap();
    assert_ne!(v1, v2);
    let stale = store.update("test/ref", b("gen 2 again"), &v1).await;
    assert!(
        matches!(stale, Err(StoreError::PreconditionFailed { .. })),
        "{stale:?}"
    );
    assert_eq!(store.get("test/ref").await.unwrap().body, b("gen 2"));
    let missing = store.update("test/no-ref", b("x"), &v1).await;
    assert!(
        matches!(
            missing,
            Err(StoreError::NotFound { .. } | StoreError::PreconditionFailed { .. })
        ),
        "{missing:?}"
    );
}

async fn put_replaces(store: &dyn Store) {
    store.put("test/put", b("a")).await.unwrap();
    store.put("test/put", b("b")).await.unwrap();
    assert_eq!(store.get("test/put").await.unwrap().body, b("b"));
}

async fn ranges_and_tails(store: &dyn Store) {
    store.put("test/range", b("0123456789")).await.unwrap();
    assert_eq!(store.get_range("test/range", 2..5).await.unwrap(), b("234"));
    assert_eq!(store.get_range("test/range", 8..20).await.unwrap(), b("89"));
    assert_eq!(store.get_tail("test/range", 3).await.unwrap(), (b("789"), 10));
    assert_eq!(
        store.get_tail("test/range", 50).await.unwrap(),
        (b("0123456789"), 10)
    );
}

async fn list_returns_one_folder_in_key_order(store: &dyn Store) {
    for k in ["test/dir/b", "test/dir/a", "test/dir/sub/c", "test/dirx/d"] {
        store.put(k, b("x")).await.unwrap();
    }
    let keys: Vec<String> = store
        .list("test/dir")
        .await
        .unwrap()
        .into_iter()
        .map(|o| o.key)
        .collect();
    assert_eq!(keys, ["test/dir/a", "test/dir/b", "test/dir/sub/c"]);
}

async fn delete_is_idempotent(store: &dyn Store) {
    store.put("test/delete", b("x")).await.unwrap();
    store.delete("test/delete").await.unwrap();
    store.delete("test/delete").await.unwrap();
    assert!(matches!(
        store.get("test/delete").await,
        Err(StoreError::NotFound { .. })
    ));
}
