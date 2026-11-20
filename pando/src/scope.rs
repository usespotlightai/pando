//! What keeps the data of one store, such as one org's prefix, apart from every other's: a
//! host's cache and branch state belong to the store that first used them, and `purge` deletes
//! everything under a store's prefix.

use std::io::ErrorKind;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use futures::{StreamExt, TryStreamExt};
use pando_engine::ports::Store;
use pando_format::keys;
use serde_json::json;

use crate::serve::Out;

/// The file in a bound directory that names its store.
const STORE_FILE: &str = "store";

/// Deletes in flight at once during a purge.
const PARALLEL_DELETES: usize = 32;

/// Ties `dir` to the store `identity`: the first use writes `dir/store`, and a use with any
/// other store fails before anything in `dir` is read, so what a host keeps for one store never
/// serves another. Of two processes that bind a new directory at once, one writes the file and
/// the other checks against it.
pub(crate) fn bind(dir: &Path, identity: &str) -> Result<()> {
    let path = dir.join(STORE_FILE);
    match std::fs::read_to_string(&path) {
        Ok(had) => same(dir, &had, identity),
        Err(e) if e.kind() == ErrorKind::NotFound => {
            std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
            let tmp = dir.join(format!(".{STORE_FILE}.{}", std::process::id()));
            std::fs::write(&tmp, format!("{identity}\n"))
                .with_context(|| format!("write {}", tmp.display()))?;
            std::fs::File::open(&tmp)?.sync_all()?;
            let linked = std::fs::hard_link(&tmp, &path);
            let _ = std::fs::remove_file(&tmp);
            match linked {
                Ok(()) => Ok(std::fs::File::open(dir)?.sync_all()?),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    let had =
                        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
                    same(dir, &had, identity)
                }
                Err(e) => Err(e).with_context(|| format!("write {}", path.display())),
            }
        }
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

fn same(dir: &Path, had: &str, identity: &str) -> Result<()> {
    if had.trim_end() != identity {
        bail!(
            "{} holds the data of the store {}, not of {identity}: give each store a directory of its own",
            dir.display(),
            had.trim_end()
        );
    }
    Ok(())
}

/// What a purge deleted.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Purged {
    /// Objects deleted.
    pub(crate) objects: u64,
    /// Their bytes.
    pub(crate) bytes: u64,
}

/// The parts of a store a purge deletes in turn, each listed on its own so that no listing
/// holds more than GC's listing of the segments does: the refs first, so no host opens a
/// branch of the store from then on, and last whatever else is left.
const PARTS: [&str; 6] = [
    keys::REFS,
    keys::CHECKPOINTS,
    keys::LOG,
    keys::CHANGES,
    keys::SEGMENTS,
    "",
];

/// Deletes every object of `store`, whose prefix `remote` names, part by part as [`PARTS`]
/// lists them, telling `done` each part's name and the objects it deleted there. `confirm` must be the prefix again, and an
/// empty prefix, a whole bucket, is refused. It fails if objects are still there after it
/// deleted what it listed: a host still writes under the prefix.
pub(crate) async fn purge(
    store: &dyn Store,
    remote: &str,
    confirm: &str,
    done: &(dyn Fn(&str, u64) + Sync),
) -> Result<Purged> {
    let prefix = remote.strip_prefix("s3://").unwrap_or(remote).trim_matches('/');
    if prefix.is_empty() {
        bail!("purge needs a prefix, --remote s3://<prefix>; it never deletes a whole bucket");
    }
    if confirm.trim_matches('/') != prefix {
        bail!("--confirm {confirm:?} is not the prefix {prefix:?} of --remote");
    }
    let mut purged = Purged { objects: 0, bytes: 0 };
    for dir in PARTS {
        let found = store.list(dir).await.with_context(|| format!("list {remote}"))?;
        let n = u64::try_from(found.len())?;
        purged.objects += n;
        purged.bytes += found.iter().map(|o| o.size).sum::<u64>();
        futures::stream::iter(found)
            .map(|o| async move {
                store
                    .delete(&o.key)
                    .await
                    .with_context(|| format!("delete {}", o.key))
            })
            .buffer_unordered(PARALLEL_DELETES)
            .try_collect::<()>()
            .await?;
        done(if dir.is_empty() { "the rest" } else { dir }, n);
    }
    let left = store.list("").await.with_context(|| format!("list {remote}"))?;
    if !left.is_empty() {
        bail!(
            "{} objects appeared under {remote} while it was purged, such as {}: a host still writes there. Detach its branches and purge again",
            left.len(),
            left[0].key
        );
    }
    Ok(purged)
}

/// [`purge`], reported on `out`.
pub(crate) async fn run_purge(store: &dyn Store, remote: &str, confirm: &str, out: &Out) -> Result<()> {
    let t0 = Instant::now();
    let p = purge(store, remote, confirm, &|part, n| {
        out.result(
            format!("deleted {n} objects of {part}"),
            json!({"event": "purged-part", "part": part, "objects": n}),
        );
    })
    .await?;
    let took = t0.elapsed();
    out.result(
        format!(
            "purged {remote}: {} objects ({} MiB) in {took:.1?}",
            p.objects,
            p.bytes >> 20
        ),
        json!({
            "event": "purged",
            "remote": remote,
            "objects": p.objects,
            "bytes": p.bytes,
            "took_ms": crate::millis(took),
        }),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use pando_s3::S3Store;

    use super::*;

    async fn filled(inner: &Arc<InMemory>, prefix: &str) -> S3Store {
        let s = S3Store::over(Arc::<InMemory>::clone(inner), prefix);
        for k in [
            "format",
            "refs/main",
            "refs/task-1",
            "segments/0001",
            "segments/0001.idx",
            "log/main/1",
        ] {
            s.put(k, Bytes::from_static(b"0123456789")).await.unwrap();
        }
        s
    }

    #[tokio::test]
    async fn a_purge_deletes_its_prefix_and_no_other() {
        let inner = Arc::new(InMemory::new());
        let a = filled(&inner, "org-a").await;
        let ab = filled(&inner, "org-ab").await;
        let b = filled(&inner, "org-b").await;
        let parts = std::sync::Mutex::new(Vec::new());
        let p = purge(&a, "s3://org-a", "org-a", &|part, n| {
            parts.lock().unwrap().push((part.to_owned(), n));
        })
        .await
        .unwrap();
        let parts = parts.into_inner().unwrap();
        let named = |p: &str, n| (p.to_owned(), n);
        assert_eq!(
            parts,
            [
                named("refs", 2),
                named("checkpoints", 0),
                named("log", 1),
                named("changes", 0),
                named("segments", 2),
                named("the rest", 1)
            ]
        );
        assert_eq!(
            p,
            Purged {
                objects: 6,
                bytes: 60
            }
        );
        assert_eq!(a.list("").await.unwrap().len(), 0);
        assert_eq!(ab.list("").await.unwrap().len(), 6);
        assert_eq!(b.list("").await.unwrap().len(), 6);
        assert_eq!(
            purge(&a, "s3://org-a/", "org-a/", &|_, _| {}).await.unwrap(),
            Purged { objects: 0, bytes: 0 }
        );
    }

    #[tokio::test]
    async fn a_purge_needs_the_prefix_twice_and_never_takes_a_bucket() {
        let inner = Arc::new(InMemory::new());
        let a = filled(&inner, "org-a").await;
        let e = purge(&a, "s3://org-a", "org-b", &|_, _| {}).await.unwrap_err();
        assert!(e.to_string().contains("is not the prefix"), "{e}");
        let root = S3Store::over(inner, "");
        for remote in ["s3://", "s3:///"] {
            let e = purge(&root, remote, "", &|_, _| {}).await.unwrap_err();
            assert!(e.to_string().contains("never deletes a whole bucket"), "{e}");
        }
        assert_eq!(a.list("").await.unwrap().len(), 6);
    }

    #[test]
    fn a_directory_serves_only_the_store_that_first_used_it() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join("cache");
        bind(&dir, "https://r2|bucket|s3://org-a").unwrap();
        bind(&dir, "https://r2|bucket|s3://org-a").unwrap();
        let e = bind(&dir, "https://r2|bucket|s3://org-b").unwrap_err();
        assert!(
            e.to_string()
                .contains("s3://org-a, not of https://r2|bucket|s3://org-b"),
            "{e}"
        );
        bind(&d.path().join("other"), "https://r2|bucket|s3://org-b").unwrap();
        let names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["store"]);
    }
}
