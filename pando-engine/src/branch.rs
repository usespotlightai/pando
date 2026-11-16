//! Branches as refs: copies and fast-forwards. Writing to a branch is [`crate::Volume`].

use std::time::Duration;

use crate::remote::Remote;
use crate::{Error, Ports, Result};

/// Makes branch `dst` a copy of `src`, forked from it. A copy slower than `max_age` is undone:
/// GC marks again after its validate delay, and a copy of an old root that finished later could
/// name deleted segments.
pub async fn copy(ports: &Ports, src: &str, dst: &str, max_age: Option<Duration>) -> Result<()> {
    let started = tokio::time::Instant::now();
    let remote = Remote::new(ports.store.clone());
    let (mut r, _) = remote.get_ref(src).await?.ok_or_else(|| no_branch(src))?;
    r.generation = 0;
    r.from = Some(src.to_owned());
    remote.create_ref(dst, &r).await?;
    if let Some(max) = max_age
        && started.elapsed() > max
    {
        remote.delete_ref(dst).await?;
        return Err(Error::Corrupt(format!(
            "the copy of {src} took longer than {max:?}; retry"
        )));
    }
    Ok(())
}

/// Moves branch `dst` to the published root of `src`, unless `dst` changed meanwhile.
pub async fn advance(ports: &Ports, dst: &str, src: &str) -> Result<()> {
    let remote = Remote::new(ports.store.clone());
    let (from, _) = remote.get_ref(src).await?.ok_or_else(|| no_branch(src))?;
    let (mut to, version) = remote.get_ref(dst).await?.ok_or_else(|| no_branch(dst))?;
    if to.size != from.size {
        return Err(Error::Corrupt(format!(
            "{dst} has size {}, {src} has {}",
            to.size, from.size
        )));
    }
    to.root = from.root;
    remote.put_ref(dst, &to, &version).await.map(drop)
}

/// Deletes branch `name`. Its segments go at the next GC that finds them unreached.
pub async fn delete(ports: &Ports, name: &str) -> Result<()> {
    Remote::new(ports.store.clone()).delete_ref(name).await
}

/// The names of every branch.
pub async fn list(ports: &Ports) -> Result<Vec<String>> {
    Remote::new(ports.store.clone()).list_refs().await
}

fn no_branch(name: &str) -> Error {
    Error::Corrupt(format!("no branch {name}"))
}
