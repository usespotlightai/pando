//! Branches as refs: copies, checkpoints and fast-forwards. Writing to a branch is
//! [`crate::Volume`].

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pando_format::{BranchId, Checkpoint, LogPos, Ref};

use crate::remote::Remote;
use crate::volume::seal_log;
use crate::{Error, Ports, Result};

/// Makes branch `dst` a copy of `src`, forked from it. A copy slower than `max_age` is undone:
/// GC marks again after its validate delay, and a copy of an old root that finished later could
/// name deleted segments.
pub async fn copy(ports: &Ports, src: &str, dst: &str, max_age: Option<Duration>) -> Result<()> {
    let started = tokio::time::Instant::now();
    let remote = Remote::new(Arc::clone(&ports.store));
    let (r, _) = remote.get_ref(src).await?.ok_or_else(|| no_branch(src))?;
    remote
        .create_ref(ports.rng.as_ref(), dst, |id| Ref {
            size: r.size,
            id,
            root: r.root,
            generation: 0,
            from: Some(src.to_owned()),
            log: None,
        })
        .await?;
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

/// Checkpoints this process has read, by name. A checkpoint never changes, so a branch from a
/// kept one creates its ref without reading the checkpoint first.
#[derive(Debug, Default)]
pub struct Checkpoints {
    kept: Mutex<HashMap<String, Checkpoint>>,
}

impl Checkpoints {
    fn get(&self, name: &str) -> Option<Checkpoint> {
        self.kept.lock().get(name).cloned()
    }

    fn keep(&self, name: &str, c: Checkpoint) {
        self.kept.lock().insert(name.to_owned(), c);
    }

    fn forget(&self, name: &str) {
        self.kept.lock().remove(name);
    }
}

/// Freezes the published root of branch `src` as checkpoint `name`, which must not exist. A
/// checkpoint slower than `max_age` is undone, as [`copy`] is.
pub async fn checkpoint(
    ports: &Ports,
    kept: &Checkpoints,
    src: &str,
    name: &str,
    max_age: Option<Duration>,
) -> Result<Checkpoint> {
    let started = tokio::time::Instant::now();
    let remote = Remote::new(Arc::clone(&ports.store));
    let (r, _) = remote.get_ref(src).await?.ok_or_else(|| no_branch(src))?;
    let c = Checkpoint {
        size: r.size,
        root: r.root,
        branch: src.to_owned(),
    };
    remote.create_checkpoint(name, &c).await?;
    if let Some(max) = max_age
        && started.elapsed() > max
    {
        remote.delete_checkpoint(name).await?;
        return Err(Error::Corrupt(format!(
            "the checkpoint of {src} took longer than {max:?}; retry"
        )));
    }
    kept.keep(name, c.clone());
    Ok(c)
}

/// Makes branch `dst` a copy of checkpoint `name`, forked from the checkpoint's branch. With
/// the checkpoint kept, the ref is created while the checkpoint is read back, in one round
/// trip; a checkpoint gone or replaced meanwhile undoes the copy. A copy slower than
/// `max_age` is undone, as [`copy`] is.
pub async fn from_checkpoint(
    ports: &Ports,
    kept: &Checkpoints,
    name: &str,
    dst: &str,
    max_age: Option<Duration>,
) -> Result<()> {
    let started = tokio::time::Instant::now();
    let remote = Remote::new(Arc::clone(&ports.store));
    let ours = match kept.get(name) {
        Some(c) => {
            let (made, now) = futures::join!(
                remote.create_ref(ports.rng.as_ref(), dst, |id| copy_of(&c, id)),
                remote.get_checkpoint(name)
            );
            made?;
            match now {
                Ok(Some(now)) if now == c => None,
                now => {
                    kept.forget(name);
                    remote.delete_ref(dst).await?;
                    Some(now?.ok_or_else(|| no_checkpoint(name))?)
                }
            }
        }
        None => Some(
            remote
                .get_checkpoint(name)
                .await?
                .ok_or_else(|| no_checkpoint(name))?,
        ),
    };
    if let Some(c) = ours {
        remote
            .create_ref(ports.rng.as_ref(), dst, |id| copy_of(&c, id))
            .await?;
        kept.keep(name, c);
    }
    if let Some(max) = max_age
        && started.elapsed() > max
    {
        remote.delete_ref(dst).await?;
        return Err(Error::Corrupt(format!(
            "the copy of checkpoint {name} took longer than {max:?}; retry"
        )));
    }
    Ok(())
}

/// Deletes checkpoint `name`. What only it reaches goes at the next GC that finds it unreached.
pub async fn delete_checkpoint(ports: &Ports, kept: &Checkpoints, name: &str) -> Result<()> {
    kept.forget(name);
    Remote::new(Arc::clone(&ports.store))
        .delete_checkpoint(name)
        .await
}

/// Every checkpoint, by name.
pub async fn list_checkpoints(ports: &Ports) -> Result<Vec<(String, Checkpoint)>> {
    let remote = Remote::new(Arc::clone(&ports.store));
    let mut out = Vec::new();
    for name in remote.list_checkpoints().await? {
        if let Some(c) = remote.get_checkpoint(&name).await? {
            out.push((name, c));
        }
    }
    Ok(out)
}

fn copy_of(c: &Checkpoint, id: BranchId) -> Ref {
    Ref {
        size: c.size,
        id,
        root: c.root,
        generation: 0,
        from: Some(c.branch.clone()),
        log: None,
    }
}

/// Moves branch `dst` to the published root of `src`, unless `dst` changed meanwhile. What
/// `dst` held goes, flushes its writer had not published included: the writer's log is sealed,
/// so a writer still running fails its next append, and `dst` moves to a new writer generation
/// whose log is closed, so no open replays the old log or resumes the old writer's state.
///
/// The seal comes before the ref moves, so no strict flush the writer logs returns after the
/// advance. When `dst` changed meanwhile, the advance fails with its writer fenced all the
/// same, as when an open seals the log and loses the take: what the writer logged stays for the
/// next open, and running the advance again is safe.
pub async fn advance(ports: &Ports, dst: &str, src: &str) -> Result<()> {
    let remote = Remote::new(Arc::clone(&ports.store));
    let (from, _) = remote.get_ref(src).await?.ok_or_else(|| no_branch(src))?;
    let (mut to, version) = remote.get_ref(dst).await?.ok_or_else(|| no_branch(dst))?;
    if to.size != from.size {
        return Err(Error::Corrupt(format!(
            "{dst} has size {}, {src} has {}",
            to.size, from.size
        )));
    }
    seal_log(&remote, &ports.cpu, dst, &to).await?;
    let generation = to.generation + 1;
    to.root = from.root;
    to.generation = generation;
    to.log = Some(LogPos {
        generation,
        flush: 0,
        next: 0,
        closed: true,
    });
    remote.put_ref(dst, &to, &version).await.map(drop)
}

/// Deletes branch `name`, then the write log of the life it deleted. A branch made later under
/// the name is another life, which reads none of that log, so a delete that stops part way
/// harms no later branch. Its segments, and any records of its log left behind, go at the next
/// GC that finds them unreached. The ref's delete is not conditional: a delete that runs while
/// the branch is deleted and made again elsewhere may delete the new branch.
pub async fn delete(ports: &Ports, name: &str) -> Result<()> {
    let remote = Remote::new(Arc::clone(&ports.store));
    let Some((r, _)) = remote.get_ref(name).await? else {
        return Ok(());
    };
    remote.delete_ref(name).await?;
    for (generation, n, _) in remote.list_log(name, r.id).await? {
        remote.delete_log_record(name, r.id, generation, n).await?;
    }
    Ok(())
}

/// The names of every branch.
pub async fn list(ports: &Ports) -> Result<Vec<String>> {
    Remote::new(Arc::clone(&ports.store)).list_refs().await
}

fn no_branch(name: &str) -> Error {
    Error::Corrupt(format!("no branch {name}"))
}

fn no_checkpoint(name: &str) -> Error {
    Error::Corrupt(format!("no checkpoint {name}"))
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use std::path::Path;

    use pando_format::CHUNK_SIZE;

    use super::*;
    use crate::{Cache, LogMode, Volume, sim};

    async fn write(ports: &Ports, cache: &Arc<Cache>, name: &str, seed: u8) {
        let v = Volume::open(ports, Arc::clone(cache), Path::new("/v"), name)
            .await
            .unwrap();
        v.write_at(&vec![seed; CHUNK_SIZE], 0).await.unwrap();
        v.checkpoint().await.unwrap();
        v.close().await;
    }

    async fn first_byte(ports: &Ports, cache: &Arc<Cache>, name: &str) -> u8 {
        let v = Volume::open(ports, Arc::clone(cache), Path::new("/r"), name)
            .await
            .unwrap();
        let mut b = [0; 1];
        v.read_at(&mut b, 0).await.unwrap();
        v.close().await;
        b[0]
    }

    #[tokio::test(start_paused = true)]
    async fn a_branch_from_a_checkpoint_keeps_the_root_it_was_taken_at() {
        let (ports, _, _) = sim::ports(1);
        Volume::create(&ports, "main", 1 << 20).await.unwrap();
        let cache = Arc::new(Cache::open(&ports, Path::new("/c")).await.unwrap());
        write(&ports, &cache, "main", 1).await;
        let kept = Checkpoints::default();
        let c = checkpoint(&ports, &kept, "main", "v1", None).await.unwrap();
        assert_eq!(c.branch, "main");
        assert!(matches!(
            checkpoint(&ports, &kept, "main", "v1", None).await,
            Err(Error::CheckpointExists(_))
        ));
        write(&ports, &cache, "main", 2).await;

        let fresh = Checkpoints::default();
        from_checkpoint(&ports, &fresh, "v1", "a", None).await.unwrap();
        from_checkpoint(&ports, &kept, "v1", "b", None).await.unwrap();
        let remote = Remote::new(Arc::clone(&ports.store));
        for name in ["a", "b"] {
            let (r, _) = remote.get_ref(name).await.unwrap().unwrap();
            assert_eq!(
                (r.root, r.generation, r.from.as_deref()),
                (c.root, 0, Some("main"))
            );
            assert_eq!(first_byte(&ports, &cache, name).await, 1);
        }
        assert!(matches!(
            from_checkpoint(&ports, &kept, "v1", "a", None).await,
            Err(Error::RefExists(_))
        ));
        assert_eq!(first_byte(&ports, &cache, "main").await, 2);
        let names: Vec<String> = list_checkpoints(&ports)
            .await
            .unwrap()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert_eq!(names, ["v1"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_kept_checkpoint_branches_in_one_round_trip() {
        let (ports, store, _) = sim::ports(2);
        Volume::create(&ports, "main", 1 << 20).await.unwrap();
        let kept = Checkpoints::default();
        checkpoint(&ports, &kept, "main", "v1", None).await.unwrap();
        store.set_latency(Duration::from_millis(10));

        let t0 = tokio::time::Instant::now();
        from_checkpoint(&ports, &kept, "v1", "a", None).await.unwrap();
        assert_eq!(t0.elapsed(), Duration::from_millis(10));

        let t0 = tokio::time::Instant::now();
        copy(&ports, "main", "b", None).await.unwrap();
        assert_eq!(t0.elapsed(), Duration::from_millis(20));
    }

    #[tokio::test(start_paused = true)]
    async fn a_branch_made_again_under_a_deleted_name_starts_with_no_log() {
        let (ports, store, _) = sim::ports(4);
        Volume::create(&ports, "main", 1 << 20).await.unwrap();
        Volume::create(&ports, "foo", 1 << 20).await.unwrap();
        let cache = Arc::new(Cache::open(&ports, Path::new("/c")).await.unwrap());
        write(&ports, &cache, "main", 1).await;
        let dead = Volume::open_logged(
            &ports,
            Arc::clone(&cache),
            Path::new("/d"),
            "foo",
            LogMode::Strict,
        )
        .await
        .unwrap();
        dead.wait_generation().await.unwrap();
        for _ in 0..10_000 {
            store.inject(sim::Fault {
                op: sim::StoreOp::Update,
                applied: false,
                permanent: false,
            });
        }
        dead.write_at(&vec![9; CHUNK_SIZE], 0).await.unwrap();
        dead.flush().await.unwrap();
        dead.close().await;
        store.clear_faults();

        delete(&ports, "foo").await.unwrap();
        assert_eq!(
            store.keys(&pando_format::keys::log_name("foo")),
            Vec::<String>::new()
        );
        copy(&ports, "main", "foo", None).await.unwrap();
        let v = Volume::open_logged(
            &ports,
            Arc::clone(&cache),
            Path::new("/e"),
            "foo",
            LogMode::Strict,
        )
        .await
        .unwrap();
        v.wait_generation().await.unwrap();
        let mut b = [0; 1];
        v.read_at(&mut b, 0).await.unwrap();
        assert_eq!(b[0], 1);
        v.write_at(&vec![2; CHUNK_SIZE], 0).await.unwrap();
        v.flush().await.unwrap();
        v.close().await;
    }

    async fn host(ports: &Ports, dir: &str) -> Arc<Cache> {
        Arc::new(Cache::open(ports, Path::new(dir)).await.unwrap())
    }

    #[tokio::test(start_paused = true)]
    async fn a_writer_of_a_deleted_branch_cannot_publish_to_one_made_again_under_its_name() {
        let (ports, store, _) = sim::ports(5);
        Volume::create(&ports, "foo", 1 << 20).await.unwrap();
        let old = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "foo")
            .await
            .unwrap();
        store.hold(sim::StoreOp::Create, pando_format::keys::SEGMENTS);
        old.write_at(&vec![9; CHUNK_SIZE], 0).await.unwrap();
        old.flush().await.unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;

        delete(&ports, "foo").await.unwrap();
        Volume::create(&ports, "foo", 1 << 20).await.unwrap();
        let new = Volume::open(&ports, host(&ports, "/b").await, Path::new("/b"), "foo")
            .await
            .unwrap();
        store.release(sim::StoreOp::Create, pando_format::keys::SEGMENTS);
        tokio::time::sleep(Duration::from_secs(10)).await;
        old.close().await;
        new.close().await;

        assert_eq!(first_byte(&ports, &host(&ports, "/c").await, "foo").await, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_host_does_not_resume_a_deleted_branch_on_one_made_again_under_its_name() {
        let (ports, store, fs) = sim::ports(6);
        Volume::create(&ports, "foo", 1 << 20).await.unwrap();
        let old = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "foo")
            .await
            .unwrap();
        store.refuse(sim::StoreOp::Create);
        old.write_at(&vec![9; CHUNK_SIZE], 0).await.unwrap();
        old.flush().await.unwrap();
        old.close().await;
        drop(old);
        fs.crash();
        store.accept();

        delete(&ports, "foo").await.unwrap();
        Volume::create(&ports, "foo", 1 << 20).await.unwrap();
        Volume::open(&ports, host(&ports, "/b").await, Path::new("/b"), "foo")
            .await
            .unwrap()
            .close()
            .await;

        let again = Volume::open(&ports, host(&ports, "/a").await, Path::new("/a"), "foo")
            .await
            .unwrap();
        let mut b = [0; 1];
        again.read_at(&mut b, 0).await.unwrap();
        again.close().await;
        assert_eq!(b[0], 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_branch_made_again_under_a_name_whose_delete_stopped_replays_none_of_its_log() {
        let (ports, store, _) = sim::ports(7);
        Volume::create(&ports, "foo", 1 << 20).await.unwrap();
        let old = Volume::open_logged(
            &ports,
            host(&ports, "/a").await,
            Path::new("/a"),
            "foo",
            LogMode::Strict,
        )
        .await
        .unwrap();
        old.wait_generation().await.unwrap();
        store.refuse(sim::StoreOp::Update);
        old.write_at(&vec![9; CHUNK_SIZE], 0).await.unwrap();
        old.flush().await.unwrap();
        old.close().await;
        store.accept();

        store.inject(sim::Fault {
            op: sim::StoreOp::Delete,
            applied: true,
            permanent: false,
        });
        assert!(delete(&ports, "foo").await.is_err());
        assert_ne!(
            store.keys(&pando_format::keys::log_name("foo")),
            Vec::<String>::new()
        );
        Volume::create(&ports, "foo", 1 << 20).await.unwrap();
        let new = Volume::open_logged(
            &ports,
            host(&ports, "/b").await,
            Path::new("/b"),
            "foo",
            LogMode::Strict,
        )
        .await
        .unwrap();
        new.wait_generation().await.unwrap();
        let mut b = [0; 1];
        new.read_at(&mut b, 0).await.unwrap();
        assert_eq!(b[0], 0);
        new.write_at(&vec![2; CHUNK_SIZE], 0).await.unwrap();
        new.flush().await.unwrap();
        new.close().await;

        assert_eq!(first_byte(&ports, &host(&ports, "/c").await, "foo").await, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_kept_checkpoint_that_is_gone_or_replaced_is_not_used() {
        let (ports, _, _) = sim::ports(3);
        Volume::create(&ports, "main", 1 << 20).await.unwrap();
        Volume::create(&ports, "other", 2 << 20).await.unwrap();
        let cache = Arc::new(Cache::open(&ports, Path::new("/c")).await.unwrap());
        write(&ports, &cache, "other", 7).await;
        let kept = Checkpoints::default();
        checkpoint(&ports, &kept, "main", "v1", None).await.unwrap();
        let remote = Remote::new(Arc::clone(&ports.store));

        remote.delete_checkpoint("v1").await.unwrap();
        assert!(from_checkpoint(&ports, &kept, "v1", "a", None).await.is_err());
        assert!(remote.get_ref("a").await.unwrap().is_none());

        checkpoint(&ports, &kept, "main", "v1", None).await.unwrap();
        remote.delete_checkpoint("v1").await.unwrap();
        let now = checkpoint(&ports, &Checkpoints::default(), "other", "v1", None)
            .await
            .unwrap();
        from_checkpoint(&ports, &kept, "v1", "a", None).await.unwrap();
        let (r, _) = remote.get_ref("a").await.unwrap().unwrap();
        assert_eq!(
            (r.size, r.root, r.from.as_deref()),
            (now.size, now.root, Some("other"))
        );
        assert_eq!(first_byte(&ports, &cache, "a").await, 7);
    }
}
