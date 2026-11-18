//! What a host does with images an earlier writer left: a writer can die after the handoff,
//! while the kernel goes on serving the branch from the image, or the host can restart.
//!
//! The next attach of the branch publishes what such an image holds beyond the store, before it
//! serves the branch, unless another writer took the branch since: then the image is older than
//! the branch and goes. An image whose owner record does not say "switched" is a partial copy,
//! older than the cache and the journal, and goes too. When the device still serves the image,
//! a new writer rejoins the run instead: its first snapshot publishes what the dead one did not.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::extents::changed_chunks;
use super::owner::{self, Owner};
use super::{Handoff, HostEnv, SnapshotOptions, use_dedup};
use crate::ports::{DeviceSwitch, LocalFs, Open};
use crate::{Error, Result, Volume};

/// The images an earlier attach of branch `name` left in `dir`.
async fn leftover_images(fs: &dyn LocalFs, dir: &Path, name: &str) -> Vec<PathBuf> {
    let Ok(entries) = fs.list(dir).await else {
        return Vec::new();
    };
    let prefix = format!("{name}.");
    let mut out: Vec<PathBuf> = entries
        .into_iter()
        .filter(|de| {
            de.name
                .strip_prefix(&prefix)
                .and_then(|r| r.strip_suffix(".img"))
                .is_some_and(|mid| !mid.is_empty() && !mid.contains('.'))
        })
        .map(|de| dir.join(de.name))
        .collect();
    out.sort();
    out
}

/// The clones of `image` and the first clone number after them.
async fn leftover_clones(fs: &dyn LocalFs, image: &Path) -> (Vec<PathBuf>, u32) {
    let (Some(dir), Some(base)) = (image.parent(), image.file_name().and_then(|n| n.to_str())) else {
        return (Vec::new(), 0);
    };
    let Ok(entries) = fs.list(dir).await else {
        return (Vec::new(), 0);
    };
    let prefix = format!("{base}.snap");
    let mut out = Vec::new();
    let mut next = 0;
    for de in entries {
        if let Some(n) = de.name.strip_prefix(&prefix).and_then(|n| n.parse::<u32>().ok()) {
            next = next.max(n + 1);
            out.push(dir.join(de.name));
        }
    }
    (out, next)
}

async fn remove_image(fs: &dyn LocalFs, v: &Volume, image: &Path) {
    let (clones, _) = leftover_clones(fs, image).await;
    for p in clones.iter().chain([&image.to_owned(), &owner::path(image)]) {
        v.tree().cache().drop_file(p);
        let _ = fs.remove(p).await;
    }
}

/// Undoes what a writer that died during a switch or a snapshot left on device `dm`: an
/// inactive table, which the next resume would make live, and a suspend, which holds all I/O.
pub(crate) async fn recover_device(devices: &dyn DeviceSwitch, dm: &str) -> Result<()> {
    if devices.live_target(dm).await?.is_none() {
        return Ok(());
    }
    devices.clear_inactive(dm).await?;
    devices.resume(dm).await?;
    Ok(())
}

/// The chunks of `image` whose extents differ from the base image it was cloned from, with
/// that base's blocks set up for recipes; `None` when the base is gone.
async fn changed_since_base(
    env: &HostEnv,
    v: &Volume,
    image: &Path,
    base: Option<pando_format::Hash>,
) -> Option<Vec<u64>> {
    let (bases, base) = (env.bases.as_ref()?, base?);
    if !bases.has(&base).await {
        return None;
    }
    let be = env.ports.image.extents(&bases.path(&base)).await.ok()?;
    let ie = env.ports.image.extents(image).await.ok()?;
    let root = pando_format::Entry {
        hash: base,
        ..pando_format::Entry::EMPTY
    };
    use_dedup(env, bases, v, root).await;
    Some(changed_chunks(&be, &ie).0)
}

/// Publishes what the images of `v`'s branch in `dir`, left by a writer that died, hold beyond
/// the store, and removes them. Returns how many chunks it published.
pub(crate) async fn adopt_leftovers(env: &HostEnv, v: &Volume, dir: &Path) -> Result<usize> {
    let fs = env.ports.fs.as_ref();
    let mut adopted = 0;
    for image in leftover_images(fs, dir, v.name()).await {
        let Some(o) = Owner::read(fs, &image).await? else {
            continue;
        };
        if !o.switched || o.generation + 1 != v.generation() {
            remove_image(fs, v, &image).await;
            continue;
        }
        let f = fs.open(&image, Open::Read).await?;
        let n = match changed_since_base(env, v, &image, o.base).await {
            Some(idxs) => v.absorb_chunks(&f, &image, &idxs).await?,
            None => v.absorb_image(env.ports.image.as_ref(), &f, &image).await?,
        };
        let done = v.checkpoint().await;
        v.set_dedup(None);
        done?;
        adopted += n;
        remove_image(fs, v, &image).await;
    }
    Ok(adopted)
}

/// Takes over branch `v` whose writer died after the handoff while the device `dm` still serves
/// its image from `dir`. The first snapshot compares the image with the base it was cloned from,
/// or takes every chunk that differs from the root, so it publishes all the dead writer did not;
/// later snapshots go on as before. `None` when `dm` does not serve an image of this branch.
pub(crate) async fn rejoin(
    env: &HostEnv,
    v: &Arc<Volume>,
    dm: &str,
    dir: &Path,
    opts: SnapshotOptions,
) -> Result<Option<Arc<Handoff>>> {
    let devices = env.devices.as_ref();
    recover_device(devices, dm).await?;
    let Some(lp) = devices.live_target(dm).await? else {
        return Ok(None);
    };
    let Some(image) = devices.loop_image(&lp).await? else {
        return Ok(None);
    };
    let fs = env.ports.fs.as_ref();
    if !leftover_images(fs, dir, v.name()).await.contains(&image) {
        return Ok(None);
    }
    let o = Owner::read(fs, &image)
        .await?
        .ok_or_else(|| Error::Corrupt(format!("{}: no owner record", image.display())))?;
    if o.generation >= v.generation() {
        return Err(Error::Corrupt(format!(
            "{} belongs to writer generation {}, not one before {}",
            image.display(),
            o.generation,
            v.generation()
        )));
    }
    let (old, next) = leftover_clones(fs, &image).await;
    let file = fs.open(&image, Open::ReadWrite).await?;
    let h = Arc::new(Handoff::new(env, dm, image.clone(), file, o.base, opts));
    h.switched.store(true, Ordering::Release);
    *h.loop_dev.lock() = Some(lp);
    h.owner(v, true).await?;
    {
        let mut s = h.snaps.lock().await;
        s.next = next;
        if let (Some(bases), Some(base)) = (env.bases.as_ref(), o.base)
            && let Ok(ext) = env.ports.image.extents(&bases.path(&base)).await
        {
            s.prev = Some(ext);
            use_dedup(
                env,
                bases,
                v,
                pando_format::Entry {
                    hash: base,
                    ..pando_format::Entry::EMPTY
                },
            )
            .await;
        }
    }
    let first = h.snaps.lock().await.prev.is_some();
    if first {
        h.snapshot_once(v).await?;
    } else {
        let path = h.snaps.lock().await.next_path(&image);
        h.frozen_clone(&path).await?;
        let snap = fs.open(&path, Open::Read).await?;
        v.absorb_image(env.ports.image.as_ref(), &snap, &path).await?;
        let mut s = h.snaps.lock().await;
        h.set_prev(&mut s, path).await?;
        drop(s);
        v.flush().await?;
    }
    h.retire_unpublished(v, old);
    h.start_snapshots(v);
    Ok(Some(h))
}
