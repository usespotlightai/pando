//! What a host does with images an earlier writer left: a writer can die after the handoff,
//! while the kernel goes on serving the branch from the image, or the host can restart.
//!
//! The next attach of the branch publishes what such an image holds beyond the store, before it
//! serves the branch, unless a writer on another host took the branch since: then the image is
//! older than the branch and goes. Writers on this host that died before they adopted the image
//! do not count, as their local state carries the image's writer on. An image with no owner
//! record, or one that does not say "switched", is a partial copy, older than the cache and the
//! journal, and goes too. When the device still serves the image, a new writer rejoins the run
//! instead: its first snapshot publishes what the dead one did not.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::extents::changed_chunks;
use super::owner::{self, Owner};
use super::{Handoff, HostEnv, SnapshotOptions, era_name, use_dedup};
use crate::ports::{DeviceSwitch, LocalFs, Open};
use crate::{Error, Result, Volume};

/// The images an earlier attach of branch `name` left in `dir`: each image, and each image
/// that is gone while a file named after it stays.
async fn leftover_images(fs: &dyn LocalFs, dir: &Path, name: &str) -> Vec<PathBuf> {
    let Ok(entries) = fs.list(dir).await else {
        return Vec::new();
    };
    let prefix = format!("{name}.");
    let mut out: Vec<PathBuf> = entries
        .into_iter()
        .filter_map(|de| {
            let (mid, rest) = de.name.strip_prefix(&prefix)?.split_once(".img")?;
            let named = !mid.is_empty() && !mid.contains('.') && (rest.is_empty() || rest.starts_with('.'));
            named.then(|| dir.join(format!("{name}.{mid}.img")))
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// The clones of `image`.
async fn leftover_clones(fs: &dyn LocalFs, image: &Path) -> Vec<PathBuf> {
    let (Some(dir), Some(base)) = (image.parent(), image.file_name().and_then(|n| n.to_str())) else {
        return Vec::new();
    };
    let Ok(entries) = fs.list(dir).await else {
        return Vec::new();
    };
    let prefix = format!("{base}.snap");
    entries
        .into_iter()
        .filter(|de| de.name.starts_with(&prefix))
        .map(|de| dir.join(de.name))
        .collect()
}

/// Removes `image` and every file named after it: its clones, its era records and the
/// temporary files a killed writer left. Its owner record goes first and durably, so no later
/// attach adopts the image again after a crash, which would put its old chunks over newer
/// writes; the files a crash leaves without it go at the next attach.
pub(crate) async fn remove_image(fs: &dyn LocalFs, v: &Volume, image: &Path) -> Result<()> {
    let (Some(dir), Some(base)) = (image.parent(), image.file_name().and_then(|n| n.to_str())) else {
        return Ok(());
    };
    match fs.remove(&owner::path(image)).await {
        Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e.into()),
        _ => fs.sync_dir(dir).await?,
    }
    let prefix = format!("{base}.");
    for de in fs.list(dir).await.unwrap_or_default() {
        if de.name == base || de.name.starts_with(&prefix) {
            let p = dir.join(de.name);
            v.tree().cache().drop_file(&p);
            let _ = fs.remove(&p).await;
        }
    }
    Ok(())
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

/// Removes the era device of `dm` that a writer which died as it detached left, unless the
/// live table of `dm` maps it.
pub(crate) async fn remove_stale_era(devices: &dyn DeviceSwitch, dm: &str) -> Result<()> {
    let name = era_name(dm);
    let Some(e) = devices.era(&name).await? else {
        return Ok(());
    };
    if devices.live_target(dm).await? == Some(e.path) {
        return Ok(());
    }
    devices.remove(&name).await?;
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
        let f = match fs.open(&image, Open::Read).await {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                remove_image(fs, v, &image).await?;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        let o = Owner::read(fs, &image).await?;
        let Some(o) = o.filter(|o| o.switched && (v.since()..=v.generation()).contains(&o.generation)) else {
            remove_image(fs, v, &image).await?;
            continue;
        };
        let n = match changed_since_base(env, v, &image, o.base).await {
            Some(idxs) => v.absorb_chunks(&f, &image, &idxs).await?,
            None => v.absorb_image(env.ports.image.as_ref(), &f, &image).await?,
        };
        let done = v.checkpoint().await;
        v.set_dedup(None);
        done?;
        adopted += n;
        remove_image(fs, v, &image).await?;
    }
    Ok(adopted)
}

/// The owner record of `image`, which `v` may rejoin: no later writer than `v` wrote it.
async fn rejoinable_owner(fs: &dyn LocalFs, image: &Path, v: &Volume) -> Result<Owner> {
    let o = Owner::read(fs, image)
        .await?
        .ok_or_else(|| Error::Corrupt(format!("{}: no owner record", image.display())))?;
    if o.generation > v.generation() {
        return Err(Error::Corrupt(format!(
            "{} belongs to writer generation {}, past {}",
            image.display(),
            o.generation,
            v.generation()
        )));
    }
    Ok(o)
}

/// Takes over branch `v` whose writer died after the handoff while the device `dm` still serves
/// its image from `dir`. The first snapshot compares the image with the base it was cloned from,
/// or takes every chunk that differs from the root, so it publishes all the dead writer did not;
/// later snapshots go on as before. When an era device sits between `dm` and the image, the
/// first snapshot clones the whole image during the suspend that begins a new era, and later
/// ones clone what each era wrote. `None` when `dm` does not serve an image of this branch.
pub(crate) async fn rejoin(
    env: &HostEnv,
    v: &Arc<Volume>,
    dm: &str,
    dir: &Path,
    opts: SnapshotOptions,
) -> Result<Option<Arc<Handoff>>> {
    let devices = env.devices.as_ref();
    recover_device(devices, dm).await?;
    let Some(live) = devices.live_target(dm).await? else {
        return Ok(None);
    };
    let era = devices
        .era(&era_name(dm))
        .await?
        .filter(|e| e.path == live)
        .map(|e| (era_name(dm), e.origin));
    let lp = era.as_ref().map_or(live, |(_, origin)| origin.clone());
    let Some(image) = devices.loop_image(&lp).await? else {
        return Ok(None);
    };
    let fs = env.ports.fs.as_ref();
    if !leftover_images(fs, dir, v.name()).await.contains(&image) {
        return Ok(None);
    }
    let o = rejoinable_owner(fs, &image, v).await?;
    let old = leftover_clones(fs, &image).await;
    let file = fs.open(&image, Open::ReadWrite).await?;
    let h = Arc::new(Handoff::new(
        env,
        dm,
        image.clone(),
        file,
        o.base,
        opts,
        v.generation(),
    ));
    h.switched.store(true, Ordering::Release);
    *h.loop_dev.lock() = Some(lp);
    h.owner(v, true).await?;
    {
        let mut s = h.snaps.lock().await;
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
    if let Some((name, _)) = era {
        *h.era.lock() = Some(name.clone());
        let mut s = h.snaps.lock().await;
        let base = s.prev.take();
        let path = s.next_path(&image);
        let (_, now) = h.frozen_clone(&path, Some(&name)).await?;
        let snap = fs.open(&path, Open::Read).await?;
        let cur = match base {
            Some(_) => env.ports.image.extents(&path).await.ok(),
            None => None,
        };
        match base.zip(cur) {
            Some((be, cur)) => {
                v.absorb_chunks(&snap, &path, &changed_chunks(&be, &cur).0)
                    .await?
            }
            None => v.absorb_image(env.ports.image.as_ref(), &snap, &path).await?,
        };
        s.prev_path = Some(path);
        s.since = now.unwrap_or(1);
        drop(s);
        v.flush().await?;
    } else if first {
        h.snapshot_once(v).await?;
    } else {
        let path = h.snaps.lock().await.next_path(&image);
        h.frozen_clone(&path, None).await?;
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
