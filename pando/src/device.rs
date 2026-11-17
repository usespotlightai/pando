//! The `attach` and `detach` commands: a branch served as a block device.

use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, Args};

/// How `attach` serves a branch.
#[derive(Args, Debug)]
pub(crate) struct AttachArgs {
    pub(crate) name: String,
    /// The ublk device to serve the branch as, /dev/ublkbN.
    #[arg(long)]
    pub(crate) device: PathBuf,
    /// Mount through this device-mapper device, /dev/mapper/<NAME>, over the ublk device.
    #[arg(long)]
    pub(crate) dm: Option<String>,
    /// Once the branch is local, copy it to an image in this directory and switch the
    /// device-mapper device to it. Needs --dm.
    #[arg(long)]
    pub(crate) images: Option<PathBuf>,
    /// The directory of the branches' local state: journals and upload state.
    #[arg(long, default_value = "/data/state")]
    pub(crate) state: PathBuf,
    /// Start the image as a clone of this host's base image of the root the branch opened at.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    pub(crate) share_bases: bool,
    /// Upload chunks whose 4 KiB blocks are in the base image as recipes of those blocks.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    pub(crate) dedup: bool,
    /// Keep uploaded chunks on this host, and take blocks from them in later runs.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    pub(crate) learn: bool,
    /// Snapshot the image this often and upload its changes during the run (0s: never).
    #[arg(long, value_parser = crate::parse_duration, default_value = "15s")]
    pub(crate) snapshot: Duration,
    /// Also snapshot once this many bytes were written since the last snapshot (0: never).
    #[arg(long, default_value_t = 64 << 20)]
    pub(crate) snapshot_bytes: u64,
    /// Parallel fetches for the recording of earlier first reads (0: none).
    #[arg(long, default_value_t = 32)]
    pub(crate) prefetch: usize,
    /// Also fetch the metadata of an ext4 file system on the branch with the --prefetch fetches.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    pub(crate) metadata: bool,
    /// Record this mount's first reads for the next mount.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    pub(crate) record: bool,
    /// After the first reads, fetch every chunk with this many parallel fetches (0: none).
    #[arg(long, default_value_t = 0)]
    pub(crate) hydrate: usize,
    /// Take over a device whose daemon died, with the requests it had in flight.
    #[arg(long)]
    pub(crate) recover: bool,
    /// Hardware queues of a new device, one thread each.
    #[arg(long, default_value_t = 2)]
    pub(crate) queues: u16,
}

/// Which device `detach` stops.
#[derive(Args, Debug)]
pub(crate) struct DetachArgs {
    /// The ublk device the branch is served as, /dev/ublkbN.
    #[arg(long)]
    pub(crate) device: PathBuf,
    /// The device-mapper device over it, removed first.
    #[arg(long)]
    pub(crate) dm: Option<String>,
}

#[cfg(target_os = "linux")]
pub(crate) use linux::{attach, detach};

#[cfg(not(target_os = "linux"))]
pub(crate) async fn attach(
    _: &pando_engine::Ports,
    _: std::sync::Arc<pando_engine::Cache>,
    _: AttachArgs,
    _: String,
) -> anyhow::Result<()> {
    anyhow::bail!("attach needs Linux")
}

#[cfg(not(target_os = "linux"))]
pub(crate) async fn detach(_: DetachArgs) -> anyhow::Result<()> {
    anyhow::bail!("detach needs Linux")
}

#[cfg(target_os = "linux")]
mod linux {
    use std::sync::Arc;
    use std::time::Instant;

    use anyhow::{Context, Result, bail};
    use pando_engine::ports::DeviceSwitch;
    use pando_engine::{AttachOptions, Attachment, Cache, Ports};
    use pando_format::Hash;
    use pando_linux::{DmSwitch, Frontend, FrontendOptions};
    use tokio::runtime::Handle;

    use super::{AttachArgs, DetachArgs};

    /// Serves branch `a.name` until `detach` stops its device, then publishes it. `store` names
    /// the store, so chunks learned from one store never serve another.
    pub(crate) async fn attach(ports: &Ports, cache: Arc<Cache>, a: AttachArgs, store: String) -> Result<()> {
        let id = pando_linux::device_id(&a.device)?;
        if a.images.is_some() && a.dm.is_none() {
            bail!("--images needs --dm");
        }
        if a.recover {
            dead_daemon(id, &a.device).await?;
        }
        let t0 = Instant::now();
        let switch: Arc<dyn DeviceSwitch> = Arc::new(DmSwitch::new());
        let learned = a
            .images
            .as_ref()
            .filter(|_| a.learn)
            .map(|d| d.join("learned").join(Hash::of(store.as_bytes()).short()));
        let opts = AttachOptions {
            images: a.images.clone(),
            device: a.dm.clone(),
            share_bases: a.share_bases,
            dedup: a.dedup,
            learned,
            snapshot_every: a.snapshot,
            snapshot_bytes: a.snapshot_bytes,
            prefetch: a.prefetch,
            record: a.record,
            metadata: a.metadata,
            hydrate: a.hydrate,
            rejoin: a.recover,
        };
        let att =
            Arc::new(Attachment::open(ports, Some(switch.clone()), cache, &a.state, &a.name, opts).await?);
        let fopts = FrontendOptions {
            queues: a.queues,
            recover: a.recover,
        };
        let mut fe = match Frontend::start(Handle::current(), att.volume().clone(), id, fopts).await {
            Ok(fe) => fe,
            Err(e) => {
                if let Err(fe) = finish(att, &a.name).await {
                    eprintln!("{fe:#}");
                }
                return Err(e).with_context(|| format!("start {}", a.device.display()));
            }
        };
        if let Some(dm) = a.dm.as_deref().filter(|_| !a.recover)
            && let Err(e) = switch.create(dm, &a.device, att.volume().size()).await
        {
            if let Err(ae) = fe.abort().await {
                eprintln!("stop {}: {ae}", a.device.display());
            }
            if let Err(fe) = finish(att, &a.name).await {
                eprintln!("{fe:#}");
            }
            return Err(e).with_context(|| format!("device-mapper {dm} over {}", a.device.display()));
        }
        println!(
            "attached {} as {} in {:.1?}{}",
            a.name,
            a.device.display(),
            t0.elapsed(),
            if att.rejoined() {
                ", rejoined its image"
            } else {
                ""
            }
        );
        let background = (!att.rejoined()).then(|| {
            let (att, name, hand_off) = (att.clone(), a.name.clone(), a.images.is_some());
            tokio::spawn(async move {
                match att.warm().await {
                    Ok(w) => println!("warmed {name}: {w:?}"),
                    Err(e) => return eprintln!("warm {name}: {e}"),
                }
                if hand_off {
                    match att.hand_off().await {
                        Ok(h) => println!("handed {name} off: {h:?}"),
                        Err(e) => eprintln!("handoff {name}: {e}"),
                    }
                }
            })
        });
        fe.stopped().await;
        if let Some(b) = background {
            b.abort();
            let _ = b.await;
        }
        let published = finish(att, &a.name).await;
        drop(fe);
        published
    }

    /// Waits until the kernel holds the I/O of device `id` for a new daemon. A live daemon
    /// still writes the branch, so recovery must not take it over.
    async fn dead_daemon(id: u32, device: &std::path::Path) -> Result<()> {
        for _ in 0..100 {
            if pando_linux::daemon_dead(id)
                .await
                .with_context(|| format!("state of {}", device.display()))?
            {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        bail!(
            "the daemon of {} is alive; recover only a device whose daemon died",
            device.display()
        )
    }

    /// Publishes the branch and frees it; the device no longer writes to its image.
    async fn finish(att: Arc<Attachment>, name: &str) -> Result<()> {
        let t0 = Instant::now();
        let att = Arc::into_inner(att).with_context(|| format!("{name}: the attachment is still in use"))?;
        if let Some(s) = att.snapshots() {
            println!("snapshots of {name}: {s:?}");
        }
        let r = att.finish().await.with_context(|| format!("publish {name}"))?;
        println!("published {name} in {:.1?}: {r:?}", t0.elapsed());
        Ok(())
    }

    /// Stops the device of `d`. A live daemon publishes the branch before the device is
    /// deleted, so this returns once the branch is in the store.
    pub(crate) async fn detach(d: DetachArgs) -> Result<()> {
        let id = pando_linux::device_id(&d.device)?;
        let switch = DmSwitch::new();
        if let Some(dm) = &d.dm {
            if pando_linux::daemon_dead(id).await? {
                pando_linux::stop(id).await?;
                if switch.live_target(dm).await?.is_some() {
                    switch.clear_inactive(dm).await?;
                    switch.resume(dm).await?;
                }
                switch.remove(dm).await.with_context(|| format!("remove {dm}"))?;
                return Ok(pando_linux::delete(id).await?);
            }
            switch.remove(dm).await.with_context(|| format!("remove {dm}"))?;
        }
        pando_linux::stop(id).await?;
        Ok(pando_linux::delete(id).await?)
    }
}
