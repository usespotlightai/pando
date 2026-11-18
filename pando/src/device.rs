//! The `attach`, `resize` and `detach` commands: a branch served as a block device. An attach
//! listens on a control socket, `/run/pando/ublkbN.sock`, that `resize` asks to grow it. A
//! request is one line, `grow <bytes>`; the answer is `ok <bytes>` or `error <why>`.

use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, Args};

/// How `attach` serves a branch.
#[derive(Args, Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each bool is an independent command-line flag"
)]
pub(crate) struct AttachArgs {
    pub(crate) name: String,
    /// The ublk device to serve the branch as, /dev/ublkbN.
    #[arg(long)]
    pub(crate) device: PathBuf,
    /// Mount through this device-mapper device, /dev/mapper/<NAME>, over the ublk device.
    #[expect(
        rustdoc::invalid_html_tags,
        reason = "the comment is the help text, where <...> marks a placeholder"
    )]
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
    /// Stop the device even while a file system on it is mounted or a program holds it open.
    /// The file system loses the writes it has not flushed.
    #[arg(long)]
    pub(crate) force: bool,
    /// How long to wait for the daemon to publish the branch and let go of the device.
    #[arg(long, value_parser = crate::parse_duration, default_value = "30m")]
    pub(crate) publish_wait: Duration,
}

/// Which attached branch `resize` grows, and to what size.
#[derive(Args, Debug)]
pub(crate) struct ResizeArgs {
    /// The ublk device the branch is served as, /dev/ublkbN.
    #[arg(long)]
    pub(crate) device: PathBuf,
    /// The new size: bytes, or a number with K, M, G or T (powers of 1024). Branches only grow.
    #[arg(value_parser = crate::parse_size)]
    pub(crate) size: u64,
}

#[cfg(target_os = "linux")]
pub(crate) use linux::{attach, detach, resize};

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

#[cfg(not(target_os = "linux"))]
pub(crate) async fn resize(_: ResizeArgs) -> anyhow::Result<()> {
    anyhow::bail!("resize needs Linux")
}

#[cfg(target_os = "linux")]
mod linux {
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use anyhow::{Context, Result, bail};
    use pando_engine::ports::DeviceSwitch;
    use pando_engine::{AttachOptions, Attachment, Cache, Ports};
    use pando_format::Hash;
    use pando_linux::{DmSwitch, Frontend, FrontendOptions};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};
    use tokio::runtime::Handle;
    use tokio::sync::watch;

    use super::{AttachArgs, DetachArgs, ResizeArgs};

    const CONTROL_DIR: &str = "/run/pando";
    /// How long a daemon waits for the request of a client that connected.
    const REQUEST_WAIT: Duration = Duration::from_secs(5);

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
        let att = Arc::new(
            Attachment::open(ports, Some(Arc::clone(&switch)), cache, &a.state, &a.name, opts).await?,
        );
        let fopts = FrontendOptions {
            queues: a.queues,
            recover: a.recover,
        };
        let mut fe = match Frontend::start(Handle::current(), Arc::clone(att.volume()), id, fopts).await {
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
        let (stop, stopping) = watch::channel(false);
        let control = match listen(id) {
            Ok(l) => Some(tokio::spawn(serve(
                l,
                Arc::clone(&att),
                id,
                a.name.clone(),
                stopping,
            ))),
            Err(e) => {
                eprintln!("{}: resize is off: {e}", control_path(id).display());
                None
            }
        };
        let background = (!att.rejoined())
            .then(|| tokio::spawn(warm(Arc::clone(&att), a.name.clone(), a.images.is_some())));
        if let Err(e) = fe.stopped().await {
            eprintln!("{}: {e}", a.device.display());
        }
        stop.send_replace(true);
        if let Some(b) = background {
            b.abort();
            let _ = b.await;
        }
        if let Some(c) = control {
            let _ = c.await;
            let _ = std::fs::remove_file(control_path(id));
        }
        let published = finish(att, &a.name).await;
        drop(fe);
        published
    }

    /// Warms `att`, then, with `hand_off`, moves its device to a local image.
    async fn warm(att: Arc<Attachment>, name: String, hand_off: bool) {
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
    }

    /// The control socket of the daemon that serves ublk device `id`.
    fn control_path(id: u32) -> PathBuf {
        Path::new(CONTROL_DIR).join(format!("ublkb{id}.sock"))
    }

    /// Listens on the control socket of device `id`, in a directory only root may enter.
    fn listen(id: u32) -> std::io::Result<UnixListener> {
        std::fs::create_dir_all(CONTROL_DIR)?;
        std::fs::set_permissions(CONTROL_DIR, Permissions::from_mode(0o700))?;
        let p = control_path(id);
        match std::fs::remove_file(&p) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
        let l = UnixListener::bind(&p)?;
        std::fs::set_permissions(&p, Permissions::from_mode(0o600))?;
        Ok(l)
    }

    /// Answers requests on `l` one at a time until `stop`. A request in progress finishes
    /// first, so a grow never stops between suspending the device and resuming it.
    async fn serve(
        l: UnixListener,
        att: Arc<Attachment>,
        id: u32,
        name: String,
        mut stop: watch::Receiver<bool>,
    ) {
        loop {
            let conn = tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                c = l.accept() => c,
            };
            match conn {
                Ok((c, _)) => {
                    if let Err(e) = answer(c, &att, id, &name).await {
                        eprintln!("resize {name}: {e:#}");
                    }
                }
                Err(e) => {
                    eprintln!("{}: {e}; resize is off", control_path(id).display());
                    return;
                }
            }
        }
    }

    async fn answer(conn: UnixStream, att: &Attachment, id: u32, name: &str) -> Result<()> {
        let (r, mut w) = conn.into_split();
        let mut line = String::new();
        tokio::time::timeout(REQUEST_WAIT, BufReader::new(r).read_line(&mut line))
            .await
            .context("no request")??;
        let reply = match parse_request(&line) {
            Ok(size) => match att.grow(size, pando_linux::resize(id, size)).await {
                Ok(()) => {
                    println!("grew {name} to {size} bytes");
                    format!("ok {size}\n")
                }
                Err(e) => format!("error {}\n", one_line(&e.to_string())),
            },
            Err(e) => format!("error {}\n", one_line(&format!("{e:#}"))),
        };
        w.write_all(reply.as_bytes()).await?;
        Ok(())
    }

    fn parse_request(line: &str) -> Result<u64> {
        match line.trim_end().split_once(' ') {
            Some(("grow", n)) => n.parse().with_context(|| format!("size {n:?}")),
            _ => bail!("unknown request {:?}", line.trim_end()),
        }
    }

    fn one_line(s: &str) -> String {
        s.replace('\n', " ")
    }

    /// Asks the daemon that serves `r.device` to grow its branch and the devices over it.
    pub(crate) async fn resize(r: ResizeArgs) -> Result<()> {
        let id = pando_linux::device_id(&r.device)?;
        let p = control_path(id);
        let conn = UnixStream::connect(&p).await.with_context(|| {
            format!(
                "connect to the daemon of {} at {}; is it attached?",
                r.device.display(),
                p.display()
            )
        })?;
        let (rd, mut w) = conn.into_split();
        w.write_all(format!("grow {}\n", r.size).as_bytes()).await?;
        w.shutdown().await?;
        let mut line = String::new();
        BufReader::new(rd).read_line(&mut line).await?;
        match line.trim_end().split_once(' ') {
            Some(("ok", _)) => {
                println!(
                    "{} is {} bytes; grow the file system on it to use them (resize2fs for ext4)",
                    r.device.display(),
                    r.size
                );
                Ok(())
            }
            Some(("error", why)) => bail!("resize {}: {why}", r.device.display()),
            _ => bail!(
                "resize {}: the daemon answered {:?}",
                r.device.display(),
                line.trim_end()
            ),
        }
    }

    /// Waits until the kernel holds the I/O of device `id` for a new daemon. A live daemon
    /// still writes the branch, so recovery must not take it over.
    async fn dead_daemon(id: u32, device: &Path) -> Result<()> {
        for _ in 0..100 {
            if pando_linux::daemon_dead(id)
                .await
                .with_context(|| format!("state of {}", device.display()))?
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
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
        if !d.force {
            if let Some(dm) = &d.dm {
                let users = switch
                    .users(dm)
                    .await
                    .with_context(|| format!("users of /dev/mapper/{dm}"))?;
                in_use(&format!("/dev/mapper/{dm}"), &users)?;
            }
            let users = pando_linux::users(&d.device, d.dm.as_deref())
                .await
                .with_context(|| format!("users of {}", d.device.display()))?;
            in_use(&d.device.display().to_string(), &users)?;
        }
        if let Some(dm) = &d.dm {
            if pando_linux::daemon_dead(id).await? {
                pando_linux::stop(id).await?;
                if switch.live_target(dm).await?.is_some() {
                    switch.clear_inactive(dm).await?;
                    switch.resume(dm).await?;
                }
                switch.remove(dm).await.with_context(|| format!("remove {dm}"))?;
                return Ok(pando_linux::delete(id, d.publish_wait).await?);
            }
            switch.remove(dm).await.with_context(|| format!("remove {dm}"))?;
        }
        pando_linux::stop(id).await?;
        Ok(pando_linux::delete(id, d.publish_wait).await?)
    }

    /// Fails while `users` hold device `dev`, so a stop cannot cut a mounted file system off.
    fn in_use(dev: &str, users: &[String]) -> Result<()> {
        if users.is_empty() {
            return Ok(());
        }
        bail!(
            "{dev} is in use: {}. Unmount it and stop what holds it, then detach again; the \
             device still serves the branch (--force stops it anyway and loses unflushed writes)",
            users.join(", ")
        )
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn a_grow_request_names_its_size() {
            assert_eq!(parse_request("grow 8589934592\n").unwrap(), 8 << 30);
            assert!(parse_request("grow\n").is_err());
            assert!(parse_request("shrink 4096\n").is_err());
            assert!(parse_request("grow -1\n").is_err());
        }

        #[test]
        fn an_unused_device_may_be_stopped() {
            in_use("/dev/ublkb1", &[]).unwrap();
        }

        #[test]
        fn a_used_device_is_refused_with_its_users() {
            let users = [
                "mounted at /mnt/b1".to_owned(),
                "under /dev/mapper/pando-b1".to_owned(),
            ];
            let e = in_use("/dev/ublkb1", &users).unwrap_err().to_string();
            assert!(e.starts_with("/dev/ublkb1 is in use: mounted at /mnt/b1, under /dev/mapper/pando-b1."));
            assert!(e.contains("still serves the branch"));
        }
    }
}
