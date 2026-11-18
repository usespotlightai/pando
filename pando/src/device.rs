//! The `attach`, `resize` and `detach` commands: a branch served as a block device, either a
//! ublk device or, with `--fuse`, a loop device over a file in a FUSE mount. An attach listens
//! on a control socket, `/run/pando/ublkbN.sock` or `/run/pando/fuse-<hash of the mount>.sock`,
//! that `resize` asks to grow it. A request is one line, `grow <bytes>`; the answer is
//! `ok <bytes>` or `error <why>`. The daemon of a FUSE mount also takes `detach` and
//! `detach force`, and answers `ok` once the branch is published.
//!
//! On SIGTERM or SIGINT an attach drains: it publishes what the branch holds, for at most
//! `--drain-wait`, and exits with [`DRAINED`] without stopping the device. The kernel holds a
//! ublk device's I/O until `attach --recover` takes it over; a FUSE mount ends with its daemon,
//! and the next `attach` of the branch on this host publishes the writes it left.

use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, Args, ValueEnum};

/// The exit status of an attach that drained and left its device to the next daemon.
pub(crate) const DRAINED: i32 = 75;

/// How an attach ended.
#[derive(Debug)]
pub(crate) enum Ended {
    /// `detach` stopped the device and the branch is published.
    Detached,
    /// A signal drained the daemon; the device waits for the next one.
    Drained(Held),
    /// A signal drained the daemon but it could not commit the writes locally; they are in the
    /// journal, and the device waits for the next daemon to replay it.
    DrainFailed(anyhow::Error, Held),
}

/// The device of a drained daemon, held open until the process exits: closing it while its
/// queues still run would hand their file descriptor to whatever opens next.
#[derive(Debug)]
pub(crate) struct Held {
    #[cfg(target_os = "linux")]
    _frontend: Served,
}

/// The frontend a daemon serves its branch through.
#[cfg(target_os = "linux")]
#[derive(Debug)]
#[expect(
    dead_code,
    reason = "a drained daemon only holds its frontend open until the process exits"
)]
enum Served {
    Ublk(pando_linux::Frontend),
    Fuse(std::sync::Arc<pando_linux::FuseFrontend>),
}

/// When `attach` takes over a device whose daemon died.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum Recover {
    /// Add a new device.
    Never,
    /// Take over the device; fail if it has none or its daemon lives.
    Always,
    /// Take over the device if it is there, as after a crash or a drain; add a new one if it is
    /// not, as after a reboot. Fail if its daemon lives.
    Auto,
}

/// How `attach` serves a branch.
#[derive(Args, Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each bool is an independent command-line flag"
)]
pub(crate) struct AttachArgs {
    pub(crate) name: String,
    /// The ublk device to serve the branch as, /dev/ublkbN.
    #[arg(long, required_unless_present = "fuse", conflicts_with = "fuse")]
    pub(crate) device: Option<PathBuf>,
    /// Serve the branch as the file disk.img in a FUSE mount at this directory, bound to a
    /// free loop device, for a kernel without ublk. The attach prints the loop device; make
    /// and mount the file system on it. Works without --dm and --images; unmount it, then
    /// `detach --fuse` publishes the branch. A signal syncs the file system and publishes,
    /// but the mount ends with the daemon, so unmount first.
    #[arg(long)]
    pub(crate) fuse: Option<PathBuf>,
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
    /// Also snapshot once writes came and none came for this long (0s: never).
    #[arg(long, value_parser = crate::parse_duration, default_value = "0s")]
    pub(crate) snapshot_idle: Duration,
    /// Snapshots read the written chunks from the image instead of cloning them when they hold
    /// at most this many bytes; they need an era device (0: always clone).
    #[arg(long, default_value_t = 0)]
    pub(crate) snapshot_copy_max: u64,
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
    /// Take over a device whose daemon died, with the requests it had in flight. `--recover`
    /// alone is `always`.
    #[arg(long, value_enum, default_value_t = Recover::Never, num_args = 0..=1, default_missing_value = "always", require_equals = true)]
    pub(crate) recover: Recover,
    /// On SIGTERM or SIGINT, how long to wait for the store to hold every write acknowledged
    /// so far before the daemon exits; the writes stay on this host either way.
    #[arg(long, value_parser = crate::parse_duration, default_value = "60s")]
    pub(crate) drain_wait: Duration,
    /// Hardware queues of a new ublk device, or threads that read FUSE requests, one thread
    /// each.
    #[arg(long, default_value_t = 2)]
    pub(crate) queues: u16,
    /// Cap the page cache's dirty data on the device the file system sits on at this many
    /// bytes, so writes reach the branch while it runs and a detach has little left to take;
    /// writers wait at the cap (0: the kernel's limits).
    #[arg(long, default_value_t = 0)]
    pub(crate) dirty_limit: u64,
    /// How flushes reach the store's write log: `behind` acknowledges a flush once it is
    /// durable on this host and logs it moments later; `strict` acknowledges it only once the
    /// log in the store has it (not with --images); `off` leaves it to the next upload.
    #[arg(long, value_enum, default_value_t = Log::Behind)]
    pub(crate) log: Log,
}

/// How `attach` logs flushes; see [`pando_engine::LogMode`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum Log {
    Off,
    Behind,
    Strict,
}

impl From<Log> for pando_engine::LogMode {
    fn from(l: Log) -> Self {
        match l {
            Log::Off => pando_engine::LogMode::Off,
            Log::Behind => pando_engine::LogMode::Behind,
            Log::Strict => pando_engine::LogMode::Strict,
        }
    }
}

/// Which device `detach` stops.
#[derive(Args, Debug)]
pub(crate) struct DetachArgs {
    /// The ublk device the branch is served as, /dev/ublkbN.
    #[arg(long, required_unless_present = "fuse", conflicts_with = "fuse")]
    pub(crate) device: Option<PathBuf>,
    /// The FUSE mount directory the branch is served in (`attach --fuse`). The daemon lets go
    /// of its loop device, unmounts and publishes.
    #[arg(long, conflicts_with = "dm")]
    pub(crate) fuse: Option<PathBuf>,
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
    #[arg(long, required_unless_present = "fuse", conflicts_with = "fuse")]
    pub(crate) device: Option<PathBuf>,
    /// The FUSE mount directory the branch is served in (`attach --fuse`).
    #[arg(long)]
    pub(crate) fuse: Option<PathBuf>,
    /// The new size: bytes, or a number with K, M, G or T (powers of 1024). Branches only grow.
    #[arg(value_parser = crate::parse_size)]
    pub(crate) size: u64,
}

#[cfg(target_os = "linux")]
pub(crate) use linux::{attach, detach, notify, resize};

#[cfg(not(target_os = "linux"))]
pub(crate) async fn attach(
    _: &pando_engine::Ports,
    _: std::sync::Arc<pando_engine::Cache>,
    _: AttachArgs,
    _: String,
    _: &crate::Out,
) -> anyhow::Result<Ended> {
    anyhow::bail!("attach needs Linux")
}

#[cfg(not(target_os = "linux"))]
pub(crate) async fn detach(_: DetachArgs) -> anyhow::Result<()> {
    anyhow::bail!("detach needs Linux")
}

#[cfg(not(target_os = "linux"))]
pub(crate) async fn resize(_: ResizeArgs, _: &crate::Out) -> anyhow::Result<()> {
    anyhow::bail!("resize needs Linux")
}

/// Tells a service manager how the daemon stands; systemd's notify protocol exists only on
/// Linux.
#[cfg(not(target_os = "linux"))]
pub(crate) fn notify(_: &str) {}

#[cfg(target_os = "linux")]
mod linux {
    use std::fs::Permissions;
    use std::io::ErrorKind;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use anyhow::{Context, Result, bail};
    use pando_engine::ports::DeviceSwitch;
    use pando_engine::{AttachOptions, Attachment, Cache, Ports};
    use pando_format::Hash;
    use pando_linux::{Daemon, DmSwitch, Frontend, FrontendOptions, FuseFrontend, FuseOptions};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::unix::OwnedWriteHalf;
    use tokio::net::{UnixListener, UnixStream};
    use tokio::runtime::Handle;
    use tokio::signal::unix::{Signal, SignalKind, signal};
    use tokio::sync::watch;
    use tokio::task::JoinHandle;

    use super::{AttachArgs, DetachArgs, Ended, Recover, ResizeArgs, Served};
    use crate::Out;

    const CONTROL_DIR: &str = "/run/pando";
    /// How long a daemon waits for the request of a client that connected.
    const REQUEST_WAIT: Duration = Duration::from_secs(5);

    /// Serves branch `a.name` until `detach` stops its device, then publishes it, or until a
    /// signal drains it. `store` names the store, so chunks learned from one store never serve
    /// another. Output goes to `out`; only an attach that runs as its own process tells
    /// systemd how it stands.
    pub(crate) async fn attach(
        ports: &Ports,
        cache: Arc<Cache>,
        a: AttachArgs,
        store: String,
        out: &Out,
    ) -> Result<Ended> {
        if let Some(dir) = a.fuse.clone() {
            return attach_fuse(ports, cache, a, dir, store, out).await;
        }
        let device = a.device.clone().context("attach needs --device or --fuse")?;
        let id = pando_linux::device_id(&device)?;
        if a.images.is_some() && a.dm.is_none() {
            bail!("--images needs --dm");
        }
        let recover = takes_over(&a, id, &device).await?;
        clear_publish_error(id)?;
        let t0 = Instant::now();
        let switch: Arc<dyn DeviceSwitch> = Arc::new(DmSwitch::new());
        let opts = options(&a, &store, recover);
        let att = Arc::new(
            Attachment::open(ports, Some(Arc::clone(&switch)), cache, &a.state, &a.name, opts).await?,
        );
        let fopts = FrontendOptions {
            queues: a.queues,
            recover,
        };
        let mut fe = match Frontend::start(Handle::current(), Arc::clone(att.volume()), id, fopts).await {
            Ok(fe) => fe,
            Err(e) => {
                if let Err(fe) = finish(att, &a.name, out).await {
                    out.err(format!("{fe:#}"));
                }
                return Err(e).with_context(|| format!("start {}", device.display()));
            }
        };
        if let Some(dm) = a.dm.as_deref().filter(|_| !recover)
            && let Err(e) = switch.create(dm, &device, att.volume().size()).await
        {
            if let Err(ae) = fe.abort().await {
                out.err(format!("stop {}: {ae}", device.display()));
            }
            if let Err(fe) = finish(att, &a.name, out).await {
                out.err(format!("{fe:#}"));
            }
            return Err(e).with_context(|| format!("device-mapper {dm} over {}", device.display()));
        }
        let mounted =
            a.dm.as_deref()
                .map_or_else(|| device.clone(), pando_linux::mapper_path);
        limit_dirty(&mounted, a.dirty_limit, out).await;
        out.line(format!(
            "attached {} as {} in {:.1?}{}",
            a.name,
            device.display(),
            t0.elapsed(),
            if att.rejoined() {
                ", rejoined its image"
            } else {
                ""
            }
        ));
        let mut signals = Signals::new()?;
        if out.is_stdio() {
            notify("READY=1");
        }
        let (stop, stopping) = watch::channel(false);
        let control = control(&att, &Front::Ublk(id), &a.name, stopping, None, out);
        let background = (!att.rejoined()).then(|| {
            tokio::spawn(warm(
                Arc::clone(&att),
                a.name.clone(),
                a.images.is_some(),
                out.clone(),
            ))
        });
        let uploads = watch_uploads(&att, &a.name, out);
        let drained = tokio::select! {
            r = fe.stopped() => {
                if let Err(e) = r {
                    out.err(format!("{}: {e}", device.display()));
                }
                false
            }
            () = signals.next() => true,
        };
        wind_down(uploads, &stop, background, control, &Front::Ublk(id)).await;
        if drained {
            if out.is_stdio() {
                notify("STOPPING=1");
            }
            let drained = drain(&att, &a.name, a.drain_wait, &mut signals, "attach --recover", out).await;
            let held = super::Held {
                _frontend: Served::Ublk(fe),
            };
            return Ok(match drained {
                Ok(()) => Ended::Drained(held),
                Err(e) => Ended::DrainFailed(e, held),
            });
        }
        finish_for_detach(att, &a.name, id, out).await?;
        drop(fe);
        Ok(Ended::Detached)
    }

    /// Serves branch `a.name` as a file in the FUSE mount at `dir`, bound to a loop device,
    /// until `detach --fuse` or an unmount ends the mount, then publishes it; or until a signal
    /// drains it.
    async fn attach_fuse(
        ports: &Ports,
        cache: Arc<Cache>,
        a: AttachArgs,
        dir: PathBuf,
        store: String,
        out: &Out,
    ) -> Result<Ended> {
        if a.dm.is_some() || a.images.is_some() {
            bail!("--fuse serves the branch without device-mapper; leave out --dm and --images");
        }
        if a.recover != Recover::Never {
            bail!(
                "--fuse cannot take over a mount: a FUSE mount ends with its daemon, and a new \
                 attach of the branch publishes what the last one left on this host"
            );
        }
        let t0 = Instant::now();
        let switch: Arc<dyn DeviceSwitch> = Arc::new(DmSwitch::new());
        let opts = options(&a, &store, false);
        let att = Arc::new(Attachment::open(ports, Some(switch), cache, &a.state, &a.name, opts).await?);
        let fopts = FuseOptions { threads: a.queues };
        let fe = match FuseFrontend::start(Handle::current(), Arc::clone(att.volume()), &dir, fopts).await {
            Ok(fe) => Arc::new(fe),
            Err(e) => {
                if let Err(fe) = finish(att, &a.name, out).await {
                    out.err(format!("{fe:#}"));
                }
                return Err(e).with_context(|| format!("serve {} over FUSE", dir.display()));
            }
        };
        limit_dirty(fe.device(), a.dirty_limit, out).await;
        out.line(format!(
            "attached {} as {} in {:.1?}: {}, {}",
            a.name,
            fe.device().display(),
            t0.elapsed(),
            fe.file().display(),
            if fe.direct_io() {
                "direct I/O"
            } else {
                "buffered: the loop device refused direct I/O"
            }
        ));
        let mut signals = Signals::new()?;
        if out.is_stdio() {
            notify("READY=1");
        }
        let front = Front::Fuse(dir.clone(), Arc::clone(&fe));
        let (stop, stopping) = watch::channel(false);
        let waiting = Waiting::default();
        let control = control(&att, &front, &a.name, stopping, Some(Arc::clone(&waiting)), out);
        let background = (!att.rejoined())
            .then(|| tokio::spawn(warm(Arc::clone(&att), a.name.clone(), false, out.clone())));
        let uploads = watch_uploads(&att, &a.name, out);
        let drained = tokio::select! {
            r = fe.stopped() => {
                if let Err(e) = r {
                    out.err(format!("{}: {e}", dir.display()));
                }
                false
            }
            () = signals.next() => true,
        };
        if drained {
            sync_users(&fe, a.drain_wait, out).await;
        }
        wind_down(uploads, &stop, background, control, &front).await;
        drop(front);
        if drained {
            if out.is_stdio() {
                notify("STOPPING=1");
            }
            let drained = drain(&att, &a.name, a.drain_wait, &mut signals, "attach", out).await;
            let held = super::Held {
                _frontend: Served::Fuse(fe),
            };
            return Ok(match drained {
                Ok(()) => Ended::Drained(held),
                Err(e) => Ended::DrainFailed(e, held),
            });
        }
        drop(fe);
        let finished = finish(att, &a.name, out).await;
        let waiter = waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(mut w) = waiter {
            let reply = match &finished {
                Ok(()) => "ok\n".to_owned(),
                Err(e) => format!("error {}\n", one_line(&format!("{e:#}"))),
            };
            if let Err(e) = w.write_all(reply.as_bytes()).await {
                out.err(format!("answer the detach of {}: {e}", a.name));
            }
        }
        finished?;
        Ok(Ended::Detached)
    }

    /// Stops the tasks an attach ran beside its frontend once the frontend stopped or a signal
    /// came: the upload watch, the warming and the control socket of `front`.
    async fn wind_down(
        uploads: JoinHandle<()>,
        stop: &watch::Sender<bool>,
        background: Option<JoinHandle<()>>,
        control: Option<JoinHandle<()>>,
        front: &Front,
    ) {
        uploads.abort();
        stop.send_replace(true);
        if let Some(b) = background {
            b.abort();
            let _ = b.await;
        }
        if let Some(c) = control {
            let _ = c.await;
            let _ = std::fs::remove_file(front.control_path());
        }
    }

    /// Reports why the uploads of branch `name` stopped, once they stop for good.
    fn watch_uploads(att: &Attachment, name: &str, out: &Out) -> JoinHandle<()> {
        let (v, name, out) = (Arc::clone(att.volume()), name.to_owned(), out.clone());
        tokio::spawn(async move {
            let e = v.upload_stopped().await;
            out.err(format!("uploads of {name} stopped: {e}"));
        })
    }

    /// Publishes the branch as [`finish`] does, and leaves an error for the `detach` that
    /// waits for the daemon of device `id`.
    async fn finish_for_detach(att: Arc<Attachment>, name: &str, id: u32, out: &Out) -> Result<()> {
        let r = finish(att, name, out).await;
        if let Err(e) = &r {
            leave_publish_error(id, e, out);
        }
        r
    }

    /// Where, in `dir`, the daemon of ublk device `id` leaves why it could not publish its
    /// branch, for the `detach` that waits for it.
    fn publish_error(dir: &Path, id: u32) -> PathBuf {
        dir.join(format!("ublk{id}.publish-error"))
    }

    /// Removes the publish error an earlier daemon of device `id` left.
    fn clear_publish_error(id: u32) -> Result<()> {
        let p = publish_error(Path::new(CONTROL_DIR), id);
        match std::fs::remove_file(&p) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("remove {}", p.display())),
        }
    }

    /// Leaves `e` for the `detach` that waits for the daemon of device `id`.
    fn leave_publish_error(id: u32, e: &anyhow::Error, out: &Out) {
        let p = publish_error(Path::new(CONTROL_DIR), id);
        if let Err(we) =
            std::fs::create_dir_all(CONTROL_DIR).and_then(|()| std::fs::write(&p, format!("{e:#}")))
        {
            out.err(format!("{}: {we}", p.display()));
        }
    }

    /// Fails with the error the daemon of ublk device `id` left in `dir`, once it let go of
    /// the device.
    fn published(dir: &Path, id: u32, device: &Path) -> Result<()> {
        let p = publish_error(dir, id);
        match std::fs::read_to_string(&p) {
            Ok(e) => {
                let _ = std::fs::remove_file(&p);
                bail!(
                    "the daemon of {} did not publish its branch: {e}",
                    device.display()
                )
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("read {}", p.display())),
        }
    }

    /// The frontend a daemon serves, as its control socket knows it.
    #[derive(Clone, Debug)]
    enum Front {
        Ublk(u32),
        Fuse(PathBuf, Arc<FuseFrontend>),
    }

    impl Front {
        /// The control socket of the daemon.
        fn control_path(&self) -> PathBuf {
            match self {
                Front::Ublk(id) => control_path(*id),
                Front::Fuse(dir, _) => fuse_control_path(dir),
            }
        }

        /// Makes the device see the volume's new size `size`.
        async fn resize(&self, size: u64) -> std::io::Result<()> {
            match self {
                Front::Ublk(id) => pando_linux::resize(*id, size).await,
                Front::Fuse(_, fe) => fe.resize().await,
            }
        }
    }

    /// Where a `detach` of a FUSE mount waits for the daemon's answer.
    type Waiting = Arc<std::sync::Mutex<Option<OwnedWriteHalf>>>;

    /// Serves the control socket of `front` until `stopping`, or until a `detach` of a FUSE
    /// mount leaves its connection in `waiting`; `None` when it cannot listen.
    fn control(
        att: &Arc<Attachment>,
        front: &Front,
        name: &str,
        stopping: watch::Receiver<bool>,
        waiting: Option<Waiting>,
        out: &Out,
    ) -> Option<JoinHandle<()>> {
        let path = front.control_path();
        match listen(&path) {
            Ok(l) => Some(tokio::spawn(serve(
                l,
                Arc::clone(att),
                front.clone(),
                name.to_owned(),
                stopping,
                waiting,
                out.clone(),
            ))),
            Err(e) => {
                let off = if waiting.is_some() {
                    "resize and `detach --fuse` are off; let go of the loop device and unmount the directory to detach"
                } else {
                    "resize is off"
                };
                out.err(format!("{}: {off}: {e}", path.display()));
                None
            }
        }
    }

    /// Whether this attach takes over ublk device `device`, number `id`, of a daemon that
    /// died; it fails while a daemon still serves the device.
    async fn takes_over(a: &AttachArgs, id: u32, device: &Path) -> Result<bool> {
        let recover = match a.recover {
            Recover::Never => false,
            Recover::Always => true,
            Recover::Auto => {
                pando_linux::daemon(id)
                    .await
                    .with_context(|| format!("state of {}", device.display()))?
                    != Daemon::NoDevice
            }
        };
        if recover {
            dead_daemon(id, device).await?;
        }
        Ok(recover)
    }

    /// How `a` asks for the branch to be served. `store` names the store, so chunks learned
    /// from one store never serve another.
    fn options(a: &AttachArgs, store: &str, rejoin: bool) -> AttachOptions {
        let learned = a
            .images
            .as_ref()
            .filter(|_| a.learn)
            .map(|d| d.join("learned").join(Hash::of(store.as_bytes()).short()));
        AttachOptions {
            images: a.images.clone(),
            device: a.dm.clone(),
            share_bases: a.share_bases,
            dedup: a.dedup,
            learned,
            snapshot_every: a.snapshot,
            snapshot_bytes: a.snapshot_bytes,
            snapshot_idle: a.snapshot_idle,
            snapshot_copy_max: a.snapshot_copy_max,
            prefetch: a.prefetch,
            record: a.record,
            metadata: a.metadata,
            hydrate: a.hydrate,
            rejoin,
            log: a.log.into(),
        }
    }

    /// SIGTERM and SIGINT.
    struct Signals {
        term: Signal,
        int: Signal,
    }

    impl Signals {
        fn new() -> Result<Signals> {
            Ok(Signals {
                term: signal(SignalKind::terminate()).context("listen for SIGTERM")?,
                int: signal(SignalKind::interrupt()).context("listen for SIGINT")?,
            })
        }

        async fn next(&mut self) {
            tokio::select! {
                _ = self.term.recv() => {}
                _ = self.int.recv() => {}
            }
        }
    }

    /// Publishes what the branch holds while the device serves, for at most `wait` or until a
    /// second signal, then makes every write acknowledged so far durable on this host.
    async fn drain(
        att: &Attachment,
        name: &str,
        wait: Duration,
        signals: &mut Signals,
        publisher: &str,
        out: &Out,
    ) -> Result<()> {
        let t0 = Instant::now();
        out.line(format!("draining {name}: publishing for at most {wait:.0?}"));
        let published = tokio::select! {
            r = tokio::time::timeout(wait, att.drain()) => match r {
                Ok(Ok(())) => true,
                Ok(Err(e)) => {
                    out.err(format!("drain {name}: {e:#}"));
                    false
                }
                Err(_) => false,
            },
            () = signals.next() => false,
        };
        att.volume()
            .flush()
            .await
            .with_context(|| format!("drain {name}: commit the writes locally"))?;
        if published {
            out.line(format!(
                "drained {name} in {:.1?}: the store holds every write acknowledged before the signal",
                t0.elapsed()
            ));
        } else {
            out.line(format!(
                "drained {name} in {:.1?}: the writes are on this host; `{publisher}` publishes them",
                t0.elapsed()
            ));
        }
        Ok(())
    }

    /// Tells systemd, when it started this daemon with `Type=notify`, how the daemon stands.
    pub(crate) fn notify(state: &str) {
        if let Some(to) = std::env::var_os("NOTIFY_SOCKET")
            && let Err(e) = notify_to(&to, state)
        {
            eprintln!("tell systemd {state}: {e}");
        }
    }

    /// Sends `state` to the systemd notify socket `to`: a path, or an abstract name after `@`.
    fn notify_to(to: &std::ffi::OsStr, state: &str) -> std::io::Result<()> {
        use std::os::linux::net::SocketAddrExt;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::net::{SocketAddr, UnixDatagram};
        let addr = match to.as_bytes().strip_prefix(b"@") {
            Some(name) => SocketAddr::from_abstract_name(name)?,
            None => SocketAddr::from_pathname(to)?,
        };
        UnixDatagram::unbound()?.send_to_addr(state.as_bytes(), &addr)?;
        Ok(())
    }

    /// Caps the dirty page cache of device `mounted` at `bytes`, unless it is 0. A failure
    /// leaves the kernel's limits in force.
    async fn limit_dirty(mounted: &Path, bytes: u64, out: &Out) {
        if bytes == 0 {
            return;
        }
        if let Err(e) = pando_linux::limit_dirty(mounted, bytes).await {
            out.err(format!("{}: the dirty limit is off: {e}", mounted.display()));
        }
    }

    /// Warms `att`, then, with `hand_off`, moves its device to a local image.
    async fn warm(att: Arc<Attachment>, name: String, hand_off: bool, out: Out) {
        match att.warm().await {
            Ok(w) => out.line(format!("warmed {name}: {w:?}")),
            Err(e) => return out.err(format!("warm {name}: {e}")),
        }
        if hand_off {
            match att.hand_off().await {
                Ok(h) => out.line(format!("handed {name} off: {h:?}")),
                Err(e) => out.err(format!("handoff {name}: {e}")),
            }
        }
    }

    /// The control socket of the daemon that serves ublk device `id`.
    fn control_path(id: u32) -> PathBuf {
        Path::new(CONTROL_DIR).join(format!("ublkb{id}.sock"))
    }

    /// Writes out the file systems on a FUSE attach's loop device before a signal ends the
    /// daemon, which ends the FUSE mount under them.
    async fn sync_users(fe: &FuseFrontend, wait: Duration, out: &Out) {
        match tokio::time::timeout(wait, fe.sync_users()).await {
            Ok(Ok(n)) => out.line(format!("synced {n} file systems on {}", fe.device().display())),
            Ok(Err(e)) => out.err(format!("sync the file systems on {}: {e}", fe.device().display())),
            Err(_) => out.err(format!(
                "sync the file systems on {}: not done in {wait:?}",
                fe.device().display()
            )),
        }
    }

    /// The control socket of the daemon that serves a branch in the FUSE mount at `dir`, the
    /// same for every spelling of the path.
    fn fuse_control_path(dir: &Path) -> PathBuf {
        let dir: PathBuf = dir.components().collect();
        let h = Hash::of(dir.as_os_str().as_encoded_bytes()).short();
        Path::new(CONTROL_DIR).join(format!("fuse-{h}.sock"))
    }

    /// Listens on control socket `p`, in a directory only root may enter.
    fn listen(p: &Path) -> std::io::Result<UnixListener> {
        std::fs::create_dir_all(CONTROL_DIR)?;
        std::fs::set_permissions(CONTROL_DIR, Permissions::from_mode(0o700))?;
        match std::fs::remove_file(p) {
            Err(e) if e.kind() != ErrorKind::NotFound => return Err(e),
            _ => {}
        }
        let l = UnixListener::bind(p)?;
        std::fs::set_permissions(p, Permissions::from_mode(0o600))?;
        Ok(l)
    }

    /// Answers requests on `l` one at a time until `stop`, or until a `detach` leaves its
    /// connection in `waiting`. A request in progress finishes first, so a grow never stops
    /// between suspending the device and resuming it.
    async fn serve(
        l: UnixListener,
        att: Arc<Attachment>,
        front: Front,
        name: String,
        mut stop: watch::Receiver<bool>,
        waiting: Option<Waiting>,
        out: Out,
    ) {
        loop {
            let conn = tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                c = l.accept() => c,
            };
            match conn {
                Ok((c, _)) => match answer(c, &att, &front, &name, waiting.as_ref(), &out).await {
                    Ok(true) => return,
                    Ok(false) => {}
                    Err(e) => out.err(format!("control {name}: {e:#}")),
                },
                Err(e) => {
                    let p = front.control_path();
                    out.err(format!("{}: {e}; resize is off", p.display()));
                    return;
                }
            }
        }
    }

    /// Answers one request; `true` when it was a `detach` that now waits in `waiting`.
    async fn answer(
        conn: UnixStream,
        att: &Attachment,
        front: &Front,
        name: &str,
        waiting: Option<&Waiting>,
        out: &Out,
    ) -> Result<bool> {
        let (r, mut w) = conn.into_split();
        let mut line = String::new();
        tokio::time::timeout(REQUEST_WAIT, BufReader::new(r).read_line(&mut line))
            .await
            .context("no request")??;
        let reply = match (parse_request(&line), front, waiting) {
            (Ok(Request::Grow(size)), ..) => match att.grow(size, front.resize(size)).await {
                Ok(()) => {
                    out.line(format!("grew {name} to {size} bytes"));
                    format!("ok {size}\n")
                }
                Err(e) => format!("error {}\n", one_line(&e.to_string())),
            },
            (Ok(Request::Detach { force }), Front::Fuse(_, fe), Some(waiting)) => {
                match stop_fuse(fe, force).await {
                    Ok(()) => {
                        out.line(format!("detaching {name}: publishing it"));
                        *waiting.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(w);
                        return Ok(true);
                    }
                    Err(e) => format!("error {}\n", one_line(&format!("{e:#}"))),
                }
            }
            (Ok(Request::Detach { .. }), ..) => {
                "error detach a ublk device with `detach --device`\n".to_owned()
            }
            (Err(e), ..) => format!("error {}\n", one_line(&format!("{e:#}"))),
        };
        w.write_all(reply.as_bytes()).await?;
        Ok(false)
    }

    /// A request on the control socket.
    #[derive(Debug, PartialEq, Eq)]
    enum Request {
        Grow(u64),
        Detach { force: bool },
    }

    fn parse_request(line: &str) -> Result<Request> {
        match line.trim_end().split_once(' ') {
            Some(("grow", n)) => Ok(Request::Grow(n.parse().with_context(|| format!("size {n:?}"))?)),
            Some(("detach", "force")) => Ok(Request::Detach { force: true }),
            None if line.trim_end() == "detach" => Ok(Request::Detach { force: false }),
            _ => bail!("unknown request {:?}", line.trim_end()),
        }
    }

    /// Lets go of the loop device of a FUSE mount and unmounts it, unless, without `force`,
    /// something still uses the loop device.
    async fn stop_fuse(fe: &FuseFrontend, force: bool) -> Result<()> {
        if !force {
            let users = pando_linux::users(fe.device(), None)
                .await
                .with_context(|| format!("users of {}", fe.device().display()))?;
            in_use(&fe.device().display().to_string(), &users)?;
        }
        fe.unmount(force)
            .await
            .with_context(|| format!("unmount {}", fe.file().display()))
    }

    fn one_line(s: &str) -> String {
        s.replace('\n', " ")
    }

    /// The control socket of the daemon that serves `device` or the FUSE mount `fuse`, and
    /// the name of what it serves.
    fn socket_of(device: Option<&Path>, fuse: Option<&Path>) -> Result<(PathBuf, PathBuf)> {
        match (device, fuse) {
            (_, Some(dir)) => Ok((fuse_control_path(dir), dir.to_owned())),
            (Some(dev), None) => Ok((control_path(pando_linux::device_id(dev)?), dev.to_owned())),
            (None, None) => bail!("name the branch's device with --device or --fuse"),
        }
    }

    /// Asks the daemon that serves `r.device` to grow its branch and the devices over it.
    pub(crate) async fn resize(r: ResizeArgs, out: &Out) -> Result<()> {
        let (p, served) = socket_of(r.device.as_deref(), r.fuse.as_deref())?;
        let conn = UnixStream::connect(&p).await.with_context(|| {
            format!(
                "connect to the daemon of {} at {}; is it attached?",
                served.display(),
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
                out.line(format!(
                    "{} is {} bytes; grow the file system on it to use them (resize2fs for ext4)",
                    served.display(),
                    r.size
                ));
                Ok(())
            }
            Some(("error", why)) => bail!("resize {}: {why}", served.display()),
            _ => bail!(
                "resize {}: the daemon answered {:?}",
                served.display(),
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
    async fn finish(att: Arc<Attachment>, name: &str, out: &Out) -> Result<()> {
        let t0 = Instant::now();
        let att = Arc::into_inner(att).with_context(|| format!("{name}: the attachment is still in use"))?;
        if let Some(s) = att.snapshots() {
            out.line(format!("snapshots of {name}: {s:?}"));
        }
        let cache = Arc::clone(att.cache());
        let r = att.finish().await.with_context(|| format!("publish {name}"))?;
        if let Some(e) = &r.log_open {
            out.err(format!(
                "{name}: the write log stays open, so the next open reads it: {e}"
            ));
        }
        let total = t0.elapsed();
        out.line(format!("published {name} in {total:.1?}: {r:?}"));
        let c = &r.checkpoint;
        let last = c.last.unwrap_or_default();
        let s = cache.stats();
        let load = |a: &std::sync::atomic::AtomicU64| a.load(std::sync::atomic::Ordering::Relaxed);
        out.line(format!(
            "publish times {name}: total={:.4} absorb={:.4} flush={:.4} wait={:.4} pack={:.4} ref={:.4} \
             after={:.4} passes_before={} bytes_before={} passes={} bytes={} stalls={} stall={:.4} \
             fetched_bytes={}",
            total.as_secs_f64(),
            r.absorb.as_secs_f64(),
            c.flush.as_secs_f64(),
            c.wait.as_secs_f64(),
            last.pack.as_secs_f64(),
            last.publish.as_secs_f64(),
            r.after.as_secs_f64(),
            c.passes_before,
            c.bytes_before,
            c.passes,
            c.bytes,
            load(&s.stalls),
            Duration::from_nanos(load(&s.stall_nanos)).as_secs_f64(),
            load(&s.bytes_fetched),
        ));
        Ok(())
    }

    /// Stops the device of `d`. A live daemon publishes the branch before the device is
    /// deleted, so this returns once the branch is in the store, or fails with why the daemon
    /// could not publish it.
    pub(crate) async fn detach(d: DetachArgs) -> Result<()> {
        if let Some(dir) = &d.fuse {
            return detach_fuse(dir, d.force, d.publish_wait).await;
        }
        let device = d.device.clone().context("detach needs --device or --fuse")?;
        detach_ublk(&d, &device).await
    }

    /// Asks the daemon of the FUSE mount at `dir` to let go of its loop device, unmount and
    /// publish; returns once it answers, or after `wait`.
    async fn detach_fuse(dir: &Path, force: bool, wait: Duration) -> Result<()> {
        let p = fuse_control_path(dir);
        let conn = UnixStream::connect(&p).await.with_context(|| {
            format!(
                "connect to the daemon of {} at {}; is it attached?",
                dir.display(),
                p.display()
            )
        })?;
        let (rd, mut w) = conn.into_split();
        let req = if force { "detach force\n" } else { "detach\n" };
        w.write_all(req.as_bytes()).await?;
        w.shutdown().await?;
        let mut line = String::new();
        let n = tokio::time::timeout(wait, BufReader::new(rd).read_line(&mut line))
            .await
            .with_context(|| {
                format!(
                    "the daemon of {} did not answer within {wait:?}; it may still be publishing \
                     its branch, or something still holds its loop device",
                    dir.display()
                )
            })??;
        match line.trim_end().split_once(' ') {
            _ if n == 0 => bail!(
                "the daemon of {} ended without answering; the writes it took are on this host, \
                 and the next attach of the branch publishes them",
                dir.display()
            ),
            None if line.trim_end() == "ok" => Ok(()),
            Some(("error", why)) => bail!("detach {}: {why}", dir.display()),
            _ => bail!(
                "detach {}: the daemon answered {:?}",
                dir.display(),
                line.trim_end()
            ),
        }
    }

    /// Stops ublk device `device`, as [`detach`] does.
    async fn detach_ublk(d: &DetachArgs, device: &Path) -> Result<()> {
        let id = pando_linux::device_id(device)?;
        let switch = DmSwitch::new();
        if !d.force {
            if let Some(dm) = &d.dm {
                let users = switch
                    .users(dm)
                    .await
                    .with_context(|| format!("users of /dev/mapper/{dm}"))?;
                in_use(&format!("/dev/mapper/{dm}"), &users)?;
            }
            let users = pando_linux::users(device, d.dm.as_deref())
                .await
                .with_context(|| format!("users of {}", device.display()))?;
            in_use(&device.display().to_string(), &users)?;
        }
        if let Some(dm) = &d.dm {
            if pando_linux::daemon_dead(id).await? {
                pando_linux::stop(id).await?;
                if switch.live_target(dm).await?.is_some() {
                    switch.clear_inactive(dm).await?;
                    switch.resume(dm).await?;
                }
                switch.remove(dm).await.with_context(|| format!("remove {dm}"))?;
                pando_linux::delete(id, d.publish_wait).await?;
                return published(Path::new(CONTROL_DIR), id, device);
            }
            switch.remove(dm).await.with_context(|| format!("remove {dm}"))?;
        }
        let lock = if d.force || pando_linux::daemon_dead(id).await? {
            None
        } else {
            pando_linux::lock(device, Duration::from_secs(10))
                .await
                .with_context(|| format!("lock {}", device.display()))?
        };
        pando_linux::stop(id).await?;
        drop(lock);
        pando_linux::delete(id, d.publish_wait).await?;
        published(Path::new(CONTROL_DIR), id, device)
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
        fn every_spelling_of_a_fuse_directory_names_one_socket() {
            let want = fuse_control_path(Path::new("/mnt/x"));
            assert_eq!(fuse_control_path(Path::new("/mnt/x/")), want);
            assert_eq!(fuse_control_path(Path::new("/mnt/./x")), want);
            assert_ne!(fuse_control_path(Path::new("/mnt/y")), want);
        }

        #[test]
        fn a_grow_request_names_its_size() {
            assert_eq!(
                parse_request("grow 8589934592\n").unwrap(),
                Request::Grow(8 << 30)
            );
            assert_eq!(
                parse_request("detach\n").unwrap(),
                Request::Detach { force: false }
            );
            assert_eq!(
                parse_request("detach force\n").unwrap(),
                Request::Detach { force: true }
            );
            assert!(parse_request("detach now\n").is_err());
            assert!(parse_request("grow\n").is_err());
            assert!(parse_request("shrink 4096\n").is_err());
            assert!(parse_request("grow -1\n").is_err());
        }

        #[test]
        fn systemd_hears_on_a_path_socket_and_an_abstract_one() {
            use std::os::linux::net::SocketAddrExt;
            use std::os::unix::net::{SocketAddr, UnixDatagram};
            let tag = format!("pando-notify-{}", std::process::id());
            let path = std::env::temp_dir().join(&tag);
            let _ = std::fs::remove_file(&path);
            let at_path = UnixDatagram::bind(&path).unwrap();
            let abstract_addr = SocketAddr::from_abstract_name(tag.as_bytes()).unwrap();
            let at_name = UnixDatagram::bind_addr(&abstract_addr).unwrap();
            let mut buf = [0; 64];
            notify_to(path.as_os_str(), "READY=1").unwrap();
            let n = at_path.recv(&mut buf).unwrap();
            assert_eq!(&buf[..n], b"READY=1");
            notify_to(format!("@{tag}").as_ref(), "STOPPING=1").unwrap();
            let n = at_name.recv(&mut buf).unwrap();
            assert_eq!(&buf[..n], b"STOPPING=1");
            std::fs::remove_file(&path).unwrap();
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

        #[test]
        fn detach_fails_with_the_error_the_daemon_left_once() {
            let dir = tempfile::tempdir().unwrap();
            let dev = Path::new("/dev/ublkb7");
            published(dir.path(), 7, dev).unwrap();
            std::fs::write(publish_error(dir.path(), 7), "publish main: refused").unwrap();
            let e = published(dir.path(), 7, dev).unwrap_err().to_string();
            assert_eq!(
                e,
                "the daemon of /dev/ublkb7 did not publish its branch: publish main: refused"
            );
            published(dir.path(), 7, dev).unwrap();
        }
    }
}
