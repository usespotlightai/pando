//! The `pando` command: manages branches in the store and serves them as block devices.

#![forbid(unsafe_code)]
#![expect(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "a command-line tool reports to its terminal"
)]

mod config;
mod device;
mod serve;

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use pando_engine::gc::{self, GcOptions};
use pando_engine::store_format::{Access, check_format};
use pando_engine::{Cache, Ports, Volume, base, branch};
use pando_format::{Ref, StoreFormat, keys};
use serve::Out;
use tokio::sync::{OnceCell, watch};

const DEFAULT_CACHE: &str = "/data/cache";

#[derive(Parser, Debug)]
#[command(version, about = "Branchable copy-on-write block devices on object storage")]
struct Cli {
    #[command(flatten)]
    store: config::StoreArgs,
    /// The local cache directory [default: /data/cache].
    #[arg(long, global = true)]
    cache: Option<PathBuf>,
    /// The socket of the host daemon: `serve` listens on it, and every other command is sent
    /// to the daemon on it and runs there.
    #[arg(long, global = true, env = "PANDO_SOCKET")]
    socket: Option<PathBuf>,
    /// Threads for CPU work (hashing, compression).
    #[arg(long, global = true, default_value_t = 8)]
    cpu_threads: usize,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Lists the branches in the store: name, size, writer generation and root.
    Refs,
    /// Creates an empty branch of SIZE bytes.
    Init { name: String, size: u64 },
    /// Creates a branch with the bytes of a disk image.
    Import { name: String, image: PathBuf },
    /// Copies branch SRC, or checkpoint SRC with --checkpoint, to a new branch DST.
    Branch {
        src: String,
        dst: String,
        /// SRC names a checkpoint. A daemon that has read the checkpoint before creates DST in
        /// one round trip to the store.
        #[arg(long)]
        checkpoint: bool,
        /// Undo a copy slower than this: GC must find it within its validate delay.
        #[arg(long, value_parser = parse_duration, default_value = "15s")]
        max_age: Duration,
    },
    /// Freezes the published root of branch SRC as checkpoint NAME. A checkpoint never changes;
    /// GC keeps what it reaches until it is deleted.
    Checkpoint {
        src: String,
        name: String,
        /// Undo a checkpoint slower than this: GC must find it within its validate delay.
        #[arg(long, value_parser = parse_duration, default_value = "15s")]
        max_age: Duration,
    },
    /// Lists the checkpoints: name, size, the branch it was taken from and root.
    Checkpoints,
    /// Deletes a checkpoint; what only it reaches goes at the next GC.
    DeleteCheckpoint { name: String },
    /// Runs the host daemon on --socket: it keeps the store's connections, the cache and the
    /// checkpoints it read for the commands sent to it. Devices it serves stop with it;
    /// `attach --recover` takes them over.
    Serve,
    /// Moves branch DST to the root of branch SRC, as a merge does.
    Advance { dst: String, src: String },
    /// Deletes a branch; its segments go at the next GC that finds them unreached.
    Delete { name: String },
    /// Builds this host's base image of a branch's root before any branch asks.
    Base {
        name: String,
        /// The directory of the host's base images.
        #[arg(long)]
        dir: PathBuf,
        #[arg(long, default_value_t = 16)]
        parallel: usize,
    },
    /// Builds the base image of each new root of the branches this host's branches were copied from.
    Follow {
        #[arg(long)]
        dir: PathBuf,
        /// How often to check the followed branches.
        #[arg(long, value_parser = parse_duration, default_value = "15s")]
        interval: Duration,
        /// Base images to keep, most recently used first.
        #[arg(long, default_value_t = 4)]
        keep: usize,
        /// At most this many bytes of disk for base images (0: no limit).
        #[arg(long, default_value_t = 0)]
        budget: u64,
        #[arg(long, default_value_t = 16)]
        parallel: usize,
        /// Check once and exit.
        #[arg(long)]
        once: bool,
    },
    /// Deletes segments no branch reaches and change lists no branch reads.
    Gc {
        /// Delete what is dead; without it GC only reports.
        #[arg(long)]
        delete: bool,
        /// Keep segments uploaded less than this long ago, with what they depend on.
        #[arg(long, value_parser = parse_duration, default_value = "24h")]
        keep_young: Duration,
        /// Mark again after this long; branch copies must finish within it.
        #[arg(long, value_parser = parse_duration, default_value = "30s")]
        validate: Duration,
    },
    /// Shows what the store holds.
    Stat,
    /// Serves a branch as a block device until `detach`, then publishes it.
    Attach(device::AttachArgs),
    /// Stops the device of an attached branch; returns once the branch is published, or fails
    /// with why the daemon could not publish it.
    Detach(device::DetachArgs),
    /// Grows an attached branch and the devices over it while it serves. The file system on
    /// it keeps its size until it is grown too, with resize2fs for ext4.
    Resize(device::ResizeArgs),
}

impl Command {
    /// How the command uses the store; `None` when it only talks to an attached device's
    /// daemon, or runs other commands that each say for themselves.
    fn access(&self) -> Option<Access> {
        match self {
            Command::Detach(_) | Command::Resize(_) | Command::Serve => None,
            Command::Refs | Command::Stat | Command::Checkpoints | Command::Gc { delete: false, .. } => {
                Some(Access::Read)
            }
            Command::Init { .. }
            | Command::Import { .. }
            | Command::Branch { .. }
            | Command::Checkpoint { .. }
            | Command::DeleteCheckpoint { .. }
            | Command::Advance { .. }
            | Command::Delete { .. }
            | Command::Base { .. }
            | Command::Follow { .. }
            | Command::Gc { delete: true, .. }
            | Command::Attach(_) => Some(Access::Write),
        }
    }

    /// Whether the command works through this host's cache.
    fn uses_cache(&self) -> bool {
        matches!(
            self,
            Command::Import { .. }
                | Command::Base { .. }
                | Command::Follow { .. }
                | Command::Gc { .. }
                | Command::Stat
                | Command::Attach(_)
        )
    }
}

impl Cli {
    /// Makes every path argument absolute, against `cwd` where it is relative.
    fn resolve(&mut self, cwd: &Path) {
        let abs = |p: &mut PathBuf| *p = cwd.join(&*p);
        if let Some(c) = &mut self.cache {
            abs(c);
        }
        match &mut self.command {
            Command::Import { image, .. } => abs(image),
            Command::Base { dir, .. } | Command::Follow { dir, .. } => abs(dir),
            Command::Attach(a) => {
                if let Some(d) = &mut a.device {
                    abs(d);
                }
                if let Some(f) = &mut a.fuse {
                    abs(f);
                }
                abs(&mut a.state);
                if let Some(i) = &mut a.images {
                    abs(i);
                }
            }
            Command::Detach(d) => {
                for p in [&mut d.device, &mut d.fuse].into_iter().flatten() {
                    abs(p);
                }
            }
            Command::Resize(r) => {
                for p in [&mut r.device, &mut r.fuse].into_iter().flatten() {
                    abs(p);
                }
            }
            Command::Refs
            | Command::Init { .. }
            | Command::Branch { .. }
            | Command::Checkpoint { .. }
            | Command::Checkpoints
            | Command::DeleteCheckpoint { .. }
            | Command::Serve
            | Command::Advance { .. }
            | Command::Delete { .. }
            | Command::Gc { .. }
            | Command::Stat => {}
        }
    }
}

/// Parses durations such as `500ms`, `15s`, `10m` and `24h`.
pub(crate) fn parse_duration(s: &str) -> Result<Duration> {
    let at = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (n, unit) = s.split_at(at);
    let n: u64 = n.parse().with_context(|| format!("duration {s:?}"))?;
    Ok(match unit {
        "ms" => Duration::from_millis(n),
        "s" | "" => Duration::from_secs(n),
        "m" => Duration::from_secs(n * 60),
        "h" => Duration::from_secs(n * 3600),
        _ => bail!("duration {s:?}: the unit must be ms, s, m or h"),
    })
}

/// Parses sizes such as `4096`, `512M` and `8G`: bytes, or a number with K, M, G or T in
/// powers of 1024.
pub(crate) fn parse_size(s: &str) -> Result<u64> {
    let at = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (n, unit) = s.split_at(at);
    let n: u64 = n.parse().with_context(|| format!("size {s:?}"))?;
    let shift = match unit {
        "" => 0,
        "K" => 10,
        "M" => 20,
        "G" => 30,
        "T" => 40,
        _ => bail!("size {s:?}: the unit must be K, M, G or T"),
    };
    n.checked_mul(1 << shift)
        .with_context(|| format!("size {s:?} is too large"))
}

/// What the commands of one process share: the ports, the cache, opened on first use, the
/// store's format as checked so far, the checkpoints read so far, and the devices of attaches
/// that drained, held until the process exits.
#[derive(Debug)]
pub(crate) struct Host {
    ports: Ports,
    cache_dir: PathBuf,
    cache: OnceCell<Arc<Cache>>,
    readable: OnceCell<StoreFormat>,
    marked: OnceCell<StoreFormat>,
    checkpoints: branch::Checkpoints,
    remote: String,
    identity: String,
    held: Mutex<Vec<device::Held>>,
    drain_failed: AtomicBool,
    attaches: watch::Sender<usize>,
}

/// The error of an attach that drained: its device waits for the next daemon.
#[derive(Debug)]
pub(crate) struct Drained;

impl std::fmt::Display for Drained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("drained; the device waits for `attach --recover`")
    }
}

impl std::error::Error for Drained {}

/// Counts an attach in [`Host::attaches`] while it lives.
struct Attaching<'a>(&'a watch::Sender<usize>);

impl Drop for Attaching<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|n| *n -= 1);
    }
}

impl Host {
    fn attaching(&self) -> Attaching<'_> {
        self.attaches.send_modify(|n| *n += 1);
        Attaching(&self.attaches)
    }

    /// Keeps the device of an attach that drained; [`Drained`] or the drain's error then.
    fn ended(&self, ended: device::Ended) -> Result<()> {
        let (held, err) = match ended {
            device::Ended::Detached => return Ok(()),
            device::Ended::Drained(h) => (h, anyhow::Error::new(Drained)),
            device::Ended::DrainFailed(e, h) => {
                self.drain_failed.store(true, Ordering::Relaxed);
                (h, e)
            }
        };
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(held);
        Err(err)
    }

    /// Whether a drained device is held; the process must then exit without closing it.
    fn holds(&self) -> bool {
        !self
            .held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    }

    /// The exit status of a process that holds drained devices.
    fn drained_status(&self) -> i32 {
        if self.drain_failed.load(Ordering::Relaxed) {
            1
        } else {
            device::DRAINED
        }
    }

    /// Waits until no attach runs.
    async fn no_attaches(&self) {
        let _ = self.attaches.subscribe().wait_for(|n| *n == 0).await;
    }

    /// Checks the store's format marker for `access`, once per process: a check for writing
    /// also covers reading.
    async fn checked(&self, access: Access) -> Result<()> {
        if self.marked.initialized() {
            return Ok(());
        }
        let cell = match access {
            Access::Read => &self.readable,
            Access::Write => &self.marked,
        };
        cell.get_or_try_init(|| async {
            Ok::<_, anyhow::Error>(check_format(self.ports.store.as_ref(), access).await?)
        })
        .await?;
        Ok(())
    }

    /// Checks the store's format marker for `command` and, if the command needs it, opens the
    /// cache alongside the check.
    async fn prepare(&self, command: &Command) -> Result<()> {
        let checked = async {
            match command.access() {
                Some(access) => self.checked(access).await,
                None => Ok(()),
            }
        };
        let opened = async {
            if command.uses_cache() {
                self.cache().await?;
            }
            Ok::<_, anyhow::Error>(())
        };
        tokio::try_join!(checked, opened)?;
        Ok(())
    }

    async fn cache(&self) -> Result<Arc<Cache>> {
        let c = self
            .cache
            .get_or_try_init(|| async {
                Ok::<_, anyhow::Error>(Arc::new(Cache::open(&self.ports, &self.cache_dir).await?))
            })
            .await?;
        Ok(Arc::clone(c))
    }
}

/// How the process ends: with a code, or at once with `status` while `host` still holds the
/// devices of drained attaches.
enum Exit {
    Code(ExitCode),
    Held { status: i32, host: Arc<Host> },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let flushers = matches!(cli.command, Command::Attach(_) | Command::Serve);
    pando_host::tune_malloc();
    if flushers && let Err(e) = pando_host::io_flusher() {
        eprintln!(
            "warning: the threads that serve the device are not I/O flushers ({e}); with much \
             dirty data and little free memory, writes to the device can stall, so set \
             --dirty-limit"
        );
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(move || {
            if flushers {
                let _ = pando_host::io_flusher();
            }
        })
        .build();
    let rt = match rt {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Error: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(start(cli)) {
        Ok(Exit::Code(code)) => code,
        Ok(Exit::Held { status, host }) => {
            let _held = host;
            std::process::exit(status)
        }
        Err(e) => {
            eprintln!("Error: {e:?}");
            ExitCode::FAILURE
        }
    }
}

async fn start(cli: Cli) -> Result<Exit> {
    let serving = matches!(cli.command, Command::Serve);
    if let Some(socket) = cli.socket.as_deref().filter(|_| !serving) {
        return serve::client(socket, std::env::args_os().collect())
            .await
            .map(Exit::Code);
    }
    let host = Arc::new(Host {
        ports: pando_host::ports(
            cli.store.open()?,
            cli.cpu_threads,
            matches!(cli.command, Command::Attach(_) | Command::Serve),
        )?,
        cache_dir: cli.cache.clone().unwrap_or_else(|| PathBuf::from(DEFAULT_CACHE)),
        cache: OnceCell::new(),
        readable: OnceCell::new(),
        marked: OnceCell::new(),
        checkpoints: branch::Checkpoints::default(),
        remote: cli.store.remote().to_owned(),
        identity: cli.store.identity(),
        held: Mutex::default(),
        drain_failed: AtomicBool::new(false),
        attaches: watch::Sender::new(0),
    });
    let done = if serving {
        let socket = cli.socket.context("serve needs --socket")?;
        host.cache().await?;
        serve::serve(Arc::clone(&host), &socket).await
    } else {
        run(&host, cli.command, &Out::stdio()).await
    };
    if host.holds() {
        if let Err(e) = &done
            && !e.is::<Drained>()
        {
            eprintln!("{e:#}");
        }
        let status = host.drained_status();
        return Ok(Exit::Held { status, host });
    }
    done?;
    Ok(Exit::Code(ExitCode::SUCCESS))
}

/// Runs one command on `host`, writing its output to `out`.
pub(crate) async fn run(host: &Host, command: Command, out: &Out) -> Result<()> {
    host.prepare(&command).await?;
    let ports = &host.ports;
    let cache = || host.cache();
    match command {
        Command::Refs => refs(ports, out).await,
        Command::Init { name, size } => Ok(Volume::create(ports, &name, size).await?),
        Command::Import { name, image } => {
            let t0 = Instant::now();
            let r = pando_engine::import(ports, &cache().await?, &name, &image).await?;
            out.line(format!(
                "imported {name}: {} MiB device, {} chunks with data ({} of ext4 metadata), {} segments ({} MiB) in {:.1?}",
                r.size >> 20,
                r.chunks,
                r.metadata,
                r.upload.segments,
                r.upload.bytes >> 20,
                t0.elapsed()
            ));
            Ok(())
        }
        Command::Branch {
            src,
            dst,
            checkpoint,
            max_age,
        } => fork(host, &src, &dst, checkpoint, max_age, out).await,
        Command::Checkpoint { src, name, max_age } => checkpoint(host, &src, &name, max_age, out).await,
        Command::Checkpoints => checkpoints(ports, out).await,
        Command::DeleteCheckpoint { name } => {
            Ok(branch::delete_checkpoint(ports, &host.checkpoints, &name).await?)
        }
        Command::Serve => bail!("serve runs only as its own process"),
        Command::Advance { dst, src } => Ok(branch::advance(ports, &dst, &src).await?),
        Command::Delete { name } => Ok(branch::delete(ports, &name).await?),
        Command::Base { name, dir, parallel } => {
            let t0 = Instant::now();
            let r = base::build_branch(ports, &cache().await?, &dir, &name, parallel).await?;
            if r.built {
                let seed = r.seed.map_or_default(|s| format!(", seeded from {}", s.short()));
                out.line(format!(
                    "base of {name} built in {:.1?}: {} chunks{seed}",
                    t0.elapsed(),
                    r.chunks
                ));
            } else {
                out.line(format!("base of {name} is here already"));
            }
            Ok(())
        }
        Command::Follow {
            dir,
            interval,
            keep,
            budget,
            parallel,
            once,
        } => {
            let mut f = base::Follower::new(ports, cache().await?, &dir, parallel, keep, budget);
            loop {
                let n = f.once().await?;
                if once {
                    out.line(format!("built {n} base images"));
                    return Ok(());
                }
                tokio::time::sleep(interval).await;
            }
        }
        Command::Gc {
            delete,
            keep_young,
            validate,
        } => {
            let opts = GcOptions {
                delete,
                keep_young,
                validate,
            };
            collect(ports, &cache().await?, &opts, out).await
        }
        Command::Attach(a) => {
            let _busy = host.attaching();
            let ended = device::attach(ports, cache().await?, a, host.identity.clone(), out).await?;
            host.ended(ended)
        }
        Command::Detach(d) => device::detach(d).await,
        Command::Resize(r) => device::resize(r, out).await,
        Command::Stat => {
            let u = gc::usage(&cache().await?).await?;
            out.line(format!(
                "{} branches, {} segments ({} MiB), {} change lists",
                u.branches,
                u.segments,
                u.segment_bytes >> 20,
                u.change_lists
            ));
            Ok(())
        }
    }
}

/// Makes branch `dst` from branch `src`, or with `checkpoint` from checkpoint `src`.
async fn fork(
    host: &Host,
    src: &str,
    dst: &str,
    checkpoint: bool,
    max_age: Duration,
    out: &Out,
) -> Result<()> {
    let t0 = Instant::now();
    if checkpoint {
        branch::from_checkpoint(&host.ports, &host.checkpoints, src, dst, Some(max_age)).await?;
    } else {
        branch::copy(&host.ports, src, dst, Some(max_age)).await?;
    }
    out.line(format!("{src} -> {dst} in {:.1?}", t0.elapsed()));
    Ok(())
}

/// Takes checkpoint `name` of branch `src`.
async fn checkpoint(host: &Host, src: &str, name: &str, max_age: Duration, out: &Out) -> Result<()> {
    let t0 = Instant::now();
    let c = branch::checkpoint(&host.ports, &host.checkpoints, src, name, Some(max_age)).await?;
    let root = c.root.hash.short();
    out.line(format!(
        "checkpoint {name} of {src} at {root} in {:.1?}",
        t0.elapsed()
    ));
    Ok(())
}

async fn checkpoints(ports: &Ports, out: &Out) -> Result<()> {
    for (name, c) in branch::list_checkpoints(ports).await? {
        out.line(format!("{name} {} {} {}", c.size, c.branch, c.root.hash.short()));
    }
    Ok(())
}

async fn collect(ports: &Ports, cache: &Arc<Cache>, opts: &GcOptions, out: &Out) -> Result<()> {
    let r = gc::collect(cache, ports.clock.as_ref(), opts).await?;
    out.line(format!(
        "{} live segments ({} MiB, {} MiB used), {} dead ({} MiB){}; kept {} of current writers, {} young, {} needed by kept; {} revived; {} stale change lists, {} stale log records",
        r.live,
        r.live_bytes >> 20,
        r.used_bytes >> 20,
        r.dead,
        r.dead_bytes >> 20,
        if r.deleted { ", deleted" } else { "" },
        r.current_writer,
        r.young,
        r.needed_by_kept,
        r.revived,
        r.stale_change_lists,
        r.stale_log_records
    ));
    Ok(())
}

async fn refs(ports: &Ports, out: &Out) -> Result<()> {
    for o in ports.store.list(keys::REFS).await? {
        let name = o.key.strip_prefix(&format!("{}/", keys::REFS)).unwrap_or(&o.key);
        let r = Ref::decode(&ports.store.get(&o.key).await?.body).with_context(|| format!("ref {name}"))?;
        let from = r.from.map_or_default(|f| format!(" from {f}"));
        out.line(format!(
            "{name} {} {} {}{from}",
            r.size,
            r.generation,
            r.root.hash.short()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_paths_resolve_against_the_client_directory() {
        let mut cli = Cli::try_parse_from(["pando", "--cache", "c", "import", "x", "disk.img"]).unwrap();
        cli.resolve(Path::new("/home/u"));
        assert_eq!(cli.cache.as_deref(), Some(Path::new("/home/u/c")));
        let Command::Import { image, .. } = cli.command else {
            panic!("not import")
        };
        assert_eq!(image, Path::new("/home/u/disk.img"));
        let mut cli = Cli::try_parse_from(["pando", "base", "x", "--dir", "/abs"]).unwrap();
        cli.resolve(Path::new("/home/u"));
        let Command::Base { dir, .. } = cli.command else {
            panic!("not base")
        };
        assert_eq!(dir, Path::new("/abs"));
    }

    #[test]
    fn durations_parse_with_their_units() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("15s").unwrap(), Duration::from_secs(15));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_hours(24));
        assert!(parse_duration("3d").is_err());
    }

    #[test]
    fn recover_alone_is_always_and_never_takes_the_branch_name() {
        use device::Recover;
        let attach = |args: &[&str]| {
            let all = ["pando", "attach"].into_iter().chain(args.iter().copied());
            let parsed = Cli::try_parse_from(all).unwrap().command;
            let a = if let Command::Attach(a) = parsed {
                Some(a)
            } else {
                None
            };
            let a = a.unwrap();
            (a.name, a.recover, a.drain_wait)
        };
        let dev = ["--device", "/dev/ublkb1"];
        assert_eq!(
            attach(&["main", dev[0], dev[1]]),
            ("main".into(), Recover::Never, Duration::from_secs(60))
        );
        assert_eq!(attach(&["main", dev[0], dev[1], "--recover"]).1, Recover::Always);
        assert_eq!(attach(&["--recover", "main", dev[0], dev[1]]).0, "main");
        assert_eq!(
            attach(&["main", dev[0], dev[1], "--recover=auto"]).1,
            Recover::Auto
        );
        assert_eq!(
            attach(&["main", dev[0], dev[1], "--drain-wait", "5m"]).2,
            Duration::from_secs(300)
        );
    }

    #[test]
    fn only_commands_that_write_may_mark_the_store() {
        let access = |args: &[&str]| {
            Cli::try_parse_from(std::iter::once("pando").chain(args.iter().copied()))
                .unwrap()
                .command
                .access()
        };
        assert_eq!(access(&["refs"]), Some(Access::Read));
        assert_eq!(access(&["stat"]), Some(Access::Read));
        assert_eq!(access(&["checkpoints"]), Some(Access::Read));
        assert_eq!(access(&["gc"]), Some(Access::Read));
        assert_eq!(access(&["gc", "--delete"]), Some(Access::Write));
        assert_eq!(access(&["init", "main", "4096"]), Some(Access::Write));
        assert_eq!(access(&["branch", "main", "run-1"]), Some(Access::Write));
        assert_eq!(access(&["checkpoint", "main", "c1"]), Some(Access::Write));
        assert_eq!(access(&["detach", "--device", "/dev/ublkb0"]), None);
        assert_eq!(access(&["serve"]), None);
    }

    #[test]
    fn sizes_parse_in_powers_of_1024() {
        assert_eq!(parse_size("4096").unwrap(), 4096);
        assert_eq!(parse_size("512M").unwrap(), 512 << 20);
        assert_eq!(parse_size("8G").unwrap(), 8 << 30);
        assert!(parse_size("8GB").is_err());
        assert!(parse_size("99999999T").is_err());
    }
}
