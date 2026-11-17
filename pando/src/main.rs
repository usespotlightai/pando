#![forbid(unsafe_code)]

mod config;
mod device;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use pando_engine::gc::{self, GcOptions};
use pando_engine::{Cache, Ports, Volume, base, branch};
use pando_format::{Ref, keys};

#[derive(Parser, Debug)]
#[command(version, about = "Branchable copy-on-write block devices on object storage")]
struct Cli {
    #[command(flatten)]
    store: config::StoreArgs,
    /// The local cache directory.
    #[arg(long, global = true, default_value = "/data/cache")]
    cache: PathBuf,
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
    /// Copies branch SRC to a new branch DST.
    Branch {
        src: String,
        dst: String,
        /// Undo a copy slower than this: GC must find it within its validate delay.
        #[arg(long, value_parser = parse_duration, default_value = "15s")]
        max_age: Duration,
    },
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
    /// Stops the device of an attached branch; returns once the branch is published.
    Detach(device::DetachArgs),
    /// Grows an attached branch and the devices over it while it serves. The file system on
    /// it keeps its size until it is grown too, with resize2fs for ext4.
    Resize(device::ResizeArgs),
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let ports = pando_host::ports(cli.store.open()?, cli.cpu_threads)?;
    let cache = || async { Ok::<_, anyhow::Error>(Arc::new(Cache::open(&ports, &cli.cache).await?)) };
    match cli.command {
        Command::Refs => refs(&ports).await,
        Command::Init { name, size } => Ok(Volume::create(&ports, &name, size).await?),
        Command::Import { name, image } => {
            let t0 = Instant::now();
            let r = pando_engine::import(&ports, &cache().await?, &name, &image).await?;
            println!(
                "imported {name}: {} MiB device, {} chunks with data ({} of ext4 metadata), {} segments ({} MiB) in {:.1?}",
                r.size >> 20,
                r.chunks,
                r.metadata,
                r.upload.segments,
                r.upload.bytes >> 20,
                t0.elapsed()
            );
            Ok(())
        }
        Command::Branch { src, dst, max_age } => {
            let t0 = Instant::now();
            branch::copy(&ports, &src, &dst, Some(max_age)).await?;
            println!("{src} -> {dst} in {:.1?}", t0.elapsed());
            Ok(())
        }
        Command::Advance { dst, src } => Ok(branch::advance(&ports, &dst, &src).await?),
        Command::Delete { name } => Ok(branch::delete(&ports, &name).await?),
        Command::Base { name, dir, parallel } => {
            let t0 = Instant::now();
            let r = base::build_branch(&ports, &cache().await?, &dir, &name, parallel).await?;
            if r.built {
                let seed = r
                    .seed
                    .map(|s| format!(", seeded from {}", s.short()))
                    .unwrap_or_default();
                println!(
                    "base of {name} built in {:.1?}: {} chunks{seed}",
                    t0.elapsed(),
                    r.chunks
                );
            } else {
                println!("base of {name} is here already");
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
            let mut f = base::Follower::new(&ports, cache().await?, &dir, parallel, keep, budget);
            loop {
                let n = f.once().await?;
                if once {
                    println!("built {n} base images");
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
            let r = gc::collect(
                &cache().await?,
                ports.clock.as_ref(),
                &GcOptions {
                    delete,
                    keep_young,
                    validate,
                },
            )
            .await?;
            println!(
                "{} live segments ({} MiB, {} MiB used), {} dead ({} MiB){}; kept {} of current writers, {} young, {} needed by kept; {} revived; {} stale change lists",
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
                r.stale_change_lists
            );
            Ok(())
        }
        Command::Attach(a) => device::attach(&ports, cache().await?, a, cli.store.identity()).await,
        Command::Detach(d) => device::detach(d).await,
        Command::Resize(r) => device::resize(r).await,
        Command::Stat => {
            let u = gc::usage(&cache().await?).await?;
            println!(
                "{} branches, {} segments ({} MiB), {} change lists",
                u.branches,
                u.segments,
                u.segment_bytes >> 20,
                u.change_lists
            );
            Ok(())
        }
    }
}

async fn refs(ports: &Ports) -> Result<()> {
    for o in ports.store.list(keys::REFS).await? {
        let name = o.key.strip_prefix(&format!("{}/", keys::REFS)).unwrap_or(&o.key);
        let r = Ref::decode(&ports.store.get(&o.key).await?.body).with_context(|| format!("ref {name}"))?;
        let from = r.from.map(|f| format!(" from {f}")).unwrap_or_default();
        println!("{name} {} {} {}{from}", r.size, r.generation, r.root.hash.short());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse_with_their_units() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("15s").unwrap(), Duration::from_secs(15));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86400));
        assert!(parse_duration("3d").is_err());
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
