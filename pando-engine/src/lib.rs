//! The Pando volume engine.
//!
//! All of Pando's logic lives here: the block map tree, the cache, fetch and its scheduler,
//! the uploader and writer fencing, dedup, base builds, seeding and the handoff steps. The
//! engine reaches the world only through the [`ports`]:
//!
//! - [`ports::Store`]: the object store.
//! - [`ports::LocalFs`] and [`ports::LocalImage`]: files and base images on this host.
//! - [`ports::DeviceSwitch`]: the device-mapper table that serves a branch.
//! - [`ports::Clock`], [`ports::Rng`], [`ports::Cpu`]: time, randomness and CPU work.
//!
//! So the engine has no Linux code and runs in the deterministic simulator. Rules for that:
//! time comes from `tokio::time` (paused in the simulator) and wall time from [`ports::Clock`];
//! CPU-heavy work (SHA-256, zstd) runs on [`ports::Cpu`]; nothing starts threads or blocks.
//! `clippy.toml` enforces these. With the `sim` feature, [`sim`] has in-memory adapters.

#![forbid(unsafe_code)]
#![allow(dead_code, reason = "handoff, hydrate, base builds and GC use these next")]

mod attach;
pub mod base;
pub mod branch;
mod cache;
mod dedup;
mod error;
mod ext4;
mod flight;
mod gate;
pub mod gc;
mod handoff;
mod import;
mod local;
mod packer;
pub mod ports;
mod recording;
mod remote;
#[cfg(feature = "sim")]
pub mod sim;
mod tree;
mod volume;

pub use attach::{AttachOptions, Attachment, FinishReport, SnapshotCounts, WarmReport};
pub use cache::{Cache, CacheStats};
pub use error::{Error, Result};
pub use handoff::HandoffReport;
pub use import::{ImportReport, import};
pub use packer::PackStats;
pub use ports::Ports;
pub use remote::RemoteStats;
pub use volume::{LogMode, Volume};
