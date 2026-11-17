//! Pando on Linux: the ublk frontend and the `DeviceSwitch` port over device-mapper and loop
//! devices. Empty on other systems.
//!
//! Each ublk queue thread waits in `io_uring_enter`. A lone read the queue can serve from
//! memory or the page cache without waiting is served there; every other request goes to the
//! engine's shared runtime, and its result comes back through a channel and an eventfd the
//! queue's ring reads.

#![cfg(target_os = "linux")]
#![allow(unsafe_code)]

mod dm;
mod sys;
mod ublk;
mod users;

pub use dm::DmSwitch;
pub use ublk::{Frontend, FrontendOptions, daemon_dead, delete, device_id, resize, stop};

/// What a stop of block device `dev` would cut off: each mount of it, each device stacked on
/// it other than the device-mapper device named `over`, and, when neither explains it, an
/// exclusive opener such as a mount in another mount namespace. Empty when `dev` does not
/// exist.
pub async fn users(dev: &std::path::Path, over: Option<&str>) -> std::io::Result<Vec<String>> {
    let (dev, over) = (dev.to_owned(), over.map(str::to_owned));
    dm::blocking(move || users::users(&dev, over.as_deref())).await
}

/// Whether `path` is a block device.
pub fn is_block_device(path: &std::path::Path) -> bool {
    dm::is_block(path)
}
