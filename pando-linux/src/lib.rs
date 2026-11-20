//! Pando on Linux: the ublk frontend, the FUSE frontend with a loop device over it, and the
//! `DeviceSwitch` port over device-mapper and loop devices. Empty on other systems.
//!
//! Each ublk queue thread waits in `io_uring_enter`. A lone read the queue can serve from
//! memory or the page cache without waiting is served there; every other request goes to the
//! engine's shared runtime, and its result comes back through a channel and an eventfd the
//! queue's ring reads.

#![cfg(target_os = "linux")]
#![expect(
    unsafe_code,
    reason = "the frontends and the device switch talk to the kernel through ioctls, io_uring, /dev/fuse and shared mappings"
)]

mod bdi;
mod dm;
mod fuse;
mod sys;
mod ublk;
mod users;

pub use dm::DmSwitch;
pub use fuse::{FILE_NAME as FUSE_FILE, FuseFrontend, FuseOptions};
pub use ublk::{
    Daemon, Frontend, FrontendOptions, daemon, daemon_dead, delete, device_id, resize, stop, user_recovery,
};

/// What a stop of block device `dev` would cut off: each mount of it, each device stacked on
/// it other than the device-mapper device named `over`, and, when neither explains it, an
/// exclusive opener such as a mount in another mount namespace. Empty when `dev` does not
/// exist.
pub async fn users(dev: &std::path::Path, over: Option<&str>) -> std::io::Result<Vec<String>> {
    let (dev, over) = (dev.to_owned(), over.map(str::to_owned));
    dm::blocking(move || users::users(&dev, over.as_deref())).await
}

/// Takes the exclusive BSD lock on block device `dev`, which udev honours: it waits for a
/// probe that holds the device to end, and udev starts no probe of it until the returned file
/// is dropped. Waits at most `wait`; `None` when the lock was not taken in that time.
pub async fn lock(
    dev: &std::path::Path,
    wait: std::time::Duration,
) -> std::io::Result<Option<std::fs::File>> {
    let dev = dev.to_owned();
    dm::blocking(move || {
        use std::os::fd::AsRawFd;
        let f = std::fs::File::open(&dev)?;
        let until = std::time::Instant::now() + wait;
        loop {
            // SAFETY: `f` is an open file for the whole call.
            if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                return Ok(Some(f));
            }
            let e = std::io::Error::last_os_error();
            if e.raw_os_error() != Some(libc::EWOULDBLOCK) {
                return Err(e);
            }
            if std::time::Instant::now() >= until {
                return Ok(None);
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    })
    .await
}

/// Caps the page cache's dirty data on block device `dev` at about `bytes`. The kernel starts
/// writing the device's dirty data back before it reaches the cap and throttles writers at the
/// cap, so a file system on `dev` holds little its device has not taken. Needs Linux 6.2 or
/// later. On an error, which names the sysfs file it could not write, the kernel's own limits
/// stay in force.
pub async fn limit_dirty(dev: &std::path::Path, bytes: u64) -> std::io::Result<()> {
    let dev = dev.to_owned();
    dm::blocking(move || bdi::limit_dirty(&dev, bytes)).await
}

/// The node of device-mapper device `name`, `/dev/mapper/<name>`.
pub fn mapper_path(name: &str) -> std::path::PathBuf {
    dm::mapper_node(name)
}

/// Whether `path` is a block device.
pub fn is_block_device(path: &std::path::Path) -> bool {
    dm::is_block(path)
}
