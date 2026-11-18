//! Pando on Linux: the ublk frontend and the `DeviceSwitch` port over device-mapper and loop
//! devices. Empty on other systems.
//!
//! Each ublk queue thread waits in `io_uring_enter`. A lone read the queue can serve from
//! memory or the page cache without waiting is served there; every other request goes to the
//! engine's shared runtime, and its result comes back through a channel and an eventfd the
//! queue's ring reads.

#![cfg(target_os = "linux")]
#![expect(
    unsafe_code,
    reason = "the ublk frontend and the device switch talk to the kernel through ioctls, io_uring and shared mappings"
)]

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

/// Whether `path` is a block device.
pub fn is_block_device(path: &std::path::Path) -> bool {
    dm::is_block(path)
}
