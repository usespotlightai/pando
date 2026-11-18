//! The ublk frontend: serves a branch as `/dev/ublkbN`.
//!
//! The device is added with user recovery that reissues requests: when the daemon dies the
//! kernel keeps the device, holds new I/O, and gives the requests that were in flight to the
//! next daemon, which recovers the device with `START_USER_RECOVERY` and `END_USER_RECOVERY`.

mod ctrl;
mod queue;

use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use pando_engine::Volume;
use tokio::runtime::Handle;

use crate::dm::open_usable;
use ctrl::{Ctrl, NewDevice};
use queue::Queue;

const DEPTH: u16 = 64;
/// How long a recovery waits for the kernel to quiesce the device of a dead daemon.
const QUIESCE_WAIT: Duration = Duration::from_secs(10);
/// How long a delete waits for the kernel to free a stopped device its daemon let go of.
const RELEASE_WAIT: Duration = Duration::from_secs(10);

/// The number of `/dev/ublkbN`.
pub fn device_id(dev: &Path) -> io::Result<u32> {
    dev.to_str()
        .and_then(|s| s.strip_prefix("/dev/ublkb"))
        .and_then(|n| n.parse().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("device {} is not /dev/ublkbN", dev.display()),
            )
        })
}

/// How a frontend starts.
#[derive(Clone, Copy, Debug)]
pub struct FrontendOptions {
    /// Hardware queues of a new device, each served by one thread pinned to its CPU. One queue
    /// per CPU is faster at depth but slower at queue depth 1, where the queue thread shares a
    /// CPU with the submitter.
    pub queues: u16,
    /// Take over the device of a daemon that died, with the requests it had in flight.
    pub recover: bool,
}

impl Default for FrontendOptions {
    fn default() -> FrontendOptions {
        FrontendOptions {
            queues: 2,
            recover: false,
        }
    }
}

/// A started ublk device and the threads that serve its queues.
#[derive(Debug)]
pub struct Frontend {
    id: u32,
    cdev: Option<std::fs::File>,
    queues: Vec<JoinHandle<io::Result<()>>>,
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> io::Result<T> + Send + 'static) -> io::Result<T> {
    tokio::task::spawn_blocking(f).await.map_err(io::Error::other)?
}

impl Frontend {
    /// Adds device `id` (or recovers it), serves its queues from `vol` on runtime `rt`, and
    /// starts it.
    pub async fn start(rt: Handle, vol: Arc<Volume>, id: u32, opts: FrontendOptions) -> io::Result<Frontend> {
        blocking(move || Frontend::start_blocking(&rt, &vol, id, opts)).await
    }

    fn start_blocking(
        rt: &Handle,
        vol: &Arc<Volume>,
        id: u32,
        opts: FrontendOptions,
    ) -> io::Result<Frontend> {
        let mut c = Ctrl::open()?;
        let (mut queues, mut depth) = (opts.queues.max(1), DEPTH);
        if opts.recover {
            let t0 = Instant::now();
            let info = loop {
                let i = c.info(id)?;
                if i.state == ctrl::STATE_QUIESCED || t0.elapsed() > QUIESCE_WAIT {
                    break i;
                }
                std::thread::sleep(Duration::from_millis(50));
            };
            if info.state != ctrl::STATE_QUIESCED {
                return Err(io::Error::other(format!(
                    "ublk device {id} is in state {}, not quiesced",
                    info.state
                )));
            }
            (queues, depth) = (info.queues, info.depth);
            c.cmd(ctrl::CMD_START_USER_RECOVERY, id, 0, None)?;
        } else {
            let flags = ctrl::F_USER_RECOVERY
                | ctrl::F_USER_RECOVERY_REISSUE
                | ctrl::F_CMD_IOCTL_ENCODE
                | (c.features() & ctrl::F_NO_AUTO_PART_SCAN);
            c.add(&NewDevice {
                id,
                queues,
                depth,
                max_io: queue::MAX_IO_BYTES,
                flags,
            })?;
        }
        let mut fe = Frontend {
            id,
            cdev: None,
            queues: Vec::new(),
        };
        let started = (|| {
            if !opts.recover {
                c.set_params(id, vol.size(), queue::MAX_IO_BYTES)?;
            }
            let name = format!("ublkc{id}");
            let devt = std::fs::read_to_string(format!("/sys/class/ublk-char/{name}/dev"))?;
            let cdev = open_usable(&Path::new("/dev").join(&name), true, Some(devt.trim()))?;
            let fd = cdev.as_raw_fd();
            fe.cdev = Some(cdev);
            let (tx, rx) = mpsc::channel();
            for q in 0..queues {
                let queue = Queue {
                    cdev: fd,
                    q,
                    depth,
                    vol: Arc::clone(vol),
                    rt: rt.clone(),
                };
                let tx = tx.clone();
                fe.queues.push(
                    std::thread::Builder::new()
                        .name(format!("ublk-q{q}"))
                        .spawn(move || queue.run(&tx))?,
                );
            }
            for _ in 0..queues {
                rx.recv().map_err(io::Error::other)??;
            }
            let nr = if opts.recover {
                ctrl::CMD_END_USER_RECOVERY
            } else {
                ctrl::CMD_START_DEV
            };
            c.cmd(nr, id, u64::from(std::process::id()), None)
        })();
        match started {
            Ok(()) => Ok(fe),
            Err(e) if opts.recover => Err(e),
            Err(e) => {
                let mut also = Vec::new();
                if let Err(se) = c.cmd(ctrl::CMD_STOP_DEV, id, 0, None) {
                    also.push(format!("stop ublk device {id}: {se}"));
                }
                if let Err(qe) = join(std::mem::take(&mut fe.queues)) {
                    also.push(qe.to_string());
                }
                fe.cdev = None;
                if let Err(de) = c.cmd(ctrl::CMD_DEL_DEV, id, 0, None) {
                    also.push(format!("delete ublk device {id}: {de}"));
                }
                if also.is_empty() {
                    Err(e)
                } else {
                    Err(io::Error::new(e.kind(), format!("{e}; then {}", also.join("; "))))
                }
            }
        }
    }

    /// The number of the device, N of `/dev/ublkbN`.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Waits until the device stops, which ends every queue. The error names each queue that
    /// ended with one.
    pub async fn stopped(&mut self) -> io::Result<()> {
        let queues = std::mem::take(&mut self.queues);
        blocking(move || join(queues)).await
    }

    /// Stops and deletes the device this frontend started, when the start of what sits on it
    /// failed. A queue that ended with an error does not keep the device from being deleted.
    pub async fn abort(mut self) -> io::Result<()> {
        let id = self.id;
        blocking(move || Ctrl::open()?.cmd(ctrl::CMD_STOP_DEV, id, 0, None)).await?;
        let stopped = self.stopped().await;
        drop(self.cdev.take());
        blocking(move || Ctrl::open()?.cmd(ctrl::CMD_DEL_DEV, id, 0, None)).await?;
        stopped
    }
}

/// A failed recovery keeps the device, which still holds the mounts, so its queues may wait
/// forever; while one runs, the character device stays open under it.
impl Drop for Frontend {
    fn drop(&mut self) {
        if !self.queues.is_empty()
            && let Some(f) = self.cdev.take()
        {
            let _ = std::os::fd::IntoRawFd::into_raw_fd(f);
        }
    }
}

fn join(queues: Vec<JoinHandle<io::Result<()>>>) -> io::Result<()> {
    let mut failed = Vec::new();
    for (q, t) in queues.into_iter().enumerate() {
        match t.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => failed.push(format!("ublk queue {q}: {e}")),
            Err(_) => failed.push(format!("ublk queue {q} panicked")),
        }
    }
    if failed.is_empty() {
        Ok(())
    } else {
        Err(io::Error::other(failed.join("; ")))
    }
}

/// Whether the daemon of device `id` died: ublk holds its I/O (quiesced) or fails it.
pub async fn daemon_dead(id: u32) -> io::Result<bool> {
    blocking(move || {
        let s = Ctrl::open()?.info(id)?.state;
        Ok(s == ctrl::STATE_QUIESCED || s == ctrl::STATE_FAIL_IO)
    })
    .await
}

/// Sets the size of started device `id` to `size` bytes, a multiple of 4 KiB. The volume it
/// serves must already be that large. Needs Linux 6.16 or later.
pub async fn resize(id: u32, size: u64) -> io::Result<()> {
    blocking(move || Ctrl::open()?.update_size(id, size)).await
}

/// Stops device `id`, which ends its queues and fails the I/O ublk holds for a dead daemon.
pub async fn stop(id: u32) -> io::Result<()> {
    blocking(move || Ctrl::open()?.cmd(ctrl::CMD_STOP_DEV, id, 0, None)).await
}

/// Deletes stopped device `id`. It waits at most `publish_wait` for the daemon to let go of the
/// device, which it does once its branch is published, and then at most 10 s for the kernel to
/// free the device. A mount or an open file that still holds the stopped device fails the
/// delete; the kernel frees the device once they let go of it.
pub async fn delete(id: u32, publish_wait: Duration) -> io::Result<()> {
    blocking(move || {
        wait_released(&Path::new("/dev").join(format!("ublkc{id}")), publish_wait).map_err(|e| {
            if e.kind() == io::ErrorKind::TimedOut {
                io::Error::new(
                    e.kind(),
                    format!(
                        "the daemon of ublk device {id} still holds it after {publish_wait:?}; it \
                         may still be publishing its branch"
                    ),
                )
            } else {
                e
            }
        })?;
        within(RELEASE_WAIT, move || {
            Ctrl::open()?.cmd(ctrl::CMD_DEL_DEV, id, 0, None)
        })
        .map_err(|e| {
            if e.kind() == io::ErrorKind::TimedOut {
                io::Error::new(
                    e.kind(),
                    format!(
                        "the daemon of ublk device {id} let go of it, which a live daemon does after \
                         its publish, but the delete did not finish within {RELEASE_WAIT:?}: a mount or an open file \
                         still holds /dev/ublkb{id}, and the kernel frees it once they let go of it"
                    ),
                )
            } else {
                e
            }
        })
    })
    .await
}

/// Waits at most `wait` until no daemon holds the ublk character device `cdev`, which only one
/// process may open at a time.
fn wait_released(cdev: &Path, wait: Duration) -> io::Result<()> {
    let t0 = Instant::now();
    loop {
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_CLOEXEC)
            .open(cdev)
        {
            Ok(_) => return Ok(()),
            Err(e) if e.raw_os_error() == Some(libc::EBUSY) && t0.elapsed() >= wait => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("{} is still open after {wait:?}", cdev.display()),
                ));
            }
            Err(e) if e.raw_os_error() == Some(libc::EBUSY) => std::thread::sleep(Duration::from_millis(100)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

/// Runs `f` on a thread of its own and waits at most `wait` for its result. A thread still
/// running then is left behind and ends with the process.
fn within<T: Send + 'static>(
    wait: Duration,
    f: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    let (tx, rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("ublk-ctrl".into())
        .spawn(move || {
            let _ = tx.send(f());
        })?;
    match rx.recv_timeout(wait) {
        Ok(r) => r,
        Err(mpsc::RecvTimeoutError::Timeout) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("no answer from the kernel after {wait:?}"),
        )),
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(io::Error::other("the control thread panicked")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_ids_come_from_the_block_device_name() {
        assert_eq!(device_id(Path::new("/dev/ublkb12")).unwrap(), 12);
        assert!(device_id(Path::new("/dev/loop0")).is_err());
    }

    #[test]
    fn a_call_that_never_returns_times_out() {
        let t0 = Instant::now();
        let e = within(Duration::from_millis(50), || -> io::Result<()> {
            std::thread::sleep(Duration::from_secs(3600));
            Ok(())
        })
        .unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::TimedOut);
        assert!(t0.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn a_call_in_time_returns_its_result() {
        assert_eq!(within(Duration::from_secs(5), || Ok(7)).unwrap(), 7);
        let e = within(Duration::from_secs(5), || -> io::Result<()> {
            Err(io::Error::other("no"))
        })
        .unwrap_err();
        assert_eq!(e.to_string(), "no");
    }

    #[test]
    fn a_missing_character_device_is_released() {
        wait_released(Path::new("/dev/ublkc987654"), Duration::ZERO).unwrap();
    }
}
