//! One ublk hardware queue on its own thread.
//!
//! The thread waits in `io_uring_enter`. It fetches every tag, serves a lone read from memory
//! or the page cache itself, and sends every other request to the engine runtime. Results come
//! back through a channel and an eventfd the ring reads, so a completion on another thread
//! wakes the queue at once.

use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;

use io_uring::{IoUring, opcode, squeue, types};
use pando_engine::Volume;
use tokio::runtime::Handle;

use crate::sys::{self, Mapping};

/// The largest request, in bytes.
pub(crate) const MAX_IO_BYTES: u32 = 512 << 10;
pub(crate) const MAX_IO: usize = MAX_IO_BYTES as usize;
/// The largest queue depth ublk allows, which sets the stride of the descriptor areas.
const MAX_QUEUE_DEPTH: usize = 4096;
const DESC_LEN: usize = 24;

const IO_FETCH: u32 = 0x20;
const IO_COMMIT_AND_FETCH: u32 = 0x21;

const OP_READ: u32 = 0;
const OP_WRITE: u32 = 1;
const OP_FLUSH: u32 = 2;
const OP_DISCARD: u32 = 3;
const OP_WRITE_ZEROES: u32 = 5;
const IO_F_FUA: u32 = 1 << 13;

const EFD_TAG: u64 = 1 << 63;

fn io_op(nr: u32) -> u32 {
    sys::ioc32(3, b'u', nr, 16)
}

/// One request, as its descriptor gives it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Request {
    op_flags: u32,
    off: u64,
    len: usize,
}

impl Request {
    fn op(&self) -> u32 {
        self.op_flags & 0xff
    }

    fn has_data(&self) -> bool {
        matches!(self.op(), OP_READ | OP_WRITE)
    }
}

/// The data buffer of one tag, handed to the engine runtime while the request runs.
#[derive(Debug, Clone, Copy)]
struct TagBuf {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: a tag's buffer is used by one request at a time, and the queue keeps the buffers
// alive until every request it handed out has returned its result.
unsafe impl Send for TagBuf {}

impl TagBuf {
    /// # Safety
    /// No one else may use this tag's buffer until its result is committed.
    unsafe fn slice<'a>(self) -> &'a mut [u8] {
        // SAFETY: the caller holds the tag; ptr and len lie inside the queue's buffers.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

/// Serves a request on the engine runtime.
async fn handle(vol: &Volume, r: Request, buf: Option<&mut [u8]>) -> i32 {
    let done = match (r.op(), buf) {
        (OP_READ, Some(b)) => vol.read_at(b, r.off).await,
        (OP_WRITE, Some(b)) => match vol.write_at(b, r.off).await {
            Ok(()) if r.op_flags & IO_F_FUA != 0 => vol.flush().await,
            done => done,
        },
        (OP_FLUSH, _) => return result(vol.flush().await, 0),
        (OP_DISCARD | OP_WRITE_ZEROES, _) => vol.zero(r.off, r.len as u64).await,
        _ => return -libc::EINVAL,
    };
    result(done, done_len(r))
}

/// The result that completes `r`: its length, which [`MAX_IO`] bounds for a request with data.
fn done_len(r: Request) -> i32 {
    i32::try_from(r.len).unwrap_or(i32::MAX)
}

fn result(done: pando_engine::Result<()>, ok: i32) -> i32 {
    match done {
        Ok(()) => ok,
        Err(e) => {
            log(format_args!("ublk io: {e}"));
            -libc::EIO
        }
    }
}

#[expect(
    clippy::print_stderr,
    reason = "queue threads and I/O tasks have no caller to return a diagnostic to; the daemon's stderr is its log"
)]
fn log(msg: std::fmt::Arguments<'_>) {
    eprintln!("{msg}");
}

/// A read served on the queue thread, when memory and the page cache hold it.
fn try_read(vol: &Volume, r: Request, buf: &mut [u8]) -> Option<i32> {
    if r.op() != OP_READ {
        return None;
    }
    match vol.try_read_at(buf, r.off) {
        Ok(true) => Some(done_len(r)),
        Ok(false) => None,
        Err(e) => Some(result(Err(e), 0)),
    }
}

/// Where a request's result goes: the queue's channel, and its eventfd, written only when
/// no wakeup is pending already.
#[derive(Clone)]
struct Results {
    tx: mpsc::Sender<(u16, i32)>,
    efd: Arc<OwnedFd>,
    pending: Arc<AtomicBool>,
}

impl Results {
    fn send(&self, tag: u16, res: i32) {
        let _ = self.tx.send((tag, res));
        if !self.pending.swap(true, Ordering::AcqRel) {
            sys::eventfd_signal(&self.efd);
        }
    }
}

/// Serves request `tag` on the engine runtime and sends its result.
async fn run(vol: Arc<Volume>, out: Results, tag: u16, req: Request, buf: TagBuf) {
    // SAFETY: the tag stays ours until the queue commits the result sent here.
    let data = req.has_data().then(|| unsafe { buf.slice() });
    let res = handle(&vol, req, data).await;
    out.send(tag, res);
}

/// What a queue thread needs.
#[derive(Debug)]
pub(crate) struct Queue {
    pub(crate) cdev: RawFd,
    pub(crate) q: u16,
    pub(crate) depth: u16,
    pub(crate) vol: Arc<Volume>,
    pub(crate) rt: Handle,
}

struct Ring {
    uring: IoUring,
    cdev: RawFd,
    q: u16,
    bufs: *mut u8,
}

impl Ring {
    fn push(&mut self, e: &squeue::Entry) -> io::Result<()> {
        // SAFETY: each entry's buffers (a tag's data buffer, the eventfd counter) live until
        // the queue ends, after its last completion.
        while unsafe { self.uring.submission().push(e) }.is_err() {
            self.uring.submit()?;
        }
        Ok(())
    }

    fn io_cmd(&mut self, nr: u32, tag: u16, res: i32) -> io::Result<()> {
        let mut cmd = [0u8; 16];
        cmd[0..2].copy_from_slice(&self.q.to_le_bytes());
        cmd[2..4].copy_from_slice(&tag.to_le_bytes());
        cmd[4..8].copy_from_slice(&res.to_le_bytes());
        let addr = self.bufs as u64 + (tag as usize * MAX_IO) as u64;
        cmd[8..16].copy_from_slice(&addr.to_le_bytes());
        let e = opcode::UringCmd16::new(types::Fd(self.cdev), io_op(nr))
            .cmd(cmd)
            .build()
            .user_data(u64::from(tag));
        self.push(&e)
    }

    fn arm_eventfd(&mut self, efd: &OwnedFd, counter: *mut u8) -> io::Result<()> {
        let e = opcode::Read::new(types::Fd(efd.as_raw_fd()), counter, 8)
            .build()
            .user_data(EFD_TAG);
        self.push(&e)
    }

    fn wait(&mut self) -> io::Result<()> {
        loop {
            match self.uring.submit_and_wait(1) {
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => {}
                r => return r.map(drop),
            }
        }
    }
}

fn round_up(n: usize, to: usize) -> usize {
    n.div_ceil(to) * to
}

impl Queue {
    /// Serves the queue until the device stops. `ready` gets the result of the setup, before
    /// the device may start.
    pub(crate) fn run(self, ready: &mpsc::Sender<io::Result<()>>) -> io::Result<()> {
        let q = self.q;
        if (q as usize) < sys::cpu_count()
            && let Err(e) = sys::pin_to_cpu(q as usize)
        {
            log(format_args!("ublk queue {q}: pin to CPU {q}: {e}"));
        }
        let setup = (|| {
            let page = sys::page_size();
            let descs = Mapping::device(
                self.cdev,
                (q as usize * round_up(MAX_QUEUE_DEPTH * DESC_LEN, page)) as u64,
                round_up(self.depth as usize * DESC_LEN, page),
            )?;
            let ring = IoUring::new(4 * u32::from(self.depth))?;
            Ok::<_, io::Error>((descs, ring, sys::eventfd()?))
        })();
        let (descs, ring, efd) = match setup {
            Ok(s) => s,
            Err(e) => {
                let _ = ready.send(Err(io::Error::new(e.kind(), e.to_string())));
                return Err(e);
            }
        };
        let mut bufs = vec![0u8; self.depth as usize * MAX_IO].into_boxed_slice();
        let mut counter = Box::new([0u8; 8]);
        let mut r = Ring {
            uring: ring,
            cdev: self.cdev,
            q,
            bufs: bufs.as_mut_ptr(),
        };
        let started = (|| {
            for tag in 0..self.depth {
                r.io_cmd(IO_FETCH, tag, -1)?;
            }
            r.arm_eventfd(&efd, counter.as_mut_ptr())?;
            r.uring.submit()
        })();
        if let Err(e) = started {
            let _ = ready.send(Err(io::Error::new(e.kind(), e.to_string())));
            return Err(e);
        }
        let _ = ready.send(Ok(()));
        let served = self.serve(&mut r, &descs, &Arc::new(efd), counter.as_mut_ptr());
        drop(r);
        drop(bufs);
        served
    }

    /// Hands `started` to the engine runtime with one wakeup: one task runs the first request
    /// and spawns the rest from inside the runtime, onto its worker's own queue.
    fn start(&self, started: &mut Vec<(u16, Request, TagBuf)>, out: &Results) {
        if started.is_empty() {
            return;
        }
        let batch = std::mem::take(started);
        let (vol, out) = (Arc::clone(&self.vol), out.clone());
        self.rt.spawn(async move {
            let mut batch = batch.into_iter();
            let first = batch.next();
            for (tag, req, buf) in batch {
                tokio::spawn(run(Arc::clone(&vol), out.clone(), tag, req, buf));
            }
            if let Some((tag, req, buf)) = first {
                run(vol, out, tag, req, buf).await;
            }
        });
    }

    fn serve(&self, r: &mut Ring, descs: &Mapping, efd: &Arc<OwnedFd>, counter: *mut u8) -> io::Result<()> {
        let _rt = self.rt.enter();
        let (tx, rx) = mpsc::channel::<(u16, i32)>();
        let out = Results {
            tx,
            efd: Arc::clone(efd),
            pending: Arc::default(),
        };
        let mut started = Vec::with_capacity(self.depth as usize);
        let (mut active, mut inflight) = (self.depth as usize, 0usize);
        let mut batch = Vec::with_capacity(2 * self.depth as usize);
        let mut failed = None;
        while active > 0 && failed.is_none() {
            if let Err(e) = r.wait() {
                failed = Some(e);
                break;
            }
            batch.clear();
            batch.extend(r.uring.completion().map(|c| (c.user_data(), c.result())));
            let new = batch.iter().filter(|(u, res)| *u != EFD_TAG && *res == 0).count();
            for &(user, res) in &batch {
                if user == EFD_TAG {
                    out.pending.store(false, Ordering::Release);
                    while let Ok((tag, res)) = rx.try_recv() {
                        inflight -= 1;
                        if let Err(e) = r.io_cmd(IO_COMMIT_AND_FETCH, tag, res) {
                            failed.get_or_insert(e);
                        }
                    }
                    if let Err(e) = r.arm_eventfd(efd, counter) {
                        failed.get_or_insert(e);
                    }
                    continue;
                }
                let Ok(tag) = u16::try_from(user) else {
                    failed.get_or_insert_with(|| {
                        io::Error::other(format!("ublk queue {}: completion of unknown tag {user}", self.q))
                    });
                    continue;
                };
                if res != 0 {
                    if res != -libc::ENODEV {
                        log(format_args!(
                            "ublk queue {} tag {tag}: {}",
                            self.q,
                            io::Error::from_raw_os_error(-res)
                        ));
                    }
                    active -= 1;
                    continue;
                }
                let at = tag as usize * DESC_LEN;
                let head = descs.read_u64(at);
                let req = Request {
                    op_flags: (head & 0xffff_ffff) as u32,
                    len: ((head >> 32) as usize) << 9,
                    off: descs.read_u64(at + 8) << 9,
                };
                if req.has_data() && req.len > MAX_IO {
                    if let Err(e) = r.io_cmd(IO_COMMIT_AND_FETCH, tag, -libc::EINVAL) {
                        failed.get_or_insert(e);
                    }
                    continue;
                }
                let buf = TagBuf {
                    // SAFETY: tag < depth, so its region lies inside the buffers.
                    ptr: unsafe { r.bufs.add(tag as usize * MAX_IO) },
                    len: req.len,
                };
                if new == 1 && inflight == 0 {
                    // SAFETY: the kernel handed this tag to us and we commit it below.
                    if let Some(res) = try_read(&self.vol, req, unsafe { buf.slice() }) {
                        if let Err(e) = r.io_cmd(IO_COMMIT_AND_FETCH, tag, res) {
                            failed.get_or_insert(e);
                        }
                        continue;
                    }
                }
                inflight += 1;
                started.push((tag, req, buf));
            }
            self.start(&mut started, &out);
        }
        for _ in 0..inflight {
            let _ = rx.recv();
        }
        failed.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_ioctls_match_the_kernel() {
        assert_eq!(io_op(IO_FETCH), 0xc010_7520);
        assert_eq!(io_op(IO_COMMIT_AND_FETCH), 0xc010_7521);
    }

    #[test]
    fn descriptor_areas_are_page_aligned() {
        assert_eq!(round_up(64 * DESC_LEN, 4096), 4096);
        assert_eq!(round_up(MAX_QUEUE_DEPTH * DESC_LEN, 4096), 24 * 4096);
    }
}
