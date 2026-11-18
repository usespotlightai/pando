//! The FUSE frontend: serves a branch as one file, `disk.img`, in a FUSE mount of its own, and
//! binds a loop device to that file. A file system on the loop device then reads and writes the
//! branch, on a kernel that offers FUSE and loop devices but not ublk.
//!
//! The file is opened with `FOPEN_DIRECT_IO`, so the kernel keeps no page cache of it: the only
//! cache of the branch's blocks is that of the file system on the loop device. The loop device
//! also uses direct I/O when the kernel allows it. FUSE `fsync` and `flush` reach
//! [`Volume::flush`]; `fallocate` that punches a hole or zeroes a range reaches
//! [`Volume::zero`], which is how discard and write-zeroes on the loop device arrive.
//!
//! Threads wait in `poll` on `/dev/fuse` and a stop eventfd. A read the volume can serve from
//! memory without waiting is answered on the thread; every other request runs on the engine's
//! runtime, which writes the reply.

use std::fs;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::SystemTime;

use pando_engine::Volume;
use parking_lot::Mutex;
use tokio::runtime::Handle;

use crate::{dm, sys};

/// The name of the file that holds the branch, in the mount directory.
pub const FILE_NAME: &str = "disk.img";

const ROOT: u64 = 1;
const FILE: u64 = 2;

const KERNEL_MAJOR: u32 = 7;
const MINOR: u32 = 31;
const MAX_WRITE: u32 = 1 << 20;
const MAX_PAGES: u16 = 256;
const MAX_BACKGROUND: u16 = 128;
const CONGESTION_THRESHOLD: u16 = 96;
const BUF_LEN: usize = MAX_WRITE as usize + 4096;
const IN_HEADER: usize = 40;
const OUT_HEADER: usize = 16;
const ATTR_LEN: usize = 88;
const BLOCK: u32 = 4096;

const INIT_ASYNC_READ: u32 = 1 << 0;
const INIT_BIG_WRITES: u32 = 1 << 5;
const INIT_ASYNC_DIO: u32 = 1 << 15;
const INIT_MAX_PAGES: u32 = 1 << 22;
const WANTED: u32 = INIT_ASYNC_READ | INIT_BIG_WRITES | INIT_ASYNC_DIO | INIT_MAX_PAGES;

const FOPEN_DIRECT_IO: u32 = 1 << 0;
const FOPEN_PARALLEL_DIRECT_WRITES: u32 = 1 << 6;

const FATTR_SIZE: u32 = 1 << 3;

const FALLOC_KEEP_SIZE: u32 = 0x01;
const FALLOC_PUNCH_HOLE: u32 = 0x02;
const FALLOC_ZERO_RANGE: u32 = 0x10;

const DT_DIR: u32 = 4;
const DT_REG: u32 = 8;

mod op {
    pub(super) const LOOKUP: u32 = 1;
    pub(super) const FORGET: u32 = 2;
    pub(super) const GETATTR: u32 = 3;
    pub(super) const SETATTR: u32 = 4;
    pub(super) const SYMLINK: u32 = 6;
    pub(super) const MKNOD: u32 = 8;
    pub(super) const MKDIR: u32 = 9;
    pub(super) const UNLINK: u32 = 10;
    pub(super) const RMDIR: u32 = 11;
    pub(super) const RENAME: u32 = 12;
    pub(super) const LINK: u32 = 13;
    pub(super) const OPEN: u32 = 14;
    pub(super) const READ: u32 = 15;
    pub(super) const WRITE: u32 = 16;
    pub(super) const STATFS: u32 = 17;
    pub(super) const RELEASE: u32 = 18;
    pub(super) const FSYNC: u32 = 20;
    pub(super) const SETXATTR: u32 = 21;
    pub(super) const REMOVEXATTR: u32 = 24;
    pub(super) const FLUSH: u32 = 25;
    pub(super) const INIT: u32 = 26;
    pub(super) const OPENDIR: u32 = 27;
    pub(super) const READDIR: u32 = 28;
    pub(super) const RELEASEDIR: u32 = 29;
    pub(super) const FSYNCDIR: u32 = 30;
    pub(super) const ACCESS: u32 = 34;
    pub(super) const CREATE: u32 = 35;
    pub(super) const INTERRUPT: u32 = 36;
    pub(super) const DESTROY: u32 = 38;
    pub(super) const BATCH_FORGET: u32 = 42;
    pub(super) const FALLOCATE: u32 = 43;
    pub(super) const RENAME2: u32 = 45;
}

/// How a FUSE frontend starts.
#[derive(Clone, Copy, Debug)]
pub struct FuseOptions {
    /// Threads that read requests from the kernel.
    pub threads: u16,
}

impl Default for FuseOptions {
    fn default() -> FuseOptions {
        FuseOptions { threads: 2 }
    }
}

/// The FUSE connection the threads serve.
#[derive(Debug)]
struct Conn {
    fd: OwnedFd,
    stop: OwnedFd,
    uid: u32,
    gid: u32,
    time: u64,
}

/// A branch served as `disk.img` in a FUSE mount, bound to a loop device.
#[derive(Debug)]
pub struct FuseFrontend {
    dir: PathBuf,
    device: PathBuf,
    direct_io: bool,
    seq: String,
    flusher: Option<String>,
    conn: Arc<Conn>,
    loop_fd: Mutex<Option<OwnedFd>>,
    threads: Mutex<Vec<JoinHandle<io::Result<()>>>>,
}

impl FuseFrontend {
    /// Mounts a FUSE file system at `dir`, which it makes if needed, serves `vol` in it as
    /// [`FILE_NAME`] on runtime `rt`, and binds a free loop device to that file. A dead FUSE
    /// mount an earlier daemon left at `dir` is unmounted first.
    pub async fn start(
        rt: Handle,
        vol: Arc<Volume>,
        dir: &Path,
        opts: FuseOptions,
    ) -> io::Result<FuseFrontend> {
        let dir = dir.to_owned();
        dm::blocking(move || start(&rt, &vol, dir, opts)).await
    }

    /// The loop device bound to the file, `/dev/loopN`.
    pub fn device(&self) -> &Path {
        &self.device
    }

    /// The file that holds the branch.
    pub fn file(&self) -> PathBuf {
        self.dir.join(FILE_NAME)
    }

    /// Whether the loop device reads and writes the file with direct I/O.
    pub fn direct_io(&self) -> bool {
        self.direct_io
    }

    /// Why the serving threads are not I/O flushers, when they are not.
    pub fn not_flusher(&self) -> Option<&str> {
        self.flusher.as_deref()
    }

    /// Waits until the FUSE connection ends, which ends every thread: the mount is gone and
    /// nothing holds the file any more, or [`Self::unmount`] stopped it. The error names each
    /// thread that ended with one.
    pub async fn stopped(&self) -> io::Result<()> {
        let threads = std::mem::take(&mut *self.threads.lock());
        dm::blocking(move || join(threads)).await
    }

    /// Lets go of the loop device, which frees itself once its last user closes it, and
    /// detaches the mount; the threads end once the loop device has let go of the file, after
    /// it has had an answer to every request. With `force`, they end at once, and whatever
    /// still uses the loop device gets I/O errors.
    pub async fn unmount(&self, force: bool) -> io::Result<()> {
        drop(self.loop_fd.lock().take());
        let dir = self.dir.clone();
        let detached = dm::blocking(move || sys::umount(&dir, libc::MNT_DETACH)).await;
        if force || detached.is_err() {
            sys::eventfd_signal(&self.conn.stop);
        } else {
            let (conn, device, seq) = (Arc::downgrade(&self.conn), self.device.clone(), self.seq.clone());
            tokio::task::spawn_blocking(move || {
                while let Some(c) = conn.upgrade() {
                    if released(&device, &seq) {
                        sys::eventfd_signal(&c.stop);
                        return;
                    }
                    drop(c);
                    std::thread::sleep(RELEASE_POLL);
                }
            });
        }
        detached
    }

    /// Makes the loop device and the file system on it see the volume's current size, after
    /// [`Volume::grow`].
    pub async fn resize(&self) -> io::Result<()> {
        let (file, device) = (self.file(), self.device.clone());
        dm::blocking(move || {
            fs::metadata(&file)?;
            dm::refresh_loop(&device)
        })
        .await
    }

    /// Ends the connection and the threads when the start of what sits on the device failed.
    pub async fn abort(self) -> io::Result<()> {
        let unmounted = self.unmount(true).await;
        let stopped = self.stopped().await;
        unmounted.and(stopped)
    }
}

const RELEASE_POLL: std::time::Duration = std::time::Duration::from_millis(10);

/// The loop device's sequence number, which changes each time it is bound to a file; empty on
/// kernels before 5.15.
fn disk_seq(device: &Path) -> String {
    fs::read_to_string(
        Path::new("/sys/block")
            .join(device.file_name().unwrap_or_default())
            .join("diskseq"),
    )
    .map_or_default(|s| s.trim().to_owned())
}

/// Whether the loop device no longer holds the file it held when its sequence number was `seq`.
fn released(device: &Path, seq: &str) -> bool {
    let block = Path::new("/sys/block").join(device.file_name().unwrap_or_default());
    !block.join("loop/backing_file").exists() || disk_seq(device) != seq
}

fn start(rt: &Handle, vol: &Arc<Volume>, dir: PathBuf, opts: FuseOptions) -> io::Result<FuseFrontend> {
    if let Err(e) = fs::metadata(&dir)
        && e.raw_os_error() == Some(libc::ENOTCONN)
    {
        sys::umount(&dir, libc::MNT_DETACH)?;
    }
    fs::create_dir_all(&dir)?;
    let fd: OwnedFd = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open("/dev/fuse")?
        .into();
    let (uid, gid) = sys::ids();
    let options = format!(
        "fd={},rootmode=40000,user_id={uid},group_id={gid},default_permissions,allow_other",
        fd.as_raw_fd()
    );
    sys::mount(
        "pando",
        &dir,
        "fuse.pando",
        libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOATIME,
        &options,
    )
    .map_err(|e| io::Error::new(e.kind(), format!("mount FUSE at {}: {e}", dir.display())))?;
    let conn = Arc::new(Conn {
        fd,
        stop: sys::eventfd()?,
        uid,
        gid,
        time: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
    });
    let mut fe = FuseFrontend {
        dir,
        device: PathBuf::new(),
        direct_io: false,
        seq: String::new(),
        flusher: sys::io_flusher().err().map(|e| e.to_string()),
        conn: Arc::clone(&conn),
        loop_fd: Mutex::new(None),
        threads: Mutex::new(Vec::new()),
    };
    let spawned = (0..opts.threads.max(1)).try_for_each(|i| {
        let (conn, rt, vol) = (Arc::clone(&conn), rt.clone(), Arc::clone(vol));
        let t = std::thread::Builder::new()
            .name(format!("fuse-{i}"))
            .spawn(move || serve(&conn, &rt, &vol))?;
        fe.threads.lock().push(t);
        Ok::<_, io::Error>(())
    });
    let bound = spawned.and_then(|()| dm::attach_loop(&fe.file()));
    match bound {
        Ok((device, lfd)) => {
            fe.direct_io = fs::read_to_string(
                Path::new("/sys/block")
                    .join(device.file_name().unwrap_or_default())
                    .join("loop/dio"),
            )
            .is_ok_and(|s| s.trim() == "1");
            fe.seq = disk_seq(&device);
            fe.device = device;
            *fe.loop_fd.lock() = Some(lfd);
            Ok(fe)
        }
        Err(e) => {
            let _ = sys::umount(&fe.dir, libc::MNT_DETACH);
            sys::eventfd_signal(&conn.stop);
            let _ = join(std::mem::take(&mut *fe.threads.lock()));
            Err(io::Error::new(
                e.kind(),
                format!("bind a loop device to {}: {e}", fe.file().display()),
            ))
        }
    }
}

fn join(threads: Vec<JoinHandle<io::Result<()>>>) -> io::Result<()> {
    let mut failed = Vec::new();
    for (i, t) in threads.into_iter().enumerate() {
        match t.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => failed.push(format!("fuse thread {i}: {e}")),
            Err(_) => failed.push(format!("fuse thread {i} panicked")),
        }
    }
    if failed.is_empty() {
        Ok(())
    } else {
        Err(io::Error::other(failed.join("; ")))
    }
}

/// Reads and answers requests until the connection ends or the frontend stops.
fn serve(conn: &Arc<Conn>, rt: &Handle, vol: &Arc<Volume>) -> io::Result<()> {
    let _ = sys::io_flusher();
    let mut buf = vec![0u8; BUF_LEN];
    let fds = [conn.fd.as_raw_fd(), conn.stop.as_raw_fd()];
    loop {
        let ready = match sys::poll_readable(&fds) {
            Ok(r) => r,
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => return Err(e),
        };
        if ready.get(1).copied().unwrap_or(false) {
            return Ok(());
        }
        let n = match sys::read(conn.fd.as_raw_fd(), &mut buf) {
            Ok(n) => n,
            Err(e) => match e.raw_os_error() {
                Some(libc::EAGAIN | libc::EINTR | libc::ENOENT) => continue,
                Some(libc::ENODEV) => return Ok(()),
                _ => return Err(e),
            },
        };
        let Some(req) = buf.get(..n) else { continue };
        handle(conn, rt, vol, req);
    }
}

/// The fixed part of a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Header {
    opcode: u32,
    unique: u64,
    node: u64,
}

fn ne32(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_ne_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

fn ne64(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_ne_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

fn header(b: &[u8]) -> Option<Header> {
    Some(Header {
        opcode: ne32(b, 4)?,
        unique: ne64(b, 8)?,
        node: ne64(b, 16)?,
    })
}

/// What a request asks for, decoded from its body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Request<'a> {
    Init {
        major: u32,
        minor: u32,
        readahead: u32,
        flags: u32,
    },
    Lookup {
        name: &'a [u8],
    },
    GetAttr,
    SetAttr {
        size: Option<u64>,
    },
    Open,
    OpenDir,
    ReadDir {
        offset: u64,
        size: u32,
    },
    Read {
        offset: u64,
        size: u32,
    },
    Write {
        offset: u64,
        data: &'a [u8],
    },
    Sync,
    Fallocate {
        offset: u64,
        len: u64,
        mode: u32,
    },
    StatFs,
    Done,
    NoReply,
    Refused(i32),
}

fn decode(h: Header, b: &[u8]) -> Option<Request<'_>> {
    let body = b.get(IN_HEADER..)?;
    Some(match h.opcode {
        op::INIT => Request::Init {
            major: ne32(body, 0)?,
            minor: ne32(body, 4)?,
            readahead: ne32(body, 8)?,
            flags: ne32(body, 12)?,
        },
        op::LOOKUP => Request::Lookup {
            name: body.split(|&c| c == 0).next().unwrap_or_default(),
        },
        op::GETATTR => Request::GetAttr,
        op::SETATTR => {
            let valid = ne32(body, 0)?;
            Request::SetAttr {
                size: (valid & FATTR_SIZE != 0).then(|| ne64(body, 16)).flatten(),
            }
        }
        op::OPEN => Request::Open,
        op::OPENDIR => Request::OpenDir,
        op::READDIR => Request::ReadDir {
            offset: ne64(body, 8)?,
            size: ne32(body, 16)?,
        },
        op::READ => Request::Read {
            offset: ne64(body, 8)?,
            size: ne32(body, 16)?,
        },
        op::WRITE => {
            let size = usize::try_from(ne32(body, 16)?).ok()?;
            Request::Write {
                offset: ne64(body, 8)?,
                data: body.get(40..40 + size)?,
            }
        }
        op::FSYNC | op::FLUSH => Request::Sync,
        op::FALLOCATE => Request::Fallocate {
            offset: ne64(body, 8)?,
            len: ne64(body, 16)?,
            mode: ne32(body, 24)?,
        },
        op::STATFS => Request::StatFs,
        op::RELEASE | op::RELEASEDIR | op::FSYNCDIR | op::ACCESS | op::DESTROY => Request::Done,
        op::FORGET | op::BATCH_FORGET | op::INTERRUPT => Request::NoReply,
        op::SYMLINK
        | op::MKNOD
        | op::MKDIR
        | op::UNLINK
        | op::RMDIR
        | op::RENAME
        | op::LINK
        | op::CREATE
        | op::RENAME2
        | op::SETXATTR
        | op::REMOVEXATTR => Request::Refused(libc::EPERM),
        _ => Request::Refused(libc::ENOSYS),
    })
}

fn handle(conn: &Arc<Conn>, rt: &Handle, vol: &Arc<Volume>, b: &[u8]) {
    let Some(h) = header(b) else { return };
    let Some(req) = decode(h, b) else {
        return conn.reply(h.unique, -libc::EINVAL, &[]);
    };
    let size = vol.size();
    match req {
        Request::Init {
            major,
            minor,
            readahead,
            flags,
        } => {
            if major < KERNEL_MAJOR {
                return conn.reply(h.unique, -libc::EPROTO, &[]);
            }
            conn.reply(h.unique, 0, &[&init_out(major, minor, readahead, flags)]);
        }
        Request::Lookup { name } if h.node == ROOT && name == FILE_NAME.as_bytes() => {
            conn.reply(h.unique, 0, &[&entry_out(&conn.attr(FILE, size))]);
        }
        Request::GetAttr | Request::SetAttr { size: None } if h.node == ROOT || h.node == FILE => {
            conn.reply(h.unique, 0, &[&attr_out(&conn.attr(h.node, size))]);
        }
        Request::SetAttr { size: Some(s) } if h.node == FILE && s == size => {
            conn.reply(h.unique, 0, &[&attr_out(&conn.attr(FILE, size))]);
        }
        Request::SetAttr { size: Some(_) } => conn.reply(h.unique, -libc::EPERM, &[]),
        Request::Lookup { .. } | Request::GetAttr | Request::SetAttr { .. } => {
            conn.reply(h.unique, -libc::ENOENT, &[]);
        }
        Request::Open if h.node == FILE => {
            conn.reply(
                h.unique,
                0,
                &[&open_out(FOPEN_DIRECT_IO | FOPEN_PARALLEL_DIRECT_WRITES)],
            );
        }
        Request::Open => conn.reply(h.unique, -libc::EISDIR, &[]),
        Request::OpenDir if h.node == ROOT => conn.reply(h.unique, 0, &[&open_out(0)]),
        Request::OpenDir => conn.reply(h.unique, -libc::ENOTDIR, &[]),
        Request::ReadDir { offset, size: max } => {
            conn.reply(
                h.unique,
                0,
                &[&dirents(offset, usize::try_from(max).unwrap_or(0))],
            );
        }
        Request::StatFs => conn.reply(h.unique, 0, &[&statfs_out(size)]),
        Request::Done => conn.reply(h.unique, 0, &[]),
        Request::NoReply => {}
        Request::Refused(e) => conn.reply(h.unique, -e, &[]),
        Request::Read { offset, size: want } => read(conn, rt, vol, h.unique, offset, want),
        Request::Write { offset, data } => write(conn, rt, vol, h.unique, offset, data),
        Request::Sync => {
            let (conn, vol) = (Arc::clone(conn), Arc::clone(vol));
            rt.spawn(async move {
                let e = errno(vol.flush().await, "flush");
                conn.reply(h.unique, e, &[]);
            });
        }
        Request::Fallocate { offset, len, mode } => fallocate(conn, rt, vol, h.unique, offset, len, mode),
    }
}

/// The part of `[offset, offset + len)` below `size`.
fn clamp(offset: u64, len: u64, size: u64) -> u64 {
    size.saturating_sub(offset).min(len)
}

fn read(conn: &Arc<Conn>, rt: &Handle, vol: &Arc<Volume>, unique: u64, offset: u64, want: u32) {
    let n = clamp(offset, u64::from(want), vol.size());
    let Ok(n) = usize::try_from(n) else {
        return conn.reply(unique, -libc::EINVAL, &[]);
    };
    let mut data = vec![0u8; n];
    if n == 0 || matches!(vol.try_read_at(&mut data, offset), Ok(true)) {
        return conn.reply(unique, 0, &[&data]);
    }
    let (conn, vol) = (Arc::clone(conn), Arc::clone(vol));
    rt.spawn(async move {
        match vol.read_at(&mut data, offset).await {
            Ok(()) => conn.reply(unique, 0, &[&data]),
            Err(e) => conn.reply(unique, errno(Err(e), "read"), &[]),
        }
    });
}

fn write(conn: &Arc<Conn>, rt: &Handle, vol: &Arc<Volume>, unique: u64, offset: u64, data: &[u8]) {
    let len = data.len() as u64;
    if clamp(offset, len, vol.size()) != len {
        return conn.reply(unique, -libc::ENOSPC, &[]);
    }
    let Ok(written) = u32::try_from(data.len()) else {
        return conn.reply(unique, -libc::EINVAL, &[]);
    };
    let data = data.to_vec();
    let (conn, vol) = (Arc::clone(conn), Arc::clone(vol));
    rt.spawn(async move {
        match vol.write_at(&data, offset).await {
            Ok(()) => conn.reply(unique, 0, &[&write_out(written)]),
            Err(e) => conn.reply(unique, errno(Err(e), "write"), &[]),
        }
    });
}

fn fallocate(
    conn: &Arc<Conn>,
    rt: &Handle,
    vol: &Arc<Volume>,
    unique: u64,
    offset: u64,
    len: u64,
    mode: u32,
) {
    let inside = clamp(offset, len, vol.size()) == len;
    let clears = mode & (FALLOC_PUNCH_HOLE | FALLOC_ZERO_RANGE) != 0;
    let known = mode & !(FALLOC_KEEP_SIZE | FALLOC_PUNCH_HOLE | FALLOC_ZERO_RANGE) == 0;
    if !known || !inside {
        return conn.reply(unique, -libc::EOPNOTSUPP, &[]);
    }
    if !clears {
        return conn.reply(unique, 0, &[]);
    }
    let (conn, vol) = (Arc::clone(conn), Arc::clone(vol));
    rt.spawn(async move {
        let e = errno(vol.zero(offset, len).await, "zero");
        conn.reply(unique, e, &[]);
    });
}

/// 0 for success; for an error, `-EIO` after it is logged.
fn errno(r: pando_engine::Result<()>, what: &str) -> i32 {
    match r {
        Ok(()) => 0,
        Err(e) => {
            log(format_args!("fuse {what}: {e}"));
            -libc::EIO
        }
    }
}

#[expect(
    clippy::print_stderr,
    reason = "FUSE threads and I/O tasks have no caller to return a diagnostic to; the daemon's stderr is its log"
)]
fn log(args: std::fmt::Arguments<'_>) {
    eprintln!("{args}");
}

impl Conn {
    /// Answers request `unique` with `error` (0 or a negative errno) and, on success, `body`.
    /// A request the kernel gave up on, or a connection that ended, takes no answer.
    fn reply(&self, unique: u64, error: i32, body: &[&[u8]]) {
        let body: &[&[u8]] = if error == 0 { body } else { &[] };
        let len = OUT_HEADER + body.iter().map(|b| b.len()).sum::<usize>();
        let head = out_header(u32::try_from(len).unwrap_or(u32::MAX), error, unique);
        let mut bufs = Vec::with_capacity(body.len() + 1);
        bufs.push(&head[..]);
        bufs.extend_from_slice(body);
        if let Err(e) = sys::writev(self.fd.as_raw_fd(), &bufs)
            && !matches!(e.raw_os_error(), Some(libc::ENOENT | libc::ENODEV))
        {
            log(format_args!("fuse reply: {e}"));
        }
    }

    fn attr(&self, node: u64, size: u64) -> [u8; ATTR_LEN] {
        let (mode, nlink, size) = if node == ROOT {
            (libc::S_IFDIR | 0o700, 2, 0)
        } else {
            (libc::S_IFREG | 0o600, 1, size)
        };
        let mut a = Enc::<ATTR_LEN>::new();
        a.u64(node)
            .u64(size)
            .u64(size.div_ceil(512))
            .u64(self.time)
            .u64(self.time)
            .u64(self.time)
            .u32(0)
            .u32(0)
            .u32(0)
            .u32(mode)
            .u32(nlink)
            .u32(self.uid)
            .u32(self.gid)
            .u32(0)
            .u32(BLOCK)
            .u32(0);
        a.done()
    }
}

/// Writes fields in native byte order into a buffer of `N` bytes.
struct Enc<const N: usize> {
    b: [u8; N],
    at: usize,
}

impl<const N: usize> Enc<N> {
    fn new() -> Self {
        Enc { b: [0; N], at: 0 }
    }

    fn put(&mut self, v: &[u8]) -> &mut Self {
        if let Some(d) = self.b.get_mut(self.at..self.at + v.len()) {
            d.copy_from_slice(v);
        }
        self.at += v.len();
        self
    }

    fn u64(&mut self, v: u64) -> &mut Self {
        self.put(&v.to_ne_bytes())
    }

    fn u32(&mut self, v: u32) -> &mut Self {
        self.put(&v.to_ne_bytes())
    }

    fn u16(&mut self, v: u16) -> &mut Self {
        self.put(&v.to_ne_bytes())
    }

    fn done(&self) -> [u8; N] {
        debug_assert!(self.at <= N);
        self.b
    }
}

fn out_header(len: u32, error: i32, unique: u64) -> [u8; OUT_HEADER] {
    let mut e = Enc::<OUT_HEADER>::new();
    e.u32(len).put(&error.to_ne_bytes()).u64(unique);
    e.done()
}

/// The answer to `INIT`: the kernel's version or ours if older, and the features both have.
fn init_out(major: u32, minor: u32, readahead: u32, flags: u32) -> [u8; 64] {
    let minor = if major > KERNEL_MAJOR {
        MINOR
    } else {
        minor.min(MINOR)
    };
    let mut e = Enc::<64>::new();
    e.u32(KERNEL_MAJOR)
        .u32(minor)
        .u32(readahead.min(MAX_WRITE))
        .u32(flags & WANTED)
        .u16(MAX_BACKGROUND)
        .u16(CONGESTION_THRESHOLD)
        .u32(MAX_WRITE)
        .u32(1)
        .u16(MAX_PAGES);
    e.done()
}

/// The answer to `LOOKUP`: the node and its attributes, cached for an hour; the attributes
/// themselves are never cached, so a stat after a grow sees the new size.
fn entry_out(attr: &[u8; ATTR_LEN]) -> [u8; 40 + ATTR_LEN] {
    let mut e = Enc::<{ 40 + ATTR_LEN }>::new();
    e.u64(ne64(attr, 0).unwrap_or(FILE))
        .u64(0)
        .u64(3600)
        .u64(0)
        .u32(0)
        .u32(0)
        .put(attr);
    e.done()
}

fn attr_out(attr: &[u8; ATTR_LEN]) -> [u8; 16 + ATTR_LEN] {
    let mut e = Enc::<{ 16 + ATTR_LEN }>::new();
    e.u64(0).u32(0).u32(0).put(attr);
    e.done()
}

fn open_out(flags: u32) -> [u8; 16] {
    let mut e = Enc::<16>::new();
    e.u64(0).u32(flags).u32(0);
    e.done()
}

fn write_out(size: u32) -> [u8; 8] {
    let mut e = Enc::<8>::new();
    e.u32(size).u32(0);
    e.done()
}

fn statfs_out(size: u64) -> [u8; 80] {
    let mut e = Enc::<80>::new();
    e.u64(size / u64::from(BLOCK))
        .u64(0)
        .u64(0)
        .u64(2)
        .u64(0)
        .u32(BLOCK)
        .u32(255)
        .u32(BLOCK);
    e.done()
}

/// The directory entries from `offset` on that fit in `max` bytes: `.`, `..` and the file.
fn dirents(offset: u64, max: usize) -> Vec<u8> {
    let entries: [(u64, &[u8], u32); 3] = [
        (ROOT, b".", DT_DIR),
        (ROOT, b"..", DT_DIR),
        (FILE, FILE_NAME.as_bytes(), DT_REG),
    ];
    let mut out = Vec::new();
    for (i, (ino, name, kind)) in (1u64..)
        .zip(entries)
        .skip(usize::try_from(offset).unwrap_or(usize::MAX))
    {
        let len = (24 + name.len()).next_multiple_of(8);
        if out.len() + len > max {
            break;
        }
        out.extend_from_slice(&ino.to_ne_bytes());
        out.extend_from_slice(&i.to_ne_bytes());
        out.extend_from_slice(&u32::try_from(name.len()).unwrap_or(0).to_ne_bytes());
        out.extend_from_slice(&kind.to_ne_bytes());
        out.extend_from_slice(name);
        out.resize(out.len() + len - 24 - name.len(), 0);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(opcode: u32, node: u64, body: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&u32::try_from(IN_HEADER + body.len()).unwrap().to_ne_bytes());
        b.extend_from_slice(&opcode.to_ne_bytes());
        b.extend_from_slice(&7u64.to_ne_bytes());
        b.extend_from_slice(&node.to_ne_bytes());
        b.extend_from_slice(&[0; 16]);
        b.extend_from_slice(body);
        b
    }

    fn decoded(opcode: u32, node: u64, body: &[u8]) -> Option<Request<'static>> {
        let b: &'static [u8] = Box::leak(request(opcode, node, body).into_boxed_slice());
        decode(header(b)?, b)
    }

    #[test]
    fn a_write_carries_its_offset_and_data() {
        let mut body = vec![0u8; 40];
        body[8..16].copy_from_slice(&8192u64.to_ne_bytes());
        body[16..20].copy_from_slice(&4u32.to_ne_bytes());
        body.extend_from_slice(b"abcd");
        assert_eq!(
            decoded(op::WRITE, FILE, &body),
            Some(Request::Write {
                offset: 8192,
                data: b"abcd"
            })
        );
        body.truncate(42);
        assert_eq!(decoded(op::WRITE, FILE, &body), None);
    }

    #[test]
    fn punch_hole_and_fsync_decode() {
        let mut body = vec![0u8; 32];
        body[8..16].copy_from_slice(&4096u64.to_ne_bytes());
        body[16..24].copy_from_slice(&(1u64 << 20).to_ne_bytes());
        body[24..28].copy_from_slice(&(FALLOC_PUNCH_HOLE | FALLOC_KEEP_SIZE).to_ne_bytes());
        assert_eq!(
            decoded(op::FALLOCATE, FILE, &body),
            Some(Request::Fallocate {
                offset: 4096,
                len: 1 << 20,
                mode: FALLOC_PUNCH_HOLE | FALLOC_KEEP_SIZE
            })
        );
        assert_eq!(decoded(op::FSYNC, FILE, &[0; 16]), Some(Request::Sync));
        assert_eq!(decoded(op::FLUSH, FILE, &[0; 24]), Some(Request::Sync));
    }

    #[test]
    fn changes_to_the_tree_are_refused_and_unknown_requests_are_not_implemented() {
        assert_eq!(
            decoded(op::UNLINK, ROOT, b"disk.img\0"),
            Some(Request::Refused(libc::EPERM))
        );
        assert_eq!(
            decoded(op::CREATE, ROOT, &[0; 24]),
            Some(Request::Refused(libc::EPERM))
        );
        assert_eq!(decoded(999, ROOT, &[]), Some(Request::Refused(libc::ENOSYS)));
        assert_eq!(decoded(op::FORGET, FILE, &[0; 8]), Some(Request::NoReply));
    }

    #[test]
    fn a_lookup_names_the_file_and_a_setattr_its_size() {
        assert_eq!(
            decoded(op::LOOKUP, ROOT, b"disk.img\0"),
            Some(Request::Lookup { name: b"disk.img" })
        );
        let mut body = vec![0u8; 88];
        body[0..4].copy_from_slice(&FATTR_SIZE.to_ne_bytes());
        body[16..24].copy_from_slice(&(1u64 << 30).to_ne_bytes());
        assert_eq!(
            decoded(op::SETATTR, FILE, &body),
            Some(Request::SetAttr { size: Some(1 << 30) })
        );
        body[0..4].copy_from_slice(&1u32.to_ne_bytes());
        assert_eq!(
            decoded(op::SETATTR, FILE, &body),
            Some(Request::SetAttr { size: None })
        );
    }

    #[test]
    fn init_keeps_only_the_features_both_sides_have() {
        let out = init_out(7, 41, 1 << 22, INIT_ASYNC_READ | INIT_MAX_PAGES | (1 << 10));
        assert_eq!(ne32(&out, 0), Some(7));
        assert_eq!(ne32(&out, 4), Some(MINOR));
        assert_eq!(ne32(&out, 8), Some(MAX_WRITE));
        assert_eq!(ne32(&out, 12), Some(INIT_ASYNC_READ | INIT_MAX_PAGES));
        assert_eq!(ne32(&out, 20), Some(MAX_WRITE));
        assert_eq!(out.get(28..30), Some(&MAX_PAGES.to_ne_bytes()[..]));
        assert_eq!(ne32(&init_out(7, 26, 0, 0), 4), Some(26));
    }

    #[test]
    fn the_directory_lists_its_entries_from_an_offset() {
        let all = dirents(0, 4096);
        assert_eq!(all.len(), 32 + 32 + 32);
        assert_eq!(ne64(&all, 64), Some(FILE));
        assert_eq!(ne64(&all, 72), Some(3));
        assert_eq!(all.get(88..96), Some(&b"disk.img"[..]));
        assert_eq!(dirents(2, 4096).len(), 32);
        assert_eq!(dirents(3, 4096), Vec::<u8>::new());
        assert_eq!(dirents(0, 40).len(), 32);
    }

    #[test]
    fn ranges_clamp_to_the_volume() {
        assert_eq!(clamp(0, 4096, 1 << 20), 4096);
        assert_eq!(clamp((1 << 20) - 512, 4096, 1 << 20), 512);
        assert_eq!(clamp(2 << 20, 4096, 1 << 20), 0);
    }

    #[test]
    fn replies_lead_with_their_length_error_and_request() {
        let h = out_header(16, -libc::ENOENT, 9);
        assert_eq!(ne32(&h, 0), Some(16));
        assert_eq!(ne32(&h, 4), Some((-libc::ENOENT).cast_unsigned()));
        assert_eq!(ne64(&h, 8), Some(9));
    }
}
