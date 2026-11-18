//! The system calls this crate makes, each behind a safe function.

use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr::NonNull;

/// An ioctl number: direction (1 write, 2 read, 3 both), type, number and argument size.
pub(crate) const fn ioc(dir: u32, kind: u8, nr: u32, size: u32) -> libc::c_ulong {
    ioc32(dir, kind, nr, size) as libc::c_ulong
}

/// [`ioc`] as the 32-bit command number `io_uring` commands carry.
pub(crate) const fn ioc32(dir: u32, kind: u8, nr: u32, size: u32) -> u32 {
    dir << 30 | size << 16 | (kind as u32) << 8 | nr
}

fn check(r: libc::c_int) -> io::Result<libc::c_int> {
    if r < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(r)
    }
}

/// An ioctl whose argument is the buffer `b`, which the kernel may read and write.
pub(crate) fn ioctl_buf(fd: RawFd, op: libc::c_ulong, b: &mut [u8]) -> io::Result<libc::c_int> {
    // SAFETY: every ioctl this crate sends with a buffer reads and writes at most the size
    // encoded in `op` or, for device-mapper, the data size in the header, which is `b.len()`.
    check(unsafe { libc::ioctl(fd, op, b.as_mut_ptr()) })
}

/// An ioctl that takes no argument and returns a number.
pub(crate) fn ioctl_int(fd: RawFd, op: libc::c_ulong) -> io::Result<libc::c_int> {
    // SAFETY: the ioctls sent this way take no pointer.
    check(unsafe { libc::ioctl(fd, op) })
}

/// Makes block device node `path` for device number `dev`, as encoded by device-mapper.
pub(crate) fn mknod_block(path: &Path, dev: u64) -> io::Result<()> {
    let p = CString::new(path.as_os_str().as_bytes()).map_err(io::Error::other)?;
    let (major, minor) = split_dev(dev);
    let devt = libc::makedev(major, minor);
    // SAFETY: `p` is a valid C string for the duration of the call.
    check(unsafe { libc::mknod(p.as_ptr(), libc::S_IFBLK | 0o600, devt) }).map(drop)
}

/// Major and minor of a device number in the kernel's encoding of 32-bit numbers.
pub(crate) fn split_dev(dev: u64) -> (u32, u32) {
    (
        ((dev >> 8) & 0xfff) as u32,
        (dev & 0xff) as u32 | ((dev >> 12) & 0xfff00) as u32,
    )
}

pub(crate) fn eventfd() -> io::Result<OwnedFd> {
    // SAFETY: eventfd takes no pointer; a non-negative result is a new descriptor we own.
    let fd = check(unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) })?;
    // SAFETY: `fd` was just returned by eventfd and is owned by nobody else.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Adds one to an eventfd counter, which wakes its reader.
pub(crate) fn eventfd_signal(fd: &OwnedFd) {
    let one = 1u64.to_ne_bytes();
    // SAFETY: the buffer holds the 8 bytes passed.
    unsafe { libc::write(fd.as_raw_fd(), one.as_ptr().cast(), 8) };
}

/// Pins the calling thread to `cpu`.
pub(crate) fn pin_to_cpu(cpu: usize) -> io::Result<()> {
    // SAFETY: cpu_set_t is plain data, valid when zeroed.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: CPU_SET writes inside `set`; cpu is checked against its size.
    unsafe { libc::CPU_SET(cpu % (8 * size_of::<libc::cpu_set_t>()), &mut set) };
    // SAFETY: `set` is valid for the size passed.
    check(unsafe { libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &raw const set) }).map(drop)
}

pub(crate) fn cpu_count() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
}

/// A shared read-only mapping of a device's memory.
#[derive(Debug)]
pub(crate) struct Mapping {
    ptr: NonNull<u64>,
    len: usize,
}

// SAFETY: the mapping is only read, with volatile reads, and unmapped once on drop.
unsafe impl Send for Mapping {}

impl Mapping {
    pub(crate) fn device(fd: RawFd, offset: u64, len: usize) -> io::Result<Mapping> {
        let offset =
            libc::off_t::try_from(offset).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        // SAFETY: a fresh mapping aliases no Rust memory; the result is checked.
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED | libc::MAP_POPULATE,
                fd,
                offset,
            )
        };
        if p == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Mapping {
            ptr: NonNull::new(p.cast()).ok_or_else(io::Error::last_os_error)?,
            len,
        })
    }

    /// Reads 8 bytes at `at`, which the kernel may change at any time.
    pub(crate) fn read_u64(&self, at: usize) -> u64 {
        assert!(at + 8 <= self.len && at.is_multiple_of(8));
        // SAFETY: `at / 8` words from the page-aligned start stay inside the mapping, checked above.
        let word = unsafe { self.ptr.as_ptr().add(at / 8) };
        // SAFETY: `word` is in bounds and aligned, and the mapping lives as long as self.
        unsafe { std::ptr::read_volatile(word) }
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: ptr and len are those of the mapping made in `device`.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}

fn cstring(p: &Path) -> io::Result<CString> {
    CString::new(p.as_os_str().as_bytes()).map_err(io::Error::other)
}

/// Mounts a file system of type `fstype` from `source` at `target` with `flags` and the
/// file system's own options `data`.
pub(crate) fn mount(
    source: &str,
    target: &Path,
    fstype: &str,
    flags: libc::c_ulong,
    data: &str,
) -> io::Result<()> {
    let s = CString::new(source).map_err(io::Error::other)?;
    let t = cstring(target)?;
    let f = CString::new(fstype).map_err(io::Error::other)?;
    let d = CString::new(data).map_err(io::Error::other)?;
    // SAFETY: every pointer is a valid C string for the duration of the call.
    check(unsafe { libc::mount(s.as_ptr(), t.as_ptr(), f.as_ptr(), flags, d.as_ptr().cast()) }).map(drop)
}

/// Unmounts the file system at `target` with `flags` such as `MNT_DETACH`.
pub(crate) fn umount(target: &Path, flags: libc::c_int) -> io::Result<()> {
    let t = cstring(target)?;
    // SAFETY: `t` is a valid C string for the duration of the call.
    check(unsafe { libc::umount2(t.as_ptr(), flags) }).map(drop)
}

/// Reads from `fd` into `buf`.
pub(crate) fn read(fd: RawFd, buf: &mut [u8]) -> io::Result<usize> {
    // SAFETY: the kernel writes at most `buf.len()` bytes into `buf`.
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    usize::try_from(n).map_err(|_| io::Error::last_os_error())
}

/// Writes the buffers in `bufs` to `fd` in one call.
pub(crate) fn writev(fd: RawFd, bufs: &[&[u8]]) -> io::Result<usize> {
    let iov: Vec<libc::iovec> = bufs
        .iter()
        .map(|b| libc::iovec {
            iov_base: b.as_ptr().cast_mut().cast(),
            iov_len: b.len(),
        })
        .collect();
    let n = libc::c_int::try_from(iov.len()).map_err(io::Error::other)?;
    // SAFETY: each iovec points into a slice of `bufs`, which outlive the call; the kernel only
    // reads them.
    let r = unsafe { libc::writev(fd, iov.as_ptr(), n) };
    usize::try_from(r).map_err(|_| io::Error::last_os_error())
}

/// Waits until one of `fds` is readable; which ones are.
pub(crate) fn poll_readable(fds: &[RawFd]) -> io::Result<Vec<bool>> {
    let mut p: Vec<libc::pollfd> = fds
        .iter()
        .map(|&fd| libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        })
        .collect();
    let n = libc::nfds_t::try_from(p.len()).map_err(io::Error::other)?;
    // SAFETY: `p` holds `n` pollfd entries the kernel may write.
    check(unsafe { libc::poll(p.as_mut_ptr(), n, -1) })?;
    Ok(p.iter().map(|e| e.revents != 0).collect())
}

/// The effective user and group of this process.
pub(crate) fn ids() -> (u32, u32) {
    // SAFETY: geteuid takes no argument and cannot fail.
    let uid = unsafe { libc::geteuid() };
    // SAFETY: getegid takes no argument and cannot fail.
    let gid = unsafe { libc::getegid() };
    (uid, gid)
}

/// Writes out the dirty data of the file system that `fd` is in.
pub(crate) fn syncfs(fd: RawFd) -> io::Result<()> {
    // SAFETY: syncfs takes a descriptor only.
    check(unsafe { libc::syncfs(fd) }).map(drop)
}

pub(crate) fn page_size() -> usize {
    // SAFETY: sysconf takes no pointer.
    let n = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(n).ok().filter(|&n| n > 0).unwrap_or(4096)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_numbers_split_like_the_kernel() {
        assert_eq!(split_dev((253 << 8) | 7), (253, 7));
        assert_eq!(split_dev((253 << 8) | 7 | (1 << 20)), (253, 7 | (1 << 8)));
    }
}
