//! The system calls this crate makes, each behind a safe function.

use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr::NonNull;

/// An ioctl number: direction (1 write, 2 read, 3 both), type, number and argument size.
pub(crate) const fn ioc(dir: u32, kind: u8, nr: u32, size: u32) -> libc::c_ulong {
    (dir << 30 | size << 16 | (kind as u32) << 8 | nr) as libc::c_ulong
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
    check(unsafe { libc::ioctl(fd, op as _, b.as_mut_ptr()) })
}

/// An ioctl that takes no argument and returns a number.
pub(crate) fn ioctl_int(fd: RawFd, op: libc::c_ulong) -> io::Result<libc::c_int> {
    // SAFETY: the ioctls sent this way take no pointer.
    check(unsafe { libc::ioctl(fd, op as _) })
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
fn split_dev(dev: u64) -> (u32, u32) {
    (
        ((dev >> 8) & 0xfff) as u32,
        ((dev & 0xff) | ((dev >> 12) & 0xfff00)) as u32,
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
    check(unsafe { libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set) }).map(drop)
}

pub(crate) fn cpu_count() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

/// A shared read-only mapping of a device's memory.
#[derive(Debug)]
pub(crate) struct Mapping {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: the mapping is only read, with volatile reads, and unmapped once on drop.
unsafe impl Send for Mapping {}

impl Mapping {
    pub(crate) fn device(fd: RawFd, offset: u64, len: usize) -> io::Result<Mapping> {
        // SAFETY: a fresh mapping aliases no Rust memory; the result is checked.
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED | libc::MAP_POPULATE,
                fd,
                offset as libc::off_t,
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
        // SAFETY: in bounds and aligned, checked above; the mapping lives as long as self.
        unsafe { std::ptr::read_volatile(self.ptr.as_ptr().add(at).cast::<u64>()) }
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: ptr and len are those of the mapping made in `device`.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}

pub(crate) fn page_size() -> usize {
    // SAFETY: sysconf takes no pointer.
    let n = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if n > 0 { n as usize } else { 4096 }
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
