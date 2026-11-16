#![allow(unsafe_code)]

use std::fs::{File, OpenOptions};
use std::io::{self, IoSliceMut};
use std::num::NonZeroU64;
use std::os::fd::AsRawFd;
use std::path::Path;

use pando_engine::ports::Advice;
use rustix::fs::{FallocateFlags, fadvise, fallocate, ioctl_ficlone};
use rustix::io::{Errno, ReadWriteFlags, preadv2};
use rustix::ioctl::{Getter, Setter, ioctl, opcode};

pub(crate) fn read_nowait(f: &File, buf: &mut [u8], offset: u64) -> io::Result<Option<usize>> {
    match preadv2(f, &mut [IoSliceMut::new(buf)], offset, ReadWriteFlags::NOWAIT) {
        Ok(n) => Ok(Some(n)),
        Err(Errno::AGAIN | Errno::OPNOTSUPP) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn start_writeback(f: &File) {
    // SAFETY: sync_file_range only reads the descriptor, which `f` keeps open for the call.
    let _ = unsafe { libc::sync_file_range(f.as_raw_fd(), 0, 0, libc::SYNC_FILE_RANGE_WRITE) };
}

pub(crate) fn advise(f: &File, offset: u64, len: u64, advice: Advice) {
    let advice = match advice {
        Advice::WillNeed => rustix::fs::Advice::WillNeed,
        Advice::DontNeed => rustix::fs::Advice::DontNeed,
    };
    let _ = fadvise(f, offset, NonZeroU64::new(len), advice);
}

/// Clones `from` to a new file `to` sharing its extents; `false` when the file system cannot.
pub(crate) fn reflink(from: &Path, to: &Path) -> io::Result<bool> {
    let src = File::open(from)?;
    let dst = OpenOptions::new().write(true).create_new(true).open(to)?;
    match ioctl_ficlone(&dst, &src) {
        Ok(()) => Ok(true),
        Err(Errno::OPNOTSUPP | Errno::XDEV | Errno::INVAL) => {
            drop(dst);
            std::fs::remove_file(to)?;
            Ok(false)
        }
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn punch_hole(f: &File, offset: u64, len: u64) -> io::Result<()> {
    Ok(fallocate(
        f,
        FallocateFlags::PUNCH_HOLE | FallocateFlags::KEEP_SIZE,
        offset,
        len,
    )?)
}

/// `struct fsxattr` of linux/fs.h.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct FsXattr {
    xflags: u32,
    extsize: u32,
    nextents: u32,
    projid: u32,
    cowextsize: u32,
    pad: [u8; 8],
}

const FS_IOC_FSGETXATTR: rustix::ioctl::Opcode = opcode::read::<FsXattr>(b'X', 31);
const FS_IOC_FSSETXATTR: rustix::ioctl::Opcode = opcode::write::<FsXattr>(b'X', 32);
const FS_XFLAG_EXTSIZE: u32 = 0x800;
const FS_XFLAG_COWEXTSIZE: u32 = 0x10000;

/// Asks the file system to allocate `f`, and its copy-on-write writes, in extents of `bytes`.
/// It must come before `f` has blocks. XFS uses both hints; other file systems refuse them.
pub(crate) fn extent_hint(f: &File, bytes: u64) -> io::Result<()> {
    let size = u32::try_from(bytes).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: FS_IOC_FSGETXATTR fills exactly one `struct fsxattr`, which FsXattr mirrors.
    let mut x = unsafe { ioctl(f, Getter::<FS_IOC_FSGETXATTR, FsXattr>::new()) }?;
    x.xflags |= FS_XFLAG_EXTSIZE | FS_XFLAG_COWEXTSIZE;
    x.extsize = size;
    x.cowextsize = size;
    // SAFETY: FS_IOC_FSSETXATTR reads exactly one `struct fsxattr`, which FsXattr mirrors.
    unsafe { ioctl(f, Setter::<FS_IOC_FSSETXATTR, FsXattr>::new(x)) }?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ioctl_numbers_match_linux() {
        assert_eq!(FS_IOC_FSGETXATTR, 0x801c_581f);
        assert_eq!(FS_IOC_FSSETXATTR, 0x401c_5820);
    }
}
