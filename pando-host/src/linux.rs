#![expect(
    unsafe_code,
    reason = "the host ports call file ioctls, sync_file_range and mallopt directly"
)]

use std::fs::{File, OpenOptions};
use std::io::{self, IoSliceMut};
use std::num::NonZeroU64;
use std::os::fd::AsRawFd;
use std::path::Path;

use pando_engine::ports::{Advice, Extent};
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

/// Allocations of this many bytes or more get a mapping of their own.
#[cfg(target_env = "gnu")]
const MMAP_THRESHOLD: libc::c_int = 128 << 10;

#[cfg(target_env = "gnu")]
pub(crate) fn tune_malloc() {
    // SAFETY: mallopt only changes the allocator's settings, which it reads under its own locks.
    unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, MMAP_THRESHOLD) };
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

/// `struct file_clone_range` of linux/fs.h.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FileCloneRange {
    src_fd: i64,
    src_offset: u64,
    src_length: u64,
    dest_offset: u64,
}

const FICLONERANGE: rustix::ioctl::Opcode = opcode::write::<FileCloneRange>(0x94, 13);

/// Makes bytes `offset..offset + len` of `to` share the extents of the same bytes of `from`;
/// `false` when the file system cannot.
pub(crate) fn reflink_range(from: &File, to: &File, offset: u64, len: u64) -> io::Result<bool> {
    let arg = FileCloneRange {
        src_fd: i64::from(from.as_raw_fd()),
        src_offset: offset,
        src_length: len,
        dest_offset: offset,
    };
    // SAFETY: FICLONERANGE is a write ioctl whose argument is one `struct file_clone_range`,
    // which FileCloneRange mirrors.
    let set = unsafe { Setter::<FICLONERANGE, FileCloneRange>::new(arg) };
    // SAFETY: `set` pairs the opcode with the type the kernel reads, and `to` and `from`, whose
    // descriptor it names, stay open for the call.
    match unsafe { ioctl(to, set) } {
        Ok(()) => Ok(true),
        Err(Errno::OPNOTSUPP | Errno::XDEV | Errno::INVAL) => Ok(false),
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

/// Allocates the first `len` bytes of `f`; `Ok(false)` when the file system has no `fallocate`.
pub(crate) fn allocate(f: &File, len: u64) -> io::Result<bool> {
    match fallocate(f, FallocateFlags::empty(), 0, len) {
        Ok(()) => Ok(true),
        Err(Errno::OPNOTSUPP) => Ok(false),
        Err(e) => Err(e.into()),
    }
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
    // SAFETY: FS_IOC_FSGETXATTR is a read ioctl whose argument is one `struct fsxattr`, which
    // FsXattr mirrors.
    let get = unsafe { Getter::<FS_IOC_FSGETXATTR, FsXattr>::new() };
    // SAFETY: `get` pairs the opcode with the type the kernel fills, and `f` stays open for the call.
    let mut x = unsafe { ioctl(f, get) }?;
    x.xflags |= FS_XFLAG_EXTSIZE | FS_XFLAG_COWEXTSIZE;
    x.extsize = size;
    x.cowextsize = size;
    // SAFETY: FS_IOC_FSSETXATTR is a write ioctl whose argument is one `struct fsxattr`, which
    // FsXattr mirrors.
    let set = unsafe { Setter::<FS_IOC_FSSETXATTR, FsXattr>::new(x) };
    // SAFETY: `set` pairs the opcode with the type the kernel reads, and `f` stays open for the call.
    unsafe { ioctl(f, set) }?;
    Ok(())
}

const FS_IOC_FIEMAP: libc::c_ulong = 0xc020_660b;
const FIEMAP_FLAG_SYNC: u32 = 1;
const FIEMAP_EXTENT_LAST: u32 = 1;
const FIEMAP_EXTENT_UNWRITTEN: u32 = 0x800;
const FIEMAP_HEADER: usize = 32;
const FIEMAP_EXTENT: usize = 56;
const FIEMAP_BATCH: u32 = 512;

fn field<const N: usize>(b: &[u8], at: usize) -> io::Result<[u8; N]> {
    b.get(at..)
        .and_then(<[u8]>::first_chunk::<N>)
        .copied()
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))
}

fn le64(b: &[u8], at: usize) -> io::Result<u64> {
    field(b, at).map(u64::from_le_bytes)
}

fn le32(b: &[u8], at: usize) -> io::Result<u32> {
    field(b, at).map(u32::from_le_bytes)
}

/// The extents of `f`, after it writes back its dirty pages.
pub(crate) fn extents(f: &File) -> io::Result<Vec<Extent>> {
    let mut buf = vec![0u8; FIEMAP_HEADER + FIEMAP_BATCH as usize * FIEMAP_EXTENT];
    let mut out = Vec::new();
    let mut start = 0u64;
    loop {
        buf.fill(0);
        buf[0..8].copy_from_slice(&start.to_le_bytes());
        buf[8..16].copy_from_slice(&(u64::MAX - start).to_le_bytes());
        buf[16..20].copy_from_slice(&FIEMAP_FLAG_SYNC.to_le_bytes());
        buf[24..28].copy_from_slice(&FIEMAP_BATCH.to_le_bytes());
        // SAFETY: the kernel reads the header and writes at most fm_extent_count extents after
        // it, which `buf` holds.
        if unsafe { libc::ioctl(f.as_raw_fd(), FS_IOC_FIEMAP, buf.as_mut_ptr()) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let n = le32(&buf, 20)? as usize;
        if n == 0 {
            return Ok(out);
        }
        let mut last = false;
        for e in buf[FIEMAP_HEADER..].as_chunks::<FIEMAP_EXTENT>().0.iter().take(n) {
            let flags = le32(e, 40)?;
            let x = Extent {
                logical: le64(e, 0)?,
                physical: le64(e, 8)?,
                len: le64(e, 16)?,
                unwritten: flags & FIEMAP_EXTENT_UNWRITTEN != 0,
            };
            start = x.logical + x.len;
            last |= flags & FIEMAP_EXTENT_LAST != 0;
            out.push(x);
        }
        if last {
            return Ok(out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extents_map_written_ranges_and_skip_holes() {
        use std::os::unix::fs::FileExt;
        let dir = tempfile::tempdir().unwrap();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.path().join("x"))
            .unwrap();
        f.write_all_at(&[1u8; 8192], 0).unwrap();
        f.write_all_at(&[2u8; 4096], 1 << 20).unwrap();
        let es = match extents(&f) {
            Err(e) if e.raw_os_error() == Some(libc::EOPNOTSUPP) => return,
            r => r.unwrap(),
        };
        let mapped: u64 = es.iter().map(|e| e.len).sum();
        assert!((8192 + 4096..1 << 20).contains(&mapped), "{es:?}");
        assert!(es.iter().all(|e| !e.unwritten && e.physical != 0));
        assert!(
            es.iter()
                .any(|e| e.logical <= 1 << 20 && e.logical + e.len > 1 << 20)
        );
    }

    #[test]
    fn ioctl_numbers_match_linux() {
        assert_eq!(FS_IOC_FSGETXATTR, 0x801c_581f);
        assert_eq!(FS_IOC_FSSETXATTR, 0x401c_5820);
    }
}
