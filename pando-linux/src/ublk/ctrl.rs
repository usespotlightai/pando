//! ublk control commands on `/dev/ublk-control`.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;

use io_uring::{IoUring, cqueue, opcode, squeue, types};

use crate::sys;

pub(crate) const CMD_GET_DEV_INFO: u32 = 0x02;
pub(crate) const CMD_ADD_DEV: u32 = 0x04;
pub(crate) const CMD_DEL_DEV: u32 = 0x05;
pub(crate) const CMD_START_DEV: u32 = 0x06;
pub(crate) const CMD_STOP_DEV: u32 = 0x07;
pub(crate) const CMD_SET_PARAMS: u32 = 0x08;
pub(crate) const CMD_START_USER_RECOVERY: u32 = 0x10;
pub(crate) const CMD_END_USER_RECOVERY: u32 = 0x11;
pub(crate) const CMD_GET_FEATURES: u32 = 0x13;
pub(crate) const CMD_UPDATE_SIZE: u32 = 0x15;

pub(crate) const F_USER_RECOVERY: u64 = 1 << 3;
pub(crate) const F_USER_RECOVERY_REISSUE: u64 = 1 << 4;
pub(crate) const F_CMD_IOCTL_ENCODE: u64 = 1 << 6;
pub(crate) const F_UPDATE_SIZE: u64 = 1 << 10;
pub(crate) const F_NO_AUTO_PART_SCAN: u64 = 1 << 18;

pub(crate) const STATE_QUIESCED: u16 = 2;
pub(crate) const STATE_FAIL_IO: u16 = 3;

const ATTR_VOLATILE_CACHE: u32 = 1 << 2;
const ATTR_FUA: u32 = 1 << 3;
const PARAM_TYPE_BASIC: u32 = 1;
const PARAM_TYPE_DISCARD: u32 = 1 << 1;
/// The most bytes one discard or write-zeroes request clears; its result still fits an i32.
const MAX_CLEAR: u32 = 1 << 30;

const INFO_LEN: usize = 64;
const PARAMS_LEN: usize = 60;

fn op(nr: u32) -> u32 {
    let dir = if matches!(nr, CMD_GET_DEV_INFO | CMD_GET_FEATURES) {
        2
    } else {
        3
    };
    sys::ioc(dir, b'u', nr, 32) as u32
}

/// What `GET_DEV_INFO` reports of a device.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Info {
    pub(crate) queues: u16,
    pub(crate) depth: u16,
    pub(crate) state: u16,
}

impl Info {
    fn decode(b: &[u8]) -> Info {
        let u16_at = |i: usize| u16::from_le_bytes([b[i], b[i + 1]]);
        Info {
            queues: u16_at(0),
            depth: u16_at(2),
            state: u16_at(4),
        }
    }
}

/// The device a new ublk device is added as.
#[derive(Clone, Copy, Debug)]
pub(crate) struct NewDevice {
    pub(crate) id: u32,
    pub(crate) queues: u16,
    pub(crate) depth: u16,
    pub(crate) max_io: u32,
    pub(crate) flags: u64,
}

fn encode_info(d: &NewDevice) -> Vec<u8> {
    let mut b = vec![0u8; INFO_LEN];
    b[0..2].copy_from_slice(&d.queues.to_le_bytes());
    b[2..4].copy_from_slice(&d.depth.to_le_bytes());
    b[8..12].copy_from_slice(&d.max_io.to_le_bytes());
    b[12..16].copy_from_slice(&d.id.to_le_bytes());
    b[24..32].copy_from_slice(&d.flags.to_le_bytes());
    b
}

/// Basic parameters: 4 KiB blocks, a volatile cache with FUA, and the device size. Discard
/// and write-zeroes clear any 4 KiB-aligned range, which then reads as zeros.
fn encode_params(size: u64, max_io: u32) -> Vec<u8> {
    let mut b = vec![0u8; PARAMS_LEN];
    b[0..4].copy_from_slice(&(PARAMS_LEN as u32).to_le_bytes());
    b[4..8].copy_from_slice(&(PARAM_TYPE_BASIC | PARAM_TYPE_DISCARD).to_le_bytes());
    b[8..12].copy_from_slice(&(ATTR_VOLATILE_CACHE | ATTR_FUA).to_le_bytes());
    b[12..16].copy_from_slice(&[12, 12, 12, 12]);
    b[16..20].copy_from_slice(&(max_io >> 9).to_le_bytes());
    b[24..32].copy_from_slice(&(size >> 9).to_le_bytes());
    b[44..48].copy_from_slice(&4096u32.to_le_bytes());
    b[48..52].copy_from_slice(&(MAX_CLEAR >> 9).to_le_bytes());
    b[52..56].copy_from_slice(&(MAX_CLEAR >> 9).to_le_bytes());
    b[56..58].copy_from_slice(&1u16.to_le_bytes());
    b
}

/// A ring for control commands, with 128-byte entries for their 32-byte payload.
pub(crate) struct Ctrl {
    file: File,
    ring: IoUring<squeue::Entry128, cqueue::Entry>,
    seq: u64,
}

impl std::fmt::Debug for Ctrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctrl")
            .field("seq", &self.seq)
            .finish_non_exhaustive()
    }
}

impl Ctrl {
    pub(crate) fn open() -> io::Result<Ctrl> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_CLOEXEC)
            .open("/dev/ublk-control")?;
        let ring = IoUring::<squeue::Entry128, cqueue::Entry>::builder().build(32)?;
        Ok(Ctrl { file, ring, seq: 0 })
    }

    /// Runs one command on device `id`. `buf` is its payload, which the kernel reads or fills.
    /// It waits for the result tagged with this command: the wait can end on a signal first.
    pub(crate) fn cmd(&mut self, nr: u32, id: u32, data: u64, buf: Option<&mut [u8]>) -> io::Result<()> {
        self.seq += 1;
        let tag = self.seq;
        let mut cmd = [0u8; 80];
        cmd[0..4].copy_from_slice(&id.to_le_bytes());
        cmd[4..6].copy_from_slice(&u16::MAX.to_le_bytes());
        if let Some(b) = &buf {
            cmd[6..8].copy_from_slice(&(b.len() as u16).to_le_bytes());
            cmd[8..16].copy_from_slice(&(b.as_ptr() as u64).to_le_bytes());
        }
        cmd[16..24].copy_from_slice(&data.to_le_bytes());
        let e = opcode::UringCmd80::new(types::Fd(self.file.as_raw_fd()), op(nr))
            .cmd(cmd)
            .build()
            .user_data(tag);
        // SAFETY: the payload, if any, is `buf`, borrowed until this function returns, which
        // is after the kernel completes the command.
        unsafe { self.ring.submission().push(&e) }.map_err(|_| io::Error::other("control ring full"))?;
        loop {
            match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e),
            }
            let mut res = None;
            for c in self.ring.completion() {
                if c.user_data() == tag {
                    res = Some(c.result());
                }
            }
            if let Some(r) = res {
                return if r < 0 {
                    Err(io::Error::from_raw_os_error(-r))
                } else {
                    Ok(())
                };
            }
        }
    }

    /// The ublk flags this kernel supports, or 0 if it cannot say.
    pub(crate) fn features(&mut self) -> u64 {
        let mut b = [0u8; 8];
        match self.cmd(CMD_GET_FEATURES, 0, 0, Some(&mut b)) {
            Ok(()) => u64::from_le_bytes(b),
            Err(_) => 0,
        }
    }

    pub(crate) fn info(&mut self, id: u32) -> io::Result<Info> {
        let mut b = [0u8; INFO_LEN];
        self.cmd(CMD_GET_DEV_INFO, id, 0, Some(&mut b))?;
        Ok(Info::decode(&b))
    }

    pub(crate) fn add(&mut self, d: &NewDevice) -> io::Result<()> {
        let mut b = encode_info(d);
        self.cmd(CMD_ADD_DEV, d.id, 0, Some(&mut b))
    }

    pub(crate) fn set_params(&mut self, id: u32, size: u64, max_io: u32) -> io::Result<()> {
        let mut b = encode_params(size, max_io);
        self.cmd(CMD_SET_PARAMS, id, 0, Some(&mut b))
    }

    /// Sets the size of started device `id` to `size` bytes, a multiple of 512. Linux 6.16
    /// and later.
    pub(crate) fn update_size(&mut self, id: u32, size: u64) -> io::Result<()> {
        if self.features() & F_UPDATE_SIZE == 0 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "this kernel cannot resize a ublk device; it needs Linux 6.16 or later",
            ));
        }
        self.cmd(CMD_UPDATE_SIZE, id, size >> 9, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_ioctls_match_the_kernel() {
        assert_eq!(op(CMD_ADD_DEV), 0xc0207504);
        assert_eq!(op(CMD_GET_DEV_INFO), 0x80207502);
        assert_eq!(op(CMD_GET_FEATURES), 0x80207513);
        assert_eq!(op(CMD_UPDATE_SIZE), 0xc0207515);
    }

    #[test]
    fn a_new_device_reads_back_its_queues_and_depth() {
        let d = NewDevice {
            id: 3,
            queues: 2,
            depth: 64,
            max_io: 512 << 10,
            flags: F_USER_RECOVERY,
        };
        let b = encode_info(&d);
        assert_eq!(
            Info::decode(&b),
            Info {
                queues: 2,
                depth: 64,
                state: 0
            }
        );
        assert_eq!(u32::from_le_bytes(b[12..16].try_into().unwrap()), 3);
        let p = encode_params(1 << 30, 512 << 10);
        assert_eq!(u64::from_le_bytes(p[24..32].try_into().unwrap()), (1 << 30) >> 9);
        assert_eq!(u32::from_le_bytes(p[16..20].try_into().unwrap()), 1024);
    }

    #[test]
    fn params_offer_discard_and_write_zeroes_in_one_segment() {
        let p = encode_params(1 << 30, 512 << 10);
        let u32_at = |i: usize| u32::from_le_bytes(p[i..i + 4].try_into().unwrap());
        assert_eq!(u32_at(0) as usize, p.len());
        assert_eq!(u32_at(4), PARAM_TYPE_BASIC | PARAM_TYPE_DISCARD);
        assert_eq!(u32_at(40), 0);
        assert_eq!(u32_at(44), 4096);
        assert_eq!(u32_at(48), 1 << 21);
        assert_eq!(u32_at(52), 1 << 21);
        assert_eq!(u16::from_le_bytes([p[56], p[57]]), 1);
    }
}
