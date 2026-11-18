//! The [`DeviceSwitch`] port over device-mapper ioctls and loop devices.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use pando_engine::ports::DeviceSwitch;

use crate::sys;

const DM_IOCTL_SIZE: u32 = 312;
const DM_IOCTL_LEN: usize = DM_IOCTL_SIZE as usize;
const DM_SPEC_LEN: usize = 40;
const DM_DATA_LEN: usize = 16 << 10;
const DM_NAME_LEN: usize = 128;
const DM_VERSION_MAJOR: u32 = 4;

const DM_DEV_CREATE: u32 = 3;
const DM_DEV_REMOVE: u32 = 4;
const DM_DEV_SUSPEND: u32 = 6;
const DM_DEV_STATUS: u32 = 7;
const DM_TABLE_LOAD: u32 = 9;
const DM_TABLE_CLEAR: u32 = 10;
const DM_TABLE_STATUS: u32 = 12;

const DM_SUSPEND_FLAG: u32 = 1 << 1;
const DM_STATUS_TABLE_FLAG: u32 = 1 << 4;
const DM_BUFFER_FULL_FLAG: u32 = 1 << 8;
const DM_SKIP_LOCKFS_FLAG: u32 = 1 << 10;

const LOOP_CTL_GET_FREE: libc::c_ulong = 0x4C82;
const LOOP_CONFIGURE: libc::c_ulong = 0x4C0A;
const LOOP_SET_CAPACITY: libc::c_ulong = 0x4C07;
const LOOP_CONFIG_LEN: usize = 304;
const LO_FLAGS_AUTOCLEAR: u32 = 4;
const LO_FLAGS_DIRECT_IO: u32 = 16;

/// A suspend that waits only for requests in flight and does not freeze the file system: the
/// image then holds every completed write, the state of a disk after a power cut, which the
/// file system's journal recovers from.
const SUSPEND_FLAGS: u32 = DM_SUSPEND_FLAG | DM_SKIP_LOCKFS_FLAG;

/// How long to wait for a new device to become usable.
const USABLE_WAIT: Duration = Duration::from_secs(30);

fn dm_op(nr: u32) -> libc::c_ulong {
    sys::ioc(3, 0xfd, nr, DM_IOCTL_SIZE)
}

/// One target of a device-mapper table.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Target {
    start: u64,
    sectors: u64,
    kind: String,
    params: String,
}

/// The header of a device-mapper reply.
#[derive(Clone, Copy, Debug)]
struct Reply {
    flags: u32,
    targets: u32,
    open_count: i32,
    dev: u64,
}

fn field<const N: usize>(b: &[u8], at: usize) -> io::Result<[u8; N]> {
    b.get(at..)
        .and_then(<[u8]>::first_chunk::<N>)
        .copied()
        .ok_or_else(|| io::Error::other("short device-mapper reply"))
}

fn le32(b: &[u8], at: usize) -> io::Result<u32> {
    field(b, at).map(u32::from_le_bytes)
}

fn le64(b: &[u8], at: usize) -> io::Result<u64> {
    field(b, at).map(u64::from_le_bytes)
}

fn len32(n: usize) -> io::Result<u32> {
    u32::try_from(n).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}

fn cstr(b: &[u8]) -> String {
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
    String::from_utf8_lossy(&b[..end]).into_owned()
}

/// Encodes a device-mapper command for device `name`.
fn encode(name: &str, flags: u32, targets: &[Target]) -> io::Result<Vec<u8>> {
    if name.len() >= DM_NAME_LEN || name.contains('\0') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("device name {name:?}"),
        ));
    }
    let mut b = vec![0u8; DM_IOCTL_LEN];
    for (i, t) in targets.iter().enumerate() {
        let mut p = t.params.as_bytes().to_vec();
        p.push(0);
        while !(DM_SPEC_LEN + p.len()).is_multiple_of(8) {
            p.push(0);
        }
        let mut spec = [0u8; DM_SPEC_LEN];
        spec[0..8].copy_from_slice(&t.start.to_le_bytes());
        spec[8..16].copy_from_slice(&t.sectors.to_le_bytes());
        if i + 1 < targets.len() {
            spec[20..24].copy_from_slice(&len32(DM_SPEC_LEN + p.len())?.to_le_bytes());
        }
        let kind = t.kind.as_bytes();
        spec[24..24 + kind.len().min(15)].copy_from_slice(&kind[..kind.len().min(15)]);
        b.extend_from_slice(&spec);
        b.extend_from_slice(&p);
    }
    b.resize(DM_IOCTL_LEN + DM_DATA_LEN.max(b.len() - DM_IOCTL_LEN), 0);
    let len = len32(b.len())?;
    b[0..4].copy_from_slice(&DM_VERSION_MAJOR.to_le_bytes());
    b[12..16].copy_from_slice(&len.to_le_bytes());
    b[16..20].copy_from_slice(&DM_IOCTL_SIZE.to_le_bytes());
    b[20..24].copy_from_slice(&len32(targets.len())?.to_le_bytes());
    b[28..32].copy_from_slice(&flags.to_le_bytes());
    b[48..48 + name.len()].copy_from_slice(name.as_bytes());
    Ok(b)
}

fn decode(b: &[u8]) -> io::Result<Reply> {
    Ok(Reply {
        targets: le32(b, 20)?,
        open_count: le32(b, 24)?.cast_signed(),
        flags: le32(b, 28)?,
        dev: le64(b, 40)?,
    })
}

/// Decodes the targets of a table status reply.
fn decode_targets(b: &[u8], r: &Reply) -> io::Result<Vec<Target>> {
    let start = le32(b, 16)? as usize;
    let data = b.get(start..).unwrap_or_default();
    let mut out = Vec::new();
    let mut off = 0;
    for _ in 0..r.targets {
        let spec = data
            .get(off..off + DM_SPEC_LEN)
            .ok_or_else(|| io::Error::other("short table status"))?;
        out.push(Target {
            start: le64(spec, 0)?,
            sectors: le64(spec, 8)?,
            kind: cstr(&spec[24..40]),
            params: cstr(&data[off + DM_SPEC_LEN..]),
        });
        off = le32(spec, 20)? as usize;
    }
    Ok(out)
}

/// Sends one device-mapper command and returns the reply buffer.
fn dm_call(nr: u32, name: &str, flags: u32, targets: &[Target]) -> io::Result<Vec<u8>> {
    let mut b = encode(name, flags, targets)?;
    let ctl = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/mapper/control")?;
    sys::ioctl_buf(ctl.as_raw_fd(), dm_op(nr), &mut b)?;
    Ok(b)
}

fn dm(nr: u32, name: &str, flags: u32, targets: &[Target]) -> io::Result<Reply> {
    dm_call(nr, name, flags, targets).and_then(|b| decode(&b))
}

/// The targets of the table device `name` serves, not of a loaded inactive one.
fn live_table(name: &str) -> io::Result<Vec<Target>> {
    let b = dm_call(DM_TABLE_STATUS, name, DM_STATUS_TABLE_FLAG, &[])?;
    let r = decode(&b)?;
    if r.flags & DM_BUFFER_FULL_FLAG != 0 {
        return Err(io::Error::other(format!(
            "the table of {name} does not fit the buffer"
        )));
    }
    decode_targets(&b, &r)
}

/// `major:minor` of the device `name` in sysfs class `class`; the kernel adds it with the device.
fn sys_dev(class: &str, name: &str) -> io::Result<String> {
    let p = Path::new("/sys/class").join(class).join(name).join("dev");
    let t0 = Instant::now();
    loop {
        match fs::read_to_string(&p) {
            Ok(s) => return Ok(s.trim().to_owned()),
            Err(e) if t0.elapsed() > Duration::from_secs(2) => return Err(e),
            Err(_) => std::thread::sleep(Duration::from_millis(10)),
        }
    }
}

fn file_name(p: &Path) -> io::Result<&str> {
    p.file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("device {}", p.display())))
}

pub(crate) fn rdev_text(rdev: u64) -> String {
    format!("{}:{}", libc::major(rdev), libc::minor(rdev))
}

/// Opens the device node at `path` once this process may open it. A device allowlist (a
/// device cgroup) can admit a new device later than the kernel makes its node; until then
/// the node is missing or every open fails with `EPERM`. `devt` is the device it must be.
pub(crate) fn open_usable(path: &Path, write: bool, devt: Option<&str>) -> io::Result<File> {
    let t0 = Instant::now();
    loop {
        match OpenOptions::new()
            .read(true)
            .write(write)
            .custom_flags(libc::O_CLOEXEC)
            .open(path)
        {
            Ok(f) => {
                let got = rdev_text(f.metadata()?.rdev());
                if let Some(want) = devt.filter(|w| *w != got) {
                    return Err(io::Error::other(format!(
                        "{} is device {got}, not {want}",
                        path.display()
                    )));
                }
                return Ok(f);
            }
            Err(e) if !matches!(e.raw_os_error(), Some(libc::EPERM | libc::ENOENT)) => return Err(e),
            Err(e) if t0.elapsed() > USABLE_WAIT => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("{} not usable after {:?}: {e}", path.display(), t0.elapsed()),
                ));
            }
            Err(_) => std::thread::sleep(Duration::from_millis(5)),
        }
    }
}

/// `major:minor` of the block device at `path`, once this process may open it.
fn dev_number(path: &Path) -> io::Result<String> {
    let n = sys_dev("block", file_name(path)?)?;
    open_usable(path, false, Some(&n))?;
    Ok(n)
}

fn linear(target: &Path, size: u64) -> io::Result<Vec<Target>> {
    Ok(vec![Target {
        start: 0,
        sectors: size >> 9,
        kind: "linear".into(),
        params: format!("{} 0", dev_number(target)?),
    }])
}

pub(crate) fn mapper_node(name: &str) -> PathBuf {
    Path::new("/dev/mapper").join(name)
}

/// The block device `/dev/<name>` of `major:minor` `devt`.
fn block_path(devt: &str) -> io::Result<Option<PathBuf>> {
    let link = match fs::read_link(Path::new("/sys/dev/block").join(devt)) {
        Ok(l) => l,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    Ok(link.file_name().map(|n| Path::new("/dev").join(n)))
}

fn create(name: &str, target: &Path, size: u64) -> io::Result<()> {
    let table = linear(target, size)?;
    let r = dm(DM_DEV_CREATE, name, 0, &[]).map_err(|e| ctx(&e, "create"))?;
    let undo = |e: io::Error, what: &str| {
        let _ = dm(DM_DEV_REMOVE, name, 0, &[]);
        ctx(&e, what)
    };
    dm(DM_TABLE_LOAD, name, 0, &table).map_err(|e| undo(e, "load"))?;
    dm(DM_DEV_SUSPEND, name, 0, &[]).map_err(|e| undo(e, "resume"))?;
    let node = mapper_node(name);
    let _ = fs::remove_file(&node);
    sys::mknod_block(&node, r.dev).map_err(|e| undo(e, "mknod"))?;
    open_usable(&node, false, None).map_err(|e| undo(e, "open"))?;
    Ok(())
}

fn ctx(e: &io::Error, what: &str) -> io::Error {
    io::Error::new(e.kind(), format!("{what}: {e}"))
}

/// Removes device `name`, retrying while a snapshot holds it suspended.
fn remove(name: &str) -> io::Result<()> {
    let mut last = None;
    for _ in 0..300 {
        match dm(DM_DEV_REMOVE, name, 0, &[]) {
            Ok(_) => {
                let _ = fs::remove_file(mapper_node(name));
                return Ok(());
            }
            Err(e) if e.raw_os_error() == Some(libc::ENXIO) => {
                let _ = fs::remove_file(mapper_node(name));
                return Ok(());
            }
            Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {
                last = Some(e);
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => return Err(e),
        }
    }
    let e = last.unwrap_or_else(|| io::Error::other("busy"));
    match dm(DM_DEV_STATUS, name, 0, &[]) {
        Ok(st) if st.open_count > 0 => Err(io::Error::new(
            e.kind(),
            format!(
                "{e}: still open {} times, by a mount (in any mount namespace) or an open file",
                st.open_count
            ),
        )),
        _ => Err(e),
    }
}

fn users(name: &str) -> io::Result<Vec<String>> {
    let st = match dm(DM_DEV_STATUS, name, 0, &[]) {
        Ok(st) => st,
        Err(e) if e.raw_os_error() == Some(libc::ENXIO) => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let (major, minor) = sys::split_dev(st.dev);
    let (mut found, _) = crate::users::mounts_and_holders(&format!("{major}:{minor}"), None)?;
    if found.is_empty() && st.open_count > 0 {
        found.push(format!(
            "open {} times, by a mount in another mount namespace or a program",
            st.open_count
        ));
    }
    Ok(found)
}

fn live_target(name: &str) -> io::Result<Option<PathBuf>> {
    let table = match live_table(name) {
        Ok(t) => t,
        Err(e) if e.raw_os_error() == Some(libc::ENXIO) => return Ok(None),
        Err(e) => return Err(e),
    };
    let [t] = table.as_slice() else {
        return Ok(None);
    };
    if t.kind != "linear" {
        return Ok(None);
    }
    let devt = t.params.split(' ').next().unwrap_or_default();
    block_path(devt)
}

/// Binds a free loop device to `image` with direct I/O, so the image is not cached twice. The
/// loop device frees itself when its last user lets go of it; the returned file is a user
/// until the table that maps it is loaded.
fn attach_loop(image: &Path) -> io::Result<(PathBuf, OwnedFd)> {
    let ctl = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/loop-control")?;
    let n = sys::ioctl_int(ctl.as_raw_fd(), LOOP_CTL_GET_FREE)?;
    drop(ctl);
    let dev = PathBuf::from(format!("/dev/loop{n}"));
    let devt = sys_dev("block", file_name(&dev)?)?;
    let lf = open_usable(&dev, true, Some(&devt))?;
    let img = OpenOptions::new().read(true).write(true).open(image)?;
    let mut cfg = [0u8; LOOP_CONFIG_LEN];
    cfg[0..4].copy_from_slice(
        &u32::try_from(img.as_raw_fd())
            .map_err(io::Error::other)?
            .to_le_bytes(),
    );
    cfg[4..8].copy_from_slice(&4096u32.to_le_bytes());
    let flags_at = 8 + 52;
    cfg[flags_at..flags_at + 4].copy_from_slice(&(LO_FLAGS_DIRECT_IO | LO_FLAGS_AUTOCLEAR).to_le_bytes());
    if let Err(direct) = sys::ioctl_buf(lf.as_raw_fd(), LOOP_CONFIGURE, &mut cfg) {
        cfg[flags_at..flags_at + 4].copy_from_slice(&LO_FLAGS_AUTOCLEAR.to_le_bytes());
        sys::ioctl_buf(lf.as_raw_fd(), LOOP_CONFIGURE, &mut cfg).map_err(|buffered| {
            io::Error::other(format!(
                "configure {}: {direct}, and without direct I/O: {buffered}",
                dev.display()
            ))
        })?;
    }
    Ok((dev, lf.into()))
}

/// Sets the size of loop device `device` to the current length of its image.
fn refresh_loop(device: &Path) -> io::Result<()> {
    let devt = sys_dev("block", file_name(device)?)?;
    let lf = open_usable(device, true, Some(&devt))?;
    sys::ioctl_int(lf.as_raw_fd(), LOOP_SET_CAPACITY).map(drop)
}

fn loop_image(device: &Path) -> io::Result<Option<PathBuf>> {
    let name = file_name(device)?;
    match fs::read_to_string(Path::new("/sys/block").join(name).join("loop/backing_file")) {
        Ok(s) => Ok(Some(PathBuf::from(s.trim_end_matches('\n')))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// The bytes written to the device, from the sectors-written field of its `stat`.
fn bytes_written(device: &Path) -> io::Result<u64> {
    let s = fs::read_to_string(Path::new("/sys/block").join(file_name(device)?).join("stat"))?;
    parse_written(&s).ok_or_else(|| io::Error::other(format!("stat of {}: {s:?}", device.display())))
}

fn parse_written(stat: &str) -> Option<u64> {
    stat.split_whitespace()
        .nth(6)?
        .parse::<u64>()
        .ok()
        .map(|n| n << 9)
}

pub(crate) fn is_block(p: &Path) -> bool {
    fs::metadata(p).is_ok_and(|m| m.file_type().is_block_device())
}

/// The host's device-mapper devices and loop devices.
#[derive(Debug, Default, Clone)]
pub struct DmSwitch {
    /// Loop devices bound by [`DeviceSwitch::attach_loop`] whose table is not loaded yet.
    loops: Arc<Mutex<HashMap<PathBuf, OwnedFd>>>,
}

impl DmSwitch {
    /// A switch with no loop devices bound yet.
    pub fn new() -> DmSwitch {
        DmSwitch::default()
    }

    /// What removing device `name` would cut off: each mount of it, each device stacked on
    /// it, and, when neither explains its open count, its other openers. Empty when `name`
    /// does not exist.
    pub async fn users(&self, name: &str) -> io::Result<Vec<String>> {
        let name = name.to_owned();
        blocking(move || users(&name)).await
    }
}

pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    tokio::task::spawn_blocking(f).await.map_err(io::Error::other)?
}

#[async_trait]
impl DeviceSwitch for DmSwitch {
    async fn create(&self, name: &str, target: &Path, size: u64) -> io::Result<()> {
        let (name, target) = (name.to_owned(), target.to_owned());
        blocking(move || create(&name, &target, size)).await
    }

    async fn remove(&self, name: &str) -> io::Result<()> {
        let name = name.to_owned();
        blocking(move || remove(&name)).await
    }

    async fn live_target(&self, name: &str) -> io::Result<Option<PathBuf>> {
        let name = name.to_owned();
        blocking(move || live_target(&name)).await
    }

    async fn load(&self, name: &str, target: &Path, size: u64) -> io::Result<()> {
        let (name, target) = (name.to_owned(), target.to_owned());
        let loops = Arc::clone(&self.loops);
        blocking(move || {
            let done = linear(&target, size).and_then(|t| dm(DM_TABLE_LOAD, &name, 0, &t));
            loops.lock().remove(&target);
            done.map(drop)
        })
        .await
    }

    async fn clear_inactive(&self, name: &str) -> io::Result<()> {
        let name = name.to_owned();
        blocking(move || dm(DM_TABLE_CLEAR, &name, 0, &[]).map(drop)).await
    }

    async fn suspend(&self, name: &str) -> io::Result<()> {
        let name = name.to_owned();
        blocking(move || dm(DM_DEV_SUSPEND, &name, SUSPEND_FLAGS, &[]).map(drop)).await
    }

    async fn resume(&self, name: &str) -> io::Result<()> {
        let name = name.to_owned();
        blocking(move || dm(DM_DEV_SUSPEND, &name, 0, &[]).map(drop)).await
    }

    async fn attach_loop(&self, image: &Path) -> io::Result<PathBuf> {
        let image = image.to_owned();
        let (dev, fd) = blocking(move || attach_loop(&image)).await?;
        self.loops.lock().insert(dev.clone(), fd);
        Ok(dev)
    }

    async fn refresh_loop(&self, device: &Path) -> io::Result<()> {
        let device = device.to_owned();
        blocking(move || refresh_loop(&device)).await
    }

    async fn loop_image(&self, device: &Path) -> io::Result<Option<PathBuf>> {
        let device = device.to_owned();
        blocking(move || loop_image(&device)).await
    }

    async fn bytes_written(&self, device: &Path) -> io::Result<u64> {
        let device = device.to_owned();
        blocking(move || bytes_written(&device)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_table_encodes_and_decodes() {
        let t = vec![
            Target {
                start: 0,
                sectors: 2048,
                kind: "linear".into(),
                params: "7:3 0".into(),
            },
            Target {
                start: 2048,
                sectors: 8,
                kind: "zero".into(),
                params: String::new(),
            },
        ];
        let b = encode("pando-main", SUSPEND_FLAGS, &t).unwrap();
        assert_eq!(b.len(), DM_IOCTL_LEN + DM_DATA_LEN);
        let r = decode(&b).unwrap();
        assert_eq!((r.flags, r.targets), (SUSPEND_FLAGS, 2));
        assert_eq!(cstr(&b[48..176]), "pando-main");
        assert_eq!(decode_targets(&b, &r).unwrap(), t);
    }

    #[test]
    fn ioctl_numbers_match_the_kernel() {
        assert_eq!(dm_op(DM_TABLE_CLEAR), 0xc138_fd0a);
        assert_eq!(dm_op(DM_DEV_SUSPEND), 0xc138_fd06);
    }

    #[test]
    fn written_bytes_come_from_the_sectors_field() {
        let stat = "  120 0 9000 30   44 3 2048 70 0 50 100 0 0 0 0";
        assert_eq!(parse_written(stat), Some(2048 << 9));
        assert_eq!(parse_written("1 2"), None);
    }

    #[test]
    fn a_long_name_is_refused() {
        assert!(encode(&"x".repeat(128), 0, &[]).is_err());
    }
}
