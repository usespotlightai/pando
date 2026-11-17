//! What uses a block device: its mounts, the devices stacked on it and its other openers.

use std::fs::{self, OpenOptions};
use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::Path;

use crate::dm::rdev_text;

/// What a stop of block device `dev` would cut off: each mount of it, each device stacked on
/// it other than the device-mapper device named `over`, and, when neither explains it, an
/// exclusive opener such as a mount in another mount namespace. Empty when `dev` does not
/// exist.
pub(crate) fn users(dev: &Path, over: Option<&str>) -> io::Result<Vec<String>> {
    let meta = match fs::metadata(dev) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    if !meta.file_type().is_block_device() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is not a block device", dev.display()),
        ));
    }
    let (mut found, under_over) = mounts_and_holders(&rdev_text(meta.rdev()), over)?;
    if found.is_empty() && !under_over && open_exclusively(dev)? {
        found.push("open exclusively, by a mount in another mount namespace or a program".into());
    }
    Ok(found)
}

/// The mounts of device `devt` (`major:minor`) and the devices stacked on it, except the
/// device-mapper device named `over`, and whether `over` is stacked on it.
pub(crate) fn mounts_and_holders(devt: &str, over: Option<&str>) -> io::Result<(Vec<String>, bool)> {
    let mut found: Vec<String> = mount_points(&fs::read_to_string("/proc/self/mountinfo")?, devt)
        .into_iter()
        .map(|p| format!("mounted at {p}"))
        .collect();
    let mut under_over = false;
    for h in holders(&Path::new("/sys/dev/block").join(devt).join("holders"))? {
        let dm_name = fs::read_to_string(Path::new("/sys/class/block").join(&h).join("dm/name"))
            .ok()
            .map(|s| s.trim().to_owned());
        match dm_name {
            Some(n) if Some(n.as_str()) == over => under_over = true,
            Some(n) => found.push(format!("under /dev/mapper/{n}")),
            None => found.push(format!("under /dev/{h}")),
        }
    }
    Ok((found, under_over))
}

/// The mount points of device `devt` in `mountinfo`, the text of `/proc/<pid>/mountinfo`.
fn mount_points(mountinfo: &str, devt: &str) -> Vec<String> {
    mountinfo
        .lines()
        .filter_map(|l| {
            let mut f = l.split(' ');
            let dev = f.nth(2)?;
            let point = f.nth(1)?;
            (dev == devt).then(|| unescape(point))
        })
        .collect()
}

/// Undoes the octal escapes (`\040`) mountinfo writes for spaces, tabs, newlines and
/// backslashes.
fn unescape(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let octal = b
            .get(i + 1..i + 4)
            .filter(|d| b[i] == b'\\' && d.iter().all(|c| (b'0'..=b'7').contains(c)));
        match octal {
            Some(d) => {
                out.push(d.iter().fold(0u8, |n, c| n.wrapping_mul(8) + (c - b'0')));
                i += 4;
            }
            None => {
                out.push(b[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The kernel names of the devices in a sysfs `holders` directory, sorted.
fn holders(dir: &Path) -> io::Result<Vec<String>> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut names = entries
        .map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<io::Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

/// Whether something holds block device `dev` exclusively, as a mount or a stacked device does.
fn open_exclusively(dev: &Path) -> io::Result<bool> {
    match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_EXCL | libc::O_CLOEXEC)
        .open(dev)
    {
        Ok(_) => Ok(false),
        Err(e) if e.raw_os_error() == Some(libc::EBUSY) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MOUNTINFO: &str = "\
22 1 259:2 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p2 rw
36 22 259:1 / /mnt/ublk\\040one rw,relatime shared:9 - ext4 /dev/ublkb1 rw
37 22 259:1 /work /srv/bind rw,relatime shared:9 - ext4 /dev/ublkb1 rw
38 22 259:10 / /mnt/other rw,relatime - ext4 /dev/ublkb10 rw
";

    #[test]
    fn mounts_match_the_device_number_exactly() {
        assert_eq!(mount_points(MOUNTINFO, "259:1"), ["/mnt/ublk one", "/srv/bind"]);
        assert_eq!(mount_points(MOUNTINFO, "259:10"), ["/mnt/other"]);
        assert!(mount_points(MOUNTINFO, "259:3").is_empty());
        assert!(mount_points("short line\n", "259:1").is_empty());
    }

    #[test]
    fn mountinfo_escapes_are_undone() {
        assert_eq!(unescape(r"/a\040b\011c\134d\012"), "/a b\tc\\d\n");
        assert_eq!(unescape(r"/plain\0"), r"/plain\0");
        assert_eq!(unescape(r"\9999"), r"\9999");
    }

    #[test]
    fn holders_are_the_entries_of_the_directory() {
        let d = tempfile::tempdir().unwrap();
        assert!(holders(&d.path().join("missing")).unwrap().is_empty());
        fs::write(d.path().join("dm-3"), "").unwrap();
        fs::write(d.path().join("dm-1"), "").unwrap();
        assert_eq!(holders(d.path()).unwrap(), ["dm-1", "dm-3"]);
    }

    #[test]
    fn a_missing_device_has_no_users() {
        assert!(users(Path::new("/dev/ublkb987654"), None).unwrap().is_empty());
    }

    #[test]
    fn only_block_devices_have_users() {
        let e = users(Path::new("/dev/null"), None).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
    }
}
