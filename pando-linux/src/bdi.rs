//! The page cache's writeback limits of a block device.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// The sysfs directory of the writeback limits of block device `dev`, a path that may go
/// through a symlink such as `/dev/mapper/NAME`.
fn bdi_dir(dev: &Path) -> io::Result<PathBuf> {
    let real = fs::canonicalize(dev)?;
    let name = real.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} names no block device", dev.display()),
        )
    })?;
    Ok(Path::new("/sys/class/block").join(name).join("bdi"))
}

pub(crate) fn limit_dirty(dev: &Path, bytes: u64) -> io::Result<()> {
    let dir = bdi_dir(dev)?;
    fs::write(dir.join("max_bytes"), bytes.to_string())?;
    fs::write(dir.join("strict_limit"), "1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_device_s_limits_are_under_its_block_class_name() {
        let dir = std::env::temp_dir().join(format!("pando-bdi-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let dev = dir.join("dm-7");
        fs::write(&dev, b"").unwrap();
        let link = dir.join("branch");
        let _ = fs::remove_file(&link);
        std::os::unix::fs::symlink(&dev, &link).unwrap();
        assert_eq!(bdi_dir(&link).unwrap(), Path::new("/sys/class/block/dm-7/bdi"));
        fs::remove_dir_all(&dir).unwrap();
    }
}
