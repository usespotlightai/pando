//! The page cache's writeback limits of a block device.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const SYS_BLOCK: &str = "/sys/class/block";

/// The sysfs directory, under `sys_block`, of the writeback limits of block device `dev`, a
/// path that may go through a symlink such as `/dev/mapper/NAME`. A device the kernel has added
/// but whose node is not in `/dev` yet is found by its own name.
fn bdi_dir(sys_block: &Path, dev: &Path) -> io::Result<PathBuf> {
    let real = match fs::canonicalize(dev) {
        Ok(p) => p,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let named = dev.file_name().map(|n| sys_block.join(n));
            if named.as_deref().is_some_and(Path::exists) {
                dev.to_path_buf()
            } else {
                return Err(e);
            }
        }
        Err(e) => return Err(e),
    };
    let name = real.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} names no block device", dev.display()),
        )
    })?;
    Ok(sys_block.join(name).join("bdi"))
}

fn write(path: &Path, value: &str) -> io::Result<()> {
    fs::write(path, value).map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))
}

fn limit_dirty_in(sys_block: &Path, dev: &Path, bytes: u64) -> io::Result<()> {
    let dir = bdi_dir(sys_block, dev)?;
    write(&dir.join("strict_limit"), "1")?;
    write(&dir.join("max_bytes"), &bytes.to_string())
}

pub(crate) fn limit_dirty(dev: &Path, bytes: u64) -> io::Result<()> {
    limit_dirty_in(Path::new(SYS_BLOCK), dev, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        dir: PathBuf,
        sys_block: PathBuf,
        link: PathBuf,
    }

    impl Fixture {
        fn new(tag: &str) -> Fixture {
            let dir = std::env::temp_dir().join(format!("pando-bdi-{tag}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            let sys_block = dir.join("sys");
            fs::create_dir_all(sys_block.join("dm-7").join("bdi")).unwrap();
            let dev = dir.join("dm-7");
            fs::write(&dev, b"").unwrap();
            let link = dir.join("branch");
            std::os::unix::fs::symlink(&dev, &link).unwrap();
            Fixture { dir, sys_block, link }
        }

        fn read(&self, file: &str) -> String {
            fs::read_to_string(self.sys_block.join("dm-7/bdi").join(file)).unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn a_device_s_limits_are_under_its_block_class_name() {
        let f = Fixture::new("dir");
        assert_eq!(
            bdi_dir(Path::new(SYS_BLOCK), &f.link).unwrap(),
            Path::new("/sys/class/block/dm-7/bdi")
        );
    }

    #[test]
    fn a_limit_turns_on_the_strict_limit_and_sets_the_bytes() {
        let f = Fixture::new("set");
        limit_dirty_in(&f.sys_block, &f.link, 64 << 20).unwrap();
        assert_eq!(f.read("strict_limit"), "1");
        assert_eq!(f.read("max_bytes"), (64u64 << 20).to_string());
    }

    #[test]
    fn a_device_without_its_node_yet_is_found_by_its_name() {
        let f = Fixture::new("node");
        let dev = f.dir.join("dev").join("dm-7");
        limit_dirty_in(&f.sys_block, &dev, 32 << 20).unwrap();
        assert_eq!(f.read("max_bytes"), (32u64 << 20).to_string());
        let e = limit_dirty_in(&f.sys_block, &f.dir.join("dev").join("dm-8"), 1).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn a_failed_write_names_its_file() {
        let f = Fixture::new("fail");
        fs::create_dir(f.sys_block.join("dm-7/bdi/max_bytes")).unwrap();
        let e = limit_dirty_in(&f.sys_block, &f.link, 1 << 20).unwrap_err();
        assert!(e.to_string().contains("max_bytes"), "{e}");
        assert_eq!(f.read("strict_limit"), "1");
    }
}
