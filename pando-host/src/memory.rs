//! The memory this process may use.

use std::path::{Path, PathBuf};

/// The memory this process may use: the least of the host's memory and the limits of its
/// memory cgroup and of the cgroups above it, cgroup v2 or v1. `None` when nothing says, as
/// off Linux.
pub fn memory_limit() -> Option<u64> {
    limit_under(Path::new("/proc"), Path::new("/sys/fs/cgroup"))
}

/// [`memory_limit`] with `/proc` at `proc` and the cgroup file systems at `cgroups`.
fn limit_under(proc: &Path, cgroups: &Path) -> Option<u64> {
    let read = |p: &Path| std::fs::read_to_string(p).ok();
    let total = read(&proc.join("meminfo")).as_deref().and_then(mem_total);
    let own = read(&proc.join("self/cgroup")).unwrap_or_default();
    limit_files(&own, cgroups)
        .iter()
        .filter_map(|f| read(f).as_deref().and_then(limit))
        .chain(total)
        .min()
}

/// `MemTotal` of `/proc/meminfo`, in bytes.
fn mem_total(meminfo: &str) -> Option<u64> {
    let line = meminfo.lines().find_map(|l| l.strip_prefix("MemTotal:"))?;
    let kib: u64 = line.trim().strip_suffix("kB")?.trim().parse().ok()?;
    kib.checked_mul(1024)
}

/// A limit file's bytes; `None` for `max`, no limit.
fn limit(s: &str) -> Option<u64> {
    s.trim().parse().ok()
}

/// The files that hold a memory limit of the cgroups in `own`, a `/proc/self/cgroup`, and of
/// every cgroup above them, under `cgroups`.
fn limit_files(own: &str, cgroups: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for line in own.lines() {
        let mut parts = line.splitn(3, ':');
        let (Some(id), Some(controllers), Some(path)) = (parts.next(), parts.next(), parts.next()) else {
            continue;
        };
        let (root, names): (PathBuf, &[&str]) = if id == "0" && controllers.is_empty() {
            (cgroups.to_path_buf(), &["memory.max", "memory.high"])
        } else if controllers.split(',').any(|c| c == "memory") {
            (cgroups.join("memory"), &["memory.limit_in_bytes"])
        } else {
            continue;
        };
        let mut at = Some(Path::new(path.trim()));
        while let Some(p) = at {
            let dir = root.join(p.strip_prefix("/").unwrap_or(p));
            files.extend(names.iter().map(|n| dir.join(n)));
            at = p.parent();
        }
    }
    files
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(root: &Path, rel: &str, s: &str) {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, s).unwrap();
    }

    #[test]
    fn the_least_of_the_host_and_every_cgroup_above() {
        let t = tempfile::tempdir().unwrap();
        let (proc, cg) = (t.path().join("proc"), t.path().join("cg"));
        put(&proc, "meminfo", "MemTotal:        1048576 kB\nMemFree: 12 kB\n");
        assert_eq!(
            limit_under(&proc, &cg),
            Some(1 << 30),
            "the host without a cgroup"
        );

        put(&proc, "self/cgroup", "0::/a/b\n");
        put(&cg, "a/b/memory.max", "max\n");
        put(&cg, "a/b/memory.high", "max\n");
        assert_eq!(limit_under(&proc, &cg), Some(1 << 30), "cgroups without limits");

        put(&cg, "a/memory.max", "402653184\n");
        assert_eq!(
            limit_under(&proc, &cg),
            Some(384 << 20),
            "a limit above its own cgroup"
        );
        put(&cg, "a/b/memory.high", "268435456\n");
        assert_eq!(limit_under(&proc, &cg), Some(256 << 20), "its own high mark");
    }

    #[test]
    fn a_cgroup_v1_memory_limit() {
        let t = tempfile::tempdir().unwrap();
        let (proc, cg) = (t.path().join("proc"), t.path().join("cg"));
        put(&proc, "self/cgroup", "5:cpu,cpuacct:/x\n4:memory:/x\n");
        put(&cg, "memory/x/memory.limit_in_bytes", "134217728\n");
        put(&cg, "memory/memory.limit_in_bytes", "9223372036854771712\n");
        assert_eq!(limit_under(&proc, &cg), Some(128 << 20));
    }

    #[test]
    fn nothing_says() {
        let t = tempfile::tempdir().unwrap();
        assert_eq!(limit_under(&t.path().join("proc"), &t.path().join("cg")), None);
    }
}
