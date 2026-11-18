"""Probe of an AgentCore Runtime V2 microVM: what the kernel and the platform let Pando use.

Serves the AgentCore HTTP contract on 0.0.0.0:8080. GET /ping answers {"status": "Healthy"}.
POST /invocations runs the checks the payload names ({"checks": [...]}, all by default) and
answers one JSON object with a result per check. Every result, each signal the process gets
and a heartbeat also go to stdout as JSON lines, which AgentCore sends to CloudWatch Logs.

AgentCore snapshots this process at its first healthy /ping and restores the snapshot for each
session, so START holds values taken before the snapshot; the clocks check compares them with
values taken in the session.
"""

import contextlib
import ctypes
import fcntl
import gzip
import hashlib
import json
import os
import random
import re
import select
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.request
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PORT = 8080
METADATA = os.environ.get("PROBE_METADATA_ENDPOINT", "http://169.254.169.254")
WORK = Path(os.environ.get("PROBE_WORK", "/tmp/probe"))  # noqa: S108
PANDO = os.environ.get("PROBE_PANDO", "/usr/local/bin/pando")
HEARTBEAT_SECONDS = 10.0
TERM_HEARTBEAT_SECONDS = 0.2
CLOCK_JUMP_SECONDS = 2.0
COMMAND_TIMEOUT = 300
PR_SET_IO_FLUSHER = 57
PR_GET_IO_FLUSHER = 58
MNT_DETACH = 2
UBLK_DEVICE = "/dev/ublkb0"
CONFIG_OPTIONS = (
    "CONFIG_BLK_DEV_UBLK",
    "CONFIG_BLK_DEV_NBD",
    "CONFIG_BLK_DEV_LOOP",
    "CONFIG_BLK_DEV_DM",
    "CONFIG_DM_ERA",
    "CONFIG_FUSE_FS",
    "CONFIG_FUSE_PASSTHROUGH",
    "CONFIG_CUSE",
    "CONFIG_VIRTIO_FS",
    "CONFIG_NFS_FS",
    "CONFIG_NFS_V3",
    "CONFIG_NFS_V4",
    "CONFIG_NFSD",
    "CONFIG_IO_URING",
    "CONFIG_EXT4_FS",
    "CONFIG_MODULES",
)

START = {
    "wall": time.time(),
    "monotonic": time.monotonic(),
    "boottime": time.clock_gettime(time.CLOCK_BOOTTIME),
    "pid": os.getpid(),
    "python_random": random.getrandbits(64),
    "urandom": os.urandom(8).hex(),
}

state = {"term_at": None, "signals": [], "pinged": False}
state_lock = threading.Lock()


def log(event: str, **fields: object) -> None:
    """Print one JSON line on stdout with the wall and monotonic clocks."""
    line = {"t": time.time(), "mono": round(time.monotonic(), 3), "event": event, **fields}
    print(json.dumps(line, default=str), flush=True)


def run(*args: object, timeout: float = COMMAND_TIMEOUT, stdin: str | None = None) -> dict:
    """Run a command and return its exit status, output and duration; never raises."""
    t0 = time.monotonic()
    try:
        p = subprocess.run(
            [str(a) for a in args],
            capture_output=True,
            text=True,
            timeout=timeout,
            input=stdin,
            check=False,
        )
    except FileNotFoundError as e:
        return {"cmd": " ".join(map(str, args)), "rc": None, "error": str(e)}
    except subprocess.TimeoutExpired:
        return {"cmd": " ".join(map(str, args)), "rc": None, "error": f"timed out after {timeout} s"}
    return {
        "cmd": " ".join(map(str, args)),
        "rc": p.returncode,
        "out": p.stdout[-4000:],
        "err": p.stderr[-4000:],
        "seconds": round(time.monotonic() - t0, 3),
    }


def ok(r: dict) -> bool:
    """Return whether a run() result exited 0."""
    return r.get("rc") == 0


def read(path: str) -> str | None:
    """Return the text of a file, or None when it cannot be read."""
    try:
        return Path(path).read_text()
    except OSError:
        return None


def check_kernel() -> dict:
    """Report the kernel, the distribution and the CPU."""
    cpu = read("/proc/cpuinfo") or ""
    return {
        "uname": dict(zip(("sysname", "nodename", "release", "version", "machine"), os.uname(), strict=True)),
        "version": read("/proc/version"),
        "cmdline": read("/proc/cmdline"),
        "os_release": read("/etc/os-release"),
        "cpus": os.cpu_count(),
        "cpu_features": sorted(
            {line.split(":", 1)[1].strip() for line in cpu.splitlines() if line.startswith("Features")}
        ),
        "cpu_parts": sorted(
            {line.split(":", 1)[1].strip() for line in cpu.splitlines() if line.startswith("CPU part")}
        ),
        "filesystems": read("/proc/filesystems"),
        "misc": read("/proc/misc"),
        "devices": read("/proc/devices"),
        "modules": read("/proc/modules"),
        "config": kernel_config(),
        "io_uring_disabled": read("/proc/sys/kernel/io_uring_disabled"),
        "glibc": run("ldd", "--version").get("out", "").splitlines()[:1],
    }


def check_dev() -> dict:
    """Report the device nodes Pando's frontends need."""
    dev = Path("/dev")
    loops = sorted(p.name for p in dev.glob("loop[0-9]*"))
    return {
        "ls": run("ls", "-l", "/dev").get("out"),
        "loop_control": (dev / "loop-control").exists(),
        "loop_devices": len(loops),
        "loop_names": loops[:16],
        "mapper_control": (dev / "mapper" / "control").exists(),
        "fuse": (dev / "fuse").exists(),
        "ublk_control": (dev / "ublk-control").exists(),
        "sys_block_loops": sorted(p.name for p in Path("/sys/block").glob("loop*"))[:16],
    }


def check_caps() -> dict:
    """Report the capabilities and other limits of this process."""
    status = read("/proc/self/status") or ""
    caps = {
        k: v.strip()
        for k, _, v in (line.partition(":") for line in status.splitlines())
        if k.startswith("Cap")
    }
    decoded = {k: run("capsh", f"--decode={v}").get("out", "").strip() for k, v in caps.items()}
    libc = ctypes.CDLL(None, use_errno=True)
    set_rc = libc.prctl(PR_SET_IO_FLUSHER, 1, 0, 0, 0)
    set_errno = ctypes.get_errno()
    get_rc = libc.prctl(PR_GET_IO_FLUSHER, 0, 0, 0, 0)
    libc.prctl(PR_SET_IO_FLUSHER, 0, 0, 0, 0)
    return {
        "status_caps": caps,
        "decoded": decoded,
        "capsh": run("capsh", "--print").get("out"),
        "uid": os.getuid(),
        "seccomp": next((line for line in status.splitlines() if line.startswith("Seccomp")), None),
        "no_new_privs": next((line for line in status.splitlines() if line.startswith("NoNewPrivs")), None),
        "io_flusher": {"set_rc": set_rc, "errno": os.strerror(set_errno) if set_rc else None, "get": get_rc},
        "rlimit_memlock": run("sh", "-c", "ulimit -l").get("out", "").strip(),
        "rlimit_nofile": run("sh", "-c", "ulimit -n").get("out", "").strip(),
    }


def check_dm() -> dict:
    """Whether device-mapper works: its targets, and a linear device over a loop device."""
    r = {
        "control": openable("/dev/mapper/control"),
        "targets": run("dmsetup", "targets"),
        "version": run("dmsetup", "version"),
        "config": {k: v for k, v in kernel_config().get("options", {}).items() if "_DM" in k},
    }
    if not Path("/dev/mapper/control").exists():
        return r
    img = WORK / "dm.img"
    WORK.mkdir(parents=True, exist_ok=True)
    with img.open("wb") as f:
        f.truncate(64 << 20)
    loop = run("losetup", "-f", "--show", img)
    r["loop"] = loop
    if ok(loop):
        dev = loop["out"].strip()
        sectors = (64 << 20) // 512
        r["create"] = run("dmsetup", "create", "probe-linear", "--table", f"0 {sectors} linear {dev} 0")
        r["remove"] = run("dmsetup", "remove", "probe-linear")
        r["era"] = era_on(dev, sectors)
        r["detach"] = run("losetup", "-d", dev)
    img.unlink(missing_ok=True)
    r["has_linear"] = ok(r.get("create", {}))
    r["era_ok"] = ok(r.get("era", {}).get("create", {}))
    return r


def era_on(origin: str, sectors: int) -> dict:
    """Create and remove a dm-era target over origin, with metadata on a loop device of its own."""
    r: dict = {"module": module("dm_era")}
    meta = WORK / "era-meta.img"
    with meta.open("wb") as f:
        f.truncate(8 << 20)
    loop = run("losetup", "-f", "--show", meta)
    r["meta_loop"] = loop
    if ok(loop):
        mdev = loop["out"].strip()
        r["zero_meta"] = run("dd", "if=/dev/zero", f"of={mdev}", "bs=4096", "count=1", "oflag=direct")
        r["create"] = run("dmsetup", "create", "probe-era", "--table", f"0 {sectors} era {mdev} {origin} 128")
        if ok(r["create"]):
            r["status"] = run("dmsetup", "status", "probe-era")
            r["remove"] = run("dmsetup", "remove", "probe-era")
        r["meta_detach"] = run("losetup", "-d", mdev)
    meta.unlink(missing_ok=True)
    return r


def check_fs() -> dict:
    """Report the file systems, their free space and the mounts."""
    return {
        "df": run("df", "-T", "-B1", "/", "/tmp", "/dev/shm").get("out"),  # noqa: S108
        "mounts": read("/proc/self/mounts"),
        "statvfs": {
            p: {"free": s.f_bavail * s.f_frsize, "size": s.f_blocks * s.f_frsize}
            for p in ("/", "/tmp")  # noqa: S108
            for s in [os.statvfs(p)]
        },
    }


def check_memory() -> dict:
    """Report memory, swap and the cgroup limits of this process."""
    cg = (read("/proc/self/cgroup") or "").strip()
    rel = cg.split("::", 1)[1] if "::" in cg else ""
    base = Path("/sys/fs/cgroup") / rel.lstrip("/")
    files = [
        "memory.max",
        "memory.high",
        "memory.swap.max",
        "memory.current",
        "cpu.max",
        "pids.max",
        "io.max",
    ]
    return {
        "meminfo": read("/proc/meminfo"),
        "cgroup": cg,
        "cgroup_files": {f: read(str(base / f)) for f in files},
        "root_cgroup_files": {f: read(f"/sys/fs/cgroup/{f}") for f in files},
        "dirty": {
            k: read(f"/proc/sys/vm/{k}") for k in ("dirty_ratio", "dirty_background_ratio", "dirty_bytes")
        },
        "overcommit": read("/proc/sys/vm/overcommit_memory"),
        "swaps": read("/proc/swaps"),
    }


def http(method: str, path: str, headers: dict[str, str], timeout: float = 3) -> tuple[int | None, str]:
    """Send a request to the metadata service; return the status (None when unreachable) and body."""
    req = urllib.request.Request(METADATA + path, method=method, headers=headers)  # noqa: S310
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")[:200]
    except (urllib.error.URLError, OSError) as e:
        return None, str(e)


def credentials_with(token_ttl_header: str, token_header: str) -> dict:
    """Run the IMDSv2 token flow with the given header names; report the role and the expiry only."""
    r: dict = {"ttl_header": token_ttl_header, "token_header": token_header}
    status, token = http("PUT", "/latest/api/token", {token_ttl_header: "300"})
    r["token_status"] = status
    if status != 200:  # noqa: PLR2004
        r["token_error"] = token[:200]
        return r
    auth = {token_header: token}
    status, roles = http("GET", "/latest/meta-data/iam/security-credentials/", auth)
    r["roles_status"] = status
    if status != 200:  # noqa: PLR2004
        r["roles_error"] = roles[:200]
        return r
    role = roles.strip().splitlines()[0] if roles.strip() else ""
    r["role"] = role
    status, body = http("GET", f"/latest/meta-data/iam/security-credentials/{role}", auth)
    r["credentials_status"] = status
    try:
        creds = json.loads(body)
    except json.JSONDecodeError:
        r["credentials_error"] = "not JSON"
        return r
    r["fields"] = sorted(creds)
    r["code"] = creds.get("Code")
    r["type"] = creds.get("Type")
    r["expiration"] = creds.get("Expiration")
    r["last_updated"] = creds.get("LastUpdated")
    r["has_session_token"] = bool(creds.get("Token"))
    return r


def check_metadata() -> dict:
    """Report credentials from the metadata service: the IMDSv2 flow, IMDSv1 and the listing."""
    r = {
        "endpoint": METADATA,
        "imdsv2_ec2_headers": credentials_with(
            "X-aws-ec2-metadata-token-ttl-seconds", "X-aws-ec2-metadata-token"
        ),
        "imdsv2_mmds_headers": credentials_with("X-metadata-token-ttl-seconds", "X-metadata-token"),
    }
    status, _ = http("GET", "/latest/meta-data/iam/security-credentials/", {})
    r["imdsv1_status"] = status
    r["imdsv1_refused"] = status in {401, 403}
    _, token = http("PUT", "/latest/api/token", {"X-aws-ec2-metadata-token-ttl-seconds": "60"})
    status, listing = http("GET", "/latest/meta-data/", {"X-aws-ec2-metadata-token": token})
    r["listing_status"] = status
    r["listing"] = listing[:1000] if status == 200 else None  # noqa: PLR2004
    status, doc = http(
        "GET", "/latest/dynamic/instance-identity/document", {"X-aws-ec2-metadata-token": token}
    )
    r["identity_document_status"] = status
    if status == 200:  # noqa: PLR2004
        with contextlib.suppress(json.JSONDecodeError):
            d = json.loads(doc)
            r["region"] = d.get("region")
            r["architecture"] = d.get("architecture")
    r["aws_env_names"] = sorted(k for k in os.environ if k.startswith(("AWS_", "AGENTCORE", "BEDROCK")))
    return r


def check_clocks() -> dict:
    """Report the clocks and random sources now, against START, taken before the snapshot."""
    now = {
        "wall": time.time(),
        "monotonic": time.monotonic(),
        "boottime": time.clock_gettime(time.CLOCK_BOOTTIME),
        "pid": os.getpid(),
        "python_random": random.getrandbits(64),
        "urandom": os.urandom(8).hex(),
    }
    return {
        "start": START,
        "now": now,
        "wall_since_start": round(now["wall"] - START["wall"], 3),
        "monotonic_since_start": round(now["monotonic"] - START["monotonic"], 3),
        "boottime_since_start": round(now["boottime"] - START["boottime"], 3),
        "boot_id": (read("/proc/sys/kernel/random/boot_id") or "").strip(),
        "uptime": read("/proc/uptime"),
        "hostname": os.uname().nodename,
        "clocksource": read("/sys/devices/system/clocksource/clocksource0/current_clocksource"),
        "vmgenid": [str(p) for p in Path("/sys/bus/platform/devices").glob("*vmgenid*")]
        + [str(p) for p in Path("/sys/bus/acpi/devices").glob("*VMGEN*")],
        "note": "the same python_random in two sessions means the snapshot duplicated userspace RNG state",
    }


def fill(path: Path, mib: int) -> str:
    """Write mib MiB of random bytes to path, fsync it, and return their SHA-256."""
    h = hashlib.sha256()
    with path.open("wb") as f:
        for _ in range(mib):
            b = os.urandom(1 << 20)
            h.update(b)
            f.write(b)
        f.flush()
        os.fsync(f.fileno())
    return h.hexdigest()


def digest(path: Path) -> str:
    """Return the SHA-256 of a file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while b := f.read(1 << 20):
            h.update(b)
    return h.hexdigest()


def ext4_on(image: Path, mnt: Path, *, direct_io: bool) -> dict:
    """Bind image to a loop device, make ext4 on it, write, trim, check and tear it down."""
    r: dict = {"direct_io": direct_io}
    args = ["losetup", "-f", "--show"] + (["--direct-io=on"] if direct_io else []) + [image]
    loop = run(*args)
    r["losetup"] = loop
    if not ok(loop):
        return r
    dev = loop["out"].strip()
    r["device"] = dev
    r["dio"] = (read(f"/sys/block/{Path(dev).name}/loop/dio") or "").strip()
    r["discard_max_bytes"] = (read(f"/sys/block/{Path(dev).name}/queue/discard_max_bytes") or "").strip()
    try:
        r["mkfs"] = run("mkfs.ext4", "-q", "-F", dev)
        mnt.mkdir(parents=True, exist_ok=True)
        r["mount"] = run("mount", "-o", "noatime", dev, mnt)
        if ok(r["mount"]):
            t0 = time.monotonic()
            want = fill(mnt / "f", 64)
            r["write_64mib_fsync_seconds"] = round(time.monotonic() - t0, 3)
            r["drop_caches"] = run("sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches")
            r["read_back_matches"] = digest(mnt / "f") == want
            (mnt / "f").unlink()
            r["fstrim"] = run("fstrim", "-v", mnt)
            r["umount"] = run("umount", mnt)
        r["fsck"] = run("e2fsck", "-fn", dev)
    finally:
        r["detach"] = run("losetup", "-d", dev)
    r["ok"] = ok(r.get("mkfs", {})) and r.get("read_back_matches") is True and ok(r.get("fsck", {}))
    return r


def check_loop_file() -> dict:
    """ext4 on a loop device over a file on the local disk: the case AWS documents."""
    d = Path(tempfile.mkdtemp())
    img = d / "disk.img"
    with img.open("wb") as f:
        f.truncate(256 << 20)
    try:
        return {"direct": ext4_on(img, d / "mnt", direct_io=True)}
    finally:
        shutil.rmtree(d, ignore_errors=True)


def check_loop_fuse() -> dict:
    """ext4 on a loop device over a file served by FUSE (bindfs), with and without direct I/O."""
    d = Path(tempfile.mkdtemp())
    src, fuse = d / "src", d / "fuse"
    src.mkdir()
    fuse.mkdir()
    r: dict = {"bindfs": run("bindfs", "--version").get("out", "").strip()}
    mount = run("bindfs", "-o", "allow_other", src, fuse)
    r["fuse_mount"] = mount
    if not ok(mount):
        shutil.rmtree(d, ignore_errors=True)
        return r
    try:
        img = fuse / "disk.img"
        with img.open("wb") as f:
            f.truncate(256 << 20)
        r["fs_type"] = run("stat", "-f", "-c", "%T", fuse).get("out", "").strip()
        r["direct"] = ext4_on(img, d / "mnt", direct_io=True)
        r["buffered"] = ext4_on(img, d / "mnt", direct_io=False)
    finally:
        r["fuse_umount"] = run("fusermount3", "-u", fuse)
        shutil.rmtree(d, ignore_errors=True)
    r["ok"] = r["direct"].get("ok") or r["buffered"].get("ok")
    return r


def wait_for(pattern: str, path: Path, seconds: float) -> re.Match | None:
    """Wait until a line of the file at path matches pattern."""
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        m = re.search(pattern, read(str(path)) or "", re.MULTILINE)
        if m:
            return m
        time.sleep(0.1)
    return None


def pando_session(run_: dict, step: str, *, write: bool) -> str | None:
    """Attach branch, mount ext4 from it, write or read the test file, and detach.

    run_ holds base (the pando command and store), branch, dir, results and frontend: "fuse"
    serves the branch in a FUSE mount with a loop device over it, "ublk" as UBLK_DEVICE.
    Returns the SHA-256 of the file: the one written, or the one read back.
    """
    base, branch, d, r = run_["base"], run_["branch"], run_["dir"], run_["results"]
    fuse, mnt, out = d / "fuse", d / "mnt", d / f"{step}.log"
    front = ["--fuse", str(fuse)] if run_["frontend"] == "fuse" else ["--device", UBLK_DEVICE]
    mnt.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    args = [*base, "--cache", str(d / f"cache-{step}"), "attach", branch, *front]
    args += ["--state", str(d / f"state-{step}"), "--log", "strict"]
    with out.open("w") as log_file:
        p = subprocess.Popen(args, stdout=log_file, stderr=subprocess.STDOUT)
    s: dict = {}
    r[step] = s
    got = None
    try:
        m = wait_for(r"^attached \S+ as (/dev/(?:loop|ublkb)\d+)", out, 120)
        s["attach_seconds"] = round(time.monotonic() - t0, 3)
        if not m:
            s["error"] = "no attached line"
            return None
        dev = m.group(1)
        s["device"] = dev
        s["dio"] = (read(f"/sys/block/{Path(dev).name}/loop/dio") or "").strip() or None
        if write:
            s["mkfs"] = run("mkfs.ext4", "-q", "-F", dev)
        s["mount"] = run("mount", "-o", "noatime", dev, mnt)
        if ok(s["mount"]):
            t1 = time.monotonic()
            got = fill(mnt / "f", 128) if write else digest(mnt / "f")
            s["file_seconds"] = round(time.monotonic() - t1, 3)
            s["umount"] = run("umount", mnt)
        t2 = time.monotonic()
        s["detach"] = run(*base, "detach", *front)
        s["detach_seconds"] = round(time.monotonic() - t2, 3)
    finally:
        try:
            s["exit"] = p.wait(timeout=120)
        except subprocess.TimeoutExpired:
            p.kill()
            s["exit"] = "killed"
        s["log"] = (read(str(out)) or "")[-3000:]
    return got


def pando_roundtrip(frontend: str) -> dict:
    """Write a branch in the bucket through frontend, publish it, and read it back on an empty cache."""
    bucket = os.environ.get("PANDO_BUCKET")
    if not bucket or not Path(PANDO).exists():
        return {"skipped": "set PANDO_BUCKET and build the image with pando"}
    os.environ["S3_BUCKET"] = bucket
    os.environ["S3_REGION"] = os.environ.get("PANDO_REGION", "us-east-1")
    os.environ["S3_CREDENTIALS"] = "instance"
    branch = f"probe-{int(time.time())}-{os.urandom(3).hex()}"
    base = [PANDO, "--remote", "s3://agentcore-probe"]
    d = Path(tempfile.mkdtemp())
    r: dict = {"branch": branch, "version": run(PANDO, "--version").get("out", "").strip()}
    try:
        r["init"] = run(*base, "init", branch, str(1 << 30))
        if not ok(r["init"]):
            return r
        ctx = {"base": base, "branch": branch, "dir": d, "results": r, "frontend": frontend}
        wrote = pando_session(ctx, "write", write=True)
        read_back = pando_session(ctx, "read", write=False)
        r["matches"] = wrote is not None and wrote == read_back
        r["delete"] = run(*base, "delete", branch)
    finally:
        shutil.rmtree(d, ignore_errors=True)
    r["ok"] = r.get("matches") is True
    return r


def check_pando() -> dict:
    """Pando's FUSE frontend end to end: the branch as a file in a FUSE mount, a loop device over it."""
    return pando_roundtrip("fuse")


def check_pando_ublk() -> dict:
    """Pando's ublk frontend end to end, when the kernel has ublk."""
    if not Path("/dev/ublk-control").exists():
        return {"skipped": "no /dev/ublk-control"}
    return pando_roundtrip("ublk")


def kernel_config() -> dict:
    """Read the kernel options a frontend depends on, from /proc/config.gz or /boot/config-<release>."""
    text, source = None, None
    with contextlib.suppress(OSError):
        text, source = gzip.decompress(Path("/proc/config.gz").read_bytes()).decode(), "/proc/config.gz"
    if text is None:
        path = f"/boot/config-{os.uname().release}"
        text, source = read(path), path
    if text is None:
        return {"source": None, "note": "neither /proc/config.gz nor /boot/config-<release> is readable"}
    values = {}
    for line in text.splitlines():
        for opt in CONFIG_OPTIONS:
            if line.startswith(f"{opt}=") or line == f"# {opt} is not set":
                values[opt] = line.partition("=")[2] or "n"
    return {"source": source, "options": {o: values.get(o, "absent") for o in CONFIG_OPTIONS}}


def module(name: str) -> dict:
    """Whether a kernel module is loaded or built in, from /sys/module, and whether modprobe finds it."""
    return {
        "sys_module": Path(f"/sys/module/{name}").exists(),
        "in_proc_modules": any(
            line.split(" ", 1)[0] == name for line in (read("/proc/modules") or "").splitlines()
        ),
        "modules_dir": Path(f"/lib/modules/{os.uname().release}").exists(),
        "modprobe": run("modprobe", name, timeout=30),
    }


def openable(path: str) -> str:
    """Whether a device node opens for reading and writing: "ok", "absent" or the error."""
    if not Path(path).exists():
        return "absent"
    try:
        os.close(os.open(path, os.O_RDWR | os.O_CLOEXEC))
    except OSError as e:
        return e.strerror or str(e)
    return "ok"


def check_ublk() -> dict:
    """Whether the kernel has ublk, which Pando's ublk frontend needs as it is."""
    r = {
        "control": openable("/dev/ublk-control"),
        "module": module("ublk_drv"),
        "config": kernel_config().get("options", {}).get("CONFIG_BLK_DEV_UBLK"),
        "io_uring_disabled": (read("/proc/sys/kernel/io_uring_disabled") or "").strip() or None,
    }
    r["ok"] = r["control"] == "ok"
    return r


def check_nbd() -> dict:
    """Whether the kernel has NBD devices, a frontend Pando could serve instead of ublk."""
    devs = sorted(p.name for p in Path("/dev").glob("nbd[0-9]*"))
    r = {
        "devices": devs[:16],
        "count": len(devs),
        "nbd0": openable("/dev/nbd0"),
        "module": module("nbd"),
        "sys_block": sorted(p.name for p in Path("/sys/block").glob("nbd*"))[:16],
        "config": kernel_config().get("options", {}).get("CONFIG_BLK_DEV_NBD"),
    }
    r["ok"] = r["nbd0"] == "ok"
    return r


FUSE_IN_HEADER = struct.Struct("<IIQQIIIHH")
FUSE_OUT_HEADER = struct.Struct("<IiQ")
FUSE_INIT = 26
FUSE_INIT_EXT = 1 << 30
FILESYSTEM_MAX_STACK_DEPTH = 2
FUSE_PASSTHROUGH_FLAGS2 = 1 << (37 - 32)
FUSE_DEV_IOC_BACKING_OPEN = (1 << 30) | (16 << 16) | (229 << 8) | 1
FUSE_DEV_IOC_BACKING_CLOSE = (1 << 30) | (4 << 16) | (229 << 8) | 2


def passthrough_session(dev: int, backing: Path) -> dict:
    """Answer FUSE_INIT on dev asking for passthrough, then register backing as a backing file."""
    r: dict = {}
    if not select.select([dev], [], [], 5)[0]:
        r["error"] = "no FUSE_INIT within 5 s"
        return r
    req = os.read(dev, 1 << 20)
    _, opcode, unique, *_ = FUSE_IN_HEADER.unpack_from(req)
    if opcode != FUSE_INIT:
        r["error"] = f"first request was opcode {opcode}"
        return r
    major, minor, readahead, flags, flags2 = struct.unpack_from("<IIIII", req, FUSE_IN_HEADER.size)
    r["kernel_protocol"] = f"{major}.{minor}"
    r["offered"] = bool(flags & FUSE_INIT_EXT and flags2 & FUSE_PASSTHROUGH_FLAGS2)
    init_out = struct.pack(
        "<IIIIHHIIHHIIH22x",
        major,
        minor,
        readahead,
        FUSE_INIT_EXT,
        16,
        12,
        1 << 20,
        1,
        256,
        0,
        FUSE_PASSTHROUGH_FLAGS2 if r["offered"] else 0,
        FILESYSTEM_MAX_STACK_DEPTH,
        0,
    )
    os.write(dev, FUSE_OUT_HEADER.pack(FUSE_OUT_HEADER.size + len(init_out), 0, unique) + init_out)
    fd = os.open(backing, os.O_RDWR | os.O_CLOEXEC)
    try:
        arg = bytearray(struct.pack("<iIQ", fd, 0, 0))
        try:
            backing_id = fcntl.ioctl(dev, FUSE_DEV_IOC_BACKING_OPEN, arg)
        except OSError as e:
            r["backing_open"] = e.strerror or str(e)
        else:
            r["backing_open"] = "ok"
            r["backing_id"] = backing_id
            with contextlib.suppress(OSError):
                fcntl.ioctl(dev, FUSE_DEV_IOC_BACKING_CLOSE, bytearray(struct.pack("<I", backing_id)))
    finally:
        os.close(fd)
    return r


def check_fuse_passthrough() -> dict:
    """Whether a FUSE daemon here can hand the kernel a backing file (FUSE passthrough, Linux 6.9)."""
    release = os.uname().release
    nums = [int(x) for x in re.findall(r"\d+", release)[:2]]
    r: dict = {
        "kernel": release,
        "kernel_at_least_6_9": nums >= [6, 9],
        "config": kernel_config().get("options", {}).get("CONFIG_FUSE_PASSTHROUGH"),
    }
    if not Path("/dev/fuse").exists():
        r["error"] = "no /dev/fuse"
        r["ok"] = False
        return r
    d = Path(tempfile.mkdtemp())
    mnt, backing = d / "mnt", d / "backing"
    mnt.mkdir()
    backing.write_bytes(b"\0" * 4096)
    r["backing_fs"] = run("stat", "-f", "-c", "%T", backing).get("out", "").strip()
    libc = ctypes.CDLL(None, use_errno=True)
    dev = os.open("/dev/fuse", os.O_RDWR | os.O_CLOEXEC)
    try:
        data = f"fd={dev},rootmode=40000,user_id=0,group_id=0".encode()
        rc = libc.mount(b"probe", str(mnt).encode(), b"fuse.probe", 0, data)
        if rc != 0:
            r["mount"] = os.strerror(ctypes.get_errno())
            r["ok"] = False
            return r
        try:
            r.update(passthrough_session(dev, backing))
        finally:
            libc.umount2(str(mnt).encode(), MNT_DETACH)
    finally:
        os.close(dev)
        shutil.rmtree(d, ignore_errors=True)
    r["ok"] = r.get("backing_open") == "ok"
    return r


GANESHA_CONF = """NFS_CORE_PARAM {{ Protocols = 4; NFS_Port = 2049; Bind_addr = 127.0.0.1;
  Enable_NLM = false; Enable_RQUOTA = false; Enable_UDP = false; }}
NFSV4 {{ Graceless = true; }}
EXPORT {{ Export_Id = 1; Path = {path}; Pseudo = /export; Access_Type = RW; Squash = No_Root_Squash;
  SecType = sys; Protocols = 4; FSAL {{ Name = VFS; }} }}
LOG {{ Default_Log_Level = WARN; }}
"""


def check_loop_nfs() -> dict:
    """ext4 on a loop device over a file on an NFSv4 mount from a localhost NFS server (Ganesha).

    The plan B if loop over FUSE fails: the kernel's NFS client is what AgentCore's documented
    NFS mounts use, and a Pando NFS server would take Ganesha's place.
    """
    filesystems = read("/proc/filesystems") or ""
    r: dict = {
        "nfs_client": [n for n in ("nfs", "nfs4") if re.search(rf"\s{n}$", filesystems, re.MULTILINE)],
        "module": module("nfsv4"),
        "config": {k: v for k, v in kernel_config().get("options", {}).items() if "NFS" in k},
    }
    d = Path(tempfile.mkdtemp())
    export, nfs, conf = d / "export", d / "nfs", d / "ganesha.conf"
    for p in (export, nfs):
        p.mkdir()
    exp_img = d / "export.img"
    with exp_img.open("wb") as f:
        f.truncate(512 << 20)
    server = None
    try:
        r["export_fs"] = run("mkfs.ext4", "-q", "-F", exp_img)
        r["export_mount"] = run("mount", "-o", "loop", exp_img, export)
        if not ok(r["export_mount"]):
            return r
        if not Path("/etc/mtab").exists():
            with contextlib.suppress(OSError):
                Path("/etc/mtab").symlink_to("/proc/self/mounts")
        conf.write_text(GANESHA_CONF.format(path=export))
        r["rpcbind"] = run("rpcbind", "-w", timeout=30)
        server = subprocess.Popen(
            [
                "ganesha.nfsd",
                "-F",
                "-f",
                str(conf),
                "-L",
                str(d / "ganesha.log"),
                "-p",
                str(d / "ganesha.pid"),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for _ in range(20):
            r["nfs_mount"] = run(
                "mount",
                "-t",
                "nfs4",
                "-o",
                "vers=4.2,proto=tcp,port=2049",
                "127.0.0.1:/export",
                nfs,
                timeout=30,
            )
            if ok(r["nfs_mount"]) or server.poll() is not None:
                break
            time.sleep(0.5)
        if not ok(r["nfs_mount"]):
            log_text = read(str(d / "ganesha.log")) or ""
            r["ganesha_log"] = [
                line for line in log_text.splitlines() if ":CRIT :" in line or ":MAJ :" in line
            ][-8:]
            return r
        try:
            img = nfs / "disk.img"
            with img.open("wb") as f:
                f.truncate(256 << 20)
            r["direct"] = ext4_on(img, d / "mnt", direct_io=True)
        finally:
            r["nfs_umount"] = run("umount", nfs, timeout=60)
    finally:
        if server is not None:
            server.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                server.wait(timeout=10)
        run("umount", export, timeout=60)
        shutil.rmtree(d, ignore_errors=True)
        r["ok"] = bool(r.get("direct", {}).get("ok"))
    return r


def summarize(results: dict) -> dict:
    """Which frontends work here, from the checks that ran, and the one to use."""

    def got(name: str, key: str = "ok") -> bool | None:
        res = results.get(name)
        return None if not isinstance(res, dict) or "skipped" in res else res.get(key)

    dm = results.get("dm") if isinstance(results.get("dm"), dict) else {}
    s = {
        "ublk_kernel": got("ublk"),
        "ublk_pando": got("pando_ublk"),
        "nbd_kernel": got("nbd"),
        "dm_linear": dm.get("has_linear"),
        "dm_era": dm.get("era_ok"),
        "loop_over_fuse": got("loop_fuse"),
        "fuse_pando": got("pando"),
        "fuse_passthrough": got("fuse_passthrough"),
        "loop_over_nfs": got("loop_nfs"),
    }
    order = (
        ("ublk_pando", "Pando's ublk frontend as is"),
        ("fuse_pando", "Pando's FUSE frontend (attach --fuse)"),
        ("ublk_kernel", "ublk: the kernel has it; run pando_ublk to confirm Pando's frontend"),
        ("loop_over_fuse", "FUSE and loop: a FUSE-served file works under a loop device"),
        ("nbd_kernel", "NBD: needs a Pando NBD frontend"),
        ("loop_over_nfs", "NFS and loop: needs a Pando NFS server"),
    )
    s["best"] = next((text for key, text in order if s[key]), None)
    s["image_handoff"] = bool(s["dm_linear"]) and bool(s["dm_era"])
    return s


def check_shell(command: str) -> dict:
    """Run a shell command, when the runtime allows it (PROBE_ALLOW_SHELL=1)."""
    if os.environ.get("PROBE_ALLOW_SHELL") != "1":
        return {"skipped": "PROBE_ALLOW_SHELL is not 1"}
    return run("sh", "-c", command)


CHECKS: dict[str, Callable[[], dict]] = {
    "kernel": check_kernel,
    "dev": check_dev,
    "caps": check_caps,
    "dm": check_dm,
    "fs": check_fs,
    "memory": check_memory,
    "metadata": check_metadata,
    "clocks": check_clocks,
    "loop_file": check_loop_file,
    "loop_fuse": check_loop_fuse,
    "ublk": check_ublk,
    "nbd": check_nbd,
    "fuse_passthrough": check_fuse_passthrough,
    "loop_nfs": check_loop_nfs,
    "pando": check_pando,
    "pando_ublk": check_pando_ublk,
}


def invoke(payload: dict) -> dict:
    """Run the checks the payload names, each on its own, and log every result."""
    names = payload.get("checks") or list(CHECKS)
    results: dict = {"started": time.time()}
    WORK.mkdir(parents=True, exist_ok=True)
    for name in names:
        fn = CHECKS.get(name)
        t0 = time.monotonic()
        if fn is None:
            res: dict = {"error": f"unknown check; known: {sorted(CHECKS)}"}
        else:
            try:
                res = fn()
            except Exception as e:  # noqa: BLE001
                res = {"error": repr(e), "trace": traceback.format_exc()[-2000:]}
        res["check_seconds"] = round(time.monotonic() - t0, 3)
        results[name] = res
        log("check", name=name, result=res)
    results["frontends"] = summarize(results)
    log("frontends", result=results["frontends"])
    if "shell" in payload:
        results["shell"] = check_shell(str(payload["shell"]))
        log("check", name="shell", result=results["shell"])
    return results


class Handler(BaseHTTPRequestHandler):
    """The AgentCore HTTP contract: GET /ping and POST /invocations."""

    def reply(self, status: int, body: dict) -> None:
        """Send body as JSON."""
        data = json.dumps(body, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        """Answer /ping."""
        if self.path != "/ping":
            self.reply(404, {"error": "not found"})
            return
        with state_lock:
            first = not state["pinged"]
            state["pinged"] = True
        if first:
            log("first healthy ping", note="AgentCore takes its snapshot after this answer")
        self.reply(200, {"status": "Healthy"})

    def do_POST(self) -> None:
        """Answer /invocations."""
        if self.path != "/invocations":
            self.reply(404, {"error": "not found"})
            return
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b"{}"
        try:
            payload = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        session = self.headers.get("X-Amzn-Bedrock-AgentCore-Runtime-Session-Id")
        log("invocation", session=session, payload=payload)
        results = invoke(payload)
        results["session"] = session
        self.reply(200, results)

    def log_message(self, fmt: str, *args: object) -> None:
        """Log requests as JSON lines instead of on stderr."""
        log("http", line=fmt % args)


def on_signal(signum: int, _frame: object) -> None:
    """Log a signal and keep running, so the logs show how long the platform waits to kill."""
    name = signal.Signals(signum).name
    with state_lock:
        state["signals"].append((name, time.time()))
        if signum == signal.SIGTERM and state["term_at"] is None:
            state["term_at"] = time.monotonic()
    log("signal", signal=name)


def heartbeat() -> None:
    """Log a heartbeat every 10 s, every 0.2 s after SIGTERM, and any jump of the wall clock."""
    offset = time.time() - time.monotonic()
    while True:
        with state_lock:
            term_at = state["term_at"]
        time.sleep(TERM_HEARTBEAT_SECONDS if term_at is not None else HEARTBEAT_SECONDS)
        now_offset = time.time() - time.monotonic()
        if abs(now_offset - offset) > CLOCK_JUMP_SECONDS:
            moved = round(now_offset - offset, 3)
            log("clock jump", wall_minus_monotonic_moved=moved, note="a snapshot restore")
            offset = now_offset
        fields = {"since_sigterm": round(time.monotonic() - term_at, 3)} if term_at is not None else {}
        log("heartbeat", **fields)


def main() -> None:
    """Serve on port 8080 until killed."""
    for s in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT, signal.SIGUSR1, signal.SIGUSR2):
        signal.signal(s, on_signal)
    threading.Thread(target=heartbeat, name="heartbeat", daemon=True).start()
    log("started", start=START, argv=sys.argv)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()  # noqa: S104


if __name__ == "__main__":
    main()
