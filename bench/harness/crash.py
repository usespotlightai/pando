"""The crash measure: `kill -9` of the storage daemon while a writer writes and fsyncs files.

The writer writes in a branch, which is then opened as a fresh host would open it.

A writer outside the system writes 256 KiB files of known content into the branch, fsyncing
each file and the directory, and logs each write as acknowledged or failed; after a failure it
retries the next file 50 ms later. Five seconds in, the daemon is killed and at once restarted
as the system restarts it: Pando with `pando attach --recover` on the same device, JuiceFS by a
fresh mount on the same mount point, ZeroFS by starting the server again (its NBD client runs
with -persist and reconnects), S3 Files by the efs-utils watchdog, which restarts the efs-proxy
process that carries the mount's NFS traffic. The writer runs 5 s more after its first write
succeeds again. The branch is then closed as usual. A fresh host is a new cache and state
directory on this host, sharing nothing with the first: there the file system is checked
(e2fsck -fn for Pando and ZeroFS, `juicefs fsck` for JuiceFS; S3 Files has no checker) and every
acknowledged file is read back.

Rows: crash.failed_writes (count), crash.stall (s: the longest gap between acknowledged writes),
crash.fsck (the checker's exit code), crash.acked and crash.correct (files).
"""

import argparse
import contextlib
import hashlib
import itertools
import os
import shutil
import signal
import string
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import override

import start
import workspace
from common import Results, log, run

FILE_BYTES = 256 << 10
BEFORE_KILL = 5
AFTER_RECOVERY = 5
RECOVERY_TIMEOUT = 120
LOG_FIELDS = 3

WRITER = string.Template(
    r"""
import errno, hashlib, os, sys, time
d, log, stop = sys.argv[1], sys.argv[2], sys.argv[3]
os.makedirs(d, exist_ok=True)
with open(log, "w", buffering=1) as out:
    n = 0
    while not os.path.exists(stop):
        data = hashlib.shake_256(str(n).encode()).digest($file_bytes)
        try:
            fd = os.open(f"{d}/f{n:06d}", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                os.write(fd, data)
                os.fsync(fd)
            finally:
                os.close(fd)
            dfd = os.open(d, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
            out.write(f"{n} ok {time.monotonic():.6f}\n")
        except OSError as e:
            out.write(f"{n} err {time.monotonic():.6f} {errno.errorcode.get(e.errno, e.errno)}\n")
            time.sleep(0.05)
        n += 1
"""
).substitute(file_bytes=FILE_BYTES)


def content(n: int) -> bytes:
    """Return the content the writer writes to file n."""
    return hashlib.shake_256(str(n).encode()).digest(FILE_BYTES)


def read_log(path: Path) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    """Return the writer's acknowledged and failed writes so far, each as (file number, time)."""
    ok, err = [], []
    for line in Path(path).read_text().splitlines():
        parts = line.split()
        if len(parts) >= LOG_FIELDS:
            (ok if parts[1] == "ok" else err).append((int(parts[0]), float(parts[2])))
    return ok, err


class Crasher[W: start.Workspace]:
    """How one system's storage daemon is killed and restarted, and how a fresh host checks a branch."""

    def __init__(self, s: W) -> None:
        """Crash branches of s."""
        self.s = s

    def kill_and_restart(self, i: int) -> None:
        """Kill the daemon serving branch i with SIGKILL and restart it as the system would."""
        raise NotImplementedError

    def close(self, i: int) -> None:
        """Close branch i as usual."""
        self.s.close(i)

    def fresh_check(self, i: int) -> tuple[int | None, Path]:
        """Open branch i with nothing from this host; return the checker's exit code and the mount point.

        The exit code is None for a system with no checker.
        """
        raise NotImplementedError

    def fresh_close(self, i: int) -> None:
        """Close what fresh_check opened."""
        raise NotImplementedError


class PandoCrasher(Crasher[start.Pando]):
    """`pando attach --recover` on the same device; a fresh host runs e2fsck -fn."""

    FRESH = 50

    @override
    def kill_and_restart(self, i: int) -> None:
        p, out = self.s.procs.pop(i)
        os.kill(p.pid, signal.SIGKILL)
        p.wait()
        out.write(p.stdout.read())
        out.close()
        self.s.attach(i, f"b{i}", "--recover")

    @override
    def fresh_check(self, i: int) -> tuple[int, Path]:
        fresh = start.Pando()
        fresh.dir = self.s.dir / "fresh"
        fresh.bucket, fresh.keys, fresh.procs = self.s.bucket, self.s.keys, {}
        fresh.dir.mkdir(parents=True, exist_ok=True)
        self.fresh = fresh
        j = self.FRESH + i
        fresh.attach(j, f"b{i}")
        code = run("e2fsck", "-fn", fresh.device_path(j), check=False)
        fresh.root(j).mkdir(exist_ok=True)
        run("mount", "-o", "ro", fresh.device_path(j), fresh.root(j))
        return code, fresh.root(j)

    @override
    def fresh_close(self, i: int) -> None:
        self.fresh.close(self.FRESH + i)
        shutil.rmtree(self.fresh.dir, ignore_errors=True)


class JuiceFSCrasher(Crasher[start.JuiceFS]):
    """A fresh mount on the same mount point; a fresh host runs `juicefs fsck`."""

    @override
    def kill_and_restart(self, i: int) -> None:
        pids = run("pgrep", "-f", f"juicefs mount .*{self.s.mnt}", capture=True, check=False).split()
        if not pids:
            msg = "no juicefs mount process"
            raise SystemExit(msg)
        for pid in pids:
            os.kill(int(pid), signal.SIGKILL)
        run("umount", "-l", self.s.mnt, check=False)
        self.s.ensure_mounted()

    @override
    def close(self, i: int) -> None:
        start.unmount(self.s.mnt)

    @override
    def fresh_check(self, i: int) -> tuple[int, Path]:
        self.s.empty()
        code = subprocess.run(
            [str(self.s.bin), "fsck", self.s.meta], capture_output=True, check=False
        ).returncode
        self.s.ensure_mounted()
        return code, self.s.root(i)

    @override
    def fresh_close(self, i: int) -> None:
        self.s.close(i)


class ZeroFSCrasher(Crasher[start.ZeroFS]):
    """The server started again on the same cache; a fresh host has a server on an empty cache.

    The fresh host runs e2fsck -fn.
    """

    @override
    def kill_and_restart(self, i: int) -> None:
        self.s.server.stop(kill=True)
        self.s.server.start()

    @override
    def close(self, i: int) -> None:
        start.unmount(self.s.root(i))
        self.s.server.disconnect(self.s.server.device(i))

    @override
    def fresh_check(self, i: int) -> tuple[int | None, Path]:
        self.s.empty()
        dev = self.s.server.connect(f"b{i}", i)
        code = run("e2fsck", "-fn", dev, check=False)
        self.s.root(i).mkdir(exist_ok=True)
        run("mount", "-o", "ro", dev, self.s.root(i))
        return code, self.s.root(i)

    @override
    def fresh_close(self, i: int) -> None:
        self.s.close(i)


class S3FilesCrasher(Crasher[start.S3Files]):
    """SIGKILL of the mount's efs-proxy, which the efs-utils watchdog restarts.

    A fresh host is a new mount.
    """

    @override
    def kill_and_restart(self, i: int) -> None:
        pids = run("pgrep", "-x", "efs-proxy", capture=True, check=False).split()
        if not pids:
            msg = "no efs-proxy process"
            raise SystemExit(msg)
        for pid in pids:
            os.kill(int(pid), signal.SIGKILL)

    @override
    def close(self, i: int) -> None:
        pass

    @override
    def fresh_check(self, i: int) -> tuple[int | None, Path]:
        self.s.empty()
        start.drop_caches()
        self.s.fs.ensure_mounted()
        return None, self.s.root(i)

    @override
    def fresh_close(self, i: int) -> None:
        self.s.close(i)


SYSTEMS: dict[str, tuple[type[start.Workspace], type[Crasher]]] = {
    "pando": (start.Pando, PandoCrasher),
    "juicefs": (start.JuiceFS, JuiceFSCrasher),
    "zerofs": (start.ZeroFS, ZeroFSCrasher),
    "s3files": (start.S3Files, S3FilesCrasher),
}


def crash_once(s: start.Workspace, crasher: Crasher, i: int, rep: int, out: Results) -> None:
    """Crash the daemon under writes to a fresh branch i of s, check it as a fresh host would; write the rows.

    Raises SystemExit when no write is acknowledged within RECOVERY_TIMEOUT of the kill.
    """
    s.open(i)
    d = s.root(i) / "crash"
    wlog = start.DIR / f"crash-writer-{i}.log"
    stop = start.DIR / f"crash-stop-{i}"
    stop.unlink(missing_ok=True)
    w = subprocess.Popen([sys.executable, "-c", WRITER, str(d), str(wlog), str(stop)])
    try:
        time.sleep(BEFORE_KILL)
        killed = time.monotonic()
        crasher.kill_and_restart(i)
        deadline = time.monotonic() + RECOVERY_TIMEOUT
        while time.monotonic() < deadline:
            ok, _ = read_log(wlog)
            if ok and ok[-1][1] > killed:
                break
            time.sleep(0.2)
        else:
            msg = f"no acknowledged write within {RECOVERY_TIMEOUT} s of the kill"
            raise SystemExit(msg)
        time.sleep(AFTER_RECOVERY)
    finally:
        stop.touch()
        try:
            w.wait(timeout=60)
        except subprocess.TimeoutExpired:
            w.kill()
            w.wait()
    ok, err = read_log(wlog)
    times = [t for _, t in ok]
    stall = max(b - a for a, b in itertools.pairwise(times))
    crasher.close(i)
    code, root = crasher.fresh_check(i)
    correct = 0
    for n, _ in ok:
        with contextlib.suppress(OSError):
            if (root / "crash" / f"f{n:06d}").read_bytes() == content(n):
                correct += 1
    crasher.fresh_close(i)
    note = f"rep {rep}"
    out.add(s.name, "crash.failed_writes", rep, len(err), "count", note)
    out.add(s.name, "crash.stall", rep, stall, "s", note)
    if code is not None:
        out.add(s.name, "crash.fsck", rep, code, "code", note)
    out.add(s.name, "crash.acked", rep, len(ok), "count", note)
    out.add(s.name, "crash.correct", rep, correct, "count", note)
    log(
        f"{s.name}: crash rep {rep}: {len(err)} failed writes, stall {stall:.2f} s, fsck exit {code}, "
        f"{correct} of {len(ok)} acknowledged files correct"
    )
    wlog.unlink(missing_ok=True)


def measure(names: list[str], reps: int) -> None:
    """Run the crash measure on the named systems.

    Raises SystemExit when any system failed.
    """
    workspace.prepare()
    out = Results("crash")
    failed = []
    for key in names:
        cls, crasher_cls = SYSTEMS[key]
        s = cls()
        reason = s.unavailable()
        if reason:
            log(f"{s.name}: skipped, {reason}")
            continue
        log(f"{s.name}: setting up")
        try:
            s.setup()
            crasher = crasher_cls(s)
            for rep in range(1, min(reps, s.max_reps) + 1):
                crash_once(s, crasher, rep, rep, out)
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            SystemExit,
            OSError,
            ValueError,
            KeyError,
        ) as e:
            log(f"{s.name}: failed, {e}")
            failed.append(s.name)
        finally:
            log(f"{s.name}: cleaning up")
            s.teardown()
    if failed:
        msg = f"crash failed for {', '.join(failed)}"
        raise SystemExit(msg)


def main_args(add_parser: Callable[..., argparse.ArgumentParser]) -> dict[str, object]:
    """Add the crash subcommand with add_parser; return its systems by key."""
    p = add_parser("crash", help="kill -9 of the storage daemon during writes")
    p.add_argument("--systems", default=",".join(SYSTEMS), help=f"comma-separated, from {', '.join(SYSTEMS)}")
    p.add_argument("--reps", type=int, default=3)
    return SYSTEMS
