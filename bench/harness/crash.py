"""The crash measure: `kill -9` of the storage daemon while a writer writes and fsyncs files in
a branch, then the branch opened as a fresh host would open it.

A writer outside the system writes 256 KiB files of known content into the branch, fsyncing
each file and the directory, and logs each write as acknowledged or failed; after a failure it
retries the next file 50 ms later. Five seconds in, the daemon is killed and at once restarted
as the system restarts it: Pando with `pando attach --recover` on the same device, JuiceFS by a
fresh mount on the same mount point. The writer runs 5 s more after its first write succeeds
again. The branch is then closed as usual. A fresh host is a new cache and state directory on
this host, sharing nothing with the first: there the file system is checked (e2fsck -fn for
Pando, `juicefs fsck` for JuiceFS) and every acknowledged file is read back.

Rows: crash.failed_writes (count), crash.stall (s: the longest gap between acknowledged writes),
crash.fsck (the checker's exit code), crash.acked and crash.correct (files).
"""

import hashlib
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import start
import workspace
from common import Results, log, run

FILE_BYTES = 256 << 10
BEFORE_KILL = 5
AFTER_RECOVERY = 5
RECOVERY_TIMEOUT = 120

WRITER = r'''
import errno, hashlib, os, sys, time
d, log, stop = sys.argv[1], sys.argv[2], sys.argv[3]
os.makedirs(d, exist_ok=True)
with open(log, "w", buffering=1) as out:
    n = 0
    while not os.path.exists(stop):
        data = hashlib.shake_256(str(n).encode()).digest(%d)
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
''' % FILE_BYTES


def content(n):
    return hashlib.shake_256(str(n).encode()).digest(FILE_BYTES)


def read_log(path):
    ok, err = [], []
    for line in Path(path).read_text().splitlines():
        parts = line.split()
        if len(parts) >= 3:
            (ok if parts[1] == "ok" else err).append((int(parts[0]), float(parts[2])))
    return ok, err


class Crasher:
    def __init__(self, s):
        self.s = s

    def kill_and_restart(self, i):
        raise NotImplementedError

    def close(self, i):
        self.s.close(i)

    def fresh_check(self, i):
        """Opens branch i with nothing from this host; returns (checker exit code, mount point)."""
        raise NotImplementedError

    def fresh_close(self, i):
        raise NotImplementedError


class PandoCrasher(Crasher):
    FRESH = 50

    def kill_and_restart(self, i):
        p, out = self.s.procs.pop(i)
        os.kill(p.pid, signal.SIGKILL)
        p.wait()
        out.write(p.stdout.read())
        out.close()
        self.s.attach(i, f"b{i}", "--recover")

    def fresh_check(self, i):
        fresh = start.Pando()
        fresh.dir = self.s.dir / "fresh"
        fresh.bucket, fresh.keys, fresh.procs = self.s.bucket, self.s.keys, {}
        fresh.dir.mkdir(parents=True, exist_ok=True)
        self.fresh = fresh
        j = self.FRESH + i
        fresh.attach(j, f"b{i}")
        code = run("e2fsck", "-fn", fresh._device(j), check=False)
        fresh.root(j).mkdir(exist_ok=True)
        run("mount", "-o", "ro", fresh._device(j), fresh.root(j))
        return code, fresh.root(j)

    def fresh_close(self, i):
        self.fresh.close(self.FRESH + i)
        shutil.rmtree(self.fresh.dir, ignore_errors=True)


class JuiceFSCrasher(Crasher):
    def kill_and_restart(self, i):
        pids = run("pgrep", "-f", f"juicefs mount .*{self.s.mnt}", capture=True, check=False).split()
        if not pids:
            raise SystemExit("no juicefs mount process")
        for pid in pids:
            os.kill(int(pid), signal.SIGKILL)
        run("umount", "-l", self.s.mnt, check=False)
        self.s._mount()

    def close(self, i):
        start.unmount(self.s.mnt)

    def fresh_check(self, i):
        self.s.empty()
        code = subprocess.run([str(self.s.bin), "fsck", self.s.meta], capture_output=True).returncode
        self.s._mount()
        return code, self.s.root(i)

    def fresh_close(self, i):
        self.s.close(i)


SYSTEMS = {"pando": (start.Pando, PandoCrasher), "juicefs": (start.JuiceFS, JuiceFSCrasher)}


def crash_once(s, crasher, i, rep, out):
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
            raise SystemExit(f"no acknowledged write within {RECOVERY_TIMEOUT} s of the kill")
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
    stall = max(b - a for a, b in zip(times, times[1:]))
    crasher.close(i)
    code, root = crasher.fresh_check(i)
    correct = 0
    for n, _ in ok:
        try:
            if (root / "crash" / f"f{n:06d}").read_bytes() == content(n):
                correct += 1
        except OSError:
            pass
    crasher.fresh_close(i)
    note = f"rep {rep}"
    out.add(s.name, "crash.failed_writes", rep, len(err), "count", note)
    out.add(s.name, "crash.stall", rep, stall, "s", note)
    out.add(s.name, "crash.fsck", rep, code, "code", note)
    out.add(s.name, "crash.acked", rep, len(ok), "count", note)
    out.add(s.name, "crash.correct", rep, correct, "count", note)
    log(f"{s.name}: crash rep {rep}: {len(err)} failed writes, stall {stall:.2f} s, fsck exit {code}, "
        f"{correct} of {len(ok)} acknowledged files correct")
    wlog.unlink(missing_ok=True)


def measure(names, reps):
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
            for rep in range(1, reps + 1):
                crash_once(s, crasher, rep, rep, out)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, SystemExit, OSError, ValueError, KeyError) as e:
            log(f"{s.name}: failed, {e}")
            failed.append(s.name)
        finally:
            log(f"{s.name}: cleaning up")
            s.teardown()
    if failed:
        raise SystemExit(f"crash failed for {', '.join(failed)}")


def main_args(sub):
    p = sub.add_parser("crash", help="kill -9 of the storage daemon during writes")
    p.add_argument("--systems", default=",".join(SYSTEMS), help=f"comma-separated, from {', '.join(SYSTEMS)}")
    p.add_argument("--reps", type=int, default=3)
    return SYSTEMS
