"""The crash measure: `kill -9` of the storage daemon while a writer writes and fsyncs files.

The writer writes in a branch, which is then opened as a fresh host would open it.

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

The reboot case (`crash-reboot`, Pando only) runs in two processes with a reboot between them.
`start` opens a fresh branch, starts the writer with its log fsynced after each acknowledged
write, and five seconds in reboots the host through /proc/sysrq-trigger, which neither syncs
nor unmounts, as a power loss would. After the boot, `check` attaches the branch with
`--recover=auto` from the state the host kept, mounts it so ext4 replays its journal, unmounts
it, runs e2fsck -fn, reads every acknowledged file back, closes the branch and checks it again as
a fresh host. `clean` unmounts and detaches what a failed check left, then removes what the runs
left on the host and in the bucket.

Rows: crash.reboot.attach (s: from the start of the attach to the device serving),
crash.reboot.fsck and crash.reboot.fresh_fsck (exit codes), crash.reboot.acked,
crash.reboot.correct and crash.reboot.fresh_correct (files).
"""

import argparse
import contextlib
import hashlib
import itertools
import json
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
from common import Results, log, read_json, run

FILE_BYTES = 256 << 10
BEFORE_KILL = 5
AFTER_RECOVERY = 5
RECOVERY_TIMEOUT = 120
LOG_FIELDS = 3
REBOOT_STATE = start.DIR / "crash-reboot.json"
BOOT_ID = Path("/proc/sys/kernel/random/boot_id")

WRITER = string.Template(
    r"""
import errno, hashlib, os, sys, time
d, log, stop = sys.argv[1], sys.argv[2], sys.argv[3]
durable = len(sys.argv) > 4
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
            if durable:
                os.fsync(out.fileno())
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

    def fresh_check(self, i: int) -> tuple[int, Path]:
        """Open branch i with nothing from this host; return the checker's exit code and the mount point."""
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


SYSTEMS: dict[str, tuple[type[start.Workspace], type[Crasher]]] = {
    "pando": (start.Pando, PandoCrasher),
    "juicefs": (start.JuiceFS, JuiceFSCrasher),
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
    correct = correct_files(root, ok)
    crasher.fresh_close(i)
    note = f"rep {rep}"
    out.add(s.name, "crash.failed_writes", rep, len(err), "count", note)
    out.add(s.name, "crash.stall", rep, stall, "s", note)
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
            for rep in range(1, reps + 1):
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


def correct_files(root: Path, ok: list[tuple[int, float]]) -> int:
    """Return how many of the acknowledged files under root/crash hold what the writer wrote."""
    correct = 0
    for n, _ in ok:
        with contextlib.suppress(OSError):
            if (root / "crash" / f"f{n:06d}").read_bytes() == content(n):
                correct += 1
    return correct


def reboot_log(rep: int) -> Path:
    """Return the path of the writer's log for reboot rep."""
    return start.DIR / f"crash-reboot-writer-{rep}.log"


def reboot_start(rep: int) -> None:
    """Open branch b<rep> under writes and reboot the host without a sync; return only on failure.

    The first rep sets Pando up; later reps keep what the first imported.

    Raises SystemExit when the writer has no acknowledged write by the reboot.
    """
    s = start.Pando()
    if rep == 1:
        workspace.prepare()
        s.setup()
    else:
        s.connect()
    s.open(rep)
    wlog = reboot_log(rep)
    stop = start.DIR / f"crash-reboot-stop-{rep}"
    stop.unlink(missing_ok=True)
    REBOOT_STATE.write_text(json.dumps({"rep": rep, "boot": BOOT_ID.read_text().strip()}))
    os.sync()
    args = [sys.executable, "-c", WRITER, str(s.root(rep) / "crash"), str(wlog), str(stop), "durable"]
    w = subprocess.Popen(args)
    time.sleep(BEFORE_KILL)
    ok, err = read_log(wlog)
    if not ok:
        stop.touch()
        w.wait(timeout=60)
        msg = f"no acknowledged write in {BEFORE_KILL} s ({len(err)} failed)"
        raise SystemExit(msg)
    log(f"{s.name}: crash reboot rep {rep}: {len(ok)} acknowledged writes, rebooting")
    time.sleep(1)
    Path("/proc/sysrq-trigger").write_text("b")
    time.sleep(60)
    msg = "the host did not reboot"
    raise SystemExit(msg)


def reboot_check(rep: int) -> None:
    """Recover branch b<rep> after the reboot, check it here and as a fresh host; write the rows.

    Raises SystemExit when the state names another rep or the host has not rebooted since.
    """
    state = read_json(REBOOT_STATE, {})
    if not isinstance(state, dict) or state.get("rep") != rep:
        msg = f"no crash-reboot start of rep {rep} on this host"
        raise SystemExit(msg)
    if state.get("boot") == BOOT_ID.read_text().strip():
        msg = "the host has not rebooted since the start"
        raise SystemExit(msg)
    s = start.Pando()
    s.connect()
    crasher = PandoCrasher(s)
    dev, root = s.device_path(rep), s.root(rep)
    t0 = time.perf_counter()
    s.attach(rep, f"b{rep}", "--recover=auto")
    attach = time.perf_counter() - t0
    root.mkdir(exist_ok=True)
    run("mount", dev, root)
    start.unmount(root)
    code = run("e2fsck", "-fn", dev, check=False)
    run("mount", dev, root)
    ok, _ = read_log(reboot_log(rep))
    correct = correct_files(root, ok)
    crasher.close(rep)
    fresh_code, fresh_root = crasher.fresh_check(rep)
    fresh_correct = correct_files(fresh_root, ok)
    crasher.fresh_close(rep)
    out = Results("crash")
    note = f"rep {rep}"
    out.add(s.name, "crash.reboot.attach", rep, attach, "s", note)
    out.add(s.name, "crash.reboot.fsck", rep, code, "code", note)
    out.add(s.name, "crash.reboot.fresh_fsck", rep, fresh_code, "code", note)
    out.add(s.name, "crash.reboot.acked", rep, len(ok), "count", note)
    out.add(s.name, "crash.reboot.correct", rep, correct, "count", note)
    out.add(s.name, "crash.reboot.fresh_correct", rep, fresh_correct, "count", note)
    log(
        f"{s.name}: crash reboot rep {rep}: attach {attach:.2f} s, "
        f"fsck exit {code}, fresh fsck exit {fresh_code}, {correct} and {fresh_correct} of {len(ok)} "
        "acknowledged files correct"
    )
    reboot_log(rep).unlink(missing_ok=True)


def reboot_clean() -> None:
    """Close the branches a failed check left; remove what the runs left on this host and in the bucket."""
    s = start.Pando()
    s.connect()
    for d in (s.dir / "fresh", s.dir):
        held = start.Pando()
        held.dir = d
        held.connect()
        for root in d.glob("m[0-9]*"):
            if root.name[1:].isdigit():
                held.close(int(root.name[1:]))
    s.teardown()
    REBOOT_STATE.unlink(missing_ok=True)
    for f in start.DIR.glob("crash-reboot-*"):
        f.unlink(missing_ok=True)


def reboot(phase: str, rep: int) -> None:
    """Run one phase of the reboot case: start, check or clean."""
    if phase == "start":
        reboot_start(rep)
    elif phase == "check":
        reboot_check(rep)
    else:
        reboot_clean()


def main_args(add_parser: Callable[..., argparse.ArgumentParser]) -> dict[str, object]:
    """Add the crash and crash-reboot subcommands with add_parser; return crash's systems by key."""
    p = add_parser("crash", help="kill -9 of the storage daemon during writes")
    p.add_argument("--systems", default=",".join(SYSTEMS), help=f"comma-separated, from {', '.join(SYSTEMS)}")
    p.add_argument("--reps", type=int, default=5)
    r = add_parser("crash-reboot", help="a host reboot without a sync during writes (Pando)")
    r.add_argument("phase", choices=["start", "check", "clean"])
    r.add_argument("--rep", type=int, default=1, help="repetition number, also the branch's number")
    return SYSTEMS
