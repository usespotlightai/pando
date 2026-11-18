"""The fio measure: direct I/O on a file in a fresh branch's file system, per system.

Each system opens a branch on a warm host, lays out one 1 GiB file (the branch's file system is
the 4 GiB workspace image), then runs each job for 20 s after a 2 s ramp, page cache dropped
before each job. Pando (after handoff) waits until the
device has switched to its local image. Rows: fio.<job> in IOPS for 4 KiB jobs and MB/s for
1 MiB jobs.
"""

import argparse
import json
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import start
import workspace
from common import Results, log, run

SIZE = "1G"
RUNTIME = 20
RAMP = 2

# (row, unit, fio arguments)
JOBS = [
    ("randread_qd1", "IOPS", ["--rw=randread", "--bs=4k", "--iodepth=1", "--numjobs=1"]),
    ("randread_qd32", "IOPS", ["--rw=randread", "--bs=4k", "--iodepth=32", "--numjobs=4"]),
    ("randwrite_qd32", "IOPS", ["--rw=randwrite", "--bs=4k", "--iodepth=32", "--numjobs=4"]),
    ("seqread", "MB/s", ["--rw=read", "--bs=1M", "--iodepth=8", "--numjobs=1"]),
    ("seqwrite", "MB/s", ["--rw=write", "--bs=1M", "--iodepth=8", "--numjobs=1"]),
]

# key: (row name in the chart, workspace class)
SYSTEMS: dict[str, tuple[str, type[start.Workspace]]] = {
    "native": ("local NVMe", start.Native),
    "pando": ("Pando (ublk)", start.Pando),
    "pando-handoff": ("Pando (after handoff)", start.PandoHandoff),
    "ebs": ("EBS gp3", start.Ebs),
    "juicefs": ("JuiceFS", start.JuiceFS),
}


def fio(path: Path, *args: str) -> dict[str, Any]:
    """Run fio on path with args; return its report of the one job group.

    Raises CalledProcessError when fio fails.
    """
    out = run(
        "fio",
        "--output-format=json",
        f"--filename={path}",
        f"--size={SIZE}",
        "--direct=1",
        "--ioengine=libaio",
        "--group_reporting",
        *args,
        capture=True,
    )
    return json.loads(out[out.index("{") :])["jobs"][0]


def measure_system(key: str, reps: int, out: Results) -> bool:
    """Run every job reps times on system key; return False when the system failed, True otherwise."""
    name, cls = SYSTEMS[key]
    s = cls()
    reason = s.unavailable()
    if reason:
        log(f"{name}: skipped, {reason}")
        return True
    log(f"{name}: setting up")
    try:
        s.setup()
        start.warm_up(s)
        s.open(1)
        if getattr(s, "handoff", False):
            log(f"{name}: {s.wait_line(1, 'handed ').strip()}")
        path = s.root(1) / "fio.dat"
        fio(path, "--name=layout", "--rw=write", "--bs=1M", "--iodepth=8")
        run("sync")
        for rep in range(1, reps + 1):
            for row, unit, args in JOBS:
                start.drop_caches()
                j = fio(
                    path,
                    f"--name={row}",
                    "--time_based",
                    f"--runtime={RUNTIME}",
                    f"--ramp_time={RAMP}",
                    *args,
                )
                side = j["write"] if "write" in args[0] else j["read"]
                v = side["iops"] if unit == "IOPS" else side["bw_bytes"] / 1e6
                out.add(name, f"fio.{row}", rep, v, unit, f"rep {rep}")
                log(f"{name}: fio rep {rep}: {row} {v:,.0f} {unit}")
        (s.root(1) / "fio.dat").unlink()
        s.close(1)
    except (
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        SystemExit,
        OSError,
        ValueError,
        KeyError,
    ) as e:
        log(f"{name}: failed, {e}")
        return False
    else:
        return True
    finally:
        log(f"{name}: cleaning up")
        s.teardown()


def measure(names: list[str], reps: int) -> None:
    """Run the fio measure on the named systems.

    Raises SystemExit when any system failed.
    """
    workspace.prepare()
    out = Results("fio")
    failed = [SYSTEMS[k][0] for k in names if not measure_system(k, reps, out)]
    if failed:
        msg = f"fio failed for {', '.join(failed)}"
        raise SystemExit(msg)


def main_args(add_parser: Callable[..., argparse.ArgumentParser]) -> dict[str, object]:
    """Add the fio subcommand with add_parser; return its systems by key."""
    p = add_parser("fio", help="raw direct I/O on a branch's file system")
    p.add_argument("--systems", default=",".join(SYSTEMS), help=f"comma-separated, from {', '.join(SYSTEMS)}")
    p.add_argument("--reps", type=int, default=5)
    return SYSTEMS
