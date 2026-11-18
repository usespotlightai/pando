"""Shared pieces of the bench harness: settings, commands, timing, results."""

import contextlib
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

DATA = Path(os.environ.get("PANDO_BENCH_DATA", "/data"))
RESULTS = Path(os.environ.get("PANDO_BENCH_RESULTS", DATA / "results"))
WORK = DATA / "work"
TOOLS = DATA / "tools"
WORKSPACE_NAME = "small"
RESULT_FIELDS = ["system", "workspace", "measure", "rep", "value", "unit", "notes"]


def env(name: str, default: str | None = None) -> str:
    """Return a setting from the environment or /etc/pando-bench.env.

    Raises SystemExit when the setting is in neither and there is no default.
    """
    if name in os.environ:
        return os.environ[name]
    with contextlib.suppress(FileNotFoundError):
        for line in Path("/etc/pando-bench.env").read_text().splitlines():
            k, _, v = line.partition("=")
            if k == name:
                return v
    if default is None:
        msg = f"{name} is not set"
        raise SystemExit(msg)
    return default


def log(msg: str) -> None:
    """Print a timestamped progress line on stderr."""
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def run(
    *args: object,
    check: bool = True,
    capture: bool = False,
    input_text: str | None = None,
    cwd: Path | str | None = None,
    env_extra: dict[str, str] | None = None,
) -> str | int:
    """Run a command; return its stdout when capture is set, else its exit code.

    Raises CalledProcessError when check is set and the command fails.
    """
    e = dict(os.environ, **(env_extra or {}))
    p = subprocess.run(
        [str(a) for a in args],
        check=check,
        cwd=cwd,
        env=e,
        input=input_text,
        stdout=subprocess.PIPE if capture else None,
        text=True,
    )
    return p.stdout if capture else p.returncode


def timed(*args: object, capture: bool = False) -> float:
    """Run a command and return its wall time in seconds."""
    t0 = time.perf_counter_ns()
    run(*args, capture=capture)
    return (time.perf_counter_ns() - t0) / 1e9


def used_bytes(path: Path | str) -> int | None:
    """Return the bytes in use on the file system that holds path, once frees from earlier deletes settle."""
    run("sync")
    last = None
    for _ in range(120):
        s = os.statvfs(path)
        used = (s.f_blocks - s.f_bfree) * s.f_frsize
        if used == last:
            return used
        last = used
        time.sleep(1)
    return last


def du_bytes(path: Path | str) -> int:
    """Return the bytes allocated to the files under path, counting each hard-linked file once."""
    run("sync")
    return int(run("du", "-s", "-x", "-B1", path, capture=True).split()[0])


def bucket_summary(bucket: str, prefix: str) -> tuple[int, int]:
    """Return the number and the bytes of the objects under s3://bucket/prefix/, listed with the host role.

    Raises SystemExit when the listing has no summary.
    """
    out = run("aws", "s3", "ls", "--recursive", "--summarize", f"s3://{bucket}/{prefix}/", capture=True)
    totals = {}
    for line in out.splitlines():
        k, sep, v = line.strip().partition(":")
        if sep and k in {"Total Objects", "Total Size"}:
            totals[k] = int(v)
    if len(totals) != 2:  # noqa: PLR2004 -- the two totals aws s3 ls prints
        msg = "no bucket summary from aws s3 ls"
        raise SystemExit(msg)
    return totals["Total Objects"], totals["Total Size"]


def bucket_bytes(bucket: str, prefix: str) -> int:
    """Return the bytes of the objects under s3://bucket/prefix/, listed with the host role."""
    return bucket_summary(bucket, prefix)[1]


def settled_bytes(bucket: str, prefix: str, quiet: float = 120, timeout: float = 3600) -> int:
    """Return bucket_bytes of prefix once its objects have not changed for quiet seconds.

    Raises SystemExit when they still change after timeout seconds.
    """
    deadline = time.monotonic() + timeout
    last, since = None, time.monotonic()
    while time.monotonic() < deadline:
        now = bucket_summary(bucket, prefix)
        if now != last:
            last, since = now, time.monotonic()
        elif time.monotonic() - since >= quiet:
            return now[1]
        time.sleep(15)
    msg = f"s3://{bucket}/{prefix}/ still changing after {timeout:.0f} s"
    raise SystemExit(msg)


def s3_config() -> Path:
    """Return an AWS CLI config file that runs 64 S3 requests at once, for copies of many small objects."""
    path = WORK / "aws-s3-64.config"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("[default]\ns3 =\n  max_concurrent_requests = 64\n")
    return path


def extract(tar: Path, dest: Path, member: str) -> Path:
    """Unpack tar into dest unless done; return the path of the file named member inside it.

    Raises SystemExit when the archive has no such file.
    """
    if not dest.exists():
        tmp = dest.with_name(dest.name + ".part")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True)
        run("tar", "-xf", tar, "-C", tmp)
        tmp.rename(dest)
    for p in sorted(dest.rglob(member)):
        if p.is_file():
            return p
    msg = f"{tar} has no {member}"
    raise SystemExit(msg)


def env_file(path: Path | str) -> dict[str, str]:
    """Return the KEY=VALUE lines of a file, for a child's environment; the values are never logged."""
    out = {}
    for line in Path(path).read_text().splitlines():
        k, sep, v = line.strip().removeprefix("export ").partition("=")
        if sep and k and not k.startswith("#"):
            out[k.strip()] = v.strip().strip("'\"")
    return out


def tree_bytes(path: Path | str) -> int:
    """Return the apparent size of the files under path."""
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            with contextlib.suppress(FileNotFoundError):
                total += (Path(root) / f).lstat().st_size
    return total


def percentile(values: list[float], p: float) -> float:
    """Return the nearest-rank percentile p of values."""
    s = sorted(values)
    k = max(0, min(len(s) - 1, -(-len(s) * p // 100) - 1))
    return s[int(k)]


def fetch(url: str, dest: Path | str, sha256: str) -> Path:
    """Download url to dest unless dest already holds a file with the given SHA-256; return dest.

    Raises SystemExit when the download has another SHA-256.
    """
    dest = Path(dest)
    if dest.exists() and file_sha256(dest) == sha256:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    log(f"downloading {url}")
    with (
        urllib.request.urlopen(url) as r,  # noqa: S310 -- pinned https URLs, checked by SHA-256 below
        tmp.open("wb") as f,
    ):
        shutil.copyfileobj(r, f)
    got = file_sha256(tmp)
    if got != sha256:
        tmp.unlink()
        msg = f"{url}: SHA-256 {got}, want {sha256}"
        raise SystemExit(msg)
    tmp.rename(dest)
    return dest


def file_sha256(path: Path) -> str:
    """Return the hex SHA-256 of a file's contents."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


class Results:
    """Appends result rows to RESULTS/<name>.csv."""

    def __init__(self, name: str) -> None:
        """Create RESULTS/<name>.csv with its header row unless it exists."""
        RESULTS.mkdir(parents=True, exist_ok=True)
        self.path = RESULTS / f"{name}.csv"
        if not self.path.exists():
            with self.path.open("a", newline="") as f:
                csv.DictWriter(f, RESULT_FIELDS, lineterminator="\n").writeheader()

    def add(  # noqa: PLR0913, PLR0917 -- one argument per CSV column
        self,
        system: str,
        measure: str,
        rep: int,
        value: float,
        unit: str,
        notes: str = "",
    ) -> None:
        """Append one row, written to the file before this returns."""
        with self.path.open("a", newline="") as f:
            csv.DictWriter(f, RESULT_FIELDS, lineterminator="\n").writerow(
                {
                    "system": system,
                    "workspace": WORKSPACE_NAME,
                    "measure": measure,
                    "rep": rep,
                    "value": f"{value:.6g}",
                    "unit": unit,
                    "notes": notes,
                },
            )


def read_json(path: Path | str, default: object = None) -> object:
    """Return the parsed JSON in a file, or default when the file does not exist."""
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return default
