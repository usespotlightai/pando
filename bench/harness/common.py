"""Shared pieces of the bench harness: settings, commands, timing, results."""

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


def env(name, default=None):
    """A setting from the environment or /etc/pando-bench.env."""
    if name in os.environ:
        return os.environ[name]
    try:
        for line in Path("/etc/pando-bench.env").read_text().splitlines():
            k, _, v = line.partition("=")
            if k == name:
                return v
    except FileNotFoundError:
        pass
    if default is None:
        raise SystemExit(f"{name} is not set")
    return default


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def run(*args, check=True, capture=False, input=None, cwd=None, env_extra=None):
    """Runs a command. Returns stdout when capture is set."""
    e = dict(os.environ, **(env_extra or {}))
    p = subprocess.run([str(a) for a in args], check=check, cwd=cwd, env=e, input=input,
                       stdout=subprocess.PIPE if capture else None, text=True)
    return p.stdout if capture else p.returncode


def timed(*args, **kw):
    """Runs a command and returns its wall time in seconds."""
    t0 = time.perf_counter_ns()
    run(*args, **kw)
    return (time.perf_counter_ns() - t0) / 1e9


def used_bytes(path):
    """Bytes in use on the file system that holds path, once frees from earlier deletes settle."""
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


def du_bytes(path):
    """Bytes allocated to the files under path, counting each hard-linked file once."""
    run("sync")
    return int(run("du", "-s", "-x", "-B1", path, capture=True).split()[0])


def bucket_bytes(bucket, prefix):
    """Bytes of the objects under s3://bucket/prefix/, listed with the host role."""
    out = run("aws", "s3", "ls", "--recursive", "--summarize", f"s3://{bucket}/{prefix}/", capture=True)
    sizes = [line.split(":")[1] for line in out.splitlines() if line.strip().startswith("Total Size:")]
    if not sizes:
        raise SystemExit("no bucket size from aws s3 ls")
    return int(sizes[0])


def env_file(path):
    """The KEY=VALUE lines of a file, for a child's environment; the values are never logged."""
    out = {}
    for line in Path(path).read_text().splitlines():
        k, sep, v = line.strip().removeprefix("export ").partition("=")
        if sep and k and not k.startswith("#"):
            out[k.strip()] = v.strip().strip("'\"")
    return out


def tree_bytes(path):
    """Apparent size of the files under path."""
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.lstat(os.path.join(root, f)).st_size
            except FileNotFoundError:
                pass
    return total


def percentile(values, p):
    """Nearest-rank percentile."""
    s = sorted(values)
    k = max(0, min(len(s) - 1, -(-len(s) * p // 100) - 1))
    return s[int(k)]


def fetch(url, dest, sha256):
    """Downloads url to dest unless dest already holds a file with the given SHA-256."""
    dest = Path(dest)
    if dest.exists() and file_sha256(dest) == sha256:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    log(f"downloading {url}")
    with urllib.request.urlopen(url) as r, open(tmp, "wb") as f:
        shutil.copyfileobj(r, f)
    got = file_sha256(tmp)
    if got != sha256:
        tmp.unlink()
        raise SystemExit(f"{url}: SHA-256 {got}, want {sha256}")
    tmp.rename(dest)
    return dest


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


class Results:
    """Appends result rows to RESULTS/<name>.csv."""

    def __init__(self, name):
        RESULTS.mkdir(parents=True, exist_ok=True)
        self.path = RESULTS / f"{name}.csv"
        new = not self.path.exists()
        self.f = open(self.path, "a", newline="")
        self.w = csv.DictWriter(self.f, RESULT_FIELDS)
        if new:
            self.w.writeheader()

    def add(self, system, measure, rep, value, unit, notes=""):
        self.w.writerow(dict(system=system, workspace=WORKSPACE_NAME, measure=measure, rep=rep,
                             value=f"{value:.6g}", unit=unit, notes=notes))
        self.f.flush()


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return default
