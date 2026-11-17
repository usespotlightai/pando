"""The cold, warm and replay measures: a fresh branch of the workspace, opened and used.

  cold     the host holds nothing of the workspace (the system's local state removed, page cache
           dropped). Fork, attach or mount, and finish `git status`.
           Rows: cold.git_status (the total), cold.open, cold.status.
  warm     the same after a sibling branch ran on this host, so the system's cache, image or
           base is local; the page cache is still dropped. Rows: warm.git_status, warm.open,
           warm.status.
  replay   the agent workload (REPLAY) on a fresh branch of a warm host, page cache dropped.
           Rows: replay.<command> and replay.seconds in seconds, and replay.total as a ratio to
           native ext4 in the same run.

Every system keeps its base where it would in production (bucket, registry, snapshot) and opens
branches on this host. Commands run as root with the workspace's Node.js on PATH.
"""

import csv
import os
import shutil
import signal
import statistics
import subprocess
import time
import urllib.request
from pathlib import Path

import fork
import workspace
from common import RESULTS, TOOLS, WORK, Results, env, env_file, log, run

DIR = WORK / "start"
NPM_CACHE = "/root/.npm"
CMD_TIMEOUT = 1800

# The 14 command types agents ran most in Spotlight sandboxes, in order: (row, directory, command).
REPLAY = [
    ("git_status", ".", "git status"),
    ("git_log", ".", "git log -50 --stat"),
    ("git_diff", ".", "git diff HEAD~20 --stat"),
    ("git_grep", ".", "git grep -n -e 'async function'"),
    ("grep_go", "data-plane", "grep -rn 'func ' --include='*.go' ."),
    ("cat_ts", ".", "find control-plane/src web/src \\( -name '*.ts' -o -name '*.tsx' \\) -print0 | xargs -0 cat"),
    ("tsc_control_plane", "control-plane", "npx tsc --noEmit --incremental false"),
    ("tsc_web", "web", "npx tsc --noEmit -p tsconfig.app.json"),
    ("oxlint_control_plane", "control-plane", "npx oxlint"),
    ("oxlint_web", "web", "npx oxlint"),
    ("vitest_web", "web", "npx vitest run src/hooks/api"),
    ("vitest_control_plane", "control-plane", "npx vitest run --project unit"),
    ("npm_ci", "web", "npm ci --prefer-offline --no-audit --no-fund"),
    ("git_status_again", ".", "git status"),
]


def shell_env():
    return {
        "PATH": f"{workspace.node_bin()}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME": "/root",
        "CI": "1",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "safe.directory",
        "GIT_CONFIG_VALUE_0": "*",
    }


def drop_caches():
    run("sync")
    with open("/proc/sys/vm/drop_caches", "w") as f:
        f.write("3\n")


def imds(path):
    token = urllib.request.urlopen(urllib.request.Request(
        "http://169.254.169.254/latest/api/token", method="PUT",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"}), timeout=5).read().decode()
    return urllib.request.urlopen(urllib.request.Request(
        f"http://169.254.169.254/latest/meta-data/{path}",
        headers={"X-aws-ec2-metadata-token": token}), timeout=5).read().decode()


def unmount(path):
    """Unmounts path, killing what still holds it after 10 s. Raises if it stays mounted."""
    for _ in range(20):
        if os.path.ismount(path):
            subprocess.run(["umount", str(path)], stderr=subprocess.DEVNULL)
        if not os.path.ismount(path):
            return
        time.sleep(0.5)
    run("fuser", "-km", path, check=False)
    time.sleep(1)
    if os.path.ismount(path):
        run("umount", path)


class Workspace:
    name = ""
    local_only = False

    def unavailable(self):
        """Why this system cannot run here, or None."""
        return None

    def setup(self):
        """Publishes the base where the system keeps it."""

    def empty(self):
        """Removes the system's local state of the workspace from this host."""

    def open(self, i):
        """Forks branch i and makes it usable here."""
        raise NotImplementedError

    def root(self, i):
        """The directory of branch i's workspace on this host."""
        raise NotImplementedError

    def exec(self, i, cwd, cmd):
        """Runs a shell command in branch i, then kills what it left running; returns its exit code."""
        p = subprocess.Popen(["bash", "-c", f"{cmd} >/dev/null 2>&1"], cwd=self.root(i) / cwd,
                             env=dict(os.environ, **shell_env()), start_new_session=True)
        try:
            return p.wait(timeout=CMD_TIMEOUT)
        finally:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            p.wait()

    def close(self, i):
        pass

    def warmed(self):
        """Brings the host to the state it reaches after running a branch of the base."""

    def teardown(self):
        pass


class Native(Workspace):
    """A plain copy on an ext4 file system of its own on the local NVMe: the floor for replay."""

    name = "native ext4 (NVMe)"
    local_only = True
    mount = "/native"

    def unavailable(self):
        return None if os.path.ismount(self.mount) else f"{self.mount} is not mounted"

    def root(self, i):
        return Path(self.mount) / f"b{i}"

    def open(self, i):
        shutil.rmtree(self.root(i), ignore_errors=True)
        run("cp", "-a", f"{workspace.SRC}/.", self.root(i))

    def close(self, i):
        shutil.rmtree(self.root(i), ignore_errors=True)


class Reflink(Workspace):
    """A reflink copy of the ext4 image on the local XFS, loop-mounted. Local only."""

    name = "XFS reflink (local)"
    local_only = True
    dir = DIR / "reflink"

    def unavailable(self):
        return fork.Reflink().unavailable()

    def setup(self):
        self.teardown()
        self.dir.mkdir(parents=True)

    def root(self, i):
        return self.dir / f"m{i}"

    def open(self, i):
        img = self.dir / f"b{i}.img"
        run("cp", "--reflink=always", workspace.IMAGE, img)
        self.root(i).mkdir(exist_ok=True)
        run("mount", "-o", "loop", img, self.root(i))

    def close(self, i):
        unmount(self.root(i))
        (self.dir / f"b{i}.img").unlink(missing_ok=True)

    def teardown(self):
        for m in self.dir.glob("m*"):
            unmount(m)
        shutil.rmtree(self.dir, ignore_errors=True)


class Download(Workspace):
    """A zstd file in the bucket, downloaded and unpacked per branch; kept on the host once fetched."""

    key = ""
    cache_name = ""

    def setup(self):
        self.bucket = env("S3_BUCKET")
        self.dir = DIR / self.cache_name
        self.teardown()
        self.dir.mkdir(parents=True)
        packed = DIR / f"{self.cache_name}.upload.zst"
        log(f"{self.name}: packing and uploading the base")
        self.pack(packed)
        run("aws", "s3", "cp", "--quiet", packed, f"s3://{self.bucket}/{self.key}")
        packed.unlink()

    def unpack(self, into):
        """Runs `into`, a shell command reading the packed base on stdin, from the host's copy if it
        has one, else from the bucket while keeping a copy."""
        cache = self.dir / "base.zst"
        if cache.exists():
            run("bash", "-o", "pipefail", "-c", f"< {cache} {into}")
            return
        run("bash", "-o", "pipefail", "-c",
            f"aws s3 cp --quiet s3://{self.bucket}/{self.key} - | tee {cache}.part | {into}")
        os.rename(f"{cache}.part", cache)

    def empty(self):
        (self.dir / "base.zst").unlink(missing_ok=True)

    def teardown(self):
        if hasattr(self, "bucket"):
            run("aws", "s3", "rm", "--quiet", f"s3://{self.bucket}/{self.key}", check=False)
        if hasattr(self, "dir"):
            for m in self.dir.glob("m*"):
                unmount(m)
            shutil.rmtree(self.dir, ignore_errors=True)


class Tar(Download):
    """tar of the workspace, zstd-compressed in the bucket, extracted to the local NVMe."""

    name = "tar from S3"
    key = "start/workspace.tar.zst"
    cache_name = "tar"

    def pack(self, out):
        run("bash", "-c", f"tar -C {workspace.SRC} -cf - . | zstd -q -T0 -3 -o {out}")

    def root(self, i):
        return self.dir / f"b{i}"

    def open(self, i):
        self.root(i).mkdir()
        self.unpack(f"zstd -q -dc | tar -x -C {self.root(i)}")

    def close(self, i):
        shutil.rmtree(self.root(i), ignore_errors=True)


class Image(Download):
    """The ext4 image, zstd-compressed in the bucket, downloaded, decompressed and loop-mounted."""

    name = "image download from S3"
    key = "start/base.img.zst"
    cache_name = "image"

    def pack(self, out):
        run("zstd", "-q", "-T0", "-3", "-o", out, workspace.IMAGE)

    def root(self, i):
        return self.dir / f"m{i}"

    def open(self, i):
        img = self.dir / f"b{i}.img"
        self.unpack(f"zstd -q -d -f --sparse -o {img}")
        self.root(i).mkdir(exist_ok=True)
        run("mount", "-o", "loop", img, self.root(i))

    def close(self, i):
        unmount(self.root(i))
        (self.dir / f"b{i}.img").unlink(missing_ok=True)


class Docker(Workspace):
    """An image of the workspace (Ubuntu with git, plus the workspace) pulled from the registry;
    a branch is a container on overlayfs."""

    name = "Docker (overlayfs)"
    base = "ubuntu:26.04"

    def unavailable(self):
        if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
            return "docker is not running"
        return None if env("PANDO_BENCH_REGISTRY", "") else "no registry on this host"

    def _login(self):
        registry = self.image.split("/")[0]
        pw = run("aws", "ecr", "get-login-password", "--region", env("AWS_REGION"), capture=True)
        subprocess.run(["docker", "login", "--username", "AWS", "--password-stdin", registry],
                       input=pw, text=True, check=True, capture_output=True)

    def setup(self):
        self.image = f"{env('PANDO_BENCH_REGISTRY')}:workspace"
        self.teardown()
        self._login()
        log(f"{self.name}: building and pushing {self.image}")
        run("docker", "pull", "-q", self.base, capture=True)
        run("docker", "run", "--name", "pando-bench-build", self.base, "sh", "-c",
            "apt-get update -qq && apt-get install -y -qq --no-install-recommends git >/dev/null "
            "&& rm -rf /var/lib/apt/lists/* && mkdir /workspace", capture=True)
        run("docker", "commit", "pando-bench-build", "pando-bench/git", capture=True)
        run("docker", "rm", "pando-bench-build", capture=True)
        run("docker", "create", "--name", "pando-bench-build", "pando-bench/git", capture=True)
        run("bash", "-o", "pipefail", "-c", f"tar -C {workspace.SRC} -cf - . | docker cp - pando-bench-build:/workspace")
        run("docker", "commit", "pando-bench-build", self.image, capture=True)
        run("docker", "rm", "pando-bench-build", capture=True)
        run("docker", "push", "-q", self.image, capture=True)

    def _name(self, i):
        return f"pando-bench-start-{i}"

    def empty(self):
        self._remove_containers()
        run("docker", "system", "prune", "-af", capture=True)

    def open(self, i):
        if subprocess.run(["docker", "image", "inspect", self.image], capture_output=True).returncode != 0:
            run("docker", "pull", "-q", self.image, capture=True)
        run("docker", "run", "-d", "--name", self._name(i), "-v", f"{TOOLS}:{TOOLS}:ro",
            "-v", f"{NPM_CACHE}:{NPM_CACHE}", self.image, "sleep", "infinity", capture=True)

    def exec(self, i, cwd, cmd):
        args = ["docker", "exec", "-w", f"/workspace/{cwd}"]
        for k, v in shell_env().items():
            args += ["-e", f"{k}={v}"]
        p = subprocess.run(args + [self._name(i), "bash", "-c", f"{cmd} >/dev/null 2>&1"], timeout=CMD_TIMEOUT)
        return p.returncode

    def close(self, i):
        run("docker", "rm", "-f", self._name(i), capture=True, check=False)

    def _remove_containers(self):
        ids = run("docker", "ps", "-aq", "--filter", "name=^pando-bench-", capture=True, check=False) or ""
        if ids.split():
            run("docker", "rm", "-f", *ids.split(), capture=True)

    def teardown(self):
        self._remove_containers()
        if hasattr(self, "image"):
            repo = env("PANDO_BENCH_REGISTRY").split("/", 1)[1]
            run("aws", "ecr", "batch-delete-image", "--region", env("AWS_REGION"), "--repository-name", repo,
                "--image-ids", "imageTag=workspace", capture=True, check=False)


class Ebs(Workspace):
    """A gp3 volume created from a snapshot of the workspace image and attached to this host."""

    name = "EBS snapshot to volume"

    def __init__(self):
        self.snap = fork.Ebs()
        self.volumes = {}

    def unavailable(self):
        return self.snap.unavailable()

    def setup(self):
        self.snap.setup()
        self.instance = imds("instance-id")
        self.dir = DIR / "ebs"
        self.dir.mkdir(parents=True, exist_ok=True)

    def root(self, i):
        return self.dir / f"m{i}"

    def _state(self, v):
        return self.snap._aws("describe-volumes", "--volume-ids", v)["Volumes"][0]

    def open(self, i):
        tags = (f"ResourceType=volume,Tags=[{{Key=Project,Value={fork.TAG}}},{{Key=Origin,Value={fork.HARNESS_TAG}}},"
                f"{{Key=Name,Value=pando-bench-start-{i}}}]")
        v = self.snap._aws("create-volume", "--snapshot-id", self.snap.snapshot, "--availability-zone", self.snap.az,
                           "--volume-type", "gp3", "--tag-specifications", tags)["VolumeId"]
        self.volumes[i] = v
        while self._state(v)["State"] != "available":
            time.sleep(0.25)
        self.snap._aws("attach-volume", "--volume-id", v, "--instance-id", self.instance,
                       "--device", f"/dev/sd{'ghijklmnop'[i % 10]}")
        serial = v.replace("-", "")
        dev = None
        while dev is None:
            for line in run("lsblk", "-dn", "-o", "NAME,SERIAL", capture=True).splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[1] == serial:
                    dev = f"/dev/{parts[0]}"
            if dev is None:
                time.sleep(0.1)
        self.root(i).mkdir(exist_ok=True)
        run("mount", dev, self.root(i))

    def close(self, i):
        unmount(self.root(i))
        v = self.volumes.pop(i, None)
        if v:
            self.snap._aws("detach-volume", "--volume-id", v)
            while self._state(v)["State"] != "available":
                time.sleep(1)
            run("aws", "ec2", "delete-volume", "--volume-id", v, "--region", self.snap.region)

    def teardown(self):
        for i in list(self.volumes):
            try:
                self.close(i)
            except (subprocess.CalledProcessError, SystemExit):
                pass
        self.snap.teardown()


class JuiceFS(Workspace):
    """JuiceFS on the bench bucket with SQLite metadata on the host; a branch is `juicefs clone`.

    The metadata stays on the host when the host is emptied, as it would on a metadata server;
    only the data cache goes.
    """

    name = "JuiceFS"
    volume = "pando-bench-juicefs"
    dir = DIR / "juicefs"
    mnt = DIR / "juicefs-mnt"

    def unavailable(self):
        return fork.JuiceFS().unavailable()

    def setup(self):
        self.bin = fork.JuiceFS()._install()
        self.bucket = env("S3_BUCKET")
        self.teardown()
        self.dir.mkdir(parents=True)
        self.mnt.mkdir(parents=True, exist_ok=True)
        self.meta = f"sqlite3://{self.dir}/meta.db"
        run(self.bin, "format", "--storage", "s3",
            "--bucket", f"https://{self.bucket}.s3.{env('AWS_REGION')}.amazonaws.com",
            "--trash-days", "0", self.meta, self.volume, capture=True)
        log(f"{self.name}: copying the workspace in")
        run(self.bin, "sync", "--quiet", "--dirs", "--links", "--perms", "--threads", "64", f"{workspace.SRC}/",
            f"jfs://PANDO_BENCH_JFS_META/base/", env_extra={"PANDO_BENCH_JFS_META": self.meta}, capture=True)
        self._mount()
        want = workspace.prepare()["files"]
        got = int(run("bash", "-c", f"find {self.mnt / 'base'} -mindepth 1 | wc -l", capture=True))
        if got != want:
            raise SystemExit(f"JuiceFS holds {got} entries of the workspace's {want}")

    def _mount(self):
        if not os.path.ismount(self.mnt):
            run(self.bin, "mount", "-d", "--cache-dir", self.dir / "cache", self.meta, self.mnt, capture=True)

    def root(self, i):
        return self.mnt / f"b{i}"

    def empty(self):
        unmount(self.mnt)
        shutil.rmtree(self.dir / "cache", ignore_errors=True)

    def open(self, i):
        self._mount()
        run(self.bin, "clone", self.mnt / "base", self.root(i), capture=True)

    def close(self, i):
        run(self.bin, "rmr", self.root(i), capture=True, check=False)

    def teardown(self):
        unmount(self.mnt)
        if hasattr(self, "bucket"):
            run("aws", "s3", "rm", "--recursive", "--quiet", f"s3://{self.bucket}/{self.volume}/", check=False)
        shutil.rmtree(self.dir, ignore_errors=True)


class Pando(Workspace):
    """Pando on the bench bucket: the image imported once; a branch is `pando branch`, attached as
    a ublk device and mounted. The host's cache and the branches' local state are its local state."""

    name = "Pando"
    prefix = "pando-bench-start"
    dir = DIR / "pando"
    handoff = False

    def unavailable(self):
        return fork.Pando().unavailable()

    def setup(self):
        self.bucket = env("S3_BUCKET")
        self.keys = dict(env_file(env("PANDO_ENV_FILE", "/etc/pando-bench.secrets")),
                         S3_BUCKET=self.bucket, S3_REGION=env("S3_REGION"))
        self.procs = {}
        self.teardown()
        self.dir.mkdir(parents=True)
        log(f"{self.name}: importing the workspace")
        self._pando("import", "base", workspace.IMAGE, capture=True)

    def _args(self):
        return ["pando", "--remote", f"s3://{self.prefix}", "--cache", str(self.dir / "cache")]

    def _pando(self, *args, **kw):
        return run(*self._args(), *args, env_extra=self.keys, **kw)

    def _device(self, i):
        return f"/dev/ublkb{i}"

    def root(self, i):
        return self.dir / f"m{i}"

    def _dm(self, i):
        return f"pando-bench-b{i}"

    def empty(self):
        for d in ("cache", "state", "images"):
            shutil.rmtree(self.dir / d, ignore_errors=True)

    def warmed(self):
        if self.handoff:
            self._pando("base", "base", "--dir", self.dir / "images" / "bases", capture=True)

    def _mounted(self, i):
        return f"/dev/mapper/{self._dm(i)}" if self.handoff else self._device(i)

    def open(self, i):
        self._pando("branch", "base", f"b{i}", capture=True)
        self.attach(i, f"b{i}")
        self.root(i).mkdir(exist_ok=True)
        run("mount", self._mounted(i), self.root(i))

    def attach(self, i, name, *extra):
        """Starts `pando attach` of branch name as device i; returns once the device is up."""
        out = open(self.dir / "attach.log", "a")
        args = ["attach", name, "--device", self._device(i), "--state", str(self.dir / "state"), *extra]
        if self.handoff:
            args += ["--dm", self._dm(i), "--images", str(self.dir / "images")]
        p = subprocess.Popen(self._args() + args,
                             env=dict(os.environ, **self.keys), stdout=subprocess.PIPE, stderr=out, text=True)
        self.procs[i] = (p, out)
        self.wait_line(i, "attached ")

    def wait_line(self, i, prefix):
        """Reads device i's attach output up to the first line starting with prefix."""
        p, out = self.procs[i]
        for line in p.stdout:
            out.write(line)
            if line.startswith(prefix):
                out.flush()
                return line
        raise SystemExit(f"pando attach of device {i} exited with {p.wait()} before printing {prefix.strip()!r}")

    def close(self, i):
        self.close_timed(i)

    def close_timed(self, i):
        """Closes branch i. Returns the seconds the unmount, `pando detach` and the end of
        `pando attach` took, the `key=value` publish times attach printed, the page cache's
        dirty bytes before the unmount, and the bytes the device took before and during it."""
        times = {}
        live = os.path.exists(self._device(i))
        if live:
            times["dirty"] = dirty_bytes()
            times["run_written"] = self.written(i)
        t0 = time.perf_counter()
        unmount(self.root(i))
        times["unmount"] = time.perf_counter() - t0
        if live:
            times["unmount_written"] = self.written(i) - times["run_written"]
        p, out = self.procs.pop(i, (None, None))
        t0 = time.perf_counter()
        if os.path.exists(self._device(i)):
            dm = ["--dm", self._dm(i)] if self.handoff else []
            self._pando("detach", "--device", self._device(i), *dm, capture=True, check=False)
        times["detach"] = time.perf_counter() - t0
        t0 = time.perf_counter()
        if p:
            rest = p.stdout.read()
            out.write(rest)
            if p.wait(timeout=600) != 0:
                log(f"{self.name}: attach of b{i} exited with {p.returncode}")
            out.close()
            times.update(publish_times(rest))
        times["exit"] = time.perf_counter() - t0
        return times

    def written(self, i):
        """Bytes written to device i since it was created."""
        return int(Path(f"/sys/block/{Path(self._device(i)).name}/stat").read_text().split()[6]) * 512

    def teardown(self):
        for i in list(getattr(self, "procs", {})):
            self.close(i)
        if hasattr(self, "bucket"):
            run("aws", "s3", "rm", "--recursive", "--quiet", f"s3://{self.bucket}/{self.prefix}/", check=False)
        shutil.rmtree(self.dir, ignore_errors=True)


def publish_times(text):
    """The values of attach's `publish times` line, as numbers."""
    for line in text.splitlines():
        head, sep, rest = line.partition(": ")
        if sep and head.startswith("publish times "):
            return {k: float(v) for k, _, v in (kv.partition("=") for kv in rest.split())}
    return {}


def dirty_bytes():
    """Dirty and writeback bytes in the page cache, host-wide."""
    fields = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    return sum(int(fields[k].split()[0]) * 1024 for k in ("Dirty", "Writeback"))


class PandoHandoff(Pando):
    """Pando as the fast-start path runs it: the ublk device under a device-mapper device that
    switches to a local image once the branch is local, the image a reflink clone of the host's
    base image of the branch's root. A warm host has built that base image."""

    name = "Pando (handoff)"
    prefix = "pando-bench-handoff"
    dir = DIR / "pando-handoff"
    handoff = True


SYSTEMS = {"native": Native, "pando": Pando, "pando-handoff": PandoHandoff, "tar": Tar, "image": Image, "docker": Docker, "ebs": Ebs,
           "juicefs": JuiceFS, "reflink": Reflink}
STARTS = ["pando", "pando-handoff", "tar", "image", "docker", "ebs", "juicefs", "reflink"]
REPLAYS = ["native", "pando", "pando-handoff", "image", "docker", "ebs", "juicefs"]


def timed_call(f, *args):
    t0 = time.perf_counter_ns()
    r = f(*args)
    return (time.perf_counter_ns() - t0) / 1e9, r


def start_once(s, measure, i, rep, out):
    drop_caches()
    t_open, _ = timed_call(s.open, i)
    t_status, code = timed_call(s.exec, i, ".", "git status")
    if code != 0:
        raise SystemExit(f"git status exited with {code}")
    note = f"rep {rep}"
    out.add(s.name, f"{measure}.git_status", rep, t_open + t_status, "s", note)
    out.add(s.name, f"{measure}.open", rep, t_open, "s", note)
    out.add(s.name, f"{measure}.status", rep, t_status, "s", note)
    log(f"{s.name}: {measure} rep {rep}: open {t_open:.2f} s, git status {t_status:.2f} s")
    s.close(i)


def warm_up(s):
    s.empty()
    s.open(0)
    s.exec(0, ".", "git status")
    s.close(0)
    s.warmed()


def replay_once(s, i, rep, out):
    s.open(i)
    drop_caches()
    total = 0.0
    for row, cwd, cmd in REPLAY:
        t, code = timed_call(s.exec, i, cwd, cmd)
        total += t
        out.add(s.name, f"replay.{row}", rep, t, "s", f"rep {rep}, exit {code}")
        log(f"{s.name}: replay rep {rep}: {row} {t:.2f} s (exit {code})")
    out.add(s.name, "replay.seconds", rep, total, "s", f"rep {rep}")
    s.close(i)
    return total


def native_replay_median():
    path = RESULTS / "replay.csv"
    if not path.exists():
        return None
    v = [float(r["value"]) for r in csv.DictReader(open(path))
         if r["system"] == Native.name and r["measure"] == "replay.seconds"]
    return statistics.median(v) if v else None


def measure(kind, names, reps):
    workspace.prepare()
    out = Results(kind)
    failed = []
    totals = {}
    for key in names:
        s = SYSTEMS[key]()
        if kind == "cold" and s.local_only:
            log(f"{s.name}: skipped, local only")
            continue
        reason = s.unavailable()
        if reason:
            log(f"{s.name}: skipped, {reason}")
            continue
        log(f"{s.name}: setting up")
        try:
            s.setup()
            if kind == "cold":
                for rep in range(1, reps + 1):
                    s.empty()
                    start_once(s, "cold", rep, rep, out)
            elif kind == "warm":
                warm_up(s)
                for rep in range(1, reps + 1):
                    start_once(s, "warm", rep, rep, out)
            else:
                warm_up(s)
                totals[s.name] = [replay_once(s, rep, rep, out) for rep in range(1, reps + 1)]
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, SystemExit, OSError, ValueError) as e:
            log(f"{s.name}: failed, {e}")
            failed.append(s.name)
        finally:
            log(f"{s.name}: cleaning up")
            s.teardown()
    if kind == "replay":
        native = native_replay_median()
        if native:
            for name, ts in totals.items():
                for rep, t in enumerate(ts, 1):
                    out.add(name, "replay.total", rep, t / native, "x", f"rep {rep}, native {native:.1f} s")
        else:
            log("no native ext4 replay to compare with; replay.total not written")
    if failed:
        raise SystemExit(f"{kind} failed for {', '.join(failed)}")


def main_args(sub):
    for kind, default, about in (("cold", STARTS, "empty host to first git status"),
                                 ("warm", STARTS, "host that ran a sibling to first git status"),
                                 ("replay", REPLAYS, "the agent workload on a fresh branch")):
        p = sub.add_parser(kind, help=about)
        p.add_argument("--systems", default=",".join(default), help=f"comma-separated, from {', '.join(SYSTEMS)}")
        p.add_argument("--reps", type=int, default=5 if kind != "replay" else 3)

