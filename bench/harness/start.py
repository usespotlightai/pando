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

import argparse
import contextlib
import csv
import os
import shutil
import signal
import statistics
import subprocess
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any, TextIO, override

import fork
import workspace
from common import DATA, RESULTS, TOOLS, WORK, Results, env, env_file, extract, fetch, log, run

DIR = WORK / "start"
NPM_CACHE = "/root/.npm"
CMD_TIMEOUT = 1800
SNAPSHOTTER_TIMEOUT = 60
# nerdctl 2.3 and later pull through the containerd transfer service, which drops the labels that
# remote snapshotters need for a lazy pull.
NERDCTL_VERSION = "2.2.1"
NERDCTL_SHA256 = "34144de7f12756aa4b9dc42a907fd95b0c5eb82a63566a650ca10c8abe7a26a0"
SOCI_VERSION = "0.16.1"
SOCI_SHA256 = "2be9f26ad853455515da69885f8906e6aecc65684cc302f8689c25cad8417bc8"
NYDUS_VERSION = "2.4.5"
NYDUS_SHA256 = "1ad7b793a1996577e462b0bf5c1a22e4450b9c04f7e9e99d4b274ccc92f19072"
NYDUS_SNAPSHOTTER_VERSION = "0.16.1"
NYDUS_SNAPSHOTTER_SHA256 = "6e8925642d90896b4dacd903b812548631f6f3dd12495b77bb16bffe533a8f82"

# The 14 command types agents ran most in Spotlight sandboxes, in order: (row, directory, command).
REPLAY = [
    ("git_status", ".", "git status"),
    ("git_log", ".", "git log -50 --stat"),
    ("git_diff", ".", "git diff HEAD~20 --stat"),
    ("git_grep", ".", "git grep -n -e 'async function'"),
    ("grep_go", "data-plane", "grep -rn 'func ' --include='*.go' ."),
    (
        "cat_ts",
        ".",
        "find control-plane/src web/src \\( -name '*.ts' -o -name '*.tsx' \\) -print0 | xargs -0 cat",
    ),
    ("tsc_control_plane", "control-plane", "npx tsc --noEmit --incremental false"),
    ("tsc_web", "web", "npx tsc --noEmit -p tsconfig.app.json"),
    ("oxlint_control_plane", "control-plane", "npx oxlint"),
    ("oxlint_web", "web", "npx oxlint"),
    ("vitest_web", "web", "npx vitest run src/hooks/api"),
    ("vitest_control_plane", "control-plane", "npx vitest run --project unit"),
    ("npm_ci", "web", "npm ci --prefer-offline --no-audit --no-fund"),
    ("git_status_again", ".", "git status"),
]


def shell_env() -> dict[str, str]:
    """Return the environment variables commands in a branch run with."""
    return {
        "PATH": f"{workspace.node_bin()}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME": "/root",
        "CI": "1",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "safe.directory",
        "GIT_CONFIG_VALUE_0": "*",
    }


def drop_caches() -> None:
    """Write dirty pages back, then drop the page cache, dentries and inodes."""
    run("sync")
    with Path("/proc/sys/vm/drop_caches").open("w") as f:
        f.write("3\n")


def imds(path: str) -> str:
    """Return the instance metadata at path, read with an IMDSv2 token."""
    token = (
        urllib.request.urlopen(
            urllib.request.Request(
                "http://169.254.169.254/latest/api/token",
                method="PUT",
                headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
            ),
            timeout=5,
        )
        .read()
        .decode()
    )
    return (
        urllib.request.urlopen(
            urllib.request.Request(
                f"http://169.254.169.254/latest/meta-data/{path}", headers={"X-aws-ec2-metadata-token": token}
            ),
            timeout=5,
        )
        .read()
        .decode()
    )


def unmount(path: Path | str) -> None:
    """Unmount path, killing what still holds it after 10 s.

    Raises CalledProcessError when it stays mounted.
    """
    for _ in range(20):
        if not Path(path).is_mount():
            return
        subprocess.run(["umount", str(path)], stderr=subprocess.DEVNULL, check=False)
        time.sleep(0.5)
    run("fuser", "-km", path, check=False)
    time.sleep(1)
    if Path(path).is_mount():
        run("umount", path)


class Workspace:
    """A way to open branches of the workspace on this host, under the start measures."""

    name = ""
    local_only = False
    max_reps = 1000

    def unavailable(self) -> str | None:
        """Return why this system cannot run here, or None."""
        return None

    def setup(self) -> None:
        """Publish the base where the system keeps it."""

    def empty(self) -> None:
        """Remove the system's local state of the workspace from this host."""

    def open(self, i: int) -> None:
        """Fork branch i and make it usable here."""
        raise NotImplementedError

    def root(self, i: int) -> Path:
        """Return the directory of branch i's workspace on this host."""
        raise NotImplementedError

    def exec(self, i: int, cwd: str, cmd: str) -> int:
        """Run a shell command in branch i, then kill what it left running; return its exit code."""
        p = subprocess.Popen(
            ["bash", "-c", f"{cmd} >/dev/null 2>&1"],
            cwd=self.root(i) / cwd,
            env=dict(os.environ, **shell_env()),
            start_new_session=True,
        )
        try:
            return p.wait(timeout=CMD_TIMEOUT)
        finally:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(p.pid, signal.SIGKILL)
            p.wait()

    def close(self, i: int) -> None:
        """Remove branch i from this host."""

    def warmed(self) -> None:
        """Bring the host to the state it reaches after running a branch of the base."""

    def teardown(self) -> None:
        """Remove everything setup and the branches made, here and where the base is kept."""


class Native(Workspace):
    """A plain copy on an ext4 file system of its own on the local NVMe: the floor for replay."""

    name = "native ext4 (NVMe)"
    local_only = True
    mount = "/native"

    @override
    def unavailable(self) -> str | None:
        return None if Path(self.mount).is_mount() else f"{self.mount} is not mounted"

    @override
    def root(self, i: int) -> Path:
        return Path(self.mount) / f"b{i}"

    @override
    def open(self, i: int) -> None:
        shutil.rmtree(self.root(i), ignore_errors=True)
        run("cp", "-a", f"{workspace.SRC}/.", self.root(i))

    @override
    def close(self, i: int) -> None:
        shutil.rmtree(self.root(i), ignore_errors=True)


class Reflink(Workspace):
    """A reflink copy of the ext4 image on the local XFS, loop-mounted. Local only."""

    name = "XFS reflink (local)"
    local_only = True
    dir = DIR / "reflink"

    @override
    def unavailable(self) -> str | None:
        return fork.Reflink().unavailable()

    @override
    def setup(self) -> None:
        self.teardown()
        self.dir.mkdir(parents=True)

    @override
    def root(self, i: int) -> Path:
        return self.dir / f"m{i}"

    @override
    def open(self, i: int) -> None:
        img = self.dir / f"b{i}.img"
        run("cp", "--reflink=always", workspace.IMAGE, img)
        self.root(i).mkdir(exist_ok=True)
        run("mount", "-o", "loop", img, self.root(i))

    @override
    def close(self, i: int) -> None:
        unmount(self.root(i))
        (self.dir / f"b{i}.img").unlink(missing_ok=True)

    @override
    def teardown(self) -> None:
        for m in self.dir.glob("m*"):
            unmount(m)
        shutil.rmtree(self.dir, ignore_errors=True)


class Download(Workspace):
    """A zstd file in the bucket, downloaded and unpacked per branch; kept on the host once fetched."""

    key = ""
    cache_name = ""

    @override
    def setup(self) -> None:
        self.bucket = env("S3_BUCKET")
        self.dir = DIR / self.cache_name
        self.teardown()
        self.dir.mkdir(parents=True)
        packed = DIR / f"{self.cache_name}.upload.zst"
        log(f"{self.name}: packing and uploading the base")
        self.pack(packed)
        run("aws", "s3", "cp", "--quiet", packed, f"s3://{self.bucket}/{self.key}")
        packed.unlink()

    def pack(self, out: Path) -> None:
        """Write the base, packed, to out."""
        raise NotImplementedError

    def unpack(self, into: str) -> None:
        """Run `into`, a shell command reading the packed base on stdin.

        The base comes from the host's copy if it has one, else from the bucket while keeping a copy.
        """
        cache = self.dir / "base.zst"
        if cache.exists():
            run("bash", "-o", "pipefail", "-c", f"< {cache} {into}")
            return
        run(
            "bash",
            "-o",
            "pipefail",
            "-c",
            f"aws s3 cp --quiet s3://{self.bucket}/{self.key} - | tee {cache}.part | {into}",
        )
        Path(f"{cache}.part").rename(cache)

    @override
    def empty(self) -> None:
        (self.dir / "base.zst").unlink(missing_ok=True)

    @override
    def teardown(self) -> None:
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

    @override
    def pack(self, out: Path) -> None:
        run("bash", "-c", f"tar -C {workspace.SRC} -cf - . | zstd -q -T0 -3 -o {out}")

    @override
    def root(self, i: int) -> Path:
        return self.dir / f"b{i}"

    @override
    def open(self, i: int) -> None:
        self.root(i).mkdir()
        self.unpack(f"zstd -q -dc | tar -x -C {self.root(i)}")

    @override
    def close(self, i: int) -> None:
        shutil.rmtree(self.root(i), ignore_errors=True)


class Image(Download):
    """The ext4 image, zstd-compressed in the bucket, downloaded, decompressed and loop-mounted."""

    name = "image download from S3"
    key = "start/base.img.zst"
    cache_name = "image"

    @override
    def pack(self, out: Path) -> None:
        run("zstd", "-q", "-T0", "-3", "-o", out, workspace.IMAGE)

    @override
    def root(self, i: int) -> Path:
        return self.dir / f"m{i}"

    @override
    def open(self, i: int) -> None:
        img = self.dir / f"b{i}.img"
        self.unpack(f"zstd -q -d -f --sparse -o {img}")
        self.root(i).mkdir(exist_ok=True)
        run("mount", "-o", "loop", img, self.root(i))

    @override
    def close(self, i: int) -> None:
        unmount(self.root(i))
        (self.dir / f"b{i}.img").unlink(missing_ok=True)


class Docker(Workspace):
    """An image of the workspace (Ubuntu with git, plus the workspace) pulled from the registry.

    A branch is a container on overlayfs.
    """

    name = "Docker (overlayfs)"
    base = "ubuntu:26.04"

    @override
    def unavailable(self) -> str | None:
        if subprocess.run(["docker", "info"], capture_output=True, check=False).returncode != 0:
            return "docker is not running"
        return None if env("PANDO_BENCH_REGISTRY", "") else "no registry on this host"

    def _login(self) -> None:
        registry = self.image.split("/")[0]
        pw = run("aws", "ecr", "get-login-password", "--region", env("AWS_REGION"), capture=True)
        subprocess.run(
            ["docker", "login", "--username", "AWS", "--password-stdin", registry],
            input=pw,
            text=True,
            check=True,
            capture_output=True,
        )

    @override
    def setup(self) -> None:
        self.image = f"{env('PANDO_BENCH_REGISTRY')}:workspace"
        self.teardown()
        self._login()
        log(f"{self.name}: building and pushing {self.image}")
        run("docker", "pull", "-q", self.base, capture=True)
        run(
            "docker",
            "run",
            "--name",
            "pando-bench-build",
            self.base,
            "sh",
            "-c",
            "apt-get update -qq && apt-get install -y -qq --no-install-recommends git >/dev/null "
            "&& rm -rf /var/lib/apt/lists/* && mkdir /workspace",
            capture=True,
        )
        run("docker", "commit", "pando-bench-build", "pando-bench/git", capture=True)
        run("docker", "rm", "pando-bench-build", capture=True)
        run("docker", "create", "--name", "pando-bench-build", "pando-bench/git", capture=True)
        run(
            "bash",
            "-o",
            "pipefail",
            "-c",
            f"tar -C {workspace.SRC} -cf - . | docker cp - pando-bench-build:/workspace",
        )
        run("docker", "commit", "pando-bench-build", self.image, capture=True)
        run("docker", "rm", "pando-bench-build", capture=True)
        run("docker", "push", "-q", self.image, capture=True)

    def container(self, i: int) -> str:
        """Return the name of branch i's container."""
        return f"pando-bench-start-{i}"

    @override
    def empty(self) -> None:
        self._remove_containers()
        run("docker", "system", "prune", "-af", capture=True)

    @override
    def open(self, i: int) -> None:
        inspect = subprocess.run(["docker", "image", "inspect", self.image], capture_output=True, check=False)
        if inspect.returncode != 0:
            run("docker", "pull", "-q", self.image, capture=True)
        run(
            "docker",
            "run",
            "-d",
            "--name",
            self.container(i),
            "-v",
            f"{TOOLS}:{TOOLS}:ro",
            "-v",
            f"{NPM_CACHE}:{NPM_CACHE}",
            self.image,
            "sleep",
            "infinity",
            capture=True,
        )

    @override
    def exec(self, i: int, cwd: str, cmd: str) -> int:
        args = ["docker", "exec", "-w", f"/workspace/{cwd}"]
        for k, v in shell_env().items():
            args += ["-e", f"{k}={v}"]
        p = subprocess.run(
            [*args, self.container(i), "bash", "-c", f"{cmd} >/dev/null 2>&1"],
            timeout=CMD_TIMEOUT,
            check=False,
        )
        return p.returncode

    @override
    def close(self, i: int) -> None:
        run("docker", "rm", "-f", self.container(i), capture=True, check=False)

    def _remove_containers(self) -> None:
        ids = run("docker", "ps", "-aq", "--filter", "name=^pando-bench-", capture=True, check=False) or ""
        if ids.split():
            run("docker", "rm", "-f", *ids.split(), capture=True)

    @override
    def teardown(self) -> None:
        self._remove_containers()
        if hasattr(self, "image"):
            repo = env("PANDO_BENCH_REGISTRY").split("/", 1)[1]
            run(
                "aws",
                "ecr",
                "batch-delete-image",
                "--region",
                env("AWS_REGION"),
                "--repository-name",
                repo,
                "--image-ids",
                "imageTag=workspace",
                capture=True,
                check=False,
            )


class Ebs(Workspace):
    """A gp3 volume created from a snapshot of the workspace image and attached to this host."""

    name = "EBS snapshot to volume"

    def __init__(self) -> None:
        """Start with no volumes, over the fork measure's snapshot."""
        self.snap = fork.Ebs()
        self.volumes: dict[int, str] = {}

    @override
    def unavailable(self) -> str | None:
        return self.snap.unavailable()

    @override
    def setup(self) -> None:
        self.snap.setup()
        self.instance = imds("instance-id")
        self.dir = DIR / "ebs"
        self.dir.mkdir(parents=True, exist_ok=True)

    @override
    def root(self, i: int) -> Path:
        return self.dir / f"m{i}"

    def _state(self, v: str) -> dict[str, Any]:
        return self.snap.aws("describe-volumes", "--volume-ids", v)["Volumes"][0]

    @override
    def open(self, i: int) -> None:
        tags = (
            f"ResourceType=volume,Tags=[{{Key=Project,Value={fork.TAG}}},{{Key=Origin,Value={fork.HARNESS_TAG}}},"
            f"{{Key=Name,Value=pando-bench-start-{i}}}]"
        )
        v = self.snap.aws(
            "create-volume",
            "--snapshot-id",
            self.snap.snapshot,
            "--availability-zone",
            self.snap.az,
            "--volume-type",
            "gp3",
            "--tag-specifications",
            tags,
        )["VolumeId"]
        self.volumes[i] = v
        while self._state(v)["State"] != "available":
            time.sleep(0.25)
        self.snap.aws(
            "attach-volume",
            "--volume-id",
            v,
            "--instance-id",
            self.instance,
            "--device",
            f"/dev/sd{'ghijklmnop'[i % 10]}",
        )
        serial = v.replace("-", "")
        dev = None
        while dev is None:
            for line in run("lsblk", "-dn", "-o", "NAME,SERIAL", capture=True).splitlines():
                parts = line.split()
                if len(parts) == fork.LSBLK_COLUMNS and parts[1] == serial:
                    dev = f"/dev/{parts[0]}"
            if dev is None:
                time.sleep(0.1)
        self.root(i).mkdir(exist_ok=True)
        run("mount", dev, self.root(i))

    @override
    def close(self, i: int) -> None:
        unmount(self.root(i))
        v = self.volumes.pop(i, None)
        if v:
            self.snap.aws("detach-volume", "--volume-id", v)
            while self._state(v)["State"] != "available":
                time.sleep(1)
            run("aws", "ec2", "delete-volume", "--volume-id", v, "--region", self.snap.region)

    @override
    def teardown(self) -> None:
        for i in list(self.volumes):
            with contextlib.suppress(subprocess.CalledProcessError, SystemExit):
                self.close(i)
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

    @override
    def unavailable(self) -> str | None:
        return fork.JuiceFS().unavailable()

    @override
    def setup(self) -> None:
        self.bin = fork.JuiceFS().install()
        self.bucket = env("S3_BUCKET")
        self.teardown()
        self.dir.mkdir(parents=True)
        self.mnt.mkdir(parents=True, exist_ok=True)
        self.meta = f"sqlite3://{self.dir}/meta.db"
        run(
            self.bin,
            "format",
            "--storage",
            "s3",
            "--bucket",
            f"https://{self.bucket}.s3.{env('AWS_REGION')}.amazonaws.com",
            "--trash-days",
            "0",
            self.meta,
            self.volume,
            capture=True,
        )
        log(f"{self.name}: copying the workspace in")
        run(
            self.bin,
            "sync",
            "--quiet",
            "--dirs",
            "--links",
            "--perms",
            "--threads",
            "64",
            f"{workspace.SRC}/",
            "jfs://PANDO_BENCH_JFS_META/base/",
            env_extra={"PANDO_BENCH_JFS_META": self.meta},
            capture=True,
        )
        self.ensure_mounted()
        want = workspace.prepare()["files"]
        got = int(run("bash", "-c", f"find {self.mnt / 'base'} -mindepth 1 | wc -l", capture=True))
        if got != want:
            msg = f"JuiceFS holds {got} entries of the workspace's {want}"
            raise SystemExit(msg)

    def ensure_mounted(self) -> None:
        """Mount the volume at mnt unless it is mounted there."""
        if not self.mnt.is_mount():
            run(self.bin, "mount", "-d", "--cache-dir", self.dir / "cache", self.meta, self.mnt, capture=True)

    @override
    def root(self, i: int) -> Path:
        return self.mnt / f"b{i}"

    @override
    def empty(self) -> None:
        unmount(self.mnt)
        shutil.rmtree(self.dir / "cache", ignore_errors=True)

    @override
    def open(self, i: int) -> None:
        self.ensure_mounted()
        run(self.bin, "clone", self.mnt / "base", self.root(i), capture=True)

    @override
    def close(self, i: int) -> None:
        run(self.bin, "rmr", self.root(i), capture=True, check=False)

    @override
    def teardown(self) -> None:
        unmount(self.mnt)
        if hasattr(self, "bucket"):
            run(
                "aws", "s3", "rm", "--recursive", "--quiet", f"s3://{self.bucket}/{self.volume}/", check=False
            )
        shutil.rmtree(self.dir, ignore_errors=True)


class Pando(Workspace):
    """Pando on the bench bucket: the image imported once; a branch is `pando branch`.

    A branch is attached as a ublk device and mounted. The host's cache and the branches' local
    state are its local state.
    """

    name = "Pando"
    prefix = "pando-bench-start"
    dir = DIR / "pando"
    handoff = False

    @override
    def unavailable(self) -> str | None:
        return fork.Pando().unavailable()

    @override
    def setup(self) -> None:
        self.bucket = env("S3_BUCKET")
        self.keys = dict(
            env_file(env("PANDO_ENV_FILE", "/etc/pando-bench.secrets")),
            S3_BUCKET=self.bucket,
            S3_REGION=env("S3_REGION"),
        )
        self.procs: dict[int, tuple[subprocess.Popen[str], TextIO]] = {}
        self.teardown()
        self.dir.mkdir(parents=True)
        log(f"{self.name}: importing the workspace")
        self._pando("import", "base", workspace.IMAGE, capture=True)

    def _args(self) -> list[str]:
        return ["pando", "--remote", f"s3://{self.prefix}", "--cache", str(self.dir / "cache")]

    def _pando(self, *args: object, check: bool = True, capture: bool = False) -> str | int:
        return run(*self._args(), *args, env_extra=self.keys, check=check, capture=capture)

    def device_path(self, i: int) -> str:
        """Return the path of ublk device i."""
        return f"/dev/ublkb{i}"

    @override
    def root(self, i: int) -> Path:
        return self.dir / f"m{i}"

    def _dm(self, i: int) -> str:
        return f"pando-bench-b{i}"

    @override
    def empty(self) -> None:
        for d in ("cache", "state", "images"):
            shutil.rmtree(self.dir / d, ignore_errors=True)

    @override
    def warmed(self) -> None:
        if self.handoff:
            self._pando("base", "base", "--dir", self.dir / "images" / "bases", capture=True)

    def _mounted(self, i: int) -> str:
        return f"/dev/mapper/{self._dm(i)}" if self.handoff else self.device_path(i)

    @override
    def open(self, i: int) -> None:
        self._pando("branch", "base", f"b{i}", capture=True)
        self.attach(i, f"b{i}")
        self.root(i).mkdir(exist_ok=True)
        run("mount", self._mounted(i), self.root(i))

    def attach(self, i: int, name: str, *extra: str) -> None:
        """Start `pando attach` of branch name as device i; return once the device is up.

        Raises SystemExit when the attach exits first.
        """
        out = (self.dir / "attach.log").open("a")
        args = ["attach", name, "--device", self.device_path(i), "--state", str(self.dir / "state"), *extra]
        if self.handoff:
            args += ["--dm", self._dm(i), "--images", str(self.dir / "images")]
        p = subprocess.Popen(
            self._args() + args,
            env=dict(os.environ, **self.keys),
            stdout=subprocess.PIPE,
            stderr=out,
            text=True,
        )
        self.procs[i] = (p, out)
        self.wait_line(i, "attached ")

    def wait_line(self, i: int, prefix: str) -> str:
        """Read device i's attach output up to the first line starting with prefix; return that line.

        Raises SystemExit when the attach exits first.
        """
        p, out = self.procs[i]
        for line in p.stdout:
            out.write(line)
            if line.startswith(prefix):
                out.flush()
                return line
        msg = f"pando attach of device {i} exited with {p.wait()} before printing {prefix.strip()!r}"
        raise SystemExit(msg)

    @override
    def close(self, i: int) -> None:
        unmount(self.root(i))
        p, out = self.procs.pop(i, (None, None))
        if Path(self.device_path(i)).exists():
            dm = ["--dm", self._dm(i)] if self.handoff else []
            self._pando("detach", "--device", self.device_path(i), *dm, capture=True, check=False)
        if p:
            out.write(p.stdout.read())
            if p.wait(timeout=600) != 0:
                log(f"{self.name}: attach of b{i} exited with {p.returncode}")
            out.close()

    @override
    def teardown(self) -> None:
        for i in list(getattr(self, "procs", {})):
            self.close(i)
        if hasattr(self, "bucket"):
            run(
                "aws", "s3", "rm", "--recursive", "--quiet", f"s3://{self.bucket}/{self.prefix}/", check=False
            )
        shutil.rmtree(self.dir, ignore_errors=True)


class PandoHandoff(Pando):
    """Pando as the fast-start path runs it: the ublk device under a device-mapper device.

    The device-mapper device switches to a local image once the branch is local, the image a
    reflink clone of the host's base image of the branch's root. A warm host has built that base
    image.
    """

    name = "Pando (handoff)"
    prefix = "pando-bench-handoff"
    dir = DIR / "pando-handoff"
    handoff = True


class LazyPull(Docker):
    """The Docker row's image, converted for a containerd snapshotter that pulls it lazily.

    A branch is a container run with nerdctl on that snapshotter, which fetches file contents
    from the registry as they are read. Its local state is the images, the containers and the
    snapshotter's cache.
    """

    snapshotter = ""
    tag = ""
    socket = ""
    root = Path()

    @override
    def unavailable(self) -> str | None:
        return super().unavailable() or (None if Path("/dev/fuse").exists() else "no /dev/fuse")

    def install(self) -> None:
        """Install the snapshotter and the conversion tools."""
        raise NotImplementedError

    def daemon(self) -> list[str]:
        """Return the command line of the snapshotter's daemon."""
        raise NotImplementedError

    def convert(self, src: str) -> None:
        """Convert image src to self.lazy and push it to the registry."""
        raise NotImplementedError

    def nerdctl(self, *args: object, check: bool = True) -> str | int:
        """Run nerdctl with args and return its stdout."""
        return run(self.nerdctl_bin, *args, capture=True, check=check, env_extra={"HOME": "/root"})

    @override
    def setup(self) -> None:
        name = f"nerdctl-{NERDCTL_VERSION}-linux-amd64.tar.gz"
        tar = fetch(
            f"https://github.com/containerd/nerdctl/releases/download/v{NERDCTL_VERSION}/{name}",
            TOOLS / name,
            NERDCTL_SHA256,
        )
        self.nerdctl_bin = extract(tar, TOOLS / f"nerdctl-{NERDCTL_VERSION}", "nerdctl")
        self.install()
        self.lazy = f"{env('PANDO_BENCH_REGISTRY')}:{self.tag}"
        super().setup()
        self.restart()
        log(f"{self.name}: converting and pushing {self.lazy}")
        self.convert(self.image)
        self.empty()

    def restart(self) -> None:
        """Stop the snapshotter, then start it again on its current root.

        Raises SystemExit when its socket does not appear within SNAPSHOTTER_TIMEOUT.
        """
        self.stop()
        Path(self.socket).unlink(missing_ok=True)
        self.root.mkdir(parents=True, exist_ok=True)
        with (DIR / f"{self.snapshotter}-snapshotter.log").open("ab") as out:
            self.proc = subprocess.Popen(
                self.daemon(),
                stdout=out,
                stderr=subprocess.STDOUT,
                env=dict(os.environ, HOME="/root"),
                start_new_session=True,
            )
        deadline = time.monotonic() + SNAPSHOTTER_TIMEOUT
        while not Path(self.socket).exists():
            if self.proc.poll() is not None or time.monotonic() > deadline:
                msg = f"{self.snapshotter} snapshotter did not start"
                raise SystemExit(msg)
            time.sleep(0.2)

    def stop(self) -> None:
        """Stop the snapshotter's daemon, if it runs."""
        p = getattr(self, "proc", None)
        if p and p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=SNAPSHOTTER_TIMEOUT)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    @override
    def container(self, i: int) -> str:
        return f"pando-bench-lazy-{i}"

    def _remove_lazy(self) -> None:
        ids = self.nerdctl("ps", "-aq", "--filter", "name=pando-bench-lazy-", check=False) or ""
        if ids.split():
            self.nerdctl("rm", "-f", *ids.split(), check=False)
        self.nerdctl("image", "prune", "-af", check=False)

    @override
    def empty(self) -> None:
        self._remove_lazy()
        self.stop()
        shutil.rmtree(self.root, ignore_errors=True)
        self.restart()

    @override
    def open(self, i: int) -> None:
        inspect = subprocess.run(
            [str(self.nerdctl_bin), "image", "inspect", self.lazy], capture_output=True, check=False
        )
        if inspect.returncode != 0:
            self.nerdctl("--snapshotter", self.snapshotter, "pull", "-q", self.lazy)
        self.nerdctl(
            "--snapshotter",
            self.snapshotter,
            "run",
            "-d",
            "--net",
            "host",
            "--name",
            self.container(i),
            "-v",
            f"{TOOLS}:{TOOLS}:ro",
            "-v",
            f"{NPM_CACHE}:{NPM_CACHE}",
            self.lazy,
            "sleep",
            "infinity",
        )

    @override
    def exec(self, i: int, cwd: str, cmd: str) -> int:
        args = [str(self.nerdctl_bin), "exec", "-w", f"/workspace/{cwd}"]
        for k, v in shell_env().items():
            args += ["-e", f"{k}={v}"]
        p = subprocess.run(
            [*args, self.container(i), "bash", "-c", f"{cmd} >/dev/null 2>&1"],
            timeout=CMD_TIMEOUT,
            check=False,
        )
        return p.returncode

    @override
    def close(self, i: int) -> None:
        self.nerdctl("rm", "-f", self.container(i), check=False)

    @override
    def teardown(self) -> None:
        if hasattr(self, "nerdctl_bin"):
            self._remove_lazy()
        self.stop()
        shutil.rmtree(self.root, ignore_errors=True)
        if hasattr(self, "lazy"):
            run(
                "aws",
                "ecr",
                "batch-delete-image",
                "--region",
                env("AWS_REGION"),
                "--repository-name",
                env("PANDO_BENCH_REGISTRY").split("/", 1)[1],
                "--image-ids",
                f"imageTag={self.tag}",
                capture=True,
                check=False,
            )
        super().teardown()


class Soci(LazyPull):
    """SOCI: the image with a SOCI index (manifest v2), run on the SOCI snapshotter."""

    name = "SOCI lazy pull"
    snapshotter = "soci"
    tag = "workspace-soci"
    socket = "/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock"
    root = DATA / "soci-snapshotter-grpc"

    @override
    def install(self) -> None:
        name = f"soci-snapshotter-{SOCI_VERSION}-linux-amd64.tar.gz"
        tar = fetch(
            f"https://github.com/awslabs/soci-snapshotter/releases/download/v{SOCI_VERSION}/{name}",
            TOOLS / name,
            SOCI_SHA256,
        )
        self.soci = extract(tar, TOOLS / f"soci-{SOCI_VERSION}", "soci")
        self.grpc = extract(tar, TOOLS / f"soci-{SOCI_VERSION}", "soci-snapshotter-grpc")

    @override
    def daemon(self) -> list[str]:
        return [str(self.grpc), "--root", str(self.root), "--address", self.socket]

    @override
    def convert(self, src: str) -> None:
        self.nerdctl("pull", "-q", src)
        run(self.soci, "convert", src, self.lazy, capture=True, env_extra={"HOME": "/root"})
        self.nerdctl("push", "-q", "--all-platforms", self.lazy)


class Nydus(LazyPull):
    """Nydus: the image converted with nydusify, run on the Nydus snapshotter over FUSE (nydusd)."""

    name = "Nydus lazy pull"
    snapshotter = "nydus"
    tag = "workspace-nydus"
    socket = "/run/containerd-nydus/containerd-nydus-grpc.sock"
    root = DATA / "nydus-snapshotter"

    @override
    def install(self) -> None:
        name = f"nydus-static-v{NYDUS_VERSION}-linux-amd64.tgz"
        tar = fetch(
            f"https://github.com/dragonflyoss/nydus/releases/download/v{NYDUS_VERSION}/{name}",
            TOOLS / name,
            NYDUS_SHA256,
        )
        tools = TOOLS / f"nydus-{NYDUS_VERSION}"
        self.nydusd = extract(tar, tools, "nydusd")
        self.builder = extract(tar, tools, "nydus-image")
        self.nydusify = extract(tar, tools, "nydusify")
        name = f"nydus-snapshotter-v{NYDUS_SNAPSHOTTER_VERSION}-linux-amd64.tar.gz"
        tar = fetch(
            f"https://github.com/containerd/nydus-snapshotter/releases/download/v{NYDUS_SNAPSHOTTER_VERSION}/{name}",
            TOOLS / name,
            NYDUS_SNAPSHOTTER_SHA256,
        )
        self.grpc = extract(
            tar, TOOLS / f"nydus-snapshotter-{NYDUS_SNAPSHOTTER_VERSION}", "containerd-nydus-grpc"
        )
        self.config = DIR / "nydus-config.toml"
        self.nydusd_config = DIR / "nydusd-config.json"
        DIR.mkdir(parents=True, exist_ok=True)
        self.nydusd_config.write_text(
            '{"device": {"backend": {"type": "registry", "config": {"timeout": 5, "connect_timeout": 5, '
            '"retry_limit": 2}}, "cache": {"type": "blobcache"}}, "mode": "direct", '
            '"digest_validate": false, "iostats_files": false, "enable_xattr": true, '
            '"amplify_io": 1048576, "fs_prefetch": {"enable": true, "threads_count": 8, '
            '"merging_size": 1048576, "prefetch_all": true}}\n'
        )
        self.config.write_text(
            f'version = 1\nroot = "{self.root}"\naddress = "{self.socket}"\ndaemon_mode = "dedicated"\n\n'
            "[system]\nenable = false\n\n"
            f'[daemon]\nnydusd_config = "{self.nydusd_config}"\nnydusd_path = "{self.nydusd}"\n'
            f'nydusimage_path = "{self.builder}"\nfs_driver = "fusedev"\nrecover_policy = "restart"\n'
            "threads_number = 4\n\n"
            "[remote.auth]\nenable_kubeconfig_keychain = false\nenable_cri_keychain = false\n"
        )

    @override
    def daemon(self) -> list[str]:
        return [str(self.grpc), "--config", str(self.config), "--log-to-stdout"]

    @override
    def convert(self, src: str) -> None:
        work = DIR / "nydusify"
        shutil.rmtree(work, ignore_errors=True)
        work.mkdir(parents=True)
        run(
            self.nydusify,
            "convert",
            "--source",
            src,
            "--target",
            self.lazy,
            "--work-dir",
            work,
            "--nydus-image",
            self.builder,
            capture=True,
            env_extra={"HOME": "/root"},
        )
        shutil.rmtree(work, ignore_errors=True)


class ZeroFS(Workspace):
    """ZeroFS on the bench bucket, ext4 on its NBD export, the server on this host.

    Checkpoints are read-only, so a branch is a full copy of the base export. The server's cache
    is the host's local state of the workspace.
    """

    name = fork.ZeroFS.name
    prefix = "pando-bench-zerofs-start"
    dir = DIR / "zerofs"

    @override
    def unavailable(self) -> str | None:
        return fork.nbd_unavailable()

    @override
    def setup(self) -> None:
        self.bucket = env("S3_BUCKET")
        self.teardown()
        self.server = fork.ZeroFSServer(self.dir / "server", self.bucket, self.prefix)
        self.server.start()
        log(f"{self.name}: copying the workspace image in")
        self.server.put("base", workspace.IMAGE)

    @override
    def root(self, i: int) -> Path:
        return self.dir / f"m{i}"

    @override
    def empty(self) -> None:
        self.server.stop()
        shutil.rmtree(self.server.dir / "cache", ignore_errors=True)
        self.server.start()

    @override
    def open(self, i: int) -> None:
        self.server.copy("base", f"b{i}")
        dev = self.server.connect(f"b{i}", i)
        self.root(i).mkdir(exist_ok=True)
        run("mount", dev, self.root(i))

    @override
    def close(self, i: int) -> None:
        unmount(self.root(i))
        self.server.disconnect(self.server.device(i))
        self.server.remove(f"b{i}")

    @override
    def teardown(self) -> None:
        for m in self.dir.glob("m*"):
            unmount(m)
        server = getattr(self, "server", None)
        if server:
            server.disconnect_all()
            server.stop()
        if hasattr(self, "bucket"):
            run(
                "aws", "s3", "rm", "--recursive", "--quiet", f"s3://{self.bucket}/{self.prefix}/", check=False
            )
        shutil.rmtree(self.dir, ignore_errors=True)


class S3Files(Workspace):
    """Amazon S3 Files, mounted over NFS; a branch is a copy of the base's objects in its bucket.

    The host's only local state is the mount and its page cache: emptying the host unmounts it.
    """

    name = fork.S3Files.name
    max_reps = 3  # every branch copies all of the workspace's objects

    @override
    def unavailable(self) -> str | None:
        return fork.S3FilesMount(DIR / "s3files", "start").unavailable()

    @override
    def setup(self) -> None:
        self.fs = fork.S3FilesMount(DIR / "s3files", f"start-{time.time_ns()}")
        log(f"{self.name}: copying the workspace in")
        self.fs.load_base()

    @override
    def root(self, i: int) -> Path:
        return self.fs.path(f"b{i}")

    @override
    def empty(self) -> None:
        self.fs.unmount()

    @override
    def open(self, i: int) -> None:
        self.fs.ensure_mounted()
        self.fs.branch(f"b{i}")

    @override
    def close(self, i: int) -> None:
        self.fs.remove(f"b{i}")

    @override
    def teardown(self) -> None:
        if hasattr(self, "fs"):
            self.fs.remove()
            self.fs.unmount()


SYSTEMS: dict[str, type[Workspace]] = {
    "native": Native,
    "pando": Pando,
    "pando-handoff": PandoHandoff,
    "tar": Tar,
    "image": Image,
    "docker": Docker,
    "soci": Soci,
    "nydus": Nydus,
    "ebs": Ebs,
    "juicefs": JuiceFS,
    "zerofs": ZeroFS,
    "s3files": S3Files,
    "reflink": Reflink,
}
STARTS = [
    "pando",
    "pando-handoff",
    "tar",
    "image",
    "docker",
    "soci",
    "nydus",
    "ebs",
    "juicefs",
    "zerofs",
    "s3files",
    "reflink",
]
REPLAYS = [
    "native",
    "pando",
    "pando-handoff",
    "image",
    "docker",
    "soci",
    "nydus",
    "ebs",
    "juicefs",
    "zerofs",
    "s3files",
]


def timed_call[T](f: Callable[..., T], *args: object) -> tuple[float, T]:
    """Call f with args; return its wall time in seconds and its result."""
    t0 = time.perf_counter_ns()
    r = f(*args)
    return (time.perf_counter_ns() - t0) / 1e9, r


def start_once(s: Workspace, measure: str, i: int, rep: int, out: Results) -> None:
    """Open branch i of s and run `git status` in it, page cache dropped; write the measure's rows.

    Raises SystemExit when `git status` fails.
    """
    drop_caches()
    t_open, _ = timed_call(s.open, i)
    t_status, code = timed_call(s.exec, i, ".", "git status")
    if code != 0:
        msg = f"git status exited with {code}"
        raise SystemExit(msg)
    note = f"rep {rep}"
    out.add(s.name, f"{measure}.git_status", rep, t_open + t_status, "s", note)
    out.add(s.name, f"{measure}.open", rep, t_open, "s", note)
    out.add(s.name, f"{measure}.status", rep, t_status, "s", note)
    log(f"{s.name}: {measure} rep {rep}: open {t_open:.2f} s, git status {t_status:.2f} s")
    s.close(i)


def warm_up(s: Workspace) -> None:
    """Bring the host from empty to warm by running `git status` in a sibling branch, 0."""
    s.empty()
    s.open(0)
    s.exec(0, ".", "git status")
    s.close(0)
    s.warmed()


def replay_once(s: Workspace, i: int, rep: int, out: Results) -> float:
    """Run REPLAY in a fresh branch i of s, page cache dropped; write its rows, return its total seconds."""
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


def native_replay_median() -> float | None:
    """Return the median replay.seconds of native ext4 in the results so far, or None if there are none."""
    path = RESULTS / "replay.csv"
    if not path.exists():
        return None
    with path.open() as f:
        v = [
            float(r["value"])
            for r in csv.DictReader(f)
            if r["system"] == Native.name and r["measure"] == "replay.seconds"
        ]
    return statistics.median(v) if v else None


def measure_system(kind: str, s: Workspace, reps: int, out: Results) -> list[float]:
    """Run the measure named by kind on s, which is set up; return its replay totals, if any, per rep."""
    reps = min(reps, s.max_reps)
    if kind == "cold":
        for rep in range(1, reps + 1):
            s.empty()
            start_once(s, "cold", rep, rep, out)
        return []
    warm_up(s)
    if kind == "warm":
        for rep in range(1, reps + 1):
            start_once(s, "warm", rep, rep, out)
        return []
    return [replay_once(s, rep, rep, out) for rep in range(1, reps + 1)]


def add_replay_totals(out: Results, totals: dict[str, list[float]]) -> None:
    """Write replay.total, each system's replay totals as a ratio to the native ext4 median."""
    native = native_replay_median()
    if native:
        for name, ts in totals.items():
            for rep, t in enumerate(ts, 1):
                out.add(name, "replay.total", rep, t / native, "x", f"rep {rep}, native {native:.1f} s")
    else:
        log("no native ext4 replay to compare with; replay.total not written")


def measure(kind: str, names: list[str], reps: int) -> None:
    """Run the cold, warm or replay measure, named by kind, on the named systems.

    Raises SystemExit when any system failed.
    """
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
            totals[s.name] = measure_system(kind, s, reps, out)
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            SystemExit,
            OSError,
            ValueError,
        ) as e:
            log(f"{s.name}: failed, {e}")
            failed.append(s.name)
        finally:
            log(f"{s.name}: cleaning up")
            s.teardown()
    if kind == "replay":
        add_replay_totals(out, totals)
    if failed:
        msg = f"{kind} failed for {', '.join(failed)}"
        raise SystemExit(msg)


def main_args(add_parser: Callable[..., argparse.ArgumentParser]) -> None:
    """Add the cold, warm and replay subcommands with add_parser."""
    for kind, default, about in (
        ("cold", STARTS, "empty host to first git status"),
        ("warm", STARTS, "host that ran a sibling to first git status"),
        ("replay", REPLAYS, "the agent workload on a fresh branch"),
    ):
        p = add_parser(kind, help=about)
        p.add_argument(
            "--systems", default=",".join(default), help=f"comma-separated, from {', '.join(SYSTEMS)}"
        )
        p.add_argument("--reps", type=int, default=5 if kind != "replay" else 3)
