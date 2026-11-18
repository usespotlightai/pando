"""The fork measure: how long a writable branch of the workspace takes, and what it costs to keep.

For each system: load the workspace once, make N branches one after another and time each,
then write 5 MiB into the first 20 of them. Rows (results/fork.csv):

  fork.p50, fork.p99, fork.first, fork.last   seconds; first and last show whether forks slow down
  fork-bytes.per_branch                        storage added per branch, before any edit
  fork-bytes.per_edit                          storage added by one 5 MiB edit to a branch
  fork.time (results/fork-times.csv)           every fork, for distributions

A system that cannot make N branches in reasonable time or money makes fewer; the notes column
says how many.
"""

import contextlib
import json
import os
import secrets
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import Any, override

import workspace
from common import (
    TOOLS,
    WORK,
    Results,
    bucket_bytes,
    du_bytes,
    env,
    env_file,
    extract,
    fetch,
    log,
    percentile,
    run,
    s3_config,
    settled_bytes,
    timed,
    used_bytes,
)

EDIT_BYTES = 5 << 20
EDITED = 20
JUICEFS_VERSION = "1.4.2"
JUICEFS_SHA256 = "79648a0a67145bf42ca4e8336578c13a5bbc9a0bd34df340b3b93dac1e975247"
ZEROFS_VERSION = "2.3.7"
ZEROFS_SHA256 = "8efa26f7abee82e5b720e57ef9c307d0f745a22cd0d81a6d3bbc095db56707c8"
EFS_UTILS_DEB = (
    "https://amazon-efs-utils.aws.com/repo/deb/ubuntu/26.04/pool/main/a/amazon-efs-utils/"
    "amazon-efs-utils-3.4.0-1_amd64.deb"
)
EFS_UTILS_SHA256 = "1c2e4df3400b3b50a2900fdc970ada9957c3da425cd88aa18984be7535b1f7e6"
NBD_DEVICES = 16
SERVER_TIMEOUT = 120
MOUNT_TIMEOUT = 900
TAG = "pando-bench"
HARNESS_TAG = "harness"
LSBLK_COLUMNS = 2


def write_edit(path: Path) -> None:
    """Write EDIT_BYTES of random data to path and fsync it."""
    with path.open("wb") as f:
        f.write(os.urandom(EDIT_BYTES))
        f.flush()
        os.fsync(f.fileno())


class System:
    """A storage system under the fork measure."""

    name = ""
    max_branches = 1000

    def unavailable(self) -> str | None:
        """Return why this system cannot run here, or None."""
        return None

    def setup(self) -> None:
        """Load the workspace into the system."""

    def fork(self, i: int) -> float:
        """Make branch i; return the seconds it took."""
        raise NotImplementedError

    def edit(self, i: int) -> bool:
        """Write 5 MiB into branch i and make it durable; return False when edits do not apply."""
        raise NotImplementedError

    def used(self) -> int:
        """Return the bytes of storage the system holds now."""
        raise NotImplementedError

    def teardown(self) -> None:
        """Remove everything setup and the branches made."""


class Reflink(System):
    """An ext4 image per branch, cloned with reflink on XFS. Local only."""

    name = "XFS reflink (local)"
    dir = WORK / "fork-reflink"
    mnt = WORK / "fork-reflink-mnt"

    @override
    def unavailable(self) -> str | None:
        self.dir.parent.mkdir(parents=True, exist_ok=True)
        mount = run(
            "findmnt", "-n", "-o", "TARGET", "--target", self.dir.parent, capture=True, check=False
        ).strip()
        info = run("xfs_info", mount, capture=True, check=False) if mount else ""
        return None if "reflink=1" in (info or "") else f"{self.dir.parent} is not on XFS with reflink"

    @override
    def setup(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)
        self.dir.mkdir(parents=True)
        self.mnt.mkdir(parents=True, exist_ok=True)

    @override
    def fork(self, i: int) -> float:
        return timed("cp", "--reflink=always", workspace.IMAGE, self.dir / f"b{i}.img")

    @override
    def edit(self, i: int) -> bool:
        run("mount", "-o", "loop", self.dir / f"b{i}.img", self.mnt)
        try:
            write_edit(self.mnt / "pando-bench-edit.bin")
        finally:
            run("umount", self.mnt)
        return True

    @override
    def used(self) -> int:
        return used_bytes(self.dir)

    @override
    def teardown(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)


class Docker(System):
    """A container per branch: an overlayfs upper layer over an image of the workspace."""

    name = "Docker (overlayfs)"
    image = "pando-bench/workspace:base"

    @override
    def unavailable(self) -> str | None:
        ok = subprocess.run(["docker", "info"], capture_output=True, check=False).returncode == 0
        return None if ok else "docker is not running"

    @override
    def setup(self) -> None:
        self.teardown()
        inspect = subprocess.run(["docker", "image", "inspect", self.image], capture_output=True, check=False)
        if inspect.returncode != 0:
            log("importing the workspace as a Docker image")
            tar = subprocess.Popen(["tar", "-C", str(workspace.SRC), "-c", "."], stdout=subprocess.PIPE)
            subprocess.run(
                ["docker", "import", "-", self.image], stdin=tar.stdout, check=True, capture_output=True
            )
            tar.stdout.close()
            if tar.wait() != 0:
                msg = "tar of the workspace failed"
                raise SystemExit(msg)
        self.root = run("docker", "info", "-f", "{{.DockerRootDir}}", capture=True).strip()

    @override
    def fork(self, i: int) -> float:
        # An imported image has no command; create does not check it.
        return timed("docker", "create", "--name", f"pando-bench-{i}", self.image, "/bin/true", capture=True)

    @override
    def edit(self, i: int) -> bool:
        src = WORK / "pando-bench-edit.bin"
        write_edit(src)
        run("docker", "cp", src, f"pando-bench-{i}:/pando-bench-edit.bin")
        src.unlink()
        run("sync")
        return True

    @override
    def used(self) -> int:
        return du_bytes(self.root)

    @override
    def teardown(self) -> None:
        ids = run("docker", "ps", "-aq", "--filter", "name=^pando-bench-", capture=True, check=False) or ""
        if ids.split():
            run("docker", "rm", "-f", *ids.split(), capture=True)


class JuiceFS(System):
    """JuiceFS on the bench bucket, metadata in SQLite on local NVMe; a branch is `juicefs clone`."""

    name = "JuiceFS"
    max_branches = 100  # each clone copies the metadata of all ~130k entries
    dir = WORK / "juicefs"
    mnt = WORK / "juicefs-mnt"
    volume = "pando-bench-fork"

    @override
    def unavailable(self) -> str | None:
        return None if shutil.which("fusermount3") or shutil.which("fusermount") else "FUSE is not installed"

    @override
    def setup(self) -> None:
        self.bin = self.install()
        self.bucket = env("S3_BUCKET")
        self.region = env("AWS_REGION")
        self.teardown()
        self.dir.mkdir(parents=True)
        self.mnt.mkdir(parents=True, exist_ok=True)
        self.meta = f"sqlite3://{self.dir}/meta.db"
        # No keys: JuiceFS uses the host role.
        run(
            self.bin,
            "format",
            "--storage",
            "s3",
            "--bucket",
            f"https://{self.bucket}.s3.{self.region}.amazonaws.com",
            "--trash-days",
            "0",
            self.meta,
            self.volume,
            capture=True,
        )
        run(self.bin, "mount", "-d", "--cache-dir", self.dir / "cache", self.meta, self.mnt, capture=True)
        log("copying the workspace into JuiceFS")
        run("cp", "-a", f"{workspace.SRC}/.", self.mnt / "base")
        run("sync")

    def install(self) -> Path:
        """Return the juicefs binary, first downloading its GitHub release and checking it if needed."""
        b = TOOLS / f"juicefs-{JUICEFS_VERSION}" / "juicefs"
        if not b.exists():
            name = f"juicefs-{JUICEFS_VERSION}-linux-amd64.tar.gz"
            tar = fetch(
                f"https://github.com/juicedata/juicefs/releases/download/v{JUICEFS_VERSION}/{name}",
                TOOLS / name,
                JUICEFS_SHA256,
            )
            b.parent.mkdir(parents=True, exist_ok=True)
            run("tar", "-xzf", tar, "-C", b.parent, "juicefs")
        return b

    @override
    def fork(self, i: int) -> float:
        return timed(self.bin, "clone", self.mnt / "base", self.mnt / f"b{i}", capture=True)

    @override
    def edit(self, i: int) -> bool:
        write_edit(self.mnt / f"b{i}" / "pando-bench-edit.bin")
        return True

    @override
    def used(self) -> int:
        meta = sum(p.stat().st_size for p in self.dir.glob("meta.db*"))
        return meta + bucket_bytes(self.bucket, self.volume)

    @override
    def teardown(self) -> None:
        if self.mnt.is_mount():
            run("umount", self.mnt, check=False)
        if hasattr(self, "bucket"):
            run(
                "aws", "s3", "rm", "--recursive", "--quiet", f"s3://{self.bucket}/{self.volume}/", check=False
            )
        shutil.rmtree(self.dir, ignore_errors=True)


class Ebs(System):
    """A gp3 volume created from a snapshot of the workspace, timed until it is available."""

    name = "EBS snapshot to volume"
    max_branches = 10  # real volumes, billed while they exist

    @override
    def unavailable(self) -> str | None:
        return None if env("PANDO_BENCH_EBS_VOLUME", "") else "no EBS volume on this host"

    def aws(self, *args: object) -> dict[str, Any]:
        """Run `aws ec2` with args in this host's region; return its parsed JSON output."""
        return json.loads(run("aws", "ec2", *args, "--region", self.region, "--output", "json", capture=True))

    @override
    def setup(self) -> None:
        self.region = env("AWS_REGION")
        self.az = self.aws("describe-volumes", "--volume-ids", env("PANDO_BENCH_EBS_VOLUME"))["Volumes"][0][
            "AvailabilityZone"
        ]
        self.volumes = []
        dev = self.device(env("PANDO_BENCH_EBS_VOLUME"))
        if workspace.IMAGE.stat().st_size > int(run("blockdev", "--getsize64", dev, capture=True)):
            msg = f"the workspace image does not fit on {dev}; raise ebs_volume_gib"
            raise SystemExit(msg)
        log(f"writing the workspace image to {dev}")
        run(
            "dd",
            f"if={workspace.IMAGE}",
            f"of={dev}",
            "bs=16M",
            "oflag=direct",
            "conv=sparse,fsync",
            "status=none",
        )
        tags = f"ResourceType=snapshot,Tags=[{{Key=Project,Value={TAG}}},{{Key=Origin,Value={HARNESS_TAG}}}]"
        snap = self.aws(
            "create-snapshot",
            "--volume-id",
            env("PANDO_BENCH_EBS_VOLUME"),
            "--description",
            "pando bench workspace",
            "--tag-specifications",
            tags,
        )
        self.snapshot = snap["SnapshotId"]
        log(f"waiting for snapshot {self.snapshot}")
        while (
            self.aws("describe-snapshots", "--snapshot-ids", self.snapshot)["Snapshots"][0]["State"]
            != "completed"
        ):
            time.sleep(10)
        self.size = snap["VolumeSize"]

    @staticmethod
    def device(volume_id: str) -> str:
        """Return the NVMe device of an attached EBS volume, whose serial is the volume id without the dash.

        Raises SystemExit when the volume is not attached.
        """
        serial = volume_id.replace("-", "")
        for line in run("lsblk", "-dn", "-o", "NAME,SERIAL", capture=True).splitlines():
            parts = line.split()
            if len(parts) == LSBLK_COLUMNS and parts[1] == serial:
                return f"/dev/{parts[0]}"
        msg = f"EBS volume {volume_id} is not attached"
        raise SystemExit(msg)

    @override
    def fork(self, i: int) -> float:
        tags = (
            f"ResourceType=volume,Tags=[{{Key=Project,Value={TAG}}},{{Key=Origin,Value={HARNESS_TAG}}},"
            f"{{Key=Name,Value=pando-bench-fork-{i}}}]"
        )
        t0 = time.perf_counter_ns()
        v = self.aws(
            "create-volume",
            "--snapshot-id",
            self.snapshot,
            "--availability-zone",
            self.az,
            "--volume-type",
            "gp3",
            "--tag-specifications",
            tags,
        )["VolumeId"]
        self.volumes.append(v)
        while self.aws("describe-volumes", "--volume-ids", v)["Volumes"][0]["State"] != "available":
            time.sleep(0.25)
        return (time.perf_counter_ns() - t0) / 1e9

    @override
    def edit(self, i: int) -> bool:
        return False

    @override
    def used(self) -> int:
        # EBS bills every volume at its provisioned size, whatever it holds.
        return len(self.volumes) * (self.size << 30)

    @override
    def teardown(self) -> None:
        for v in getattr(self, "volumes", []):
            run("aws", "ec2", "delete-volume", "--volume-id", v, "--region", self.region, check=False)
        if getattr(self, "snapshot", None):
            run(
                "aws",
                "ec2",
                "delete-snapshot",
                "--snapshot-id",
                self.snapshot,
                "--region",
                self.region,
                check=False,
            )


class Pando(System):
    """Pando on the bench bucket: the workspace image imported once; a branch is `pando branch`.

    An edit attaches the branch as a ublk device, mounts it, writes, and detaches, which
    publishes the branch. Storage is the bucket prefix; the host's cache is not counted.
    """

    name = "Pando"
    prefix = "pando-bench-pando"
    dir = WORK / "pando"
    mnt = WORK / "pando-mnt"
    device = "/dev/ublkb0"

    @override
    def unavailable(self) -> str | None:
        if not shutil.which("pando"):
            return "pando is not on PATH"
        if not Path("/dev/ublk-control").exists():
            return "no ublk driver"
        if not os.access(env("PANDO_ENV_FILE", "/etc/pando-bench.secrets"), os.R_OK):
            return "no store keys in PANDO_ENV_FILE"
        return None

    @override
    def setup(self) -> None:
        self.bucket = env("S3_BUCKET")
        self.keys = dict(
            env_file(env("PANDO_ENV_FILE", "/etc/pando-bench.secrets")),
            S3_BUCKET=self.bucket,
            S3_REGION=env("S3_REGION"),
        )
        self.teardown()
        self.dir.mkdir(parents=True)
        self.mnt.mkdir(parents=True, exist_ok=True)
        log("importing the workspace into Pando")
        self._pando("import", "base", workspace.IMAGE, capture=True)

    def _pando(self, *args: object, check: bool = True, capture: bool = False) -> str | int:
        return run(
            "pando",
            "--remote",
            f"s3://{self.prefix}",
            "--cache",
            self.dir / "cache",
            *args,
            env_extra=self.keys,
            check=check,
            capture=capture,
        )

    @override
    def fork(self, i: int) -> float:
        t0 = time.perf_counter_ns()
        self._pando("branch", "base", f"b{i}", capture=True)
        return (time.perf_counter_ns() - t0) / 1e9

    @override
    def edit(self, i: int) -> bool:
        args = [
            "pando",
            "--remote",
            f"s3://{self.prefix}",
            "--cache",
            str(self.dir / "cache"),
            "attach",
            f"b{i}",
            "--device",
            self.device,
            "--state",
            str(self.dir / "state"),
        ]
        with (self.dir / "attach.log").open("a") as out:
            p = subprocess.Popen(
                args, env=dict(os.environ, **self.keys), stdout=subprocess.PIPE, stderr=out, text=True
            )
            try:
                for line in p.stdout:
                    out.write(line)
                    if line.startswith("attached "):
                        break
                else:
                    msg = f"pando attach b{i} exited with {p.wait()}"
                    raise SystemExit(msg)
                run("mount", self.device, self.mnt)
                try:
                    write_edit(self.mnt / "pando-bench-edit.bin")
                finally:
                    run("umount", self.mnt)
                self._pando("detach", "--device", self.device, capture=True)
                out.write(p.stdout.read())
                if p.wait() != 0:
                    msg = f"pando attach b{i} exited with {p.returncode}"
                    raise SystemExit(msg)
            finally:
                if p.poll() is None:
                    p.kill()
                    p.wait()
        return True

    @override
    def used(self) -> int:
        return bucket_bytes(self.bucket, self.prefix)

    @override
    def teardown(self) -> None:
        if self.mnt.is_mount():
            run("umount", self.mnt, check=False)
        if Path(self.device).exists() and hasattr(self, "keys"):
            self._pando("detach", "--device", self.device, check=False)
        if hasattr(self, "bucket"):
            run(
                "aws", "s3", "rm", "--recursive", "--quiet", f"s3://{self.bucket}/{self.prefix}/", check=False
            )
        shutil.rmtree(self.dir, ignore_errors=True)


def wait_port(port: int, proc: subprocess.Popen[bytes], what: str) -> None:
    """Return once 127.0.0.1:port accepts a connection.

    Raises SystemExit when proc exits first or SERVER_TIMEOUT passes.
    """
    deadline = time.monotonic() + SERVER_TIMEOUT
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            msg = f"{what} exited with {proc.returncode}"
            raise SystemExit(msg)
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
        except OSError:
            time.sleep(0.2)
        else:
            return
    msg = f"{what} did not listen on port {port} within {SERVER_TIMEOUT} s"
    raise SystemExit(msg)


def mounted(path: Path) -> bool:
    """Return whether path is a mount point, from the mount table, without touching the file system."""
    return any(line.split()[1] == str(path) for line in Path("/proc/self/mounts").read_text().splitlines())


class ZeroFSServer:
    """A ZeroFS server on this host over a prefix of the bench bucket, with the host role's credentials.

    Its file system is mounted over 9P at mnt; each file in mnt/.nbd is an NBD export of that
    name. Data written through either is durable in the bucket once flushed or fsynced.
    """

    NINEP_PORT = 5564
    NBD_PORT = 10809

    def __init__(self, base: Path, bucket: str, prefix: str) -> None:
        """Keep the server's files in base; store its data under s3://bucket/prefix."""
        self.dir = base
        self.bucket = bucket
        self.prefix = prefix
        self.mnt = base / "fs"
        self.proc: subprocess.Popen[bytes] | None = None

    @staticmethod
    def install() -> Path:
        """Return the zerofs binary, first downloading its GitHub release and checking it if needed."""
        b = TOOLS / f"zerofs-{ZEROFS_VERSION}" / "zerofs"
        if not b.exists():
            tar = fetch(
                f"https://github.com/Barre/ZeroFS/releases/download/v{ZEROFS_VERSION}/zerofs-pgo-multiplatform.tar.gz",
                TOOLS / f"zerofs-{ZEROFS_VERSION}.tar.gz",
                ZEROFS_SHA256,
            )
            shutil.copy(extract(tar, b.parent / "release", "zerofs-linux-amd64-pgo"), b)
            b.chmod(0o755)
        return b

    def start(self, cache: Path | None = None) -> None:
        """Start the server with its cache in cache (default dir/cache) and mount its file system.

        Raises SystemExit when another process holds its ports or it does not come up.
        """
        self.dir.mkdir(parents=True, exist_ok=True)
        password = self.dir / "password"
        if not password.exists():
            password.write_text(secrets.token_hex(16))
            password.chmod(0o600)
        config = self.dir / "zerofs.toml"
        config.write_text(
            f'[cache]\ndir = "{cache or self.dir / "cache"}"\ndisk_size_gb = 32.0\nmemory_size_gb = 2.0\n\n'
            f'[storage]\nurl = "s3://{self.bucket}/{self.prefix}"\n'
            f'encryption_password = "{password.read_text()}"\n\n'
            f'[aws]\nregion = "{env("AWS_REGION")}"\n\n'
            f'[servers.ninep]\naddresses = ["127.0.0.1:{self.NINEP_PORT}"]\n\n'
            f'[servers.nbd]\naddresses = ["127.0.0.1:{self.NBD_PORT}"]\n'
        )
        config.chmod(0o600)
        for port in (self.NINEP_PORT, self.NBD_PORT):
            with contextlib.suppress(OSError):
                socket.create_connection(("127.0.0.1", port), timeout=1).close()
                msg = f"another process listens on port {port}"
                raise SystemExit(msg)
        with (self.dir / "zerofs.log").open("ab") as out:
            self.proc = subprocess.Popen(
                [str(self.install()), "run", "-c", str(config)], stdout=out, stderr=subprocess.STDOUT
            )
        wait_port(self.NINEP_PORT, self.proc, "zerofs")
        wait_port(self.NBD_PORT, self.proc, "zerofs")
        self.mnt.mkdir(parents=True, exist_ok=True)
        run(
            "mount",
            "-t",
            "9p",
            "-o",
            f"trans=tcp,port={self.NINEP_PORT},version=9p2000.L,msize=1048576,access=user",
            "127.0.0.1",
            self.mnt,
        )
        (self.mnt / ".nbd").mkdir(exist_ok=True)

    def stop(self, *, kill: bool = False) -> float:
        """Unmount the file system and stop the server: SIGTERM, which flushes, or SIGKILL with kill.

        Returns the time.monotonic() of the SIGKILL, or of the stop.
        """
        if kill and self.proc:
            self.proc.kill()
        stopped = time.monotonic()
        if mounted(self.mnt) and (kill or run("umount", self.mnt, check=False) != 0):
            run("umount", "-l", self.mnt, check=False)
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=SERVER_TIMEOUT)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        if self.proc:
            self.proc.wait()
        self.proc = None
        return stopped

    def put(self, name: str, image: Path) -> None:
        """Store image as the export name and flush it to the bucket."""
        run("cp", "--sparse=always", image, self.mnt / ".nbd" / name)
        run("sync", self.mnt / ".nbd" / name)

    def copy(self, src: str, dst: str) -> None:
        """Copy export src to a new export dst, a full copy, and flush it to the bucket."""
        run("cp", "--sparse=always", self.mnt / ".nbd" / src, self.mnt / ".nbd" / dst)
        run("sync", self.mnt / ".nbd" / dst)

    def remove(self, name: str) -> None:
        """Delete export name."""
        (self.mnt / ".nbd" / name).unlink(missing_ok=True)

    @staticmethod
    def device(i: int) -> str:
        """Return the NBD device that serves branch i."""
        return f"/dev/nbd{i % NBD_DEVICES}"

    def connect(self, name: str, i: int) -> str:
        """Connect export name to branch i's NBD device with ZeroFS's recommended options; return it."""
        dev = self.device(i)
        run(
            "nbd-client",
            "127.0.0.1",
            self.NBD_PORT,
            dev,
            "-N",
            name,
            "-persist",
            "-timeout",
            "600",
            "-connections",
            "4",
            capture=True,
        )
        return dev

    @staticmethod
    def disconnect(dev: str) -> None:
        """Disconnect an NBD device; the export has every write the device flushed."""
        run("nbd-client", "-d", dev, capture=True, check=False)

    def disconnect_all(self) -> None:
        """Disconnect every NBD device on this host."""
        for n in range(NBD_DEVICES):
            if Path(f"/sys/block/nbd{n}/pid").exists():
                self.disconnect(f"/dev/nbd{n}")


def nbd_unavailable() -> str | None:
    """Return why this host cannot serve NBD devices, or None."""
    if not Path("/sys/module/nbd").exists():
        return "the nbd module is not loaded"
    return None if shutil.which("nbd-client") else "nbd-client is not installed"


class ZeroFS(System):
    """ZeroFS on the bench bucket, ext4 on its NBD export.

    Checkpoints are read-only, so a branch is a full copy of the base export, made through the
    server's 9P file system.
    """

    name = "ZeroFS (NBD)"
    max_branches = 10  # each branch copies the whole image through the server
    dir = WORK / "zerofs"
    mnt = WORK / "zerofs-mnt"
    prefix = "pando-bench-zerofs-fork"

    @override
    def unavailable(self) -> str | None:
        return nbd_unavailable()

    @override
    def setup(self) -> None:
        self.bucket = env("S3_BUCKET")
        self.teardown()
        self.server = ZeroFSServer(self.dir, self.bucket, self.prefix)
        self.server.start()
        self.mnt.mkdir(parents=True, exist_ok=True)
        log("copying the workspace image into ZeroFS")
        self.server.put("base", workspace.IMAGE)

    @override
    def fork(self, i: int) -> float:
        t0 = time.perf_counter_ns()
        self.server.copy("base", f"b{i}")
        return (time.perf_counter_ns() - t0) / 1e9

    @override
    def edit(self, i: int) -> bool:
        dev = self.server.connect(f"b{i}", i)
        try:
            run("mount", dev, self.mnt)
            try:
                write_edit(self.mnt / "pando-bench-edit.bin")
            finally:
                run("umount", self.mnt)
        finally:
            self.server.disconnect(dev)
        return True

    @override
    def used(self) -> int:
        return bucket_bytes(self.bucket, self.prefix)

    @override
    def teardown(self) -> None:
        if self.mnt.is_mount():
            run("umount", self.mnt, check=False)
        server = getattr(self, "server", None)
        if server:
            server.disconnect_all()
            server.stop()
        if hasattr(self, "bucket"):
            run(
                "aws", "s3", "rm", "--recursive", "--quiet", f"s3://{self.bucket}/{self.prefix}/", check=False
            )
        shutil.rmtree(self.dir, ignore_errors=True)


class S3FilesMount:
    """Amazon S3 Files: the bench's file system over its own versioned bucket, mounted over NFS.

    Its mount helper, from efs-utils, runs an efs-proxy process per mount that carries NFS over
    TLS; the efs-utils watchdog restarts it if it dies. Files written through the mount reach the
    bucket about a minute after their last write. A branch is a copy of the base's objects in the
    bucket, which the file system then imports.
    """

    def __init__(self, mnt: Path, prefix: str) -> None:
        """Mount the file system at mnt and keep branches under prefix/ in its bucket."""
        self.mnt = mnt
        self.prefix = prefix
        self.fs = env("PANDO_BENCH_S3FILES", "")
        self.bucket = env("PANDO_BENCH_S3FILES_BUCKET", "")

    def unavailable(self) -> str | None:
        """Return why this host cannot use S3 Files, or None."""
        return None if self.fs and self.bucket else "no S3 Files file system on this host"

    @staticmethod
    def install() -> None:
        """Install efs-utils, which has the s3files mount helper, from its pinned Ubuntu package.

        Its CloudWatch logging is turned off: the host role has no CloudWatch access.
        """
        if shutil.which("mount.s3files"):
            return
        deb = fetch(EFS_UTILS_DEB, TOOLS / "amazon-efs-utils_3.4.0-1_amd64.deb", EFS_UTILS_SHA256)
        run(
            "apt-get",
            "install",
            "-y",
            "-q",
            deb,
            capture=True,
            env_extra={"DEBIAN_FRONTEND": "noninteractive"},
        )
        conf = Path("/etc/amazon/efs/s3files-utils.conf")
        if conf.exists():
            run("sed", "-i", r"/^\[cloudwatch-log\]/,/^\[/ s/^enabled = true/enabled = false/", conf)

    def ensure_mounted(self) -> None:
        """Mount the file system at mnt unless it is mounted there, retrying while its mount target comes up.

        Raises SystemExit when it cannot be mounted within MOUNT_TIMEOUT.
        """
        if self.mnt.is_mount():
            return
        self.install()
        self.mnt.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + MOUNT_TIMEOUT
        while run("mount", "-t", "s3files", f"{self.fs}:/", self.mnt, check=False) != 0:
            if time.monotonic() > deadline:
                msg = f"could not mount S3 Files {self.fs} within {MOUNT_TIMEOUT} s"
                raise SystemExit(msg)
            time.sleep(15)

    def unmount(self) -> None:
        """Unmount the file system."""
        if self.mnt.is_mount() and run("umount", self.mnt, check=False) != 0:
            run("umount", "-l", self.mnt, check=False)

    def path(self, name: str) -> Path:
        """Return the directory of name (a branch, or base) on the mount."""
        return self.mnt / self.prefix / name

    def load_base(self) -> None:
        """Copy the workspace into base through the mount and wait until the bucket holds all of it."""
        self.ensure_mounted()
        parallel_copy(workspace.SRC, self.path("base"))
        run("sync", "-f", self.path("base"))
        log("waiting for S3 Files to export the base to its bucket")
        settled_bytes(self.bucket, f"{self.prefix}/base")
        self.entries = count_entries(self.path("base"))

    def branch(self, name: str) -> None:
        """Copy the base's objects to name/ in the bucket, then wait until the mount shows all of them.

        Raises SystemExit when the file system has not imported them within MOUNT_TIMEOUT.
        """
        run(
            "aws",
            "s3",
            "cp",
            "--recursive",
            "--quiet",
            f"s3://{self.bucket}/{self.prefix}/base/",
            f"s3://{self.bucket}/{self.prefix}/{name}/",
            env_extra={"AWS_CONFIG_FILE": str(s3_config())},
        )
        deadline = time.monotonic() + MOUNT_TIMEOUT
        while not self.path(name).is_dir() or count_entries(self.path(name)) < self.entries:
            if time.monotonic() > deadline:
                msg = f"S3 Files did not show {name} within {MOUNT_TIMEOUT} s"
                raise SystemExit(msg)
            time.sleep(2)

    def remove(self, name: str = "") -> None:
        """Delete name's objects, or all under prefix/, from the bucket and so from the file system."""
        key = f"{self.prefix}/{name}/" if name else f"{self.prefix}/"
        run(
            "aws",
            "s3",
            "rm",
            "--recursive",
            "--quiet",
            f"s3://{self.bucket}/{key}",
            env_extra={"AWS_CONFIG_FILE": str(s3_config())},
            check=False,
        )

    def used(self) -> int:
        """Return the bytes under prefix/ in the bucket once the file system's exports have settled."""
        return settled_bytes(self.bucket, self.prefix)


def parallel_copy(src: Path, dest: Path, jobs: int = 64) -> None:
    """Copy the tree src into dest with jobs copies at once, for file systems with a round trip per file.

    Directories are made with the default mode and the current time.
    """
    dest.mkdir(parents=True, exist_ok=True)
    xargs = f"xargs -0 -r -P {jobs} -n 256"
    dirs = f'cd "$1" && find . -mindepth 1 -type d -print0 | (cd "$2" && {xargs} mkdir -p --)'
    files = f'cd "$1" && find . ! -type d -print0 | {xargs} cp -a --parents -t "$2" --'
    for script in (dirs, files):
        run("sh", "-c", script, "-", src, dest)


def count_entries(path: Path) -> int:
    """Return the number of files, directories and links under path."""
    return int(run("bash", "-c", f"find {path} -mindepth 1 | wc -l", capture=True))


class S3Files(System):
    """Amazon S3 Files; a branch is a copy of the base's objects in its bucket, then imported."""

    name = "Amazon S3 Files"
    max_branches = 3  # each branch copies every object of the workspace
    mnt = WORK / "s3files-mnt"

    @override
    def unavailable(self) -> str | None:
        return S3FilesMount(self.mnt, "fork").unavailable()

    @override
    def setup(self) -> None:
        self.fs = S3FilesMount(self.mnt, f"fork-{time.time_ns()}")
        log("copying the workspace into S3 Files")
        self.fs.load_base()

    @override
    def fork(self, i: int) -> float:
        t0 = time.perf_counter_ns()
        self.fs.branch(f"b{i}")
        return (time.perf_counter_ns() - t0) / 1e9

    @override
    def edit(self, i: int) -> bool:
        write_edit(self.fs.path(f"b{i}") / "pando-bench-edit.bin")
        return True

    @override
    def used(self) -> int:
        return self.fs.used()

    @override
    def teardown(self) -> None:
        if hasattr(self, "fs"):
            self.fs.remove()
            self.fs.unmount()


SYSTEMS: dict[str, type[System]] = {
    "pando": Pando,
    "reflink": Reflink,
    "docker": Docker,
    "juicefs": JuiceFS,
    "ebs": Ebs,
    "zerofs": ZeroFS,
    "s3files": S3Files,
}


def measure(names: list[str], branches: int, rep: int) -> None:
    """Run the fork measure on the named systems, up to branches branches each, as repetition rep.

    Raises SystemExit when any system failed.
    """
    workspace.prepare()
    summary, times = Results("fork"), Results("fork-times")
    failed = []
    for key in names:
        s = SYSTEMS[key]()
        reason = s.unavailable()
        if reason:
            log(f"{s.name}: skipped, {reason}")
            continue
        n = min(branches, s.max_branches)
        note = f"{n} branches"
        log(f"{s.name}: setting up")
        try:
            s.setup()
            before = s.used()
            took = []
            for i in range(1, n + 1):
                took.append(s.fork(i))
                times.add(s.name, "fork.time", i, took[-1], "s", f"rep {rep}")
            after = s.used()
            log(f"{s.name}: {n} forks, p50 {percentile(took, 50) * 1e3:.1f} ms")
            for m, v in (
                ("fork.p50", percentile(took, 50)),
                ("fork.p99", percentile(took, 99)),
                ("fork.first", took[0]),
                ("fork.last", took[-1]),
            ):
                summary.add(s.name, m, rep, v, "s", note)
            summary.add(s.name, "fork-bytes.per_branch", rep, max(after - before, 0) / n, "B", note)
            k = min(EDITED, n)
            if all(s.edit(i) for i in range(1, k + 1)):
                summary.add(
                    s.name,
                    "fork-bytes.per_edit",
                    rep,
                    max(s.used() - after, 0) / k,
                    "B",
                    f"{k} branches, 5 MiB each",
                )
        except (subprocess.CalledProcessError, SystemExit, OSError) as e:
            log(f"{s.name}: failed, {e}")
            failed.append(s.name)
        finally:
            log(f"{s.name}: cleaning up")
            s.teardown()
    if failed:
        msg = f"fork failed for {', '.join(failed)}"
        raise SystemExit(msg)
