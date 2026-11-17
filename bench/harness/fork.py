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

import json
import os
import shutil
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
    fetch,
    log,
    percentile,
    run,
    timed,
    used_bytes,
)

EDIT_BYTES = 5 << 20
EDITED = 20
JUICEFS_VERSION = "1.4.2"
JUICEFS_SHA256 = "79648a0a67145bf42ca4e8336578c13a5bbc9a0bd34df340b3b93dac1e975247"
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


SYSTEMS: dict[str, type[System]] = {
    "pando": Pando,
    "reflink": Reflink,
    "docker": Docker,
    "juicefs": JuiceFS,
    "ebs": Ebs,
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
