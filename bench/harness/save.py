"""The save measure: how long a branch takes to become durable after `npm ci`, and what it adds.

After `npm ci` in a fresh branch on a warm host: the time from its last write until the branch
is durable where another host would open it, and the bytes `npm ci`'s changes added to the store.

  Pando     unmount and `pando detach`, which publishes the branch. Bytes: growth of its bucket
            prefix from before `npm ci`, snapshots taken during the run included.
  tar       tar and zstd of the branch, streamed to the bucket. Bytes: the object.
  Docker    `docker commit` and `docker push` to the registry. Bytes: the pushed image's size
            beyond the workspace image's.
  EBS       unmount, `create-snapshot`, until the snapshot is completed. Bytes: blocks the
            snapshot holds beyond the workspace snapshot (ListChangedBlocks).
  JuiceFS   `sync` of the mount; data is uploaded as files close, and the metadata is in SQLite
            on the host, standing in for a metadata server. Bytes: growth of its bucket prefix.

Rows: save.time in seconds, save.uploaded in bytes, save.npm_ci in seconds.
"""

import argparse
import json
import subprocess
import time
from collections.abc import Callable
from typing import override

import fork
import start
import workspace
from common import Results, bucket_bytes, env, log, run

NPM_CI = ("web", "npm ci --prefer-offline --no-audit --no-fund")


class Saver[W: start.Workspace]:
    """How one system makes a branch durable, and how many bytes that added."""

    def __init__(self, s: W) -> None:
        """Save branches of s."""
        self.s = s

    def before(self) -> int:
        """Return the store bytes before `npm ci`, for systems that measure growth."""
        return 0

    def save(self, i: int) -> int | None:
        """Make branch i durable; return the bytes it added, or None to measure growth with before."""
        raise NotImplementedError

    def after(self, i: int) -> None:
        """Close branch i once its save is measured."""
        self.s.close(i)


class PandoSaver(Saver[start.Pando]):
    """Unmount and `pando detach`, which publishes the branch."""

    @override
    def before(self) -> int:
        return bucket_bytes(self.s.bucket, self.s.prefix)

    @override
    def save(self, i: int) -> int | None:
        self.s.close(i)

    @override
    def after(self, i: int) -> None:
        pass


class TarSaver(Saver[start.Tar]):
    """tar and zstd of the branch, streamed to the bucket."""

    @override
    def save(self, i: int) -> int | None:
        key = f"save/b{i}.tar.zst"
        run(
            "bash",
            "-o",
            "pipefail",
            "-c",
            f"tar -C {self.s.root(i)} -cf - . | zstd -q -T0 -3 "
            f"| aws s3 cp --quiet - s3://{self.s.bucket}/{key}",
        )
        head = json.loads(
            run("aws", "s3api", "head-object", "--bucket", self.s.bucket, "--key", key, capture=True)
        )
        run("aws", "s3", "rm", "--quiet", f"s3://{self.s.bucket}/{key}")
        return head["ContentLength"]


class DockerSaver(Saver[start.Docker]):
    """`docker commit` and `docker push` to the registry."""

    def _size(self, tag: str) -> int:
        repo = env("PANDO_BENCH_REGISTRY").split("/", 1)[1]
        out = run(
            "aws",
            "ecr",
            "describe-images",
            "--region",
            env("AWS_REGION"),
            "--repository-name",
            repo,
            "--image-ids",
            f"imageTag={tag}",
            "--output",
            "json",
            capture=True,
        )
        return json.loads(out)["imageDetails"][0]["imageSizeInBytes"]

    @override
    def save(self, i: int) -> int | None:
        image = f"{env('PANDO_BENCH_REGISTRY')}:save-{i}"
        run("docker", "commit", self.s.container(i), image, capture=True)
        run("docker", "push", "-q", image, capture=True)
        return self._size(f"save-{i}") - self._size("workspace")

    @override
    def after(self, i: int) -> None:
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
            f"imageTag=save-{i}",
            capture=True,
            check=False,
        )
        run("docker", "rmi", f"{env('PANDO_BENCH_REGISTRY')}:save-{i}", capture=True, check=False)
        self.s.close(i)


class EbsSaver(Saver[start.Ebs]):
    """Unmount, `create-snapshot`, until the snapshot is completed."""

    @override
    def save(self, i: int) -> int | None:
        start.unmount(self.s.root(i))
        snap = self.s.snap
        tags = (
            f"ResourceType=snapshot,Tags=[{{Key=Project,Value={fork.TAG}}},"
            f"{{Key=Origin,Value={fork.HARNESS_TAG}}}]"
        )
        self.snapshot = snap.aws(
            "create-snapshot",
            "--volume-id",
            self.s.volumes[i],
            "--description",
            "pando bench save",
            "--tag-specifications",
            tags,
        )["SnapshotId"]
        while (
            snap.aws("describe-snapshots", "--snapshot-ids", self.snapshot)["Snapshots"][0]["State"]
            != "completed"
        ):
            time.sleep(2)

    def changed(self) -> int:
        """Return the bytes of the blocks the last saved snapshot holds beyond the workspace snapshot."""
        blocks, size, token = 0, 0, None
        while True:
            args = [
                "aws",
                "ebs",
                "list-changed-blocks",
                "--region",
                env("AWS_REGION"),
                "--output",
                "json",
                "--first-snapshot-id",
                self.s.snap.snapshot,
                "--second-snapshot-id",
                self.snapshot,
            ]
            if token:
                args += ["--next-token", token]
            page = json.loads(run(*args, capture=True))
            blocks += len(page.get("ChangedBlocks", []))
            size = page.get("BlockSize", size)
            token = page.get("NextToken")
            if not token:
                return blocks * size

    @override
    def after(self, i: int) -> None:
        run(
            "aws",
            "ec2",
            "delete-snapshot",
            "--snapshot-id",
            self.snapshot,
            "--region",
            self.s.snap.region,
            check=False,
        )
        self.s.close(i)


class JuiceFSSaver(Saver[start.JuiceFS]):
    """`sync` of the mount; data is uploaded as files close."""

    @override
    def before(self) -> int:
        return bucket_bytes(self.s.bucket, self.s.volume)

    @override
    def save(self, i: int) -> int | None:
        run("sync", "-f", self.s.root(i))


SYSTEMS: dict[str, tuple[type[start.Workspace], type[Saver]]] = {
    "pando": (start.Pando, PandoSaver),
    "tar": (start.Tar, TarSaver),
    "docker": (start.Docker, DockerSaver),
    "ebs": (start.Ebs, EbsSaver),
    "juicefs": (start.JuiceFS, JuiceFSSaver),
}


def save_once(s: start.Workspace, saver: Saver, i: int, rep: int, out: Results) -> None:
    """Run `npm ci` in a fresh branch i of s, then save it with saver; write the rows.

    Raises SystemExit when `npm ci` fails.
    """
    s.open(i)
    before = saver.before()
    t, code = start.timed_call(s.exec, i, *NPM_CI)
    out.add(s.name, "save.npm_ci", rep, t, "s", f"rep {rep}, exit {code}")
    if code != 0:
        msg = f"npm ci exited with {code}"
        raise SystemExit(msg)
    t, uploaded = start.timed_call(saver.save, i)
    if isinstance(saver, EbsSaver):
        uploaded = saver.changed()
    elif uploaded is None:
        uploaded = saver.before() - before
    saver.after(i)
    note = f"rep {rep}"
    out.add(s.name, "save.time", rep, t, "s", note)
    out.add(s.name, "save.uploaded", rep, uploaded, "B", note)
    log(f"{s.name}: save rep {rep}: {t:.2f} s, {uploaded / 2**20:.1f} MiB")


def measure(names: list[str], reps: int) -> None:
    """Run the save measure on the named systems.

    Raises SystemExit when any system failed.
    """
    workspace.prepare()
    out = Results("save")
    failed = []
    for key in names:
        cls, saver_cls = SYSTEMS[key]
        s = cls()
        reason = s.unavailable()
        if reason:
            log(f"{s.name}: skipped, {reason}")
            continue
        log(f"{s.name}: setting up")
        try:
            s.setup()
            start.warm_up(s)
            saver = saver_cls(s)
            for rep in range(1, reps + 1):
                save_once(s, saver, rep, rep, out)
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
        msg = f"save failed for {', '.join(failed)}"
        raise SystemExit(msg)


def main_args(add_parser: Callable[..., argparse.ArgumentParser]) -> dict[str, object]:
    """Add the save subcommand with add_parser; return its systems by key."""
    p = add_parser("save", help="time and bytes to make a branch durable after npm ci")
    p.add_argument("--systems", default=",".join(SYSTEMS), help=f"comma-separated, from {', '.join(SYSTEMS)}")
    p.add_argument("--reps", type=int, default=3)
    return SYSTEMS
