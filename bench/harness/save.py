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
  ZeroFS    unmount and disconnect of the NBD device, whose flush makes the server upload what
            it holds. Bytes: growth of its bucket prefix.
  S3 Files  `sync` of the mount: the file system is durable on its own, and another host mounts
            it directly. Bytes: the object versions it exported to its bucket for the branch,
            once its exports settle (it waits for a file to go a minute without writes).

Rows: save.time in seconds, save.uploaded in bytes, save.npm_ci in seconds. Pando also splits
save.time: save.unmount, save.detach and save.exit (the end of `pando attach`); within the
detach, attach's publish times (save.flush, save.wait, and save.pack and save.ref of the last
upload pass); the bytes the device took during npm ci (save.run_written) and during the unmount
(save.unmount_written); the page cache's dirty bytes before the unmount (save.dirty); and the
upload passes and segment bytes before the detach (save.passes_before, save.bytes_before) and
during it (save.passes, save.bytes); and the reads that waited for the store while the branch
was attached (save.stalls, save.stall in seconds, save.fetched_bytes).
"""

import argparse
import datetime as dt
import json
import subprocess
import time
from collections.abc import Callable
from typing import override

import fork
import start
import workspace
from common import Results, bucket_bytes, env, log, run, settled_bytes

NPM_CI = ("web", "npm ci --prefer-offline --no-audit --no-fund")


class Saver[W: start.Workspace]:
    """How one system makes a branch durable, and how many bytes that added."""

    def __init__(self, s: W) -> None:
        """Save branches of s."""
        self.s = s
        self.split: dict[str, float] = {}

    def before(self) -> int:
        """Return the store bytes before `npm ci`, for systems that measure growth."""
        return 0

    def save(self, i: int) -> int | None:
        """Make branch i durable; return the bytes it added, or None to count them with uploaded."""
        raise NotImplementedError

    def uploaded(self, before: int) -> int:
        """Return the bytes the last save added, given what before returned ahead of `npm ci`."""
        return self.before() - before

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
        self.split = self.s.close_timed(i)

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

    @override
    def uploaded(self, before: int) -> int:
        return self.changed()

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


class ZeroFSSaver(Saver[start.ZeroFS]):
    """Unmount and disconnect the NBD device; the flush makes the server upload what it holds."""

    @override
    def before(self) -> int:
        return bucket_bytes(self.s.bucket, self.s.prefix)

    @override
    def save(self, i: int) -> int | None:
        start.unmount(self.s.root(i))
        self.s.server.disconnect(self.s.server.device(i))


class S3FilesSaver(Saver[start.S3Files]):
    """`sync` of the mount; the bytes are the object versions exported for the branch after `npm ci`."""

    @override
    def before(self) -> int:
        self.since = dt.datetime.now(dt.UTC)
        return 0

    @override
    def save(self, i: int) -> int | None:
        self.branch = i
        run("sync", "-f", self.s.root(i))

    @override
    def uploaded(self, before: int) -> int:
        fs = self.s.fs
        prefix = f"{fs.prefix}/b{self.branch}/"
        settled_bytes(fs.bucket, prefix.rstrip("/"))
        out = run(
            "aws",
            "s3api",
            "list-object-versions",
            "--bucket",
            fs.bucket,
            "--prefix",
            prefix,
            "--query",
            "Versions[].[LastModified, Size]",
            "--output",
            "json",
            capture=True,
        )
        return sum(
            size
            for modified, size in json.loads(out) or []
            if dt.datetime.fromisoformat(modified) >= self.since
        )


SYSTEMS: dict[str, tuple[type[start.Workspace], type[Saver]]] = {
    "pando": (start.Pando, PandoSaver),
    "pando-capped": (start.PandoCapped, PandoSaver),
    "tar": (start.Tar, TarSaver),
    "docker": (start.Docker, DockerSaver),
    "ebs": (start.Ebs, EbsSaver),
    "juicefs": (start.JuiceFS, JuiceFSSaver),
    "zerofs": (start.ZeroFS, ZeroFSSaver),
    "s3files": (start.S3Files, S3FilesSaver),
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
    if uploaded is None:
        uploaded = saver.uploaded(before)
    saver.after(i)
    note = f"rep {rep}"
    out.add(s.name, "save.time", rep, t, "s", note)
    out.add(s.name, "save.uploaded", rep, uploaded, "B", note)
    for key, value in saver.split.items():
        if key != "total":
            out.add(s.name, f"save.{key}", rep, value, split_unit(key), note)
    log(f"{s.name}: save rep {rep}: {t:.2f} s, {uploaded / 2**20:.1f} MiB")


def split_unit(key: str) -> str:
    """Return the unit of the save row named save.<key>."""
    if key.startswith("passes") or key == "stalls":
        return "n"
    if "bytes" in key or key.endswith("written") or key == "dirty":
        return "B"
    return "s"


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
            for rep in range(1, min(reps, s.max_reps) + 1):
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
