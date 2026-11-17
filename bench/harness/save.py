"""The save measure: after `npm ci` in a fresh branch on a warm host, the time from its last
write until the branch is durable where another host would open it, and the bytes `npm ci`'s
changes added to the store.

  Pando     unmount and `pando detach`, which publishes the branch. Bytes: growth of its bucket
            prefix from before `npm ci`, snapshots taken during the run included.
  tar       tar and zstd of the branch, streamed to the bucket. Bytes: the object.
  Docker    `docker commit` and `docker push` to the registry. Bytes: the pushed image's size
            beyond the workspace image's.
  EBS       unmount, `create-snapshot`, until the snapshot is completed. Bytes: blocks the
            snapshot holds beyond the workspace snapshot (ListChangedBlocks).
  JuiceFS   `sync` of the mount; data is uploaded as files close, and the metadata is in SQLite
            on the host, standing in for a metadata server. Bytes: growth of its bucket prefix.

Rows: save.time in seconds, save.uploaded in bytes, save.npm_ci in seconds. Pando also splits
save.time: save.unmount, save.detach and save.exit (the end of `pando attach`); within the
detach, attach's publish times (save.flush, save.wait, and save.pack and save.ref of the last
upload pass); the bytes the device took during npm ci (save.run_written) and during the unmount
(save.unmount_written); the page cache's dirty bytes before the unmount (save.dirty); and the
upload passes and segment bytes before the detach (save.passes_before, save.bytes_before) and
during it (save.passes, save.bytes); and the reads that waited for the store while the branch
was attached (save.stalls, save.stall in seconds, save.fetched_bytes).
"""

import json
import subprocess
import time

import fork
import start
import workspace
from common import Results, bucket_bytes, env, log, run

NPM_CI = ("web", "npm ci --prefer-offline --no-audit --no-fund")


class Saver:
    def __init__(self, s):
        self.s = s

    def before(self, i):
        """Store bytes before `npm ci`, for systems that measure growth."""
        return 0

    def save(self, i, rep):
        """Makes branch i durable; returns the bytes it added."""
        raise NotImplementedError

    def after(self, i, rep):
        """Closes branch i once its save is measured."""
        self.s.close(i)


class PandoSaver(Saver):
    def before(self, i):
        return bucket_bytes(self.s.bucket, self.s.prefix)

    def save(self, i, rep):
        self.split = self.s.close_timed(i)
        return None

    def after(self, i, rep):
        pass


class TarSaver(Saver):
    def save(self, i, rep):
        key = f"save/b{i}.tar.zst"
        run("bash", "-o", "pipefail", "-c",
            f"tar -C {self.s.root(i)} -cf - . | zstd -q -T0 -3 | aws s3 cp --quiet - s3://{self.s.bucket}/{key}")
        head = json.loads(run("aws", "s3api", "head-object", "--bucket", self.s.bucket, "--key", key, capture=True))
        run("aws", "s3", "rm", "--quiet", f"s3://{self.s.bucket}/{key}")
        return head["ContentLength"]


class DockerSaver(Saver):
    def _size(self, tag):
        repo = env("PANDO_BENCH_REGISTRY").split("/", 1)[1]
        out = run("aws", "ecr", "describe-images", "--region", env("AWS_REGION"), "--repository-name", repo,
                  "--image-ids", f"imageTag={tag}", "--output", "json", capture=True)
        return json.loads(out)["imageDetails"][0]["imageSizeInBytes"]

    def save(self, i, rep):
        image = f"{env('PANDO_BENCH_REGISTRY')}:save-{i}"
        run("docker", "commit", self.s._name(i), image, capture=True)
        run("docker", "push", "-q", image, capture=True)
        return self._size(f"save-{i}") - self._size("workspace")

    def after(self, i, rep):
        repo = env("PANDO_BENCH_REGISTRY").split("/", 1)[1]
        run("aws", "ecr", "batch-delete-image", "--region", env("AWS_REGION"), "--repository-name", repo,
            "--image-ids", f"imageTag=save-{i}", capture=True, check=False)
        run("docker", "rmi", f"{env('PANDO_BENCH_REGISTRY')}:save-{i}", capture=True, check=False)
        self.s.close(i)


class EbsSaver(Saver):
    def save(self, i, rep):
        start.unmount(self.s.root(i))
        snap = self.s.snap
        tags = f"ResourceType=snapshot,Tags=[{{Key=Project,Value={fork.TAG}}},{{Key=Origin,Value={fork.HARNESS_TAG}}}]"
        self.snapshot = snap._aws("create-snapshot", "--volume-id", self.s.volumes[i],
                                  "--description", "pando bench save", "--tag-specifications", tags)["SnapshotId"]
        while snap._aws("describe-snapshots", "--snapshot-ids", self.snapshot)["Snapshots"][0]["State"] != "completed":
            time.sleep(2)
        return None

    def changed(self):
        blocks, size, token = 0, 0, None
        while True:
            args = ["aws", "ebs", "list-changed-blocks", "--region", env("AWS_REGION"), "--output", "json",
                    "--first-snapshot-id", self.s.snap.snapshot, "--second-snapshot-id", self.snapshot]
            if token:
                args += ["--next-token", token]
            page = json.loads(run(*args, capture=True))
            blocks += len(page.get("ChangedBlocks", []))
            size = page.get("BlockSize", size)
            token = page.get("NextToken")
            if not token:
                return blocks * size

    def after(self, i, rep):
        run("aws", "ec2", "delete-snapshot", "--snapshot-id", self.snapshot, "--region", self.s.snap.region, check=False)
        self.s.close(i)


class JuiceFSSaver(Saver):
    def before(self, i):
        return bucket_bytes(self.s.bucket, self.s.volume)

    def save(self, i, rep):
        run("sync", "-f", self.s.root(i))
        return None


SYSTEMS = {
    "pando": (start.Pando, PandoSaver),
    "pando-uncapped": (start.PandoUncapped, PandoSaver),
    "tar": (start.Tar, TarSaver),
    "docker": (start.Docker, DockerSaver),
    "ebs": (start.Ebs, EbsSaver),
    "juicefs": (start.JuiceFS, JuiceFSSaver),
}


def save_once(s, saver, i, rep, out):
    s.open(i)
    before = saver.before(i)
    t, code = start.timed_call(s.exec, i, *NPM_CI)
    out.add(s.name, "save.npm_ci", rep, t, "s", f"rep {rep}, exit {code}")
    if code != 0:
        raise SystemExit(f"npm ci exited with {code}")
    t, uploaded = start.timed_call(saver.save, i, rep)
    if isinstance(saver, EbsSaver):
        uploaded = saver.changed()
    elif uploaded is None:
        uploaded = saver.before(i) - before
    saver.after(i, rep)
    note = f"rep {rep}"
    out.add(s.name, "save.time", rep, t, "s", note)
    out.add(s.name, "save.uploaded", rep, uploaded, "B", note)
    for key, value in getattr(saver, "split", {}).items():
        if key != "total":
            out.add(s.name, f"save.{key}", rep, value, split_unit(key), note)
    log(f"{s.name}: save rep {rep}: {t:.2f} s, {uploaded / 2**20:.1f} MiB")


def split_unit(key):
    if key.startswith("passes") or key == "stalls":
        return "n"
    if "bytes" in key or key.endswith("written") or key == "dirty":
        return "B"
    return "s"


def measure(names, reps):
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
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, SystemExit, OSError, ValueError, KeyError) as e:
            log(f"{s.name}: failed, {e}")
            failed.append(s.name)
        finally:
            log(f"{s.name}: cleaning up")
            s.teardown()
    if failed:
        raise SystemExit(f"save failed for {', '.join(failed)}")


def main_args(sub):
    p = sub.add_parser("save", help="time and bytes to make a branch durable after npm ci")
    p.add_argument("--systems", default=",".join(SYSTEMS), help=f"comma-separated, from {', '.join(SYSTEMS)}")
    p.add_argument("--reps", type=int, default=3)
    return SYSTEMS
