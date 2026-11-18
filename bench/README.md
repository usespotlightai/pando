# Benchmarks

This plan defines how to compare Pando with the storage systems used by AI sandboxes: what to
measure, which systems to compare and where to run them. `bench/harness` implements `fork`,
`fork-bytes`, `cold`, `warm`, `replay`, `save`, `fio` and `crash`.

## The question

An agent sandbox needs a writable copy of a large workspace: a repository and everything
installed in it. The host may have none of that data, and the sandbox's changes need to be
preserved when the run ends. Pando provides storage for this workflow; it is not a sandbox.

The benchmarks compare Pando with other storage systems. Every system runs on the same VM,
against the same bucket, with the same agent workload. Compute, network and scheduling stay
the same across systems, isolating differences in storage.

Memory snapshots, which resume running processes, are out of scope because Pando stores disks.

## Workloads

The workspace is the public [Spotlight](https://github.com/usespotlightai/spotlight) repository
at a pinned commit, after its setup command (`npm ci` in `control-plane/` and `web/`). It has
two benchmark sizes:

- **small**: the workspace above, with 1.4 GB and 129k files.
- **large**: `small` with generated files added to reach 10 GB and 400k files, for systems whose
  cost grows with size or file count.

The agent workload runs the 14 command types agents used most often in Spotlight sandboxes,
in the following order on a fresh branch:

| Class | Commands |
|---|---|
| git | `git status`, `git log -50 --stat`, `git diff HEAD~20 --stat` |
| read and search | `git grep`, `grep -rn` over the Go code, `find ... \| xargs cat` over the TypeScript |
| typecheck | `tsc --noEmit` in `control-plane/` and `web/` |
| lint | `oxlint` in `control-plane/` and `web/` |
| tests | `vitest run` on a web library and the control-plane unit tests |
| install | `npm ci` in `web/` |
| git | `git status` again |

## Measures

| ID | Measure | Method |
|---|---|---|
| `fork` | time to make a writable branch | p50 / p99 over up to 1,000 branches of one base; also the first branch vs the last |
| `fork-bytes` | storage per branch | bytes added per branch, and per 5 MiB edit to each of 20 branches |
| `cold` | time to complete the first command on an empty host | fork, then attach/mount, then finish `git status`; empty cache, nothing on the host |
| `warm` | time to complete the first command on a host that ran a sibling | same, with the system's cache or base already on the host |
| `replay` | slowdown while the agent works | the 14 commands, total and per command, as a ratio to native ext4 on local NVMe |
| `save` | time to durable | from the last write to the branch being readable on another host; also bytes uploaded after `npm ci` |
| `fio` | raw I/O | 4 KiB random read QD1 and 4 x QD32, 4 KiB random write 4 x QD32, 1 MiB sequential read and write |
| `crash` | crash survival | `kill -9` of the storage daemon mid-write: failed writes, fsck, file correctness on a fresh host |

Run a measure with `bench/run.sh <measure>` on the host, or pick it in the workflow's `measure`
input (`all` runs every implemented one). For `fork`, each system loads the workspace once, then
makes branches one after another:

| System | A branch is | Timed until | Branches |
|---|---|---|---|
| XFS reflink (local) | `cp --reflink=always` of the ext4 image | the copy returns | 1,000 |
| Docker (overlayfs) | `docker create` from an image of the workspace | the container exists | 1,000 |
| JuiceFS | `juicefs clone` of the workspace directory | the clone returns | 100, as each clone copies the metadata of every entry |
| EBS snapshot to volume | `create-volume` from a snapshot of the workspace | the volume is `available` | 10, as each is a billed volume |
| Pando | `pando branch` of a branch made by `pando import` of the ext4 image | the command returns | 1,000 |
| ZeroFS (NBD) | `cp --sparse=always` of the ext4 image's NBD export file in ZeroFS's 9P mount | `sync` of the copy returns | 10, as each copies the whole device file |
| Amazon S3 Files | a copy of the workspace's files through the NFS mount, 64 files at a time | `sync -f` returns | 3, as each copies every file |

Storage is what the system holds after the branches minus before them, divided by the number
of branches: bytes used on the file system for local systems, metadata plus bucket objects for
JuiceFS, bucket objects for Pando, ZeroFS and S3 Files (local caches are not counted; S3 Files
once its exports to the bucket have settled), and the provisioned size for EBS, which bills that
whatever a volume holds. The edit row writes and fsyncs a 5 MiB file of
random bytes into each of the first 20 branches. For Pando that means attaching the branch as a
ublk device, mounting it, writing, unmounting and running `pando detach`, which publishes the
branch.

`bench/run.sh cold`, `warm` and `replay` open fresh branches and use them. Each system keeps its
base where it would in production, and every branch is opened on the bench host:

| System | Base kept in | Opening a branch |
|---|---|---|
| Pando | the bucket, after `pando import` | `pando branch`, `pando attach` as a ublk device, mount |
| Pando (handoff) | the bucket, after `pando import`; on a warm host also the host's base image (`pando base`) | as Pando, with `--dm` and `--images`: the device switches to a local image, a reflink clone of the base image, once the branch is local |
| tar from S3 | a zstd tar in the bucket | download and extract to the local NVMe |
| image download from S3 | a zstd ext4 image in the bucket | download, decompress, loop-mount |
| Docker (overlayfs) | an image in ECR: Ubuntu with git, plus the workspace | `docker pull`, `docker run` |
| SOCI lazy pull | the Docker image with a SOCI index (v2) in ECR | `nerdctl pull` and `nerdctl run` with the SOCI snapshotter, which fetches file contents on first read |
| Nydus lazy pull | the Docker image converted to Nydus (RAFS) in ECR | `nerdctl pull` and `nerdctl run` with the Nydus snapshotter (FUSE), which fetches file contents on first read |
| EBS snapshot to volume | a snapshot of the ext4 image | `create-volume`, `attach-volume`, mount |
| JuiceFS | the bucket, metadata in SQLite on the host | mount, `juicefs clone` |
| XFS reflink (local) | the ext4 image on the host | reflink copy, loop-mount (`warm` only) |
| ZeroFS (NBD) | the ext4 image as an NBD export in ZeroFS, on the bucket | copy of the export, `nbd-client`, mount |
| Amazon S3 Files | the workspace's files on the S3 Files file system, on its own bucket | NFS mount, copy of the files |

SOCI and Nydus pull with nerdctl 2.2: later versions pull through the containerd transfer service,
which drops the labels a remote snapshotter needs, so the image would be pulled in full.

- **cold** empties the host first: Pando's cache and branch state, the downloaded tar or image,
  Docker's images and the SOCI and Nydus snapshotters' state, ZeroFS's cache, the S3 Files mount,
  JuiceFS's data cache. JuiceFS's metadata stays, as it would on a metadata
  server. The page cache is dropped. The rows are the time to open the branch, the time of the
  first `git status`, and their sum. Pando records each mount's first reads in the bucket and
  prefetches them on the next mount, so its first repetition, with no recording yet, is slower
  than the rest.
- **warm** first opens a sibling branch and runs `git status` in it, so the system's cache,
  image or base is on the host, then drops the page cache and measures the same way. Pando
  (handoff) also builds the host's base image of the root, as `pando follow` would.
- **replay** opens a fresh branch on a warm host, drops the page cache and runs the 14 commands
  of the agent workload, each timed. `replay.total` is the sum as a ratio to native ext4 in the
  same run, on a 64 GiB ext4 file system of its own on the local NVMe (`/native`). Commands run
  in their own process group, killed when the command ends, so nothing they leave behind holds
  the mount when the branch closes.
- **save** runs `npm ci` in a fresh branch on a warm host, then times the step that makes the
  branch durable where another host would open it, and counts the bytes `npm ci`'s changes
  added to the store:

  | System | Made durable by | Bytes counted |
  |---|---|---|
  | Pando | unmount and `pando detach`, which publishes the branch | growth of its bucket prefix since before `npm ci`, snapshots during the run included |
  | tar from S3 | tar and zstd of the branch, streamed to the bucket | the object |
  | Docker (overlayfs) | `docker commit` and `docker push` | the pushed image's size beyond the workspace image's |
  | EBS snapshot to volume | unmount, `create-snapshot`, until it completes | blocks the snapshot holds beyond the workspace snapshot (`ListChangedBlocks`) |
  | JuiceFS | `sync` of the mount; data uploads as each file closes, metadata is in SQLite on the host | growth of its bucket prefix |
  | ZeroFS (NBD) | unmount and `nbd-client -d`, which flush the device to ZeroFS and so to the bucket | growth of its bucket prefix |
  | Amazon S3 Files | `sync -f` of the branch; another host's mount then reads it, and the bucket has it about a minute later | object versions written to the bucket since before `npm ci`, once its exports have settled |

  Pando's time is also split into the unmount, `pando detach` and the end of `pando attach`,
  with the publish's own flush, upload wait, pack and ref times, the bytes the device took
  during `npm ci` and during the unmount, the page cache's dirty bytes before the unmount, and
  what the uploads put before and during the detach (`save.*` rows in `save.csv`).
  `pando-capped` is Pando attached with `--dirty-limit` at 64 MiB, so the kernel writes the
  branch back while `npm ci` runs.

- **fio** opens a fresh branch on a warm host, lays out one 1 GiB file and runs each job for 20 s
  after a 2 s ramp with direct I/O, page cache dropped before each job. Pando (after handoff)
  waits until the device has switched to its local image. Local NVMe is the `/native` ext4 file
  system and EBS gp3 a volume from the workspace snapshot at gp3's baseline (3,000 IOPS,
  125 MB/s).
- **crash** has a writer write 256 KiB files into a fresh branch, fsyncing each file and the
  directory, and log each write as acknowledged or failed (after a failure it tries the next
  file 50 ms later). Five seconds in, the storage daemon gets `kill -9` and is restarted at
  once: Pando with `pando attach --recover` on the same device, JuiceFS with a new mount on the
  same mount point, ZeroFS as a new server on the same cache. For S3 Files the daemon is the
  mount's `efs-proxy`, which the efs-utils watchdog restarts. The writer runs 5 s more once a write
  succeeds again, and the branch closes as usual. A system that acknowledges no write within 120 s
  of the kill has not recovered: its pending I/O is failed where it can be (`nbd-client -d` for
  ZeroFS, whose NBD client does not reconnect to the new server) and it is checked all the same.
  A fresh host is then a new cache and state directory on the same VM, sharing nothing with the
  first: the file system is checked (`e2fsck -fn` for Pando and ZeroFS, `juicefs fsck` for
  JuiceFS; S3 Files has no checker, so it is a new mount after the page cache is dropped) and
  every acknowledged file is read back. The stall is the longest gap between acknowledged writes.

Repeat every run 5 times (3 for `replay`, `save`, `fio` and `crash`), dropping caches between
repetitions, and report the median and range.
Write raw results to `bench/results/<measure>.csv` with the columns
`system,workspace,measure,rep,value,unit,notes`. A plot script converts those results to
`docs/bench/<measure>.svg` for the top-level README to embed.

## Systems

### How sandbox platforms store a workspace

The table below lists each platform's storage design, based on its own docs and engineering
posts, and the implementation used on the bench VM. When a platform's code is unavailable,
the benchmark uses the closest open source system with the same design. Those results are
labelled "X-style", not "X".

| Approach | Used by | Implementation on the bench VM |
|---|---|---|
| A chunked, content-addressed block device served over NBD from object storage, with a copy-on-write overlay | E2B ([infra](https://github.com/e2b-dev/infra), open source) | E2B's block and NBD packages if they run on their own; otherwise ZeroFS over NBD |
| A compressed disk image in S3, downloaded to an NVMe cache on restore | Vercel Sandbox ([snapshot post](https://vercel.com/blog/optimizing-vercel-sandbox-snapshots)) | a zstd-compressed ext4 image in S3, downloaded, decompressed and loop-mounted |
| OCI image layers from a registry on overlayfs; object-storage FUSE mounts for volumes | Daytona ([architecture](https://www.daytona.io/docs/en/architecture/), [volumes](https://www.daytona.io/docs/en/volumes)) | `docker pull` from ECR onto overlayfs, and a lazy pull with the SOCI snapshotter |
| A lazy-loading, content-addressed FUSE file system with tiered caching | Modal (ImageFS, [VM sandbox file system](https://modal.com/blog/vm-sandbox-filesystem)) | Nydus lazy pull, the closest open source design |
| Chunks in object storage, metadata in SQLite replicated with Litestream, NVMe read-through cache, checkpoints as copy-on-write clones | Fly.io Sprites ([design post](https://fly.io/blog/design-and-implementation/)) | JuiceFS with SQLite metadata and `juicefs clone` |
| Reflink copies of disk and memory files on local NVMe | CodeSandbox ([VM cloning post](https://codesandbox.io/blog/cloning-microvms-using-userfaultfd)) | an ext4 image cloned with reflink on XFS (local only) |
| Not published | Morph Cloud, Freestyle, Runloop | none; named, not measured |

### Other storage a sandbox builder would consider

| System | Description | Branch primitive | Notes |
|---|---|---|---|
| native ext4 on NVMe | performance floor | none (full copy) | baseline for `replay` and `fio` |
| tar from S3 | the usual workspace restore | none | download and extract per run |
| EBS snapshot to volume | AWS-native storage | `CreateVolume` from a snapshot | lazy-loaded from S3; also with Fast Snapshot Restore |
| overlayfs, btrfs snapshot, ZFS clone, qcow2 overlay | local copy-on-write | snapshot / clone | local only: no `cold` or `save` across hosts |
| [ZeroFS](https://www.zerofs.net) | S3 as NFS, 9P or NBD block devices | read-only checkpoints; a writable copy is a full copy | closest design to Pando (a block device with ext4 on top, on S3) |
| Amazon S3 Files | NFS mount of an S3 bucket (GA April 2026) | none (copy the files) | writes reach the bucket after about a minute |

The following are excluded:

- Archil is a managed service rather than storage a sandbox builder runs, and branching on your
  own bucket is an enterprise feature, so its branch and save rows could not be measured on equal
  terms.
- Mountpoint for Amazon S3 and goofys cannot modify existing files in place, and Mountpoint cannot
  rename them, so `git` fails on them. s3fs supports both only by rewriting whole objects, which
  makes `git` and `npm ci` impractically slow. Daytona uses FUSE mounts of object storage for
  volumes and external buckets, not for the workspace.
- Firecracker memory forking (zeroboot, forkd, Sandlock, CodeSandbox's userfaultfd) handles
  compute state rather than workspace storage. It could sit on top of Pando.

## The bench VM

The benchmarks use one AWS host in eu-north-1 and a bucket in the same region:

- **instance**: `m8id.4xlarge` (16 vCPU, 64 GiB, 950 GB local NVMe), about $1.11/h on demand.
  Local NVMe holds each system's cache, the native baseline and reflink images (XFS). An attached
  gp3 EBS volume serves the EBS rows. A second, identical host is started only for the cross-host
  `cold` and `save` measures.
- **OS**: Ubuntu 26.04 with `ublk_drv`, `nbd` and `dm-mod` loaded, with no other tenants.
- **cost**: a full run should take 6-8 hours on one host and about 1 hour on the second: SOCI,
  Nydus, ZeroFS and Amazon S3 Files alone took 5.5 hours, most of it S3 Files, which copies and
  deletes all 129k files over NFS for every branch. Compute should cost under $15, plus S3, S3
  Files and ECR requests.

### Creating the hosts

`bench/infra/` is the Terraform for the hosts. It creates:

- host `a` (and host `b` with `second_host = true`) in an availability zone that offers the
  instance type, in the default VPC unless `subnet_id` is set;
- the bench bucket (private, SSE-S3, deleted with its contents on destroy);
- an ECR repository for the workspace image;
- the gp3 volume the EBS rows start from, attached to host `a`;
- an Amazon S3 Files file system on a bucket of its own (versioned, as S3 Files requires), with a
  mount target in the host's subnet;
- an instance role that can use the buckets, the repository and the S3 Files file system, and can
  snapshot, create, attach and delete EBS volumes and snapshots tagged `Project = pando-bench`,
  and nothing else;
- SSH access from the CIDR blocks you list, and all traffic between the hosts.

```bash
cd bench/infra
terraform init
terraform apply -var 'ssh_public_key=ssh-ed25519 AAAA...' -var 'ssh_cidrs=["203.0.113.4/32"]'
```

At boot, each host splits its instance-store NVMe with LVM into 64 GiB of ext4 at `/native` and
the rest as XFS with reflink at `/data`, loads `ublk_drv`, `nbd`, `dm_mod` and `loop`, installs
Docker, fio, the btrfs, ZFS and qemu tools and the AWS CLI, configures containerd for the SOCI and
Nydus snapshotters, lets `fusermount3` mount under `/data` (Ubuntu's AppArmor profile allows only
home, `/mnt`, `/media` and `/tmp`), and turns off background package updates. `/etc/pando-bench.env` holds the host's name, the region, the bucket, the registry URL
and (on host `a`) the EBS volume id; login shells load it. Node.js and the systems under test are
installed by the harness, not here.

Things to know:

- **Cost guard.** Each host powers itself off `max_hours` (default 8) after every boot. A stopped
  host stops billing compute but keeps its EBS volumes, and its `/data` is empty after the next
  start, so copy results to the bucket as they are produced. `terraform destroy` removes
  everything, including the bucket's contents.
- **Pando's credentials.** Pando reads `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY` and cannot
  use the instance role yet. Until it can, pass keys through `PANDO_ENV_FILE`; Terraform does not
  create long-lived keys, so none end up in its state or in the instance user data.
- **Fast Snapshot Restore** is billed per snapshot and availability zone for every hour it is
  on. The harness should turn it on only for its own rows and off again afterwards.

### Running from GitHub Actions

`.github/workflows/bench.yml` runs the bench end to end: it creates the hosts, waits for them to
boot, builds `pando` and copies it, the repository and Pando's store keys to them, runs
`bench/run.sh`, uploads `/data/results` as a workflow artifact and destroys everything, also when
a step fails. It runs only when started by hand (Actions > bench > Run workflow), in the `bench`
environment, so pull requests never reach its AWS role or secrets. Each run makes a fresh SSH key
and opens SSH to the runner's own address only.

`bench/run.sh smoke` checks that the host is ready: kernel modules, XFS with reflink on `/data`,
the bucket through the host role, the registry, Docker, fio, the EBS volume, `pando`,
`nbd-client`, the S3 Files bucket, and Pando's store keys.

One-time setup:

1. Apply `bench/infra/bootstrap/` once with administrator credentials. It creates the role the
   workflow assumes (through GitHub's OIDC login, so no AWS keys are stored in GitHub), the
   bucket for the bench's Terraform state, a permissions boundary for the host role, and the IAM
   user `pando-bench-store` for Pando's keys. Keep its local state file somewhere safe.

   ```bash
   cd bench/infra/bootstrap && terraform init && terraform apply
   ```

2. In the AWS console, create an access key for `pando-bench-store`. It can only use the bench
   buckets.
3. In the repository settings, create the environment `bench` with required reviewers, and add:

   | kind | name | value |
   |---|---|---|
   | variable | `AWS_ROLE_ARN` | bootstrap output `role_arn` |
   | variable | `BENCH_STATE_BUCKET` | bootstrap output `state_bucket` |
   | variable | `BENCH_HOST_BOUNDARY` | bootstrap output `host_permissions_boundary` |
   | variable | `AWS_REGION` | optional, default `eu-north-1` |
   | secret | `S3_ACCESS_KEY_ID` | the access key id from step 2 |
   | secret | `S3_SECRET_ACCESS_KEY` | the secret access key from step 2 |

If a run is killed before its destroy step, start the workflow with `action: destroy` to remove
what it left. The hosts also power themselves off after `max_hours`.
