# Benchmarks

This plan defines how to compare Pando with the storage systems used by AI sandboxes: what to
measure, which systems to compare and where to run them. The harness has not been written yet.

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

`bench/harness` implements `fork` and `fork-bytes` so far (`bench/run.sh fork`, or the `fork`
choice of the workflow's `measure` input). Each system loads the workspace once, then makes
branches one after another:

| System | A branch is | Timed until | Branches |
|---|---|---|---|
| XFS reflink (local) | `cp --reflink=always` of the ext4 image | the copy returns | 1,000 |
| Docker (overlayfs) | `docker create` from an image of the workspace | the container exists | 1,000 |
| JuiceFS | `juicefs clone` of the workspace directory | the clone returns | 100, as each clone copies the metadata of every entry |
| EBS snapshot to volume | `create-volume` from a snapshot of the workspace | the volume is `available` | 10, as each is a billed volume |
| Pando | `pando branch` of a branch made by `pando import` of the ext4 image | the command returns | 1,000 |

Storage is what the system holds after the branches minus before them, divided by the number
of branches: bytes used on the file system for local systems, metadata plus bucket objects for
JuiceFS, bucket objects for Pando (its local cache is not counted), and the provisioned size for
EBS, which bills that whatever a volume holds. The edit row writes and fsyncs a 5 MiB file of
random bytes into each of the first 20 branches. For Pando that means attaching the branch as a
ublk device, mounting it, writing, unmounting and running `pando detach`, which publishes the
branch.

Repeat every run 5 times, dropping caches between repetitions, and report the median and range.
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
| A lazy-loading, content-addressed FUSE file system with tiered caching | Modal (ImageFS, [VM sandbox file system](https://modal.com/blog/vm-sandbox-filesystem)) | Nydus or eStargz lazy pull, the closest open source design |
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
| Amazon S3 Files | NFS mount of an S3 bucket (GA April 2026) | none (copy objects) | writes reach the bucket after about a minute |
| [Archil](https://archil.com) | managed S3-backed POSIX disk for agents | branches / checkpoints | mounts on our VM; branching on our own bucket is an enterprise feature |

The following are excluded:

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
- **cost**: a full run should take 4-6 hours on one host and about 1 hour on the second. Compute
  should cost under $15, plus S3 and ECR requests.

### Creating the hosts

`bench/infra/` is the Terraform for the hosts. It creates:

- host `a` (and host `b` with `second_host = true`) in an availability zone that offers the
  instance type, in the default VPC unless `subnet_id` is set;
- the bench bucket (private, SSE-S3, deleted with its contents on destroy);
- an ECR repository for the workspace image;
- the gp3 volume the EBS rows start from, attached to host `a`;
- an instance role that can use the bucket and the repository, and can snapshot, create, attach
  and delete EBS volumes and snapshots tagged `Project = pando-bench`, and nothing else;
- SSH access from the CIDR blocks you list, and all traffic between the hosts.

```bash
cd bench/infra
terraform init
terraform apply -var 'ssh_public_key=ssh-ed25519 AAAA...' -var 'ssh_cidrs=["203.0.113.4/32"]'
```

At boot, each host formats its instance-store NVMe as XFS with reflink and mounts it at `/data`,
loads `ublk_drv`, `nbd`, `dm_mod` and `loop`, installs Docker, fio, the btrfs, ZFS and qemu tools
and the AWS CLI, and turns off background package updates. `/etc/pando-bench.env` holds the
host's name, the region, the bucket, the registry URL and (on host `a`) the EBS volume id; login
shells load it. Node.js and the systems under test are installed by the harness, not here.

Things to know:

- **Cost guard.** Each host powers itself off `max_hours` (default 8) after every boot. A stopped
  host stops billing compute but keeps its EBS volumes, and its `/data` is empty after the next
  start, so copy results to the bucket as they are produced. `terraform destroy` removes
  everything, including the bucket's contents.
- **Pando's credentials.** Pando reads `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY` and cannot
  use the instance role yet. Until it can, pass keys through `PANDO_ENV_FILE`; Terraform does not
  create long-lived keys, so none end up in its state or in the instance user data.
- **Not created here.** Amazon S3 Files and Archil are set up in their own consoles, against the
  bench bucket.
- **Fast Snapshot Restore** is billed per snapshot and availability zone for every hour it is
  on. The harness should turn it on only for its own rows and off again afterwards.

### Running from GitHub Actions

`.github/workflows/bench.yml` runs the bench end to end: it creates the hosts, waits for them to
boot, copies the repository and Pando's store keys to them, runs `bench/run.sh`, uploads
`/data/results` as a workflow artifact and destroys everything, also when a step fails. It runs
only when started by hand (Actions > bench > Run workflow), in the `bench` environment, so pull
requests never reach its AWS role or secrets. Each run makes a fresh SSH key and opens SSH to the
runner's own address only.

Until the measures are written, `bench/run.sh` runs `smoke`, which checks that the host is
ready: kernel modules, XFS with reflink on `/data`, the bucket through the host role, the
registry, Docker, fio, the EBS volume, and Pando's store keys.

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
