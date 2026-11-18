# AgentCore Runtime V2 probe

This probe checks whether Pando can run inside an Amazon Bedrock AgentCore Runtime V2 microVM. V2
runs each session in a Firecracker microVM restored from a snapshot. The probe answers four
questions:

- Which block frontends does the guest kernel offer, and which is best?
  - ublk is Pando's frontend on EC2. It needs `/dev/ublk-control`.
  - FUSE and loop is Pando's `attach --fuse`. It needs `/dev/fuse` and loop devices.
  - NBD, FUSE passthrough, and loop over a localhost NFS mount are the other options.
  - device-mapper (`linear` and `era`) decides whether the handoff to a local image works.
- Can a FUSE file back a loop device that carries ext4?
- Do the instance credentials come through the metadata service?
- How does the platform end a session?

The runtime serves the AgentCore HTTP contract:

- `GET /ping` answers `{"status":"Healthy"}`.
- `POST /invocations` runs the checks and answers with one JSON object.

Every result, signal and heartbeat is also written as a JSON line on stdout, which AgentCore
sends to CloudWatch Logs.

Nothing here runs by itself. You deploy it and pay for it: a runtime, one ECR repository and
one S3 bucket. `terraform destroy` removes all three, including the bucket's contents.

## What it checks

`{"checks": [...]}` picks checks by name. An empty payload runs them all.

| check | what it reports |
|---|---|
| `kernel` | `uname`, `/proc/version`, the kernel command line, `/proc/modules`, `/proc/filesystems`, `/proc/misc`, `/proc/devices`, whether io_uring is disabled, the CPU's features, and glibc. The release binary needs glibc 2.39 or newer. The probe image is Debian 13. |
| `dev` | `/dev`, and whether `fuse`, `loop-control`, `ublk-control` and `mapper/control` exist (`fuse`, `loop_control`, `ublk_control`, `mapper_control`), with the loop devices already there. |
| `caps` | The effective and bounding capabilities, decoded. It also tries `PR_SET_IO_FLUSHER`, the memory-pressure guard Pando's FUSE threads use, which needs `CAP_SYS_RESOURCE`. It also reports the memlock and nofile limits. |
| `dm` | `/dev/mapper/control`, `dmsetup targets`, the dm kernel options, and a `linear` target over a loop device. It also creates an `era` target over the same device, with metadata on a second loop device. Pando's `--dm` and `--images` need both. `has_linear` and `era_ok` give the answers. |
| `fs` | The file system of `/`, `/tmp` and `/dev/shm`, with sizes and free space. |
| `memory` | `/proc/meminfo`, the cgroup and its memory limits, CPUs, and the dirty-writeback settings. |
| `metadata` | The IMDSv2 token flow at `169.254.169.254`: the role name, credential `Code` and `Type`, `Expiration`, `LastUpdated`, and whether a session token is present. It never reports the key, the secret or the token. It also reports whether IMDSv1 is refused and the region from the identity document. It tries the EC2 header names, then Firecracker MMDS's `X-metadata-token` names. |
| `clocks` | The wall, monotonic and boot clocks, Python's `random` and `/dev/urandom`, each read before the snapshot and again in the session. |
| `loop_file` | ext4 on a loop device over a plain file in `/tmp`: mkfs, a 64 MiB write with fsync, dropped caches, a read-back checksum, fstrim and `e2fsck -fn`. It runs with loop direct I/O and then buffered. |
| `loop_fuse` | The same as `loop_file`, with the backing file inside a bindfs FUSE mount. Pando's FUSE frontend works if this works. |
| `ublk` | Whether `/dev/ublk-control` opens, `/sys/module/ublk_drv`, `/proc/modules`, whether `modprobe ublk_drv` works, `CONFIG_BLK_DEV_UBLK`, and whether io_uring is disabled. |
| `nbd` | The `/dev/nbd*` devices, whether `/dev/nbd0` opens, `/sys/module/nbd`, `/sys/block/nbd*`, `modprobe nbd`, and `CONFIG_BLK_DEV_NBD`. |
| `fuse_passthrough` | The kernel version, whether it is 6.9 or later, and `CONFIG_FUSE_PASSTHROUGH`. It then mounts a FUSE file system of its own and answers `FUSE_INIT` asking for passthrough (`offered` is whether the kernel offered it). It tries to register a backing file with `FUSE_DEV_IOC_BACKING_OPEN`. `ok` is whether that registration worked. |
| `loop_nfs` | The kernel's NFS client in `/proc/filesystems`, `modprobe nfsv4`, and the NFS options. It then runs the `loop_file` steps with direct I/O over a file on an NFSv4 mount. The mount is served from 127.0.0.1 by Ganesha, a userspace NFS server, from an ext4 export. This is plan B if loop over FUSE fails: Pando would serve NFS in Ganesha's place. |
| `pando` | End to end against the bucket, with the runtime role's credentials (`S3_CREDENTIALS=instance`). It runs `pando init` with a 1 GiB branch. It then runs `attach --fuse`, mkfs, writes 128 MiB, and detaches, which publishes the branch. It attaches again on an empty cache, checks the checksum, detaches, and deletes the branch. Each step's time and output are in the result. |
| `pando_ublk` | The same as `pando`, through Pando's ublk frontend as `/dev/ublkb0`. It is skipped when there is no `/dev/ublk-control`. |
| `shell` | Runs `{"shell": "<command>"}` with `sh -c`. It works only if the runtime was deployed with `allow_shell = true`. |

Every check reports kernel options when `/proc/config.gz` or `/boot/config-<release>` can be
read; `kernel.config.source` says which one was read. `kernel.filesystems` and `kernel.modules`
hold `/proc/filesystems` and `/proc/modules` in full.

After the checks, `frontends` sums up what works. It covers `ublk_kernel`, `ublk_pando`,
`nbd_kernel`, `dm_linear`, `dm_era`, `loop_over_fuse`, `fuse_pando`, `fuse_passthrough` and
`loop_over_nfs`. Each is `true`, `false`, or `null` when its check did not run. `best` names the
best one that works, in this order: Pando over ublk, Pando over FUSE, ublk in the kernel, FUSE
and loop, NBD, NFS and loop. `image_handoff` is whether `linear` and `era` both work.

Each result has `check_seconds`. Checks that can pass or fail also have `ok`. A check that throws
reports `error` and `trace` instead.

## Deploy

You need:

- An AWS account and region where AgentCore Runtime V2 is offered. The default is `us-east-1`;
  set `-var region=...` for another.
- The AWS CLI v2 signed in to that account.
- Terraform 1.10 or newer.
- Docker with buildx that can build `linux/arm64`. Docker Desktop and OrbStack can, on either
  architecture.

Run these from the repository root, in this order.

1. Create the repository, bucket and role. With no image digest, Terraform skips the runtime.

   ```bash
   terraform -chdir=bench/agentcore init
   ```

   ```bash
   terraform -chdir=bench/agentcore apply
   ```

2. Build the image from this checkout and push it. The image builds Pando from source for arm64
   and then installs the probe. The build takes several minutes under emulation. The command
   ends with a line `image_digest = "sha256:..."`.

   ```bash
   bench/agentcore/probe.sh push
   ```

3. Create the runtime on that exact image. Use the digest printed in step 2.

   ```bash
   terraform -chdir=bench/agentcore apply -var image_digest=sha256:...
   ```

   AgentCore starts the container, waits for a healthy `/ping`, and takes its snapshot. The
   runtime must be healthy within 120 s or creation fails. The probe answers `/ping` at once.
   If creation fails, the log group (`terraform -chdir=bench/agentcore output log_group`) shows
   how far the process got.

To try a change, push again and apply with the new digest. To allow the `shell` check, add
`-var allow_shell=true`.

## Invoke and read the results

```bash
bench/agentcore/probe.sh invoke
```

This starts a new session and runs every check. It prints the session ID on stderr and the
results file, `bench/agentcore/results/<session>.json`, on stdout. The `pando` check takes a
few minutes, and the command waits up to 15 minutes.

To run some checks only, or to run again in the same microVM:

```bash
bench/agentcore/probe.sh invoke "" '{"checks": ["dev", "caps", "loop_fuse"]}'
```

```bash
bench/agentcore/probe.sh invoke pando-probe-<session> '{"checks": ["clocks"]}'
```

The same results are in CloudWatch Logs, one `check` line per check, among the `started`,
`first healthy ping`, `invocation`, signal and heartbeat lines:

```bash
bench/agentcore/probe.sh logs 1h
```

To read the key answers:

- **Which frontend?** `frontends.best`, then the booleans next to it.
- **Can Pando's FUSE path run?** `loop_fuse.ok` and `pando.ok`. Then `pando.write.dio` shows
  whether the loop device used direct I/O (`1`), and `pando.write.log` holds Pando's output. If `loop_fuse` fails, read `dev` and `caps`:
  mounting FUSE needs `CAP_SYS_ADMIN` and `/dev/fuse`.
- **Can the ublk path run?** `ublk.ok` says the kernel has it, and `pando_ublk.ok` says Pando's
  frontend works as is. `frontends.image_handoff` says whether `--dm` and `--images` work too.
- **Plan B.** `nbd.ok` and `loop_nfs.ok` are frontends Pando does not have yet.
  `fuse_passthrough.ok` would let a FUSE frontend hand the kernel the cache file of a
  materialized branch.
- **Is the memory-pressure guard available?** `caps.io_flusher.set_rc` is `0` when it is. Without it, Pando still runs,
  but its FUSE threads are not exempt from writeback throttling. Pass `--dirty-limit` to bound
  the dirty data instead.
- **Credentials.** `metadata.imdsv2_ec2_headers` and `metadata.imdsv2_mmds_headers` show which
  header names the service accepted: the one with a `role` and an `expiration` worked. Pando's instance credentials use the EC2 names. If only the MMDS names
  work, Pando cannot use `S3_CREDENTIALS=instance` here.
- **Snapshot effects.** Run `clocks` in two sessions. `start.python_random` and `start.urandom`
  are the same in every session, because they were drawn before the snapshot. `now.urandom`
  should differ between sessions, and so should `now.python_random` if Python reseeds. A
  `wall_since_start` much larger than `monotonic_since_start` shows the monotonic clock did not
  count the time the snapshot sat on disk. Heartbeat lines log a `clock jump` when it moves by more
  than 2 s while running.

## How a session ends

```bash
bench/agentcore/probe.sh signals pando-probe-<session>
```

This stops the session, waits 60 s, and prints the session's lines about signals, startup, the
first ping and clock jumps, plus its last five heartbeats. After `SIGTERM`, the heartbeat runs
every 0.2 s and carries `since_sigterm`. The last heartbeat shows how long the platform waited
before it killed the microVM. That is the budget a detach has to publish in. A session that
reaches `idle_timeout_seconds` (default 900) or `max_lifetime_seconds` (default 28800) ends the
same way.

## Remove

```bash
terraform -chdir=bench/agentcore destroy
```

This deletes the runtime, the repository with its images, the bucket with its objects, the role
and the log policy. The log group is created by AgentCore, not by Terraform. Delete it in the
console or with `aws logs delete-log-group` if you don't want it kept.
