# Pando

Writable branches of a large workspace for AI agents, stored in S3.

Each agent run needs its own copy of a repository and everything installed in it, often on a
machine where neither is present. It also needs to preserve its changes when the run ends.
Pando gives each run a block device backed by a branch of a shared base. Creating a branch takes
about 100 ms and adds one small ref object. The host fetches only what the run reads and uploads only what
it changed when the run ends. Any host with access to the bucket can open any branch.

Pando takes its name from a grove of aspens in Utah: a single organism with thousands of trunks
sharing one root system.

## How it works

- **Branches are block devices.** Pando serves each attached branch to the kernel over ublk, with
  a normal file system (ext4) on top. Agents see an ordinary disk, so `git`, `npm`
  and compilers work without changes. The disk has 4 KiB sectors, a volatile write cache with
  FUA, and discard: a discarded range reads as zeros, and its chunks no longer take space.
- **Chunks and branch refs.** The device is split into 64 KiB chunks named by their hash. A
  persistent radix tree maps each chunk index to a chunk. Each branch is one root entry in a
  small ref object, and forking copies that ref. Branches share every chunk they have not changed.
- **Segment storage.** Chunks and tree nodes are packed into ~8 MiB segments containing zstd
  frames of up to 1 MiB. A single range GET fetches neighbouring chunks, and uploads take a few
  PUTs rather than one per write. Where possible, changed chunks are stored as 4 KiB block
  references to the base, reducing the data uploaded by `npm ci` by about 13x.
- **Write-back with fencing.** A flush is immediately durable locally and uploads in the
  background. A conditional PUT updates the ref. Opening a branch takes a new writer generation
  that fences any older writer.
- **Fast starts.** Each host keeps a base image per root and clones it with reflink. A later
  branch of the same root therefore starts in about 250 ms. The first run records the frames it
  touched, and the next run on another host prefetches them. Once the branch is local, Pando
  hands the device to the kernel through device-mapper over a loop device and leaves the I/O path.
- **Crash safety.** ublk user recovery and a local journal preserve acknowledged writes through
  a daemon crash. After restarting, the daemon resumes serving the device where it left off.

## Crates

| Crate | Contents |
|---|---|
| `pando-format` | the store format: hashes, entries, refs, tree nodes, frames, segment indexes, recipes, change lists, object keys. No I/O. |
| `pando-engine` | the volume engine: the block map tree, the cache, fetch and its scheduler, the uploader and writer fencing, dedup, base builds, seeding and the handoff steps. It reaches the outside world only through ports. |
| `pando-s3` | the object-store port over any S3-compatible store with conditional writes (AWS S3, Cloudflare R2, MinIO) |
| `pando-host` | the host ports on a real machine: files, base images, clock, randomness, CPU pool. Linux gets reflinks, extent hints and hole punching; other systems get plain fallbacks. |
| `pando-linux` | the ublk block device frontend and the device-mapper switch |
| `pando-dst` | deterministic simulation, crash and fault scenarios, and conformance suites for every port adapter |
| `pando` | the command-line tool |

## Building

Build with Rust 1.100, pinned in `rust-toolchain.toml`. The block device requires Linux with
`ublk_drv`. The engine's tests run anywhere; on macOS, `pando-host` falls back to plain file
operations. To build and test the Linux crates from macOS, `tools/cargo.sh` runs cargo in a
Linux container and takes the same arguments.

```bash
cargo test --workspace
```

`tools/check.sh` runs the checks CI runs, in a container with every tool pinned
(`tools/Dockerfile`, `tools/install.sh`): with no arguments it runs the lints, the supply-chain
checks and the tests. It also takes `asan`, `fuzz [seconds]`, `coverage`, `mutants`, or any
single check by name, such as `clippy` or `shellcheck`. The fuzz targets are in `fuzz/`.

Dependabot proposes weekly updates for the crates, actions, base images and Terraform providers
(`.github/dependabot.yml`). A crate update passes `vet` only once each new version has an audit
or exemption in `supply-chain/`. Dependabot moves the `rust` image only within its minor version,
so a new Rust release is a manual change to `rust-toolchain.toml`, `rust-version` in
`Cargo.toml`, and the `FROM` and `rustup` lines in `tools/Dockerfile`, all in one commit. The
nightly in `tools/nightly` and `fuzz/README.md` also moves by hand.

Fixtures in `pando-format/tests/fixtures` pin the byte encoding of every stored object.

## Alternatives

Pando is not a sandbox; it provides the disk beneath one. The comparisons below cover the storage
systems sandboxes are built on, using the same machine, bucket and agent workload. See
[Benchmarks](#benchmarks) for measured results and [bench/README.md](bench/README.md) for the
plan and methodology.

**Storage used by sandbox platforms**, according to their own docs and engineering posts:

| Approach | Used by | Benchmarked as |
|---|---|---|
| chunked block device over NBD from object storage, copy-on-write overlay | E2B | E2B-style (ZeroFS NBD, or E2B's own block layer) |
| compressed disk image in S3, cached on NVMe | Vercel Sandbox | image download |
| OCI image layers on overlayfs, FUSE mounts of object storage for volumes | Daytona | `docker pull`, SOCI lazy pull |
| lazy-loading, content-addressed FUSE file system | Modal | Nydus / eStargz lazy pull |
| chunks in object storage, SQLite metadata, copy-on-write checkpoints | Fly.io Sprites | JuiceFS |
| reflink copies on local NVMe | CodeSandbox | XFS reflink (local only) |

Morph Cloud, Freestyle and Runloop have not published how they store disks.

**Comparison for Pando's intended workload:**

| | Writable fork | Fork cost | Starts on an empty host | Saves only the change |
|---|---|---|---|---|
| Pando | yes | ~100 ms, a 135 B ref | yes, fetches what it reads | yes, 4 KiB dedup |
| image or tar from S3 | copy | full copy | downloads everything | no |
| OCI layers on overlayfs | yes | ms on a host with the image | pulls the image (lazily with SOCI / Nydus) | a new layer per save |
| EBS snapshot to volume | yes | seconds to create | lazy-loaded, slow first reads | incremental snapshots, minutes |
| overlayfs, btrfs, XFS reflink, ZFS | yes | ms | no, local only | no |
| JuiceFS | `juicefs clone` | metadata copy, grows with files | yes | yes |
| ZeroFS | no, read-only checkpoints | full copy of the device file | yes, no data prefetch | yes, no dedup |
| Amazon S3 Files | no | copy | yes | per file |
| Archil | yes (enterprise on your own bucket) | ? | yes | yes |

## Benchmarks

> **Partly measured.** Every chart has results; SOCI, Nydus, ZeroFS and Amazon S3 Files
> are not set up yet and fill in as their runs finish on the bench VM (AWS `m8id.4xlarge`, eu-north-1; see
> [bench/README.md](bench/README.md)). Run `python3 bench/plot.py` to redraw them from
> `bench/results/`.

The workspace is [Spotlight](https://github.com/usespotlightai/spotlight) after its setup command:
1.4 GB and 129k files. All systems use the same VM and bucket. Results show medians of 5 runs,
with Pando in blue. "Pando" serves the branch over ublk until it is detached; "Pando (handoff)"
adds `--dm` and `--images`, so the device switches to a local image once the branch is local.

### Fork

Time to create a writable branch, and the storage each branch and each edit adds. Docker and
XFS reflink branches exist only on the host's disk. Pando, JuiceFS and EBS branches are durable
and open on any host, so each fork waits on the network. These rows are from one run so far, not
five. JuiceFS made 100 branches and EBS 10; the others made 1,000.

<!-- bench:fork -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/bench/fork-dark.svg">
  <img alt="Bar chart of the time each system takes to make a writable branch of the workspace" src="docs/bench/fork.svg">
</picture>

<details>
<summary>Numbers</summary>

| p50 over up to 1,000 branches | median | range |
|---|---|---|
| Pando | 106 ms | 106 ms to 106 ms |
| Docker (overlayfs, local) | 11.1 ms | 11.1 ms to 11.1 ms |
| EBS snapshot to volume | 2.6 s | 2.6 s to 2.6 s |
| JuiceFS | 12.8 s | 12.8 s to 12.8 s |
| ZeroFS (NBD) | — | |
| XFS reflink (local) | 1 ms | 1 ms to 1 ms |

| storage added per branch | median | range |
|---|---|---|
| Pando | 135 B | 135 B to 135 B |
| Docker (overlayfs, local) | 36.5 KiB | 36.5 KiB to 36.5 KiB |
| EBS snapshot to volume | 16 GiB | 16 GiB to 16 GiB |
| JuiceFS | 25.4 MiB | 25.4 MiB to 25.4 MiB |
| ZeroFS (NBD) | — | |
| XFS reflink (local) | 4.61 KiB | 4.61 KiB to 4.61 KiB |

| storage added by a 5 MiB edit to a branch | median | range |
|---|---|---|
| Pando | 5.08 MiB | 5.08 MiB to 5.08 MiB |
| Docker (overlayfs, local) | 5 MiB | 5 MiB to 5 MiB |
| EBS snapshot to volume | none: billed by volume size | |
| JuiceFS | 5 MiB | 5 MiB to 5 MiB |
| ZeroFS (NBD) | — | |
| XFS reflink (local) | 5.75 MiB | 5.75 MiB to 5.75 MiB |

</details>
<!-- /bench:fork -->

### Cold start

Time to fork, mount and finish the first `git status` on a host that has never seen the workspace.
Pando's slowest repetition, 2.6 s, is the first mount of the base; later mounts prefetch the
reads it recorded in the bucket.

<!-- bench:cold -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/bench/cold-dark.svg">
  <img alt="Bar chart of the time from an empty host to the first git status finishing, per system" src="docs/bench/cold.svg">
</picture>

<details>
<summary>Numbers</summary>

| fork, mount and git status done | median | range |
|---|---|---|
| Pando | 1.19 s | 1.15 s to 2.6 s |
| Pando (handoff) | 1.19 s | 934 ms to 2.25 s |
| tar from S3 | 4.44 s | 4.41 s to 4.48 s |
| image download from S3 | 4.72 s | 4.69 s to 4.79 s |
| Docker (overlayfs) | 26.9 s | 26.8 s to 27.3 s |
| SOCI lazy pull | — | |
| Nydus lazy pull | — | |
| EBS snapshot to volume | 10.9 s | 10.1 s to 12 s |
| JuiceFS | 3.88 min | 3.8 min to 3.92 min |
| ZeroFS (NBD) | — | |
| Amazon S3 Files | — | |
| XFS reflink (local) | local only | |

</details>
<!-- /bench:cold -->

### Warm start

Time to start on a host that has already run a branch of the same base. Each system has its
base on the host: Docker's image is pulled, tar and the image download have their file cached,
and Pando (handoff) has the host's base image of the root. The page cache is dropped before each
run. Pando (handoff) switches to the image as soon as the branch opens, so `git status` reads
through device-mapper and a loop device over a reflink clone and takes about 1 s, against 0.47 s
over ublk.

<!-- bench:warm -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/bench/warm-dark.svg">
  <img alt="Bar chart of the time to the first git status on a host that already ran a branch of the same base" src="docs/bench/warm.svg">
</picture>

<details>
<summary>Numbers</summary>

| fork, mount and git status done | median | range |
|---|---|---|
| Pando | 1.29 s | 1.04 s to 1.35 s |
| Pando (handoff) | 1.8 s | 1.77 s to 1.87 s |
| tar from S3 | 2.51 s | 2.5 s to 2.52 s |
| image download from S3 | 2.8 s | 2.8 s to 3.12 s |
| Docker (overlayfs) | 1.27 s | 1.23 s to 1.31 s |
| SOCI lazy pull | — | |
| Nydus lazy pull | — | |
| EBS snapshot to volume | 11 s | 10.5 s to 11.4 s |
| JuiceFS | 18.8 s | 18.4 s to 19.1 s |
| ZeroFS (NBD) | — | |
| Amazon S3 Files | — | |
| XFS reflink (local) | 563 ms | 410 ms to 613 ms |

</details>
<!-- /bench:warm -->

### Agent workload

Time to run the 14 command types agents use most often in Spotlight sandboxes (`git status`,
`tsc`, `oxlint`, `vitest`, `npm ci`, ...), expressed as a ratio to native ext4 on local NVMe.
Each system ran the replay 3 times on a fresh branch of a warm host, page cache dropped. Native
took 70.5 s. Most of JuiceFS's time is `npm ci`, about 9.5 min against 7.6 s; in its first
repetition one `vitest` run failed, and it passed in the other two.

<!-- bench:replay -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/bench/replay-dark.svg">
  <img alt="Bar chart of each system&#x27;s total time for the agent command replay as a ratio to native ext4" src="docs/bench/replay.svg">
</picture>

<details>
<summary>Numbers</summary>

| total time as a ratio to native (1.00x) | median | range |
|---|---|---|
| native ext4 (NVMe) | 1.00x | 1.00x to 1.01x |
| Pando | 1.07x | 1.06x to 1.17x |
| Pando (handoff) | 1.05x | 1.04x to 1.06x |
| image download from S3 | 1.03x | 1.03x to 1.04x |
| Docker (overlayfs) | 1.02x | 1.02x to 1.03x |
| Nydus lazy pull | — | |
| EBS snapshot to volume | 1.17x | 1.16x to 1.23x |
| JuiceFS | 9.96x | 9.90x to 12.07x |
| ZeroFS (NBD) | — | |
| Amazon S3 Files | — | |

</details>
<!-- /bench:replay -->

### Save

After `npm ci`, time from the last write until another host can open the branch, and the bytes
`npm ci`'s changes added to the store. Pando's time is unmount and `pando detach`, which uploads
what changed since its last background snapshot and publishes the branch. JuiceFS uploads each
file as it closes, so its save is a `sync` that finds nothing left, and the upload cost lands in
`npm ci` itself: about 15 min on JuiceFS against 8 s on the others. tar uploads the whole
workspace every time. EBS's bytes are the 512 KiB blocks the snapshot holds beyond the workspace
snapshot. Three runs each.

<!-- bench:save -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/bench/save-dark.svg">
  <img alt="Bar chart of the time and bytes each system needs to make a branch durable after npm ci" src="docs/bench/save.svg">
</picture>

<details>
<summary>Numbers</summary>

| time to durable after npm ci | median | range |
|---|---|---|
| Pando | 4.94 s | 2.43 s to 7.68 s |
| tar from S3 | 3.2 s | 3.17 s to 3.2 s |
| Docker (overlayfs) | 41.1 s | 40.9 s to 41.2 s |
| EBS snapshot to volume | 93.9 s | 83.8 s to 102 s |
| JuiceFS | 2.76 ms | 2.5 ms to 3.52 ms |
| ZeroFS (NBD) | — | |

| uploaded after npm ci | median | range |
|---|---|---|
| Pando | 208 MiB | 207 MiB to 214 MiB |
| tar from S3 | 323 MiB | 323 MiB to 323 MiB |
| Docker (overlayfs) | 194 MiB | 194 MiB to 194 MiB |
| EBS snapshot to volume | 1.05 GiB | 1.05 GiB to 1.05 GiB |
| JuiceFS | 732 MiB | 665 MiB to 785 MiB |
| ZeroFS (NBD) | — | |

</details>
<!-- /bench:save -->

### Raw I/O

Direct I/O with fio on a 1 GiB file in a fresh branch's file system, three runs each. Pando
(ublk) comes out ahead of the local NVMe because its daemon keeps the branch in cache files read
and written through the host's page cache: direct I/O skips the page cache of the device, not of
the daemon, so these rows mostly measure memory. After handoff, reads go through device-mapper
and a loop device to the image on the NVMe; writes are slow there, about 21 MB/s sequential,
for a reason not yet found. EBS gp3 runs at its baseline of 3,000 IOPS and 125 MiB/s.

<!-- bench:fio -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/bench/fio-dark.svg">
  <img alt="Bar charts of random and sequential I/O throughput per system" src="docs/bench/fio.svg">
</picture>

<details>
<summary>Numbers</summary>

| 4 KiB random read, QD1 | median | range |
|---|---|---|
| local NVMe | 11k | 10.4k to 11.1k |
| Pando (ublk) | 109k | 108k to 110k |
| Pando (after handoff) | 6.72k | 5.99k to 7.24k |
| EBS gp3 | 1.54k | 1.54k to 1.6k |
| JuiceFS | 6.37k | 5.98k to 6.58k |
| ZeroFS (NBD) | — | |

| 4 KiB random read, 4 x QD32 | median | range |
|---|---|---|
| local NVMe | 269k | 269k to 269k |
| Pando (ublk) | 814k | 813k to 817k |
| Pando (after handoff) | 237k | 233k to 265k |
| EBS gp3 | 2.99k | 2.99k to 3k |
| JuiceFS | 76.7k | 76.4k to 77.3k |
| ZeroFS (NBD) | — | |

| 4 KiB random write, 4 x QD32 | median | range |
|---|---|---|
| local NVMe | 134k | 134k to 134k |
| Pando (ublk) | 467k | 455k to 481k |
| Pando (after handoff) | 2.47k | 2.14k to 5.09k |
| EBS gp3 | 2.99k | 2.99k to 3k |
| JuiceFS | 484 | 456 to 493 |
| ZeroFS (NBD) | — | |

| 1 MiB sequential read | median | range |
|---|---|---|
| local NVMe | 1,910 MB/s | 1,910 MB/s to 1,910 MB/s |
| Pando (ublk) | 6,430 MB/s | 6,419 MB/s to 6,461 MB/s |
| Pando (after handoff) | 1,849 MB/s | 1,826 MB/s to 1,856 MB/s |
| EBS gp3 | 131 MB/s | 131 MB/s to 131 MB/s |
| JuiceFS | 1,611 MB/s | 1,576 MB/s to 1,660 MB/s |
| ZeroFS (NBD) | — | |

| 1 MiB sequential write | median | range |
|---|---|---|
| local NVMe | 908 MB/s | 908 MB/s to 908 MB/s |
| Pando (ublk) | 1,689 MB/s | 1,523 MB/s to 1,806 MB/s |
| Pando (after handoff) | 21 MB/s | 16 MB/s to 24 MB/s |
| EBS gp3 | 131 MB/s | 131 MB/s to 131 MB/s |
| JuiceFS | 1,328 MB/s | 1,317 MB/s to 1,330 MB/s |
| ZeroFS (NBD) | — | |

</details>
<!-- /bench:fio -->

### Crash

The storage daemon is killed with `kill -9` while a writer writes and fsyncs 256 KiB files, and is
restarted at once (`pando attach --recover`, or a new JuiceFS mount). The branch is then opened
with a new cache and state directory, sharing nothing with the first, its file system checked
and every acknowledged file read back. Medians of three runs.

| System | Failed writes | Stall | fsck | Files correct |
|---|---|---|---|---|
| Pando | 0 | 0.12 s | clean | all acknowledged (1,403 to 1,438 per run) |
| JuiceFS | 2 | 0.23 s | clean | all acknowledged (264 to 270 per run) |
| ZeroFS | — | — | — | — |
