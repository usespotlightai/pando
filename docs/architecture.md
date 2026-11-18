# Architecture

## Boundaries

The engine holds every decision: what to fetch, when to upload, when a writer is fenced, when
a handoff swaps the device. It reaches the world only through ports. So the crash-critical
paths (uploads, owner records, the cache log, the handoff) run in the simulator, and on
macOS for development.

| Port | Production adapter | Simulated adapter |
|---|---|---|
| `Store` | `pando-s3` | `sim::SimStore` (injects failures, also writes that land but report failure) |
| `LocalFs`, `LocalImage` | `pando-host::HostFs` | `sim::SimFs` (keeps synced data apart, `crash()` drops the rest) |
| `DeviceSwitch` | `pando-linux::DmSwitch` | `sim::SimDevices` |
| `Clock` | `SystemClock` | `SimClock` (moves with paused `tokio::time`) |
| `Rng` | `SystemRng` | `SimRng` (seeded) |
| `Cpu` | `PoolCpu` (foreground and background pools) | `InlineCpu` |

## Runtimes

The engine runs on one shared multi-thread tokio runtime. CPU work (SHA-256, zstd) goes to the
`Cpu` port, so it never blocks I/O tasks.

Each ublk queue thread waits in `io_uring_enter` on its own ring. So:

1. A lone read the queue can serve from memory or the page cache without waiting is served on
   the queue thread (`Volume::try_read_at`).
2. Every other request goes to the engine runtime.
3. The result comes back through a channel plus an eventfd that the queue's ring reads, so a
   completion on another thread wakes the queue at once.

## Integrity

Every object from the store is checked against its content hash once, when a frame is decoded.
A chunk rebuilt from a recipe or built from local blocks is checked against its hash. The
`Verified` type is the only way such bytes enter the engine. Detach waits until the upload is
durable in the store.

The store's `format` marker names its format version; a build refuses a store whose major
version it does not read. See [store-format.md](store-format.md).

## Durability

A flush commits locally first: the chunks go to the cache log, one sync, then the new root is
recorded. Two background tasks then take it to the store:

- The **shipper** merges the flushes that wait into one record of the branch's write log and
  creates it at `log/<hash of the branch name>/<writer generation>/<n>`. Records are numbered
  from 0 without gaps, and a create fails when the number is taken.
- The **uploader** packs pending chunks and tree nodes into segments and moves the ref. The ref's
  `log` line says which records its root covers; the shipper then deletes them.

Writes wait in memory until a commit, so a commit also starts without a flush once 16 MiB of
written chunks wait, and past 32 MiB a write that needs a new chunk buffer commits first. Such a
commit is local only: it leaves the log to the uploader, as a flush does when 32 MiB of flushes
already wait for the shipper. A daemon's memory for writes therefore stays bounded however much
is written between flushes.

An open that does not resume this host's own state reads the records the ref does not cover, from
the ref's log position through the last writer's log, and applies them as writes. It seals each
log it read by creating a seal record at its first free number, so a writer that is still running
fails its next append and stops. Once the new writer publishes, it deletes what earlier writers
left in the log except those seals. A record that would leave a gap is not applied, nor anything
after it: an open always sees a prefix of the flushes. A detach marks the ref's log as closed, and
the next open then reads no records. An open that resumes this host's own state reads and seals
nothing: a lock on that state means the process that wrote it has stopped. Deleting a branch
deletes its log, so a branch made later under the same name starts with none.

With `--log behind`, the default, a flush returns once it is durable on the host, and the shipper
creates its record within about 50 ms. With `--log strict`, a flush returns once its record is
created or an upload published it; a strict branch cannot hand off to an image, since the kernel
then flushes the image without Pando. With `--log off`, only uploads reach the store.

## Host daemon

`pando serve` runs every command a client sends it in one process, on one cache. A request is the
client's working directory and arguments, and relative paths in the arguments resolve against
that directory; the answer streams the command's output and ends with its exit code. Store
settings and credentials stay in the daemon, which refuses a request for another store or cache.
A client that disconnects cancels its command, except `attach`, whose device keeps serving until
`detach`. On SIGTERM or SIGINT the daemon takes no more requests, lets each attach drain as
a lone `attach` would, and exits with status 75 while it holds their devices; `attach --recover`
takes them over.
