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
  creates it at `log/<hash of the branch name>/<branch id>/<writer generation>/<n>`. Records
  are numbered from 0 without gaps, and a create fails when the number is taken.
- The **uploader** packs pending chunks and tree nodes into segments and moves the ref. The ref's
  `log` line says which records its root covers; the shipper then deletes them.

A write goes to the journal, one local file per shard of chunks, and is acknowledged once the file
has it, without a sync; memory keeps where in the journal each write is. A commit also starts
without a flush once 256 MiB of written chunks wait, and past 1 GiB a write commits first. Such a
commit is local only: it leaves the log to the uploader, as a flush does when 32 MiB of flushes
already wait for the shipper. A commit removes the journal it took while its pages are still in
the page cache, so journal writes never reach the disk; only the cache log does. The daemon serves
the device, so the kernel does not slow its own writes when its dirty pages fill its memory: the
journal may take a quarter of the memory the daemon may use (its memory cgroup's limit, or the
host's memory), and under a smaller limit the two thresholds shrink to a quarter and a half of
that, and the cache log waits for the disk once it is more than 16 MiB behind. A daemon's memory
for writes therefore stays bounded however much is written between flushes.

An open that does not resume this host's own state reads the records the ref does not cover, from
the ref's log position through the last writer's log, and applies them as writes. It seals each
log it read by creating a seal record at its first free number, so a writer that is still running
fails its next append and stops. Once the new writer publishes, it deletes what earlier writers
left in the log except those seals. A record that would leave a gap is not applied, nor anything
after it: an open always sees a prefix of the flushes. A detach marks the ref's log as closed, and
the next open then reads no records. An open that resumes this host's own state reads and seals
nothing: a lock on that state means the process that wrote it has stopped. When that state is
ahead of the ref, the new writer logs no flush until an upload has published it, since an open
elsewhere would apply the records to the ref's root, without the flushes the writer resumed.
Each ref has a branch id, drawn at random when the ref is created, and a branch made later
under a deleted branch's name gets another. The id is in the keys of the branch's log and in each
host's state of the branch, so the new branch reads none of the old one's log, no host resumes
the old branch's state on it, and a writer still attached to the old branch fails its next ref
update, since the ref's bytes, and so its ETag, differ. Deleting a branch deletes its ref only if
it still has the ETag the delete read (`If-Match`, which AWS S3 honours), so a branch made
again under the name meanwhile survives the delete, then deletes its log; GC deletes what a delete that stopped part
way left.
`pando advance` seals the log and moves the branch to a new writer generation whose log is
closed: a writer still attached fails its next append or upload, and no open replays its records
or resumes its state. [specs/README.md](../specs/README.md) describes the TLA+ specification of
the log and how the engine's traces are checked against it.

With `--log behind`, the default, a flush returns once it is durable on the host, and the shipper
creates its record within about 50 ms. With `--log strict`, a flush returns once its record is
created or an upload published it; a strict branch cannot hand off to an image, since the kernel
then flushes the image without Pando. With `--log off`, only uploads reach the store.

## Cache log

The cache log is an append-only log of the objects this host holds: the chunks and tree nodes
it wrote, and those it fetched. It is a directory, `<cache>/objects`, of segment files: appends go
to the last one, and a record that would take it past 16 GiB starts the next. Each commit appends new versions of the chunks it changed, so a
sweep drops the old ones. A sweep runs once 1 GiB has been appended since the last one, or as many
bytes as that sweep kept, if that is more. It keeps:

- what each open branch reaches from its root, and the objects not yet uploaded under its saved
  state;
- what commits, grows, imports and uploads in flight start from, and what the commits, grows
  and imports append or put again while they run;
- the objects not yet uploaded under the saved state of each branch closed on this host.
  `<cache>/states` lists those state files.

It drops every other record. Before it writes padding over each run of dropped records, it lists
the run in `<log>.pads` and syncs that file; it then frees the pages with a hole punch, without
a sync of the log: offsets stay and nothing is copied. An open writes the padding of every listed
run again, and its scan skips padding. The next sync of the log unlists the runs it covered, and
a sweep that finds more than 1 MiB listed syncs the log itself. A sweep marks in an epoch of its own, older
than the one later puts use, so a commit that starts after a sweep keeps only what it puts again.
Under constant random writes, the log stays near the live data plus the trees that uploads in
flight still need.

Hole punches free the disk of dropped records, but not their place in a file, so after each
sweep the log compacts: each segment before the last two whose records take at most a quarter of
it has them appended again at the end, and once a sync makes the copies durable, its file is
removed. The segment just before the last waits a whole segment of appends, so that under churn
most of its records are written again before a compaction would copy them. A moved record counts
as old as where it was first appended. A segment with appends not yet settled stays for a later
sweep. The log's files thus stay near four times what it keeps, plus the last two segments, well
within the largest file a file system takes.

With `--cache-budget`, the chunks an open branch reaches that are already in the store stay only
while they fit. A sweep keeps what it must, the chunks appended since the last sweep began, and
every tree node, then the chunks read most recently, until the log is at seven eighths of the
budget. The next sweep runs once the log nears the budget again, and at the latest after an
eighth of it is appended. While the log is a quarter past the budget, a fetch waits for the sweep
under way or due.

## Host daemon

`pando serve` runs every command a client sends it in one process, on one cache. A request is the
client's working directory and arguments, and relative paths in the arguments resolve against
that directory; the answer streams the command's output and ends with its exit code. Store
settings and credentials stay in the daemon, which refuses a request for another store or cache.
A client that disconnects cancels its command, except `attach`, whose device keeps serving until
`detach`. On SIGTERM or SIGINT the daemon takes no more requests, lets each attach drain as
a lone `attach` would, and exits with status 75 while it holds their devices; `attach --recover`
takes them over.
