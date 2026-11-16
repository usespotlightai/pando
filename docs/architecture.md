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
| `DeviceSwitch` | `pando-linux` | to come with the handoff |
| `Clock` | `SystemClock` | `SimClock` (moves with paused `tokio::time`) |
| `Rng` | `SystemRng` | `SimRng` (seeded) |
| `Cpu` | `PoolCpu` (foreground and background pools) | `InlineCpu` |

## Runtimes

The engine runs on one shared multi-thread tokio runtime. CPU work (SHA-256, zstd) goes to the
`Cpu` port, so it never blocks I/O tasks.

libublk runs a current-thread runtime on each queue thread, without an I/O driver, parked in
`io_uring_enter`. A wake from another thread reaches it only at the next completion or after
a 1 s safety timeout. So:

1. A read the queue can serve from a local image or the cache without waiting is served on the
   queue thread (`LocalFile::read_at_nowait`).
2. Every other request goes to the engine runtime.
3. The result comes back through a completion queue plus an eventfd that the queue task polls
   with `ops::poll_add`.

## Integrity

Every object from the store is checked against its content hash once, when a frame is decoded.
A chunk rebuilt from a recipe or built from local blocks is checked against its hash. The
`Verified` type is the only way such bytes enter the engine. Detach waits until the upload is
durable in the store.
