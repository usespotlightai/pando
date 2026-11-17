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
