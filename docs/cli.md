# The command-line contract

What a program that runs `pando`, such as Spotlight's host, can rely on: the JSON lines that
`--json` prints, the exit statuses, and how to tell when a device serves.
[tests/kernel/cli.sh](../tests/kernel/cli.sh) checks it on a real device.

## Output

`--json` works with every command. Standard output then carries only JSON objects, one per
line, each with an `event` field; the lines for a person go to standard error. Without
`--json`, standard output carries those lines for a person, whose wording may change. Fields may
be added to an event; none is removed or renamed within a major version. Sizes are bytes,
durations are whole milliseconds (`_ms`), and roots are hashes in hex.

A command that fails prints an `error` line as its last line before it exits; an attach that
drained prints `drained` instead:

```json
{"code":3,"event":"error","kind":"in-use","message":"/dev/ublkb1 is in use: mount /mnt/w. Unmount it…"}
```

## Exit statuses

| Status | `kind` | Means |
|---|---|---|
| 0 | | Done. |
| 1 | `failed` | Anything below does not cover. |
| 2 | | The arguments do not parse. |
| 3 | `in-use` | Another process holds the device, mount or cache: a mounted device at `detach`, a live daemon at `attach --recover`, a cache another `pando` has open. Nothing changed. |
| 4 | `store-unreachable` | The store did not answer, or kept failing in a way a retry may fix, after the retries of the store client (about three minutes). |
| 5 | `fenced` | Another writer took the branch; this writer stopped. |
| 6 | `unsupported` | This host lacks what the command needs, such as ublk, or its user recovery, for `attach --device`. `attach` checks this before it touches the store. |
| 75 | `try-again` | The work goes on and ends later: an attach drained on SIGTERM and left its device to the next `attach --recover`, or a `detach` ran out of `--publish-wait` while the daemon still publishes. |

## Readiness

`attach` prints its `attached` line once the device serves: a program may then make or mount the
file system on `block_device`. Under systemd with `Type=notify` the attach also sends `READY=1`
then.

## Events

### attach

```json
{"block_device":"/dev/ublkb1","branch":"task-1","device":"/dev/ublkb1","dm":null,"event":"attached","generation":4,"recovered":false,"rejoined":false,"took_ms":312}
```

- `attached`: the device serves. `block_device` is the device to make and mount the file system
  on: the device-mapper device with `--dm`, else `device`. `generation` is this writer's
  generation, one past the ref's when it opened. `recovered` is true when it took the device over
  from a daemon that died; `rejoined` when it kept the local image a drained daemon left. With `--fuse`,
  `device` is the loop device and the line also has `fuse`, `file` and `direct_io`.
- `state`: how the branch stands changed. `state` is `serving`, `lagging` (uploads are a minute
  behind the flushes, as while the store is unreachable), `disk-full` (the cache's disk is
  full; writes fail with ENOSPC, flushes go on) or `uploads-stopped` (with `error`: no later
  upload can publish the branch). Also `upload_lag_ms`.
- `published`: after `detach`, the store holds the branch. `took_ms`.
- `drained`: after SIGTERM or SIGINT. `published` says whether the store holds every write
  acknowledged before the signal; either way the writes are on this host, and the attach exits
  with 75.

### detach

```json
{"device":"/dev/ublkb1","event":"detached","took_ms":2210}
```

Printed once the daemon published the branch and let go of the device; `fuse` in place of
`device` with `--fuse`.

### status

One line per attached branch on this host, or for the one `--device` or `--fuse` names:

```json
{"branch":"task-1","cache_bytes":734003200,"degraded":false,"device":"/dev/ublkb1","error":null,"event":"status","generation":4,"state":"serving","unuploaded_bytes":1048576,"upload_lag_ms":1200}
```

`state` is as in `attach`'s `state` line, and `degraded` is true for any state but `serving`.
`unuploaded_bytes` counts the 64 KiB chunks flushed but not yet in the store, or is null when the
daemon could not count them; `cache_bytes` is the disk the cache's object log takes; `error` is
why uploads stopped. `status` asks each daemon on its control socket, so it works while the
branch's cache is open. A daemon of an earlier release
leaves `generation`, `unuploaded_bytes`, `cache_bytes` and `error` null.

### check-host

One `check` line per check, then a `host` line; it exits with 6 when neither `attach --device`
nor `attach --fuse` can work:

```json
{"check":"ublk","detail":"/dev/ublk-control is there","event":"check","fix":null,"ok":true}
{"event":"host","fuse":true,"ublk":true}
```

The checks are `ublk`, `device-mapper` (`--dm` and `--images`), `loop` and `fuse` (`--fuse`),
`user-recovery` (a takeover after a crash or an upgrade) and `root`. A failed check's `fix` says
what to do.

### Other commands

| Command | Event | Fields |
|---|---|---|
| `init` | `created` | `branch`, `size` |
| `import` | `imported` | `branch`, `size`, `chunks`, `segments`, `segment_bytes`, `took_ms` |
| `branch` | `branched` | `from`, `branch`, `from_checkpoint`, `took_ms` |
| `checkpoint` | `checkpointed` | `checkpoint`, `branch`, `root`, `took_ms` |
| `checkpoints` | `checkpoint`, one per line | `checkpoint`, `size`, `branch`, `root` |
| `delete-checkpoint` | `deleted-checkpoint` | `checkpoint` |
| `advance` | `advanced` | `branch`, `to` |
| `delete` | `deleted` | `branch` |
| `refs` | `ref`, one per line | `branch`, `size`, `generation`, `root`, `from` |
| `resize` | `resized` | `device`, `size` |
| `base` | `base` | `branch`, `built`, `chunks`, `took_ms` |
| `follow --once` | `followed` | `built` |
| `gc` | `gc` | the counts of the report: `live`, `live_bytes`, `used_bytes`, `dead`, `dead_bytes`, `deleted`, `current_writer`, `young`, `needed_by_kept`, `revived`, `stale_change_lists`, `stale_log_records` |
| `stat` | `stat` | `branches`, `segments`, `segment_bytes`, `change_lists` |
| `purge` | `purged-part`, one per part as it finishes, then `purged` | `part`, `objects`; then `remote`, `objects`, `bytes`, `took_ms` |
