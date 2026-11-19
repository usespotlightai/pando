# Running Pando on a host

Each attached branch is one instance of the systemd template unit
[`pando-attach@.service`](../packaging/systemd/pando-attach@.service). The unit restarts a
daemon that crashed or drained, and it attaches the branch again after a reboot.

## Install

Each [release](https://github.com/usespotlightai/pando/releases) has an archive for x86_64 and
for aarch64 Linux, `install.sh`, and `SHA256SUMS` over all of them. The binaries need glibc
2.39 or newer (Ubuntu 24.04, Debian 13) and a kernel with ublk. To install release 0.1.0:

```
curl -fsSLO https://github.com/usespotlightai/pando/releases/download/v0.1.0/install.sh
curl -fsSLO https://github.com/usespotlightai/pando/releases/download/v0.1.0/SHA256SUMS
sha256sum -c --ignore-missing SHA256SUMS
sudo bash install.sh 0.1.0
```

`install.sh` fetches the archive for this machine, checks it against `SHA256SUMS`, and unpacks
it to `/usr/local/lib/pando/<version>`. It points `/usr/local/bin/pando` at it, installs the
unit, runs `systemctl daemon-reload`, and creates `/etc/pando/attach`. `--from DIR` takes the
archive and `SHA256SUMS` from a directory instead, for host images built without network access.
`SHA256SUMS` comes from the same release as the files it covers, so it catches a corrupt
download, not a release someone replaced. `gh attestation verify <file> --repo
usespotlightai/pando` checks that a file, `install.sh` or an archive, was built by this
repository's release workflow from a commit on main.

| Path | Holds |
|---|---|
| `/usr/local/lib/pando/<version>/` | each installed release: the binary and the unit |
| `/usr/local/bin/pando` | a link to the current release's binary |
| `/etc/systemd/system/pando-attach@.service` | the unit |
| `/etc/pando/pando.env` | the host's settings, mode 0600: `S3_BUCKET`, `S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` and `S3_SESSION_TOKEN`, or `S3_CREDENTIALS=instance` for the instance role, and `PANDO_ARGS` for global flags such as `--cache` |
| `/etc/pando/attach/<name>.env` | one branch: `PANDO_BRANCH`, `PANDO_DEVICE` (`/dev/ublkbN`), and `PANDO_ATTACH_ARGS` for the other `attach` flags, such as `--remote`, `--dm`, `--images` and `--state` |

Credentials go in the env file, never in `PANDO_ARGS` or `PANDO_ATTACH_ARGS`: those end up on
the command line. Quote a value that holds spaces: `PANDO_ATTACH_ARGS="--state /data/state --dm b1"`.

## Lifecycle

**Attach.** Write `/etc/pando/attach/<name>.env`, then run
`systemctl enable --now pando-attach@<name>`. The unit is `Type=notify`: it is started once the
device serves. A mount of the device orders itself with `After=pando-attach@<name>.service` and
`Wants=` it, never `Requires=` or `BindsTo=`, because those would unmount the file system on
every restart of the daemon.

**Detach.** Unmount, then run `pando detach --device /dev/ublkbN` (with `--dm` if there is one).
The daemon publishes the branch and exits 0, and the unit stops without a restart. Then run
`systemctl disable pando-attach@<name>` and remove the env file, so a reboot does not attach the
branch again.

**Drain (SIGTERM or SIGINT).** The daemon stops serving control requests and publishes what
the branch holds, snapshotting a handed-off image first, while the device keeps serving. It waits
at most `--drain-wait` (60 s) for the store to hold every write acknowledged before the signal.
A second signal, a failed upload or the deadline ends the wait early. Either way the daemon then
commits every write acknowledged so far durably on this host. It exits with status 75 and leaves
the device in place: the kernel holds its I/O until the next daemon takes over, so a mounted file
system waits rather than fails. If even the local commit fails, it exits with status 1, and the
next daemon replays the journal. A signal that comes before the device serves ends the daemon at
once, as a crash would. `systemctl stop` alone leaves the device waiting; `detach` is what ends a
branch.

**Upgrade.** Run `install.sh --restart <version>`. It switches `/usr/local/bin/pando` to the
new release in one rename, so commands started from then on run the new binary while running
daemons keep theirs. It then restarts each running `pando-attach@` unit in turn: the old daemon
drains, and the new binary takes the device over with `--recover=auto`. I/O pauses for the drain
and the takeover; a mounted file system waits rather than fails. Without `--restart`, each daemon
moves to the new binary at its next restart. To roll back, run `install.sh --restart` with the
earlier version: it is still in `/usr/local/lib/pando` and is not fetched again. Remove a
release's directory once no daemon runs it (`ls -l /proc/*/exe | grep /usr/local/lib/pando`).

**Crash.** systemd starts a new daemon at once, and `--recover=auto` takes the device over with
the requests in flight. The new daemon replays the journal of acknowledged writes, so none is
lost. The same happens when something other than systemd sends SIGTERM (status 75 forces a
restart).

**Reboot.** At shutdown, systemd unmounts the file systems over the device first, which flushes
them, and then stops the unit, which drains it. At boot the enabled unit starts again and finds
no device, so `--recover=auto` adds a new one. It serves the branch from what the host kept:
the state and journal under `--state`, and after a handoff the image, which holds every write
since the handoff. Writes the store had not received are published once the branch opens.
The file systems are mounted again after the unit starts.

A power loss or kernel panic skips the drain. Writes the host made durable survive, which
covers every write acknowledged before a flush (FLUSH or FUA), as on a disk with a volatile
write cache. Writes after the last flush may be lost.

**Store outage.** While the store does not answer, the device serves what the host holds: reads
of chunks it has fetched, most writes, and flushes, which are durable on the host. A read or
write that needs the store fails with EIO after `--store-wait` (30 s) instead of hanging, so no
process waits in D state. A write needs it when it covers part of a chunk the host has not
fetched, or lands in a region of the branch's index the host has not read. The fetch goes on,
and a retry once the store answers finds it done. With `--log strict`, a flush the log does not
take within the wait fails the same way. Uploads retry with a backoff that grows to 30 s between
tries. Once they fall 60 s behind the flushes, the daemon logs it and sets its systemd status to
`degraded`; it logs again when they catch up. Until then the writes are on this host only, as
in [a host that never comes back](#a-host-that-never-comes-back). A `detach` during an outage
fails after `--publish-wait`, with the device stopped and the daemon still publishing; run
`detach` again to wait for it.

**Full disk.** The cache directory keeps a 128 MiB file, `reserve`, that holds room for
commits. When a write finds the disk of the cache or the state full, the daemon removes the
reserve, logs that the disk is full and sets its systemd status to `degraded`. From then on
writes and discards fail at once with ENOSPC and change nothing, while reads and flushes go on:
a flush commits into the reserve's room, so every write acknowledged before it stays durable.
A file system on the device usually turns read-only at the first failed write of its own
journal; unmount it, and mount it again once there is room. Once the disk has 256 MiB free, the
daemon allocates the reserve again, logs that the disk has room and takes writes; until then a
`detach` still publishes. The reserve covers the cache's file system only: keep `--state` on
the same one. Reads of chunks this host has not fetched fail with EIO while the disk is full,
since the cache cannot keep them.

**Cache budget.** Every chunk this host fetches or writes stays in the cache's object log while
a branch on the host still reads it, so without a limit the log grows to the data its branches
have touched. `--cache-budget SIZE` (a global flag, such as `--cache-budget 200G` in
`PANDO_ARGS`) limits the disk it takes. Each sweep keeps what was read since the last one,
then the tree nodes, then the chunks read most recently, and drops the rest, which the next
read of them fetches from the store again. A sweep runs whenever the log nears the budget, and
leaves it within seven eighths of it. Writes not uploaded yet always stay, so the log passes the
budget while uploads fall behind, and so do the writes a branch had not uploaded when it was
attached, until its next attach. The journal, the state and the 128 MiB reserve come on top of
the budget; leave room for them, and for writes during an outage, on the cache's disk.

## A host that never comes back

The store holds the branch as of its last upload. Before a handoff the daemon uploads within
about 2 s of each flush; after a handoff it uploads each snapshot, taken every `--snapshot` (15 s)
or after `--snapshot-bytes` (64 MiB) of writes. Writes made since then are on the lost host only:
in its journal, or in its image after a handoff. They are lost with it.

Attaching the branch on another host opens it at the last published root and takes a new writer
generation, which fences the lost host if it ever returns: its uploads fail, and it cannot change
the branch.

Nothing reports how many writes were lost yet. The ref records only what was published, and the
upload lag of each branch is not exported. Until it is, the bound above is the only figure.
