# Running Pando on a host

Each attached branch is one instance of the systemd template unit
[`pando-attach@.service`](../packaging/systemd/pando-attach@.service). The unit restarts a
daemon that crashed or drained, and it attaches the branch again after a reboot.

## Install

| Path | Holds |
|---|---|
| `/usr/local/bin/pando` | the binary |
| `/etc/systemd/system/pando-attach@.service` | the unit |
| `/etc/pando/pando.env` | the host's settings, mode 0600: `S3_BUCKET`, `S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, and `PANDO_ARGS` for global flags such as `--cache` |
| `/etc/pando/attach/<name>.env` | one branch: `PANDO_BRANCH`, `PANDO_DEVICE` (`/dev/ublkbN`), and `PANDO_ATTACH_ARGS` for the other `attach` flags, such as `--remote`, `--dm`, `--images` and `--state` |

Credentials go in the env file, never in `PANDO_ARGS` or `PANDO_ATTACH_ARGS`: those end up on
the command line.

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
A second signal, a failed upload or the deadline ends the wait early; the daemon then commits the
writes durably on this host instead. It exits with status 75 and leaves the device in place: the
kernel holds its I/O until the next daemon takes over, so a mounted file system waits rather than
fails. If even the local commit fails, it exits with status 1, and the next daemon replays the
journal. `systemctl stop` alone leaves the device waiting; `detach` is what ends a branch.

**Upgrade.** Replace the binary, then run `systemctl restart pando-attach@<name>` for each
branch. The old daemon drains, and the new binary takes the device over with `--recover=auto`.
I/O pauses for the drain and the takeover.

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
