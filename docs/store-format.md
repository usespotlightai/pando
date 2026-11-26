# Store format versions

Every store holds one marker object, `format`, at its prefix root:

```
pando-format 1.0
```

## What each command does with it

Every command that opens the store reads the marker before it reads or writes anything else in
the store. Commands that open the cache (`attach`, `import`, `base`, `follow`, `gc`, `stat`)
read it while the cache opens, so it adds no time. Opening the cache recovers this host's
object log and touches nothing in the store, so a refused command may have done that much. The
others (`init`, `refs`, `branch`, `checkpoint`, `checkpoints`, `delete-checkpoint`, `advance`,
`delete`) wait for one more GET.

- A command that writes (`init`, `import`, `branch`, `checkpoint`, `delete-checkpoint`,
  `advance`, `delete`, `base`, `follow`, `gc --delete`, `attach`) creates the marker when the
  store has none. If another host creates it first, the command checks that host's marker
  instead.
- A command that only reads (`refs`, `checkpoints`, `stat`, `gc` without `--delete`) never
  writes it.
- `detach` and `resize` talk only to the daemon of an attached device and skip the check.
- The host daemon (`pando serve`) checks the marker for the first command it runs that reads
  and for the first that writes, and not again: the commands after them pay no extra GET.
- A store without a marker is one no build has written to, and is in this build's format.
- A marker with a major version this build does not read stops the command before it touches
  anything else in the store: `the store is in format 2.0; this pando reads format 1.x`.

## Upgrade rules

A build reads and writes every store whose major version equals its own, whatever the minor
version.

**Minor version** (1.0 to 1.1): only additions that a build of an older minor version can
ignore, such as:

- a new kind of object under a new key prefix;
- a new line after the first in a text object (refs, the marker), which older decoders skip.

An older build keeps working on a store with a newer minor version. A newer build writes its
own minor version only when it creates the marker; it does not rewrite an existing one.

**Major version** (1.x to 2.0): any other change to the bytes of a stored object: entries,
refs, checkpoints, write log records, tree nodes, frames, segment indexes, recipes, change
lists or recordings, or the key layout. Builds of the old major version refuse the store, so they cannot misread it or mix old
objects into it.

A major change needs:

1. new golden fixtures, checked in both directions (`tools/golden/run.sh write`, then `read`);
2. a migration, or a new store, for the data in existing stores;
3. a new marker, written last, once the store holds only objects of the new format.

## Format 1.0

Each ref has an `id` line after its first line: the branch id, 32 hex digits, drawn at random
when the ref is created. A branch made again under a deleted branch's name gets another, so its
ref's bytes, and so its ETag, differ from the deleted one's. The write log of a branch is at
`log/<hash of the branch name>/<branch id>/<writer generation>/<n>`, so each life of a branch has
a log of its own.

A ref that a writer took has a `writer` line last: 16 hex digits the writer drew at random when
it took the branch. A host that died as it put the ref reads it back and knows whether the
writer of that generation is its own, or a rival's that took the same generation first.

Each kind of object does not carry its own version yet. Add one when a change has to let objects
of two formats live in the same store.
