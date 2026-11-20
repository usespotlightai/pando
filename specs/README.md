# Specs

`WriteLog.tla` is a TLA+ specification of one branch's write log: the open, the scan and seals,
the generation take, the shipper, the uploader, the trims, `close_log` and `pando advance`, with
hosts that crash, lose their disk and race each other on the store. TLC checks it on small models, and traces of
the engine from `pando-dst` are checked against it, so the spec stays the protocol the code
runs.

## What it checks

A branch's contents are modelled by their history, the sequence of flushes that made them. A
writes record holds its writer's history at flush `from` and at flush `to`. Applying the record
to a state between those two gives the state at `to`; applying it to any other state mixes two
histories, which on a real disk is a file system that no flush ever produced.

| Invariant | Meaning |
|---|---|
| `OpenSeesAPrefix` | An open never mixes histories: what it replays is a state some writer committed. This is the "an open always sees a prefix of the flushes" in `docs/architecture.md`. |
| `StrictFlushesSurvive` | A writer that takes the branch holds every strict flush that any writer returned from before the take. |
| `RefHoldsAWriterState` | The ref only ever holds a state some writer committed. |
| `AdvanceDropsWhatItReplaced` | A writer that takes the branch after an advance holds no flush older than the advance, unless the root the advance moved to holds it. |

Each action names the code it models:

| Action | Code |
|---|---|
| `Open`, `Resumed` | `Volume::open_logged`, `VolState::resumes`, `Inner::resumed_ahead` |
| `ScanRead`, `ScanSeal`, `Replay` | `log::scan`, `Inner::replay_log` |
| `Take` | `Inner::take_generation` |
| `Write` | a write to the volume, kept by the host's journal unless the host fails first; `Inner::start_journal` gives a resumed writer the journal's writes back |
| `Commit` | `Inner::commit_once`: a flush, or a checkpoint's or a detach's commit; `logged = FALSE` is one the queue drops and leaves to the uploader |
| `ShipBatch`, `ShipNumber`, `ShipCreate`, `ShipFence`, `ShipCovered`, `ShipAbandon` | `Log::take_batch`, `Inner::shipper`, `Inner::put_record` |
| `UploadStart`, `UploadPut` | `Inner::uploader`, `Inner::upload`, `Inner::log_pos` |
| `TrimOwn`, `TrimListOlder`, `TrimDeleteOlder` | `Inner::trim_log` |
| `CloseStop`, `ClosePut` | `Inner::close_log` |
| `Ack` | `Inner::wait_logged`, the return of a strict flush |
| `Stop`, `Lose` | a crash, kill or detach; a host lost with its disk |
| `AdvanceRead`, `AdvanceScan`, `AdvanceSeal`, `AdvancePut`, `AdvanceStop` | `branch::advance`, `log::seal` |

Not modelled: transient store errors (a failed call is retried or ends the process, which
`Stop` covers), the contents of segments and checkpoints, deleting a branch and making a new one
under the same name, and more than one branch.

## Running

`tools/tlc.sh` runs TLC in a pinned Java container, with a `tla2tools.jar` checked against the
SHA-256 in the script. On CI it uses the runner's Java.

```bash
tools/tlc.sh
```

That checks every model in `specs/*.cfg`, the ones a pull request runs. `WriteLog.cfg` has two
hosts, two writes, three opens and strict mode, and checks every behaviour of up to 24 steps:
about 3.8 million distinct states. `WriteLog-advance.cfg` adds one advance with two opens, to 16
steps: about 1.1 million states.

```bash
tools/tlc.sh nightly
```

That checks `specs/nightly/*.cfg`: every behaviour of the first model with no bound on its
length, both log modes, three hosts, and an advance with three opens. `deep.yml` runs them each
night.

A single model takes TLC's arguments after it, such as `-depth` or `-simulate`:

```bash
tools/tlc.sh specs/WriteLog.cfg -simulate num=100000
```

A violation prints the behaviour that breaks the invariant, one state per step.

## Traces of the engine

The race scenario in `pando-dst/tests/scenarios.rs` runs two hosts that open, flush, checkpoint,
detach, crash and lose their disk on one branch, over a simulated store with latency. A third
host advances the branch now and then, to a new empty branch or to a copy of what it published. With
`PANDO_TLA_TRACES` set, each seed also writes every call on the branch's ref and log, in the
order the store applied them, and every write and flush of a writer, as
`race-<seed>/TraceData.tla`.
`trace/WriteLogTrace.tla` maps each event to one action of the spec with the same host, record,
flushes, log position and outcome, and lets the actions no call shows run in between. TLC then
searches for a behaviour of the spec that matches the whole trace.

```bash
PANDO_DST_SEEDS=40 PANDO_TLA_TRACES=/w/target/tla-traces tools/cargo.sh test -p pando-dst --test scenarios racing
```

```bash
tools/tlc.sh traces target/tla-traces
```

`tools/cargo.sh` mounts the repository at `/w`, hence the path in the first command. A rejected
trace is one the spec does not allow: either the code does something the spec does not model or
the spec is wrong. The last `matched` line names the first event that no behaviour reaches. The
race runs without store faults, and it fails when a trace holds a call the spec has no action
for, such as an unconditional put of the ref.

## Changing the protocol

A change to the log, the take, the upload or the close changes the spec in the same pull
request: update the action and its comment, run `tools/tlc.sh`, then the traces. A new
invariant or a weaker guard should first fail on the model it is meant to catch, so the check is
known to see it.

## Findings

- **A resumed root the ref does not hold.** A host that resumed its own state ahead of the ref
  logged its next flush on top of that state, and an open on another host applied the record to
  the ref's root, without the resumed flushes. The other host then saw a later flush without an
  earlier one. TLC found it in 15 steps. The fix is `Inner::resumed_ahead`, which leaves the
  resumed root to the uploader, so the log takes no flush until an upload publishes that root.
  The unit test
  `a_writer_that_resumes_unlogged_flushes_logs_no_flush_without_them` and the race scenario
  both fail without the fix.
- **`pando advance` onto a branch with a live or unclosed log.** `advance` moved the ref's root
  and kept its log position, so the next open applied the old writer's records to the new root.
  TLC found it in 10 steps. Sealing the log is not enough on its own: when the new root is one
  the old writer had published, the old writer's host resumes its local state on its next open,
  flushes the advance dropped included, since the ref still names its generation. `advance` now
  seals the writer's log (`log::seal`), so a writer still running fails its next append, and
  moves the ref to a new generation whose log is closed. Without the seal TLC breaks
  `StrictFlushesSurvive` in 13 steps; without the new generation it breaks
  `AdvanceDropsWhatItReplaced` in 11. The unit tests
  `an_advance_drops_and_fences_a_running_writer` and
  `a_host_does_not_resume_what_an_advance_dropped` each fail without their half of the fix, and
  the race scenario fails at seed 0 on the old `advance` and on one that keeps the generation.
