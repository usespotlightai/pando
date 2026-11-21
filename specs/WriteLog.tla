------------------------------ MODULE WriteLog ------------------------------
(***************************************************************************)
(* The write log of one branch, as pando-engine/src/volume/log.rs and the  *)
(* open, generation take, upload and close in volume/mod.rs run it, and    *)
(* as branch::advance moves the branch elsewhere and branch::delete       *)
(* deletes it to make it again, with hosts that crash, lose their disk,    *)
(* and race each other on the store.                                       *)
(*                                                                         *)
(* A branch's contents are modelled by their history: the sequence of the  *)
(* flushes that made them. A writes record holds the history of its        *)
(* writer at flush `from` and at flush `to`; applying it to a state between *)
(* the two gives the state at `to`, and applying it to any other state     *)
(* mixes two histories, which on a real disk is a file system no flush ever *)
(* produced.                                                               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Hosts,          \* hosts that open the branch, one process each at a time
    Modes,          \* the log modes an open may take: "behind", "strict"
    MaxWrites,      \* writes the writers make in all, each a flush once committed
    MaxOpens,       \* opens in all
    MaxAdvances,    \* `pando advance` runs onto the branch
    MaxDeletes,     \* `pando delete` runs, each followed by a create under the same name
    Depth           \* the longest behaviour checked, in steps; 0 for every behaviour

VARIABLES
    ref,        \* the branch's ref: its life's id, root, writer generation, log position,
                \* and whether it exists
    store,      \* the log records in the store, of every life of the branch
    disk,       \* each host's local state, which a later open on that host may resume
    proc,       \* each host's process
    nextId,     \* the next flush id
    opens,      \* opens so far
    advances,   \* `pando advance`: how many ran, the one running, and the first flush
                \* not dropped by the last one
    acked,      \* strict flushes some writer returned from
    histories,  \* every history a writer committed or a ref held
    deleted     \* `pando delete`: how many ran, the one running, the branch's lives so
                \* far, and the writes made on a branch since deleted. A life's id is
                \* its number

vars == <<ref, store, disk, proc, nextId, opens, advances, acked, histories, deleted>>

Max(a, b) == IF a >= b THEN a ELSE b
Last(s) == s[Len(s)]
Range(s) == {s[i] : i \in DOMAIN s}
IsPrefix(a, b) == Len(a) <= Len(b) /\ SubSeq(b, 1, Len(a)) = a

Pos(g, f, n, c) == [gen |-> g, flush |-> f, next |-> n, closed |-> c]

\* The version a conditional PUT names. On S3 it is the ETag, a hash of the ref's
\* bytes, so two refs with the same contents have the same version. A ref's
\* contents include its life's id.
Ver(r) == <<r.id, r.root, r.gen, r.log>>
NewRef(id, root, gen, log) ==
    [id |-> id, root |-> root, gen |-> gen, log |-> log, exists |-> TRUE]

\* Record n of writer g of life l, at keys::log_record(name, l, g, n).
Seal(l, g, n) ==
    [l |-> l, g |-> g, n |-> n, kind |-> "seal", from |-> 0, to |-> 0, base |-> <<>>,
     hist |-> <<>>]
Writes(l, g, n, from, to, base, hist) ==
    [l |-> l, g |-> g, n |-> n, kind |-> "writes", from |-> from, to |-> to, base |-> base,
     hist |-> hist]

At(l, g, n) == {r \in store : r.l = l /\ r.g = g /\ r.n = n}

\* A host's local state: the root it committed last, the writes its journal
\* holds since, what it published, and the writer and life that wrote it.
NoDisk ==
    [present |-> FALSE, root |-> <<>>, pending |-> <<>>, published |-> <<>>,
     publishing |-> <<>>, gen |-> 0, life |-> 0]

Idle ==
    [pc |-> "idle",
     \* the ref the open read
     rroot |-> <<>>, rgen |-> 0, rver |-> <<>>, start |-> Pos(0, 0, 0, FALSE),
     \* the scan: generation, record number, last flush applied, gap, state
     g |-> 0, n |-> 0, have |-> 0, gap |-> FALSE, st |-> <<>>, mixed |-> FALSE,
     replayed |-> FALSE,
     \* the writer
     gen |-> 0, mode |-> "behind", hists |-> <<<<>>>>, seq |-> 0, opened |-> <<>>,
     \* the writes in memory no commit has taken yet
     pending |-> <<>>,
     logged |-> 0, uploaded |-> 0, upRoot |-> <<>>, dropped |-> 0, records |-> <<>>,
     batching |-> FALSE, shipping |-> FALSE, shipN |-> 0, shipFrom |-> 0, shipTo |-> 0,
     refused |-> FALSE, shipperDone |-> FALSE,
     upping |-> FALSE, upSeq |-> 0, ver |-> <<>>,
     covered |-> 0, published |-> FALSE, olderTrimmed |-> FALSE,
     toDelete |-> {}, stopped |-> FALSE, closing |-> FALSE, closeSeq |-> 0,
     fenced |-> FALSE, waiting |-> {}, ackedAtTake |-> {},
     \* the last advance before the take: its root and first flush not dropped
     advRoot |-> <<>>, advFloor |-> 0,
     \* the life of the branch the open read, whose log the writer reads and writes
     life |-> 0]

\* The state after flush i of writer p.
H(p, i) == p.hists[i + 1]

\* Log.records.partition_point: the records whose last flush is at or below s.
Covering(p, s) == Len(SelectSeq(p.records, LAMBDA t : t <= s))

NoAdvance ==
    [count |-> 0, pc |-> "idle", ver |-> <<>>, life |-> 0, gen |-> 0, n |-> 0, floor |-> 0,
     root |-> <<>>]

NoDelete == [count |-> 0, pc |-> "idle", life |-> 0, keys |-> {}, lives |-> 0, old |-> {}]


Init ==
    /\ ref = NewRef(0, <<>>, 0, Pos(0, 0, 0, FALSE))
    /\ store = {}
    /\ disk = [h \in Hosts |-> NoDisk]
    /\ proc = [h \in Hosts |-> Idle]
    /\ nextId = 1
    /\ opens = 0
    /\ advances = NoAdvance
    /\ acked = {}
    /\ histories = {<<>>}
    /\ deleted = NoDelete

-----------------------------------------------------------------------------
(* Opening: Volume::open_logged, log::scan, Inner::take_generation.        *)

\* VolState::resumes: the ref is of the life this host's state wrote, holds the
\* root this host published or was publishing, and names the generation this
\* host's state names.
Resumes(d, r) ==
    /\ d.present
    /\ d.life = r.id
    /\ d.published = r.root \/ d.publishing = r.root
    /\ d.gen = r.gen

\* Where the scan goes after generation g-1: a generation past the ref's ends it.
EnterGen(p, g) ==
    IF g > p.rgen
    THEN [p EXCEPT !.pc = "replay"]
    ELSE [p EXCEPT !.pc = "scan", !.g = g,
                   !.n = IF g = p.start.gen THEN p.start.next ELSE 0,
                   !.have = IF g = p.start.gen THEN p.start.flush ELSE 0]

FirstGen(start) ==
    LET g0 == Max(start.gen, 1)
    IN IF g0 = start.gen /\ start.closed THEN g0 + 1 ELSE g0

\* A resumed state the ref does not hold is the writer's flush 1, left to the
\* uploader (Inner::resumed_ahead).
Resumed(p, d) ==
    IF d.root = p.rroot
    THEN [p EXCEPT !.pc = "take", !.hists = <<d.root>>, !.opened = d.root, !.st = d.root,
                   !.pending = d.pending]
    ELSE [p EXCEPT !.pc = "take", !.hists = <<p.rroot, d.root>>, !.seq = 1, !.dropped = 1,
                   !.opened = d.root, !.st = d.root, !.pending = d.pending]

\* An open reads the ref, and either resumes this host's state or scans the log.
Open(h, m) ==
    /\ proc[h].pc = "idle"
    /\ ref.exists
    /\ opens < MaxOpens
    /\ opens' = opens + 1
    /\ LET r == ref
           base == [Idle EXCEPT !.life = r.id, !.rroot = r.root, !.rgen = r.gen, !.rver = Ver(r),
                                !.start = r.log, !.gen = r.gen + 1, !.mode = m,
                                !.st = r.root, !.upRoot = r.root]
       IN IF Resumes(disk[h], r)
          THEN proc' = [proc EXCEPT ![h] = Resumed(base, disk[h])]
          ELSE IF r.gen = 0 \/ (r.log.closed /\ r.log.gen = r.gen)
          THEN proc' = [proc EXCEPT ![h] = [base EXCEPT !.pc = "replay"]]
          ELSE proc' = [proc EXCEPT ![h] = EnterGen(base, FirstGen(r.log))]
    /\ UNCHANGED <<deleted, ref, store, disk, nextId, advances, acked, histories>>

\* One GET of record (g, n): a writes record is applied, skipped, or past a gap;
\* a seal ends the generation; a missing record is sealed next.
ScanRead(h) ==
    LET p == proc[h]
        found == At(p.life, p.g, p.n)
    IN /\ p.pc = "scan"
       /\ IF found = {}
          THEN proc' = [proc EXCEPT ![h].pc = "seal"]
          ELSE LET rec == CHOOSE x \in found : TRUE
                   p1 == [p EXCEPT !.n = p.n + 1]
                   gap == p.gap \/ rec.from > p.have
                   fits == IsPrefix(rec.base, p.st) /\ IsPrefix(p.st, rec.hist)
               IN proc' = [proc EXCEPT ![h] =
                    IF rec.kind = "seal" THEN EnterGen(p, p.g + 1)
                    ELSE IF rec.to <= p.have THEN p1
                    ELSE IF gap THEN [p1 EXCEPT !.gap = TRUE]
                    ELSE [p1 EXCEPT !.have = rec.to, !.replayed = TRUE,
                                    !.st = IF fits THEN rec.hist ELSE p.st,
                                    !.mixed = p.mixed \/ ~fits]]
       /\ UNCHANGED <<deleted, ref, store, disk, nextId, opens, advances, acked, histories>>

\* The conditional create of a seal at the first free number; when another
\* record took the number first, the scan reads it.
ScanSeal(h) ==
    LET p == proc[h]
    IN /\ p.pc = "seal"
       /\ IF At(p.life, p.g, p.n) = {}
          THEN /\ store' = store \cup {Seal(p.life, p.g, p.n)}
               /\ proc' = [proc EXCEPT ![h] = EnterGen(p, p.g + 1)]
          ELSE /\ proc' = [proc EXCEPT ![h].pc = "scan"]
               /\ UNCHANGED store
       /\ UNCHANGED <<deleted, ref, disk, nextId, opens, advances, acked, histories>>

\* Inner::start_journal starts afresh, and Inner::replay_log commits what the
\* scan applied as the writer's flush 1.
Replay(h) ==
    LET p == proc[h]
        hists == IF p.replayed THEN <<p.rroot, p.st>> ELSE <<p.rroot>>
    IN /\ p.pc = "replay"
       /\ proc' = [proc EXCEPT ![h] =
                    [p EXCEPT !.pc = "take", !.hists = hists, !.seq = Len(hists) - 1,
                              !.opened = p.st]]
       /\ disk' = [disk EXCEPT ![h] =
                    [present |-> TRUE, root |-> p.st, pending |-> <<>>, published |-> p.rroot,
                     publishing |-> <<>>, gen |-> p.rgen, life |-> p.life]]
       /\ UNCHANGED <<deleted, ref, store, nextId, opens, advances, acked, histories>>

\* The conditional PUT of the ref that takes the next writer generation.
Take(h) ==
    LET p == proc[h]
    IN /\ p.pc = "take"
       /\ IF ref.exists /\ Ver(ref) = p.rver
          THEN /\ ref' = [ref EXCEPT !.gen = p.gen, !.log = p.start]
               /\ proc' = [proc EXCEPT ![h] =
                            [p EXCEPT !.pc = "run", !.ver = Ver(ref'), !.ackedAtTake = acked,
                                      !.advRoot = advances.root, !.advFloor = advances.floor,
                                      !.rroot = <<>>, !.rgen = 0, !.rver = <<>>,
                                      !.start = Pos(0, 0, 0, FALSE), !.g = 0, !.n = 0,
                                      !.have = 0, !.gap = FALSE, !.st = <<>>,
                                      !.replayed = FALSE]]
               /\ disk' = [disk EXCEPT ![h].gen = p.gen]
          ELSE /\ proc' = [proc EXCEPT ![h] = Idle]
               /\ UNCHANGED <<deleted, ref, disk>>
       /\ UNCHANGED <<deleted, store, nextId, opens, advances, acked, histories>>

-----------------------------------------------------------------------------
(* A running writer: Inner::commit_once, the shipper, the uploader,        *)
(* trim_log, close_log and wait_logged.                                    *)

Running(h) == proc[h].pc = "run"
Live(h) == Running(h) /\ ~proc[h].fenced

\* A write lands in the process's memory and, unless the host fails first, in
\* its journal. It takes the next flush id.
Write(h, kept) ==
    /\ Running(h)
    /\ nextId <= MaxWrites
    /\ proc' = [proc EXCEPT ![h].pending = Append(@, nextId)]
    /\ disk' = IF kept THEN [disk EXCEPT ![h].pending = Append(@, nextId)] ELSE disk
    /\ nextId' = nextId + 1
    /\ deleted' = IF proc[h].life < deleted.lives
                  THEN [deleted EXCEPT !.old = @ \cup {nextId}] ELSE deleted
    /\ UNCHANGED <<ref, store, opens, advances, acked, histories>>

\* A commit takes every write in memory, locally and durably. A flush is a commit
\* that returns to its caller, who waits for the log in strict mode; a commit
\* with no caller is a checkpoint's or a detach's. A commit that bounds memory,
\* or is too large for a record, is left to the uploader: the queue drops it and
\* what waits before it.
Commit(h, logged, flush) ==
    LET p == proc[h]
        now == H(p, p.seq) \o p.pending
    IN /\ Live(h)
       /\ p.pending /= <<>>
       /\ proc' = [proc EXCEPT ![h] =
                    [p EXCEPT !.hists = Append(p.hists, now), !.seq = p.seq + 1, !.pending = <<>>,
                              !.dropped = IF logged THEN p.dropped ELSE p.seq + 1,
                              !.waiting = IF flush /\ p.mode = "strict"
                                          THEN p.waiting \cup {<<p.seq + 1, Last(p.pending)>>}
                                          ELSE p.waiting]]
       /\ disk' = [disk EXCEPT ![h].root = now, ![h].pending = <<>>]
       /\ histories' = histories \cup {now}
       /\ UNCHANGED <<deleted, ref, store, nextId, opens, advances, acked>>

ShipBase(p) == Max(p.logged, p.uploaded)

\* The shipper runs until a create of its own fails or the log stops; it does not
\* look for a fence the uploader found.
Shipper(h) == Running(h) /\ ~proc[h].shipperDone

ShipIdle(p) == ~p.batching /\ ~p.shipping /\ ~p.stopped

\* The shipper takes the flushes after its base into one batch.
ShipBatch(h, to) ==
    LET p == proc[h]
        base == ShipBase(p)
    IN /\ Shipper(h)
       /\ ShipIdle(p)
       /\ p.dropped <= base
       /\ base < to /\ to <= p.seq
       /\ proc' = [proc EXCEPT ![h] =
                    [p EXCEPT !.batching = TRUE, !.shipFrom = base, !.shipTo = to]]
       /\ UNCHANGED <<deleted, ref, store, disk, nextId, opens, advances, acked, histories>>

\* Once the batch is encoded, the shipper takes the record's number before the
\* create starts, unless the log stopped meanwhile.
ShipNumber(h) ==
    LET p == proc[h]
    IN /\ Running(h)
       /\ p.batching
       /\ proc' = [proc EXCEPT ![h] =
                    IF p.stopped
                    THEN [p EXCEPT !.batching = FALSE, !.shipFrom = 0, !.shipTo = 0]
                    ELSE [p EXCEPT !.batching = FALSE, !.shipping = TRUE,
                                   !.shipN = Len(p.records), !.records = Append(p.records, p.shipTo)]]
       /\ UNCHANGED <<deleted, ref, store, disk, nextId, opens, advances, acked, histories>>

\* Flushes the queue dropped are covered once an upload published them.
ShipCovered(h) ==
    LET p == proc[h]
        base == ShipBase(p)
    IN /\ Shipper(h)
       /\ ShipIdle(p)
       /\ p.dropped > p.logged /\ p.dropped <= base /\ p.seq <= base
       /\ proc' = [proc EXCEPT ![h].logged = base]
       /\ UNCHANGED <<deleted, ref, store, disk, nextId, opens, advances, acked, histories>>

\* The conditional create of the record. When another record took the number,
\* the shipper reads that record back before it fences the writer.
ShipCreate(h) ==
    LET p == proc[h]
    IN /\ Running(h)
       /\ p.shipping /\ ~p.refused
       /\ IF At(p.life, p.gen, p.shipN) = {}
          THEN /\ store' = store \cup
                    {Writes(p.life, p.gen, p.shipN, p.shipFrom, p.shipTo, H(p, p.shipFrom),
                            H(p, p.shipTo))}
               /\ proc' = [proc EXCEPT ![h] =
                            [p EXCEPT !.shipping = FALSE, !.logged = Max(p.logged, p.shipTo),
                                      !.shipN = 0, !.shipFrom = 0, !.shipTo = 0]]
          ELSE /\ proc' = [proc EXCEPT ![h].refused = TRUE]
               /\ UNCHANGED store
       /\ UNCHANGED <<deleted, ref, disk, nextId, opens, advances, acked, histories>>

\* The read-back finds another record: the writer is fenced and the shipper ends.
ShipFence(h) ==
    /\ Running(h)
    /\ proc[h].shipping /\ proc[h].refused
    /\ proc' = [proc EXCEPT ![h] =
                 [@ EXCEPT !.shipping = FALSE, !.refused = FALSE, !.fenced = TRUE,
                           !.shipperDone = TRUE, !.shipN = 0, !.shipFrom = 0, !.shipTo = 0]]
    /\ UNCHANGED <<deleted, ref, store, disk, nextId, opens, advances, acked, histories>>

\* A create that failed transiently gives up once the log stopped, and the
\* shipper ends.
ShipAbandon(h) ==
    /\ Running(h)
    /\ proc[h].shipping /\ proc[h].stopped /\ ~proc[h].refused
    /\ proc' = [proc EXCEPT ![h] =
                 [@ EXCEPT !.shipping = FALSE, !.shipperDone = TRUE,
                           !.shipN = 0, !.shipFrom = 0, !.shipTo = 0]]
    /\ UNCHANGED <<deleted, ref, store, disk, nextId, opens, advances, acked, histories>>

\* The uploader packs the current root and records it as publishing.
UploadStart(h) ==
    LET p == proc[h]
    IN /\ Live(h)
       /\ ~p.upping
       /\ H(p, p.seq) /= p.upRoot
       /\ proc' = [proc EXCEPT ![h] = [p EXCEPT !.upping = TRUE, !.upSeq = p.seq]]
       /\ disk' = [disk EXCEPT ![h].publishing = H(p, p.seq)]
       /\ UNCHANGED <<deleted, ref, store, nextId, opens, advances, acked, histories>>

\* The conditional PUT of the ref with the packed root and the log position.
UploadPut(h) ==
    LET p == proc[h]
        root == H(p, p.upSeq)
        pos == Pos(p.gen, p.upSeq, Covering(p, p.upSeq), p.stopped)
    IN /\ Running(h)
       /\ p.upping
       /\ IF ref.exists /\ Ver(ref) = p.ver
          THEN /\ ref' = NewRef(p.life, root, p.gen, pos)
               /\ proc' = [proc EXCEPT ![h] =
                            [p EXCEPT !.upping = FALSE, !.upSeq = 0, !.ver = Ver(ref'),
                                      !.uploaded = Max(p.uploaded, p.upSeq), !.upRoot = root,
                                      !.covered = Max(p.covered, pos.next), !.published = TRUE]]
               /\ disk' = [disk EXCEPT ![h].published = root, ![h].publishing = <<>>]
               /\ histories' = histories \cup {root}
          ELSE /\ proc' = [proc EXCEPT ![h] =
                            [p EXCEPT !.upping = FALSE, !.upSeq = 0, !.fenced = TRUE]]
               /\ UNCHANGED <<deleted, ref, disk, histories>>
       /\ UNCHANGED <<deleted, store, nextId, opens, advances, acked>>

\* The writer deletes a record the published ref covers. Trims run from the
\* shipper and from close_log at once, so a record may be deleted again, and a
\* shipper fenced by the uploader still trims.
TrimOwn(h, n) ==
    /\ Running(h)
    /\ n < proc[h].covered
    /\ store' = store \ At(proc[h].life, proc[h].gen, n)
    /\ UNCHANGED <<deleted, proc, ref, disk, nextId, opens, advances, acked, histories>>

\* Once it published, the writer lists its life's log and picks what earlier
\* writers left, except the seal that ends each earlier writer's log.
TrimListOlder(h) ==
    LET p == proc[h]
        older == {r \in store : r.l = p.life /\ r.g < p.gen}
        last(g) == CHOOSE n \in {r.n : r \in {x \in older : x.g = g}} :
                     \A x \in older : x.g = g => x.n <= n
        kept == {r \in older : r.kind = "seal" /\ r.n = last(r.g)}
    IN /\ Running(h)
       /\ p.published /\ ~p.olderTrimmed
       /\ proc' = [proc EXCEPT ![h] =
                    [p EXCEPT !.olderTrimmed = TRUE,
                              !.toDelete = {<<r.g, r.n>> : r \in older \ kept}]]
       /\ UNCHANGED <<deleted, ref, store, disk, nextId, opens, advances, acked, histories>>

TrimDeleteOlder(h, k) ==
    /\ Running(h)
    /\ k \in proc[h].toDelete
    /\ store' = store \ At(proc[h].life, k[1], k[2])
    /\ proc' = [proc EXCEPT ![h].toDelete = @ \ {k}]
    /\ UNCHANGED <<deleted, ref, disk, nextId, opens, advances, acked, histories>>

\* close_log, once every flush is published: the log stops, and once no record
\* is in flight the ref records that this writer appends no more.
CloseStop(h) ==
    LET p == proc[h]
    IN /\ Running(h)
       /\ ~p.stopped
       /\ p.uploaded >= p.seq
       /\ proc' = [proc EXCEPT ![h] =
                    [p EXCEPT !.stopped = TRUE, !.closing = TRUE, !.closeSeq = p.seq]]
       /\ UNCHANGED <<deleted, ref, store, disk, nextId, opens, advances, acked, histories>>

ClosePut(h) ==
    LET p == proc[h]
        pos == Pos(p.gen, p.closeSeq, Covering(p, p.closeSeq), TRUE)
    IN /\ Running(h)
       /\ p.closing /\ ~p.shipping
       /\ IF ref.exists /\ Ver(ref) = p.ver
          THEN /\ ref' = NewRef(p.life, p.upRoot, p.gen, pos)
               /\ proc' = [proc EXCEPT ![h] =
                            [p EXCEPT !.closing = FALSE, !.closeSeq = 0, !.ver = Ver(ref'),
                                      !.covered = Max(p.covered, pos.next), !.published = TRUE]]
          ELSE /\ proc' = [proc EXCEPT ![h] = [p EXCEPT !.closing = FALSE, !.closeSeq = 0]]
               /\ UNCHANGED ref
       /\ UNCHANGED <<deleted, store, disk, nextId, opens, advances, acked, histories>>

\* wait_logged: a strict flush returns once the log or an upload has it, even
\* if the writer was fenced since. A writer of a deleted branch's returns count
\* for nothing.
Ack(h, w) ==
    /\ Running(h)
    /\ w \in proc[h].waiting
    /\ proc[h].logged >= w[1] \/ proc[h].uploaded >= w[1]
    /\ acked' = IF w[2] >= advances.floor /\ proc[h].life = deleted.lives
                THEN acked \cup {w[2]} ELSE acked
    /\ proc' = [proc EXCEPT ![h].waiting = @ \ {w}]
    /\ UNCHANGED <<deleted, ref, store, disk, nextId, opens, advances, histories>>

-----------------------------------------------------------------------------
(* Failures and other commands.                                            *)

\* The process stops: a daemon crash, a kill, or a detach. The host's state stays.
Stop(h) ==
    /\ proc[h].pc /= "idle"
    /\ proc' = [proc EXCEPT ![h] = Idle]
    /\ UNCHANGED <<deleted, ref, store, disk, nextId, opens, advances, acked, histories>>

\* The host is lost with its disk.
Lose(h) ==
    /\ proc[h].pc /= "idle" \/ disk[h].present
    /\ proc' = [proc EXCEPT ![h] = Idle]
    /\ disk' = [disk EXCEPT ![h] = NoDisk]
    /\ UNCHANGED <<deleted, ref, store, nextId, opens, advances, acked, histories>>

\* branch::advance reads the ref, seals the writer's log, and moves the ref to
\* another branch's root under a new generation whose log is closed. What the
\* branch held is meant to go, strict flushes included: a flush that returns
\* after it counts only if it is newer than the advance.
AdvanceRead ==
    /\ advances.pc = "idle"
    /\ advances.count < MaxAdvances
    /\ ref.exists
    /\ advances' = [advances EXCEPT
                       !.count = @ + 1, !.ver = Ver(ref), !.life = ref.id, !.gen = ref.gen,
                       !.n = IF ref.log.gen = ref.gen THEN ref.log.next ELSE 0,
                       !.pc = IF ref.gen = 0 \/ (ref.log.closed /\ ref.log.gen = ref.gen)
                              THEN "put" ELSE "scan"]
    /\ UNCHANGED <<deleted, ref, store, disk, proc, nextId, opens, acked, histories>>

\* log::seal: one GET of the writer's record n; a seal ends the log.
AdvanceScan ==
    LET found == At(advances.life, advances.gen, advances.n)
    IN /\ advances.pc = "scan"
       /\ advances' = [advances EXCEPT
                          !.pc = IF found = {} THEN "seal"
                                 ELSE IF (CHOOSE r \in found : TRUE).kind = "seal" THEN "put"
                                 ELSE "scan",
                          !.n = IF found = {} THEN @ ELSE @ + 1]
       /\ UNCHANGED <<deleted, ref, store, disk, proc, nextId, opens, acked, histories>>

AdvanceSeal ==
    /\ advances.pc = "seal"
    /\ IF At(advances.life, advances.gen, advances.n) = {}
       THEN /\ store' = store \cup {Seal(advances.life, advances.gen, advances.n)}
            /\ advances' = [advances EXCEPT !.pc = "put"]
       ELSE /\ advances' = [advances EXCEPT !.pc = "scan"]
            /\ UNCHANGED store
    /\ UNCHANGED <<deleted, ref, disk, proc, nextId, opens, acked, histories>>

\* The conditional PUT, to a root no writer made or to one some writer did.
AdvancePut(root) ==
    /\ advances.pc = "put"
    /\ IF ref.exists /\ Ver(ref) = advances.ver
       THEN /\ ref' = NewRef(advances.life, root, advances.gen + 1,
                              Pos(advances.gen + 1, 0, 0, TRUE))
            /\ histories' = histories \cup {root}
            /\ acked' = {}
            /\ advances' = [advances EXCEPT !.pc = "idle", !.floor = nextId, !.root = root]
       ELSE /\ advances' = [advances EXCEPT !.pc = "idle"]
            /\ UNCHANGED <<deleted, ref, histories, acked>>
    /\ UNCHANGED <<deleted, store, disk, proc, nextId, opens>>

AdvanceRoots == histories \cup {<<MaxWrites + advances.count>>}

\* The advance's process ends before it moved the ref.
AdvanceStop ==
    /\ advances.pc /= "idle"
    /\ advances' = [advances EXCEPT !.pc = "idle"]
    /\ UNCHANGED <<deleted, ref, store, disk, proc, nextId, opens, acked, histories>>

\* branch::delete reads the ref, deletes it, lists the log of the life it read
\* and deletes each record it listed. Nothing fences a writer still running on
\* the branch, and the delete of the ref is not conditional: a delete that read
\* a life deleted since deletes whatever life the name holds by then.
DeleteRead ==
    /\ deleted.pc = "idle"
    /\ deleted.count < MaxDeletes
    /\ ref.exists
    /\ deleted' = [deleted EXCEPT !.count = @ + 1, !.pc = "ref", !.life = ref.id]
    /\ UNCHANGED <<ref, store, disk, proc, nextId, opens, advances, acked, histories>>

DeleteRef ==
    /\ deleted.pc = "ref"
    /\ ref' = [ref EXCEPT !.exists = FALSE]
    /\ deleted' = [deleted EXCEPT !.pc = "list"]
    /\ UNCHANGED <<store, disk, proc, nextId, opens, advances, acked, histories>>

DeleteList ==
    LET listed == {r \in store : r.l = deleted.life}
    IN /\ deleted.pc = "list"
       /\ deleted' = [deleted EXCEPT !.pc = IF listed = {} THEN "idle" ELSE "delete",
                                      !.keys = {<<r.g, r.n>> : r \in listed}]
       /\ UNCHANGED <<ref, store, disk, proc, nextId, opens, advances, acked, histories>>

DeleteRecord(k) ==
    /\ deleted.pc = "delete"
    /\ k \in deleted.keys
    /\ store' = store \ At(deleted.life, k[1], k[2])
    /\ deleted' = [deleted EXCEPT !.keys = @ \ {k},
                                   !.pc = IF deleted.keys = {k} THEN "idle" ELSE "delete"]
    /\ UNCHANGED <<ref, disk, proc, nextId, opens, advances, acked, histories>>

\* The delete's process ends before it deleted every record it listed.
DeleteStop ==
    /\ deleted.pc /= "idle"
    /\ deleted' = [deleted EXCEPT !.pc = "idle", !.keys = {}]
    /\ UNCHANGED <<ref, store, disk, proc, nextId, opens, advances, acked, histories>>

\* Volume::create under the deleted branch's name: a new life of the branch,
\* with an id no earlier life had, which holds none of the old one's writes, and
\* of whose strict flushes none returned yet.
Create ==
    /\ ~ref.exists
    /\ ref' = NewRef(deleted.lives + 1, <<>>, 0, Pos(0, 0, 0, FALSE))
    /\ deleted' = [deleted EXCEPT !.lives = @ + 1, !.old = 1..(nextId - 1)]
    /\ acked' = {}
    /\ UNCHANGED <<store, disk, proc, nextId, opens, advances, histories>>

\* GC deletes a record of a life no ref names. It lists the records before the
\* refs, and a life's ref exists before any of its records, so a record it
\* lists of a life no ref names is of a life that is gone.
GcSweep(r) ==
    /\ r \in store
    /\ ~ref.exists \/ r.l /= ref.id
    /\ store' = store \ {r}
    /\ UNCHANGED <<deleted, ref, disk, proc, nextId, opens, advances, acked, histories>>

Next ==
    \/ \E h \in Hosts :
        \/ \E m \in Modes : Open(h, m)
        \/ ScanRead(h) \/ ScanSeal(h) \/ Replay(h) \/ Take(h)
        \/ \E kept \in BOOLEAN : Write(h, kept)
        \/ \E logged, flush \in BOOLEAN : Commit(h, logged, flush)
        \/ \E to \in 1..MaxWrites + 1 : ShipBatch(h, to)
        \/ ShipNumber(h) \/ ShipCovered(h) \/ ShipCreate(h) \/ ShipFence(h)
        \/ ShipAbandon(h)
        \/ UploadStart(h) \/ UploadPut(h)
        \/ \E n \in 0..MaxWrites + 1 : TrimOwn(h, n)
        \/ TrimListOlder(h)
        \/ \E k \in proc[h].toDelete : TrimDeleteOlder(h, k)
        \/ CloseStop(h) \/ ClosePut(h)
        \/ \E w \in proc[h].waiting : Ack(h, w)
        \/ Stop(h) \/ Lose(h)
    \/ AdvanceRead \/ AdvanceScan \/ AdvanceSeal \/ AdvanceStop
    \/ \E root \in AdvanceRoots : AdvancePut(root)
    \/ DeleteRead \/ DeleteRef \/ DeleteList \/ \E k \in deleted.keys : DeleteRecord(k)
    \/ DeleteStop \/ Create
    \/ \E r \in store : GcSweep(r)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties.                                                             *)

\* An open never mixes two histories: what it replays is a state some writer
\* committed. This is the docs' "an open always sees a prefix of the flushes".
OpenSeesAPrefix ==
    \A h \in Hosts :
        /\ ~proc[h].mixed
        /\ proc[h].pc \in {"replay", "take", "run"} => proc[h].st \in histories

\* A writer that takes the branch holds every strict flush any writer returned
\* from before.
StrictFlushesSurvive ==
    \A h \in Hosts : Running(h) => proc[h].ackedAtTake \subseteq Range(proc[h].opened)

\* A flush an advance dropped stays dropped: a writer that takes the branch
\* after it holds no flush older than the advance unless the advance's root did.
AdvanceDropsWhatItReplaced ==
    \A h \in Hosts :
        Running(h) =>
            \A i \in Range(proc[h].opened) :
                i >= proc[h].advFloor \/ i \in Range(proc[h].advRoot)

\* A branch made under a deleted branch's name holds none of the deleted
\* branch's writes: not in its ref, and not in what a writer that took it opened.
DeleteDropsTheOldBranch ==
    /\ ref.exists => Range(ref.root) \cap deleted.old = {}
    /\ \A h \in Hosts :
        Running(h) /\ proc[h].life = deleted.lives =>
            Range(proc[h].opened) \cap deleted.old = {}

\* The ref only ever holds a state some writer committed.
RefHoldsAWriterState == ref.root \in histories

Symmetry == Permutations(Hosts)

WithinDepth == Depth = 0 \/ TLCGet("level") <= Depth
=============================================================================
