---------------------------- MODULE WriteLogTrace ----------------------------
(***************************************************************************)
(* Checks a trace of pando-dst's race (module TraceData, from              *)
(* pando-dst/src/trace.rs) against WriteLog. Each call the engine made on  *)
(* the branch's ref and log is one step of the spec, with the same host,   *)
(* record numbers, flush numbers, log position and outcome, and so is each *)
(* flush a writer committed; the advancer's calls are the steps of an      *)
(* advance. The steps no call shows (the replay, the start of a record or  *)
(* an upload, the stop of a log) happen between them wherever the spec     *)
(* allows.                                                                 *)
(*                                                                         *)
(* TLC searches for a behaviour that matches every event. NotAccepted      *)
(* fails exactly when it finds one, so a reported violation is a trace the *)
(* spec accepts, and a run without one is a trace it rejects; the last     *)
(* "matched" line then names the first event no behaviour reaches.         *)
(***************************************************************************)
EXTENDS TraceData, Naturals, Sequences, TLC

VARIABLES ref, store, disk, proc, nextId, opens, advances, acked, histories, i

vars == <<ref, store, disk, proc, nextId, opens, advances, acked, histories, i>>

WL == INSTANCE WriteLog WITH Hosts <- TraceHosts, Modes <- {"behind", "strict"},
                             MaxFlushes <- TraceFlushes, MaxOpens <- TraceOpens,
                             MaxAdvances <- TraceAdvances, Depth <- 0

Init == WL!Init /\ i = 0

LogIs(e, pos) == pos = WL!Pos(e.lgen, e.lflush, e.lnext, e.lclosed)

Found(e) ==
    IF e.found
    THEN \E r \in WL!At(e.g, e.n) :
            /\ r.kind = e.kind
            /\ e.kind = "writes" => r.from = e.from /\ r.to = e.to
    ELSE WL!At(e.g, e.n) = {}

\* The ref each kind of conditional PUT writes.
TakeWrites(e, p) == p.pc = "take" /\ e.gen = p.gen /\ LogIs(e, p.start)
UploadWrites(e, p) ==
    /\ p.pc = "run" /\ p.upping /\ e.gen = p.gen
    /\ LogIs(e, WL!Pos(p.gen, p.upSeq, WL!Covering(p, p.upSeq), p.stopped))
CloseWrites(e, p) ==
    /\ p.pc = "run" /\ p.closing /\ ~p.shipping /\ e.gen = p.gen
    /\ LogIs(e, WL!Pos(p.gen, p.closeSeq, WL!Covering(p, p.closeSeq), TRUE))

Moved(e) == e.ok <=> ref' /= ref

\* The calls of branch::advance: the read of the ref, the seal of the writer's
\* log, and the update of the ref to the root the trace names.
Advancing(e) ==
    CASE e.op = "get_ref" -> WL!AdvanceRead
      [] e.op = "get_log" ->
            /\ advances.pc = "scan" /\ advances.gen = e.g /\ advances.n = e.n /\ Found(e)
            /\ WL!AdvanceScan
      [] e.op = "create_log" ->
            /\ advances.pc = "seal" /\ advances.gen = e.g /\ advances.n = e.n
            /\ e.kind = "seal" /\ (e.ok <=> WL!At(e.g, e.n) = {})
            /\ WL!AdvanceSeal
      [] e.op = "update_ref" ->
            /\ advances.pc = "put" /\ e.gen = advances.gen + 1
            /\ LogIs(e, WL!Pos(advances.gen + 1, 0, 0, TRUE))
            /\ e.root \in WL!AdvanceRoots
            /\ WL!AdvancePut(e.root) /\ Moved(e)

Observe(e) ==
    LET h == e.host
        p == proc[h]
    IN CASE e.host = TraceAdvancer -> Advancing(e)
         [] e.op = "get_ref" -> WL!Open(h, e.mode)
         [] e.op = "get_log" ->
                /\ p.pc = "scan" /\ p.g = e.g /\ p.n = e.n /\ Found(e)
                /\ WL!ScanRead(h)
         [] e.op = "create_log" /\ e.kind = "seal" ->
                /\ p.pc = "seal" /\ p.g = e.g /\ p.n = e.n
                /\ e.ok <=> WL!At(e.g, e.n) = {}
                /\ WL!ScanSeal(h)
         [] e.op = "create_log" ->
                /\ p.pc = "run" /\ p.shipping /\ ~p.refused
                /\ p.gen = e.g /\ p.shipN = e.n /\ p.shipFrom = e.from /\ p.shipTo = e.to
                /\ e.ok <=> WL!At(e.g, e.n) = {}
                /\ WL!ShipCreate(h)
         [] e.op = "update_ref" ->
                \/ TakeWrites(e, p) /\ WL!Take(h) /\ Moved(e)
                \/ UploadWrites(e, p) /\ WL!UploadPut(h) /\ Moved(e)
                \/ CloseWrites(e, p) /\ WL!ClosePut(h) /\ Moved(e)
         [] e.op = "delete_log" ->
                \/ p.gen = e.g /\ WL!TrimOwn(h, e.n)
                \/ WL!TrimDeleteOlder(h, <<e.g, e.n>>)
         [] e.op = "list_log" -> WL!TrimListOlder(h)
         [] e.op = "flush" -> WL!Flush(h, TRUE)
         [] e.op = "stop" -> WL!Stop(h)
         [] e.op = "lose" -> WL!Lose(h)

Hidden ==
    /\ UNCHANGED i
    /\ \E h \in TraceHosts :
        \/ WL!Replay(h)
        \/ \E to \in 1..TraceFlushes + 1 : WL!ShipBatch(h, to)
        \/ WL!ShipNumber(h)
        \/ WL!ShipCovered(h)
        \/ WL!ShipFence(h)
        \/ WL!ShipAbandon(h)
        \/ WL!UploadStart(h)
        \/ WL!CloseStop(h)

Next ==
    \/ /\ i < Len(Trace)
       /\ Observe(Trace[i + 1])
       /\ i' = i + 1
       /\ IF i + 1 > TLCGet(1) THEN TLCSet(1, i + 1) /\ PrintT(<<"matched", i + 1>>) ELSE TRUE
    \/ Hidden

Spec == Init /\ TLCSet(1, 0) /\ [][Next]_vars

NotAccepted == i < Len(Trace)
=============================================================================
