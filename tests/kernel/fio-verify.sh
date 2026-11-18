#!/usr/bin/env bash
# fio writes the jobs in jobs.fio to a pando device and checks every block (crc32c). Then the
# branch is detached, which publishes it, and attached again on an empty cache, so every read
# comes from the store, and fio checks every block again.
# MODES: plain (the ublk device), dm (device-mapper over it), handoff (device-mapper switched to
# a local image while fio writes, with a snapshot every second).
# ublk ids 60-61, dm name pt-fio.
TEST=fio-verify
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "$0")/lib.sh"
W=/data/$TEST
A=/dev/ublkb60
B=/dev/ublkb61
DM=pt-fio
SIZE=$((2 << 30))
MODES=${MODES:-plain dm handoff}
pid=
cleanup() {
	"$PANDO" detach --device "$A" --dm "$DM" >/dev/null 2>&1
	"$PANDO" detach --device "$A" >/dev/null 2>&1
	"$PANDO" detach --device "$B" >/dev/null 2>&1
	[[ -n $pid ]] && wait "$pid" 2>/dev/null
	rm -rf "$W"
}
trap cleanup EXIT
rm -rf "$W"
mkdir -p "$W"
: >"$R/$TEST.log"

runfio() {
	DEV=$1 fio --output="$W/fio.out" "${@:2}" "$(dirname "$0")/jobs.fio" 2>"$W/fio.err"
	local rc=$?
	grep -hiE "verify|bad|error" "$W/fio.err" | head -5 | tee -a "$R/$TEST.log"
	return "$rc"
}

fails=0
for mode in $MODES; do
	b=$mode
	log=$R/$TEST-$mode.log
	flags=()
	dev=$A
	case $mode in
	dm)
		flags=(--dm "$DM")
		dev=/dev/mapper/$DM
		;;
	handoff)
		flags=(--dm "$DM" --images "$W/img" --snapshot 1s)
		dev=/dev/mapper/$DM
		;;
	*) ;;
	esac
	pando "$W/cache-$b" init "$b" "$SIZE"
	mark "$TEST start $mode $$"
	: >"$log"
	attach_bg "$W/cache-$b" "$log" "$b" --device "$A" --state "$W/state" "${flags[@]}" --record false
	if ! attached "$log" "$b" || ! usable "$dev"; then
		say "$mode: attach FAILED: $(tail -2 "$log" || true)"
		fails=$((fails + 1))
		kill -9 "$pid"
		pid=
		continue
	fi
	t0=$(date +%s)
	runfio "$dev"
	w=$?
	t1=$(date +%s)
	if [[ ${#flags[@]} -gt 0 ]]; then "$PANDO" detach --device "$A" --dm "$DM" >>"$log" 2>&1; else "$PANDO" detach --device "$A" >>"$log" 2>&1; fi
	wait "$pid"
	d=$?
	pid=
	t2=$(date +%s)
	: >"$log.re"
	attach_bg "$W/cache-$b-re" "$log.re" "$b" --device "$B" --state "$W/state-re" --record false
	if attached "$log.re" "$b" && usable "$B"; then
		runfio "$B" --verify_only
		v=$?
	else v=99; fi
	"$PANDO" detach --device "$B" >>"$log.re" 2>&1
	wait "$pid"
	pid=
	t3=$(date +%s)
	ho=$(grep -c "^handed $b off" "$log")
	k=$(kernel_errors "$TEST start $mode $$")
	res=pass
	[[ $w = 0 && $d = 0 && $v = 0 && $k = 0 ]] || {
		res=FAIL
		fails=$((fails + 1))
	}
	[[ $mode = handoff && $ho = 0 ]] && {
		res="FAIL (no handoff)"
		fails=$((fails + 1))
	}
	say "$mode: write+verify rc=$w ($((t1 - t0)) s), detach rc=$d ($((t2 - t1)) s), verify from the store rc=$v ($((t3 - t2)) s), handed off=$ho, kernel I/O errors=$k -> $res"
	rm -rf "$W/cache-$b" "$W/cache-$b-re" "$W/img" "$W/state" "$W/state-re"
done
if [[ $fails = 0 ]]; then say "RESULT pass"; else say "RESULT FAIL ($fails)"; fi
[[ $fails = 0 ]]
