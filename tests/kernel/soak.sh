#!/usr/bin/env bash
# One segment of a long run of a branch on a cache with --cache-budget; soak.yml chains the
# segments, each on a fresh runner, and carries /data (the store, the cache and the state)
# from one to the next, so the object log is the same file across the whole run. The first
# segment writes the branch, publishes it and attaches it again with the budget; a later one
# attaches with --recover=auto, which finds no device and serves the branch from what the host
# kept, as after a reboot, then reads every block back. Each segment then cycles until
# SOAK_SECONDS: a checked read of the read region, random reads, and a checked rewrite of the
# write region, while it samples the log's disk every 10 s. It ends with a drain (SIGTERM) and
# `gc --delete --unversioned`, so the store stays bounded: a versioned bucket would keep every
# rewrite. With SOAK_LAST, the segment detaches instead, a fresh host reads the branch whole,
# and the log's disk over the whole run is checked: from 30 s after each attach, within twice
# the budget and the write region (the log passes the budget by what one sweep's reads fetch,
# and by writes not uploaded yet), and no higher in the last segment than in the first; nor
# the daemon's memory. SOAK_SECONDS counts from after the attach and the read-back.
#   SOAK_PHASE=start|resume  SOAK_SECONDS=17400  SOAK_SEGMENT=1  SOAK_LAST=1
# ublk ids 77-78.
TEST=soak
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "$0")/lib.sh"
W=/data/soak
A=/dev/ublkb77
B=/dev/ublkb78
MiB=$((1 << 20))
BUDGET=$((${SOAK_BUDGET_MIB:-256} * MiB))
READ=$((${SOAK_READ_MIB:-2048} * MiB))
WRITE=$((${SOAK_WRITE_MIB:-128} * MiB))
SIZE=$((4 << 30))
PHASE=${SOAK_PHASE:?start or resume}
SECONDS_LEFT=${SOAK_SECONDS:?seconds to run}
SEG=${SOAK_SEGMENT:-1}
LIMIT=$((2 * BUDGET + WRITE))
pid=
spid=
trap '[[ -n $spid ]] && kill "$spid" 2>/dev/null' EXIT
case $PHASE in
start)
	rm -rf "$W"
	mkdir -p "$W"
	echo "$RUN" >"$W/run"
	echo "seg,t,log_bytes,log_apparent,pads_bytes,rss_kib" >"$W/samples.csv"
	echo "seg,store_bytes" >"$W/store.csv"
	: >"$W/settled.csv"
	echo 0 >"$W/seed"
	;;
resume) RUN=$(cat "$W/run") ;;
*)
	say "SOAK_PHASE must be start or resume"
	exit 2
	;;
esac
: >"$R/$TEST.log"
fails=0
C=$W/cache

check() {
	local what=$1 got=$2
	case $got in
	ok*) say "segment $SEG: $what: $got" ;;
	*)
		say "segment $SEG: $what: FAILED: $got"
		fails=$((fails + 1))
		;;
	esac
}

logged() { du -B1 "$C/objects" | cut -f1; }

# region <dev> <read|write> <out> <args...>: the read region from 0 or the write region after
# it, in 1 MiB blocks, checked with crc32c.
region() {
	local dev=$1 which=$2 out=$3 off=0 size=$READ
	shift 3
	[[ $which = write ]] && off=$READ size=$WRITE
	fio --name="soak-$which" --filename="$dev" --ioengine=libaio --direct=1 --bs=1M --iodepth=8 \
		--offset="$off" --size="$size" --verify=crc32c --verify_fatal=1 "$@" >>"$out" 2>&1
}

# reads_back <dev> <what>: both regions match what was last written to them.
reads_back() {
	local dev=$1 what=$2 seed
	seed=$(cat "$W/seed")
	if region "$dev" read "$W/verify.out" --rw=read --do_verify=1 --verify_only=1 &&
		region "$dev" write "$W/verify.out" --rw=write --randseed="$seed" --do_verify=1 --verify_only=1; then
		check "$what" ok
	else
		check "$what" "$(grep -iE "verify|error|bad" "$W/verify.out" | tail -2 | tr '\n' ' ' || true)"
	fi
}

attach_budget() {
	attach_bg "$C" "$1" --cache-budget "$((BUDGET / MiB))M" soak --device "$A" --state "$W/state" \
		--record false --recover=auto
	attached "$1" soak 1 600 && usable "$A"
}

if [[ $PHASE = start ]]; then
	pando "$C" init soak "$SIZE"
	attach_bg "$C" "$W/fill.log" soak --device "$A" --state "$W/state" --record false
	if ! attached "$W/fill.log" soak || ! usable "$A"; then
		say "attach FAILED: $(tail -1 "$W/fill.log" || true)"
		exit 1
	fi
	region "$A" read "$W/fill.out" --rw=write --do_verify=0 || check "fill" "$(tail -2 "$W/fill.out" | tr '\n' ' ' || true)"
	region "$A" write "$W/fill.out" --rw=write --randseed=0 --do_verify=0 || check "fill" "$(tail -2 "$W/fill.out" | tr '\n' ' ' || true)"
	if timeout 900 "$PANDO" detach --device "$A" >>"$W/fill.log" 2>&1 && exited "$pid" 300 && wait "$pid"; then
		check "fill and publish $(((READ + WRITE) / MiB)) MiB" ok
	else
		check "fill and publish" "$(tail -1 "$W/fill.log" || true)"
	fi
	pid=
fi

log=$W/seg$SEG.log
if attach_budget "$log"; then
	echo "$SEG,$(($(date +%s || true) + 30))" >>"$W/settled.csv"
	check "attach with the budget ($PHASE)" ok
else
	check "attach with the budget ($PHASE)" "$(tail -1 "$log" || true)"
	cp "$log" "$R/"
	exit 1
fi
[[ $PHASE = resume ]] && reads_back "$A" "every block reads back after the restart"
t_end=$(($(date +%s) + SECONDS_LEFT))

(
	while :; do
		pads=$(stat -c %s "$C/objects.pads" 2>/dev/null || echo 0)
		rss=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')
		now=$(date +%s)
		bytes=$(logged || true)
		apparent=$(du -B1 --apparent-size "$C/objects" | cut -f1 || true)
		echo "$SEG,$now,$bytes,$apparent,$pads,${rss:-0}" >>"$W/samples.csv"
		sleep 10
	done
) &
spid=$!

cycles=0
now() { date +%s; }
while (($(now || true) + 900 < t_end)); do
	cycles=$((cycles + 1))
	: >"$W/cycle.out"
	region "$A" read "$W/cycle.out" --rw=read --do_verify=1 --verify_only=1 || check "cycle $cycles: read region" "$(grep -iE "verify|error|bad" "$W/cycle.out" | tail -2 | tr '\n' ' ' || true)"
	left=$((t_end - $(now || true) - 600))
	((left > 300)) && left=300
	if ((left > 0)) && ! fio --name=soak-random --filename="$A" --ioengine=libaio --direct=1 --bs=64k \
		--iodepth=16 --size="$READ" --rw=randread --time_based --runtime="$left" --randseed="$cycles$SEG" \
		>>"$W/cycle.out" 2>&1; then
		check "cycle $cycles: random reads" "$(grep -iE "error|bad" "$W/cycle.out" | tail -2 | tr '\n' ' ' || true)"
	fi
	seed=$((SEG * 100000 + cycles))
	if region "$A" write "$W/cycle.out" --rw=write --randseed="$seed" --do_verify=1; then
		echo "$seed" >"$W/seed"
	else
		check "cycle $cycles: write region" "$(grep -iE "verify|error|bad" "$W/cycle.out" | tail -2 | tr '\n' ' ' || true)"
		break
	fi
done
kill "$spid" 2>/dev/null
wait "$spid" 2>/dev/null
spid=
((cycles > 0)) || check "cycles" "none ran in $SECONDS_LEFT s"
top=$(awk -F, -v s="$SEG" 'FNR == NR { from[$1] = $2; next } $1 == s && $2 >= from[s] && $3 > m { m = $3 } END { print m + 0 }' "$W/settled.csv" "$W/samples.csv")
if ((top <= LIMIT)); then
	check "the log stays near the budget over $cycles cycles" "ok, at most $((top / MiB)) MiB of $((BUDGET / MiB))"
else
	check "the log stays near the budget over $cycles cycles" "$((top / MiB)) MiB, over $((LIMIT / MiB))"
fi

if [[ -z ${SOAK_LAST:-} ]]; then
	kill -TERM "$pid"
	if exited "$pid" 180; then
		wait "$pid"
		st=$?
		if [[ $st = 75 ]]; then check "drain" ok; else check "drain" "exit $st: $(tail -1 "$log" || true)"; fi
	else
		check "drain" "still running after 180 s"
	fi
	pid=
	if pando "$W/gc-cache" gc --delete --unversioned --keep-young 10m --validate 5s >"$W/gc.out" 2>&1; then
		check "gc" "ok, $(tail -1 "$W/gc.out" || true)"
	else
		check "gc" "$(tail -1 "$W/gc.out" || true)"
	fi
else
	if timeout 900 "$PANDO" detach --device "$A" >>"$log" 2>&1 && exited "$pid" 300 && wait "$pid"; then
		check "detach" ok
	else
		check "detach" "$(tail -1 "$log" || true)"
	fi
	pid=
	attach_bg "$W/cache-f" "$W/fresh.log" soak --device "$B" --state "$W/state-f" --record false
	if attached "$W/fresh.log" soak && usable "$B"; then
		reads_back "$B" "a fresh host reads every block"
		timeout 300 "$PANDO" detach --device "$B" >>"$W/fresh.log" 2>&1
		exited "$pid" 300
	else
		check "fresh host" "attach: $(tail -1 "$W/fresh.log" || true)"
	fi
	pid=
	# mean <segment> <column>: the mean of a sampled column in a segment, from 30 s after its attach.
	mean() { awk -F, -v s="$1" -v c="$2" 'FNR == NR { from[$1] = $2; next } $1 == s && $2 >= from[s] { n++; t += $c } END { if (n) printf "%d", t / n; else print 0 }' "$W/settled.csv" "$W/samples.csv"; }
	first=$(mean 1 3)
	last=$(mean "$SEG" 3)
	if ((first > 0 && last > 0 && last <= first + first / 4 + 32 * MiB)); then
		check "the log does not grow across segments" "ok, mean $((first / MiB)) MiB in segment 1, $((last / MiB)) MiB in segment $SEG"
	else
		check "the log does not grow across segments" "mean $((first / MiB)) MiB in segment 1, $((last / MiB)) MiB in segment $SEG"
	fi
	first=$(mean 1 6)
	last=$(mean "$SEG" 6)
	if ((first > 0 && last > 0 && last <= first + first / 2 + 64 * 1024)); then
		check "the daemon's memory does not grow across segments" "ok, mean RSS $((first / 1024)) MiB in segment 1, $((last / 1024)) MiB in segment $SEG"
	else
		check "the daemon's memory does not grow across segments" "mean RSS $((first / 1024)) MiB in segment 1, $((last / 1024)) MiB in segment $SEG"
	fi
fi
store=$(du -sB1 /data/minio | cut -f1 || true)
final=$(logged || true)
pads=$(stat -c %s "$C/objects.pads" 2>/dev/null || echo 0)
echo "$SEG,$store" >>"$W/store.csv"
say "segment $SEG: store $((store / MiB)) MiB, log $((final / MiB)) MiB, pads $pads bytes"
cp "$W/samples.csv" "$W/store.csv" "$R/"
if [[ $fails != 0 ]]; then
	for f in "$W"/*.log "$W"/*.out; do cp "$f" "$R/$TEST.$(basename "$f")"; done
	dmesg | tail -100 >"$R/$TEST.dmesg"
fi
if [[ $fails = 0 ]]; then say "RESULT pass"; else say "RESULT FAIL ($fails checks)"; fi
[[ $fails = 0 ]]
