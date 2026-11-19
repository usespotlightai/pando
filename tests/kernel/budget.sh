#!/usr/bin/env bash
# A branch four times the size of the cache's budget. fio writes it whole and the branch is
# detached, which publishes it. A second attach on the same cache, with the budget, must bring
# the object log within it, then serve a full fio check of every block while the log's disk
# stays near the budget: what it drops comes from the store again. A fresh host then reads the
# branch whole. ublk ids 75-76.
TEST=budget
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "$0")/lib.sh"
W=/data/$TEST
A=/dev/ublkb75
B=/dev/ublkb76
MiB=$((1 << 20))
BUDGET=$((256 * MiB))
DATA=$((1024 * MiB))
SIZE=$((2 << 30))
pid=
spid=
stopall() {
	[[ -n $spid ]] && kill "$spid" 2>/dev/null
	timeout 60 "$PANDO" detach --device "$A" --force >/dev/null 2>&1
	timeout 60 "$PANDO" detach --device "$B" --force >/dev/null 2>&1
	[[ -n $pid ]] && {
		exited "$pid" 60 || kill -9 "$pid" 2>/dev/null
		wait "$pid" 2>/dev/null
	}
	pid=
}
trap 'stopall; rm -rf "$W"' EXIT
rm -rf "$W"
mkdir -p "$W"
: >"$R/$TEST.log"
fails=0

check() {
	local what=$1 got=$2
	case $got in
	ok*) say "$what: $got" ;;
	*)
		say "$what: FAILED: $got"
		fails=$((fails + 1))
		;;
	esac
}

# logged <cache>: the bytes of disk the cache's object log takes.
logged() { du -B1 "$1/objects" | cut -f1; }

# runfio <dev> <out> <args...>: the whole data area in 1 MiB blocks, checked with crc32c.
runfio() {
	local dev=$1 out=$2
	shift 2
	fio --name=budget --filename="$dev" --ioengine=libaio --direct=1 --bs=1M --iodepth=8 \
		--size="$DATA" --verify=crc32c --verify_fatal=1 "$@" >"$out" 2>&1
}

C=$W/cache
pando "$C" init b "$SIZE"
log=$W/a.log
attach_bg "$C" "$log" b --device "$A" --state "$W/state" --record false
if ! attached "$log" b || ! usable "$A"; then
	say "attach FAILED: $(tail -1 "$log" || true)"
	exit 1
fi
mark "pando budget: write"
if runfio "$A" "$W/write.out" --rw=write --do_verify=0; then
	check "write $((DATA / MiB)) MiB" ok
else
	check "write $((DATA / MiB)) MiB" "$(grep -iE "error|bad" "$W/write.out" | head -2 | tr '\n' ' ' || true)"
fi
if timeout 600 "$PANDO" detach --device "$A" >>"$log" 2>&1 && exited "$pid" 300 && wait "$pid"; then
	check "publish" ok
else
	check "publish" "$(tail -1 "$log" || true)"
fi
pid=
before=$(logged "$C")
say "object log after the write: $((before / MiB)) MiB"

log=$W/b.log
attach_bg "$C" "$log" --cache-budget "$((BUDGET / MiB))M" b --device "$A" --state "$W/state" --record false
if ! attached "$log" b || ! usable "$A"; then
	say "attach with the budget FAILED: $(tail -1 "$log" || true)"
	exit 1
fi
within=
for _ in $(seq 600); do
	now=$(logged "$C")
	if ((now <= BUDGET)); then
		within=$now
		break
	fi
	sleep 0.1
done
if [[ -n $within ]]; then
	check "the log comes within the budget" "ok, $((within / MiB)) MiB"
else
	now=$(logged "$C")
	check "the log comes within the budget" "$((now / MiB)) MiB after 60 s"
fi

peak=$W/peak
echo 0 >"$peak"
(
	top=0
	while :; do
		n=$(logged "$C")
		((n > top)) && top=$n && echo "$top" >"$peak"
		sleep 0.2
	done
) &
spid=$!
mark "pando budget: verify"
if runfio "$A" "$W/verify.out" --rw=read --do_verify=1 --verify_only=1; then
	check "every block reads back through the budget" ok
else
	check "every block reads back through the budget" "$(grep -iE "verify|error|bad" "$W/verify.out" | head -2 | tr '\n' ' ' || true)"
fi
kill "$spid" 2>/dev/null
wait "$spid" 2>/dev/null
spid=
top=$(cat "$peak")
limit=$((BUDGET + BUDGET / 2))
if ((top <= limit)); then
	check "the log stays near the budget" "ok, at most $((top / MiB)) MiB of $((BUDGET / MiB))"
else
	check "the log stays near the budget" "$((top / MiB)) MiB, over $((limit / MiB))"
fi
if timeout 300 "$PANDO" detach --device "$A" >>"$log" 2>&1 && exited "$pid" 300 && wait "$pid"; then
	check "detach" ok
else
	check "detach" "$(tail -1 "$log" || true)"
fi
pid=

attach_bg "$W/cache-f" "$W/f.log" b --device "$B" --state "$W/state-f" --record false
if attached "$W/f.log" b && usable "$B"; then
	if runfio "$B" "$W/fresh.out" --rw=read --do_verify=1 --verify_only=1; then
		check "a fresh host reads every block" ok
	else
		check "a fresh host reads every block" "$(grep -iE "verify|error|bad" "$W/fresh.out" | head -2 | tr '\n' ' ' || true)"
	fi
	timeout 300 "$PANDO" detach --device "$B" >>"$W/f.log" 2>&1
	exited "$pid" 300
	pid=
else
	check "fresh host" "attach: $(tail -1 "$W/f.log" || true)"
fi

if [[ $fails != 0 ]]; then
	for f in "$W"/*.log "$W"/*.out; do cp "$f" "$R/$TEST.$(basename "$f")"; done
	dmesg | tail -100 >"$R/$TEST.dmesg"
fi
if [[ $fails = 0 ]]; then say "RESULT pass"; else say "RESULT FAIL ($fails checks)"; fi
[[ $fails = 0 ]]
