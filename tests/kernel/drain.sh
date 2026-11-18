#!/usr/bin/env bash
# pando gets SIGTERM, drains and exits with status 75, leaving its device for the next daemon.
#   restart: SIGTERM while fio writes 4 KiB blocks with crc32c; `attach --recover=auto` takes the
#     device over, fio finishes and checks every block, then the branch is checked from the store.
#   quiet: fio finishes, then SIGTERM; the drain must publish every write, so the device, cache
#     and state are thrown away and the branch from the store must hold every block.
#   reboot: fio finishes, then SIGTERM with --drain-wait 0s, so the writes may stay on this host;
#     the device is deleted as a reboot would, `attach --recover=auto` adds a new one from the
#     state left behind, and the branch must hold every block, live and from the store.
# MODES lists "kind runs" pairs, split by ";". ublk ids 68-69.
TEST=drain
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "$0")/lib.sh"
W=/data/$TEST
A=/dev/ublkb68
B=/dev/ublkb69
SIZE=$((1 << 30))
MODES=${MODES:-restart 3;quiet 2;reboot 2}
pid=
wpid=
stopall() {
	[[ -n $wpid ]] && {
		kill -9 "$wpid" 2>/dev/null
		wait "$wpid" 2>/dev/null
	}
	timeout 60 "$PANDO" detach --device "$A" --force >/dev/null 2>&1
	timeout 60 "$PANDO" detach --device "$B" --force >/dev/null 2>&1
	[[ -n $pid ]] && {
		exited "$pid" 60 || kill -9 "$pid" 2>/dev/null
		wait "$pid" 2>/dev/null
	}
	pid=
	wpid=
}
trap 'stopall; rm -rf "$W"' EXIT
rm -rf "$W"
mkdir -p "$W"
: >"$R/$TEST.log"

blockfio() {
	[[ -b $1 ]] || {
		echo "$1 is not a block device"
		return 1
	}
	local d=$1
	shift
	fio --name=w --filename="$d" --ioengine=libaio --direct=1 --rw=randwrite --bs=4k --iodepth=32 \
		--size=256M --randrepeat=1 --verify=crc32c --verify_backlog=256 --verify_fatal=1 "$@"
}

# fiofail <out>: the first line of a fio log that tells why it failed.
fiofail() { grep -i "verify\|bad\|error" "$1" | head -1 || true; }

# verify <dev> <out>: checks every block fio wrote.
verify() {
	if blockfio "$1" --verify_only >"$2" 2>&1; then echo verified; else echo "verify FAILED: $(fiofail "$2" || true)"; fi
}

# term <log>: sends SIGTERM and checks the daemon drained: status 75 and a "drained" line.
term() {
	local log=$1 st
	kill -TERM "$pid"
	if ! exited "$pid" 120; then
		echo "drain hung"
		return
	fi
	wait "$pid"
	st=$?
	pid=
	if [[ $st != 75 ]]; then
		echo "exit $st, want 75: $(tail -1 "$log" || true)"
		return
	fi
	if ! grep -q "^drained " "$log"; then
		echo "no drained line: $(tail -1 "$log" || true)"
		return
	fi
	echo drained
}

# reopen <branch> <n>: attaches the branch on an empty cache as $B and prints the check of it.
reopen() {
	local b=$1 n=$2 out
	attach_bg "$W/cache-$n-re" "$W/$n.re.log" "$b" --device "$B" --state "$W/state-$n-re" --record false
	if ! attached "$W/$n.re.log" "$b" || ! usable "$B"; then
		echo "re-open FAILED: $(tail -1 "$W/$n.re.log" || true)"
		return
	fi
	out=$(verify "$B" "$W/$n.fio-re")
	"$PANDO" detach --device "$B" >>"$W/$n.re.log" 2>&1
	wait "$pid"
	pid=
	echo "$out"
}

# publish <log>: detaches $A and waits for the daemon to publish the branch.
publish() {
	if timeout 300 "$PANDO" detach --device "$A" >>"$1" 2>&1 && exited "$pid" 300 && wait "$pid"; then
		pid=
		return 0
	fi
	pid=
	return 1
}

one() {
	local kind=$1 n=$2 b=d$2 log=$W/$2.log live=- drained=- store=- res=pass pause
	local att=(--device "$A" --state "$W/state-$n" --record false)
	pando "$W/cache-$n" init "$b" "$SIZE"
	[[ $kind = reboot ]] && att+=(--drain-wait 0s)
	attach_bg "$W/cache-$n" "$log" "$b" "${att[@]}"
	if ! attached "$log" "$b" || ! usable "$A"; then
		say "$n $kind: attach FAILED: $(tail -1 "$log" || true)"
		stopall
		fails=$((fails + 1))
		return
	fi
	case $kind in
	restart)
		blockfio "$A" >"$W/$n.fio" 2>&1 &
		wpid=$!
		pause=$(rand_between 1 5)
		sleep "$pause"
		drained=$(term "$log")
		attach_bg "$W/cache-$n" "$log" "$b" "${att[@]}" --recover=auto
		if ! attached "$log" "$b" 2; then
			live="recover FAILED: $(tail -1 "$log" || true)"
		elif ! exited "$wpid" 300; then
			live="fio hung"
		elif wait "$wpid"; then
			live=verified
		else
			live="fio FAILED: $(fiofail "$W/$n.fio" || true)"
		fi
		wpid=
		publish "$log" || live="$live, publish FAILED"
		;;
	quiet)
		if blockfio "$A" >"$W/$n.fio" 2>&1; then live=written; else live="fio FAILED: $(fiofail "$W/$n.fio" || true)"; fi
		drained=$(term "$log")
		grep -q "the store holds every write" "$log" || drained="$drained, not published: $(grep "^drained " "$log" | tail -1 || true)"
		stopall
		;;
	reboot)
		if blockfio "$A" >"$W/$n.fio" 2>&1; then live=written; else live="fio FAILED: $(fiofail "$W/$n.fio" || true)"; fi
		drained=$(term "$log")
		timeout 60 "$PANDO" detach --device "$A" --force >>"$log" 2>&1
		attach_bg "$W/cache-$n" "$log" "$b" "${att[@]}" --recover=auto
		if attached "$log" "$b" 2 && usable "$A"; then
			live="$live, $(verify "$A" "$W/$n.fio-live")"
		else
			live="$live, re-attach FAILED: $(tail -1 "$log" || true)"
		fi
		publish "$log" || live="$live, publish FAILED"
		;;
	*)
		say "unknown mode $kind"
		fails=$((fails + 1))
		return
		;;
	esac
	rm -rf "$W/cache-$n" "$W/state-$n"
	store=$(reopen "$b" "$n")
	case "$live $drained $store" in
	*FAILED* | *hung* | *want* | *"no drained"* | *"not published"*)
		res=FAIL
		fails=$((fails + 1))
		;;
	*) ;;
	esac
	say "$n $kind: live: $live | drain: $drained | from the store: $store -> $res"
	stopall
	rm -rf "$W/cache-$n-re" "$W/state-$n-re"
}

fails=0
n=0
IFS=";" read -ra rows <<<"$MODES"
for row in "${rows[@]}"; do
	read -r kind runs <<<"$row"
	for _ in $(seq "$runs"); do
		n=$((n + 1))
		one "$kind" "$n"
	done
done
if [[ $fails = 0 ]]; then say "RESULT pass ($n runs)"; else say "RESULT FAIL ($fails of $n runs)"; fi
[[ $fails = 0 ]]
