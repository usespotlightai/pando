#!/usr/bin/env bash
# pando gets kill -9 while a writer runs, then the branch is recovered and checked.
#   block: fio writes 4 KiB blocks with crc32c; after the kill, `attach --recover` takes the
#     device over with the requests in flight, fio finishes and checks every block, then the
#     branch is checked again from the store on an empty cache.
#   ext4 process: fsstress on ext4; after the kill and `attach --recover`, ext4 is unmounted and
#     `e2fsck -fn` must pass, live and from the store on an empty cache.
#   ext4 host: fsstress on ext4; after the kill the device and the cache are thrown away, and
#     the branch from the store must be a clean ext4 (one of its flushes, never a mix).
# MODES lists "kind runs" pairs, split by ";". ublk ids 62-63.
TEST=crash
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "$0")/lib.sh"
W=/data/$TEST
A=/dev/ublkb62
B=/dev/ublkb63
SIZE=$((2 << 30))
FSSTRESS=${FSSTRESS:-/var/lib/xfstests/ltp/fsstress}
MODES=${MODES:-block 5;ext4-process 5;ext4-host 5}
pid=
wpid=
stopall() {
	[[ -n $wpid ]] && {
		kill -9 "$wpid" 2>/dev/null
		pkill -9 -f "$FSSTRESS" 2>/dev/null
		wait "$wpid" 2>/dev/null
	}
	umount -l "/mnt/$TEST" "/mnt/$TEST-check" 2>/dev/null
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
mkdir -p "$W" "/mnt/$TEST" "/mnt/$TEST-check"
: >"$R/$TEST.log"

blockfio() {
	[[ -b $1 ]] || {
		echo "$1 is not a block device"
		return 1
	}
	local d=$1
	shift
	fio --name=w --filename="$d" --ioengine=libaio --direct=1 --rw=randwrite --bs=4k --iodepth=32 \
		--size=512M --randrepeat=1 --verify=crc32c --verify_backlog=256 --verify_fatal=1 "$@"
}

# fscheck <dev> <out>: mount (replays the journal), umount, then e2fsck -fn.
fscheck() {
	mount -o noatime "$1" "/mnt/$TEST-check" 2>"$2.mount" || {
		echo "mount FAILED: $(head -1 "$2.mount" || true)"
		return
	}
	umount "/mnt/$TEST-check"
	if e2fsck -fn "$1" >"$2" 2>&1; then echo clean; else echo "e2fsck FAILED rc=$?: $(grep -vE '^(Pass|e2fsck|/dev)' "$2" | head -2 | tr '\n' ' ' || true)"; fi
}

# reopen <branch> <n>: attaches the branch on an empty cache as $B and prints the check of it.
reopen() {
	local b=$1 n=$2 kind=$3 out
	attach_bg "$W/cache-$n-re" "$W/$n.re.log" "$b" --device "$B" --state "$W/state-$n-re" --record false
	if ! attached "$W/$n.re.log" "$b" || ! usable "$B"; then
		echo "re-open FAILED: $(tail -1 "$W/$n.re.log" || true)"
		return
	fi
	if [[ $kind = block ]]; then
		blockfio "$B" --verify_only >"$W/$n.fio-re" 2>&1 && out=verified || out="verify FAILED: $(grep -i "verify\|bad" "$W/$n.fio-re" | head -1 || true)"
	else
		out=$(fscheck "$B" "$W/$n.fsck-re")
	fi
	"$PANDO" detach --device "$B" >>"$W/$n.re.log" 2>&1
	wait "$pid"
	pid=
	echo "$out"
}

one() {
	local kind=$1 n=$2 b=c$2 log=$W/$2.log live=- store=- res=pass pause
	pando "$W/cache-$n" init "$b" "$SIZE"
	attach_bg "$W/cache-$n" "$log" "$b" --device "$A" --state "$W/state-$n" --record false
	if ! attached "$log" "$b" || ! usable "$A"; then
		say "$n $kind: attach FAILED: $(tail -1 "$log" || true)"
		stopall
		fails=$((fails + 1))
		return
	fi
	if [[ $kind = block ]]; then
		blockfio "$A" >"$W/$n.fio" 2>&1 &
		wpid=$!
		pause=$(rand_between 1 6)
		sleep "$pause"
	else
		mkfs.ext4 -q -F "$A" && mount -o noatime "$A" "/mnt/$TEST" && mkdir "/mnt/$TEST/w"
		"$FSSTRESS" -d "/mnt/$TEST/w" -n 1000000 -p 4 -s "$n" >"$W/$n.fsstress" 2>&1 &
		wpid=$!
		pause=$(rand_between 2 8)
		sleep "$pause"
	fi
	kill -9 "$pid"
	wait "$pid" 2>/dev/null
	pid=
	case $kind in
	block | ext4-process)
		attach_bg "$W/cache-$n" "$log" "$b" --device "$A" --state "$W/state-$n" --record false --recover
		if ! attached "$log" "$b" 2; then
			live="recover FAILED: $(tail -1 "$log" || true)"
		elif [[ $kind = block ]]; then
			if exited "$wpid" 300; then wait "$wpid" && live=verified || live="fio FAILED: $(grep -i "verify\|bad\|error" "$W/$n.fio" | head -1 || true)"; else live="fio hung"; fi
			wpid=
		else
			sleep 3
			kill -9 "$wpid" 2>/dev/null
			pkill -9 -f "$FSSTRESS"
			wait "$wpid" 2>/dev/null
			wpid=
			for _ in $(seq 600); do
				pgrep -f "$FSSTRESS" >/dev/null || break
				sleep 0.05
			done
			for _ in $(seq 20); do
				umount "/mnt/$TEST" 2>"$W/$n.umount" && break
				sleep 0.5
			done
			if ! mountpoint -q "/mnt/$TEST"; then live=$(fscheck "$A" "$W/$n.fsck-live"); else live="umount FAILED: $(head -1 "$W/$n.umount" || true)"; fi
		fi
		if timeout 300 "$PANDO" detach --device "$A" >>"$log" 2>&1 && exited "$pid" 300 && wait "$pid"; then :; else live="$live, publish FAILED"; fi
		pid=
		;;
	ext4-host)
		stopall
		;;
	*) ;;
	esac
	rm -rf "$W/cache-$n" "$W/state-$n"
	store=$(reopen "$b" "$n" "${kind%%-*}")
	case "$live $store" in
	*FAILED* | *hung*)
		res=FAIL
		fails=$((fails + 1))
		;;
	*) ;;
	esac
	say "$n $kind: live: $live | from the store: $store -> $res"
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
