#!/usr/bin/env bash
# The disk that holds the cache and the state fills while a branch serves. The cache and state
# sit on a small ext4 file system of their own, filled by another file but for a little room;
# fio then writes into ext4 on the branch until a write fails. It must fail with ENOSPC or EIO,
# not hang; the daemon reports the disk full and still serves reads. Once the other file is
# gone the daemon reports room again, writes go on, the branch publishes, and a fresh host finds
# a file system that passes e2fsck and a file written after the recovery. ublk ids 73-74.
TEST=diskfull
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "$0")/lib.sh"
W=/data/$TEST
S=$W/small
M=/mnt/$TEST
A=/dev/ublkb73
B=/dev/ublkb74
SIZE=$((2 << 30))
MiB=$((1 << 20))
pid=
fpid=
loop=
stopall() {
	[[ -n $fpid ]] && {
		kill -9 "$fpid" 2>/dev/null
		wait "$fpid" 2>/dev/null
	}
	umount "$M" 2>/dev/null
	umount "$M-fresh" 2>/dev/null
	timeout 60 "$PANDO" detach --device "$A" --force >/dev/null 2>&1
	timeout 60 "$PANDO" detach --device "$B" --force >/dev/null 2>&1
	[[ -n $pid ]] && {
		exited "$pid" 60 || kill -9 "$pid" 2>/dev/null
		wait "$pid" 2>/dev/null
	}
	umount "$S" 2>/dev/null
	[[ -n $loop ]] && losetup -d "$loop" 2>/dev/null
	pid=
}
trap 'stopall; rm -rf "$W"' EXIT
rm -rf "$W"
mkdir -p "$W" "$S" "$M" "$M-fresh"
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

# waitline <log> <pattern> <timeout_s>: waits for a line of the daemon log.
waitline() {
	for _ in $(seq $(($3 * 20))); do
		grep -q "$2" "$1" 2>/dev/null && return 0
		sleep 0.05
	done
	return 1
}

# fsck <dev> <out>: mount (replays the journal), umount, then e2fsck -fn; prints ok or why not.
fsck() {
	mount -o noatime "$1" "$M-fresh" 2>"$2.mount" || {
		echo "mount: $(head -1 "$2.mount" || true)"
		return
	}
	umount "$M-fresh"
	local rc=0
	e2fsck -fn "$1" >"$2" 2>&1 || rc=$?
	if [[ $rc = 0 ]]; then echo ok; else echo "e2fsck rc=$rc: $(grep -vE '^(Pass|e2fsck|/dev)' "$2" | head -3 | tr '\n' ' ' || true)"; fi
}

truncate -s $((768 * MiB)) "$W/small.img"
mkfs.ext4 -q -F -m 0 "$W/small.img"
loop=$(losetup -f --show "$W/small.img")
mount "$loop" "$S"
head -c $((4 * MiB)) /dev/urandom >"$W/after"

log=$W/o.log
pando "$S/cache" init o "$SIZE"
attach_bg "$S/cache" "$log" o --device "$A" --state "$S/state" --record false
if ! attached "$log" o || ! usable "$A"; then
	say "attach FAILED: $(tail -1 "$log" || true)"
	exit 1
fi
mkfs.ext4 -q -F -E lazy_itable_init=0,lazy_journal_init=0 "$A"
mount -o noatime "$A" "$M"
sync

free=$(df -B1 --output=avail "$S" | tail -1 | tr -d ' ')
fallocate -l $((free - 64 * MiB)) "$S/filler"
left=$(df -B1M --output=avail "$S" | tail -1 | tr -d ' ')
say "room left beside the 128 MiB reserve: $left MiB"

mark "pando diskfull: fill"
fio --name=fill --filename="$M/fill" --ioengine=libaio --direct=1 --rw=write --bs=1M --iodepth=8 \
	--size=1G --end_fsync=1 >"$W/fio.out" 2>&1 &
fpid=$!
if exited "$fpid" 300; then
	wait "$fpid"
	st=$?
	fpid=
	if [[ $st != 0 ]] && grep -qE "No space left on device|Input/output error" "$W/fio.out"; then
		why=$(grep -oE "No space left on device|Input/output error" "$W/fio.out" | head -1)
		check "fio on a full disk" "ok, $why"
	else
		check "fio on a full disk" "exit $st: $(grep -iE "error|err=" "$W/fio.out" | head -2 | tr '\n' ' ' || true)"
	fi
else
	check "fio on a full disk" "still running after 300 s, state $(ps -o stat= -p "$fpid" || true)"
fi
stuck=$(ps -eo stat=,pid=,comm= | awk '$1 ~ /^D/')
if [[ -z $stuck ]]; then check "nothing in D state" ok; else check "nothing in D state" "$stuck"; fi
if waitline "$log" "the cache's disk is full" 30; then check "disk full reported" ok; else check "disk full reported" "no line"; fi
if timeout 30 dd if="$A" of=/dev/null bs=1M count=64 iflag=direct status=none; then
	check "reads while full" ok
else
	check "reads while full" "failed or slow"
fi
if timeout 30 dd if="$W/after" of="$A" bs=1M seek=$((SIZE / MiB - 4)) oflag=direct status=none 2>"$W/dd.err"; then
	check "a write while full" "it was accepted"
elif grep -q "No space left on device" "$W/dd.err"; then
	check "a write while full" "ok, ENOSPC"
else
	check "a write while full" "$(head -1 "$W/dd.err" || true)"
fi

mark "pando diskfull: free"
rm -f "$S/filler"
if waitline "$log" "the cache's disk has room again" 30; then check "room reported" ok; else check "room reported" "no line"; fi
umount "$M" || umount -l "$M"
if mount -o noatime "$A" "$M" 2>"$W/mount.err"; then
	rm -f "$M/fill"
	cp "$W/after" "$M/after"
	sync -f "$M/after"
	umount "$M"
	check "writes after the recovery" ok
else
	check "writes after the recovery" "mount: $(head -1 "$W/mount.err" || true)"
fi
live=$(fsck "$A" "$R/$TEST.fsck-live")
check "file system on this host" "$live"

if timeout 300 "$PANDO" detach --device "$A" >>"$log" 2>&1 && exited "$pid" 300 && wait "$pid"; then
	check "publish" ok
else
	check "publish" "$(tail -1 "$log" || true)"
fi
pid=

attach_bg "$W/cache-f" "$W/f.log" o --device "$B" --state "$W/state-f" --record false
if attached "$W/f.log" o && usable "$B"; then
	fresh=$(fsck "$B" "$R/$TEST.fsck-fresh")
	check "file system on a fresh host" "$fresh"
	if mount -o ro,noatime "$B" "$M-fresh" && cmp -s "$M-fresh/after" "$W/after"; then
		check "the file written after the recovery" ok
	else
		check "the file written after the recovery" "missing or differs"
	fi
	umount "$M-fresh" 2>/dev/null
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
