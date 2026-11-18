#!/usr/bin/env bash
# The FUSE frontend (`attach --fuse`): a branch served as disk.img in a FUSE mount, with a loop
# device over it, the path for kernels without ublk. fio checks every block it wrote through
# the loop device, ext4 on it survives a detach and an attach on an empty cache, discard
# reaches the branch, a mounted loop device refuses a detach, the branch grows while it serves,
# and ext4 writes complete while the daemon and the writer share a small memory cgroup. Then
# fio measures the loop device against a ublk device of the same branch on this kernel; the
# numbers are reported, not checked. ublk id 68.
TEST=fuse
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "$0")/lib.sh"
W=/data/$TEST
U=/dev/ublkb68
SIZE=$((2 << 30))
PERF=${PERF:-1}
pid=
stopall() {
	umount "$W/mnt" 2>/dev/null
	for d in "$W"/fuse-*; do
		[[ -d $d ]] && timeout 60 "$PANDO" detach --fuse "$d" --force >/dev/null 2>&1
	done
	timeout 60 "$PANDO" detach --device "$U" >/dev/null 2>&1
	[[ -n $pid ]] && {
		exited "$pid" 60 || kill -9 "$pid" 2>/dev/null
		wait "$pid" 2>/dev/null
	}
	pid=
	[[ -d /sys/fs/cgroup/pando-$TEST ]] && rmdir "/sys/fs/cgroup/pando-$TEST" 2>/dev/null
}
trap 'stopall; rm -rf "$W"' EXIT
rm -rf "$W"
mkdir -p "$W/mnt"
: >"$R/$TEST.log"
mark "$TEST start $$"
fails=0
check() {
	if [[ $2 = ok ]]; then
		say "$1: ok"
	else
		say "$1: FAILED ($2)"
		fails=$((fails + 1))
	fi
}

# up <branch> <cache> <name> [args...]: attaches the branch over FUSE at $W/fuse-<name> and
# sets dev to its loop device.
up() {
	local b=$1 c=$2 n=$3
	shift 3
	: >"$W/$n.log"
	attach_bg "$W/$c" "$W/$n.log" "$b" --fuse "$W/fuse-$n" --state "$W/state-$c" --record false "$@"
	attached "$W/$n.log" "$b" || return 1
	dev=$(sed -n "s/^attached $b as \(\/dev\/loop[0-9]*\) .*/\1/p" "$W/$n.log" | tail -1)
	[[ -n $dev ]] && usable "$dev"
}

# down <name> [args...]: detaches $W/fuse-<name> and waits for the daemon.
down() {
	local n=$1
	shift
	"$PANDO" detach --fuse "$W/fuse-$n" "$@" >>"$W/detach.log" 2>&1
	local rc=$?
	if [[ $rc = 0 ]]; then
		exited "$pid" 60 || rc=1
		wait "$pid" || rc=1
		pid=
	fi
	return "$rc"
}

# check_is <what> <want> <got>: passes when got is want.
check_is() {
	if [[ $2 = "$3" ]]; then check "$1" ok; else check "$1" "got ${3:-nothing}"; fi
}

pando "$W/cache-a" init f "$SIZE"
up f cache-a a || {
	say "attach FAILED: $(tail -3 "$W/a.log" || true)"
	exit 1
}
say "$(grep '^attached' "$W/a.log" || true)"
dio=$(cat "/sys/block/$(basename "$dev")/loop/dio" || true)
check_is "the loop device uses direct I/O" 1 "$dio"
q=/sys/block/$(basename "$dev")/queue
limits=$(grep -H . "$q/discard_granularity" "$q/discard_max_bytes" "$q/max_sectors_kb" || true)
say "loop limits: ${limits//$'\n'/ }"

if DEV=$dev fio --output="$W/fio.out" "$(dirname "$0")/jobs.fio" 2>"$W/fio.err"; then
	verify=ok
else
	verify=$(grep -hiE "verify|bad|error" "$W/fio.err" | head -1 || true)
	verify=${verify:-fio failed}
fi
check "fio verifies every block it wrote through the loop device" "$verify"

mkfs.ext4 -q -F "$dev"
mount -o noatime "$dev" "$W/mnt"
head -c $((256 << 20)) /dev/urandom >"$W/mnt/data"
head -c $((128 << 20)) /dev/urandom >"$W/mnt/gone"
sync
rm "$W/mnt/gone"
want=$(sha256sum "$W/mnt/data")
want=${want%% *}
trim=$(fstrim -v "$W/mnt" || true)
trimmed=$(grep -o '([0-9]* bytes)' <<<"$trim" | tr -dc 0-9 || true)
trims="${trimmed:-0} bytes"
[[ ${trimmed:-0} -ge $((128 << 20)) ]] && trims=ok
check "fstrim reaches the branch" "$trims"
if "$PANDO" detach --fuse "$W/fuse-a" >"$W/busy.log" 2>&1; then
	busy="detach went ahead"
elif grep -q "is in use" "$W/busy.log"; then
	busy=ok
else
	busy=$(tail -1 "$W/busy.log" || true)
fi
check "a mounted loop device refuses a detach" "$busy"
serving=gone
[[ -r $W/mnt/data ]] && kill -0 "$pid" && serving=ok
check "the branch still serves after the refused detach" "$serving"
umount "$W/mnt"
if down a; then detached=ok; else detached=$(tail -1 "$W/detach.log" || true); fi
check "detach publishes the branch" "$detached"
left=$(losetup -j "$W/fuse-a/disk.img" 2>/dev/null || true)
check_is "the loop device is gone" "" "$left"

up f cache-b b || say "re-attach FAILED: $(tail -3 "$W/b.log" || true)"
if e2fsck -fn "$dev" >"$W/fsck" 2>&1; then
	fsck=ok
else
	fsck=$(grep -vE '^(Pass|e2fsck|/dev)' "$W/fsck" | head -1 || true)
fi
check "ext4 is clean on an empty cache" "$fsck"
mount -o noatime "$dev" "$W/mnt"
got=$(sha256sum "$W/mnt/data")
check_is "the file reads back from the store" "$want" "${got%% *}"
"$PANDO" resize --fuse "$W/fuse-b" 3G >>"$W/detach.log" 2>&1
got=$(blockdev --getsize64 "$dev")
check_is "resize grows the loop device" $((3 << 30)) "$got"
resize2fs "$dev" >/dev/null 2>&1
fs=$(df -B1 --output=size "$W/mnt" | tail -1 || true)
grown="${fs// /} bytes"
[[ ${fs// /} -gt $((2 << 30)) ]] && grown=ok
check "ext4 grows on it" "$grown"
umount "$W/mnt"
if down b; then detached=ok; else detached=$(tail -1 "$W/detach.log" || true); fi
check "detach publishes the grown branch" "$detached"

# memory_cgroup <dir>: makes a cgroup with a memory limit under the container's own, whose
# processes first move to a leaf of their own, as cgroup v2 asks before it hands out a controller.
memory_cgroup() {
	local root=/sys/fs/cgroup p procs
	mkdir -p "$root/rest" "$1" 2>/dev/null || return 1
	for _ in 1 2 3; do
		mapfile -t procs <"$root/cgroup.procs"
		for p in "${procs[@]}"; do
			echo "$p" >"$root/rest/cgroup.procs" 2>/dev/null
		done
	done
	echo +memory >"$root/cgroup.subtree_control" 2>/dev/null || return 1
	echo $((1 << 30)) >"$1/memory.max" 2>/dev/null
}
cg=/sys/fs/cgroup/pando-$TEST
if memory_cgroup "$cg"; then
	pando "$W/cache-m" init m "$SIZE"
	echo $$ >"$cg/cgroup.procs"
	up m cache-m m --dirty-limit $((64 << 20)) || say "attach in the cgroup FAILED: $(tail -3 "$W/m.log" || true)"
	mkfs.ext4 -q -F "$dev"
	mount -o noatime "$dev" "$W/mnt"
	if timeout 300 dd if=/dev/urandom of="$W/mnt/big" bs=1M count=1536 conv=fsync status=none; then
		pressure=ok
	elif grep -q "^oom_kill [1-9]" "$cg/memory.events"; then
		pressure="the daemon was OOM-killed: its anonymous memory grows with the data written"
	else
		pressure="the writes did not complete in 300 s"
	fi
	check "1.5 GiB of ext4 writes complete with the daemon in a 1 GiB cgroup" "$pressure"
	events=$(cat "$cg/memory.events" || true)
	say "cgroup memory events: ${events//$'\n'/ }"
	umount "$W/mnt"
	if ! down m; then
		say "detach in the cgroup FAILED: $(tail -1 "$W/detach.log" || true)"
	fi
	echo $$ >/sys/fs/cgroup/rest/cgroup.procs
else
	say "memory pressure: skipped, no writable cgroup v2"
fi

# perf <dev> <label>: fio's raw device rows, as bench/run.sh fio runs them, 10 s each, reads
# first so both devices read the same data.
perf() {
	local job args out row
	for job in "randread_qd1 --rw=randread --bs=4k --iodepth=1" \
		"randread_qd32 --rw=randread --bs=4k --iodepth=32 --numjobs=4" \
		"seqread --rw=read --bs=1M --iodepth=8" \
		"randwrite_qd32 --rw=randwrite --bs=4k --iodepth=32 --numjobs=4" \
		"seqwrite --rw=write --bs=1M --iodepth=8"; do
		read -ra args <<<"$job"
		out=$(fio --name=p --filename="$1" --ioengine=libaio --direct=1 --size=1G --runtime=10 --ramp_time=2 \
			--time_based --group_reporting --output-format=terse "${args[@]:1}" 2>/dev/null)
		row=$(awk -F';' '{printf "%d IOPS, %.0f MB/s, read lat %.0f us, write lat %.0f us", $8+$49, ($7+$48)/1000, $40, $81}' <<<"$out" || true)
		say "perf $2 ${args[0]}: $row"
	done
}
if [[ $PERF = 1 ]]; then
	pando "$W/cache-p" init p "$SIZE"
	up p cache-p p || say "attach for perf FAILED"
	dd if=/dev/urandom of="$dev" bs=1M count=1024 oflag=direct status=none
	perf "$dev" fuse
	down p
	attach_bg "$W/cache-p" "$W/u.log" p --device "$U" --state "$W/state-cache-p" --record false
	if attached "$W/u.log" p && usable "$U"; then
		perf "$U" ublk
		"$PANDO" detach --device "$U" >>"$W/detach.log" 2>&1
		wait "$pid"
	else
		say "perf ublk: skipped, no ublk device: $(tail -1 "$W/u.log" || true)"
	fi
	pid=
fi

k=$(kernel_errors "$TEST start $$")
check_is "no kernel I/O errors" 0 "$k"

# Last, as the file system it leaves behind fails its I/O: a signal ends the daemon under a
# mounted ext4 whose writes were never synced, and they still reach the branch.
pando "$W/cache-s" init s "$SIZE"
up s cache-s s || say "attach for the signal FAILED: $(tail -3 "$W/s.log" || true)"
mkfs.ext4 -q -F "$dev"
mount -o noatime "$dev" "$W/mnt"
head -c $((64 << 20)) /dev/urandom >"$W/mnt/late"
want=$(sha256sum "$W/mnt/late")
want=${want%% *}
kill -TERM "$pid"
exited "$pid" 120 || kill -9 "$pid"
wait "$pid"
pid=
umount -l "$W/mnt"
synced=$(grep -c "^synced 1 file systems" "$W/s.log" || true)
check_is "a signal syncs the file system on the loop device" 1 "$synced"
up s cache-t t || say "re-attach after the signal FAILED: $(tail -3 "$W/t.log" || true)"
mount -o ro,noatime "$dev" "$W/mnt"
got=$(sha256sum "$W/mnt/late" 2>/dev/null || true)
check_is "writes made before the signal reach the branch" "$want" "${got%% *}"
umount "$W/mnt"
down t || say "detach after the signal FAILED: $(tail -1 "$W/detach.log" || true)"

if [[ $fails = 0 ]]; then say "RESULT pass"; else say "RESULT FAIL ($fails)"; fi
[[ $fails = 0 ]]
