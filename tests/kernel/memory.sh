#!/usr/bin/env bash
# The daemon's memory does not grow with the data written. For each frontend, ublk and FUSE with
# a loop device, the daemon and a writer share a memory cgroup of MEM_LIMIT bytes (384 MiB), and
# the writer puts 1.5 GiB of random data in a file on ext4, with an fsync at the end. The writes
# must complete, the daemon must not be OOM-killed, its peak anonymous memory must stay under
# ANON_MAX bytes (320 MiB), and the file must read back. Without ublk the ublk run is skipped.
# ublk id 70.
TEST=memory
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "$0")/lib.sh"
W=/data/$TEST
U=/dev/ublkb70
SIZE=$((4 << 30))
MB=1536
MEM_LIMIT=${MEM_LIMIT:-$((384 << 20))}
ANON_MAX=${ANON_MAX:-$((320 << 20))}
cg=/sys/fs/cgroup/pando-$TEST
pid=
sampler=
stopall() {
	[[ -n $sampler ]] && kill "$sampler" 2>/dev/null
	umount "$W/mnt" 2>/dev/null
	[[ -d $W/fuse ]] && timeout 60 "$PANDO" detach --fuse "$W/fuse" --force >/dev/null 2>&1
	timeout 60 "$PANDO" detach --device "$U" --force >/dev/null 2>&1
	[[ -n $pid ]] && {
		exited "$pid" 60 || kill -9 "$pid" 2>/dev/null
		wait "$pid" 2>/dev/null
	}
	pid=
	echo $$ >/sys/fs/cgroup/rest/cgroup.procs 2>/dev/null
	[[ -d $cg ]] && rmdir "$cg" 2>/dev/null
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

# memory_cgroup <dir> <bytes>: makes a cgroup with a memory limit under the container's own,
# whose processes first move to a leaf of their own, as cgroup v2 asks before it hands out a
# controller.
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
	echo "$2" >"$1/memory.max" 2>/dev/null
}

# sample <pid> <out>: appends the daemon's anonymous memory in KiB to out until it ends.
sample() {
	while kill -0 "$1" 2>/dev/null; do
		awk '/^RssAnon:/ { print $2 }' "/proc/$1/status" 2>/dev/null >>"$2"
		sleep 0.1
	done
}

# attach <frontend> <branch>: attaches the branch in the cgroup and sets dev and pid.
attach() {
	local fe=$1 b=$2 args
	if [[ $fe = fuse ]]; then args=(--fuse "$W/fuse"); else args=(--device "$U"); fi
	: >"$W/$fe.log"
	attach_bg "$W/cache-$fe" "$W/$fe.log" "$b" "${args[@]}" --state "$W/state-$fe" --record false \
		--dirty-limit $((64 << 20))
	attached "$W/$fe.log" "$b" || return 1
	if [[ $fe = fuse ]]; then
		dev=$(sed -n "s/^attached $b as \(\/dev\/loop[0-9]*\) .*/\1/p" "$W/$fe.log" | tail -1)
	else
		dev=$U
	fi
	[[ -n $dev ]] && usable "$dev"
}

# detach <frontend>: detaches and waits for the daemon; forced when it is gone already, so a
# dead device fails its I/O and an unmount over it does not hang.
detach() {
	local args=(--device "$U")
	[[ $1 = fuse ]] && args=(--fuse "$W/fuse")
	if kill -0 "$pid" 2>/dev/null; then
		"$PANDO" detach "${args[@]}" >>"$W/detach.log" 2>&1
	else
		timeout 60 "$PANDO" detach "${args[@]}" --force >>"$W/detach.log" 2>&1
	fi
	local rc=$?
	exited "$pid" 60 || rc=1
	wait "$pid" || rc=1
	pid=
	return "$rc"
}

if ! memory_cgroup "$cg" "$MEM_LIMIT"; then
	say "RESULT skip: no writable cgroup v2"
	exit 0
fi
head -c $((MB << 20)) /dev/urandom >"$W/data"
want=$(sha256sum <"$W/data")
for fe in ublk fuse; do
	if [[ $fe = ublk && ! -e /dev/ublk-control ]]; then
		say "$fe: skipped, no ublk on this kernel"
		continue
	fi
	b=m$fe
	pando "$W/cache-$fe" init "$b" "$SIZE"
	echo 0 >"$cg/memory.peak" 2>/dev/null
	before=$(awk '/^oom_kill / { print $2 }' "$cg/memory.events")
	echo $$ >"$cg/cgroup.procs"
	if ! attach "$fe" "$b"; then
		check "$fe: attach in a $((MEM_LIMIT >> 20)) MiB cgroup" "$(tail -1 "$W/$fe.log" || true)"
		echo $$ >/sys/fs/cgroup/rest/cgroup.procs
		stopall
		continue
	fi
	: >"$W/$fe.anon"
	sample "$pid" "$W/$fe.anon" &
	sampler=$!
	mkfs.ext4 -q -F "$dev"
	mount -o noatime "$dev" "$W/mnt"
	t0=$(date +%s.%N)
	if timeout 600 dd if="$W/data" of="$W/mnt/big" bs=1M conv=fsync status=none; then
		wrote=ok
	else
		wrote="the writes failed"
	fi
	t1=$(date +%s.%N)
	kill -0 "$pid" 2>/dev/null || wrote="the daemon is gone"
	oom=$(awk '/^oom_kill / { print $2 }' "$cg/memory.events")
	[[ $oom = "$before" ]] || wrote="the daemon was OOM-killed"
	check "$fe: 1.5 GiB of ext4 writes complete in a $((MEM_LIMIT >> 20)) MiB cgroup" "$wrote"
	peak=$(sort -n "$W/$fe.anon" | tail -1 || true)
	peak=$((${peak:-0} << 10))
	took=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.1f", b - a }' || true)
	cgpeak=$(cat "$cg/memory.peak" 2>/dev/null || echo 0)
	say "$fe: writes took $took s, daemon peak anon $((peak >> 20)) MiB, cgroup peak $((cgpeak >> 20)) MiB"
	if [[ $peak -lt $ANON_MAX ]]; then anon=ok; else anon="$((peak >> 20)) MiB"; fi
	check "$fe: the daemon's anonymous memory stays under $((ANON_MAX >> 20)) MiB" "$anon"
	if [[ $wrote = ok ]]; then
		echo 3 >/proc/sys/vm/drop_caches
		got=$(sha256sum <"$W/mnt/big")
		if [[ ${got%% *} = "${want%% *}" ]]; then same=ok; else same="the file differs"; fi
		check "$fe: the file reads back" "$same"
	fi
	if ! kill -0 "$pid" 2>/dev/null; then
		detach "$fe"
		umount -l "$W/mnt"
		check "$fe: detach publishes the branch" "the daemon is gone"
		sampler=
		echo $$ >/sys/fs/cgroup/rest/cgroup.procs
		continue
	fi
	umount "$W/mnt"
	if detach "$fe"; then published=ok; else published=$(tail -1 "$W/detach.log" || true); fi
	check "$fe: detach publishes the branch" "$published"
	kill "$sampler" 2>/dev/null
	wait "$sampler" 2>/dev/null
	sampler=
	echo $$ >/sys/fs/cgroup/rest/cgroup.procs
done

k=$(kernel_errors "$TEST start $$")
if [[ $k = 0 ]]; then check "no kernel I/O errors" ok; else check "no kernel I/O errors" "$k"; fi
if [[ $fails = 0 ]]; then say "RESULT pass"; else say "RESULT FAIL ($fails)"; fi
[[ $fails = 0 ]]
