#!/usr/bin/env bash
# Discard and write-zeroes on a pando device. The device must offer both with 4 KiB
# granularity, and a cleared range must read as zeros: live, from the store on an empty cache,
# and on a branch copied from a parent with data there, while the parent keeps its data.
# fstrim on ext4 must trim and leave a clean file system. ublk ids 66-67.
TEST=discard
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "$0")/lib.sh"
W=/data/$TEST
A=/dev/ublkb66
B=/dev/ublkb67
SIZE=$((1 << 30))
DATA=$((256 << 20))
pid=
stopall() {
	umount "/mnt/$TEST" 2>/dev/null
	timeout 60 "$PANDO" detach --device "$A" >/dev/null 2>&1
	timeout 60 "$PANDO" detach --device "$B" >/dev/null 2>&1
	[[ -n $pid ]] && {
		exited "$pid" 60 || kill -9 "$pid" 2>/dev/null
		wait "$pid" 2>/dev/null
	}
	pid=
}
trap 'stopall; rm -rf "$W"' EXIT
rm -rf "$W"
mkdir -p "$W" "/mnt/$TEST"
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

# last <file>: the last line of the file.
last() { tail -1 "$1"; }

# up <branch> <dev> <cache>: attaches the branch and waits for its device.
up() {
	: >"$W/$1.log"
	attach_bg "$W/$3" "$W/$1.log" "$1" --device "$2" --state "$W/state-$3" --record false
	attached "$W/$1.log" "$1" && usable "$2"
}

down() {
	"$PANDO" detach --device "$1" >>"$W/detach.log" 2>&1
	wait "$pid"
	local rc=$?
	pid=
	return "$rc"
}

# same <dev> <file>: the first bytes of the device equal the file.
same() {
	local n at
	n=$(stat -c %s "$2")
	if cmp -s -n "$n" "$1" "$2"; then
		echo ok
	else
		at=$(cmp -n "$n" "$1" "$2" | awk '{print $5}' || true)
		echo "differs at $at"
	fi
}

head -c "$DATA" /dev/urandom >"$W/parent.img"
pando "$W/cache-p" init p "$SIZE"
up p "$A" cache-p || {
	say "attach FAILED: $(last "$W/p.log" || true)"
	exit 1
}
dd if="$W/parent.img" of="$A" bs=1M oflag=direct status=none
down "$A" || {
	say "publish FAILED: $(last "$W/detach.log" || true)"
	exit 1
}
pando "$W/cache-p" branch p c

up c "$A" cache-c || {
	say "attach FAILED: $(last "$W/c.log" || true)"
	exit 1
}
q=/sys/block/$(basename "$A")/queue
granularity=$(cat "$q/discard_granularity")
discard_max=$(cat "$q/discard_max_bytes")
zeroes_max=$(cat "$q/write_zeroes_max_bytes")
limits="discard_granularity=$granularity discard_max_bytes=$discard_max write_zeroes_max_bytes=$zeroes_max"
say "limits: $limits"
offered=$limits
[[ $granularity = 4096 && $discard_max -gt 0 && $zeroes_max -gt 0 ]] && offered=ok
check "the device offers discard and write-zeroes" "$offered"

cp "$W/parent.img" "$W/child.img"
clear() { # clear <how> <offset> <length>
	if [[ $1 = discard ]]; then
		blkdiscard -o "$2" -l "$3" "$A"
	else
		blkdiscard -z -o "$2" -l "$3" "$A"
	fi
	dd if=/dev/zero of="$W/child.img" bs=4096 seek=$(($2 / 4096)) count=$(($3 / 4096)) conv=notrunc status=none
}
clear discard $(((1 << 20) + 4096)) $((64 << 20))
clear zeroout $(((128 << 20) + 12288)) $(((32 << 20) + 8192))
clear discard $((200 << 20)) 4096
check "cleared ranges read as zeros" "$(same "$A" "$W/child.img" || true)"
down "$A" || check "publish" "$(last "$W/detach.log" || true)"

up c "$B" cache-c-re || check "re-attach" "$(last "$W/c.log" || true)"
check "cleared ranges read as zeros from the store" "$(same "$B" "$W/child.img" || true)"
down "$B"
up p "$B" cache-p-re || check "re-attach the parent" "$(last "$W/p.log" || true)"
check "the parent keeps its data" "$(same "$B" "$W/parent.img" || true)"
down "$B"

pando "$W/cache-e" init e "$SIZE"
up e "$A" cache-e || {
	say "attach FAILED: $(last "$W/e.log" || true)"
	exit 1
}
t0=$(date +%s.%N)
mkfs.ext4 -q -F "$A"
t1=$(date +%s.%N)
secs=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b - a}')
say "mkfs.ext4 with a discard of the whole device: $secs s"
mount -o noatime "$A" "/mnt/$TEST"
head -c $((200 << 20)) /dev/urandom >"/mnt/$TEST/f"
sync
rm "/mnt/$TEST/f"
sync
trim=$(fstrim -v "/mnt/$TEST" || true)
trimmed=$(grep -o '([0-9]* bytes)' <<<"$trim" | tr -dc 0-9 || true)
trimmed=${trimmed:-0}
say "fstrim trimmed $trimmed bytes"
if [[ $trimmed -ge $((200 << 20)) ]]; then trims=ok; else trims="$trimmed bytes"; fi
check "fstrim trims the freed blocks" "$trims"
umount "/mnt/$TEST"
if e2fsck -fn "$A" >"$W/fsck" 2>&1; then
	fsck=ok
else
	fsck=$(grep -vE '^(Pass|e2fsck|/dev)' "$W/fsck" | head -1 || true)
fi
check "ext4 is clean after fstrim" "$fsck"
down "$A"

k=$(kernel_errors "$TEST start $$")
if [[ $k = 0 ]]; then errors=ok; else errors=$k; fi
check "no kernel I/O errors" "$errors"
if [[ $fails = 0 ]]; then say "RESULT pass"; else say "RESULT FAIL ($fails)"; fi
[[ $fails = 0 ]]
