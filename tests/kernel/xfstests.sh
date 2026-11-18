#!/usr/bin/env bash
# xfstests (fstests) with ext4, on two loop devices over files first, then on two pando
# volumes. A test fails here only when it fails on pando and not on loop: the kernel and the
# container decide the rest. Tests in xfstests.known are pando failures with a known cause.
# The dm-log-writes tests get a loop device over a sparse file as LOGWRITES_DEV.
# TESTS goes to ./check (default "-g quick"). DEVS picks the runs (default "loop pando").
# XFST_NAME names the results (default xfstests); each run keeps the kernel log and the files
# of each failed test under its own directory. MUST_RUN=1 makes a test that does not run a
# failure. ublk ids 64-65.
TEST=${XFST_NAME:-xfstests}
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "$0")/lib.sh"
self=$(readlink -f "$0")
here=$(dirname "$self")
W=/data/$TEST
T=/dev/ublkb64
S=/dev/ublkb65
SIZE=$((10 << 30))
TESTS=${TESTS:--g quick}
read -ra tests <<<"$TESTS"
DEVS=${DEVS:-loop pando}
MUST_RUN=${MUST_RUN:-0}
XFST=/var/lib/xfstests
pids=()
loops=()
stopall() {
	umount /mnt/xt-test /mnt/xt-scratch 2>/dev/null
	"$PANDO" detach --device "$T" >/dev/null 2>&1
	"$PANDO" detach --device "$S" >/dev/null 2>&1
	for p in "${pids[@]}"; do wait "$p" 2>/dev/null; done
	for l in "${loops[@]}"; do losetup -d "$l" 2>/dev/null; done
	pids=()
	loops=()
}
trap 'stopall; rm -rf "$W"' EXIT
rm -rf "$W"
mkdir -p "$W" /mnt/xt-test /mnt/xt-scratch
: >"$R/$TEST.log"

attach() { # branch dev
	pando "$W/cache-$1" init "$1" "$SIZE"
	: >"$R/$TEST-$1.log"
	attach_bg "$W/cache-$1" "$R/$TEST-$1.log" "$1" --device "$2" --state "$W/state-$1" --record false
	pids+=("$pid")
	attached "$R/$TEST-$1.log" "$1" && usable "$2"
}

# run <devs>: one xfstests run; prints its failures to $W/failed-<devs>.
run() {
	local devs=$1 out=$R/$TEST-$1 td sd ld
	rm -rf "$out"
	mkdir -p "$out"
	case $devs in
	loop)
		truncate -s "$SIZE" "$W/test.img" "$W/scratch.img"
		td=$(losetup -f --show "$W/test.img")
		loops+=("$td")
		sd=$(losetup -f --show "$W/scratch.img")
		loops+=("$sd")
		;;
	pando)
		if ! attach xt-test "$T" || ! attach xt-scratch "$S"; then
			say "pando: attach FAILED"
			return 1
		fi
		td=$T
		sd=$S
		;;
	*) ;;
	esac
	rm -f "$W/log.img"
	truncate -s "$SIZE" "$W/log.img"
	ld=$(losetup -f --show "$W/log.img")
	loops+=("$ld")
	mkfs.ext4 -q -F -b 4096 "$td"
	cat >"$XFST/local.config" <<-EOF
		export FSTYP=ext4
		export TEST_DEV=$td
		export TEST_DIR=/mnt/xt-test
		export SCRATCH_DEV=$sd
		export SCRATCH_MNT=/mnt/xt-scratch
		export LOGWRITES_DEV=$ld
		export MKFS_OPTIONS="-b 4096"
	EOF
	dmesg -W >"$out/dmesg.txt" 2>&1 &
	local dpid=$!
	mark "$TEST start $devs $$"
	(cd "$XFST" && ./check -E "$here/xfstests.exclude" "${tests[@]}" >"$out/check.out" 2>&1)
	local rc=$?
	kill "$dpid" 2>/dev/null
	wait "$dpid" 2>/dev/null
	cp "$XFST/results/check.log" "$out/" 2>/dev/null
	local d
	for d in "$XFST/results/"*/; do
		mkdir -p "$out/$(basename "$d")"
		cp "$d"*.out.bad "$d"*.full "$d"*.dmesg "$out/$(basename "$d")/" 2>/dev/null
	done
	grep -E "^(Ran|Not run|Failures|Passed)" "$out/check.out" | sed "s/^/$devs: /" | tee -a "$R/$TEST.log"
	grep "^Failures:" "$out/check.out" | cut -d: -f2 | tr ' ' '\n' | sed '/^$/d' | sort >"$W/failed-$devs"
	stopall
	rm -rf "$XFST/results"
	grep -q "^Ran:" "$out/check.out" || {
		say "$devs: check did not run (rc=$rc)"
		return 1
	}
	if [[ $MUST_RUN = 1 ]] && grep -q "^Not run:" "$out/check.out"; then
		grep -F "[not run]" "$out/check.out" | sed "s/^/$devs: /" | tee -a "$R/$TEST.log"
		say "$devs: tests did not run"
		return 1
	fi
}

ok=1
for d in $DEVS; do run "$d" || ok=0; done
if [[ $ok = 1 && " $DEVS " == *" pando "* ]]; then
	known=$here/xfstests.known
	base=$W/failed-loop
	[[ -f $base ]] || : >"$base"
	new=$(comm -23 "$W/failed-pando" "$base" | grep -vxFf <(grep -o '^[a-z0-9]*/[0-9]*' "$known") || true)
	say "pando-only failures: ${new:-none}"
	[[ -z $new ]] || ok=0
fi
if [[ $ok = 1 ]]; then say "RESULT pass"; else say "RESULT FAIL"; fi
[[ $ok = 1 ]]
