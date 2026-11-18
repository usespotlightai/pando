#!/usr/bin/env bash
# The store stops answering while a branch serves: MinIO gets SIGSTOP, so connections open and
# requests wait, as with a store that is blackholed. A read of a chunk this host does not hold
# must fail with EIO within --store-wait and leave no process in D state; reads of what the host
# holds, writes and fsync go on, and the daemon reports the uploads behind. After SIGCONT the
# read works and the daemon reports the uploads caught up. A detach while the store is stopped
# again fails at --publish-wait; once it answers, a second detach publishes, and a fresh host
# reads every write from the store. ublk ids 71-72.
TEST=outage
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "$0")/lib.sh"
W=/data/$TEST
A=/dev/ublkb71
B=/dev/ublkb72
SIZE=$((256 << 20))
WAIT=10
MiB=$((1 << 20))
pid=
rpid=
resume() { pkill -CONT -x minio; }
stopall() {
	resume
	[[ -n $rpid ]] && {
		kill -9 "$rpid" 2>/dev/null
		wait "$rpid" 2>/dev/null
	}
	timeout 60 "$PANDO" detach --device "$A" --force >/dev/null 2>&1
	timeout 60 "$PANDO" detach --device "$B" --force >/dev/null 2>&1
	[[ -n $pid ]] && {
		exited "$pid" 60 || kill -9 "$pid" 2>/dev/null
		wait "$pid" 2>/dev/null
	}
	pid=
	rpid=
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
		[[ -n $pid ]] && ps -L -o tid=,stat=,pcpu=,wchan:32=,comm= -p "$pid" >>"$R/$TEST.threads" 2>&1
		;;
	esac
}

# readback <dev> <MiB offset> <MiB count> <file> <file MiB offset>: whether the device holds
# that part of the file.
readback() {
	dd if="$1" of="$W/got" bs=1M skip="$2" count="$3" iflag=direct status=none 2>>"$W/dd.log" || return 1
	dd if="$4" of="$W/want" bs=1M skip="$5" count="$3" status=none || return 1
	cmp -s "$W/got" "$W/want"
}

# serve <cache> <log> <device>: attaches branch o on an empty cache and waits for the device.
serve() {
	attach_bg "$1" "$2" o --device "$3" --state "$1.state" --record false --store-wait "${WAIT}s"
	attached "$2" o && usable "$3"
}

# publish <dev> <log>: detaches the device and waits for the daemon to publish the branch.
publish() {
	if timeout 300 "$PANDO" detach --device "$1" >>"$2" 2>&1 && exited "$pid" 300 && wait "$pid"; then
		pid=
		return 0
	fi
	pid=
	return 1
}

# waitline <log> <pattern> <timeout_s>: waits for a line of the daemon log.
waitline() {
	for _ in $(seq $(($3 * 20))); do
		grep -q "$2" "$1" 2>/dev/null && return 0
		sleep 0.05
	done
	return 1
}

head -c $((32 * MiB)) /dev/urandom >"$W/data"
head -c $((4 * MiB)) /dev/urandom >"$W/new"
pando "$W/cache-w" init o "$SIZE"
if ! serve "$W/cache-w" "$W/w.log" "$A"; then
	say "attach FAILED: $(tail -1 "$W/w.log" || true)"
	exit 1
fi
dd if="$W/data" of="$A" bs=1M oflag=direct conv=fsync status=none
publish "$A" "$W/w.log" || check publish "the first detach did not publish"

log=$W/o.log
if ! serve "$W/cache-o" "$log" "$A"; then
	say "re-attach FAILED: $(tail -1 "$log" || true)"
	exit 1
fi
if readback "$A" 0 1 "$W/data" 0; then check "read before the outage" ok; else check "read before the outage" "differs"; fi

mark "pando outage: SIGSTOP"
pkill -STOP -x minio || check "stop the store" "no minio"
t0=$(date +%s)
if timeout 5 dd if="$A" of=/dev/null bs=64k count=1 iflag=direct status=none && readback "$A" 0 1 "$W/data" 0; then
	check "cached read during the outage" ok
else
	check "cached read during the outage" "failed or slow"
fi
if timeout 5 dd if="$W/new" of="$A" bs=1M seek=128 oflag=direct conv=fsync status=none 2>>"$W/dd.log"; then
	check "write and fsync during the outage" ok
else
	check "write and fsync during the outage" "failed or slow"
fi

dd if="$A" of=/dev/null bs=64k skip=$((24 * 16)) count=1 iflag=direct status=none 2>"$W/eio" &
rpid=$!
if exited "$rpid" $((WAIT + 15)); then
	wait "$rpid"
	st=$?
	rpid=
	t1=$(date +%s)
	if [[ $st != 0 ]] && grep -q "Input/output error" "$W/eio"; then
		check "uncached read during the outage" "ok, EIO after $((t1 - t0)) s"
	else
		check "uncached read during the outage" "exit $st: $(cat "$W/eio" || true)"
	fi
else
	check "uncached read during the outage" "still waiting after $((WAIT + 15)) s, state $(ps -o stat= -p "$rpid" || true)"
fi
stuck=$(ps -eo stat=,pid=,comm= | awk '$1 ~ /^D/ && $3 == "dd"')
if [[ -z $stuck ]]; then check "no dd in D state" ok; else check "no dd in D state" "$stuck"; fi

if waitline "$log" "uploads of o are .* behind" 90; then check "degraded reported" ok; else check "degraded reported" "no line"; fi

mark "pando outage: SIGCONT"
resume
if readback "$A" 24 1 "$W/data" 24; then check "uncached read after the outage" ok; else check "uncached read after the outage" "differs or failed"; fi
if readback "$A" 128 4 "$W/new" 0; then check "the outage's write" ok; else check "the outage's write" "differs"; fi
if waitline "$log" "uploads of o caught up" 120; then check "caught up reported" ok; else check "caught up reported" "no line"; fi

mark "pando outage: SIGSTOP before detach"
pkill -STOP -x minio || check "stop the store again" "no minio"
timeout 5 dd if="$W/new" of="$A" bs=1M seek=160 count=1 oflag=direct conv=fsync status=none 2>>"$W/dd.log" ||
	check "write before the detach" "failed or slow"
if timeout 60 "$PANDO" detach --device "$A" --publish-wait 5s >>"$W/detach.log" 2>&1; then
	check "detach during the outage" "it returned before the store answered"
elif grep -q "may still be publishing" "$W/detach.log"; then
	check "detach during the outage" "ok, fails at --publish-wait"
else
	check "detach during the outage" "$(tail -1 "$W/detach.log" || true)"
fi
mark "pando outage: SIGCONT before detach"
resume
if publish "$A" "$log"; then check "detach after the outage" ok; else check "detach after the outage" "$(tail -1 "$log" || true)"; fi

if serve "$W/cache-f" "$W/f.log" "$B"; then
	if readback "$B" 0 32 "$W/data" 0 && readback "$B" 128 4 "$W/new" 0 && readback "$B" 160 1 "$W/new" 0; then
		check "a fresh host reads every write" ok
	else
		check "a fresh host reads every write" "differs"
	fi
	publish "$B" "$W/f.log" || true
else
	check "a fresh host reads every write" "attach: $(tail -1 "$W/f.log" || true)"
fi

if [[ $fails != 0 ]]; then
	for f in "$W"/*.log; do cp "$f" "$R/$TEST.$(basename "$f")"; done
fi
if [[ $fails = 0 ]]; then say "RESULT pass"; else say "RESULT FAIL ($fails checks)"; fi
[[ $fails = 0 ]]
