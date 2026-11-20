#!/usr/bin/env bash
# The contract docs/cli.md gives Spotlight, on a real device: `check-host --json` finds ublk,
# `attach --json` prints its "attached" line once the device serves, `status --json` reports
# the branch, a second attach of the live device and a detach of a mounted device exit with 3
# (in use) and an "error" line, and a detach prints "detached" while the attach prints
# "published" and exits 0. ublk id 80.
TEST=cli
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "$0")/lib.sh"
W=/data/$TEST
A=/dev/ublkb80
MNT=$W/mnt
pid=
stopall() {
	mountpoint -q "$MNT" && umount -l "$MNT"
	timeout 60 "$PANDO" detach --device "$A" --force >/dev/null 2>&1
	[[ -n $pid ]] && {
		exited "$pid" 60 || kill -9 "$pid" 2>/dev/null
		wait "$pid" 2>/dev/null
	}
	pid=
}
trap 'stopall; rm -rf "$W"' EXIT
rm -rf "$W"
mkdir -p "$W" "$MNT"
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

# has <file> <text>...: whether one line of the file holds every text.
has() {
	local f=$1
	shift
	awk -v n=$# 'BEGIN { for (i = 2; i <= ARGC; i++) want[i - 1] = ARGV[i]; ARGC = 2 }
		{ m = 0; for (i = 1; i <= n; i++) if (index($0, want[i])) m++; if (m == n) found = 1 }
		END { exit !found }' "$f" "$@"
}

# expect <what> <code> <file> <text>...: the last command exited with code and printed a line
# with every text.
expect() {
	local what=$1 want=$2 got=$rc f=$3
	shift 3
	if [[ $got != "$want" ]]; then
		check "$what" "exit $got, not $want: $(tail -2 "$f" | tr '\n' ' ' || true)"
	elif ! has "$f" "$@"; then
		check "$what" "no line with $*: $(tail -2 "$f" | tr '\n' ' ' || true)"
	else
		check "$what" ok
	fi
}

pando "$W/c" --json check-host >"$W/host.json" 2>>"$W/err.log"
rc=$?
expect "check-host finds ublk" 0 "$W/host.json" '"event":"host"' '"ublk":true'

pando "$W/c" --json init c $((64 << 20)) >"$W/init.json" 2>>"$W/err.log"
rc=$?
expect "init reports the branch" 0 "$W/init.json" '"event":"created"' '"branch":"c"'

"$PANDO" --json --remote "s3://$TEST-$RUN" --cache "$W/c" attach c --device "$A" --state "$W/state" \
	--record false >"$W/attach.json" 2>"$W/attach.log" &
pid=$!
ready=
for _ in $(seq 1200); do
	if has "$W/attach.json" '"event":"attached"'; then
		ready=1
		break
	fi
	sleep 0.05
done
if [[ -n $ready ]] && usable "$A"; then
	rc=0
	expect "attach reports the device once it serves" 0 "$W/attach.json" '"event":"attached"' \
		"\"device\":\"$A\"" '"branch":"c"' '"generation":1'
else
	check "attach reports the device once it serves" "$(tail -2 "$W/attach.log" | tr '\n' ' ' || true)"
	exit 1
fi

pando "$W/c2" --json attach c --device "$A" --state "$W/state2" --recover >"$W/again.json" 2>>"$W/err.log"
rc=$?
expect "a takeover of a live daemon is in use" 3 "$W/again.json" '"event":"error"' '"kind":"in-use"'

mkfs.ext4 -q "$A" && mount "$A" "$MNT" && head -c $((4 << 20)) /dev/urandom >"$MNT/f" && sync
"$PANDO" --json status --device "$A" >"$W/status.json" 2>>"$W/err.log"
rc=$?
expect "status reports the branch" 0 "$W/status.json" '"event":"status"' "\"device\":\"$A\"" \
	'"branch":"c"' '"generation":1' '"unuploaded_bytes":'

"$PANDO" --json detach --device "$A" >"$W/busy.json" 2>>"$W/err.log"
rc=$?
expect "a detach of a mounted device is in use" 3 "$W/busy.json" '"event":"error"' '"kind":"in-use"'

umount "$MNT"
"$PANDO" --json detach --device "$A" >"$W/detach.json" 2>>"$W/err.log"
rc=$?
expect "detach reports the device" 0 "$W/detach.json" '"event":"detached"' "\"device\":\"$A\""

if exited "$pid" 300; then
	wait "$pid"
	rc=$?
	expect "attach publishes and exits 0" 0 "$W/attach.json" '"event":"published"' '"branch":"c"'
else
	check "attach publishes and exits 0" "still running"
fi
pid=

if [[ $fails != 0 ]]; then
	for f in "$W"/*.json "$W"/*.log; do [[ -f $f ]] && cp "$f" "$R/$TEST.$(basename "$f")"; done
fi
if [[ $fails = 0 ]]; then say "RESULT pass"; else say "RESULT FAIL ($fails checks)"; fi
[[ $fails = 0 ]]
