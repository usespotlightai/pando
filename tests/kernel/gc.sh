#!/usr/bin/env bash
# GC on a bucket of its own, with a branch that stays and one deleted. `gc --delete` refuses the
# bucket while it keeps no earlier versions. Once versioning is on, GC deletes what only the
# deleted branch reached, and the restore in docs/hosts.md, deleting the delete markers, brings
# the branch back: a fresh host reads it whole. ublk id 79.
TEST=gc
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "$0")/lib.sh"
W=/data/$TEST
A=/dev/ublkb79
MiB=$((1 << 20))
pid=
stopall() {
	timeout 60 "$PANDO" detach --device "$A" --force >/dev/null 2>&1
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

: "${S3_ENDPOINT:?}" "${S3_REGION:?}" "${S3_ACCESS_KEY_ID:?}" "${S3_SECRET_ACCESS_KEY:?}" "${SSL_CERT_FILE:?}"
export S3_BUCKET=$TEST-$RUN
bucket=$S3_ENDPOINT/$S3_BUCKET
s3() { curl -fsS --cacert "$SSL_CERT_FILE" --aws-sigv4 "aws:amz:$S3_REGION:s3" --user "$S3_ACCESS_KEY_ID:$S3_SECRET_ACCESS_KEY" "$@"; }
if ! s3 -X PUT "$bucket" >/dev/null; then
	say "could not create bucket $S3_BUCKET"
	exit 1
fi

# markers: the key and version of every delete marker under this test's prefix, one a line.
markers() {
	s3 "$bucket?versions&prefix=$TEST-$RUN/" | awk 'BEGIN { RS = "</DeleteMarker>" } /<DeleteMarker>/ {
		sub(/.*<DeleteMarker>/, "")
		k = $0; sub(/.*<Key>/, "", k); sub(/<\/Key>.*/, "", k)
		v = $0; sub(/.*<VersionId>/, "", v); sub(/<\/VersionId>.*/, "", v)
		print k, v
	}'
}

collect() { pando "$W/gc" gc --delete --keep-young 0s --validate 1s >"$W/gc.out" 2>&1; }

head -c $((8 * MiB)) /dev/urandom >"$W/keep.img"
head -c $((8 * MiB)) /dev/urandom >"$W/gone.img"
if pando "$W/c" import keep "$W/keep.img" >"$W/import.out" 2>&1 &&
	pando "$W/c" import gone "$W/gone.img" >>"$W/import.out" 2>&1 &&
	pando "$W/c" delete gone >>"$W/import.out" 2>&1; then
	check "import two branches and delete one" ok
else
	check "import two branches and delete one" "$(tail -1 "$W/import.out" || true)"
fi

if collect; then
	check "gc --delete refuses a bucket without versioning" "it ran: $(tail -1 "$W/gc.out" || true)"
elif grep -q -- "--unversioned" "$W/gc.out"; then
	check "gc --delete refuses a bucket without versioning" ok
else
	check "gc --delete refuses a bucket without versioning" "$(tail -1 "$W/gc.out" || true)"
fi

versioning='<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>'
if ! s3 -X PUT --data "$versioning" "$bucket?versioning" >/dev/null; then
	say "could not turn on versioning for $S3_BUCKET"
	exit 1
fi
dead=0
if collect; then
	dead=$(grep -o '[0-9]* dead' "$W/gc.out" | cut -d' ' -f1 || true)
	if [[ ${dead:-0} -gt 0 ]] && grep -q ", deleted" "$W/gc.out"; then
		check "gc --delete deletes from a versioned bucket" "ok, $dead segments"
	else
		check "gc --delete deletes from a versioned bucket" "$(tail -1 "$W/gc.out" || true)"
	fi
else
	check "gc --delete deletes from a versioned bucket" "$(tail -1 "$W/gc.out" || true)"
fi

n=0
while read -r key version; do
	[[ -z $key ]] && continue
	s3 -X DELETE "$S3_ENDPOINT/$S3_BUCKET/$key?versionId=$version" >/dev/null && n=$((n + 1))
done < <(markers || true)
if pando "$W/c" refs >"$W/refs.out" 2>&1 && grep -q "^gone " "$W/refs.out"; then
	check "deleting $n delete markers restores the branch" ok
else
	check "deleting $n delete markers restores the branch" "$(cat "$W/refs.out" || true)"
fi

log=$W/fresh.log
attach_bg "$W/fresh" "$log" gone --device "$A" --state "$W/state" --record false
if attached "$log" gone && usable "$A"; then
	if cmp -n $((8 * MiB)) "$A" "$W/gone.img" >"$W/cmp.out" 2>&1; then
		check "a fresh host reads the restored branch whole" ok
	else
		check "a fresh host reads the restored branch whole" "$(cat "$W/cmp.out" || true)"
	fi
	timeout 300 "$PANDO" detach --device "$A" >>"$log" 2>&1
	exited "$pid" 300
else
	check "a fresh host reads the restored branch whole" "attach: $(tail -1 "$log" || true)"
fi
pid=

if [[ $fails != 0 ]]; then
	for f in "$W"/*.out "$W"/*.log; do [[ -f $f ]] && cp "$f" "$R/$TEST.$(basename "$f")"; done
fi
if [[ $fails = 0 ]]; then say "RESULT pass"; else say "RESULT FAIL ($fails checks)"; fi
[[ $fails = 0 ]]
