#!/usr/bin/env bash
# Runs kernel tests in a privileged container on this host, which needs the ublk_drv, dm_mod
# and loop modules. PANDO is the pando binary to test; results land in RESULTS. DATA, when set,
# is a host directory for the store, caches and images (xfstests needs tens of GiB).
#   tests/kernel/host.sh fio-verify crash discard
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
PANDO=${PANDO:?set PANDO to the pando binary}
RESULTS=${RESULTS:-$PWD/kernel-results}
mkdir -p "$RESULTS"
[[ $# -gt 0 ]] || set -- fio-verify crash discard
docker build -q -t pando-kernel-tests "$here" >/dev/null
data=()
[[ -n ${DATA:-} ]] && data=(-v "$DATA":/data)
pando=$(readlink -f "$PANDO")
docker run --rm --privileged -v /dev:/dev "${data[@]}" -v "$pando":/pando:ro -v "$RESULTS":/results \
	-e MODES -e TESTS -e DEVS -e ASAN_OPTIONS pando-kernel-tests "$@"
