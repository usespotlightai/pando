# shellcheck shell=bash
# Shared by the kernel tests. Each test sources this, sets its own ublk ids and dm names, and
# keeps its data in $W. The store comes from run.sh (S3_* variables); each test uses its own
# prefix in it.
set -uo pipefail
: "${TEST:?set by the test that sources lib.sh}" "${RUN:?exported by run.sh}"
PANDO=${PANDO:-/pando}
R=${R:-/results}
mkdir -p "$R"

say() { echo "$*" | tee -a "$R/$TEST.log"; }

# pando <cache> <args...>: the CLI on this test's prefix of the store, with its cache dir.
pando() {
	local c=$1
	shift
	"$PANDO" --remote "s3://$TEST-$RUN" --cache "$c" "$@"
}

# attach_bg <cache> <log> <args...>: starts `attach` in the background, appending to the log,
# and sets pid to the daemon itself, so a kill reaches it and not a subshell.
attach_bg() {
	local c=$1 log=$2
	shift 2
	"$PANDO" --remote "s3://$TEST-$RUN" --cache "$c" attach "$@" >>"$log" 2>&1 &
	# shellcheck disable=SC2034 # pid is read by the test that sources lib.sh
	pid=$!
}

# attached <log> <branch> [count] [timeout_s]: waits for the count-th "attached" line.
attached() {
	local n=${3:-1} t=${4:-60} c
	for _ in $(seq $((t * 20))); do
		c=$(grep -c "^attached $2 " "$1" 2>/dev/null)
		[[ -n $c && $c -ge $n ]] && return 0
		sleep 0.05
	done
	return 1
}

# usable <dev>: waits until the block device answers.
usable() {
	for _ in $(seq 400); do
		blockdev --getsize64 "$1" >/dev/null 2>&1 && return 0
		sleep 0.05
	done
	return 1
}

# exited <pid> <timeout_s>: waits for a process to end.
exited() {
	for _ in $(seq $(($2 * 20))); do
		kill -0 "$1" 2>/dev/null || return 0
		sleep 0.05
	done
	return 1
}

# kernel_errors <marker>: I/O errors the kernel logged since the marker line.
kernel_errors() {
	dmesg | sed -n "/$1/,\$p" | grep -ciE "I/O error|blk_update_request|buffer I/O" || true
}

mark() { echo "$1" >/dev/kmsg; }

# rand_between <lo> <hi>: a random number of seconds, two decimals.
rand_between() { awk -v s="$RANDOM" -v lo="$1" -v hi="$2" 'BEGIN{srand(s); printf "%.2f", lo + rand() * (hi - lo)}'; }
