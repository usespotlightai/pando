#!/usr/bin/env bash
# xfstests with dm-log-writes: ext4 runs on a thin volume over the scratch device, every write
# and flush is logged, then the log is replayed onto the device up to each flush or FUA point
# and the file system must be clean at each point (generic/455, generic/482, generic/757).
# The kernel needs the log-writes and thin-pool targets. A test that does not run is a failure.
TEST="log-writes"
here=$(dirname "$(readlink -f "$0")")
source "$here/lib.sh"
: > "$R/$TEST.log"
for t in log-writes thin-pool; do
	dmsetup targets | grep -q "^$t " || { say "the kernel has no $t target (dm_log_writes, dm_thin_pool)"; say "RESULT FAIL"; exit 1; }
done
XFST_NAME=$TEST MUST_RUN=1 TESTS="generic/455 generic/482 generic/757" exec "$here/xfstests.sh"
