#!/usr/bin/env bash
# Runs on a bench host (see bench/infra), as root, from a copy of the repository.
#   bench/run.sh smoke         checks that the host is ready for the benchmarks
#   bench/run.sh workspace     builds the workspace image (once per host)
#   bench/run.sh fork [args]   the fork measure (python3 bench/harness fork --help)
#   bench/run.sh cold | warm | replay [args]   the start and workload measures
#   bench/run.sh fio | save | crash [args]     raw I/O, saving after npm ci, and crash survival
# Results go to $PANDO_BENCH_RESULTS (default /data/results). Pando's store keys, when the
# workflow passed them, are in the file named by PANDO_ENV_FILE and are never printed.
#
# Of the measures in bench/README.md, `fio` and `crash` are not implemented yet.
set -euo pipefail

set -a
# shellcheck source=/dev/null
. /etc/pando-bench.env
set +a
export PANDO_BENCH_HOST PANDO_BENCH_REGISTRY PANDO_BENCH_EBS_VOLUME AWS_REGION S3_BUCKET S3_REGION
export PANDO_ENV_FILE=${PANDO_ENV_FILE:-/etc/pando-bench.secrets}
results=${PANDO_BENCH_RESULTS:-/data/results}
mkdir -p "$results"

failed=0
check() { # name, command...
	local name=$1
	shift
	if "$@" >/dev/null 2>&1; then
		echo "ok    $name"
	else
		echo "FAIL  $name"
		failed=1
	fi
}

imds() {
	local token
	token=$(curl -sf -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')
	curl -sf -H "X-aws-ec2-metadata-token: $token" "http://169.254.169.254/latest/meta-data/$1"
}

os_name() (
	# shellcheck source=/dev/null
	. /etc/os-release && echo "${PRETTY_NAME?}"
)

memory_kib() {
	awk '/MemTotal/ {print $2}' /proc/meminfo
}

data_bytes() {
	df -B1 --output=size /data | tail -1 | tr -d ' '
}

fact() { # name, command...: prints name=output, with what output there is when the command fails
	local name=$1 value
	shift
	value=$("$@") || true
	echo "$name=$value"
}

host_facts() {
	{
		echo "host=$PANDO_BENCH_HOST"
		fact instance_type imds instance-type
		fact availability_zone imds placement/availability-zone
		fact kernel uname -r
		fact os os_name
		fact cpus nproc
		fact memory_kib memory_kib
		fact data_bytes data_bytes
	} >"$results/host-$PANDO_BENCH_HOST.txt"
}

store_keys_work() {
	[[ -r $PANDO_ENV_FILE ]] || return 1
	(
		set -a
		# shellcheck source=/dev/null
		. "$PANDO_ENV_FILE"
		set +a
		env -u AWS_SESSION_TOKEN AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID?}" \
			AWS_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY?}" \
			aws s3api head-bucket --bucket "$S3_BUCKET" --region "$S3_REGION"
	)
}

bucket_round_trip() {
	local key="smoke/$PANDO_BENCH_HOST-$$" got
	echo pando | aws s3 cp - "s3://$S3_BUCKET/$key" || return 1
	got=$(aws s3 cp "s3://$S3_BUCKET/$key" -) || return 1
	[[ $got == pando ]] || return 1
	aws s3 rm "s3://$S3_BUCKET/$key"
}

registry_login() {
	aws ecr get-login-password --region "$AWS_REGION" |
		docker login --username AWS --password-stdin "${PANDO_BENCH_REGISTRY%%/*}"
}

reflink_copy() {
	local f=/data/.reflink-$$ r
	head -c 1M /dev/urandom >"$f" && cp --reflink=always "$f" "$f.2"
	r=$?
	rm -f "$f" "$f.2"
	return "$r"
}

smoke() {
	host_facts
	for m in ublk_drv nbd dm_mod; do
		check "kernel module $m" test -d "/sys/module/$m"
	done
	check "/data is XFS with reflink" sh -c 'findmnt -n -o FSTYPE /data | grep -qx xfs && xfs_info /data | grep -q "reflink=1"'
	check "reflink copy on /data" reflink_copy
	check "bucket via the host role" bucket_round_trip
	check "registry login" registry_login
	check "docker" docker info
	check "fio" fio --version
	if [[ $PANDO_BENCH_HOST == a ]]; then
		check "EBS volume attached" sh -c "lsblk -dn -o SERIAL | grep -q '${PANDO_BENCH_EBS_VOLUME/-/}'"
	fi
	check "Pando store keys" store_keys_work
	if [[ $failed -eq 0 ]]; then
		echo "smoke: ready"
	else
		echo "smoke: not ready"
	fi
	return "$failed"
}

case ${1:-} in
smoke)
	smoke | tee "$results/smoke-$PANDO_BENCH_HOST.txt"
	exit "${PIPESTATUS[0]}"
	;;
workspace | fork | cold | warm | replay | fio | save | crash) python3 "$(dirname "$0")/harness" "$@" ;;
*)
	echo "usage: bench/run.sh smoke | workspace | fork | cold | warm | replay | fio | save | crash [args]" >&2
	exit 2
	;;
esac
