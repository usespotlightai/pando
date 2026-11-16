#!/usr/bin/env bash
# Runs on a bench host (see bench/infra), as root, from a copy of the repository.
#   bench/run.sh smoke    checks that the host is ready for the benchmarks
# Results go to $PANDO_BENCH_RESULTS (default /data/results). Pando's store keys, when the
# workflow passed them, are in the file named by PANDO_ENV_FILE and are never printed.
#
# The measures in bench/README.md are not implemented yet; only `smoke` exists.
set -euo pipefail

set -a
. /etc/pando-bench.env
set +a
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

host_facts() {
  {
    echo "host=$PANDO_BENCH_HOST"
    echo "instance_type=$(imds instance-type)"
    echo "availability_zone=$(imds placement/availability-zone)"
    echo "kernel=$(uname -r)"
    echo "os=$(. /etc/os-release && echo "$PRETTY_NAME")"
    echo "cpus=$(nproc)"
    echo "memory_kib=$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
    echo "data_bytes=$(df -B1 --output=size /data | tail -1 | tr -d ' ')"
  } >"$results/host-$PANDO_BENCH_HOST.txt"
}

store_keys_work() {
  [ -r "$PANDO_ENV_FILE" ] || return 1
  (
    set -a
    # shellcheck source=/dev/null
    . "$PANDO_ENV_FILE"
    set +a
    env -u AWS_SESSION_TOKEN AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$S3_SECRET_ACCESS_KEY" \
      aws s3api head-bucket --bucket "$S3_BUCKET" --region "$S3_REGION"
  )
}

bucket_round_trip() {
  local key="smoke/$PANDO_BENCH_HOST-$$"
  echo pando | aws s3 cp - "s3://$S3_BUCKET/$key" &&
    [ "$(aws s3 cp "s3://$S3_BUCKET/$key" -)" = pando ] &&
    aws s3 rm "s3://$S3_BUCKET/$key"
}

registry_login() {
  aws ecr get-login-password --region "$AWS_REGION" |
    docker login --username AWS --password-stdin "${PANDO_BENCH_REGISTRY%%/*}"
}

smoke() {
  host_facts
  check "kernel module ublk_drv" grep -q '^ublk_drv ' /proc/modules
  check "kernel module nbd" grep -q '^nbd ' /proc/modules
  check "kernel module dm_mod" grep -qE '^dm_mod ' /proc/modules
  check "/data is XFS with reflink" sh -c 'findmnt -n -o FSTYPE /data | grep -qx xfs && xfs_info /data | grep -q "reflink=1"'
  check "reflink copy on /data" sh -c 'f=/data/.reflink-$$; head -c 1M /dev/urandom >$f && cp --reflink=always $f $f.2; r=$?; rm -f $f $f.2; exit $r'
  check "bucket via the host role" bucket_round_trip
  check "registry login" registry_login
  check "docker" docker info
  check "fio" fio --version
  if [ "$PANDO_BENCH_HOST" = a ]; then
    check "EBS volume attached" sh -c "lsblk -dn -o SERIAL | grep -q '${PANDO_BENCH_EBS_VOLUME/-/}'"
  fi
  check "Pando store keys" store_keys_work
  [ "$failed" -eq 0 ] && echo "smoke: ready" || echo "smoke: not ready"
  return "$failed"
}

case ${1:-} in
  smoke) smoke | tee "$results/smoke-$PANDO_BENCH_HOST.txt"; exit "${PIPESTATUS[0]}" ;;
  *) echo "usage: bench/run.sh smoke" >&2; exit 2 ;;
esac
