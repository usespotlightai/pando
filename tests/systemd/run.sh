#!/usr/bin/env bash
# Runs a branch under the pando-attach@ unit on this host's systemd, as docs/hosts.md sets it up,
# with ext4 mounted on it, and checks every file written so far after each step:
#   install: packaging/install.sh puts release 1 from archives made by packaging/package.sh.
#   upgrade: install.sh --restart puts release 2 while a writer fsyncs files; the old daemon
#     drains, the new binary takes the device over, and the writer sees no error.
#   crash: SIGKILL to the daemon; systemd restarts it and --recover=auto takes the device over.
#   stop: systemctl stop drains and leaves the device; systemctl start takes it over.
#   detach: umount and pando detach; the unit stops for good with status 0.
#   store: the cache and state are deleted and the unit opens the branch from the store.
#   gc: pando-gc.service reports on the store once and deletes nothing.
# It changes this host's /usr/local and /etc, so it refuses a host where pando is installed.
# Run as root with the ublk_drv module loaded. PANDO is the pando binary; RESULTS gets the log.
# The store is MinIO from the kernel test image (tests/kernel), over TLS on 127.0.0.1:9100.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "${here}/../.." && pwd)
PANDO=$(readlink -f "${PANDO:?set PANDO to the pando binary}")
RESULTS=${RESULTS:-${PWD}/kernel-results}
W=${DATA:-/var/tmp}/pando-systemd
U=pando-attach@sd.service
DEV=/dev/ublkb70
MNT=${W}/mnt
mkdir -p "${RESULTS}"
LOG=${RESULTS}/systemd.log
: >"${LOG}"
writer=
pid=

say() { echo "$*" | tee -a "${LOG}"; }
fail() {
	say "FAIL: $*"
	exit 1
}

uid=$(id -u)
[[ ${uid} == 0 ]] || fail "run as root"
[[ -d /run/systemd/system ]] || fail "systemd is not running"
[[ -e /dev/ublk-control ]] || fail "no /dev/ublk-control: load ublk_drv"
units=(/etc/systemd/system/pando-attach@.service /etc/systemd/system/pando-gc.service /etc/systemd/system/pando-gc.timer)
for p in /usr/local/bin/pando /usr/local/lib/pando /etc/pando "${units[@]}"; do
	[[ ! -e ${p} ]] || fail "${p} exists: this test installs pando and needs a host without it"
done

# store CMD...: runs CMD with the host's store settings, as the unit gets them.
store() {
	(
		set -a
		# shellcheck source=/dev/null
		. /etc/pando/pando.env || exit 1
		set +a
		"$@" --cache "${W}/cache"
	)
}

cleanup() {
	journalctl -u "${U}" -u pando-gc.service --no-pager >"${RESULTS}/systemd-journal.log" 2>&1
	[[ -n ${writer} ]] && kill "${writer}" 2>/dev/null
	mountpoint -q "${MNT}" && umount -l "${MNT}"
	systemctl stop "${U}" 2>/dev/null
	[[ -b ${DEV} ]] && store pando detach --device "${DEV}" --force >/dev/null 2>&1
	systemctl reset-failed "${U}" pando-gc.service 2>/dev/null
	docker rm -f pando-systemd-minio >/dev/null 2>&1
	rm -rf /usr/local/bin/pando /usr/local/lib/pando /etc/pando /var/cache/pando-gc "${units[@]}"
	systemctl daemon-reload
	rm -rf "${W}"
}
trap cleanup EXIT
rm -rf "${W}"
mkdir -p "${W}/certs" "${W}/minio" "${W}/rel" "${MNT}" || fail "mkdir ${W}"

# show PROPERTY: the unit's property.
show() { systemctl show -P "$1" "${U}"; }

# active STEP: waits up to 120 s for the unit to be active with a daemon, and sets pid to it.
active() {
	local state
	for _ in $(seq 240); do
		state=$(show ActiveState)
		pid=$(show MainPID)
		[[ ${state} == active && ${pid} != 0 ]] && return
		sleep 0.5
	done
	fail "$1: ${U} is ${state}"
}

# files N: writes files f1..fN of 1 MiB from the source, each with fsync, 0.1 s apart.
files() {
	local i
	for i in $(seq "$1"); do
		[[ -e ${MNT}/f${i} ]] && continue
		dd if="${W}/src" of="${MNT}/f${i}" bs=1M skip=$((i - 1)) count=1 conv=fsync status=none || return 1
		sleep 0.1
	done
}

# check STEP: every file written so far still holds its bytes.
check() {
	local n=0 f i
	for f in "${MNT}"/f*; do
		[[ -e ${f} ]] || continue
		i=${f##*/f}
		dd if="${W}/src" of="${W}/want" bs=1M skip=$((i - 1)) count=1 status=none || fail "$1: reading the source"
		cmp -s "${f}" "${W}/want" || fail "$1: f${i} differs from what was written"
		n=$((n + 1))
	done
	[[ ${n} -gt 0 ]] || fail "$1: no files"
	say "$1: ${n} files hold their bytes"
}

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 -subj /CN=localhost \
	-addext "subjectAltName=IP:127.0.0.1" -addext "basicConstraints=critical,CA:FALSE" \
	-keyout "${W}/certs/private.key" -out "${W}/certs/public.crt" 2>/dev/null || fail "certificate"
secret=$(openssl rand -hex 16)
if ! docker image inspect pando-kernel-tests >/dev/null 2>&1; then
	docker build -q -t pando-kernel-tests "${root}/tests/kernel" >/dev/null || fail "the kernel test image"
fi
MINIO_ROOT_USER=pando-tests MINIO_ROOT_PASSWORD=${secret} docker run -d --name pando-systemd-minio \
	--network host -e MINIO_ROOT_USER -e MINIO_ROOT_PASSWORD -v "${W}/certs":/certs:ro -v "${W}/minio":/data \
	--entrypoint /usr/local/bin/minio pando-kernel-tests \
	server /data --certs-dir /certs --address 127.0.0.1:9100 --console-address 127.0.0.1:9101 >/dev/null ||
	fail "MinIO did not start"
(
	umask 077
	printf 'user = "pando-tests:%s"\n' "${secret}" >"${W}/curl.conf"
)
made=
for _ in $(seq 100); do
	if curl -fsS -K "${W}/curl.conf" --cacert "${W}/certs/public.crt" --aws-sigv4 "aws:amz:us-east-1:s3" \
		-X PUT https://127.0.0.1:9100/pando >/dev/null 2>&1; then
		made=1
		break
	fi
	sleep 0.2
done
[[ -n ${made} ]] || fail "MinIO made no bucket"

arch=$(uname -m)
for v in 0.0.0-test.1 0.0.0-test.2; do
	"${root}/packaging/package.sh" "${v}" "${arch}" "${PANDO}" "${W}/rel" >/dev/null || fail "package ${v}"
done
(cd "${W}/rel" && sha256sum -- *.tar.gz >SHA256SUMS) || fail "SHA256SUMS"

"${root}/packaging/install.sh" --from "${W}/rel" 0.0.0-test.1 >>"${LOG}" 2>&1 || fail "install: install.sh failed"
link=$(readlink /usr/local/bin/pando)
[[ ${link} == /usr/local/lib/pando/0.0.0-test.1/pando ]] || fail "install: /usr/local/bin/pando is ${link}"
(
	umask 077
	cat >/etc/pando/pando.env <<EOF
S3_BUCKET=pando
S3_ENDPOINT=https://127.0.0.1:9100
S3_REGION=us-east-1
S3_ACCESS_KEY_ID=pando-tests
S3_SECRET_ACCESS_KEY=${secret}
SSL_CERT_FILE=${W}/certs/public.crt
PANDO_ARGS="--cache ${W}/cache"
EOF
	cat >/etc/pando/attach/sd.env <<EOF
PANDO_BRANCH=sd
PANDO_DEVICE=${DEV}
PANDO_ATTACH_ARGS="--state ${W}/state --record false"
EOF
)
store pando init sd $((1 << 30)) >>"${LOG}" 2>&1 || fail "init failed"
head -c $((64 << 20)) /dev/urandom >"${W}/src"

systemctl start "${U}" || fail "install: systemctl start failed"
active install
first=${pid}
mkfs.ext4 -q "${DEV}" || fail "install: mkfs.ext4 failed"
mount "${DEV}" "${MNT}" || fail "install: mount failed"
files 8 || fail "install: a write failed"
check install

t0=$(date '+%Y-%m-%d %H:%M:%S')
files 48 &
writer=$!
sleep 1
"${root}/packaging/install.sh" --from "${W}/rel" --restart 0.0.0-test.2 >>"${LOG}" 2>&1 ||
	fail "upgrade: install.sh --restart failed"
active upgrade
[[ ${pid} != "${first}" ]] || fail "upgrade: the daemon was not restarted"
exe=$(readlink "/proc/${pid}/exe")
[[ ${exe} == /usr/local/lib/pando/0.0.0-test.2/pando ]] || fail "upgrade: the daemon runs ${exe}"
journalctl -u "${U}" --since "${t0}" --no-pager >"${W}/journal" 2>&1
grep -q "drained sd" "${W}/journal" || fail "upgrade: the old daemon did not drain"
wait "${writer}" || fail "upgrade: a write failed during the restart"
writer=
check upgrade

second=${pid}
restarts=$(show NRestarts)
kill -KILL "${second}"
for _ in $(seq 120); do
	now=$(show NRestarts)
	[[ ${now} != "${restarts}" ]] && break
	sleep 0.5
done
active crash
[[ ${pid} != "${second}" ]] || fail "crash: systemd did not restart the daemon"
files 56 || fail "crash: a write failed after the restart"
check crash

systemctl stop "${U}" || fail "stop: systemctl stop failed"
result=$(show Result)
[[ ${result} == success ]] || fail "stop: the drain ended with ${result}"
[[ -b ${DEV} ]] || fail "stop: the device is gone"
systemctl start "${U}" || fail "stop: systemctl start failed"
active stop
files 64 || fail "stop: a write failed after the start"
check stop

umount "${MNT}" || fail "detach: umount failed"
store pando detach --device "${DEV}" >>"${LOG}" 2>&1 || fail "detach: pando detach failed"
for _ in $(seq 120); do
	state=$(show ActiveState)
	[[ ${state} == inactive ]] && break
	sleep 0.5
done
sleep 3
state=$(show ActiveState)
result=$(show Result)
[[ ${state} == inactive ]] || fail "detach: the unit is ${state}"
[[ ${result} == success ]] || fail "detach: the unit ended with ${result}"
say "detach: the unit stopped with status 0 and was not restarted"

rm -rf "${W}/cache" "${W}/state"
systemctl start "${U}" || fail "store: systemctl start failed"
active store
mount "${DEV}" "${MNT}" || fail "store: mount failed"
check store
umount "${MNT}" || fail "store: umount failed"
store pando detach --device "${DEV}" >>"${LOG}" 2>&1 || fail "store: detach failed"

[[ $(systemctl is-enabled pando-gc.timer 2>/dev/null || true) == disabled ]] || fail "gc: the timer is not installed and off"
t0=$(date '+%Y-%m-%d %H:%M:%S')
systemctl start pando-gc.service || fail "gc: the unit failed: $(journalctl -u pando-gc.service --since "${t0}" --no-pager -o cat | tail -1 || true)"
report=$(journalctl -u pando-gc.service --since "${t0}" --no-pager -o cat | grep "live segments" || true)
[[ -n ${report} ]] || fail "gc: no report in the journal"
[[ ${report} != *", deleted"* ]] || fail "gc: the report-only run deleted: ${report}"
say "gc: ${report}"
say "RESULT pass"
