#!/usr/bin/env bash
# Installs a pando release on this host and makes it the current one, or makes a release
# installed before current again. Run as root.
#   install.sh [--from DIR] [--restart] VERSION
# Fetches pando-VERSION-ARCH-linux.tar.gz and SHA256SUMS from the GitHub release vVERSION
# (PANDO_RELEASE_URL replaces https://github.com/usespotlightai/pando/releases/download), or
# takes them from DIR, and checks the archive against SHA256SUMS before unpacking it.
# Each version lives in /usr/local/lib/pando/VERSION. /usr/local/bin/pando is a link to the
# current one, replaced in one rename: a running daemon keeps the binary it started with, and
# every process started after the rename runs the new one. Installs the release's systemd
# units, pando-attach@.service and pando-gc.service with pando-gc.timer, which stays off until
# enabled, and creates /etc/pando/attach, mode 0700.
# --restart then restarts each running pando-attach@ unit, one at a time: its daemon drains and
# the new binary takes its device over. It stops at the first unit that fails to come back, and
# fails when systemd is not running.
set -euo pipefail
usage="usage: install.sh [--from DIR] [--restart] VERSION"
from=
restart=
while [[ $# -gt 0 ]]; do
	case $1 in
	--from)
		from=${2:?--from needs a directory}
		shift 2
		;;
	--restart)
		restart=1
		shift
		;;
	-*)
		echo "${usage}" >&2
		exit 2
		;;
	*) break ;;
	esac
done
version=${1:?${usage}}
case ${version} in
'' | .* | *[!0-9A-Za-z.+-]*)
	echo "version ${version}: only letters, digits, '.', '+' and '-'" >&2
	exit 2
	;;
*) ;;
esac
uid=$(id -u)
if [[ ${uid} != 0 ]]; then
	echo "install.sh must run as root" >&2
	exit 1
fi
machine=$(uname -m)
case ${machine} in
x86_64 | amd64) arch=x86_64 ;;
aarch64 | arm64) arch=aarch64 ;;
*)
	echo "pando has no release for ${machine}" >&2
	exit 2
	;;
esac

name=pando-${version}-${arch}-linux
lib=/usr/local/lib/pando
dir=${lib}/${version}
bin=/usr/local/bin/pando

if [[ ! -x ${dir}/pando ]]; then
	work=$(mktemp -d)
	trap 'rm -rf "${work}"' EXIT
	if [[ -n ${from} ]]; then
		cp "${from}/${name}.tar.gz" "${from}/SHA256SUMS" "${work}/"
	else
		url=${PANDO_RELEASE_URL:-https://github.com/usespotlightai/pando/releases/download}/v${version}
		for f in "${name}.tar.gz" SHA256SUMS; do
			curl -fsSL --proto '=https' --tlsv1.2 --retry 3 -o "${work}/${f}" "${url}/${f}"
		done
	fi
	sum=$(awk -v f="${name}.tar.gz" '$2 == f || $2 == "*" f' "${work}/SHA256SUMS")
	if [[ -z ${sum} ]]; then
		echo "SHA256SUMS has no line for ${name}.tar.gz" >&2
		exit 1
	fi
	(cd "${work}" && sha256sum -c --quiet --strict <<<"${sum}")
	mkdir -p "${lib}"
	rm -rf "${dir}.new"
	mkdir "${dir}.new"
	tar -xzf "${work}/${name}.tar.gz" -C "${dir}.new" --strip-components=1 --no-same-owner
	mv -T "${dir}.new" "${dir}"
fi

running=$("${dir}/pando" --version)
install -d -m 0700 /etc/pando /etc/pando/attach
for f in "${dir}"/*.service "${dir}"/*.timer; do
	[[ -e ${f} ]] || continue
	unit=/etc/systemd/system/${f##*/}
	install -m 0644 "${f}" "${unit}.new"
	mv -f "${unit}.new" "${unit}"
done
ln -sfn "${dir}/pando" "${bin}.new"
mv -Tf "${bin}.new" "${bin}"
echo "${bin} is ${running}"

if [[ ! -d /run/systemd/system ]]; then
	if [[ -n ${restart} ]]; then
		echo "--restart needs systemd, which is not running here: no daemon was restarted" >&2
		exit 1
	fi
	exit 0
fi
systemctl daemon-reload
[[ -n ${restart} ]] || exit 0
units=$(systemctl list-units --type=service --state=active --plain --no-legend 'pando-attach@*' | awk '{print $1}')
for u in ${units}; do
	echo "restarting ${u}"
	if ! systemctl restart "${u}"; then
		echo "${u} did not come back; the units after it still run the old binary" >&2
		systemctl --no-pager status "${u}" >&2 || true
		exit 1
	fi
done
