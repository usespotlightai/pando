#!/usr/bin/env bash
# Packs a release archive of pando: the binary and the systemd unit, in a directory named after
# the archive. The same inputs give the same bytes.
#   packaging/package.sh VERSION ARCH BINARY OUTDIR
# Writes OUTDIR/pando-VERSION-ARCH-linux.tar.gz. ARCH is x86_64 or aarch64 and must be the
# machine BINARY is built for.
set -euo pipefail
usage="usage: package.sh VERSION ARCH BINARY OUTDIR"
version=${1:?${usage}}
arch=${2:?${usage}}
binary=${3:?${usage}}
out=${4:?${usage}}
here=$(cd "$(dirname "$0")" && pwd)

case ${version} in
'' | .* | *[!0-9A-Za-z.+-]*)
	echo "version ${version}: only letters, digits, '.', '+' and '-'" >&2
	exit 2
	;;
*) ;;
esac
case ${arch} in
x86_64) machine='x86-64' ;;
aarch64) machine='ARM aarch64' ;;
*)
	echo "arch ${arch}: x86_64 or aarch64" >&2
	exit 2
	;;
esac
kind=$(file -b "${binary}")
if [[ ${kind} != *"ELF 64-bit"*"${machine}"* ]]; then
	echo "${binary} is not a ${arch} Linux binary: ${kind}" >&2
	exit 1
fi

name=pando-${version}-${arch}-linux
stage=$(mktemp -d)
trap 'rm -rf "${stage}"' EXIT
mkdir "${stage}/${name}"
install -m 0755 "${binary}" "${stage}/${name}/pando"
install -m 0644 "${here}/systemd/pando-attach@.service" "${stage}/${name}/"
mkdir -p "${out}"
tar -C "${stage}" --sort=name --mtime=@"${SOURCE_DATE_EPOCH:-0}" --owner=0 --group=0 --numeric-owner \
	-cf - "${name}" | gzip -n -9 >"${out}/${name}.tar.gz"
echo "${out}/${name}.tar.gz"
