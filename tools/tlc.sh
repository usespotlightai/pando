#!/usr/bin/env bash
# Model-checks the TLA+ specs in specs/ with TLC, and checks traces of the engine against them.
# On a host it re-runs itself in a pinned Java container; in CI (CI=true) it runs the runner's
# Java. tla2tools.jar is checked against the SHA-256 recorded here.
#   tools/tlc.sh                    every model in specs/*.cfg, the ones a pull request checks
#   tools/tlc.sh nightly            every model in specs/nightly/*.cfg
#   tools/tlc.sh path/M.cfg ARGS    one model, with ARGS passed to TLC
#   tools/tlc.sh traces DIR         every DIR/*/TraceData.tla against specs/trace/WriteLogTrace.tla
# A model M.cfg or M-variant.cfg in any directory checks the spec specs/M.tla. Paths are relative
# to the repository root, and DIR must be inside it; pando-dst writes DIR with PANDO_TLA_TRACES
# (see specs/README.md).
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)

version=1.7.4
sha256=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
image=eclipse-temurin:21-jre-noble@sha256:b4f9d2a0bc273417cf5219b7b832bbf0493ed500b7b57d5001d9f83374659060

sum() {
	if command -v sha256sum >/dev/null; then
		sha256sum "$1" | cut -d' ' -f1
	else
		shasum -a 256 "$1" | cut -d' ' -f1
	fi
}

if [[ -n ${PANDO_TLA_JAR:-} ]]; then
	jar=${PANDO_TLA_JAR}
else
	cache=${XDG_CACHE_HOME:-${HOME}/.cache}/pando-tla
	jar=${cache}/tla2tools-${version}.jar
	have=
	if [[ -f ${jar} ]]; then
		have=$(sum "${jar}")
	fi
	if [[ ${have} != "${sha256}" ]]; then
		mkdir -p "${cache}"
		tmp=$(mktemp "${cache}/download.XXXXXX")
		curl -fsSL -o "${tmp}" "https://github.com/tlaplus/tlaplus/releases/download/v${version}/tla2tools.jar"
		got=$(sum "${tmp}")
		if [[ ${got} != "${sha256}" ]]; then
			rm -f "${tmp}"
			echo "tla2tools.jar ${version} has SHA-256 ${got}, not ${sha256}" >&2
			exit 1
		fi
		mv "${tmp}" "${jar}"
	fi
	if [[ -z ${CI:-} ]]; then
		tty=()
		[[ -t 1 ]] && tty=(-t)
		exec docker run --rm "${tty[@]}" -e PANDO_TLA_JAR=/opt/tla2tools.jar \
			-v "${jar}":/opt/tla2tools.jar:ro -v "${root}":/w:ro -w /w "${image}" tools/tlc.sh "$@"
	fi
fi

# Each run has its own temporary directory, since TLC unpacks its standard modules there.
tlc() { # java-options... -- tlc-args...
	local opts=() tmp status=0
	while [[ $1 != -- ]]; do
		opts+=("$1")
		shift
	done
	shift
	tmp=$(mktemp -d)
	mkdir "${tmp}/java" "${tmp}/states"
	java -XX:+UseParallelGC -XX:MaxRAMPercentage=75 "-Djava.io.tmpdir=${tmp}/java" "${opts[@]}" \
		-cp "${jar}" tlc2.TLC -deadlock -cleanup -metadir "${tmp}/states" "$@" || status=$?
	rm -rf "${tmp}"
	return "${status}"
}

model() { # cfg [args...]
	local dir cfg name
	dir=$(dirname "$1")
	dir=$(cd "${dir}" && pwd)
	cfg=${dir}/${1##*/}
	shift
	name=$(basename "${cfg}" .cfg)
	echo "::group::TLC ${cfg#"${root}"/}" >&2
	(cd "${root}/specs" && tlc -- -workers auto -config "${cfg}" "$@" "${name%%-*}.tla")
	echo "::endgroup::" >&2
}

# A trace is accepted when TLC reports NotAccepted violated (exit status 12): it found a
# behaviour of the spec that matches every event.
trace() { # TraceData.tla; prints accepted, or why the trace is rejected; changes directory
	local t=$1 dir out status
	dir=$(dirname "${t}")
	cd "${dir}"
	set +e
	out=$(tlc -Xmx1g "-DTLA-Library=${root}/specs:${PWD}" -- -workers 1 \
		-config "${root}/specs/trace/WriteLogTrace.cfg" "${root}/specs/trace/WriteLogTrace.tla" 2>&1)
	status=$?
	set -e
	if [[ ${status} -eq 12 ]]; then
		echo accepted
	else
		echo "rejected: ${t} (TLC exit ${status})"
		grep -E '"matched"|Error|error|Exception' <<<"${out}" | tail -n 8
	fi
}

traces() { # dir
	local dir=$1 t results cpus running n=0 accepted rejected
	results=$(mktemp -d)
	cpus=$(nproc)
	for t in "${dir}"/*/TraceData.tla; do
		[[ -f ${t} ]] || continue
		running=$(jobs -rp | wc -l)
		while [[ ${running} -ge ${cpus} ]]; do
			wait -n
			running=$(jobs -rp | wc -l)
		done
		n=$((n + 1))
		(trace "${t}" >"${results}/${n}") &
	done
	wait
	accepted=$(cat "${results}"/* 2>/dev/null | grep -cx accepted || true)
	rejected=$((n - accepted))
	cat "${results}"/* 2>/dev/null | grep -vx accepted >&2 || true
	rm -rf "${results}"
	echo "traces: ${accepted} accepted, ${rejected} rejected" >&2
	[[ ${accepted} -gt 0 && ${rejected} -eq 0 ]]
}

case ${1:-} in
"")
	for cfg in "${root}"/specs/*.cfg; do
		model "${cfg}"
	done
	;;
nightly)
	for cfg in "${root}"/specs/nightly/*.cfg; do
		model "${cfg}"
	done
	;;
traces)
	traces "${2:?usage: tlc.sh traces DIR}"
	;;
*)
	model "$@"
	;;
esac
