#!/usr/bin/env bash
# Runs the repository checks. On a host it re-runs itself in the tools image (tools/Dockerfile).
# In CI (CI=true) it runs the same tools directly, installed by tools/install.sh in ~/.pando-tools.
#   tools/check.sh             the pull request checks: lint, supply and test
#   tools/check.sh lint        formatting, clippy, rustdoc, unused dependencies, feature
#                              combinations, TOML, spelling, shell, workflows, Python, Dockerfiles
#                              and Terraform
#   tools/check.sh supply      cargo-deny and cargo-vet
#   tools/check.sh test        the workspace tests under nextest, then the doctests
#   tools/check.sh asan        the pando-host and pando-linux tests under AddressSanitizer
#   tools/check.sh fuzz [S]    every fuzz target for S seconds (default 60)
#   tools/check.sh coverage    line coverage of the workspace tests, in target/coverage
#   tools/check.sh mutants [ARGS]  cargo-mutants, with ARGS passed through
#   tools/check.sh release     a release build of pando with its dependency list embedded
# Any single check by name also works, such as tools/check.sh clippy shellcheck.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)

if [[ -n ${CI:-} ]]; then
	PATH=${HOME}/.pando-tools/bin:${PATH}
elif [[ -z ${PANDO_TOOLS:-} ]]; then
	image=$("$root/tools/image.sh")
	tty=
	[[ -t 1 ]] && tty=-t
	exec docker run --rm $tty -e PANDO_TOOLS=1 -e CARGO_TARGET_DIR=/target -e GITHUB_TOKEN \
		-v pando-cargo:/usr/local/cargo/registry -v pando-target:/target \
		-v "$root":/w -w /w "$image" tools/check.sh "$@"
fi

cd "$root"
nightly=$(cat tools/nightly)
target_dir=${CARGO_TARGET_DIR:-$root/target}
host_triple=$(rustc -vV | sed -n 's/^host: //p')

found=()
files() { # find patterns...; sets found
	local out
	out=$(find_files "$@")
	mapfile -t found <<<"${out}"
}

find_files() { # find patterns...
	find . \( -path ./target -o -path ./fuzz/target -o -path ./.git -o -path ./.claude -o -path ./tools/golden \
		-o -name node_modules -o -name .terraform \) -prune -o -type f \( "$@" \) -print | sort
}

run() {
	echo "::group::$*" >&2
	"$@"
	echo "::endgroup::" >&2
}

check_fmt() { run cargo fmt --all -- --check; }
check_clippy() { run cargo clippy --workspace --all-targets --all-features --locked -- -D warnings; }
check_doc() {
	RUSTDOCFLAGS="-D warnings -A rustdoc::private-intra-doc-links" run cargo doc --workspace --no-deps --all-features --document-private-items --locked
}
check_shear() { run cargo shear; }
check_features() {
	run cargo hack clippy --workspace --each-feature --all-targets --locked -- -D warnings
}
check_taplo() {
	run taplo fmt --check
	run taplo lint
}
check_typos() { run typos; }
check_shellcheck() {
	files -name '*.sh'
	run shellcheck --external-sources "${found[@]}"
}
check_shfmt() {
	files -name '*.sh'
	run shfmt --diff "${found[@]}"
}
check_actionlint() { run actionlint; }
check_zizmor() {
	if [[ -n ${GITHUB_TOKEN:-} ]]; then
		GH_TOKEN=$GITHUB_TOKEN run zizmor --persona pedantic .github
	else
		run zizmor --persona pedantic --offline .github
	fi
}
check_ruff() {
	run ruff check
	run ruff format --check
}
check_hadolint() {
	files -name Dockerfile
	run hadolint "${found[@]}"
}
check_terraform() {
	local d
	run terraform fmt -check -recursive -diff bench/infra
	for d in bench/infra bench/infra/bootstrap; do
		run terraform -chdir="$d" init -backend=false -input=false -lockfile=readonly -no-color >/dev/null
		run terraform -chdir="$d" validate -no-color
	done
	run tflint --chdir=bench/infra --init --config="${root}/.tflint.hcl"
	run tflint --chdir=bench/infra --recursive --config="$root/.tflint.hcl"
}

check_deny() { run cargo deny --all-features --locked check; }
check_vet() { run cargo vet --locked; }

check_test() {
	run cargo nextest run --workspace --all-features --locked --profile ci
	run cargo test --workspace --all-features --locked --doc
}

check_asan() {
	RUSTFLAGS="-Zsanitizer=address" RUSTDOCFLAGS="-Zsanitizer=address" \
		ASAN_OPTIONS=detect_leaks=1:abort_on_error=1 \
		run cargo "+$nightly" test --locked --target "$host_triple" -p pando-host -p pando-linux --lib
}

check_fuzz() { # seconds
	local secs=${1:-60} t
	for t in $(cd fuzz && cargo "+$nightly" fuzz list); do
		mkdir -p "fuzz/corpus/$t" "fuzz/seeds/$t"
		run cargo "+$nightly" fuzz run --fuzz-dir fuzz "$t" "fuzz/corpus/$t" "fuzz/seeds/$t" -- \
			-max_total_time="$secs"
	done
}

check_coverage() {
	run cargo llvm-cov nextest --workspace --all-features --locked --profile ci \
		--lcov --output-path "$target_dir/coverage/lcov.info"
	run cargo llvm-cov report --summary-only
}

check_release() { run cargo auditable build --release --locked -p pando; }

check_mutants() { run cargo mutants --workspace --all-features "$@"; }

check_lint() {
	check_fmt
	check_clippy
	check_doc
	check_shear
	check_features
	check_taplo
	check_typos
	check_shellcheck
	check_shfmt
	check_actionlint
	check_zizmor
	check_ruff
	check_hadolint
	check_terraform
}
check_supply() {
	check_deny
	check_vet
}

if [[ $# -eq 0 ]]; then
	set -- lint supply test
fi
case $1 in
fuzz | mutants)
	check=$1
	shift
	"check_${check}" "$@"
	exit
	;;
*) ;;
esac
for check in "$@"; do
	case $check in
	fmt | clippy | doc | shear | features | taplo | typos | shellcheck | shfmt | actionlint | zizmor | ruff | \
		hadolint | terraform | deny | vet | test | asan | coverage | release | lint | supply)
		"check_$check"
		;;
	*)
		echo "unknown check $check" >&2
		exit 2
		;;
	esac
done
