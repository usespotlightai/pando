#!/usr/bin/env bash
# Runs cargo in the tools image (tools/Dockerfile), a Linux container, so macOS hosts build and
# test the Linux crates too.
#   tools/cargo.sh test --workspace
# The target dir and the registry live in docker volumes; target/rust-out stays on the host.
# PANDO_ENV_FILE passes a file of S3_* variables to the container without printing them.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
image=$("${root}/tools/image.sh")
args=(--rm)
[[ -t 1 ]] && args+=(-t)
[[ -n ${PANDO_ENV_FILE:-} ]] && args+=(--env-file "${PANDO_ENV_FILE}")
docker run "${args[@]}" -e CARGO_TARGET_DIR=/target -e PANDO_RUST_OUT -e RUST_BACKTRACE \
	-e PANDO_DST_TRACE -e PANDO_DST_SEEDS -e PANDO_DST_BUDGET_SEEDS -e PANDO_DST_FIRST -e PANDO_TLA_TRACES \
	-v pando-cargo:/usr/local/cargo/registry -v pando-target:/target \
	-v "${root}":/w -w /w "${image}" cargo "$@"
