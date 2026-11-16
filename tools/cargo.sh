#!/usr/bin/env bash
# Runs cargo in a Linux container, so macOS hosts build and test the Linux crates too.
#   tools/cargo.sh test --workspace
# The target dir and the registry live in docker volumes; target/rust-out stays on the host.
# PANDO_ENV_FILE passes a file of S3_* variables to the container without printing them.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
tty=; [ -t 1 ] && tty=-t
envfile=; [ -n "${PANDO_ENV_FILE:-}" ] && envfile="--env-file $PANDO_ENV_FILE"
docker run --rm $tty $envfile -e CARGO_TARGET_DIR=/target -e PANDO_RUST_OUT -e RUST_BACKTRACE -e PANDO_DST_TRACE -e PANDO_DST_SEEDS -e PANDO_DST_FIRST \
  -v pando-cargo:/usr/local/cargo/registry -v pando-rustup:/usr/local/rustup -v pando-target:/target \
  -v "$root":/w -w /w rust:1 cargo "$@"
