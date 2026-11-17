#!/usr/bin/env bash
# Prints the name of the tools image for the current tools/Dockerfile, building it first if
# this machine does not have it.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
tag=pando-tools:$(cat "$here/Dockerfile" "$here/install.sh" "$here/nightly" | sha256sum | cut -c1-16)
if ! docker image inspect "$tag" >/dev/null 2>&1; then
	docker build -q -t "$tag" "$here" >&2
fi
echo "$tag"
