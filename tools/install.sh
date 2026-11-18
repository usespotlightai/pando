#!/usr/bin/env bash
# Installs the pinned check tools into a prefix, for tools/Dockerfile and for CI.
#   tools/install.sh PREFIX
# Cargo tools are built from source with their lock files. Other tools are release binaries,
# checked against the SHA-256 recorded here. This file is the one place their versions live.
set -euo pipefail
prefix=${1:?usage: install.sh PREFIX}
mkdir -p "$prefix/bin"

NIGHTLY=$(cat "$(dirname "$0")/nightly")

cargo_tools=(
	cargo-auditable@0.7.7
	cargo-deny@0.20.2
	cargo-fuzz@0.13.2
	cargo-hack@0.6.46
	cargo-llvm-cov@0.9.1
	cargo-mutants@27.1.0
	cargo-nextest@0.9.151
	cargo-shear@1.14.0
	cargo-vet@0.10.3
	taplo-cli@0.10.0
)

machine=$(uname -m)
case ${machine} in
x86_64 | amd64) arch=x86_64 ;;
aarch64 | arm64) arch=aarch64 ;;
*)
	echo "unsupported machine ${machine}" >&2
	exit 2
	;;
esac

# name version url-for-x86_64 sha256-x86_64 url-for-aarch64 sha256-aarch64
binaries=(
	"shellcheck 0.11.0
	https://github.com/koalaman/shellcheck/releases/download/v0.11.0/shellcheck-v0.11.0.linux.x86_64.tar.xz
	8c3be12b05d5c177a04c29e3c78ce89ac86f1595681cab149b65b97c4e227198
	https://github.com/koalaman/shellcheck/releases/download/v0.11.0/shellcheck-v0.11.0.linux.aarch64.tar.xz
	12b331c1d2db6b9eb13cfca64306b1b157a86eb69db83023e261eaa7e7c14588"
	"shfmt 3.14.1
	https://github.com/mvdan/sh/releases/download/v3.14.1/shfmt_v3.14.1_linux_amd64
	76e77641faa025814b77f153b29796b8e6fa2fca03e0c76a691608b86c7ea7bf
	https://github.com/mvdan/sh/releases/download/v3.14.1/shfmt_v3.14.1_linux_arm64
	5f2db09dae91fca848f7adbdd014632e921a383863a2ad7e0450ad3aba0c6489"
	"actionlint 1.7.12
	https://github.com/rhysd/actionlint/releases/download/v1.7.12/actionlint_1.7.12_linux_amd64.tar.gz
	8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8
	https://github.com/rhysd/actionlint/releases/download/v1.7.12/actionlint_1.7.12_linux_arm64.tar.gz
	325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6"
	"zizmor 1.32.0
	https://github.com/zizmorcore/zizmor/releases/download/v1.32.0/zizmor-x86_64-unknown-linux-gnu.tar.gz
	bb110cc765eca9cd7efb2e284a956be5974c60027b0970f16397d86208a9957b
	https://github.com/zizmorcore/zizmor/releases/download/v1.32.0/zizmor-aarch64-unknown-linux-gnu.tar.gz
	cb152b207f815116d7fa1c1ee4614f61578803351395052908095e79adfd9fd0"
	"typos 1.52.1
	https://github.com/crate-ci/typos/releases/download/v1.52.1/typos-v1.52.1-x86_64-unknown-linux-musl.tar.gz
	c034dbec8797cfa0c4548b3726c1eebbafcf703b8a17f15a961b037788eebcdd
	https://github.com/crate-ci/typos/releases/download/v1.52.1/typos-v1.52.1-aarch64-unknown-linux-musl.tar.gz
	3c82c3e97aafdd686fbb3f8c7b82bca3a870d21a0bcadd41babd37e84cc2f2df"
	"ruff 0.17.4
	https://github.com/astral-sh/ruff/releases/download/0.17.4/ruff-x86_64-unknown-linux-gnu.tar.gz
	3ab2b34aeacdbc1ea5c529cdbda3e05792633d587228123fd09f3b5a0facb27a
	https://github.com/astral-sh/ruff/releases/download/0.17.4/ruff-aarch64-unknown-linux-gnu.tar.gz
	b939676db4532f748dc38c41361a29260f18b3e24a787d46d9278e449c25c2cf"
	"hadolint 2.15.1
	https://github.com/hadolint/hadolint/releases/download/v2.15.1/hadolint-linux-x86_64
	c7187db94eeeeca956519a6af171adc31453941a1e777961f6e680f697c8c507
	https://github.com/hadolint/hadolint/releases/download/v2.15.1/hadolint-linux-arm64
	f6198ef8090f404dbb771abfee086eb8c48ac177f30da7fd3510aca35b344b5d"
	"terraform 1.17.0
	https://releases.hashicorp.com/terraform/1.17.0/terraform_1.17.0_linux_amd64.zip
	c168cab7c637a8769fdb90089ddd19de701bc6ff8aca64dc2e2185fe46f59f67
	https://releases.hashicorp.com/terraform/1.17.0/terraform_1.17.0_linux_arm64.zip
	f32478134ffc6d710eec474bbd316652cdddc0f4e336973a3745e8d3a3309cdf"
	"tflint 0.66.0
	https://github.com/terraform-linters/tflint/releases/download/v0.66.0/tflint_linux_amd64.zip
	22be11cadc25cd02401e94c0ccc91aa63467debb8485c2a755d98f4c0a9d1e2e
	https://github.com/terraform-linters/tflint/releases/download/v0.66.0/tflint_linux_arm64.zip
	1105a0a5ddee7cdac7957a3475571a946b839192ea1bc0c722c6c4686001067e"
)

install_binary() { # name version url sha256
	local name=$1 version=$2 url=$3 sum=$4 stamp=$prefix/versions/$1
	local installed
	installed=$(cat "$stamp" 2>/dev/null || true)
	if [[ ${installed} == "${version}" ]]; then
		return
	fi
	local work
	work=$(mktemp -d)
	curl -fsSL --retry 3 -o "$work/download" "$url"
	echo "$sum  $work/download" | sha256sum -c --quiet -
	case $url in
	*.tar.xz) tar -xJf "$work/download" -C "$work" ;;
	*.tar.gz) tar -xzf "$work/download" -C "$work" ;;
	*.zip) unzip -q "$work/download" -d "$work" ;;
	*) cp "$work/download" "$work/$name" ;;
	esac
	local bin
	bin=$(find "$work" -type f -name "$name" | head -n 1)
	install -m 0755 "$bin" "$prefix/bin/$name"
	rm -rf "$work"
	mkdir -p "$prefix/versions"
	echo "$version" >"$stamp"
}

for entry in "${binaries[@]}"; do
	read -r name version url_x86 sum_x86 url_arm sum_arm <<<"${entry//[$'\n\t']/ }"
	if [[ ${arch} == x86_64 ]]; then
		install_binary "$name" "$version" "$url_x86" "$sum_x86"
	else
		install_binary "$name" "$version" "$url_arm" "$sum_arm"
	fi
done

for tool in "${cargo_tools[@]}"; do
	cargo install --quiet --locked --root "$prefix" "$tool"
done

if [[ ${PANDO_INSTALL_NIGHTLY:-1} == 1 ]]; then
	rustup toolchain install "$NIGHTLY" --profile minimal --component rust-src,llvm-tools
fi
