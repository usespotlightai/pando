# The image tools/cargo.sh and tools/check.sh run in: the pinned Rust toolchain, the pinned
# nightly for fuzzing and the sanitizers, and every check tool from tools/install.sh.
# Build with tools/ as the context.
FROM rust:1.100.0-slim-trixie@sha256:6f14b2b4b278ec0d50aa1d69541c304ce0f95e0f6268487aa9924af1c2e9aebe
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN apt-get update -qq \
	&& apt-get install -y -qq --no-install-recommends \
		ca-certificates curl git xz-utils unzip pkg-config libssl-dev build-essential \
	&& rm -rf /var/lib/apt/lists/*
RUN rustup toolchain install 1.100 --profile minimal --component clippy,rustfmt,llvm-tools \
	&& rustup default 1.100
COPY install.sh nightly /opt/pando-tools/
RUN /opt/pando-tools/install.sh /usr/local \
	&& rm -rf /usr/local/cargo/registry /usr/local/cargo/git
ENV CARGO_TERM_COLOR=always
