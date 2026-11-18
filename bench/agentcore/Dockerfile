# The AgentCore V2 probe: probe.py on port 8080, with the tools its checks run and a pando built
# from this checkout for the end-to-end check. AgentCore runs arm64 images only. Build with the
# repository root as the context (bench/agentcore/push.sh does):
#   docker build --platform linux/arm64 -f bench/agentcore/Dockerfile .
FROM rust:1.100.0-slim-trixie@sha256:6f14b2b4b278ec0d50aa1d69541c304ce0f95e0f6268487aa9924af1c2e9aebe AS pando
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p pando && cp target/release/pando /pando

FROM debian:trixie@sha256:9c6e381421d0f48d1b8133e573f33c185ee16cbf505eda9c4a8a4791b25ea8a6
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
	python3 util-linux e2fsprogs fuse3 bindfs dmsetup kmod libcap2-bin procps ca-certificates \
	nfs-common rpcbind nfs-ganesha nfs-ganesha-vfs >/dev/null \
	&& rm -rf /var/lib/apt/lists/*
COPY --from=pando /pando /usr/local/bin/pando
COPY bench/agentcore/probe.py /probe/probe.py
# hadolint ignore=DL3002
USER 0
EXPOSE 8080
ENTRYPOINT ["python3", "-u", "/probe/probe.py"]
