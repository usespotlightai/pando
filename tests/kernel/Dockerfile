# The kernel tests: fio, e2fsprogs, device-mapper tools, xfstests built from a pinned tag with
# the fixes in xfstests-*.patch, and MinIO built from its last source release as the store: the
# tests write gigabytes, more than the AWS emulator keeps. Build with tests/kernel as the context.
FROM golang:1.25-trixie@sha256:2c4c60ef415fbfa5e90300722293bef36c5e63fae17570ce18f580af933dbd73 AS minio
ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
RUN CGO_ENABLED=0 GOBIN=/out go install github.com/minio/minio@$MINIO_COMMIT

FROM debian:trixie@sha256:9c6e381421d0f48d1b8133e573f33c185ee16cbf505eda9c4a8a4791b25ea8a6
ARG XFSTESTS_REF=v2026.10.15
ARG XFSTESTS_COMMIT=d07dca72d2455f4ba8c660ad4c008541ccccb707
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
	fio e2fsprogs xfsprogs dmsetup util-linux kmod procps psmisc curl ca-certificates openssl \
	build-essential autoconf automake libtool pkg-config gettext git \
	libacl1-dev libattr1-dev libaio-dev uuid-dev liburing-dev xfslibs-dev libgdbm-dev libcap-dev \
	acl attr bc dump gawk quota xfsdump libcap2-bin keyutils perl python3 file sqlite3 >/dev/null \
	&& rm -rf /var/lib/apt/lists/*
RUN for try in 1 2 3 4 5; do \
		for url in https://kernel.googlesource.com/pub/scm/fs/xfs/xfstests-dev \
			https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git; do \
			if git -c http.lowSpeedLimit=1000 -c http.lowSpeedTime=30 clone -q --depth 1 --branch $XFSTESTS_REF "$url" /opt/xfstests; then \
				got=$(git -C /opt/xfstests rev-parse HEAD); \
				[ "$got" = "$XFSTESTS_COMMIT" ] && break 2; \
				echo "xfstests $XFSTESTS_REF from $url is $got, want $XFSTESTS_COMMIT" >&2; \
			fi; \
			rm -rf /opt/xfstests; \
		done; \
		if [ "$try" = 5 ]; then echo "xfstests $XFSTESTS_REF: no mirror gave $XFSTESTS_COMMIT in 5 tries" >&2; exit 1; fi; \
		sleep $((try * 15)); \
	done
WORKDIR /opt/xfstests
COPY xfstests-*.patch /tmp/xfstests-patches/
RUN git apply /tmp/xfstests-patches/*.patch && rm -rf /tmp/xfstests-patches
RUN make -s -j"$(nproc)" >/dev/null 2>&1 && make -s install >/dev/null 2>&1 \
	&& test -x /var/lib/xfstests/check && test -x /var/lib/xfstests/ltp/fsstress \
	&& test -x /var/lib/xfstests/src/log-writes/replay-log
WORKDIR /
RUN useradd -m fsgqa && useradd -m fsgqa2 && useradd 123456-fsgqa && groupadd -f fsgqa
COPY --from=minio /out/minio /usr/local/bin/minio
COPY . /tests
ENTRYPOINT ["/tests/run.sh"]
