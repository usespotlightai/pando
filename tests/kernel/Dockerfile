# The kernel tests: fio, e2fsprogs, device-mapper tools, xfstests built from a pinned tag, and
# MinIO built from its last source release as the store: the tests write gigabytes, more than
# the AWS emulator keeps. Build with tests/kernel as the context.
FROM golang:1.25-trixie AS minio
ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
RUN CGO_ENABLED=0 GOBIN=/out go install github.com/minio/minio@$MINIO_COMMIT

FROM debian:trixie
ARG XFSTESTS_REF=v2026.10.15
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
	fio e2fsprogs xfsprogs dmsetup util-linux kmod procps psmisc curl ca-certificates openssl \
	build-essential autoconf automake libtool pkg-config gettext git \
	libacl1-dev libattr1-dev libaio-dev uuid-dev liburing-dev xfslibs-dev libgdbm-dev libcap-dev \
	acl attr bc dump gawk quota xfsdump libcap2-bin keyutils perl python3 file sqlite3 >/dev/null \
	&& rm -rf /var/lib/apt/lists/*
RUN for try in 1 2 3 4 5; do \
		git -c http.lowSpeedLimit=1000 -c http.lowSpeedTime=30 clone -q --depth 1 --branch $XFSTESTS_REF https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git /opt/xfstests && break; \
		rm -rf /opt/xfstests; sleep $((try * 15)); \
	done \
	&& cd /opt/xfstests && make -s -j"$(nproc)" >/dev/null 2>&1 && make -s install >/dev/null 2>&1 \
	&& test -x /var/lib/xfstests/check && test -x /var/lib/xfstests/ltp/fsstress \
	&& test -x /var/lib/xfstests/src/log-writes/replay-log
RUN useradd -m fsgqa && useradd -m fsgqa2 && useradd 123456-fsgqa && groupadd -f fsgqa
COPY --from=minio /out/minio /usr/local/bin/minio
COPY . /tests
ENTRYPOINT ["/tests/run.sh"]
