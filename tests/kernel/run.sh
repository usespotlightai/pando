#!/usr/bin/env bash
# Entry point of the kernel test image: starts MinIO as the store, over TLS with a certificate
# made for this run, then runs each test named in the arguments (fio-verify, crash, discard, xfstests, log-writes).
# Exits non-zero when one fails. Runs in a mount namespace of its own, so a mount a test leaves
# never holds a device open in the container's first namespace.
set -uo pipefail
if [ -z "${PRIVATE_MOUNTS:-}" ]; then
	PRIVATE_MOUNTS=1 exec unshare --mount --propagation private "$0" "$@"
fi
here=$(dirname "$(readlink -f "$0")")
R=${R:-/results}; mkdir -p "$R"
certs=/data/minio-certs; rm -rf /data/minio "$certs"; mkdir -p /data/minio "$certs"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 -subj /CN=localhost \
	-addext "subjectAltName=IP:127.0.0.1" -addext "basicConstraints=critical,CA:FALSE" \
	-keyout "$certs/private.key" -out "$certs/public.crt" 2>/dev/null
S3_SECRET_ACCESS_KEY=$(openssl rand -hex 16)
export S3_ACCESS_KEY_ID=pando-tests S3_SECRET_ACCESS_KEY
MINIO_ROOT_USER=$S3_ACCESS_KEY_ID MINIO_ROOT_PASSWORD=$S3_SECRET_ACCESS_KEY \
	minio server /data/minio --certs-dir "$certs" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 > "$R/minio.log" 2>&1 &
export SSL_CERT_FILE=$certs/public.crt S3_ENDPOINT=https://127.0.0.1:9000 S3_BUCKET=pando S3_REGION=us-east-1
RUN=$(date +%s); export RUN
made=
for _ in $(seq 100); do
	curl -fsS --cacert "$SSL_CERT_FILE" --aws-sigv4 "aws:amz:$S3_REGION:s3" --user "$S3_ACCESS_KEY_ID:$S3_SECRET_ACCESS_KEY" \
		-X PUT "$S3_ENDPOINT/$S3_BUCKET" > /dev/null 2>&1 && { made=1; break; }
	sleep 0.2
done
[ -n "$made" ] || { echo "could not create the bucket"; tail -5 "$R/minio.log"; exit 1; }
status=0
for t in "$@"; do
	echo "== $t"
	R=$R "$here/$t.sh" || status=1
done
exit $status
