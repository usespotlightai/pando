#!/usr/bin/env bash
# Drives the AgentCore probe that this directory's Terraform deploys. Needs docker with buildx,
# the AWS CLI v2 and terraform; run it from any directory.
#   probe.sh push                 build the arm64 image from this checkout, push it, print its digest
#   probe.sh invoke [SESSION] [PAYLOAD]
#                                 run the checks in a session (a new one unless SESSION is given),
#                                 PAYLOAD as JSON (default {}: every check); the answer goes to
#                                 results/SESSION.json
#   probe.sh stop SESSION         end the session, as AgentCore does at the idle timeout
#   probe.sh logs [SINCE]         the probe's log lines since SINCE (default 30m)
#   probe.sh signals SESSION      stop the session, wait, then print its signal and last heartbeat lines
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
tf() { terraform -chdir="$here" output -raw "$1"; }

new_session() {
	local now hex
	now=$(date +%s)
	hex=$(od -An -N12 -tx1 /dev/urandom)
	echo "pando-probe-$now-${hex//[$' \n']/}"
}

push() {
	local repo region meta digest
	repo=$(tf repository)
	region=$(tf region)
	aws ecr get-login-password --region "$region" | docker login --username AWS --password-stdin "${repo%%/*}"
	meta=$(mktemp)
	docker buildx build --platform linux/arm64 --provenance=false --metadata-file "$meta" \
		-f "$here/Dockerfile" -t "$repo:latest" --push "$root"
	digest=$(grep -o '"containerimage.digest": *"sha256:[0-9a-f]*"' "$meta" | grep -o 'sha256:[0-9a-f]*')
	rm -f "$meta"
	echo "image_digest = \"$digest\""
}

invoke() {
	local session=${1:-$(new_session)} payload=${2:-'{}'} arn region
	arn=$(tf runtime_arn)
	region=$(tf region)
	mkdir -p "$here/results"
	echo "session $session" >&2
	aws bedrock-agentcore invoke-agent-runtime --region "$region" --agent-runtime-arn "$arn" \
		--runtime-session-id "$session" --content-type application/json --accept application/json \
		--payload "$payload" --cli-binary-format raw-in-base64-out --cli-read-timeout 900 \
		"$here/results/$session.json" >/dev/null
	echo "$here/results/$session.json"
}

stop() {
	local session=${1:?usage: probe.sh stop SESSION} arn region
	arn=$(tf runtime_arn)
	region=$(tf region)
	aws bedrock-agentcore stop-runtime-session --region "$region" --agent-runtime-arn "$arn" \
		--runtime-session-id "$session" >/dev/null
	date -u +"stopped $session at %Y-%m-%dT%H:%M:%SZ"
}

logs() {
	local group region
	group=$(tf log_group)
	region=$(tf region)
	aws logs tail "$group" --region "$region" --since "${1:-30m}" --format short
}

signals() {
	stop "$@"
	sleep 60
	local lines
	lines=$(logs 15m)
	grep -E '"event": "(signal|started|first healthy ping|clock jump)"' <<<"$lines" || true
	grep '"event": "heartbeat"' <<<"$lines" | tail -5
}

cmd=${1:-}
shift || true
case $cmd in
push | invoke | stop | logs | signals) "$cmd" "$@" ;;
*)
	sed -n '2,13p' "$0" >&2
	exit 2
	;;
esac
