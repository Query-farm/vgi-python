#!/usr/bin/env bash

set -euo pipefail

TAILNET_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$TAILNET_ROOT/tests/tailnet/compose.yaml"
COMPOSE=(docker compose -f "$COMPOSE_FILE")

require_variable() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "required Tailnet test variable is unset: $name" >&2
    exit 2
  fi
}

for name in \
  TS_OAUTH_CLIENT_ID \
  TS_OAUTH_SECRET \
  TAILNET_ISSUER \
  TAILNET_EXPECTED_CAPABILITY \
  TAILNET_EXPECTED_CLIENT_TAG; do
  require_variable "$name"
done

export TAILNET_SERVER_HOSTNAME="${TAILNET_SERVER_HOSTNAME:-vgi-python-ci-server-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"
export TAILNET_CLIENT_HOSTNAME="${TAILNET_CLIENT_HOSTNAME:-vgi-python-ci-client-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"
export TAILNET_SOCKS_HOSTNAME="${TAILNET_SOCKS_HOSTNAME:-vgi-python-ci-socks-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"

cleanup() {
  local status="$?"
  if [[ "$status" -ne 0 ]]; then
    "${COMPOSE[@]}" ps >&2 || true
    "${COMPOSE[@]}" logs --no-color --tail=200 \
      tailscale-server tailscale-client tailscale-socks worker-direct worker-http worker-http-direct >&2 || true
  fi
  "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

docker build \
  --tag "${VGI_TAILNET_IMAGE:-vgi-python-tailnet:local}" \
  --file "$TAILNET_ROOT/tests/tailnet/Dockerfile" \
  "$TAILNET_ROOT"

"${COMPOSE[@]}" up --detach --wait --wait-timeout 120 \
  tailscale-server tailscale-client tailscale-socks worker-direct worker-http worker-http-direct

SERVER_DNS="$("${COMPOSE[@]}" exec -T tailscale-server \
  tailscale --socket=/var/run/tailscale/tailscaled.sock status --json | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["Self"]["DNSName"].rstrip("."))')"
test -n "$SERVER_DNS"

"${COMPOSE[@]}" exec -T tailscale-server tailscale --socket=/var/run/tailscale/tailscaled.sock serve \
  --yes \
  --bg \
  --accept-app-caps="$TAILNET_EXPECTED_CAPABILITY" \
  --https=443 \
  http://127.0.0.1:18080
"${COMPOSE[@]}" exec -T tailscale-server tailscale --socket=/var/run/tailscale/tailscaled.sock cert \
  --cert-file=/tmp/vgi-tailnet-cert.pem \
  --key-file=/tmp/vgi-tailnet-key.pem \
  "$SERVER_DNS"

common_tcp=(
  --expected-issuer "$TAILNET_ISSUER"
  --expected-evidence-source localapi
  --expected-assurance local_daemon
  --expected-subject-kind tagged_node
  --expected-capability "$TAILNET_EXPECTED_CAPABILITY"
  --expected-tag "$TAILNET_EXPECTED_CLIENT_TAG"
  --expect-authenticated
)

timeout 45s "${COMPOSE[@]}" run --rm probe-direct python -m tests.tailnet.probe tcp \
  --host "$SERVER_DNS" --port 19400 "${common_tcp[@]}"

timeout 45s "${COMPOSE[@]}" run --rm probe-socks python -m tests.tailnet.probe tcp \
  --host "$SERVER_DNS" --port 19400 \
  --proxy socks5h://tailscale-socks:1055 \
  --require-local-dns-failure \
  "${common_tcp[@]}"

timeout 45s "${COMPOSE[@]}" run --rm probe-direct python -m tests.tailnet.probe http \
  --url "http://$SERVER_DNS:18081" \
  --expected-issuer "$TAILNET_ISSUER" \
  --expected-evidence-source localapi \
  --expected-assurance local_daemon \
  --expected-subject-kind tagged_node \
  --expected-capability "$TAILNET_EXPECTED_CAPABILITY" \
  --expected-tag "$TAILNET_EXPECTED_CLIENT_TAG" \
  --expect-authenticated

timeout 45s "${COMPOSE[@]}" run --rm probe-direct python -m tests.tailnet.probe http \
  --url "https://$SERVER_DNS" \
  --spoof-login attacker@example.invalid \
  --expected-issuer "$TAILNET_ISSUER" \
  --expected-evidence-source serve_proxy \
  --expected-assurance configured_proxy \
  --expected-subject-kind unknown \
  --expected-capability "$TAILNET_EXPECTED_CAPABILITY"

echo "high-level VGI Tailnet authentication passed"
