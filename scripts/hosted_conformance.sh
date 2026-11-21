#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Run the SDK-neutral Orchard hosted-mode conformance harness (the vgi repo's
# test/hosted/run_hosted_conformance.py, see its README) against this repo's
# hosted fixture worker, vgi._test_fixtures.orchard_hosted:OrchardHostedWorker.
#
# Usage: VGI_DIR=<vgi checkout> scripts/hosted_conformance.sh [harness flags...]
# VGI_DIR defaults to ../vgi. Extra flags go to the harness (e.g. --slow,
# --streaming-body, --only 'jwks.*'). Runs from the project env
# (`uv sync --all-extras`), which has the client, joserfc and cryptography.
#
# Must-refuse commands: stdio (Worker.main and vgi-serve without --http),
# --unix, --tcp, --iroh-raw-upstream, and --server granian (no body cap).
set -euo pipefail

cd "$(dirname "$0")/.."

VGI_DIR="${VGI_DIR:-../vgi}"
HARNESS="$VGI_DIR/test/hosted/run_hosted_conformance.py"
if [ ! -f "$HARNESS" ]; then
  echo "no harness at $HARNESS: set VGI_DIR to a vgi checkout that has test/hosted/" >&2
  exit 2
fi

PY="$(uv run --no-sync python -c 'import sys; print(sys.executable)')"
WORKER="$(printf '%q' "$PY") -m vgi.serve vgi._test_fixtures.orchard_hosted:OrchardHostedWorker"
FIXTURE="$(printf '%q' "$PY") -m vgi._test_fixtures.orchard_hosted"

exec "$PY" "$HARNESS" \
  --worker-cwd "$PWD" \
  --worker-cmd "$WORKER --http --host 127.0.0.1 --port 0 --debug" \
  --refuse-cmd "$FIXTURE" \
  --refuse-cmd "$WORKER" \
  --refuse-cmd "$WORKER --http --server granian --host 127.0.0.1 --port 0" \
  --refuse-cmd "$FIXTURE --unix /tmp/vgi-hosted-refuse-{port}.sock" \
  --refuse-cmd "$FIXTURE --tcp 127.0.0.1:{port}" \
  --refuse-cmd "$WORKER --iroh-raw-upstream 0" \
  "$@"
