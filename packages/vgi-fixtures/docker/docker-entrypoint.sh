#!/bin/sh
# Entrypoint for the vgi-fixtures container image.
#
# The VGI DuckDB extension's container (oci://) transport invokes the image as
#   docker run -i ... IMAGE <mode>
# where <mode> is the transport it wants: "stdio" (private per-process, piped over
# stdin/stdout exactly like the subprocess transport) or "tcp"/"http" (a shared
# server). This image implements *stdio only* — the fixture workers speak vgi-rpc
# over stdin/stdout and advertise no farm.query.vgi.transports label, so the
# extension stays on per-process stdio for it.
#
# Which fixture worker runs is chosen by $VGI_FIXTURE_WORKER. It is baked per image
# tag via the Dockerfile build arg (e.g. the ":<ver>-versioned" tag bakes
# vgi-fixture-versioned-worker) and can be overridden at runtime with
#   docker run -e VGI_FIXTURE_WORKER=vgi-fixture-bad-enum-worker ...
set -eu

mode="${1:-stdio}"
worker="${VGI_FIXTURE_WORKER:-vgi-fixture-worker}"

# Only ever exec a published vgi-fixture-* console script — never an arbitrary
# command smuggled in through the env var.
case "$worker" in
	vgi-fixture-*) : ;;
	*)
		echo "vgi-fixtures: refusing to run non-fixture command '$worker'" >&2
		exit 64
		;;
esac

case "$mode" in
	stdio)
		# The worker serves vgi-rpc over stdin/stdout and runs until EOF.
		exec "$worker"
		;;
	tcp | http)
		echo "vgi-fixtures: transport '$mode' is not supported by this image (stdio only)." >&2
		echo "             It advertises no farm.query.vgi.transports label, so the extension" >&2
		echo "             uses per-process stdio; do not force connection=$mode for it." >&2
		exit 64
		;;
	*)
		echo "vgi-fixtures: unknown transport mode '$mode' (expected: stdio)" >&2
		exit 64
		;;
esac
