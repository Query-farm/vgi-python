# syntax=docker/dockerfile:1
#
# vgi-fixtures — example/test VGI worker images for the VGI DuckDB extension.
#
# Each worker speaks vgi-rpc over stdin/stdout; the extension's oci:// container
# transport runs `docker run -i ... IMAGE stdio` and pipes the worker like a
# subprocess (full fidelity — same semantics as the local subprocess transport,
# unlike HTTP). Which worker runs is selected by VGI_FIXTURE_WORKER, baked per
# image tag via the build arg below (overridable at runtime with `-e`).
#
# Built from the SAME wheels published to PyPI in the release (the release
# workflow copies them into ./dist), so the image matches the released
# vgi-fixtures exactly — no separate source path to drift.

FROM python:3.13-slim

# vgi-python (core) + vgi-fixtures (worker entry points + the vgi/_test_fixtures
# sources the fixtures wheel force-includes). Installing both local wheels; the
# extras the fixtures require ([duckdb], [test-fixtures-writable]) resolve from
# PyPI. Kept as one heavy layer so the per-worker ARG below doesn't bust it.
COPY dist/*.whl /tmp/wheels/
RUN pip install --no-cache-dir /tmp/wheels/*.whl && rm -rf /tmp/wheels

# Worker selection — placed AFTER the install so the (expensive) layer above is
# shared across every per-worker tag; only this cheap tail rebuilds per worker.
ARG VGI_FIXTURE_WORKER=vgi-fixture-worker
ENV VGI_FIXTURE_WORKER=${VGI_FIXTURE_WORKER}

COPY packages/vgi-fixtures/docker/docker-entrypoint.sh /usr/local/bin/vgi-fixtures-entrypoint
RUN chmod +x /usr/local/bin/vgi-fixtures-entrypoint

LABEL org.opencontainers.image.source="https://github.com/Query-farm/vgi-python" \
      org.opencontainers.image.description="Example/test VGI worker (stdio) for the VGI DuckDB extension" \
      org.opencontainers.image.licenses="MIT"

# The extension passes the transport mode as argv[1]; stdio is the default + only
# mode this image implements.
ENTRYPOINT ["/usr/local/bin/vgi-fixtures-entrypoint"]
CMD ["stdio"]
