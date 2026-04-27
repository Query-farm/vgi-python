"""End-to-end tests that prove ``Client.from_http`` works against the example worker.

These are the canonical smoke tests for non-DuckDB use — a TypeScript port
that reads this file should understand exactly what its equivalent needs to
do: connect, list catalogs, attach, list schemas, invoke a scalar function,
invoke a table function.

Other conformance tests use the parametrized ``client_transport`` fixture
to run the same probe against subprocess + HTTP. This file is HTTP-only
and makes the HTTP code paths a first-class, reviewable slice.
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.catalog import SchemaObjectType

pytest.importorskip("vgi_rpc.http")


@pytest.fixture(scope="module")
def http_example_base_url() -> Iterator[str]:
    """Spawn ``vgi-fixture-http`` once per module and yield its base URL."""
    from contextlib import ExitStack

    from tests._http_fixtures import free_port, run_example_http_server, wait_for_http_server

    stack = ExitStack()
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    stack.enter_context(run_example_http_server(port=port))
    wait_for_http_server(base_url)
    try:
        yield base_url
    finally:
        stack.close()


def test_catalogs_over_http(http_example_base_url: str) -> None:
    """``Client.catalogs()`` over HTTP — the TS catalog-browsing use case."""
    from vgi.client.client import Client

    with Client.from_http(http_example_base_url) as client:
        names = [c.name for c in client.catalogs()]

    assert "example" in names, f"expected 'example' catalog over HTTP; got {names!r}"


def test_attach_and_list_schemas_over_http(http_example_base_url: str) -> None:
    """Attach to a catalog and list its schemas over HTTP."""
    from vgi.client.client import Client

    with Client.from_http(http_example_base_url) as client:
        attach = client.catalog_attach(
            name="example",
            options={},
            data_version_spec=None,
            implementation_version=None,
        )
        schema_names = [s.name for s in client.schemas(attach_id=attach.attach_id)]

    assert "main" in schema_names


def test_schema_contents_functions_over_http(http_example_base_url: str) -> None:
    """List functions in the example/main schema over HTTP — drives bind/RPC catalog dispatch."""
    from vgi.client.client import Client

    with Client.from_http(http_example_base_url) as client:
        attach = client.catalog_attach(
            name="example",
            options={},
            data_version_spec=None,
            implementation_version=None,
        )
        scalars = client.schema_contents(
            attach_id=attach.attach_id,
            name="main",
            type=SchemaObjectType.SCALAR_FUNCTION,
        )

    names = {fi.name for fi in scalars}
    # These are stable registrations in vgi/examples/worker.py.
    assert {"double", "add_values"}.issubset(names), names


def test_scalar_function_over_http(http_example_base_url: str) -> None:
    """Invoke ``double`` over HTTP — proves scalar bind/init/exchange works."""
    from vgi.client.client import Client

    schema = pa.schema([("x", pa.int64())])
    batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=schema)

    with Client.from_http(http_example_base_url) as client:
        out = list(
            client.scalar_function(
                function_name="double",
                arguments=Arguments(positional=(pa.scalar("x"),)),
                input=iter([batch]),
            )
        )

    result = [r for b in out for r in b.column("result").to_pylist()]
    assert result == [2, 4, 6]


def test_table_function_over_http(http_example_base_url: str) -> None:
    """Invoke ``sequence`` over HTTP — proves table function bind/init works."""
    from vgi.client.client import Client

    with Client.from_http(http_example_base_url) as client:
        out = list(
            client.table_function(
                function_name="sequence",
                arguments=Arguments(positional=(pa.scalar(5),)),
            )
        )

    assert sum(b.num_rows for b in out) == 5


def test_server_capabilities_over_http(http_example_base_url: str) -> None:
    """``Client.server_capabilities`` returns the advertised HTTP caps."""
    from vgi.client.client import Client

    with Client.from_http(http_example_base_url) as client:
        caps = client.server_capabilities()

    # Default example server advertises no upload-URL support and no limits —
    # we assert the shape of the response (used later by Phase 4).
    assert hasattr(caps, "upload_url_support")
    assert hasattr(caps, "max_request_bytes")
    assert hasattr(caps, "max_upload_bytes")


def test_server_capabilities_requires_http() -> None:
    """``server_capabilities`` is HTTP-only; subprocess clients must reject it."""
    from vgi.client.client import Client, ClientError

    with Client("vgi-fixture-worker") as client, pytest.raises(ClientError, match="HTTP"):
        client.server_capabilities()


# ---------------------------------------------------------------------------
# Bearer-token auth round-trip
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def http_example_bearer_base_url() -> Iterator[str]:
    """Spawn ``vgi-fixture-http`` with a static bearer token required."""
    from contextlib import ExitStack

    from tests._http_fixtures import free_port, run_example_http_server, wait_for_http_server

    stack = ExitStack()
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    stack.enter_context(
        run_example_http_server(
            port=port,
            env={"VGI_BEARER_TOKENS": "ts-secret=ts-client"},
        )
    )
    # Readiness probe uses the no-auth /capabilities endpoint — that path is
    # always reachable regardless of whether auth is required for RPC calls.
    wait_for_http_server(base_url)
    try:
        yield base_url
    finally:
        stack.close()


def test_bearer_token_accepted(http_example_bearer_base_url: str) -> None:
    """Client with the right bearer token reaches ``catalogs()``."""
    from vgi.client.client import Client

    with Client.from_http(http_example_bearer_base_url, bearer_token="ts-secret") as client:
        names = [c.name for c in client.catalogs()]

    assert "example" in names


def test_bearer_token_required(http_example_bearer_base_url: str) -> None:
    """Client with no token is rejected by the auth-required server.

    Proves bearer wiring is real: a missing token surfaces as an error
    rather than silently succeeding.
    """
    from vgi.client.client import Client

    with Client.from_http(http_example_bearer_base_url) as client, pytest.raises(Exception):  # noqa: B017
        client.catalogs()
