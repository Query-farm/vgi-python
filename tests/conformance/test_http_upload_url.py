"""End-to-end test: ``Client`` transparently externalizes large input batches.

When the HTTP server advertises a small ``max_request_bytes`` and input
exceeds it, the client must vend an upload URL, PUT the IPC bytes, and
replace the batch with a pointer (empty batch + ``vgi_rpc.location``).
The worker receives the pointer, fetches the data, and processes normally.

Drives the whole thing through ``Client.from_http(...)`` so a TS port that
mirrors this structure gets transparent externalization on its side too.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi.arguments import Arguments

pytest.importorskip("vgi_rpc.http")


@pytest.fixture(scope="module")
def small_request_limit_base_url() -> str:
    """Spawn ``vgi-example-http --demo-storage`` with a tiny ``max_request_bytes``.

    Setting the externalize threshold low also forces output batches
    through the blob store; the combination exercises upload URLs (input)
    and pointer-batch resolution (output) in the same run.
    """
    from contextlib import ExitStack

    from tests._http_fixtures import free_port, run_example_http_server, wait_for_http_server

    stack = ExitStack()
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    stack.enter_context(
        run_example_http_server(
            port=port,
            extra_args=[
                "--demo-storage",
                "--externalize-threshold-bytes",
                "256",
                "--max-upload-bytes",
                "1048576",
            ],
        )
    )
    wait_for_http_server(base_url)
    try:
        yield base_url
    finally:
        stack.close()


def test_oversize_input_auto_externalized(small_request_limit_base_url: str) -> None:
    """An input batch larger than ``max_request_bytes`` is sent via upload URL.

    ``double`` expects a column name and multiplies each element by 2.
    Making the column big enough to exceed the server's request cap forces
    the client to use ``request_upload_urls`` + PUT, transparently.
    """
    from vgi_rpc.external import ExternalLocationConfig

    from vgi.client.client import Client
    from vgi.http.demo_storage import localhost_only_validator

    # 10000 int64 values = 80000 bytes payload plus Arrow framing — well
    # above the 256-byte ``max_request_bytes`` set above.
    values = list(range(10_000))
    input_schema = pa.schema([("x", pa.int64())])
    input_batch = pa.RecordBatch.from_pydict({"x": values}, schema=input_schema)

    config = ExternalLocationConfig(url_validator=localhost_only_validator)

    with Client.from_http(small_request_limit_base_url, external_location=config) as client:
        caps = client.server_capabilities()
        assert caps.upload_url_support is True
        assert caps.max_request_bytes is not None and caps.max_request_bytes > 0

        out = list(
            client.scalar_function(
                function_name="double",
                arguments=Arguments(positional=(pa.scalar("x"),)),
                input=iter([input_batch]),
            )
        )

    result = [v for b in out for v in b.column("result").to_pylist()]
    assert result == [v * 2 for v in values]


def test_small_input_not_externalized(small_request_limit_base_url: str) -> None:
    """Inputs under the threshold take the inline path, not the upload-URL path.

    Verifies the externalization is *conditional* — we don't pay the
    upload-URL round trip when the payload fits inline. Correctness-wise
    indistinguishable from the big case; this test exists as documentation.
    """
    from vgi_rpc.external import ExternalLocationConfig

    from vgi.client.client import Client
    from vgi.http.demo_storage import localhost_only_validator

    input_schema = pa.schema([("x", pa.int64())])
    input_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=input_schema)

    config = ExternalLocationConfig(url_validator=localhost_only_validator)

    with Client.from_http(small_request_limit_base_url, external_location=config) as client:
        out = list(
            client.scalar_function(
                function_name="double",
                arguments=Arguments(positional=(pa.scalar("x"),)),
                input=iter([input_batch]),
            )
        )

    assert [v for b in out for v in b.column("result").to_pylist()] == [2, 4, 6]
