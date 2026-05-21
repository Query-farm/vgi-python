# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance coverage for ``vgi/test/sql/integration/protocol_version/``.

Mirrors the C++ integration test ``protocol_version/version_mismatch.test``:
the C++ extension drives a worker that advertises an incompatible
``protocol_version`` and asserts the dispatch-boundary mismatch surfaces as an
``IOException``. Here we drive the same fixture worker
(``vgi-fixture-bad-protocol-worker``) through the Python ``Client`` and assert
the equivalent ``ClientError``.

The framework-level enforcement rule (exact major+minor match, directional
message, both transports) is exhaustively unit-tested in
``vgi-rpc/tests/test_protocol_version.py``. This file is the VGI-level proof
that the version travels on real VGI requests and that a mismatch is fatal —
not a silent no-op.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi._test_fixtures.bad_protocol import BAD_PROTOCOL_VERSION
from vgi.arguments import Arguments
from vgi.client.client import Client, ClientError
from vgi.protocol import VgiProtocol

# The version the real Python client / C++ extension declares and sends.
CLIENT_PROTOCOL_VERSION = VgiProtocol.protocol_version


def test_client_protocol_version_is_canonical() -> None:
    """Sanity-check the constants this test reasons about haven't drifted apart."""
    assert CLIENT_PROTOCOL_VERSION == "1.0.0"
    # The fixture must be a genuine mismatch in the major/minor that the
    # framework compares; a patch-only difference would (correctly) pass.
    assert BAD_PROTOCOL_VERSION.split(".")[:2] != CLIENT_PROTOCOL_VERSION.split(".")[:2]


def test_mismatched_worker_protocol_version_raises() -> None:
    """A worker enforcing 99.0.0 must reject the 1.0.0 client at dispatch."""
    with Client("vgi-fixture-bad-protocol-worker") as client, pytest.raises(ClientError) as exc_info:
        list(
            client.table_function(
                function_name="sequence",
                arguments=Arguments(positional=(pa.scalar(5),)),
            )
        )

    message = str(exc_info.value)
    # The mismatch is reported, not swallowed, and names both versions.
    assert "protocol_version mismatch" in message
    assert CLIENT_PROTOCOL_VERSION in message
    assert BAD_PROTOCOL_VERSION in message
    # Directional hint: the client (1.0.0) is older than the worker (99.0.0).
    assert "upgrade the VGI extension/client" in message
