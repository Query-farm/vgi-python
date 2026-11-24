# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance coverage for ``vgi/test/sql/integration/protocol_version/``.

Mirrors the C++ integration test ``protocol_version/version_mismatch.test``:
the C++ extension drives a worker that advertises an incompatible
``protocol_version`` and asserts the dispatch-boundary mismatch surfaces as an
``IOException``. Here we drive the same fixture worker
(``vgi-fixture-bad-protocol-worker``) through the Python ``Client`` and assert
the equivalent ``ClientError``.

The framework-level enforcement rule (major.minor inside the worker's advertised
window -- exactly 3.4-3.4 in protocol 3.4 -- refused as
``protocol_version_unsupported`` with both windows named; PROTOCOL_VERSIONS_V1) is exhaustively unit-tested in
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
PREVIOUS_MINOR_PROTOCOL_VERSION = "3.3.0"


def _mm(version: str) -> str:
    """``MAJOR.MINOR`` of a semver: what refusals name (patch is not negotiated)."""
    return ".".join(version.split(".")[:2])


def test_client_protocol_version_is_canonical() -> None:
    """Sanity-check the constants this test reasons about haven't drifted apart."""
    assert CLIENT_PROTOCOL_VERSION == "3.4.0"
    assert VgiProtocol.protocol_name == "vgi.v3"
    # The fixture must be a genuine mismatch in the major/minor that the
    # framework compares; a patch-only difference would (correctly) pass.
    assert BAD_PROTOCOL_VERSION.split(".")[:2] != CLIENT_PROTOCOL_VERSION.split(".")[:2]


def test_mismatched_worker_protocol_version_raises() -> None:
    """A worker enforcing 99.0.0 must reject the 3.4.0 client at dispatch."""
    with Client("vgi-fixture-bad-protocol-worker") as client, pytest.raises(ClientError) as exc_info:
        list(
            client.table_function(
                function_name="sequence",
                schema_path=["main"],
                arguments=Arguments(positional=(pa.scalar(5),)),
            )
        )

    message = str(exc_info.value)
    # The refusal is reported, not swallowed, and names both sides' windows.
    assert "protocol_version_unsupported" in message
    assert f"client speaks vgi.v3 {_mm(CLIENT_PROTOCOL_VERSION)}" in message
    bad = _mm(BAD_PROTOCOL_VERSION)
    assert f"this server supports vgi.v3 {bad}-{bad}" in message
    # Directional hint: the client (3.4) is older than the worker (99.0).
    assert "client is too old" in message
    assert "Upgrade the client" in message


def test_previous_minor_worker_is_rejected_instead_of_silently_backfilled() -> None:
    """A 3.1 client must not interpret a 3.0 response with a different schema."""
    command = [
        "env",
        f"VGI_BAD_PROTOCOL_VERSION={PREVIOUS_MINOR_PROTOCOL_VERSION}",
        "vgi-fixture-bad-protocol-worker",
    ]
    with Client(command, pool=None) as client, pytest.raises(ClientError) as exc_info:
        list(
            client.table_function(
                function_name="sequence",
                schema_path=["main"],
                arguments=Arguments(positional=(pa.scalar(5),)),
            )
        )

    message = str(exc_info.value)
    assert "protocol_version_unsupported" in message
    assert f"client speaks vgi.v3 {_mm(CLIENT_PROTOCOL_VERSION)}" in message
    prev = _mm(PREVIOUS_MINOR_PROTOCOL_VERSION)
    assert f"this server supports vgi.v3 {prev}-{prev}" in message
    assert "server is too old" in message
