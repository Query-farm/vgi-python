# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Cross-SDK conformance, rule 5 on an OS-owned transport: no secret in an unsealed value.

Over stdio (and a unix socket) a worker may skip sealing, so whatever its
catalog puts in ``attach_opaque_data`` reaches the client, and the DuckDB
extension, in the clear. A ``secret=True`` attach option must not be among it
(``docs/protocol/vgi-opaque-data-sealing.md``, rule 5).

Environment:

``VGI_SDK_STDIO_WORKER``
    The worker command, run over stdio. **Unset, the module skips.** Split with
    ``shlex``, like ``Client(server_path=...)``.
``VGI_SDK_SECRET_CATALOG``
    As for the HTTP group: the catalog to attach. Default ``ticket_probe``,
    then any catalog with a secret option whose required options are strings.

Run::

    VGI_SDK_STDIO_WORKER=./vgi-example-worker-go uv run pytest tests/sdk_conformance -q
"""

from __future__ import annotations

import os

import pytest

from vgi.catalog import AttachOpaqueData
from vgi.client import Client

from .test_opaque_sealing import _assert_canary_absent, _attach, find_secret_catalog

_WORKER = os.environ.get("VGI_SDK_STDIO_WORKER", "").strip()
pytestmark = pytest.mark.skipif(not _WORKER, reason="set VGI_SDK_STDIO_WORKER to an SDK fixture worker command")


def test_secret_option_never_appears_in_the_unsealed_value() -> None:
    """Over stdio, the secret option is absent from attach and transaction values."""
    with Client(_WORKER, pool=None) as client:
        secret, reason = find_secret_catalog(client.catalogs())
        if secret is None:
            pytest.skip(reason)
        name, options, canary = secret
        result = _attach(client, name, options)
        value = bytes(result.attach_opaque_data or b"")
        assert value, f"catalog {name!r} returned no attach_opaque_data"
        _assert_canary_absent(value, canary, "attach_opaque_data (stdio)")
        if result.supports_transactions:
            tx = client.catalog_transaction_begin(attach_opaque_data=AttachOpaqueData(value))
            if tx:
                _assert_canary_absent(bytes(tx), canary, "transaction_opaque_data (stdio)")
