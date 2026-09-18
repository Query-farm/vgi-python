# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The subprocess transport, pooled and direct, kept covered under the launch test transport.

The suite reaches its fixture workers through the launcher by default (see
``TEST_TRANSPORT`` in ``conftest.py``), so without these the default subprocess
path — the one ``Client(server_path)`` users get — would only run under
``VGI_TEST_CLIENT_TRANSPORT=subprocess``.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from tests.conftest import SUBPROCESS_FIXTURE_WORKER
from vgi.arguments import Arguments
from vgi.client.client import Client, _default_pool


@pytest.mark.parametrize("pool", [_default_pool, None], ids=["pooled", "direct"])
def test_table_function_round_trip(pool: object) -> None:
    """A table function streams its rows over a subprocess worker."""
    with Client(SUBPROCESS_FIXTURE_WORKER, pool=pool) as client:  # type: ignore[arg-type]
        assert client._transport == "subprocess"
        batches = list(
            client.table_function(
                function_name="sequence",
                schema_path=["main"],
                arguments=Arguments(positional=(pa.scalar(4),)),
            )
        )

    assert pa.Table.from_batches(batches).column("n").to_pylist() == [0, 1, 2, 3]


@pytest.mark.parametrize("pool", [_default_pool, None], ids=["pooled", "direct"])
def test_catalog_listing(pool: object) -> None:
    """Catalog discovery goes through the catalog pool for a subprocess client."""
    client = Client(SUBPROCESS_FIXTURE_WORKER, pool=pool)  # type: ignore[arg-type]
    assert any(c.name == "example" for c in client.catalogs())
