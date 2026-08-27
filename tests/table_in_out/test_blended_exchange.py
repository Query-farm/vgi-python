# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""RPC-level exchange coverage for blended (`RowTransformFunction`) table functions.

`test_blended_metadata.py` covers `resolve_metadata()` in isolation — no worker
subprocess. This file is the first exchange-level coverage for the blended
fixtures registered in `vgi/_test_fixtures/table_in_out.py`: it drives them
through `Client.table_in_out_function(...)`'s new `parent_row_callback`, which
decodes `vgi_rpc.parent_row#b64` provenance for a 1->N / 1->0 emit (see
`vgi.protocol._decode_parent_rows`).
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from tests.conftest import make_schema
from vgi.arguments import Arguments
from vgi.client import Client
from vgi.client.client import ClientError


class TestBlendedExplodeProvenance:
    """`blended_explode`: 1->N fan-out with per-output-row parent_row provenance."""

    def test_parent_row_pairing_is_correct(self, fixture_worker: str) -> None:
        """Each output row's parent index must point at the input row that produced it.

        Asserting only the final row *set* would pass even for a naive
        "assume identity" implementation whenever fan-out counts differ
        across rows -- so this reconstructs the exact expected
        (parent_row, emitted_value) pairs in emission order and compares.
        """
        input_schema = make_schema([pa.field("n", pa.int64())])
        counts = [0, 1, 3, 2]
        batch = pa.RecordBatch.from_pydict({"n": counts}, schema=input_schema)

        parent_rows_seen: list[int] = []

        with Client(fixture_worker) as client:
            results = list(
                client.table_in_out_function(
                    function_name="blended_explode",
                    schema_name="main",
                    input=iter([batch]),
                    parent_row_callback=parent_rows_seen.extend,
                )
            )

        assert len(results) == 1
        out = results[0]
        i_values = out.column("i").to_pylist()

        assert len(i_values) == sum(counts)
        assert len(parent_rows_seen) == len(i_values)

        expected_pairs = [(row_idx, k) for row_idx, n in enumerate(counts) for k in range(n)]
        actual_pairs = list(zip(parent_rows_seen, i_values, strict=True))
        assert actual_pairs == expected_pairs

    def test_all_rows_filtered_to_empty_result(self, fixture_worker: str) -> None:
        """n=0 for every input row -> a correctly-schemed, zero-row output."""
        input_schema = make_schema([pa.field("n", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"n": [0, 0, 0]}, schema=input_schema)

        parent_rows_seen: list[list[int]] = []

        with Client(fixture_worker) as client:
            results = list(
                client.table_in_out_function(
                    function_name="blended_explode",
                    schema_name="main",
                    input=iter([batch]),
                    parent_row_callback=parent_rows_seen.append,
                )
            )

        assert len(results) == 1
        out = results[0]
        assert out.num_rows == 0
        assert out.schema.names == ["i"]
        assert parent_rows_seen == [[]]

    def test_identity_map_when_row_counts_match(self, fixture_worker: str) -> None:
        """n=1 for every row -> a 1:1 emit; the identity map is a valid decode.

        `blended_explode` never encodes `vgi_rpc.parent_row` for a 1:1 emit
        (the C++ reference assumes identity for that case too), so this
        exercises `_decode_parent_rows`'s absent-metadata branch, not just
        the explicit-array branch the other tests exercise.
        """
        input_schema = make_schema([pa.field("n", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"n": [1, 1, 1]}, schema=input_schema)

        parent_rows_seen: list[int] = []

        with Client(fixture_worker) as client:
            results = list(
                client.table_in_out_function(
                    function_name="blended_explode",
                    schema_name="main",
                    input=iter([batch]),
                    parent_row_callback=parent_rows_seen.extend,
                )
            )

        assert len(results) == 1
        assert results[0].column("i").to_pylist() == [0, 0, 0]
        assert parent_rows_seen == [0, 1, 2]


class TestHostileProvenance:
    """`hostile_provenance`: adversarial worker payloads must raise, never corrupt."""

    @pytest.mark.parametrize("mode", ["range", "length", "base64"])
    def test_malformed_provenance_raises_client_error(
        self,
        client_transport: Any,
        mode: str,
    ) -> None:
        """Each hostile mode must raise ClientError on both subprocess and HTTP.

        Symmetry matters here: an asymmetry between transports would mean one
        parse path validates and the other doesn't -- a security bug, not
        just an inconsistency.
        """
        input_schema = make_schema([pa.field("x", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=input_schema)

        with client_transport() as client, pytest.raises(ClientError):
            list(
                client.table_in_out_function(
                    function_name="hostile_provenance",
                    schema_name="main",
                    input=iter([batch]),
                    arguments=Arguments(named={"mode": pa.scalar(mode)}),
                    parent_row_callback=lambda _rows: None,
                )
            )

    def test_no_callback_no_decode_no_raise(self, fixture_worker: str) -> None:
        """Without `parent_row_callback`, malformed provenance is never even decoded.

        `decode_parent_rows` is derived from `parent_row_callback is not None`
        -- this is the backward-compat guarantee that an ordinary (non-blended)
        table-in-out caller, which never passes the callback, is completely
        unaffected by this feature existing.
        """
        input_schema = make_schema([pa.field("x", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=input_schema)

        with Client(fixture_worker) as client:
            results = list(
                client.table_in_out_function(
                    function_name="hostile_provenance",
                    schema_name="main",
                    input=iter([batch]),
                    arguments=Arguments(named={"mode": pa.scalar("range")}),
                )
            )

        assert len(results) == 1
        assert results[0].column("hv").to_pylist() == [1, 2, 3]


class TestBlendedArityAndVarargs:
    """Broader coverage once the core provenance mechanism is proven."""

    def test_geo_encode_two_arg_overload(self, fixture_worker: str) -> None:
        """`geo_encode(lat, lon)` -- the 2-arg overload, identity 1:1 map."""
        input_schema = make_schema([pa.field("latitude", pa.float64()), pa.field("longitude", pa.float64())])
        batch = pa.RecordBatch.from_pydict(
            {"latitude": [52.0, 48.5], "longitude": [13.0, 2.3]},
            schema=input_schema,
        )

        parent_rows_seen: list[int] = []

        with Client(fixture_worker) as client:
            results = list(
                client.table_in_out_function(
                    function_name="geo_encode",
                    schema_name="main",
                    input=iter([batch]),
                    arguments=Arguments(named={"precision": pa.scalar(1)}),
                    parent_row_callback=parent_rows_seen.extend,
                )
            )

        assert len(results) == 1
        assert results[0].column("geohash").to_pylist() == ["52.0:13.0", "48.5:2.3"]
        assert parent_rows_seen == [0, 1]

    def test_geo_encode_three_arg_overload(self, fixture_worker: str) -> None:
        """`geo_encode(lat, lon, alt)` -- same Meta.name, resolved by input arity."""
        input_schema = make_schema(
            [
                pa.field("latitude", pa.float64()),
                pa.field("longitude", pa.float64()),
                pa.field("altitude", pa.float64()),
            ]
        )
        batch = pa.RecordBatch.from_pydict(
            {"latitude": [52.0], "longitude": [13.0], "altitude": [100.0]},
            schema=input_schema,
        )

        with Client(fixture_worker) as client:
            results = list(
                client.table_in_out_function(
                    function_name="geo_encode",
                    schema_name="main",
                    input=iter([batch]),
                    arguments=Arguments(named={"precision": pa.scalar(0)}),
                )
            )

        assert len(results) == 1
        assert results[0].column("geohash").to_pylist() == ["52.0:13.0:100.0"]

    def test_row_sum_varargs(self, fixture_worker: str) -> None:
        """`row_sum(a, b, c)` -- varargs positional input, positionally-named columns."""
        input_schema = make_schema([pa.field("col0", pa.float64()), pa.field("col1", pa.float64())])
        batch = pa.RecordBatch.from_pydict({"col0": [1.0, -2.0], "col1": [2.0, -3.0]}, schema=input_schema)

        with Client(fixture_worker) as client:
            results = list(
                client.table_in_out_function(
                    function_name="row_sum",
                    schema_name="main",
                    input=iter([batch]),
                    arguments=Arguments(named={"absolute": pa.scalar(True)}),
                )
            )

        assert len(results) == 1
        assert results[0].column("row_sum").to_pylist() == [3.0, 5.0]
