# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for the time-travel AT clause carried on ``BindRequest``.

The framework threads the per-scan ``AT (TIMESTAMP|VERSION ...)`` clause onto the
``BindRequest``, which is embedded in every ``InitRequest`` as ``bind_call`` — so a
function reads it at init via ``params.init_call.bind_call.at_value`` (or the
``ProcessParams.at_value`` / ``BindParams.at_value`` accessors). The end-to-end
behaviour is covered by the C++ sqllogictest
``test/sql/integration/table/time_travel_pushdown.test``; these tests pin the
Python protocol contract (round-trip + additive wire compatibility + accessors).
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.ipc as ipc

from vgi.arguments import Arguments
from vgi.protocol import BindRequest, FunctionType, InitRequest
from vgi.table_function import BindParams, ProcessParams


def _bind_request(at_unit: str | None = None, at_value: str | None = None) -> BindRequest:
    return BindRequest(
        function_name="f",
        arguments=Arguments(positional=()),
        function_type=FunctionType.TABLE,
        at_unit=at_unit,
        at_value=at_value,
    )


def test_at_fields_are_in_the_wire_schema() -> None:
    """at_unit/at_value appear in the derived Arrow wire schema."""
    names = [f.name for f in BindRequest.ARROW_SCHEMA]
    assert "at_unit" in names
    assert "at_value" in names


def test_round_trips_at_clause() -> None:
    """A populated AT clause survives serialize/deserialize."""
    br = _bind_request("VERSION", "2")
    rt = BindRequest.deserialize_from_bytes(br.serialize_to_bytes())
    assert rt.at_unit == "VERSION"
    assert rt.at_value == "2"


def test_defaults_to_none_without_at_clause() -> None:
    """Absent AT clause stays None across the round-trip."""
    br = _bind_request()
    assert br.at_unit is None and br.at_value is None
    rt = BindRequest.deserialize_from_bytes(br.serialize_to_bytes())
    assert rt.at_unit is None and rt.at_value is None


def test_backward_compatible_missing_columns_deserialize_to_none() -> None:
    """Missing at_* columns (an older extension) must still deserialize, to None."""
    full = ipc.open_stream(_bind_request().serialize_to_bytes()).read_next_batch()
    old = full.select([n for n in full.schema.names if n not in ("at_unit", "at_value")])
    assert "at_unit" not in old.schema.names
    rt = BindRequest.deserialize_from_batch(old)
    assert rt.at_unit is None and rt.at_value is None


def test_bind_params_accessor() -> None:
    """BindParams.at_* reads through bind_call."""
    params = BindParams(
        args=None,
        bind_call=_bind_request("VERSION", "1"),
        settings={},
        secrets=None,  # type: ignore[arg-type]
    )
    assert params.at_unit == "VERSION"
    assert params.at_value == "1"


def test_process_params_accessor_reads_through_init_call() -> None:
    """ProcessParams.at_* reads through init_call.bind_call."""
    init = InitRequest(
        bind_call=_bind_request("TIMESTAMP", "2026-03-05 00:00:00"),
        output_schema=pa.schema([pa.field("x", pa.int64())]),
    )
    params = ProcessParams(
        args=None,
        init_call=init,
        init_response=None,
        output_schema=pa.schema([pa.field("x", pa.int64())]),
        settings={},
        secrets={},
        storage=None,  # type: ignore[arg-type]
    )
    assert params.at_unit == "TIMESTAMP"
    assert params.at_value == "2026-03-05 00:00:00"


def test_process_params_accessor_none_when_no_init_call() -> None:
    """ProcessParams.at_* is None when there is no init_call."""
    params = ProcessParams(
        args=None,
        init_call=None,
        init_response=None,
        output_schema=pa.schema([]),
        settings={},
        secrets={},
        storage=None,  # type: ignore[arg-type]
    )
    assert params.at_unit is None
    assert params.at_value is None
