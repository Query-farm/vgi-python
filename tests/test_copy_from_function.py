# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Worker-side unit tests for CopyFromFunction and the example_lines format."""

from __future__ import annotations

import tempfile

import pyarrow as pa
import pytest

from vgi._test_fixtures.copy_from import ExampleLinesCopyFromArgs, ExampleLinesCopyFromFunction
from vgi._test_fixtures.worker import ExampleCatalog
from vgi.arguments import Arguments
from vgi.invocation import FunctionType
from vgi.protocol import BindRequest, CopyFromContext


class _CollectOut:
    """Minimal OutputCollector stand-in for read()."""

    def __init__(self) -> None:
        self.batches: list[pa.RecordBatch] = []

    def emit(self, batch: pa.RecordBatch, **_kwargs: object) -> None:
        self.batches.append(batch)

    def finish(self) -> None:  # pragma: no cover - read() never calls finish itself
        pass


def _write(text: str) -> str:
    """Write ``text`` to a throwaway file and return its path."""
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fp:
        fp.write(text)
        return fp.name


EXPECTED = pa.schema([("a", pa.int64()), ("b", pa.string())])


def test_on_bind_binds_to_expected_schema() -> None:
    """on_bind binds output to the COPY target schema."""
    cf = CopyFromContext(format="example_lines", file_path="/x", expected_schema=EXPECTED)
    br = BindRequest(
        function_name="example_lines_copy_reader",
        arguments=Arguments(named={"null_string": pa.scalar("NA")}),
        function_type=FunctionType.TABLE,
        copy_from=cf,
    )
    resp = ExampleLinesCopyFromFunction.bind(br)
    assert resp.output_schema.equals(EXPECTED)


def test_on_bind_without_copy_from_context_raises() -> None:
    """on_bind rejects a non-COPY invocation."""
    br = BindRequest(
        function_name="example_lines_copy_reader",
        arguments=Arguments(named={"null_string": pa.scalar("NA")}),
        function_type=FunctionType.TABLE,
    )
    with pytest.raises(ValueError, match="COPY FROM format reader"):
        ExampleLinesCopyFromFunction.bind(br)


def test_read_parses_and_coerces_with_null_string() -> None:
    """read() parses, null-maps, and casts to the target schema."""
    path = _write("1,foo\n2,NA\n3,baz\n")
    out = _CollectOut()
    ExampleLinesCopyFromFunction.read(
        path=path,
        options=ExampleLinesCopyFromArgs(null_string="NA"),
        expected_schema=EXPECTED,
        params=None,
        out=out,
    )
    table = pa.Table.from_batches(out.batches)
    assert table.schema.equals(EXPECTED)
    assert table.to_pydict() == {"a": [1, 2, 3], "b": ["foo", None, "baz"]}


def test_read_custom_delimiter_and_skip_rows() -> None:
    """read() honors delimiter and skip_rows options."""
    path = _write("# header\n1|a\n2|b\n")
    out = _CollectOut()
    ExampleLinesCopyFromFunction.read(
        path=path,
        options=ExampleLinesCopyFromArgs(null_string="NA", delimiter="|", skip_rows=1),
        expected_schema=EXPECTED,
        params=None,
        out=out,
    )
    assert pa.Table.from_batches(out.batches).to_pydict() == {"a": [1, 2], "b": ["a", "b"]}


def test_read_on_error_fail_vs_skip() -> None:
    """on_error 'fail' raises; 'skip' drops the bad row."""
    path = _write("1,a\nBADROW\n3,c\n")
    with pytest.raises(ValueError, match="example_lines: row has"):
        ExampleLinesCopyFromFunction.read(
            path=path,
            options=ExampleLinesCopyFromArgs(null_string="NA"),  # on_error defaults to "fail"
            expected_schema=EXPECTED,
            params=None,
            out=_CollectOut(),
        )

    out = _CollectOut()
    ExampleLinesCopyFromFunction.read(
        path=path,
        options=ExampleLinesCopyFromArgs(null_string="NA", on_error="skip"),
        expected_schema=EXPECTED,
        params=None,
        out=out,
    )
    assert pa.Table.from_batches(out.batches).num_rows == 2


def test_catalog_advertises_copy_format() -> None:
    """The example catalog advertises the example_lines format."""
    formats = ExampleCatalog().copy_from_formats(attach_opaque_data=b"", transaction_opaque_data=None)
    by_name = {f.format_name: f for f in formats}
    assert "example_lines" in by_name
    fmt = by_name["example_lines"]
    assert fmt.handler == "example_lines_copy_reader"
    assert fmt.direction == "from"
    assert fmt.comment == "Toy delimited-text reader for tests"
    assert fmt.tags.get("category") == "copy_from"
    opt_schema = pa.ipc.read_schema(pa.py_buffer(fmt.options))
    assert set(opt_schema.names) == {"delimiter", "null_string", "skip_rows", "on_error"}
    assert opt_schema.field("null_string").metadata[b"vgi_doc"] == b"Token parsed as SQL NULL"


def test_bind_request_copy_from_wire_roundtrip() -> None:
    """copy_from survives a BindRequest wire round-trip."""
    cf = CopyFromContext(format="example_lines", file_path="/p", expected_schema=EXPECTED)
    br = BindRequest(
        function_name="h",
        arguments=Arguments(named={"null_string": pa.scalar("NA")}),
        function_type=FunctionType.TABLE,
        copy_from=cf,
    )
    restored = BindRequest.deserialize_from_bytes(br.serialize_to_bytes())
    assert restored.copy_from is not None
    assert restored.copy_from.format == "example_lines"
    assert restored.copy_from.file_path == "/p"
    assert restored.copy_from.expected_schema.equals(EXPECTED)
