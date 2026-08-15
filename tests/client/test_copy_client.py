# Copyright 2025, 2026 Query Farm LLC - https://query.farm

# ruff: noqa: S101, D101, D102, D103
"""Tests for the [`Client`][] custom-COPY drivers.

``COPY ... FROM`` is a producer table function carrying a
[`CopyFromContext`][]; ``COPY ... TO`` is a buffered Sink+Combine function
carrying a [`CopyToContext`][] and no Source phase. These tests drive both
against the real ``vgi-fixture-worker`` over every client transport, plus the
catalog-level format discovery that tells a caller which handler to name.
"""

from __future__ import annotations

import pathlib
from typing import Any

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.client.client import Client, ClientError

MAIN = "main"
READER = "example_lines_copy_reader"
READER_FORMAT = "example_lines"
WRITER = "example_lines_writer"
WRITER_FORMAT = "example_lines_out"

SOURCE_SCHEMA = pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.string())])


@pytest.fixture
def client(client_transport: Any) -> Any:
    """Yield a started ``Client`` for each transport under test."""
    with client_transport() as started:
        yield started


def _rows() -> list[pa.RecordBatch]:
    return [
        pa.RecordBatch.from_pydict({"a": [1, 2], "b": ["x", None]}, schema=SOURCE_SCHEMA),
        pa.RecordBatch.from_pydict({"a": [3], "b": ["z"]}, schema=SOURCE_SCHEMA),
    ]


def _write(client: Client, dest: str, **options: Any) -> None:
    """Run the fixture writer with ``null_string`` plus whatever else is asked."""
    named = {"null_string": pa.scalar("NA")}
    named.update(options)
    client.copy_to(
        function_name=WRITER,
        schema_name=MAIN,
        format=WRITER_FORMAT,
        file_path=dest,
        input=iter(_rows()),
        input_schema=SOURCE_SCHEMA,
        arguments=Arguments(named=named),
    )


# ---------------------------------------------------------------------------
# COPY ... TO
# ---------------------------------------------------------------------------


class TestCopyTo:
    def test_writes_the_destination(self, client: Client, tmp_path: pathlib.Path) -> None:
        """Every sunk batch reaches the writer, and ``close()`` produces the file."""
        dest = str(tmp_path / "out.txt")
        _write(client, dest)
        assert pathlib.Path(dest).read_text() == "1,x\n2,NA\n3,z\n"

    def test_returns_nothing(self, client: Client, tmp_path: pathlib.Path) -> None:
        """A COPY-TO sink has no Source phase, so the driver returns None."""
        dest = str(tmp_path / "out.txt")
        assert _write(client, dest) is None

    def test_options_reach_the_writer(self, client: Client, tmp_path: pathlib.Path) -> None:
        """COPY options arrive as the function's normal named arguments."""
        dest = str(tmp_path / "out.psv")
        _write(client, dest, delimiter=pa.scalar("|"), header=pa.scalar(True))
        assert pathlib.Path(dest).read_text() == "a|b\n1|x\n2|NA\n3|z\n"

    def test_empty_input_still_closes_the_destination(self, client: Client, tmp_path: pathlib.Path) -> None:
        """An empty COPY must still produce a file — the bind carries the schema."""
        dest = str(tmp_path / "empty.txt")
        client.copy_to(
            function_name=WRITER,
            schema_name=MAIN,
            format=WRITER_FORMAT,
            file_path=dest,
            input=iter([]),
            input_schema=SOURCE_SCHEMA,
            arguments=Arguments(named={"null_string": pa.scalar("NA"), "header": pa.scalar(True)}),
        )
        assert pathlib.Path(dest).read_text() == "a,b\n"

    def test_ordered_writer_preserves_source_order(self, client: Client, tmp_path: pathlib.Path) -> None:
        """The ordered variant is single-sink by contract; one connection satisfies it."""
        dest = str(tmp_path / "ordered.txt")
        client.copy_to(
            function_name="example_lines_ordered_writer",
            schema_name=MAIN,
            format="example_lines_ordered_out",
            file_path=dest,
            input=iter(_rows()),
            input_schema=SOURCE_SCHEMA,
            arguments=Arguments(named={"null_string": pa.scalar("NA")}),
        )
        assert pathlib.Path(dest).read_text() == "1,x\n2,NA\n3,z\n"

    def test_writer_failure_propagates(self, client: Client, tmp_path: pathlib.Path) -> None:
        """A raise inside ``write()`` surfaces as a ClientError carrying the message."""
        dest = str(tmp_path / "boom.txt")
        with pytest.raises(ClientError, match="fail_on_value"):
            _write(client, dest, fail_on_value=pa.scalar("z"))

    def test_option_constraint_is_enforced_at_bind(self, client: Client, tmp_path: pathlib.Path) -> None:
        """``header_repeat`` is ``le=3``; the worker rejects 9 before any row is sunk."""
        dest = str(tmp_path / "never.txt")
        with pytest.raises(ClientError):
            _write(client, dest, header=pa.scalar(True), header_repeat=pa.scalar(9))
        assert not pathlib.Path(dest).exists()

    def test_missing_required_option_is_rejected(self, client: Client, tmp_path: pathlib.Path) -> None:
        """``null_string`` has no default, so omitting it fails the bind."""
        with pytest.raises(ClientError):
            client.copy_to(
                function_name=WRITER,
                schema_name=MAIN,
                format=WRITER_FORMAT,
                file_path=str(tmp_path / "no.txt"),
                input=iter(_rows()),
                input_schema=SOURCE_SCHEMA,
            )


# ---------------------------------------------------------------------------
# COPY ... FROM
# ---------------------------------------------------------------------------


class TestCopyFrom:
    def test_reads_into_the_expected_schema(self, client: Client, tmp_path: pathlib.Path) -> None:
        """Emitted batches match ``expected_schema`` exactly — DuckDB inserts no cast."""
        src = tmp_path / "in.txt"
        src.write_text("1,x\n2,NA\n3,z\n")
        batches = list(
            client.copy_from(
                function_name=READER,
                schema_name=MAIN,
                format=READER_FORMAT,
                file_path=str(src),
                expected_schema=SOURCE_SCHEMA,
                arguments=Arguments(named={"null_string": pa.scalar("NA")}),
            )
        )
        assert [b.schema for b in batches] == [SOURCE_SCHEMA] * len(batches)
        table = pa.Table.from_batches(batches, schema=SOURCE_SCHEMA)
        assert table.to_pydict() == {"a": [1, 2, 3], "b": ["x", None, "z"]}

    def test_options_reach_the_reader(self, client: Client, tmp_path: pathlib.Path) -> None:
        src = tmp_path / "in.psv"
        src.write_text("header line\n1|x\n2|NONE\n")
        batches = list(
            client.copy_from(
                function_name=READER,
                schema_name=MAIN,
                format=READER_FORMAT,
                file_path=str(src),
                expected_schema=SOURCE_SCHEMA,
                arguments=Arguments(
                    named={
                        "null_string": pa.scalar("NONE"),
                        "delimiter": pa.scalar("|"),
                        "skip_rows": pa.scalar(1),
                    }
                ),
            )
        )
        table = pa.Table.from_batches(batches, schema=SOURCE_SCHEMA)
        assert table.to_pydict() == {"a": [1, 2], "b": ["x", None]}

    def test_malformed_row_fails_by_default(self, client: Client, tmp_path: pathlib.Path) -> None:
        src = tmp_path / "bad.txt"
        src.write_text("1,x\n2\n")
        with pytest.raises(ClientError, match="expected 2"):
            list(
                client.copy_from(
                    function_name=READER,
                    schema_name=MAIN,
                    format=READER_FORMAT,
                    file_path=str(src),
                    expected_schema=SOURCE_SCHEMA,
                    arguments=Arguments(named={"null_string": pa.scalar("NA")}),
                )
            )

    def test_malformed_row_can_be_skipped(self, client: Client, tmp_path: pathlib.Path) -> None:
        src = tmp_path / "bad.txt"
        src.write_text("1,x\n2\n3,z\n")
        batches = list(
            client.copy_from(
                function_name=READER,
                schema_name=MAIN,
                format=READER_FORMAT,
                file_path=str(src),
                expected_schema=SOURCE_SCHEMA,
                arguments=Arguments(named={"null_string": pa.scalar("NA"), "on_error": pa.scalar("skip")}),
            )
        )
        table = pa.Table.from_batches(batches, schema=SOURCE_SCHEMA)
        assert table.to_pydict() == {"a": [1, 3], "b": ["x", "z"]}

    def test_missing_source_file_raises(self, client: Client, tmp_path: pathlib.Path) -> None:
        with pytest.raises(ClientError):
            list(
                client.copy_from(
                    function_name=READER,
                    schema_name=MAIN,
                    format=READER_FORMAT,
                    file_path=str(tmp_path / "absent.txt"),
                    expected_schema=SOURCE_SCHEMA,
                    arguments=Arguments(named={"null_string": pa.scalar("NA")}),
                )
            )


# ---------------------------------------------------------------------------
# Round trip + discovery
# ---------------------------------------------------------------------------


def test_copy_to_then_copy_from_round_trips(client: Client, tmp_path: pathlib.Path) -> None:
    """The fixture reader and writer are symmetric, so the values survive."""
    dest = str(tmp_path / "round.txt")
    _write(client, dest)
    batches = list(
        client.copy_from(
            function_name=READER,
            schema_name=MAIN,
            format=READER_FORMAT,
            file_path=dest,
            expected_schema=SOURCE_SCHEMA,
            arguments=Arguments(named={"null_string": pa.scalar("NA")}),
        )
    )
    table = pa.Table.from_batches(batches, schema=SOURCE_SCHEMA)
    assert table.to_pydict() == {"a": [1, 2, 3], "b": ["x", None, "z"]}


def test_copy_formats_lists_both_directions(client: Client) -> None:
    """Discovery is what turns a FORMAT name into the handler to invoke."""
    attached = client.catalog_attach(name="example", data_version_spec=None, implementation_version=None)
    formats = {f.format_name: f for f in client.copy_formats(attach_opaque_data=attached.attach_opaque_data)}

    assert formats[READER_FORMAT].handler == READER
    assert formats[READER_FORMAT].direction == "from"
    assert formats[WRITER_FORMAT].handler == WRITER
    assert formats[WRITER_FORMAT].direction == "to"
    assert formats["example_lines_ordered_out"].ordered is True


def test_copy_formats_advertises_option_schemas(client: Client) -> None:
    """Each format carries its option schema, so a caller can build ``Arguments``."""
    attached = client.catalog_attach(name="example", data_version_spec=None, implementation_version=None)
    formats = {f.format_name: f for f in client.copy_formats(attach_opaque_data=attached.attach_opaque_data)}

    options = pa.ipc.read_schema(pa.BufferReader(bytes(formats[READER_FORMAT].options)))
    assert set(options.names) == {"null_string", "delimiter", "skip_rows", "on_error"}


@pytest.mark.parametrize("client_transport", ["subprocess-pooled"], indirect=True)
def test_copy_to_rejects_a_non_copy_handler(client: Client, tmp_path: pathlib.Path) -> None:
    """A buffered function that is not a COPY writer is caught, not silently drained.

    Subprocess only: reaching the guard means running the buffered Source phase,
    which the HTTP stream session does not implement (a pre-existing gap in
    ``table_buffering_function``, unrelated to COPY).
    """
    with pytest.raises(ClientError):
        client.copy_to(
            function_name="echo_buffering",
            schema_name=MAIN,
            format=WRITER_FORMAT,
            file_path=str(tmp_path / "nope.txt"),
            input=iter(_rows()),
            input_schema=SOURCE_SCHEMA,
        )
