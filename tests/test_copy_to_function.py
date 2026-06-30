# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Worker-side unit tests for CopyToFunction and the example_lines_out format."""

from __future__ import annotations

import tempfile
import types

import pyarrow as pa

from vgi._test_fixtures.copy_to import (
    ExampleLinesCopyToArgs,
    ExampleLinesCopyToFunction,
    SecretLinesCopyToArgs,
    SecretLinesCopyToFunction,
)
from vgi._test_fixtures.worker import ExampleCatalog
from vgi.table_function import ResolvedSecrets, SecretsAccessor

SCHEMA = pa.schema([("a", pa.int64()), ("b", pa.string())])


class _Store:
    """Minimal in-memory BoundStorage stub (append + ordered log scan)."""

    def __init__(self) -> None:
        self.log: list[tuple[int, bytes]] = []

    def state_append(self, ns: bytes, key: bytes, val: bytes) -> None:
        self.log.append((len(self.log), val))

    def state_log_scan(self, ns: bytes, key: bytes, after_id: int = -1, limit: int | None = None) -> list:
        rows = [(i, v) for (i, v) in self.log if i > after_id]
        return rows if limit is None else rows[:limit]


def _tmp_path() -> str:
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
        return fh.name


def _read(path: str) -> str:
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _params(store: _Store) -> types.SimpleNamespace:
    bind_call = types.SimpleNamespace(input_schema=SCHEMA)
    init_call = types.SimpleNamespace(bind_call=bind_call)
    return types.SimpleNamespace(storage=store, init_call=init_call, execution_id=b"x", args=None)


def test_write_then_close_round_trips_with_null_string() -> None:
    """write() buffers shards; close() concatenates them to a delimited file."""
    store = _Store()
    params = _params(store)
    opts = ExampleLinesCopyToArgs(null_string="NA")
    out_name = _tmp_path()

    ExampleLinesCopyToFunction.write(
        batch=pa.record_batch({"a": [1, 2], "b": ["foo", None]}, schema=SCHEMA),
        options=opts,
        file_path=out_name,
        params=params,
    )
    ExampleLinesCopyToFunction.write(
        batch=pa.record_batch({"a": [3], "b": ["baz"]}, schema=SCHEMA),
        options=opts,
        file_path=out_name,
        params=params,
    )
    n = ExampleLinesCopyToFunction.close(options=opts, file_path=out_name, params=params)
    assert n == 3
    assert _read(out_name) == "1,foo\n2,NA\n3,baz\n"


def test_close_honors_delimiter_and_header() -> None:
    """Non-default delimiter + header row are applied."""
    store = _Store()
    params = _params(store)
    opts = ExampleLinesCopyToArgs(null_string="NA", delimiter="|", header=True)
    out_name = _tmp_path()
    ExampleLinesCopyToFunction.write(
        batch=pa.record_batch({"a": [1], "b": ["x"]}, schema=SCHEMA),
        options=opts,
        file_path=out_name,
        params=params,
    )
    n = ExampleLinesCopyToFunction.close(options=opts, file_path=out_name, params=params)
    assert n == 1
    assert _read(out_name) == "a|b\n1|x\n"


def test_close_empty_input_with_header_writes_header_only() -> None:
    """An empty COPY with header=true still emits the header row (0 data rows)."""
    store = _Store()
    params = _params(store)
    out_name = _tmp_path()
    n = ExampleLinesCopyToFunction.close(
        options=ExampleLinesCopyToArgs(null_string="NA", header=True),
        file_path=out_name,
        params=params,
    )
    assert n == 0
    assert _read(out_name) == "a,b\n"


# ---------------------------------------------------------------------------
# Secret forwarding (CopyToFunction.on_secrets hook)
# ---------------------------------------------------------------------------


def _secret_params(store: _Store, secrets: ResolvedSecrets) -> types.SimpleNamespace:
    """A TableBufferingParams-shaped stub carrying resolved secrets for write/close."""
    bind_call = types.SimpleNamespace(input_schema=SCHEMA)
    init_call = types.SimpleNamespace(bind_call=bind_call)
    return types.SimpleNamespace(storage=store, init_call=init_call, execution_id=b"x", secrets=secrets)


def test_on_secrets_requests_destination_scoped_secret() -> None:
    """on_secrets() registers a pending lookup scoped to the COPY destination path."""
    accessor = SecretsAccessor(None)  # nothing resolved yet → first-call behavior
    params = types.SimpleNamespace(
        args=SecretLinesCopyToArgs(),  # default secret_type='vgi_example'
        bind_call=types.SimpleNamespace(copy_to=types.SimpleNamespace(file_path="s3://bucket/out.bin")),
        secrets=accessor,
    )
    SecretLinesCopyToFunction.on_secrets(params)
    # The hook must have asked the framework for a scoped two-phase resolution.
    assert accessor.needs_resolution
    pending = accessor.pending_lookups
    assert len(pending) == 1
    assert pending[0].secret_type == "vgi_example"
    assert pending[0].scope == "s3://bucket/out.bin"


def test_close_forwards_resolved_secret_api_key() -> None:
    """close() writes the resolved (destination-scoped) secret's api_key + row count."""
    store = _Store()
    out_name = _tmp_path()
    secrets = ResolvedSecrets(
        {
            "writer_creds": {
                "type": pa.scalar("vgi_example"),
                "scope": pa.scalar(out_name),
                "api_key": pa.scalar("WRITER_KEY"),
            }
        }
    )
    params = _secret_params(store, secrets)
    opts = SecretLinesCopyToArgs()

    SecretLinesCopyToFunction.write(
        batch=pa.record_batch({"a": [1, 2], "b": ["x", "y"]}, schema=SCHEMA),
        options=opts,
        file_path=out_name,
        params=params,
    )
    n = SecretLinesCopyToFunction.close(options=opts, file_path=out_name, params=params)
    assert n == 2
    assert _read(out_name) == "api_key=WRITER_KEY\nrows=2\n"


def test_close_writes_none_when_secret_absent() -> None:
    """A genuinely missing secret resolves to 'NONE' (silent miss, not an error)."""
    store = _Store()
    out_name = _tmp_path()
    params = _secret_params(store, ResolvedSecrets())
    opts = SecretLinesCopyToArgs()
    n = SecretLinesCopyToFunction.close(options=opts, file_path=out_name, params=params)
    assert n == 0
    assert _read(out_name) == "api_key=NONE\nrows=0\n"


def test_catalog_advertises_secret_lines_out_format() -> None:
    """The secret-forwarding writer is advertised like any other COPY TO format."""
    formats = ExampleCatalog().copy_from_formats(attach_opaque_data=b"", transaction_opaque_data=None)
    by = {(f.direction, f.format_name): f for f in formats}
    assert ("to", "secret_lines_out") in by
    assert by[("to", "secret_lines_out")].handler == "secret_lines_writer"


def test_catalog_advertises_copy_to_format() -> None:
    """The example catalog advertises example_lines_out with direction='to'."""
    formats = ExampleCatalog().copy_from_formats(attach_opaque_data=b"", transaction_opaque_data=None)
    by = {(f.direction, f.format_name): f for f in formats}
    assert ("to", "example_lines_out") in by
    fmt = by[("to", "example_lines_out")]
    assert fmt.handler == "example_lines_writer"
    assert fmt.comment == "Toy delimited-text writer for tests"
    assert fmt.tags.get("category") == "copy_to"
    opt_schema = pa.ipc.read_schema(pa.py_buffer(fmt.options))
    assert set(opt_schema.names) == {
        "delimiter",
        "null_string",
        "header",
        "header_repeat",
        "on_exists",
        "fail_on_value",
    }
