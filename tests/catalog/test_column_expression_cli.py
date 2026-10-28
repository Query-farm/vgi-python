# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""CLI defaults/USING reach the durable provider through real signed HTTP RPCs."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest
from click.testing import CliRunner
from vgi_rpc.http._testing import _SyncTestClient

from vgi.catalog.catalog_interface import AttachOpaqueData, SerializedSchema
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.sql import SqlExpression, column_expressions
from vgi.client import Client
from vgi.client.cli import cli
from vgi.serve import create_app

from .column_paths_reference import ColumnPathsCatalog, ColumnPathsWorker
from .typing_helpers import CatalogObjectArgs, CatalogObjectTransactionArgs, present


@pytest.mark.parametrize("automatic_attach", [False, True])
def test_cli_column_expression_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, automatic_attach: bool
) -> None:
    monkeypatch.setattr(ColumnPathsCatalog, "database_path", str(tmp_path / "cli-columns.sqlite"), raising=False)
    key = b"column-cli-reference-signing-key"
    backend = ColumnPathsWorker(quiet=True)
    backend._signing_key = key
    runner = CliRunner()
    with Client.from_http(
        "http://vgi.test", httpx_client=_SyncTestClient(create_app(ColumnPathsWorker, describe=False, signing_key=key))
    ) as client:
        # Replace only CLI transport construction; attach, codecs, capability
        # negotiation and provider mutations still use the public HTTP path.
        monkeypatch.setattr("vgi.client.cli_catalog.Client", lambda _: client)
        monkeypatch.setattr("vgi.client.Client", lambda _: client)
        attached = runner.invoke(cli, ["catalog", "attach", "columns", "--worker", "unused"])
        assert attached.exit_code == 0, attached.output
        token_hex = json.loads(attached.output)["attach_opaque_data"]
        token = AttachOpaqueData(bytes.fromhex(token_hex))
        args: CatalogObjectArgs = dict(attach_opaque_data=token, schema_path=["main"], name="cli.values")
        client.table_create(
            **args, columns=SerializedSchema(pa.schema([pa.field("a.b", pa.int32())]).serialize().to_pybytes())
        )
        provider_args: CatalogObjectTransactionArgs = dict(
            attach_opaque_data=AttachOpaqueData(present(backend._unwrap_attach(token))),
            schema_path=args["schema_path"],
            name=args["name"],
            transaction_opaque_data=None,
        )
        backend._get_catalog().seed_rows(**provider_args, rows=[{"a.b": 3}])
        attachment = ["--catalog", "columns"] if automatic_attach else ["--attach-opaque-data", token_hex]

        def command(*arguments: str) -> None:
            result = runner.invoke(
                cli,
                ["catalog", "table", "column", *arguments, *attachment, "--worker", "unused"],
            )
            assert result.exit_code == 0, result.output

        def default() -> SqlExpression | None:
            info = client.table_get(**args)
            assert info is not None
            schema = pa.ipc.read_schema(pa.py_buffer(info.columns))
            return column_expressions(schema.field("a.b")).default

        command("set-default", "main", "cli.values", "a.b", "9")
        expression = default()
        assert expression is not None and expression.sql == "9"
        assert expression.schema_path == ["main"]
        assert expression.evaluation_context == QueryEvaluationContext()
        backend._get_catalog().seed_rows(**provider_args, rows=[{}])

        # An absent --using stays absent; a present expression reads old rows.
        command("set-type", "main", "cli.values", "--column", '{"name":"a.b","type":"int64"}')
        command(
            "set-type",
            "main",
            "cli.values",
            "--column",
            '{"name":"a.b","type":"float64"}',
            "--using",
            '"a.b" + 0.5',
        )
        rows = [row for part in backend._get_catalog().physical_rows(**provider_args) for row in part["rows"]]
        assert rows == [{"a.b": 3.5}, {"a.b": 9.5}]

        command("set-default", "main", "cli.values", "a.b", "NULL")
        expression = default()
        assert expression is not None and expression.sql == "NULL"
        command("drop-default", "main", "cli.values", "a.b")
        assert default() is None
