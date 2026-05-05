"""Tests for Worker.table_function_dynamic_to_string()."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
import pytest
from vgi_rpc.rpc import AuthContext, CallContext, OutputCollector

from vgi.arguments import Arg, Arguments
from vgi.invocation import FunctionType
from vgi.protocol import (
    BindRequest,
    TableFunctionDynamicToStringRequest,
    TableFunctionDynamicToStringResponse,
)
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi.worker import Worker


@dataclass(slots=True, frozen=True)
class _Args:
    count: Annotated[int, Arg(0, doc="Number of rows")]


@init_single_worker
@bind_fixed_schema
class _OverrideFunc(TableFunctionGenerator[_Args]):
    """Returns a non-empty diagnostic map."""

    class Meta:
        name = "with_override"

    FIXED_SCHEMA = pa.schema([pa.field("n", pa.int64())])

    @classmethod
    def dynamic_to_string(cls, params: BindParams[_Args], execution_id: bytes) -> Mapping[str, str]:
        return {"rows_produced": "42", "execution": execution_id.hex()[:8]}

    @classmethod
    def process(cls, params: ProcessParams[_Args], state: None, out: OutputCollector) -> None:
        out.finish()


@init_single_worker
@bind_fixed_schema
class _DefaultFunc(TableFunctionGenerator[_Args]):
    """Doesn't override dynamic_to_string — base returns {}."""

    class Meta:
        name = "no_override"

    FIXED_SCHEMA = pa.schema([pa.field("n", pa.int64())])

    @classmethod
    def process(cls, params: ProcessParams[_Args], state: None, out: OutputCollector) -> None:
        out.finish()


@init_single_worker
@bind_fixed_schema
class _RaiseFunc(TableFunctionGenerator[_Args]):
    """Raises from the user hook — dispatcher must swallow and return empty."""

    class Meta:
        name = "raises"

    FIXED_SCHEMA = pa.schema([pa.field("n", pa.int64())])

    @classmethod
    def dynamic_to_string(cls, params: BindParams[_Args], execution_id: bytes) -> Mapping[str, str]:
        raise RuntimeError("user hook is broken")

    @classmethod
    def process(cls, params: ProcessParams[_Args], state: None, out: OutputCollector) -> None:
        out.finish()


def _request(function_name: str) -> TableFunctionDynamicToStringRequest:
    return TableFunctionDynamicToStringRequest(
        bind_call=BindRequest(
            function_name=function_name,
            arguments=Arguments(positional=(pa.scalar(1),)),
            function_type=FunctionType.TABLE,
        ),
        global_execution_id=b"\x01\x02\x03\x04\x05\x06\x07\x08",
    )


def _ctx() -> CallContext:
    return CallContext(auth=AuthContext.anonymous(), emit_client_log=lambda *a, **kw: None)


class TestTableFunctionDynamicToString:
    """Behavioral cases for the dispatcher."""

    def test_override_returns_user_keys(self) -> None:
        """Override returns ordered key/value lists in insertion order."""

        class _MyWorker(Worker):
            functions = [_OverrideFunc]

        result = _MyWorker().table_function_dynamic_to_string(_request("with_override"), _ctx())
        assert isinstance(result, TableFunctionDynamicToStringResponse)
        assert result.keys == ["rows_produced", "execution"]
        assert result.values == ["42", "01020304"]

    def test_no_override_returns_empty(self) -> None:
        """Default base implementation returns {}; dispatcher emits empty parallel lists."""

        class _MyWorker(Worker):
            functions = [_DefaultFunc]

        result = _MyWorker().table_function_dynamic_to_string(_request("no_override"), _ctx())
        assert result.keys == []
        assert result.values == []

    def test_user_hook_raises_returns_empty(self, caplog: pytest.LogCaptureFixture) -> None:
        """User hook raising must not propagate; dispatcher logs and returns empty."""

        class _MyWorker(Worker):
            functions = [_RaiseFunc]

        with caplog.at_level(logging.ERROR, logger="vgi.worker"):
            result = _MyWorker().table_function_dynamic_to_string(_request("raises"), _ctx())

        assert result.keys == []
        assert result.values == []
        assert any("dynamic_to_string" in rec.message for rec in caplog.records), (
            "expected an error log when user hook raises"
        )
