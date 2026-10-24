# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The SDK's own errors carry vgi-rpc error codes (WIRE_PROTOCOL.md §8).

Bad input is ``INVALID_ARGUMENT``, a missing object is ``NOT_FOUND``, a write
against a read-only catalog is ``FAILED_PRECONDITION``, an unsupported operation
is ``UNIMPLEMENTED``. A worker's own bug -- its output violating its declared
schema -- stays unclassified (``UNKNOWN``) so it is never mistaken for bad input.
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest
from vgi_rpc.errors import Code, error_code_of
from vgi_rpc.rpc import RpcError

from tests.conftest import FIXTURE_WORKER
from vgi.arguments import Arg, Arguments, ArgumentValidationError
from vgi.catalog.attach_option import MissingAttachOptionsError
from vgi.client.client import Client, ClientError
from vgi.exceptions import (
    ArgumentTypeError,
    CatalogReadOnlyError,
    InvalidArgumentError,
    NotFoundError,
    SchemaValidationError,
    UnsupportedOperationError,
)
from vgi.scalar_function import TypeMismatchError


def _is_numeric(t: pa.DataType) -> bool:
    return pa.types.is_integer(t) or pa.types.is_floating(t)


class TestDeclaredCodes:
    """Each SDK error class declares the code the cross-SDK mapping assigns it."""

    @pytest.mark.parametrize(
        ("exc", "code", "base"),
        [
            (ArgumentValidationError("bad", constraint="must be >= 1"), Code.INVALID_ARGUMENT, ValueError),
            (ArgumentTypeError("bad type"), Code.INVALID_ARGUMENT, SchemaValidationError),
            (InvalidArgumentError("no overload"), Code.INVALID_ARGUMENT, ValueError),
            (MissingAttachOptionsError("cat", ["token"]), Code.INVALID_ARGUMENT, ValueError),
            (NotFoundError("Unknown function: 'x'"), Code.NOT_FOUND, ValueError),
            (CatalogReadOnlyError("read-only"), Code.FAILED_PRECONDITION, Exception),
            (UnsupportedOperationError("Table create not implemented."), Code.UNIMPLEMENTED, NotImplementedError),
        ],
    )
    def test_code_and_hierarchy(self, exc: BaseException, code: Code, base: type[BaseException]) -> None:
        """The code is declared and the class still satisfies existing handlers."""
        assert error_code_of(exc) == code
        assert isinstance(exc, base)

    def test_worker_output_schema_violation_stays_unknown(self) -> None:
        """A worker's output not matching its schema is a bug, not bad input."""
        assert error_code_of(SchemaValidationError("output mismatch")) == Code.UNKNOWN

    def test_type_mismatch_on_input_is_invalid_argument(self) -> None:
        """An input parameter of the wrong type is the caller's error."""
        exc = TypeMismatchError("x", param_name="x", expected_type=pa.int64(), actual_type=pa.string())
        assert error_code_of(exc) == Code.INVALID_ARGUMENT
        assert isinstance(exc, TypeError)

    def test_type_mismatch_on_return_stays_unknown(self) -> None:
        """A wrong return type is the worker's bug."""
        exc = TypeMismatchError("out", param_name="return", expected_type=pa.int64(), actual_type=pa.string())
        assert error_code_of(exc) == Code.UNKNOWN

    def test_type_bound_failure_is_invalid_argument(self) -> None:
        """A failed ``type_bound`` raises the coded subclass of SchemaValidationError."""
        arg: Arg[object] = Arg(0, type_bound=_is_numeric)
        with pytest.raises(SchemaValidationError) as excinfo:
            arg.validate_type_bound(pa.string())
        assert isinstance(excinfo.value, ArgumentTypeError)
        assert error_code_of(excinfo.value) == Code.INVALID_ARGUMENT


def _rpc_error(exc: BaseException) -> RpcError:
    cause = exc.__cause__
    assert isinstance(cause, RpcError), f"expected an RpcError cause, got {cause!r}"
    return cause


@pytest.fixture(scope="module")
def client() -> Iterator[Client]:
    """A client on the fixture worker."""
    with Client(FIXTURE_WORKER) as c:
        yield c


class TestOverTheWire:
    """The fixture worker's rejections arrive with their code."""

    def test_scalar_type_rejection(self, client: Client) -> None:
        """``double('abc')``: a string fails the numeric type bound."""
        batch = pa.record_batch({"x": pa.array(["abc"])})
        with pytest.raises(ClientError) as excinfo:
            list(client.scalar_function(function_name="double", schema_path=["main"], input=iter([batch])))
        assert _rpc_error(excinfo.value).error_code == "INVALID_ARGUMENT"

    def test_table_argument_constraint(self, client: Client) -> None:
        """``sequence(10, batch_size := 0)``: violates ``batch_size >= 1``."""
        with pytest.raises(ClientError) as excinfo:
            list(
                client.table_function(
                    function_name="sequence",
                    schema_path=["main"],
                    arguments=Arguments(positional=(pa.scalar(10),), named={"batch_size": pa.scalar(0)}),
                )
            )
        err = _rpc_error(excinfo.value)
        assert "must be >= 1" in str(err)
        assert err.error_code == "INVALID_ARGUMENT"

    def test_unknown_function(self, client: Client) -> None:
        """An unknown function name is NOT_FOUND."""
        with pytest.raises(ClientError) as excinfo:
            list(
                client.table_function(
                    function_name="nonexistent_function",
                    schema_path=["main"],
                    arguments=Arguments(positional=()),
                )
            )
        assert _rpc_error(excinfo.value).error_code == "NOT_FOUND"
