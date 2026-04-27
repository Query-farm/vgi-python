"""Conformance test: scalar function on_bind() sees attach_id and transaction_id.

When a scalar function is invoked through an ATTACHed catalog, the C++
extension forwards the catalog's attach_id (and any active transaction_id)
on the bind RPC. The scalar surface must expose those to user code via
BindParameters so a function backing a versioned/transactional catalog
can route to the right backend.
"""

from __future__ import annotations

from typing import Annotated, Any

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.protocol import BindRequest, FunctionType
from vgi.scalar_function import (
    BindParameters,
    BindResult,
    Param,
    Returns,
    ScalarFunction,
)

_seen: dict[str, Any] = {}


class _AttachIdEcho(ScalarFunction):
    class Meta:
        name = "attach_id_echo"

    @classmethod
    def on_bind(cls, params: BindParameters) -> BindResult:
        _seen["attach_id"] = params.attach_id
        _seen["transaction_id"] = params.transaction_id
        return BindResult(pa.int64())

    @classmethod
    def compute(
        cls,
        x: Annotated[pa.Int64Array, Param(doc="x")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        return x


def _bind(attach_id: bytes | None, transaction_id: bytes | None) -> None:
    _seen.clear()
    _AttachIdEcho.bind(
        BindRequest(
            function_name="attach_id_echo",
            arguments=Arguments(),
            function_type=FunctionType.SCALAR,
            input_schema=pa.schema([("x", pa.int64())]),
            attach_id=attach_id,
            transaction_id=transaction_id,
        )
    )


def test_on_bind_receives_attach_id() -> None:
    """attach_id from BindRequest reaches BindParameters.attach_id."""
    _bind(attach_id=b"\xaa" * 16, transaction_id=None)
    assert _seen["attach_id"] == b"\xaa" * 16
    assert _seen["transaction_id"] is None


def test_on_bind_receives_transaction_id() -> None:
    """transaction_id from BindRequest reaches BindParameters.transaction_id."""
    _bind(attach_id=b"\xaa" * 16, transaction_id=b"\xbb" * 8)
    assert _seen["attach_id"] == b"\xaa" * 16
    assert _seen["transaction_id"] == b"\xbb" * 8


def test_on_bind_attach_id_optional() -> None:
    """Both fields are None when the function is invoked without a catalog."""
    _bind(attach_id=None, transaction_id=None)
    assert _seen["attach_id"] is None
    assert _seen["transaction_id"] is None
