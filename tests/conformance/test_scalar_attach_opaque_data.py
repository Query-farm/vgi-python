# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance test: scalar function on_bind() sees attach_opaque_data and transaction_opaque_data.

When a scalar function is invoked through an ATTACHed catalog, the C++
extension forwards the catalog's attach_opaque_data (and any active transaction_opaque_data)
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


class _AttachOpaqueDataEcho(ScalarFunction):
    class Meta:
        name = "attach_opaque_data_echo"

    @classmethod
    def on_bind(cls, params: BindParameters) -> BindResult:
        _seen["attach_opaque_data"] = params.attach_opaque_data
        _seen["transaction_opaque_data"] = params.transaction_opaque_data
        return BindResult(pa.int64())

    @classmethod
    def compute(
        cls,
        x: Annotated[pa.Int64Array, Param(doc="x")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        return x


def _bind(attach_opaque_data: bytes | None, transaction_opaque_data: bytes | None) -> None:
    _seen.clear()
    # The worker unwraps the sealed attach to the framework plaintext
    # ``uuid(16) || catalog_bytes`` and threads it as ``attach_plaintext``; the
    # body sees only the catalog bytes. Mimic that here (this test bypasses the
    # worker), prepending a dummy UUID so the strip yields the catalog bytes back.
    attach_plaintext = (b"\x00" * 16 + attach_opaque_data) if attach_opaque_data is not None else None
    _AttachOpaqueDataEcho.bind(
        BindRequest(
            function_name="attach_opaque_data_echo",
            arguments=Arguments(),
            function_type=FunctionType.SCALAR,
            input_schema=pa.schema([("x", pa.int64())]),
            attach_opaque_data=attach_opaque_data,
            transaction_opaque_data=transaction_opaque_data,
        ),
        attach_plaintext=attach_plaintext,
    )


def test_on_bind_receives_attach_opaque_data() -> None:
    """attach_opaque_data from BindRequest reaches BindParameters.attach_opaque_data."""
    _bind(attach_opaque_data=b"\xaa" * 16, transaction_opaque_data=None)
    assert _seen["attach_opaque_data"] == b"\xaa" * 16
    assert _seen["transaction_opaque_data"] is None


def test_on_bind_receives_transaction_opaque_data() -> None:
    """transaction_opaque_data from BindRequest reaches BindParameters.transaction_opaque_data."""
    _bind(attach_opaque_data=b"\xaa" * 16, transaction_opaque_data=b"\xbb" * 8)
    assert _seen["attach_opaque_data"] == b"\xaa" * 16
    assert _seen["transaction_opaque_data"] == b"\xbb" * 8


def test_on_bind_attach_opaque_data_optional() -> None:
    """Both fields are None when the function is invoked without a catalog."""
    _bind(attach_opaque_data=None, transaction_opaque_data=None)
    assert _seen["attach_opaque_data"] is None
    assert _seen["transaction_opaque_data"] is None
