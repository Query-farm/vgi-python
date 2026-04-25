"""Example worker that declares attach-time options of many types and echoes them back.

This worker exists to exercise the attach-time options pipeline end-to-end:
- Declared options (``AttachOptions`` inner class) advertised via the ``catalogs()``
  RPC for pre-attach discovery.
- Values received at ``catalog_attach`` are serialized into the returned
  ``attach_id`` so they survive pooled-worker reuse (subprocess) and stateless
  transports (HTTP). Nothing is stored on ``self``.
- The ``echo_attach_options`` table function decodes ``attach_id`` on every
  invocation and returns a one-row batch containing every declared option.

Run directly as a worker::

    vgi-example-attach-options-worker

Or serve over HTTP via ``vgi-serve``.
"""

from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import CallContext, OutputCollector
from vgi_rpc.utils import deserialize_record_batch, serialize_record_batch_bytes

from vgi.catalog.attach_option import AttachOption, AttachOptionSpec, extract_attach_option_specs
from vgi.catalog.catalog_interface import (
    AttachId,
    CatalogAttachResult,
    CatalogInfo,
    ReadOnlyCatalogInterface,
    TransactionId,
)
from vgi.catalog.descriptors import Catalog, Schema
from vgi.invocation import BindResponse
from vgi.schema_utils import schema
from vgi.table_function import BindParams, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi.worker import Worker

__all__ = [
    "AttachOptionsWorker",
    "EchoAttachOptionsFunction",
    "main",
]


CATALOG_NAME = "attach_options"
_ATTACH_ID_SEP = b"\x00"
_UUID_BYTES = 16


# ---------------------------------------------------------------------------
# Declared attach-time options: one per supported type
# ---------------------------------------------------------------------------


class AttachOptions:
    """Attach-time options covering the supported Arrow/DuckDB type space."""

    # Scalar primitives
    opt_bool: Annotated[bool, AttachOption(desc="Boolean option")] = True
    opt_int8: Annotated[int, AttachOption(desc="int8", arrow_type=pa.int8())] = -8
    opt_int16: Annotated[int, AttachOption(desc="int16", arrow_type=pa.int16())] = -16
    opt_int32: Annotated[int, AttachOption(desc="int32", arrow_type=pa.int32())] = -32
    opt_int64: Annotated[int, AttachOption(desc="int64")] = -64
    opt_uint8: Annotated[int, AttachOption(desc="uint8", arrow_type=pa.uint8())] = 8
    opt_uint16: Annotated[int, AttachOption(desc="uint16", arrow_type=pa.uint16())] = 16
    opt_uint32: Annotated[int, AttachOption(desc="uint32", arrow_type=pa.uint32())] = 32
    opt_uint64: Annotated[int, AttachOption(desc="uint64", arrow_type=pa.uint64())] = 64
    opt_float32: Annotated[float, AttachOption(desc="float32", arrow_type=pa.float32())] = 1.5
    opt_float64: Annotated[float, AttachOption(desc="float64")] = 2.5
    opt_string: Annotated[str, AttachOption(desc="UTF-8 string")] = "hello"
    opt_blob: Annotated[bytes, AttachOption(desc="Binary blob")] = b"\x00\x01\x02"

    # Temporal
    opt_date: Annotated[datetime.date, AttachOption(desc="Date", arrow_type=pa.date32())] = datetime.date(2026, 4, 24)
    opt_time: Annotated[
        datetime.time, AttachOption(desc="Time of day", arrow_type=pa.time64("us"))
    ] = datetime.time(12, 34, 56)
    opt_timestamp: Annotated[
        datetime.datetime, AttachOption(desc="Naive timestamp", arrow_type=pa.timestamp("us"))
    ] = datetime.datetime(2026, 4, 24, 12, 34, 56)
    opt_timestamp_tz: Annotated[
        datetime.datetime,
        AttachOption(desc="Timestamp with UTC tz", arrow_type=pa.timestamp("us", tz="UTC")),
    ] = datetime.datetime(2026, 4, 24, 12, 34, 56, tzinfo=datetime.UTC)

    # Precision
    opt_decimal: Annotated[
        Decimal, AttachOption(desc="Decimal(18,4)", arrow_type=pa.decimal128(18, 4))
    ] = Decimal("123.4500")

    # Nested
    opt_list: Annotated[
        list[int], AttachOption(desc="List of int64", arrow_type=pa.list_(pa.int64()))
    ] = [1, 2, 3]
    opt_struct: Annotated[
        dict[str, object],
        AttachOption(
            desc="Struct",
            arrow_type=pa.struct([pa.field("a", pa.int64()), pa.field("b", pa.string())]),
        ),
    ] = {"a": 1, "b": "x"}


# Resolve once at import time; used both to build the echo function's output schema
# and to backfill defaults in catalog_attach.
_ATTACH_OPTION_SPECS: list[AttachOptionSpec] = extract_attach_option_specs(AttachOptions)

_ECHO_SCHEMA: pa.Schema = schema({spec.name: spec.type for spec in _ATTACH_OPTION_SPECS})


# ---------------------------------------------------------------------------
# attach_id encoding / decoding
# ---------------------------------------------------------------------------


def _build_echo_batch(received: dict[str, Any]) -> pa.RecordBatch:
    """Merge received option values with declared defaults, return a one-row batch."""
    row: dict[str, Any] = {spec.name: spec.default for spec in _ATTACH_OPTION_SPECS}
    for name, value in received.items():
        if name in row:
            row[name] = value
    return pa.RecordBatch.from_pylist([row], schema=_ECHO_SCHEMA)


def _encode_attach_id(received: dict[str, Any]) -> AttachId:
    batch = _build_echo_batch(received)
    ipc_bytes = serialize_record_batch_bytes(batch)
    return AttachId(uuid.uuid4().bytes + _ATTACH_ID_SEP + ipc_bytes)


def _decode_attach_id(attach_id: bytes) -> pa.RecordBatch:
    raw = bytes(attach_id)
    if len(raw) <= _UUID_BYTES + 1 or raw[_UUID_BYTES : _UUID_BYTES + 1] != _ATTACH_ID_SEP:
        raise ValueError("attach_id does not carry an options payload")
    ipc_bytes = raw[_UUID_BYTES + 1 :]
    batch, _ = deserialize_record_batch(ipc_bytes)
    return batch


# ---------------------------------------------------------------------------
# Echo table function
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class _EchoArgs:
    """No arguments — the echo function reads state from ``attach_id``."""


@dataclass(kw_only=True)
class _EchoState(ArrowSerializableDataclass):
    emitted: bool = False


@init_single_worker
class EchoAttachOptionsFunction(TableFunctionGenerator[_EchoArgs, _EchoState]):
    """Return the attach-time option values that were passed at ATTACH.

    One row, one column per declared option. The values come from ``attach_id``
    so the function is safe under pool reuse (subprocess) and stateless
    dispatch (HTTP): no per-attach state lives on ``self``.
    """

    FunctionArguments = _EchoArgs

    class Meta:
        name = "echo_attach_options"
        description = "Echo the attach-time option values carried in attach_id"
        categories = ["generator", "testing"]

    FIXED_SCHEMA: ClassVar[pa.Schema] = _ECHO_SCHEMA

    @classmethod
    def on_bind(cls, params: BindParams[_EchoArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[_EchoArgs]) -> _EchoState:
        return _EchoState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_EchoArgs],
        state: _EchoState,
        out: OutputCollector,
    ) -> None:
        if state.emitted:
            out.finish()
            return

        assert params.init_call is not None
        attach_id = params.init_call.bind_call.attach_id
        if attach_id is None:
            raise ValueError("echo_attach_options requires an attach_id")

        batch = _decode_attach_id(attach_id)
        # Re-cast to the declared schema so column order matches what bind promised.
        batch = batch.select(_ECHO_SCHEMA.names)
        out.emit(batch)
        state.emitted = True


# ---------------------------------------------------------------------------
# Catalog interface
# ---------------------------------------------------------------------------


_CATALOG_DESCRIPTOR = Catalog(
    name=CATALOG_NAME,
    schemas=[
        Schema(
            name="main",
            tables=(),
            views=(),
            functions=(EchoAttachOptionsFunction,),
        ),
    ],
)


class AttachOptionsCatalog(ReadOnlyCatalogInterface):
    """Catalog that advertises AttachOptions and echoes values via attach_id."""

    catalog = _CATALOG_DESCRIPTOR
    catalog_name = CATALOG_NAME

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        ctx: CallContext | None = None,
    ) -> CatalogAttachResult:
        del data_version_spec, implementation_version, ctx
        if name != CATALOG_NAME:
            raise ValueError(f"Unknown catalog: {name!r}. Available: {CATALOG_NAME}")

        attach_id = _encode_attach_id(options)

        return CatalogAttachResult(
            attach_id=attach_id,
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_id_required=True,
            default_schema="main",
            settings=[],
            resolved_data_version=None,
            resolved_implementation_version=None,
        )

    def catalogs(self) -> list[CatalogInfo]:
        return [
            CatalogInfo(
                name=CATALOG_NAME,
                implementation_version=None,
                data_version_spec=None,
                attach_option_specs=[spec.serialize() for spec in _ATTACH_OPTION_SPECS],
            ),
        ]

    def catalog_version(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        ctx: CallContext | None = None,
    ) -> int:
        del attach_id, transaction_id, ctx
        return 1


# ---------------------------------------------------------------------------
# Worker + entry point
# ---------------------------------------------------------------------------


class AttachOptionsWorker(Worker):
    """Worker exposing :class:`AttachOptionsCatalog`."""

    # The AttachOptions inner class is picked up by Worker.__init_subclass__,
    # which extracts specs and injects them into the catalog interface.
    AttachOptions = AttachOptions

    catalog_interface = AttachOptionsCatalog
    catalog_name = CATALOG_NAME
    functions = [EchoAttachOptionsFunction]


def main() -> None:
    AttachOptionsWorker.main()


if __name__ == "__main__":
    main()
