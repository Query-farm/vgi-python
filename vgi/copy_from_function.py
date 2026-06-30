# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Base class for custom ``COPY ... FROM`` format readers.

A :class:`CopyFromFunction` lets a VGI catalog act as a remote file-format
reader: the user runs ``COPY target FROM 'path' (FORMAT <name>, opt val, ...)``
and the worker parses the source and streams Arrow batches that DuckDB inserts
into the local ``target`` table.

Mechanically a ``CopyFromFunction`` is an ordinary producer-mode table function
(so it reuses the entire table-function bind/init/scan path on both sides). What
makes it a COPY format is twofold:

* it sets :attr:`CopyFromFunction.COPY_FROM_FORMAT` to the SQL ``FORMAT``
  identifier, and
* the catalog advertises it via
  :meth:`vgi.catalog.catalog_interface.ReadOnlyCatalogInterface.copy_from_formats`,
  so the VGI DuckDB extension registers a DuckDB ``CopyFunction`` for it.

The COPY statement's file path and the target table's schema arrive on the bind
through :class:`vgi.protocol.CopyFromContext` (``params.bind_call.copy_from`` /
``params.init_call.bind_call.copy_from``). The COPY options arrive as the
function's normal ``Arg``-annotated arguments — declare them on
``FunctionArguments`` exactly like any other function; their ``doc`` becomes the
option description surfaced by ``vgi_copy_formats()`` / ``vgi_function_arguments()``.

Subclasses implement :meth:`read`, emitting Arrow batches whose schema matches
``expected_schema`` exactly — DuckDB inserts **no** cast between the scan and the
INSERT, so a type/arity mismatch is rejected by the extension at COPY bind.
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, final

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

from vgi.invocation import BindResponse
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)

if TYPE_CHECKING:
    from vgi_rpc.rpc import OutputCollector

__all__ = ["CopyFromFunction"]


@dataclass(kw_only=True)
class _CopyFromState(ArrowSerializableDataclass):
    """Single-shot guard: the whole source is read on the first ``process()`` tick."""

    done: bool = False


@init_single_worker
class CopyFromFunction[TArgs](TableFunctionGenerator[TArgs, _CopyFromState]):
    """Base class for custom ``COPY ... FROM`` format readers.

    Subclass and:

    * set :attr:`COPY_FROM_FORMAT` to the SQL ``FORMAT`` identifier,
    * declare any options as ``Arg``-annotated ``FunctionArguments`` (the source
      ``file_path`` is supplied by the COPY statement, **not** as an option),
    * implement :meth:`read` to parse the source and emit Arrow batches matching
      ``expected_schema``.

    Register the subclass in the catalog's function list like any table function.
    """

    #: SQL ``FORMAT`` identifier users type, e.g. ``COPY t FROM 'x' (FORMAT myfmt)``.
    COPY_FROM_FORMAT: ClassVar[str]

    #: Reserved for a future ``COPY ... TO``; only ``"from"`` is supported today.
    COPY_FROM_DIRECTION: ClassVar[str] = "from"

    #: Optional free-text comment surfaced by ``vgi_copy_formats()``.
    COPY_FROM_COMMENT: ClassVar[str | None] = None

    @final
    @classmethod
    def on_bind(cls, params: BindParams[TArgs]) -> BindResponse:
        """Bind the output schema to the COPY target's schema.

        DuckDB forces the scan's output types to the target table's columns, so a
        COPY-FROM reader must produce exactly ``expected_schema``.
        """
        cf = params.bind_call.copy_from
        if cf is None:
            fmt = getattr(cls, "COPY_FROM_FORMAT", "?")
            raise ValueError(
                f"{cls.__name__} is a COPY FROM format reader; invoke it via "
                f"COPY <table> FROM '<path>' (FORMAT {fmt}), not as a table function."
            )
        # Forward credentials for secret-backed cloud sources (S3/GCS/HTTP/...)
        # via the framework's two-phase secret bind. on_bind is @final (the output
        # schema is fixed to the COPY target), so on_secrets is the seam.
        cls.on_secrets(params)
        # ``expected_schema`` is transparently a ``pa.Schema`` here — the
        # ArrowType(binary) annotation only governs the wire encoding.
        return BindResponse(output_schema=cf.expected_schema)

    @classmethod
    def on_secrets(cls, params: BindParams[TArgs]) -> None:
        """Request the credentials this reader needs to reach its source.

        Override to forward ``CREATE SECRET`` values to :meth:`read` for
        secret-backed cloud sources. Call ``params.secrets.get(secret_type,
        scope=..., name=...)`` — typically scoping by the source path
        (``params.bind_call.copy_from.file_path``) so DuckDB resolves the
        longest-prefix-matching secret. The framework issues a two-phase bind retry
        to resolve every requested secret from the caller's secret store, then
        surfaces the resolved values on ``params.secrets`` (a
        :class:`ResolvedSecrets`) at :meth:`read` time. Pass ``required=True`` to
        ``get()`` to make a missing secret fail the bind.

        Default: request nothing (no secrets forwarded).
        """
        # Default no-op. Subclasses override to call params.secrets.get(...).

    @final
    @classmethod
    def initial_state(cls, params: ProcessParams[TArgs]) -> _CopyFromState:
        """Allocate the single-shot read guard."""
        return _CopyFromState()

    @final
    @classmethod
    def process(
        cls,
        params: ProcessParams[TArgs],
        state: _CopyFromState,
        out: OutputCollector,
    ) -> None:
        """Drive :meth:`read` once, then finish the stream."""
        if state.done:
            out.finish()
            return
        assert params.init_call is not None  # producer mode always has an init_call
        cf = params.init_call.bind_call.copy_from
        if cf is None:  # pragma: no cover - defended at bind
            raise ValueError(f"{cls.__name__}: missing COPY FROM context at process time")
        cls.read(
            path=cf.file_path,
            options=params.args,
            expected_schema=params.output_schema,
            params=params,
            out=out,
        )
        state.done = True
        out.finish()

    @classmethod
    @abstractmethod
    def read(
        cls,
        *,
        path: str,
        options: TArgs,
        expected_schema: pa.Schema,
        params: ProcessParams[TArgs],
        out: OutputCollector,
    ) -> None:
        """Parse ``path`` and emit Arrow batches via ``out.emit(...)``.

        Args:
            path: Source path from the ``COPY ... FROM 'path'`` statement.
            options: Parsed COPY options (the ``FunctionArguments`` instance).
            expected_schema: The COPY target's schema. Every emitted batch must
                have this exact schema (names + types, in order).
            params: Full process parameters (settings, secrets, storage, auth).
            out: Collector to emit batches / log. ``finish()`` is called for you.
        """
