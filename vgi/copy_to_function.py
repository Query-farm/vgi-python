# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Base class for custom ``COPY ... TO`` format writers.

A :class:`CopyToFunction` lets a VGI catalog act as a remote sink: the user runs
``COPY (query|table) TO 'path' (FORMAT '<alias>.<fmt>', opt val)`` and DuckDB
streams the rows out to the worker, which writes them to a destination (a
proprietary format, a remote API/object store, a custom sink).

Mechanically a ``CopyToFunction`` is a buffered (Sink+Combine) function with **no
Source phase** — it reuses the entire ``table_buffering_process`` /
``table_buffering_combine`` machinery on both sides:

* :meth:`write` is called once per input batch (the buffered ``process()`` step,
  fanned out across DuckDB's sink threads / per-thread workers). Persist the batch
  to a shard via ``params.storage`` (``execution_id``-scoped — see below).
* :meth:`close` is called **exactly once** on the coordinator worker (the buffered
  ``combine()`` step, driven by DuckDB's once-only ``copy_to_finalize``). Read the
  shards back and perform the terminal write+flush+close of the destination.

There is no finalize/drain phase, so the destination MUST be fully written and
closed inside :meth:`close` — a writer that forgets leaves a silent partial file.

**Cross-process invariant.** ``write()`` and ``close()`` may run on different
worker processes (pool rotation / HTTP). Any shard state ``close()`` needs MUST
live in cross-process storage scoped by ``params.execution_id`` (``params.storage``
is the canonical choice) or be written to a destination that tolerates concurrent
writers (object-store multipart, append API). Buffering on ``self`` / module
globals silently breaks under rotation — identical to ``TableBufferingFunction``.

The destination ``path`` + ``format`` arrive via the bind's ``copy_to`` context
(:meth:`copy_to_path`); the COPY options arrive as the function's normal
``Arg``-annotated arguments (``params.args``). The source schema is the input
schema (``params.init_call.bind_call.input_schema``); ``write()`` also receives
each batch directly.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, ClassVar, final

import pyarrow as pa

from vgi.invocation import BindResponse
from vgi.table_buffering_function import TableBufferingFunction, TableBufferingParams

if TYPE_CHECKING:
    from vgi_rpc.rpc import OutputCollector

    from vgi.table_function import BindParams

__all__ = ["CopyToFunction"]


@final
class _NoFinalizeState:
    """Sentinel — the COPY-TO path never runs a Source/finalize phase."""


class CopyToFunction[TArgs](TableBufferingFunction[TArgs, None]):
    """Base class for custom ``COPY ... TO`` format writers.

    Subclass and:

    * set :attr:`COPY_TO_FORMAT` to the SQL ``FORMAT`` identifier,
    * declare any options as ``Arg``-annotated ``FunctionArguments`` (the
      destination ``file_path`` is supplied by the COPY statement, **not** an
      option),
    * implement :meth:`write` (per input batch) and :meth:`close` (terminal write).

    Register the subclass in the catalog's function list like any function.

    Ordering: by default the sink is **parallel** (per-thread workers write shards,
    ``combine()`` merges) and rows arrive in no particular order. If the writer
    requires rows in source order, set ``Meta.sink_order_dependent = True`` — the
    extension then uses a **single-threaded** sink (DuckDB ``REGULAR_COPY_TO_FILE``),
    so one worker receives every batch in source order (when
    ``preserve_insertion_order`` is on, the default). This trades write parallelism
    for ordering.
    """

    #: SQL ``FORMAT`` identifier users type, e.g. ``COPY t TO 'x' (FORMAT myfmt)``.
    COPY_TO_FORMAT: ClassVar[str]

    #: Direction marker surfaced to discovery; always ``"to"`` for this base.
    COPY_TO_DIRECTION: ClassVar[str] = "to"

    #: Optional free-text comment surfaced by ``vgi_copy_formats()``.
    COPY_TO_COMMENT: ClassVar[str | None] = None

    # ------------------------------------------------------------------
    # Buffered-function plumbing (final — subclasses override write/close)
    # ------------------------------------------------------------------

    @final
    @classmethod
    def on_bind(cls, params: BindParams[TArgs]) -> BindResponse:
        """A sink produces no rows — bind to an empty output schema.

        ``on_bind`` is ``@final`` (a writer has no output schema to compute), but
        a cloud writer still needs credentials. The seam is :meth:`on_secrets`,
        called here so a subclass can request ``CREATE SECRET`` values via the
        framework's two-phase secret bind without overriding ``on_bind`` itself.
        """
        cls.on_secrets(params)
        return BindResponse(output_schema=pa.schema([]))

    @classmethod
    def on_secrets(cls, params: BindParams[TArgs]) -> None:
        """Request the credentials this writer needs to reach its destination.

        Override to forward ``CREATE SECRET`` values to :meth:`write` / :meth:`close`
        for secret-backed cloud writes (S3/GCS/HTTP/...). Call
        ``params.secrets.get(secret_type, scope=..., name=...)`` — typically scoping
        by the destination path (:meth:`copy_to_path` / ``params.bind_call.copy_to``)
        so DuckDB resolves the longest-prefix-matching secret. The framework issues a
        two-phase bind retry to resolve every requested secret from the caller's
        secret store, then surfaces the resolved values on ``params.secrets`` (a
        :class:`ResolvedSecrets`) at :meth:`write` / :meth:`close` time. Requested
        secrets that don't exist resolve to "not found" rather than an error — pass
        ``required=True`` to ``get()`` to make a missing secret fail the bind.

        The destination path is available without an ``init_call`` here via
        ``params.bind_call.copy_to.file_path`` (use :meth:`copy_to_path` only at
        write/close time, where an ``init_call`` exists).

        Default: request nothing (no secrets forwarded), so existing writers that
        never touched credentials are unaffected.
        """
        # Default no-op. Subclasses override to call params.secrets.get(...).

    @final
    @classmethod
    def copy_to_path(cls, params: TableBufferingParams[TArgs]) -> str:
        """Destination path from the ``COPY ... TO 'path'`` statement."""
        assert params.init_call is not None
        cf = params.init_call.bind_call.copy_to
        if cf is None:  # pragma: no cover - defended at bind
            raise ValueError(
                f"{cls.__name__} is a COPY TO format writer; invoke it via "
                f"COPY <source> TO '<path>' (FORMAT {getattr(cls, 'COPY_TO_FORMAT', '?')})."
            )
        return cf.file_path

    @final
    @classmethod
    def process(cls, batch: pa.RecordBatch, params: TableBufferingParams[TArgs]) -> bytes:
        """Sink one input batch (→ :meth:`write`); return the execution_id bucket."""
        cls.write(batch=batch, options=params.args, file_path=cls.copy_to_path(params), params=params)
        return params.execution_id

    @final
    @classmethod
    def combine(cls, state_ids: list[bytes], params: TableBufferingParams[TArgs]) -> list[bytes]:
        """Terminal write (→ :meth:`close`), once on the coordinator. No Source phase."""
        cls.close(options=params.args, file_path=cls.copy_to_path(params), params=params)
        return []  # no finalize streams — the COPY path never drains output

    @final
    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[TArgs],
        finalize_state_id: bytes,
        state: None,
        out: OutputCollector,
    ) -> None:
        """Never invoked on the COPY-TO path (combine returns no finalize ids)."""
        out.finish()

    # ------------------------------------------------------------------
    # Author hooks
    # ------------------------------------------------------------------

    @classmethod
    @abstractmethod
    def write(
        cls,
        *,
        batch: pa.RecordBatch,
        options: TArgs,
        file_path: str,
        params: TableBufferingParams[TArgs],
    ) -> None:
        """Persist one input ``batch`` to a shard (called per sink batch).

        Store the batch in cross-process storage scoped by
        ``params.execution_id`` (``params.storage``) so :meth:`close` — which may
        run on a different worker process — can read it back; or write directly to
        a concurrency-tolerant destination. Do NOT buffer on ``self``.
        """

    @classmethod
    @abstractmethod
    def close(
        cls,
        *,
        options: TArgs,
        file_path: str,
        params: TableBufferingParams[TArgs],
    ) -> int:
        """Write the destination and close it, once. Return the row count.

        Read the shards persisted by :meth:`write` (via ``params.storage``) and
        perform the terminal write + flush + close of ``file_path``. Called even
        when zero rows were written (empty COPY) — produce an empty/header-only
        file. The returned count is informational (DuckDB reports its own
        ``rows_copied``); return the number of rows written.
        """
