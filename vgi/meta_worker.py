# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""MetaWorker — composes multiple [`Worker`][] instances in a single process.

Each `Worker` manages its own catalog interface. The MetaWorker dispatches
[`VgiProtocol`][] calls to the right `Worker` by catalog name — for ``attach``
from the request, and for everything else from the name it sealed into the
``attach_opaque_data`` at attach time.

attach_opaque_data encapsulation:
    Sub-workers may vend byte-identical attach_opaque_data — the built-in
    read-only catalog returns one class constant for every catalog it serves —
    so the value alone cannot say which catalog it came from. At
    ``catalog_attach`` the MetaWorker opens what the sub-worker sealed, records
    the catalog name inside the plaintext, and re-seals it (see
    [`vgi.attach_header`][]). Routing then means opening the envelope and
    reading that name.

    The name lives *inside* the AEAD because the signing key is process-wide: a
    header outside the seal would be client-editable, and every sub-worker's key
    would still open the envelope, so a caller could route one catalog's attach
    into another. It also rides along through HTTP state serialization, so a
    rehydrate that lands on a different instance still routes correctly.

Usage::

    MetaWorker.serve(ExampleWorker, WritableWorker)
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any

from vgi_rpc.rpc import CallContext, Stream

from vgi.catalog.catalog_interface import CatalogAttachResult
from vgi.invocation import GlobalInitResponse
from vgi.protocol import (
    BindRequest,
    CatalogAttachRequest,
    CatalogsResponse,
    InitRequest,
    ProcessState,
)
from vgi.worker import Worker

logger = logging.getLogger("vgi.meta_worker")


def _attach_opaque_data_short(attach_opaque_data: bytes | None) -> str:
    """Stable, low-cardinality identifier for an attach_opaque_data, suitable for logs."""
    if not attach_opaque_data:
        return "-"
    return attach_opaque_data.hex()[:16]


def _make_attach_delegate(name: str) -> Any:
    """Create a method that routes on the sealed catalog name and delegates.

    Copies the signature from [`Worker`][] so vgi_rpc's validation passes.
    """
    import inspect

    # Get the Worker method's signature to copy parameter names
    worker_method = getattr(Worker, name)
    sig = inspect.signature(worker_method)

    def method(self: MetaWorker, **kwargs: Any) -> Any:
        attach_opaque_data = kwargs.pop("attach_opaque_data")
        worker = self._worker_for_attach(attach_opaque_data, method_name=name)
        # The attach is passed through untouched: the sub-worker opens the same
        # envelope and its own unwrap strips the routing header.
        return getattr(worker, name)(attach_opaque_data=attach_opaque_data, **kwargs)

    # Copy the signature from the Worker method so vgi_rpc validation passes
    method.__name__ = name
    method.__qualname__ = f"MetaWorker.{name}"
    method.__signature__ = sig  # type: ignore[attr-defined]
    return method


# Methods where attach_opaque_data is the first parameter (most catalog methods)
_ATTACH_ID_METHODS = [
    "catalog_detach",
    "catalog_version",
    "catalog_transaction_begin",
    "catalog_transaction_commit",
    "catalog_transaction_rollback",
    "catalog_schemas",
    "catalog_schema_get",
    "catalog_schema_create",
    "catalog_schema_drop",
    "catalog_schema_contents_tables",
    "catalog_schema_contents_views",
    "catalog_schema_contents_functions",
    "catalog_schema_contents_macros",
    "catalog_copy_from_formats",
    "catalog_table_get",
    "catalog_table_drop",
    "catalog_table_scan_function_get",
    "catalog_table_scan_branches_get",
    "catalog_table_column_statistics_get",
    "catalog_table_insert_function_get",
    "catalog_table_update_function_get",
    "catalog_table_delete_function_get",
    "catalog_table_comment_set",
    "catalog_table_column_comment_set",
    "catalog_table_rename",
    "catalog_table_column_add",
    "catalog_table_column_drop",
    "catalog_table_column_rename",
    "catalog_table_column_default_set",
    "catalog_table_column_default_drop",
    "catalog_table_column_type_change",
    "catalog_table_not_null_drop",
    "catalog_table_not_null_set",
    "catalog_view_get",
    "catalog_view_create",
    "catalog_view_drop",
    "catalog_view_rename",
    "catalog_view_comment_set",
    "catalog_macro_get",
    "catalog_macro_drop",
    "catalog_index_get",
    "catalog_index_drop",
    "catalog_schema_contents_indexes",
]


class MetaWorker:
    """Composes multiple [`Worker`][] instances, dispatching [`VgiProtocol`][] calls.

    Each `Worker` has its own catalog interface and function registry. Calls are
    routed by the catalog name that ``catalog_attach`` sealed into the
    ``attach_opaque_data``; see the module docstring.
    """

    def __init__(self, workers: list[Worker]) -> None:
        """Initialize with a list of [`Worker`][] instances."""
        self._workers = workers
        self._name_to_index: dict[str, int] = {}
        # The HTTP transport's state-rehydration path expects the
        # implementation to expose ``_vgi_tracer`` directly. Borrow the
        # first worker's tracer; all workers in one process share whatever
        # the otel config produced.
        self._vgi_tracer = workers[0]._vgi_tracer

        for i, w in enumerate(workers):
            try:
                cat = w._get_catalog()
                for info in cat.catalogs():
                    self._name_to_index[info.name] = i
            except ValueError:
                pass

        # Detailed startup record — each sub-worker's index → catalog mapping.
        # Routing resolves a sealed catalog name through this map, so having it
        # in the log means a dispatch line naming a catalog can be tied back to a
        # sub-worker without source diving.
        if logger.isEnabledFor(logging.INFO):
            mapping = [
                {
                    "index": i,
                    "worker_class": type(w).__name__,
                    "catalogs": [name for name, idx in self._name_to_index.items() if idx == i],
                }
                for i, w in enumerate(workers)
            ]
            logger.info(
                "MetaWorker initialized: %d workers, mapping=%s",
                len(workers),
                mapping,
            )

    def _worker_for_attach(self, attach_opaque_data: bytes | None, *, method_name: str = "?") -> Worker:
        """Route to the sub-worker whose catalog minted ``attach_opaque_data``.

        Reads the catalog name sealed into the envelope at ``catalog_attach``.
        Any sub-worker can open it (the signing key is process-wide), so the
        first is used as the opener.
        """
        worker = self._maybe_worker_for_attach(attach_opaque_data)
        if worker is None:
            msg = (
                f"Cannot route {method_name}: attach_opaque_data carries no catalog name "
                f"this process recognizes (known catalogs: {sorted(self._name_to_index)})."
            )
            raise ValueError(msg)
        return worker

    def _maybe_worker_for_attach(self, attach_opaque_data: bytes | None) -> Worker | None:
        """Like :meth:`_worker_for_attach` but ``None`` instead of raising."""
        if not attach_opaque_data:
            return None
        catalog_name = self._workers[0]._attach_catalog_name(attach_opaque_data)
        if catalog_name is None:
            return None
        idx = self._name_to_index.get(catalog_name)
        if idx is None:
            return None
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "dispatch catalog=%r sub_worker_index=%d aid=%s",
                catalog_name,
                idx,
                _attach_opaque_data_short(attach_opaque_data),
            )
        return self._workers[idx]

    def _candidates_for(self, function_name: str, schema_name: str | None) -> list[Worker]:
        """Every sub-worker declaring ``function_name``, most specific first."""
        if schema_name is not None:
            key = (schema_name.lower(), function_name)
            scoped = [w for w in self._workers if key in type(w)._build_schema_registry()]
            if scoped:
                return scoped
        return [w for w in self._workers if function_name in type(w)._build_registry()]

    def _worker_for_unattached(self, function_name: str, schema_name: str | None) -> Worker | None:
        """Pick a sub-worker for a call that carries no usable attach.

        Non-catalog callers (the legacy ``Worker.functions`` list, where
        ``BindRequest.schema_name`` is ``None``) invoke a function without ever
        opening a catalog, so there is no attach to route on and the sole
        declarer of the name is the intended target.

        When more than one sub-worker declares the same name there is no basis
        to choose. Guessing is what silently routed
        ``b.main.test_same_name_catalog(1)`` into the ``twin_a`` implementation
        and returned a plausible wrong answer rather than an error
        (``scalar/same_name_catalogs.test``). Raise instead — reaching here with
        an ambiguous name means the routing key was lost, which is a bug worth
        surfacing.
        """
        candidates = self._candidates_for(function_name, schema_name)
        if not candidates:
            return None
        if len(candidates) > 1:
            where = f"{schema_name}.{function_name}" if schema_name else function_name
            msg = (
                f"Cannot route {where!r}: {len(candidates)} sub-workers declare it "
                f"({', '.join(type(w).__name__ for w in candidates)}) and the call carries "
                f"no attach_opaque_data naming the catalog."
            )
            raise ValueError(msg)
        return candidates[0]

    def _resolve_function(self, request: BindRequest) -> Any:
        """Dispatch function-class resolution to the worker that hosts it.

        The sealed catalog name is the only key that distinguishes two
        sub-workers declaring the same function name. It survives HTTP state
        serialization, so the rehydrate path — which calls this on *this* object
        with the attach the sub-worker persisted — routes the same way a live
        call does.

        Only a call with no attach at all (a non-catalog caller) falls through to
        the registry scan.
        """
        worker = self._maybe_worker_for_attach(request.attach_opaque_data)
        if worker is not None:
            return worker._resolve_function(request)

        fallback_worker = self._worker_for_unattached(request.function_name, request.schema_name)
        if fallback_worker is not None:
            return fallback_worker._resolve_function(request)
        msg = f"Unknown function: '{request.function_name}'"
        raise ValueError(msg)

    # ========== Catalog listing ==========

    def catalog_catalogs(self) -> CatalogsResponse:
        """Return union of all catalog discovery records across all workers."""
        infos = []
        for w in self._workers:
            try:
                cat = w._get_catalog()
                infos.extend(cat.catalogs())
            except ValueError:
                pass
        return CatalogsResponse.from_infos(infos)

    # ========== Catalog attach (dispatch by name, wrap result) ==========

    def catalog_attach(
        self,
        request: CatalogAttachRequest,
        *,
        ctx: CallContext | None = None,
    ) -> CatalogAttachResult:
        """Attach to a catalog — dispatch by name with dynamic fallback."""
        idx = self._name_to_index.get(request.name)

        if idx is not None:
            result = self._workers[idx].catalog_attach(request, ctx=ctx)
        else:
            for i, w in enumerate(self._workers):
                try:
                    result = w.catalog_attach(request, ctx=ctx)
                    idx = i
                    self._name_to_index[request.name] = i
                    break
                except (ValueError, NotImplementedError):
                    continue
            else:
                msg = f"No worker handles catalog '{request.name}'"
                raise ValueError(msg)

        # Record which catalog this attach belongs to, inside the seal. Without
        # it nothing downstream can tell two sub-workers apart: the catalog's own
        # bytes are implementation-defined and routinely identical between
        # catalogs (the built-in read-only interface returns one class constant
        # for all of them).
        if result.attach_opaque_data is not None:
            encapsulated = self._workers[idx]._seal_attach_with_catalog(bytes(result.attach_opaque_data), request.name)
            result = dataclasses.replace(result, attach_opaque_data=encapsulated)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "catalog_attach catalog=%r sub_worker_index=%d aid=%s",
                request.name,
                idx,
                _attach_opaque_data_short(result.attach_opaque_data),
            )
        return result

    # ========== Name-based dispatch (no attach_opaque_data) ==========

    def catalog_create(self, request: Any) -> None:
        """Create a catalog — dispatch to first worker that handles it."""
        for w in self._workers:
            try:
                w.catalog_create(request)
                return
            except (ValueError, NotImplementedError):
                continue
        msg = f"No worker handles catalog_create for '{request.name}'"
        raise ValueError(msg)

    def catalog_drop(self, name: str) -> None:
        """Drop a catalog — dispatch to the worker that owns it."""
        idx = self._name_to_index.get(name)
        if idx is not None:
            self._workers[idx].catalog_drop(name=name)
        else:
            msg = f"No worker owns catalog '{name}'"
            raise ValueError(msg)

    # ========== Request-object methods (attach_opaque_data inside request) ==========

    def catalog_table_create(self, request: Any) -> None:
        """Create a table — dispatch via attach_opaque_data in request."""
        self._worker_for_attach(request.attach_opaque_data, method_name="catalog_table_create").catalog_table_create(
            request
        )

    def catalog_macro_create(self, request: Any) -> None:
        """Create a macro — dispatch via attach_opaque_data in request."""
        self._worker_for_attach(request.attach_opaque_data, method_name="catalog_macro_create").catalog_macro_create(
            request
        )

    def catalog_index_create(self, request: Any) -> None:
        """Create an index — dispatch via attach_opaque_data in request."""
        self._worker_for_attach(request.attach_opaque_data, method_name="catalog_index_create").catalog_index_create(
            request
        )

    # ========== bind / init (unwrap attach_opaque_data from request) ==========

    def bind(self, request: BindRequest, ctx: CallContext) -> Any:
        """Dispatch bind to the right worker."""
        worker = self._maybe_worker_for_attach(request.attach_opaque_data)
        if worker is not None:
            return worker.bind(request, ctx=ctx)

        fallback_worker = self._worker_for_unattached(request.function_name, request.schema_name)
        if fallback_worker is not None:
            logger.debug(
                "dispatch method=bind function=%r schema=%r fallback=registry_scan",
                request.function_name,
                request.schema_name,
            )
            return fallback_worker.bind(request, ctx=ctx)

        msg = f"Unknown function '{request.function_name}'"
        raise ValueError(msg)

    def init(self, request: InitRequest, ctx: CallContext) -> Stream[ProcessState, GlobalInitResponse]:
        """Dispatch init to the right worker."""
        if request.bind_call:
            worker = self._maybe_worker_for_attach(request.bind_call.attach_opaque_data)
            if worker is not None:
                return worker.init(request, ctx=ctx)

        fn_name = request.bind_call.function_name if request.bind_call else ""
        schema = request.bind_call.schema_name if request.bind_call else None
        fallback_worker = self._worker_for_unattached(fn_name, schema)
        if fallback_worker is not None:
            logger.debug(
                "dispatch method=init function=%r schema=%r fallback=registry_scan",
                fn_name,
                schema,
            )
            return fallback_worker.init(request, ctx=ctx)

        msg = f"Unknown function '{fn_name}'"
        raise ValueError(msg)

    def _unwrap_bind_call_attach_opaque_data(
        self,
        request: Any,
        *,
        method_name: str = "?",
    ) -> tuple[Any, Any | None]:
        """Resolve the target sub-worker from ``request.bind_call.attach_opaque_data``.

        Returns ``(request, worker)``; the request is passed through untouched
        (the sub-worker opens the same envelope and strips the routing header
        itself). Returns ``(request, None)`` when there is no attach or its
        catalog is unknown here — the caller then falls back to a registry scan
        by function name.

        Used by the sibling RPCs that carry ``bind_call.attach_opaque_data``
        (cardinality, statistics, dynamic_to_string) so they route exactly as
        ``init``/``process`` do for the same logical attach — otherwise their
        storage reads land on a different shard.
        """
        bind_call = getattr(request, "bind_call", None)
        if bind_call is None:
            return request, None
        aid = getattr(bind_call, "attach_opaque_data", None)
        worker = self._maybe_worker_for_attach(aid)
        if worker is not None and logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "dispatch method=%s function=%r aid=%s",
                method_name,
                getattr(bind_call, "function_name", "?"),
                _attach_opaque_data_short(aid),
            )
        return request, worker

    def table_function_cardinality(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch cardinality estimation to the right worker."""
        patched, worker = self._unwrap_bind_call_attach_opaque_data(
            request,
            method_name="table_function_cardinality",
        )
        if worker is not None:
            return worker.table_function_cardinality(patched, ctx=ctx)
        fn_name = request.bind_call.function_name if request.bind_call else ""
        for w in self._workers:
            registry = type(w)._build_registry()
            if fn_name in registry:
                return w.table_function_cardinality(request, ctx=ctx)
        msg = f"Unknown function '{fn_name}'"
        raise ValueError(msg)

    def table_function_statistics(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch per-column statistics lookup to the right worker."""
        patched, worker = self._unwrap_bind_call_attach_opaque_data(
            request,
            method_name="table_function_statistics",
        )
        if worker is not None:
            return worker.table_function_statistics(patched, ctx=ctx)
        fn_name = request.bind_call.function_name if request.bind_call else ""
        for w in self._workers:
            registry = type(w)._build_registry()
            if fn_name in registry:
                return w.table_function_statistics(request, ctx=ctx)
        msg = f"Unknown function '{fn_name}'"
        raise ValueError(msg)

    def table_function_dynamic_to_string(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch the dynamic_to_string profiler hook to the right worker."""
        patched, worker = self._unwrap_bind_call_attach_opaque_data(
            request,
            method_name="table_function_dynamic_to_string",
        )
        if worker is not None:
            return worker.table_function_dynamic_to_string(patched, ctx=ctx)
        fn_name = request.bind_call.function_name if request.bind_call else ""
        for w in self._workers:
            registry = type(w)._build_registry()
            if fn_name in registry:
                return w.table_function_dynamic_to_string(request, ctx=ctx)
        # Function not registered with any worker — return empty rather than
        # raising. EXPLAIN ANALYZE must never break the query.
        from vgi.protocol import TableFunctionDynamicToStringResponse

        return TableFunctionDynamicToStringResponse(keys=[], values=[])

    # ========== Aggregate function dispatch ==========

    def _dispatch_aggregate(self, request: Any, method_name: str, ctx: CallContext) -> Any:
        """Dispatch an aggregate RPC to the right worker by function_name."""
        fn_name = getattr(request, "function_name", "")
        worker = self._maybe_worker_for_attach(getattr(request, "attach_opaque_data", None))
        if worker is not None:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "dispatch method=%s function=%r aid=%s",
                    method_name,
                    fn_name,
                    _attach_opaque_data_short(request.attach_opaque_data),
                )
            return getattr(worker, method_name)(request, ctx=ctx)
        for w in self._workers:
            registry = type(w)._build_registry()
            if fn_name in registry:
                logger.debug(
                    "dispatch method=%s function=%r fallback=registry_scan",
                    method_name,
                    fn_name,
                )
                return getattr(w, method_name)(request, ctx=ctx)
        raise ValueError(f"Unknown aggregate function '{fn_name}'")

    def aggregate_bind(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch aggregate_bind to the right worker."""
        return self._dispatch_aggregate(request, "aggregate_bind", ctx)

    def aggregate_update(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch aggregate_update to the right worker."""
        return self._dispatch_aggregate(request, "aggregate_update", ctx)

    def aggregate_combine(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch aggregate_combine to the right worker."""
        return self._dispatch_aggregate(request, "aggregate_combine", ctx)

    def aggregate_finalize(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch aggregate_finalize to the right worker."""
        return self._dispatch_aggregate(request, "aggregate_finalize", ctx)

    def aggregate_destructor(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch aggregate_destructor to the right worker."""
        return self._dispatch_aggregate(request, "aggregate_destructor", ctx)

    def aggregate_window_init(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch aggregate_window_init to the right worker."""
        return self._dispatch_aggregate(request, "aggregate_window_init", ctx)

    def aggregate_window(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch aggregate_window to the right worker."""
        return self._dispatch_aggregate(request, "aggregate_window", ctx)

    def aggregate_window_destructor(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch aggregate_window_destructor to the right worker."""
        return self._dispatch_aggregate(request, "aggregate_window_destructor", ctx)

    def aggregate_window_batch(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch aggregate_window_batch to the right worker."""
        return self._dispatch_aggregate(request, "aggregate_window_batch", ctx)

    def aggregate_streaming_open(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch aggregate_streaming_open to the right worker."""
        return self._dispatch_aggregate(request, "aggregate_streaming_open", ctx)

    def aggregate_streaming_chunk(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch aggregate_streaming_chunk to the right worker."""
        return self._dispatch_aggregate(request, "aggregate_streaming_chunk", ctx)

    def aggregate_streaming_close(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch aggregate_streaming_close to the right worker."""
        return self._dispatch_aggregate(request, "aggregate_streaming_close", ctx)

    # ========== Buffered table function dispatch ==========
    # Routing key is function_name (same as aggregate). The underlying
    # _dispatch_aggregate helper isn't aggregate-specific — it just looks up
    # the function by name in each worker's registry.

    def table_buffering_process(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch table_buffering_process to the right worker."""
        return self._dispatch_aggregate(request, "table_buffering_process", ctx)

    def table_buffering_combine(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch table_buffering_combine to the right worker."""
        return self._dispatch_aggregate(request, "table_buffering_combine", ctx)

    def table_buffering_destructor(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch table_buffering_destructor to the right worker."""
        return self._dispatch_aggregate(request, "table_buffering_destructor", ctx)

    def _load_table_buffering_params(
        self,
        request: Any,
        ctx: CallContext,
        *,
        attach_already_unwrapped: bool = False,
    ) -> Any:
        """Dispatch the finalize-tick driver's cold-load to the right worker.

        ``run_table_buffering_finalize_tick`` calls this via
        ``ctx.implementation._load_table_buffering_params(...)``. The catalog
        name sealed into the attach_opaque_data steers us to the right
        sub-worker.

        ``attach_already_unwrapped`` is forwarded to the sub-worker — see
        ``Worker._load_table_buffering_params`` for semantics. Note that when it
        is set the caller holds an already-opened plaintext, which carries no
        routing header (``Worker`` strips it on open), so those calls fall
        through to the registry scan below just as they did before.
        """
        fn_name = getattr(request, "function_name", "")
        if not attach_already_unwrapped:
            worker = self._maybe_worker_for_attach(getattr(request, "attach_opaque_data", None))
            if worker is not None:
                return worker._load_table_buffering_params(
                    request,
                    ctx,
                    attach_already_unwrapped=attach_already_unwrapped,
                )
        for w in self._workers:
            registry = type(w)._build_registry()
            if fn_name in registry:
                return w._load_table_buffering_params(
                    request,
                    ctx,
                    attach_already_unwrapped=attach_already_unwrapped,
                )
        raise ValueError(f"Unknown table_buffering function '{fn_name}'")

    # ========== Serve entry point ==========

    @classmethod
    def serve(cls, *worker_classes: type[Worker]) -> None:
        """Instantiate workers and serve via vgi_rpc.

        Defaults to stdin/stdout for the subprocess transport; passes
        argv through to ``run_server()`` so the worker also participates
        in the AF_UNIX launcher path when launched with
        ``--unix PATH --idle-timeout SEC`` (the vgi C++ extension uses
        this to share warm workers across DuckDB processes).
        """
        from vgi_rpc.rpc import run_server

        from vgi.protocol import VgiProtocol

        # Log startup (some tests check that stderr has output)
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
        logger.info("worker_starting")

        workers = [wc() for wc in worker_classes]
        meta = cls(workers)
        run_server(VgiProtocol, meta)


# Register all attach_opaque_data-based delegate methods on MetaWorker
for _method_name in _ATTACH_ID_METHODS:
    setattr(MetaWorker, _method_name, _make_attach_delegate(_method_name))
