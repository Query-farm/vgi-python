"""MetaWorker — composes multiple Worker instances in a single process.

Each Worker manages its own catalog interface. The MetaWorker dispatches
VgiProtocol calls to the right Worker based on catalog name (for attach)
and wrapped attach_opaque_data (for everything else).

attach_opaque_data wrapping:
    Each sub-worker may use the same underlying attach_opaque_data. The MetaWorker
    prepends a 1-byte worker index to distinguish them:
        wrapped = bytes([worker_index]) + original_attach_opaque_data

Usage::

    MetaWorker.serve(ExampleWorker, WritableWorker)
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any

from vgi_rpc.rpc import CallContext, Stream

from vgi.catalog.catalog_interface import AttachOpaqueData, CatalogAttachResult
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
    """Create a method that unwraps attach_opaque_data and delegates to the right worker.

    Copies the signature from Worker so vgi_rpc's validation passes.
    """
    import inspect

    # Get the Worker method's signature to copy parameter names
    worker_method = getattr(Worker, name)
    sig = inspect.signature(worker_method)

    def method(self: MetaWorker, **kwargs: Any) -> Any:
        attach_opaque_data = kwargs.pop("attach_opaque_data")
        worker, original_id = self._unwrap_attach_opaque_data(attach_opaque_data)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "dispatch method=%s sub_worker_index=%d wrapped_aid=%s unwrapped_aid=%s",
                name,
                self._wrapped_index(attach_opaque_data),
                _attach_opaque_data_short(attach_opaque_data),
                _attach_opaque_data_short(original_id),
            )
        return getattr(worker, name)(attach_opaque_data=original_id, **kwargs)

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
    "catalog_table_get",
    "catalog_table_drop",
    "catalog_table_scan_function_get",
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
    """Composes multiple Worker instances, dispatching VgiProtocol calls.

    Each Worker has its own catalog interface and function registry.
    The MetaWorker wraps/unwraps attach_opaque_data values to route calls to the right worker.
    """

    def __init__(self, workers: list[Worker]) -> None:
        """Initialize with a list of Worker instances."""
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
        # The 1-byte index is the prefix MetaWorker prepends to attach_opaque_data values;
        # making it explicit here means a stray ``wrapped_aid=…`` in a dispatch
        # log can be cross-referenced without source diving.
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

    def _resolve_function(self, request: BindRequest) -> Any:
        """Dispatch function-class resolution to the worker that hosts it.

        The HTTP state-rehydration path calls this on the implementation
        without any attach_opaque_data, so route by function name across all
        sub-workers.
        """
        for w in self._workers:
            registry = type(w)._build_registry()
            if request.function_name in registry:
                return w._resolve_function(request)
        msg = f"Unknown function: '{request.function_name}'"
        raise ValueError(msg)

    # ========== attach_opaque_data wrapping ==========
    #
    # Wire format of a MetaWorker-wrapped attach_opaque_data:
    #
    #   [ 'M' 'W' 0x00 ][ <index byte> ][ <original attach_opaque_data bytes> ]
    #     ^^^^^^^^^^^^^   ^^^^^^^^^^^^   ^^^^^^^^^^^^^^^^^^^^^^^^^^
    #     magic prefix    sub-worker      original attach_opaque_data as
    #     (3 bytes)       index (1B)      vended by the sub-worker
    #
    # The magic prefix means a wrapped attach_opaque_data is self-identifying: a
    # 4-byte ``MW\0<idx>`` prefix is unmistakable in logs and storage shard
    # ids (shows as ``att-4d570000...`` in hex). Without it, a bare 17-byte
    # attach_opaque_data whose first byte happened to be ``\x00`` was indistinguishable
    # from a wrapped ``[0][16 bytes]`` — making MetaWorker routing bugs
    # silently mis-route (see the dynamic_to_string failure that motivated
    # this marker).
    #
    # 4-byte total overhead (3 magic + 1 index). Sub-workers see the
    # un-prefixed bytes and don't need to know MetaWorker exists.

    _WRAP_MAGIC = b"MW\x00"
    _WRAP_OVERHEAD = len(_WRAP_MAGIC) + 1  # magic + index byte

    def _wrap_attach_opaque_data(self, worker_index: int, attach_opaque_data: bytes) -> bytes:
        """Prepend MetaWorker magic + sub-worker index to a sub-worker's attach_opaque_data."""
        return self._WRAP_MAGIC + bytes([worker_index]) + attach_opaque_data

    def _is_wrapped(self, attach_opaque_data: bytes) -> bool:
        """Quick predicate — does ``attach_opaque_data`` start with the MetaWorker magic?"""
        return (
            len(attach_opaque_data) >= self._WRAP_OVERHEAD
            and attach_opaque_data[: len(self._WRAP_MAGIC)] == self._WRAP_MAGIC
        )

    def _wrapped_index(self, wrapped_id: bytes) -> int:
        """Return the sub-worker index encoded in a wrapped attach_opaque_data.

        Assumes ``_is_wrapped(wrapped_id)`` already returned True. Returns
        -1 for shapes that don't match (defensive — callers use this only
        for logging).
        """
        if not self._is_wrapped(wrapped_id):
            return -1
        return wrapped_id[len(self._WRAP_MAGIC)]

    def _unwrap_attach_opaque_data(self, wrapped_id: bytes) -> tuple[Worker, bytes]:
        """Verify the magic, then split into (sub-worker, original attach_opaque_data).

        Raises ``KeyError`` when the magic is missing or the index byte is
        out of range. Callers in this module catch and fall back to a
        function-name-based registry scan (the legacy path for clients that
        never round-tripped through ``catalog_attach``).
        """
        if not self._is_wrapped(wrapped_id):
            raise KeyError(
                f"attach_opaque_data is not MetaWorker-wrapped (missing magic): "
                f"first8={wrapped_id[:8].hex() if wrapped_id else '-'}"
            )
        idx = wrapped_id[len(self._WRAP_MAGIC)]
        original = wrapped_id[self._WRAP_OVERHEAD :]
        if idx >= len(self._workers):
            raise KeyError(f"MetaWorker sub-worker index {idx} out of range (have {len(self._workers)} workers)")
        return self._workers[idx], original

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

        wrapped = self._wrap_attach_opaque_data(idx, result.attach_opaque_data)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "catalog_attach catalog=%r sub_worker_index=%d original_aid=%s wrapped_aid=%s",
                request.name,
                idx,
                _attach_opaque_data_short(result.attach_opaque_data),
                _attach_opaque_data_short(wrapped),
            )
        return dataclasses.replace(result, attach_opaque_data=AttachOpaqueData(wrapped))

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
        worker, original_id = self._unwrap_attach_opaque_data(request.attach_opaque_data)
        patched = dataclasses.replace(request, attach_opaque_data=original_id)
        worker.catalog_table_create(patched)

    def catalog_macro_create(self, request: Any) -> None:
        """Create a macro — dispatch via attach_opaque_data in request."""
        worker, original_id = self._unwrap_attach_opaque_data(request.attach_opaque_data)
        patched = dataclasses.replace(request, attach_opaque_data=original_id)
        worker.catalog_macro_create(patched)

    def catalog_index_create(self, request: Any) -> None:
        """Create an index — dispatch via attach_opaque_data in request."""
        worker, original_id = self._unwrap_attach_opaque_data(request.attach_opaque_data)
        patched = dataclasses.replace(request, attach_opaque_data=original_id)
        worker.catalog_index_create(patched)

    # ========== bind / init (unwrap attach_opaque_data from request) ==========

    def bind(self, request: BindRequest, ctx: CallContext) -> Any:
        """Dispatch bind to the right worker."""
        if request.attach_opaque_data:
            try:
                worker, original_id = self._unwrap_attach_opaque_data(request.attach_opaque_data)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "dispatch method=bind function=%r sub_worker_index=%d wrapped_aid=%s unwrapped_aid=%s",
                        request.function_name,
                        self._wrapped_index(request.attach_opaque_data),
                        _attach_opaque_data_short(request.attach_opaque_data),
                        _attach_opaque_data_short(original_id),
                    )
                request = dataclasses.replace(request, attach_opaque_data=original_id)
                return worker.bind(request, ctx=ctx)
            except (IndexError, KeyError):
                pass  # Invalid wrapped id — fall through to registry search

        for w in self._workers:
            registry = type(w)._build_registry()
            if request.function_name in registry:
                logger.debug(
                    "dispatch method=bind function=%r fallback=registry_scan",
                    request.function_name,
                )
                return w.bind(request, ctx=ctx)

        msg = f"Unknown function '{request.function_name}'"
        raise ValueError(msg)

    def init(self, request: InitRequest, ctx: CallContext) -> Stream[ProcessState, GlobalInitResponse]:
        """Dispatch init to the right worker."""
        if request.bind_call and request.bind_call.attach_opaque_data:
            try:
                worker, original_id = self._unwrap_attach_opaque_data(request.bind_call.attach_opaque_data)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "dispatch method=init function=%r sub_worker_index=%d wrapped_aid=%s unwrapped_aid=%s",
                        request.bind_call.function_name,
                        self._wrapped_index(request.bind_call.attach_opaque_data),
                        _attach_opaque_data_short(request.bind_call.attach_opaque_data),
                        _attach_opaque_data_short(original_id),
                    )
                bind_call = dataclasses.replace(request.bind_call, attach_opaque_data=original_id)
                request = dataclasses.replace(request, bind_call=bind_call)
                return worker.init(request, ctx=ctx)
            except (IndexError, KeyError):
                pass  # Invalid wrapped id — fall through

        fn_name = request.bind_call.function_name if request.bind_call else ""
        for w in self._workers:
            registry = type(w)._build_registry()
            if fn_name in registry:
                logger.debug(
                    "dispatch method=init function=%r fallback=registry_scan",
                    fn_name,
                )
                return w.init(request, ctx=ctx)

        msg = f"Unknown function '{fn_name}'"
        raise ValueError(msg)

    def _unwrap_bind_call_attach_opaque_data(
        self,
        request: Any,
        *,
        method_name: str = "?",
    ) -> tuple[Any, Any | None]:
        """Resolve the target sub-worker by unwrapping ``request.bind_call.attach_opaque_data``.

        Returns ``(patched_request, worker)`` where ``patched_request`` has the
        unwrapped (sub-worker-relative) attach_opaque_data and ``worker`` is the matching
        sub-worker. Returns ``(request, None)`` when the attach_opaque_data is missing or
        unwrapping fails — caller falls back to a registry scan by function name.

        Mirrors the unwrap that ``init``/``bind`` already perform for the
        wrapped attach_opaque_data MetaWorker prepends. Without this, sibling RPCs that
        carry ``bind_call.attach_opaque_data`` (cardinality, statistics, dynamic_to_string)
        would deliver the wrapped 18-byte id to the sub-worker — which then
        derives a different shard_key than ``init``/``process`` use for the
        same logical attach, so storage reads land on the wrong DO.
        """
        bind_call = getattr(request, "bind_call", None)
        if bind_call is None:
            return request, None
        wrapped_aid = getattr(bind_call, "attach_opaque_data", None)
        if not wrapped_aid:
            return request, None
        try:
            worker, original_id = self._unwrap_attach_opaque_data(wrapped_aid)
        except (IndexError, KeyError):
            return request, None
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "dispatch method=%s function=%r sub_worker_index=%d wrapped_aid=%s unwrapped_aid=%s",
                method_name,
                getattr(bind_call, "function_name", "?"),
                self._wrapped_index(wrapped_aid),
                _attach_opaque_data_short(wrapped_aid),
                _attach_opaque_data_short(original_id),
            )
        patched_bind_call = dataclasses.replace(bind_call, attach_opaque_data=original_id)
        patched_request = dataclasses.replace(request, bind_call=patched_bind_call)
        return patched_request, worker

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
        if hasattr(request, "attach_opaque_data") and request.attach_opaque_data:
            try:
                worker, original_id = self._unwrap_attach_opaque_data(request.attach_opaque_data)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "dispatch method=%s function=%r sub_worker_index=%d wrapped_aid=%s unwrapped_aid=%s",
                        method_name,
                        fn_name,
                        self._wrapped_index(request.attach_opaque_data),
                        _attach_opaque_data_short(request.attach_opaque_data),
                        _attach_opaque_data_short(original_id),
                    )
                request = dataclasses.replace(request, attach_opaque_data=original_id)
                return getattr(worker, method_name)(request, ctx=ctx)
            except (IndexError, KeyError):
                pass
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
