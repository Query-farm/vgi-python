"""MetaWorker — composes multiple Worker instances in a single process.

Each Worker manages its own catalog interface. The MetaWorker dispatches
VgiProtocol calls to the right Worker based on catalog name (for attach)
and wrapped attach_id (for everything else).

attach_id wrapping:
    Each sub-worker may use the same underlying attach_id. The MetaWorker
    prepends a 1-byte worker index to distinguish them:
        wrapped = bytes([worker_index]) + original_attach_id

Usage::

    MetaWorker.serve(ExampleWorker, WritableWorker)
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any

from vgi_rpc import RpcServer
from vgi_rpc.rpc import CallContext, Stream, serve_stdio

from vgi.catalog.catalog_interface import AttachId, CatalogAttachResult
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


def _make_attach_delegate(name: str) -> Any:
    """Create a method that unwraps attach_id and delegates to the right worker.

    Copies the signature from Worker so vgi_rpc's validation passes.
    """
    import inspect

    # Get the Worker method's signature to copy parameter names
    worker_method = getattr(Worker, name)
    sig = inspect.signature(worker_method)

    def method(self: MetaWorker, **kwargs: Any) -> Any:
        attach_id = kwargs.pop("attach_id")
        worker, original_id = self._unwrap_attach_id(attach_id)
        return getattr(worker, name)(attach_id=original_id, **kwargs)

    # Copy the signature from the Worker method so vgi_rpc validation passes
    method.__name__ = name
    method.__qualname__ = f"MetaWorker.{name}"
    method.__signature__ = sig  # type: ignore[attr-defined]
    return method


# Methods where attach_id is the first parameter (most catalog methods)
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
    The MetaWorker wraps/unwraps attach_ids to route calls to the right worker.
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

        logger.info(
            "MetaWorker initialized: %d workers, catalogs=%s",
            len(workers),
            list(self._name_to_index.keys()),
        )

    def _resolve_function(self, request: BindRequest) -> Any:
        """Dispatch function-class resolution to the worker that hosts it.

        The HTTP state-rehydration path calls this on the implementation
        without any attach_id, so route by function name across all
        sub-workers.
        """
        for w in self._workers:
            registry = type(w)._build_registry()
            if request.function_name in registry:
                return w._resolve_function(request)
        msg = f"Unknown function: '{request.function_name}'"
        raise ValueError(msg)

    # ========== attach_id wrapping ==========

    def _wrap_attach_id(self, worker_index: int, attach_id: bytes) -> bytes:
        """Prepend worker index to attach_id for disambiguation."""
        return bytes([worker_index]) + attach_id

    def _unwrap_attach_id(self, wrapped_id: bytes) -> tuple[Worker, bytes]:
        """Extract worker index and original attach_id."""
        idx = wrapped_id[0]
        original = wrapped_id[1:]
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

        wrapped = self._wrap_attach_id(idx, result.attach_id)
        return dataclasses.replace(result, attach_id=AttachId(wrapped))

    # ========== Name-based dispatch (no attach_id) ==========

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

    # ========== Request-object methods (attach_id inside request) ==========

    def catalog_table_create(self, request: Any) -> None:
        """Create a table — dispatch via attach_id in request."""
        worker, original_id = self._unwrap_attach_id(request.attach_id)
        patched = dataclasses.replace(request, attach_id=original_id)
        worker.catalog_table_create(patched)

    def catalog_macro_create(self, request: Any) -> None:
        """Create a macro — dispatch via attach_id in request."""
        worker, original_id = self._unwrap_attach_id(request.attach_id)
        patched = dataclasses.replace(request, attach_id=original_id)
        worker.catalog_macro_create(patched)

    def catalog_index_create(self, request: Any) -> None:
        """Create an index — dispatch via attach_id in request."""
        worker, original_id = self._unwrap_attach_id(request.attach_id)
        patched = dataclasses.replace(request, attach_id=original_id)
        worker.catalog_index_create(patched)

    # ========== bind / init (unwrap attach_id from request) ==========

    def bind(self, request: BindRequest, ctx: CallContext) -> Any:
        """Dispatch bind to the right worker."""
        if request.attach_id:
            try:
                worker, original_id = self._unwrap_attach_id(request.attach_id)
                request = dataclasses.replace(request, attach_id=original_id)
                return worker.bind(request, ctx=ctx)
            except (IndexError, KeyError):
                pass  # Invalid wrapped id — fall through to registry search

        for w in self._workers:
            registry = type(w)._build_registry()
            if request.function_name in registry:
                return w.bind(request, ctx=ctx)

        msg = f"Unknown function '{request.function_name}'"
        raise ValueError(msg)

    def init(self, request: InitRequest, ctx: CallContext) -> Stream[ProcessState, GlobalInitResponse]:
        """Dispatch init to the right worker."""
        if request.bind_call and request.bind_call.attach_id:
            try:
                worker, original_id = self._unwrap_attach_id(request.bind_call.attach_id)
                bind_call = dataclasses.replace(request.bind_call, attach_id=original_id)
                request = dataclasses.replace(request, bind_call=bind_call)
                return worker.init(request, ctx=ctx)
            except (IndexError, KeyError):
                pass  # Invalid wrapped id — fall through

        fn_name = request.bind_call.function_name if request.bind_call else ""
        for w in self._workers:
            registry = type(w)._build_registry()
            if fn_name in registry:
                return w.init(request, ctx=ctx)

        msg = f"Unknown function '{fn_name}'"
        raise ValueError(msg)

    def table_function_cardinality(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch cardinality estimation to the right worker."""
        fn_name = request.bind_call.function_name if request.bind_call else ""
        for w in self._workers:
            registry = type(w)._build_registry()
            if fn_name in registry:
                return w.table_function_cardinality(request, ctx=ctx)
        msg = f"Unknown function '{fn_name}'"
        raise ValueError(msg)

    def table_function_statistics(self, request: Any, ctx: CallContext) -> Any:
        """Dispatch per-column statistics lookup to the right worker."""
        fn_name = request.bind_call.function_name if request.bind_call else ""
        for w in self._workers:
            registry = type(w)._build_registry()
            if fn_name in registry:
                return w.table_function_statistics(request, ctx=ctx)
        msg = f"Unknown function '{fn_name}'"
        raise ValueError(msg)

    # ========== Aggregate function dispatch ==========

    def _dispatch_aggregate(self, request: Any, method_name: str, ctx: CallContext) -> Any:
        """Dispatch an aggregate RPC to the right worker by function_name."""
        fn_name = getattr(request, "function_name", "")
        if hasattr(request, "attach_id") and request.attach_id:
            try:
                worker, original_id = self._unwrap_attach_id(request.attach_id)
                import dataclasses

                request = dataclasses.replace(request, attach_id=original_id)
                return getattr(worker, method_name)(request, ctx=ctx)
            except (IndexError, KeyError):
                pass
        for w in self._workers:
            registry = type(w)._build_registry()
            if fn_name in registry:
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

    # ========== Serve entry point ==========

    @classmethod
    def serve(cls, *worker_classes: type[Worker]) -> None:
        """Instantiate workers and serve via vgi_rpc on stdin/stdout."""
        from vgi.protocol import VgiProtocol

        # Log startup (some tests check that stderr has output)
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
        logger.info("worker_starting")

        workers = [wc() for wc in worker_classes]
        meta = cls(workers)
        server = RpcServer(VgiProtocol, meta)
        serve_stdio(server)


# Register all attach_id-based delegate methods on MetaWorker
for _method_name in _ATTACH_ID_METHODS:
    setattr(MetaWorker, _method_name, _make_attach_delegate(_method_name))
