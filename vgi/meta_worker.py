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
from vgi_rpc.rpc import CallContext, serve_stdio

from vgi.catalog.catalog_interface import CatalogAttachResult
from vgi.protocol import (
    BindRequest,
    CatalogAttachRequest,
    CatalogsResponse,
    InitRequest,
)
from vgi.worker import Worker

logger = logging.getLogger("vgi.meta_worker")


class MetaWorker:
    """Composes multiple Worker instances, dispatching VgiProtocol calls.

    Each Worker has its own catalog interface and function registry.
    The MetaWorker wraps/unwraps attach_ids to route calls to the right worker.
    """

    def __init__(self, workers: list[Worker]) -> None:
        """Initialize with a list of Worker instances."""
        self._workers = workers
        self._name_to_index: dict[str, int] = {}

        # Build catalog name → worker index mapping
        for i, w in enumerate(workers):
            try:
                cat = w._get_catalog()
                for name in cat.catalogs():
                    self._name_to_index[name] = i
            except ValueError:
                pass  # Worker has no catalog — skip

        logger.info(
            "MetaWorker initialized: %d workers, catalogs=%s",
            len(workers),
            list(self._name_to_index.keys()),
        )

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
        """Return union of all catalog names across all workers."""
        names: list[str] = []
        for w in self._workers:
            try:
                cat = w._get_catalog()
                names.extend(cat.catalogs())
            except ValueError:
                pass
        return CatalogsResponse(items=names)

    # ========== Catalog attach (dispatch by name, wrap result) ==========

    def catalog_attach(self, request: CatalogAttachRequest) -> CatalogAttachResult:
        """Attach to a catalog — dispatch by name with dynamic fallback."""
        idx = self._name_to_index.get(request.name)

        if idx is not None:
            # Static match
            result = self._workers[idx].catalog_attach(request)
        else:
            # Dynamic: try each worker until one accepts
            for i, w in enumerate(self._workers):
                try:
                    result = w.catalog_attach(request)
                    idx = i
                    self._name_to_index[request.name] = i  # Cache for future
                    break
                except (ValueError, NotImplementedError):
                    continue
            else:
                msg = f"No worker handles catalog '{request.name}'"
                raise ValueError(msg)

        # Wrap the attach_id
        wrapped = self._wrap_attach_id(idx, result.attach_id)
        return CatalogAttachResult(
            attach_id=wrapped,
            supports_transactions=result.supports_transactions,
            supports_time_travel=result.supports_time_travel,
            catalog_version_frozen=result.catalog_version_frozen,
            catalog_version=result.catalog_version,
            attach_id_required=result.attach_id_required,
            default_schema=result.default_schema,
            settings=result.settings,
            secret_types=result.secret_types,
        )

    # ========== bind / init (unwrap attach_id from request) ==========

    def bind(self, request: BindRequest, ctx: CallContext) -> Any:
        """Dispatch bind to the right worker via attach_id or function registry."""
        if request.attach_id:
            worker, original_id = self._unwrap_attach_id(request.attach_id)
            request = dataclasses.replace(request, attach_id=original_id)
            return worker.bind(request, ctx=ctx)

        # No attach_id: search function registries
        for w in self._workers:
            registry = type(w)._build_registry()
            if request.function_name in registry:
                return w.bind(request, ctx=ctx)

        msg = f"No worker has function '{request.function_name}'"
        raise ValueError(msg)

    def init(self, request: InitRequest, ctx: CallContext) -> Any:
        """Dispatch init to the right worker via attach_id or function registry."""
        if request.bind_call and request.bind_call.attach_id:
            worker, original_id = self._unwrap_attach_id(request.bind_call.attach_id)
            bind_call = dataclasses.replace(request.bind_call, attach_id=original_id)
            request = dataclasses.replace(request, bind_call=bind_call)
            return worker.init(request, ctx=ctx)

        # No attach_id: search function registries
        fn_name = request.bind_call.function_name if request.bind_call else ""
        for w in self._workers:
            registry = type(w)._build_registry()
            if fn_name in registry:
                return w.init(request, ctx=ctx)

        msg = f"No worker has function '{fn_name}'"
        raise ValueError(msg)

    # ========== All attach_id-based catalog methods ==========
    #
    # These all follow the same pattern: unwrap attach_id, delegate to worker.
    # We use __getattr__ to auto-generate delegators for any method that
    # the MetaWorker doesn't explicitly define.

    def __getattr__(self, name: str) -> Any:
        """Auto-delegate catalog methods that take attach_id as first arg."""
        if not name.startswith("catalog_"):
            msg = f"'{type(self).__name__}' object has no attribute '{name}'"
            raise AttributeError(msg)

        def _delegate(attach_id: bytes, **kwargs: Any) -> Any:
            worker, original_id = self._unwrap_attach_id(attach_id)
            method = getattr(worker, name)
            return method(attach_id=original_id, **kwargs)

        return _delegate

    # ========== Explicit overrides for methods with non-standard signatures ==========

    def catalog_detach(self, attach_id: bytes) -> None:
        """Detach from a catalog."""
        worker, original_id = self._unwrap_attach_id(attach_id)
        worker.catalog_detach(attach_id=original_id)

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

    def catalog_table_create(self, request: Any) -> None:
        """Create a table — dispatch via attach_id in request."""
        worker, original_id = self._unwrap_attach_id(request.attach_id)
        # Replace attach_id in the request
        patched = dataclasses.replace(request, attach_id=original_id)
        worker.catalog_table_create(patched)

    # ========== Serve entry point ==========

    @classmethod
    def serve(cls, *worker_classes: type[Worker]) -> None:
        """Instantiate workers and serve via vgi_rpc on stdin/stdout."""
        from vgi.protocol import VgiProtocol

        workers = [wc() for wc in worker_classes]
        meta = cls(workers)
        server = RpcServer(VgiProtocol, meta)
        serve_stdio(server)

    @classmethod
    def main(cls, *worker_classes: type[Worker]) -> None:
        """Entry point — same as serve but parses CLI args."""
        cls.serve(*worker_classes)
