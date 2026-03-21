"""HTTP utilities for VGI workers.

Provides the worker description page and demo blob storage for HTTP-mode workers.
"""

from __future__ import annotations

from vgi.http.demo_storage import (
    DemoBlobStorage,
    MaxRequestBytesMiddleware,
    add_blob_routes,
    localhost_only_validator,
)
from vgi.http.worker_page import WorkerPageResource, build_worker_page

__all__ = [
    "DemoBlobStorage",
    "MaxRequestBytesMiddleware",
    "WorkerPageResource",
    "add_blob_routes",
    "build_worker_page",
    "localhost_only_validator",
]
