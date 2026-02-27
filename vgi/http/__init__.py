"""HTTP page utilities for VGI workers.

Provides the worker description page for HTTP-mode workers.
"""

from __future__ import annotations

from vgi.http.worker_page import WorkerPageResource, build_worker_page

__all__ = [
    "WorkerPageResource",
    "build_worker_page",
]
