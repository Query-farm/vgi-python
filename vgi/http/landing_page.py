"""Falcon resources that serve the shared landing page and its JSON contract.

Three routes make up the standardized worker landing surface (see
``docs/http-landing-contract.md`` and ``vgi/http/describe_json.py``):

* ``GET {prefix}/`` → the vendored, self-contained :data:`landing.html` for
  browsers; a small JSON status for health checks / ``?format=json``.
* ``GET {prefix}/describe.json`` → the versioned describe contract.
* ``GET {prefix}/describe/{catalog}/{schema}/{table}.json`` → lazy per-object
  columns.

The page is byte-identical across every VGI language worker; only these
producers differ per language.
"""

from __future__ import annotations

import importlib.resources
import json
import logging
from typing import TYPE_CHECKING

import falcon

from vgi.http.describe_json import build_columns_json, build_describe_json

if TYPE_CHECKING:
    from vgi.worker import Worker

logger = logging.getLogger(__name__)


def load_landing_html() -> bytes:
    """Return the vendored shared landing page bytes."""
    return (importlib.resources.files("vgi.http") / "landing.html").read_bytes()


class LandingPageResource:
    """``GET {prefix}/`` — HTML for browsers, JSON status for health checks."""

    def __init__(self, *, server_id: str) -> None:
        """Load the vendored page bytes once."""
        self._server_id = server_id
        self._html = load_landing_html()

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Serve HTML to browsers; JSON status to health checks / ``?format=json``."""
        accept = req.accept or ""
        want_json = req.get_param("format") == "json" or (
            "application/json" in accept and not req.client_accepts("text/html")
        )
        if want_json:
            resp.content_type = "application/json"
            resp.text = json.dumps({"status": "ok", "server_id": self._server_id, "protocol": "vgi"})
            return
        resp.content_type = "text/html; charset=utf-8"
        resp.data = self._html


class DescribeJsonResource:
    """``GET {prefix}/describe.json`` — the versioned landing contract."""

    def __init__(self, worker_cls: type[Worker], *, oauth: bool, server_id: str) -> None:
        """Capture the worker class and runtime-derived oauth/server_id."""
        self._worker_cls = worker_cls
        self._oauth = oauth
        self._server_id = server_id

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Emit the full ``describe.json`` document."""
        doc = build_describe_json(self._worker_cls, oauth=self._oauth, server_id=self._server_id)
        resp.content_type = "application/json"
        resp.text = json.dumps(doc)


class ColumnsResource:
    """``GET {prefix}/describe/{catalog}/{schema}/{table}.json`` — lazy columns."""

    def __init__(self, worker_cls: type[Worker]) -> None:
        """Capture the worker class."""
        self._worker_cls = worker_cls

    def on_get(self, req: falcon.Request, resp: falcon.Response, catalog: str, schema: str, table: str) -> None:
        """Emit the lazy column payload for one table or view (404 if absent)."""
        # Falcon strips the ``.json`` suffix from the route template; be tolerant
        # if a client passes it through anyway.
        if table.endswith(".json"):
            table = table[: -len(".json")]
        cols = build_columns_json(self._worker_cls, catalog, schema, table)
        resp.content_type = "application/json"
        if cols is None:
            resp.status = falcon.HTTP_404
            resp.text = json.dumps({"error": "object not found"})
            return
        resp.text = json.dumps(cols)
