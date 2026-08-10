"""Falcon resources that serve the shared landing page and its client bundle.

Two routes make up the standardized worker landing surface:

* ``GET {prefix}/`` → the vendored, self-contained :data:`landing.html` for
  browsers; a small JSON status document for health checks / ``?format=json``.
* ``GET {prefix}/vgi-client.js`` → the vendored browser build of the
  ``@query-farm/vgi`` client, which the page imports.

The page reads catalog metadata by speaking the VGI protocol through that
client — the same protocol the DuckDB extension speaks. There is no worker-side
metadata document: what used to be a per-language ``describe.json`` producer
(plus a separately versioned JSON contract and a cross-language conformance
harness) is now one client implementation shared by every worker.

The worker serves the bundle itself rather than the page importing it from a
CDN: this page is same-origin with an authenticated worker and carries its
session cookie, so third-party script here would run with full access to that
origin — and a CDN dependency would break air-gapped deployments that today
need nothing but the worker.

Worker identity (name, docstring, version, language) is not catalog data and
has no protocol method, so it rides on the status document above.
"""

from __future__ import annotations

import importlib.resources
import json
import logging
from importlib.metadata import version as _pkg_version
from typing import TYPE_CHECKING

import falcon

if TYPE_CHECKING:
    from vgi.worker import Worker

logger = logging.getLogger(__name__)

CUPOLA_BASE = "https://cupola.query-farm.services"


def load_landing_html() -> bytes:
    """Return the vendored shared landing page bytes."""
    return (importlib.resources.files("vgi.http") / "landing.html").read_bytes()


def load_client_bundle() -> bytes:
    """Return the vendored browser build of the VGI JS client."""
    return (importlib.resources.files("vgi.http") / "vgi-client.js").read_bytes()


def _pkg_ver() -> str:
    try:
        return _pkg_version("vgi-python")
    except Exception:  # noqa: BLE001
        return "unknown"


class LandingPageResource:
    """``GET {prefix}/`` — HTML for browsers, JSON status for health checks."""

    def __init__(self, worker_cls: type[Worker], *, server_id: str, oauth: bool) -> None:
        """Load the vendored page bytes once and capture worker identity."""
        self._server_id = server_id
        self._oauth = oauth
        self._html = load_landing_html()
        doc = ""
        if worker_cls.__doc__:
            doc = worker_cls.__doc__.strip().split("\n")[0]
        self._status = {
            "status": "ok",
            "server_id": server_id,
            "protocol": "vgi",
            "worker": worker_cls.__name__,
            "doc": doc,
            "version": _pkg_ver(),
            "lang": "python",
            "oauth": oauth,
            "cupola_base": CUPOLA_BASE,
        }

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Serve HTML to browsers; JSON status to health checks / ``?format=json``."""
        accept = req.accept or ""
        want_json = req.get_param("format") == "json" or (
            "application/json" in accept and not req.client_accepts("text/html")
        )
        if want_json:
            resp.content_type = "application/json"
            resp.text = json.dumps(self._status)
            return
        resp.content_type = "text/html; charset=utf-8"
        resp.data = self._html


class ClientBundleResource:
    """``GET {prefix}/vgi-client.js`` — the browser build the page imports."""

    def __init__(self) -> None:
        """Load the vendored bundle once."""
        self._js = load_client_bundle()

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Serve the bundle as an ES module."""
        del req
        resp.content_type = "text/javascript; charset=utf-8"
        # Immutable for a given worker build; the page and bundle are vendored
        # and released together.
        resp.set_header("Cache-Control", "public, max-age=3600")
        resp.data = self._js
