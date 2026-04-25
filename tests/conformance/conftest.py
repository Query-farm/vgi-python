"""Conformance-suite-specific fixtures.

Top-level ``tests/conftest.py`` already exposes ``client_transport``,
``http_worker``, and ``example_worker``. This file is reserved for future
conformance-specific fixtures (e.g. an attached-catalog factory) so
cross-cutting helpers don't leak into the broader test suite.
"""

from __future__ import annotations

CPP_INTEGRATION_ROOT = "/Users/rusty/Development/vgi/test/sql/integration"
