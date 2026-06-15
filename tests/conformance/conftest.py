# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance-suite-specific fixtures.

Top-level ``tests/conftest.py`` already exposes ``client_transport``,
``http_worker``, and ``fixture_worker``. This file is reserved for future
conformance-specific fixtures (e.g. an attached-catalog factory) so
cross-cutting helpers don't leak into the broader test suite.
"""

from __future__ import annotations

import os
from pathlib import Path

# Path to the VGI C++ extension's integration-test corpus (a sibling repo,
# not shipped here). Override with VGI_CPP_INTEGRATION_ROOT; the directory-
# parity test skips when this path is absent.
CPP_INTEGRATION_ROOT = os.environ.get(
    "VGI_CPP_INTEGRATION_ROOT",
    str(Path.home() / "Development" / "vgi" / "test" / "sql" / "integration"),
)
