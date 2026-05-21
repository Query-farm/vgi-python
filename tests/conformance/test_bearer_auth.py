# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/bearer_auth/``.

HTTP-only area. Depends on ``Client`` gaining HTTP + auth plumbing before the
Python probe can exercise it. Until then, bearer-token behaviour is covered
by C++ integration tests and by direct ``vgi_rpc.http`` tests.
"""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area("bearer_auth", ["bearer_token.test"])
