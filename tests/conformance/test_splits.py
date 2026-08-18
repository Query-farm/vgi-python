# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/splits/``.

Split-based scans divide a scan into named, independently redeemable units so a
distributed engine can retry a task without re-reading or skipping rows. The
behaviour is exercised end to end by the C++ SLT suite listed here, against the
split fixtures in ``vgi/_test_fixtures/table/splits.py``.

The envelope those splits are carried in — including the two security refusals
(the ``alg:none`` downgrade and cross-principal replay) and the cross-SDK byte
vectors every implementation must agree on — is tested Python-side in
``tests/test_split_token.py``, which is where a wire disagreement between five
independent implementations would actually surface.
"""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "splits",
    [
        "cache_interaction.test",
        "more_splits_than_threads.test",
        "parity.test",
        "rollback.test",
        "skew.test",
        "zero_row_split.test",
    ],
)
