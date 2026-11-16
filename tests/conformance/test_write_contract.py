# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/write_contract/``.

Protocol 3.3 write-path contracts the C++ extension enforces: CTAS
``created``, the table-create reply, delete row-id dedupe, MERGE without a
row id, required scan branches, decimal widths on write input, and (in the
``example_*`` files, against this SDK's standard example worker's
``example.contract`` schema, see ``vgi/_test_fixtures/contract.py``) an INSERT
whose count comes from finalize only, worker-enforced NOT NULL, and decimal32 /
decimal64 / decimal128 columns re-encoded at their declared widths.
"""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "write_contract",
    [
        "ctas_not_created.test",
        "decimal_width.test",
        "delete_rowid_dedupe.test",
        "example_decimal_width.test",
        "example_insert_finalize.test",
        "merge_no_rowid.test",
        "scan_branches_required.test",
        "table_create_reply.test",
    ],
)
