"""CI Worker - Full-featured catalog for continuous integration testing.

Usage:
    vgi-ci-worker [--quiet]

This worker provides a comprehensive catalog implementation for testing:
- Per-attachment isolated state (each attachment has its own namespace)
- Transaction support with begin/commit/rollback
- Actual table data storage
- Version tracking for cache invalidation

No functions are included - use vgi-example-worker for function testing.
"""

from __future__ import annotations

from vgi.ci.catalog import CICatalog
from vgi.worker import Worker


class CIWorker(Worker):
    """CI Worker for catalog testing.

    This worker provides a full-featured catalog implementation
    for testing DDL operations, transactions, and state isolation.

    Features:
    - Available catalogs: "ci", "test"
    - Per-attachment isolated state
    - Transaction support with rollback
    - Table data storage (not just metadata)
    - Version tracking

    No functions are included - use vgi-example-worker for function testing.
    """

    catalog_name = "ci"
    catalog_interface = CICatalog
    functions: list[type] = []  # No functions, catalog-only


def main() -> None:
    """Run the CI worker process."""
    parser = CIWorker.create_argument_parser()
    args = parser.parse_args()
    CIWorker(quiet=args.quiet).run()


if __name__ == "__main__":
    main()
