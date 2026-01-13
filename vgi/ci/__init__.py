"""VGI CI Worker - Full-featured catalog for continuous integration testing.

This module provides a comprehensive catalog implementation suitable for:
- CI/CD pipeline testing
- Integration tests for DuckDB extensions
- Catalog DDL operation testing
- Per-attachment state isolation

Key features:
- Each attachment gets fully isolated state (schemas, tables, views)
- Transaction support with rollback capability
- Stores actual table data (not just metadata)
- Version tracking per attachment
"""

from vgi.ci.catalog import CICatalog
from vgi.ci.storage import AttachmentState, AttachmentStorage
from vgi.ci.worker import CIWorker, main

__all__ = [
    "AttachmentState",
    "AttachmentStorage",
    "CICatalog",
    "CIWorker",
    "main",
]
