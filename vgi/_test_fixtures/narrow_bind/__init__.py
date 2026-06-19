# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Narrow-bind reproducer fixture.

Exposes a catalog whose virtual table advertises *more* columns in its
listing (``catalog_schema_contents_tables`` / ``catalog_table_get``) than
its scan function returns from ``on_bind``. A client that trusts the bind
``output_schema`` without checking it against the planned catalog columns
indexes past the end of the worker's narrower batch in
``ArrowTableFunction::ArrowToDuckDB`` and SIGSEGVs. The fix makes the
client fail closed at bind with a clear ``BinderException``.

Driven by ``test/sql/integration/narrow_bind_mismatch.test`` in
``~/Development/vgi``.
"""
