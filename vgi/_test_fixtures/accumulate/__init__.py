# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Accumulate fixture worker: persistent named row collections over BoundStorage.

Exposes ``accumulate`` / ``accumulate_read`` / ``accumulate_clear`` table
functions that persist rows through the framework's ``FunctionStorage``,
scoped per ATTACH session. The fixture is the end-to-end exercise of the
``BoundStorage`` interfaces: persistent attach-scoped K/V segments (including
ranged scans/deletes for TTL eviction), atomic counters, and execution-scoped
staging logs.

Hosted inside the consolidated ``vgi-fixture-worker`` alongside the other
reproducer catalogs; driven by ``test/sql/integration/accumulate/*.test`` in
``~/Development/vgi`` and mirrored by ``tests/conformance/test_accumulate.py``.
"""

from vgi._test_fixtures.accumulate.worker import AccumulateWorker

__all__ = ["AccumulateWorker"]
