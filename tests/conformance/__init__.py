"""Conformance tests that mirror the C++ DuckDB integration suite.

These tests drive the Python ``Client`` against the example worker across every
feature area exercised by ``vgi/test/sql/integration/``. The goal is drift
detection: if the worker or ``VgiProtocol`` grows a capability that the Python
client doesn't expose, a test here fails.
"""
