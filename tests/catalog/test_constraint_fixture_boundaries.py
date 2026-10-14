# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Writable reconciliation facts require actual value checks before storage."""

from datetime import UTC, datetime

import pytest

from vgi._test_fixtures.schema_reconcile.worker import TABLES, _all_rows, _insert_row, _update_row


def test_reconciliation_storage_rejects_nested_null_without_publishing(tmp_path, monkeypatch):
    """All write entry points enforce required values, including nested leaves."""
    monkeypatch.setenv("VGI_SCHEMA_RECONCILE_DB", str(tmp_path / "reconcile.sqlite"))
    spec = TABLES["demo"]
    payload = {"id": 1, "ts": datetime(2026, 1, 1, tzinfo=UTC), "nested": {"a": 2}, "tags": []}
    _insert_row(spec, 1, payload)
    original = _all_rows(spec)
    with pytest.raises(ValueError, match="NULL|null"):
        _insert_row(spec, 2, {**payload, "nested": {"a": None}})
    with pytest.raises(ValueError, match="NULL|null"):
        _update_row(spec, 1, {"nested": {"a": None}})
    assert _all_rows(spec) == original
