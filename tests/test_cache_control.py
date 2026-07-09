# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Unit tests for CacheControl rendering and the _merge_cache_control emit path."""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi.cache_control import (
    CACHE_ETAG_KEY,
    CACHE_EXPIRES_KEY,
    CACHE_LAST_MODIFIED_KEY,
    CACHE_NO_STORE_KEY,
    CACHE_REVALIDATABLE_KEY,
    CACHE_SCOPE_CATALOG,
    CACHE_SCOPE_KEY,
    CACHE_SCOPE_TRANSACTION,
    CACHE_STALE_IF_ERROR_KEY,
    CACHE_STALE_WHILE_REVALIDATE_KEY,
    CACHE_TTL_KEY,
    CacheControl,
)
from vgi.protocol import _TrackingOutputCollector


class _RecordingCollector:
    """Minimal inner OutputCollector that records (batch, metadata) per emit."""

    def __init__(self) -> None:
        """Initialize the recorded-calls list."""
        self.calls: list[tuple[pa.RecordBatch, dict[str, str] | None]] = []

    def emit(self, batch: pa.RecordBatch, metadata: dict[str, str] | None = None) -> None:
        """Record the emitted batch and its metadata."""
        self.calls.append((batch, metadata))


def _batch(value: int = 0) -> pa.RecordBatch:
    """Build a one-row single-column batch for emit testing."""
    return pa.RecordBatch.from_pydict({"n": [value]})


class TestCacheControlRendering:
    """CacheControl.to_metadata() renders the vgi.cache.* key/value dict."""

    def test_ttl_only(self) -> None:
        """ttl-only advertisement renders ttl + the always-present scope."""
        md = CacheControl(ttl=300).to_metadata()
        assert md[CACHE_TTL_KEY] == "300"
        # scope is always emitted so the client never infers the default.
        assert md[CACHE_SCOPE_KEY] == CACHE_SCOPE_CATALOG
        # Nothing else set.
        assert set(md) == {CACHE_TTL_KEY, CACHE_SCOPE_KEY}

    def test_all_fields(self) -> None:
        """Every field renders to its documented vgi.cache.* key."""
        md = CacheControl(
            ttl=60,
            expires="2026-01-01T00:00:00Z",
            scope=CACHE_SCOPE_TRANSACTION,
            etag='"abc"',
            last_modified="2025-12-31T00:00:00Z",
            revalidatable=True,
            stale_while_revalidate=10,
            stale_if_error=20,
        ).to_metadata()
        assert md == {
            CACHE_TTL_KEY: "60",
            CACHE_EXPIRES_KEY: "2026-01-01T00:00:00Z",
            CACHE_SCOPE_KEY: CACHE_SCOPE_TRANSACTION,
            CACHE_ETAG_KEY: '"abc"',
            CACHE_LAST_MODIFIED_KEY: "2025-12-31T00:00:00Z",
            CACHE_REVALIDATABLE_KEY: "1",
            CACHE_STALE_WHILE_REVALIDATE_KEY: "10",
            CACHE_STALE_IF_ERROR_KEY: "20",
        }

    def test_no_store_renders_one_and_omits_when_false(self) -> None:
        """no_store renders "1" when true and is omitted when false."""
        assert CacheControl(no_store=True).to_metadata()[CACHE_NO_STORE_KEY] == "1"
        assert CACHE_NO_STORE_KEY not in CacheControl(ttl=1).to_metadata()

    def test_booleans_omitted_when_false(self) -> None:
        """Boolean fields left false do not emit any key."""
        md = CacheControl(ttl=1).to_metadata()
        assert CACHE_REVALIDATABLE_KEY not in md
        assert CACHE_NO_STORE_KEY not in md

    def test_invalid_scope_rejected(self) -> None:
        """An unknown scope value is rejected at construction."""
        with pytest.raises(ValueError, match="scope"):
            CacheControl(scope="bogus")

    def test_negative_duration_rejected(self) -> None:
        """A negative duration field is rejected at construction."""
        with pytest.raises(ValueError, match="ttl"):
            CacheControl(ttl=-1)

    def test_not_modified_renders_one_and_omits_when_false(self) -> None:
        """not_modified renders "1" when true (a 304) and is omitted when false."""
        from vgi.cache_control import CACHE_NOT_MODIFIED_KEY

        md = CacheControl(not_modified=True, ttl=0, etag='"v1"', revalidatable=True).to_metadata()
        assert md[CACHE_NOT_MODIFIED_KEY] == "1"
        assert CACHE_NOT_MODIFIED_KEY not in CacheControl(ttl=1).to_metadata()


class TestMergeCacheControlEmitPath:
    """_TrackingOutputCollector folds CacheControl onto the FIRST emitted batch."""

    def test_first_batch_carries_keys(self) -> None:
        """The first emitted batch carries the rendered vgi.cache.* keys."""
        inner = _RecordingCollector()
        out = _TrackingOutputCollector(inner)  # type: ignore[arg-type]
        out.emit(_batch(0), cache_control=CacheControl(ttl=300))

        (_, metadata) = inner.calls[0]
        assert metadata is not None
        assert metadata[CACHE_TTL_KEY] == "300"
        assert metadata[CACHE_SCOPE_KEY] == CACHE_SCOPE_CATALOG

    def test_subsequent_batches_get_no_cache_keys(self) -> None:
        """Only the first batch carries cache keys; later batches carry none."""
        inner = _RecordingCollector()
        out = _TrackingOutputCollector(inner)  # type: ignore[arg-type]
        out.emit(_batch(0), cache_control=CacheControl(ttl=300))
        out.emit(_batch(1))
        out.emit(_batch(2))

        # First batch carries keys; later batches carry no metadata at all
        # (so no duplicated/conflicting vgi.cache.* keys downstream).
        assert inner.calls[0][1] is not None
        assert inner.calls[0][1][CACHE_TTL_KEY] == "300"
        assert inner.calls[1][1] is None
        assert inner.calls[2][1] is None

    def test_cache_control_on_later_batch_raises(self) -> None:
        """Passing cache_control after the first batch is a programming error."""
        inner = _RecordingCollector()
        out = _TrackingOutputCollector(inner)  # type: ignore[arg-type]
        out.emit(_batch(0))
        with pytest.raises(RuntimeError, match="first emitted batch"):
            out.emit(_batch(1), cache_control=CacheControl(ttl=300))

    def test_no_cache_control_leaves_metadata_untouched(self) -> None:
        """Without cache_control the emitted metadata stays None."""
        inner = _RecordingCollector()
        out = _TrackingOutputCollector(inner)  # type: ignore[arg-type]
        out.emit(_batch(0))
        assert inner.calls[0][1] is None

    def test_explicit_metadata_preserved_alongside_cache_control(self) -> None:
        """Explicit metadata keys survive alongside the cache_control keys."""
        inner = _RecordingCollector()
        out = _TrackingOutputCollector(inner)  # type: ignore[arg-type]
        out.emit(_batch(0), metadata={"custom.key": "v"}, cache_control=CacheControl(ttl=5))

        (_, metadata) = inner.calls[0]
        assert metadata is not None
        assert metadata["custom.key"] == "v"
        assert metadata[CACHE_TTL_KEY] == "5"

    def test_no_store_flows_through_emit(self) -> None:
        """A no_store advertisement reaches the emitted batch metadata."""
        inner = _RecordingCollector()
        out = _TrackingOutputCollector(inner)  # type: ignore[arg-type]
        out.emit(_batch(0), cache_control=CacheControl(no_store=True))

        (_, metadata) = inner.calls[0]
        assert metadata is not None
        assert metadata[CACHE_NO_STORE_KEY] == "1"


class TestRevalidatableFixture:
    """CacheRevalidatableFunction answers 304 when if_none_match matches (M6)."""

    @staticmethod
    def _run(if_none_match: str | None):
        """Drive the fixture's process() with a duck-typed params stub."""
        import types

        from vgi._test_fixtures.table.cache import (
            CacheRevalidatableFunction,
            _CacheNonceState,
        )
        from vgi.cache_control import CACHE_NOT_MODIFIED_KEY

        inner = _RecordingCollector()
        # The fixture emits via VgiOutputCollector.emit(..., cache_control=...);
        # _TrackingOutputCollector folds cache_control into the recorded metadata.
        out = _TrackingOutputCollector(inner)  # type: ignore[arg-type]
        params = types.SimpleNamespace(
            if_none_match=if_none_match,
            output_schema=CacheRevalidatableFunction.FIXED_SCHEMA,
        )
        state = _CacheNonceState(nonce=42)
        CacheRevalidatableFunction.process(params, state, out)  # type: ignore[arg-type]
        return inner, state, CACHE_NOT_MODIFIED_KEY

    def test_matching_validator_yields_not_modified(self) -> None:
        """A matching if_none_match yields a 0-row not_modified batch (no data)."""
        from vgi._test_fixtures.table.cache import CacheRevalidatableFunction

        collector, _state, not_modified_key = self._run(if_none_match=CacheRevalidatableFunction.ETAG)

        assert len(collector.calls) == 1
        batch, metadata = collector.calls[0]
        assert batch.num_rows == 0  # 304: no data re-streamed
        assert metadata is not None
        assert metadata[not_modified_key] == "1"

    def test_no_validator_yields_fresh_data(self) -> None:
        """No conditional request → the fixture emits its nonce row (fresh)."""
        collector, state, not_modified_key = self._run(if_none_match=None)

        assert len(collector.calls) == 1
        batch, metadata = collector.calls[0]
        assert batch.num_rows == 1
        assert batch.column(0)[0].as_py() == state.nonce
        assert metadata is not None
        assert not_modified_key not in metadata
