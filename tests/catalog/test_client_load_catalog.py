# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""``Client.load_catalog``: whole-catalog enumeration honours ``supports_catalog_contents``.

Runs the client against the ``contents_*`` fixture catalogs over every client
transport and asserts which catalog RPCs it actually sent: one
``catalog_contents`` when advertised, the per-schema RPCs when not advertised or
when ``catalog_contents`` fails, and ``if_none_match`` revalidation.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from vgi._test_fixtures.catalog_contents import (
    BROKEN_MESSAGE,
    CATALOG_BROKEN,
    CATALOG_HASH,
    CATALOG_LEGACY,
    CATALOG_PROBE,
    CATALOG_REVAL,
)
from vgi.catalog import (
    AttachOpaqueData,
    CatalogAttachResult,
    SchemaContentsInfo,
    SchemaInfo,
    TransactionOpaqueData,
)
from vgi.client import CatalogClientMixin, CatalogContents, CatalogSnapshot, catalog_mixin
from vgi.client.catalog_mixin import CatalogClientError

_KINDS = (
    "tables",
    "views",
    "scalar_functions",
    "aggregate_functions",
    "table_functions",
    "scalar_macros",
    "table_macros",
    "indexes",
)

Call = tuple[str, str | None]


class _RecordingProxy:
    """Wraps a VgiProtocol proxy, recording each ``catalog_*`` method sent."""

    def __init__(self, proxy: Any, calls: list[Call]) -> None:
        self._proxy = proxy
        self._calls = calls

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._proxy, name)
        if not name.startswith("catalog_") or not callable(attr):
            return attr

        def call(*args: Any, **kwargs: Any) -> Any:
            self._calls.append((name, kwargs.get("if_none_match")))
            return attr(*args, **kwargs)

        return call


def _record(client: Any) -> list[Call]:
    """Make ``client`` record every catalog RPC it sends; returns the live list."""
    calls: list[Call] = []
    original = client._catalog_connect

    @contextmanager
    def recording() -> Iterator[Any]:
        with original() as proxy:
            yield _RecordingProxy(proxy, calls)

    client._catalog_connect = recording
    return calls


def _attach(client: Any, name: str) -> CatalogAttachResult:
    return client.catalog_attach(name=name, data_version_spec=None, implementation_version=None)  # type: ignore[no-any-return]


def _names(entry: SchemaContentsInfo) -> dict[str, list[str]]:
    return {kind: sorted(item.name for item in getattr(entry, kind)) for kind in _KINDS}


# ---------------------------------------------------------------------------
# Against the fixture catalogs, over every client transport
# ---------------------------------------------------------------------------


def test_advertised_uses_one_catalog_contents_call(client_transport: Any) -> None:
    """contents_probe: one ``catalog_contents``, every kind decoded, equal to the per-schema answer."""
    with client_transport() as client:
        attach = _attach(client, CATALOG_PROBE)
        assert attach.supports_catalog_contents
        calls = _record(client)
        snapshot = client.load_catalog(attach=attach)
        assert calls == [("catalog_contents", None)]
        assert snapshot.source == "catalog_contents"
        assert snapshot.fallback_reason is None
        assert snapshot.etag is None
        assert [list(s.schema.path) for s in snapshot.schemas] == [["main"], ["extra"]]
        main = _names(snapshot.schemas[0])
        assert main["tables"] == ["ten"]
        assert main["views"] == ["answer"]
        assert "double" in main["scalar_functions"]
        assert main["aggregate_functions"]
        assert main["table_functions"]
        assert main["scalar_macros"] == ["contents_triple"]
        assert main["table_macros"] == ["contents_range"]
        assert _names(snapshot.schemas[1])["tables"] == ["five"]

        # The one-call answer decodes to exactly what the per-schema RPCs return.
        calls.clear()
        per_schema = client._load_catalog_per_schema(attach.attach_opaque_data, None, None)
        assert "catalog_contents" not in [name for name, _ in calls]
        assert snapshot.schemas == per_schema.schemas


def test_failing_catalog_contents_falls_back_to_per_schema(client_transport: Any) -> None:
    """contents_broken: ``catalog_contents`` fails, the per-schema RPCs answer instead."""
    with client_transport() as client:
        attach = _attach(client, CATALOG_BROKEN)
        assert attach.supports_catalog_contents
        calls = _record(client)
        snapshot = client.load_catalog(attach=attach)
        names = [name for name, _ in calls]
        assert names[:2] == ["catalog_contents", "catalog_schemas"]
        assert names.count("catalog_contents") == 1
        assert {n for n in names[2:]} <= {
            "catalog_schema_contents_tables",
            "catalog_schema_contents_views",
            "catalog_schema_contents_functions",
            "catalog_schema_contents_macros",
            "catalog_schema_contents_indexes",
        }
        assert "catalog_schema_contents_tables" in names
        assert snapshot.source == "per_schema"
        assert snapshot.fallback_reason is not None and BROKEN_MESSAGE in snapshot.fallback_reason
        assert [list(s.schema.path) for s in snapshot.schemas] == [["main"], ["extra"]]
        assert _names(snapshot.schemas[0])["views"] == ["answer"]


def test_not_advertised_never_sends_catalog_contents(client_transport: Any) -> None:
    """contents_legacy: no ``catalog_contents`` on the wire — not to load, not to revalidate."""
    with client_transport() as client:
        attach = _attach(client, CATALOG_LEGACY)
        assert not attach.supports_catalog_contents
        calls = _record(client)
        first = client.load_catalog(attach=attach)
        second = client.load_catalog(attach=attach, previous=first)
        names = [name for name, _ in calls]
        assert "catalog_contents" not in names
        assert names[0] == "catalog_schemas"
        assert first.source == second.source == "per_schema"
        assert first.fallback_reason is None
        assert [list(s.schema.path) for s in first.schemas] == [["main"], ["extra"]]
        assert first.schemas == second.schemas


@pytest.mark.parametrize("catalog", [CATALOG_REVAL, CATALOG_HASH])
def test_revalidation_with_if_none_match(client_transport: Any, catalog: str) -> None:
    """contents_reval / contents_hash: ``if_none_match`` → ``not_modified`` keeps; a change replaces."""
    with client_transport() as client:
        attach = _attach(client, catalog)
        try:
            calls = _record(client)
            first = client.load_catalog(attach=attach)
            assert calls == [("catalog_contents", None)]
            assert first.etag is not None
            if catalog == CATALOG_REVAL:
                assert first.etag == "gen-1"
            assert [list(s.schema.path) for s in first.schemas] == [["main"]]

            calls.clear()
            hit = client.load_catalog(attach=attach, previous=first)
            assert calls == [("catalog_contents", first.etag)]
            assert hit.not_modified
            assert hit.source == "catalog_contents"
            assert hit.schemas == first.schemas
            assert hit.etag == first.etag

            client.schema_create(attach_opaque_data=attach.attach_opaque_data, path=["added"])
            calls.clear()
            changed = client.load_catalog(attach=attach, previous=hit)
            assert calls == [("catalog_contents", first.etag)]
            assert not changed.not_modified
            assert changed.etag not in (None, first.etag)
            if catalog == CATALOG_REVAL:
                assert changed.etag == "gen-2"
            assert sorted(list(s.schema.path) for s in changed.schemas) == [["added"], ["main"]]
        finally:
            client.catalog_detach(attach_opaque_data=attach.attach_opaque_data)


# ---------------------------------------------------------------------------
# Rules that need a misbehaving or transactional worker (stubbed)
# ---------------------------------------------------------------------------


_ATTACH = AttachOpaqueData(b"a" * 16)


def _schema(name: str) -> SchemaContentsInfo:
    return SchemaContentsInfo(schema=SchemaInfo(attach_opaque_data=_ATTACH, path=[name], comment=None, tags={}))


class _StubClient(CatalogClientMixin):
    """Answers ``contents`` from a script and the per-schema RPCs with one empty schema."""

    def __init__(self, answers: list[CatalogContents | Exception]) -> None:
        self.answers = answers
        self.calls: list[Call] = []

    def contents(self, *, attach_opaque_data: AttachOpaqueData, if_none_match: str | None = None) -> CatalogContents:
        self.calls.append(("catalog_contents", if_none_match))
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    def schemas(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None = None
    ) -> list[SchemaInfo]:
        self.calls.append(("catalog_schemas", None))
        return [
            SchemaInfo(
                attach_opaque_data=_ATTACH,
                path=["fallback"],
                comment=None,
                tags={},
                estimated_object_count={"table": 0},
            )
        ]

    def schema_contents(self, **kwargs: Any) -> list[Any]:
        self.calls.append((f"schema_contents:{kwargs['type'].value}", None))
        return []


def _attach_result(supports: bool) -> CatalogAttachResult:
    return CatalogAttachResult(
        attach_opaque_data=_ATTACH,
        supports_transactions=True,
        supports_time_travel=False,
        catalog_version_frozen=False,
        catalog_version=1,
        attach_opaque_data_required=False,
        default_schema="main",
        supports_catalog_contents=supports,
        resolved_data_version=None,
        resolved_implementation_version=None,
    )


def test_transactional_load_uses_per_schema_rpcs() -> None:
    """``catalog_contents`` returns only the committed catalog, so a transaction never uses it."""
    client = _StubClient([])
    snapshot = client.load_catalog(attach=_attach_result(True), transaction_opaque_data=TransactionOpaqueData(b"t"))
    assert client.calls[0] == ("catalog_schemas", None)
    assert "catalog_contents" not in [name for name, _ in client.calls]
    assert snapshot.source == "per_schema" and snapshot.fallback_reason is None


def test_per_schema_skips_kinds_counted_as_zero() -> None:
    """``estimated_object_count`` 0 is a hard "none": that kind's RPC is skipped."""
    client = _StubClient([])
    client.load_catalog(attach=_attach_result(False))
    names = [name for name, _ in client.calls]
    assert "schema_contents:table" not in names
    assert "schema_contents:view" in names
    assert "schema_contents:index" in names


def test_unsolicited_not_modified_falls_back() -> None:
    """``not_modified`` to a request that sent no ``if_none_match`` is a protocol violation."""
    client = _StubClient([CatalogContents(catalog_version=1, etag="e", not_modified=True, schemas=[])])
    snapshot = client.load_catalog(attach=_attach_result(True))
    assert snapshot.source == "per_schema"
    assert snapshot.fallback_reason is not None and "not_modified" in snapshot.fallback_reason


def test_older_snapshot_is_retried_once_then_falls_back() -> None:
    """A snapshot older than the one held (a lagging replica) is retried, then per-schema."""
    previous = CatalogSnapshot(schemas=[_schema("main")], catalog_version=5, etag=None, source="catalog_contents")
    stale = CatalogContents(catalog_version=3, etag=None, not_modified=False, schemas=[_schema("old")])
    client = _StubClient([stale, stale])
    snapshot = client.load_catalog(attach=_attach_result(True), previous=previous)
    assert [name for name, _ in client.calls][:3] == ["catalog_contents", "catalog_contents", "catalog_schemas"]
    assert snapshot.source == "per_schema"
    assert snapshot.fallback_reason is not None and "older" in snapshot.fallback_reason

    fresh = CatalogContents(catalog_version=6, etag=None, not_modified=False, schemas=[_schema("new")])
    client = _StubClient([stale, fresh])
    snapshot = client.load_catalog(attach=_attach_result(True), previous=previous)
    assert snapshot.source == "catalog_contents" and snapshot.catalog_version == 6


def test_version_zero_is_never_older() -> None:
    """Version 0 means "unknown" and is accepted as is."""
    previous = CatalogSnapshot(schemas=[], catalog_version=5, etag=None, source="catalog_contents")
    client = _StubClient([CatalogContents(catalog_version=0, etag=None, not_modified=False, schemas=[_schema("x")])])
    snapshot = client.load_catalog(attach=_attach_result(True), previous=previous)
    assert snapshot.source == "catalog_contents" and client.calls == [("catalog_contents", None)]


def test_per_schema_snapshot_is_not_revalidated() -> None:
    """Only a ``catalog_contents`` snapshot's etag is sent back."""
    previous = CatalogSnapshot(schemas=[], catalog_version=None, etag="x", source="per_schema")
    client = _StubClient([CatalogContents(catalog_version=1, etag="e", not_modified=False, schemas=[])])
    client.load_catalog(attach=_attach_result(True), previous=previous)
    assert client.calls == [("catalog_contents", None)]


def test_error_on_revalidation_falls_back() -> None:
    """A failing conditional call reloads through the per-schema RPCs."""
    previous = CatalogSnapshot(schemas=[], catalog_version=1, etag="e", source="catalog_contents")
    client = _StubClient([CatalogClientError("boom")])
    snapshot = client.load_catalog(attach=_attach_result(True), previous=previous)
    assert client.calls[0] == ("catalog_contents", "e")
    assert snapshot.source == "per_schema" and snapshot.fallback_reason == "boom"


def test_absent_supports_catalog_contents_decodes_false() -> None:
    """An attach result from a worker that predates the field reads as not advertised."""
    batch = _attach_result(True)._serialize()
    assert CatalogAttachResult.deserialize_from_batch(batch).supports_catalog_contents
    old = batch.drop_columns(["supports_catalog_contents"])
    assert not CatalogAttachResult.deserialize_from_batch(old).supports_catalog_contents


class _UndecodableContentsProxy:
    """A worker proxy whose ``catalog_contents`` item bytes are not a valid ``SchemaInfo``."""

    def catalog_contents(self, **_: Any) -> Any:
        entry = SimpleNamespace(
            path=["main"],
            schema=b"not an arrow ipc stream",
            tables=[],
            views=[],
            scalar_functions=[],
            aggregate_functions=[],
            table_functions=[],
            scalar_macros=[],
            table_macros=[],
            indexes=[],
        )
        return SimpleNamespace(catalog_version=1, etag=None, not_modified=False, schemas=[entry])

    def catalog_schemas(self, **_: Any) -> Any:
        return SimpleNamespace(to_infos=list)


@pytest.fixture
def undecodable_contents_client(monkeypatch: pytest.MonkeyPatch) -> CatalogClientMixin:
    """A client whose real ``_catalog_connect`` hands out `_UndecodableContentsProxy`."""

    @contextmanager
    def connect(*_: Any, **__: Any) -> Iterator[Any]:
        yield _UndecodableContentsProxy()

    monkeypatch.setattr(catalog_mixin._catalog_pool, "connect", connect)

    class _Client(CatalogClientMixin):
        def _worker_argv(self) -> list[str]:
            return ["unused"]

    return _Client()


def test_undecodable_contents_raises_catalog_client_error(undecodable_contents_client: CatalogClientMixin) -> None:
    """A decode failure surfaces as ``CatalogClientError``, like every other catalog call."""
    with pytest.raises(CatalogClientError):
        undecodable_contents_client.contents(attach_opaque_data=_ATTACH)


def test_undecodable_contents_falls_back_to_per_schema(undecodable_contents_client: CatalogClientMixin) -> None:
    """``load_catalog`` treats an undecodable answer as a failure and uses the per-schema RPCs."""
    snapshot = undecodable_contents_client.load_catalog(attach=_attach_result(True))
    assert snapshot.source == "per_schema"
    assert snapshot.fallback_reason
