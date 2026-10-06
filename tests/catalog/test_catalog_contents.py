# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""``catalog_contents``: wire shape, revalidation (etag / not_modified), the content-hash etag and the worker cache."""

from __future__ import annotations

import re
from typing import Any

import pytest

from vgi._test_fixtures.catalog import InMemoryCatalog, SchemaData
from vgi._test_fixtures.catalog_contents import (
    CATALOG_HASH,
    CATALOG_MEMORY,
    CATALOG_PROBE,
    CATALOG_REVAL,
    CONTENTS_WORKERS,
    ContentsHashWorker,
    ContentsMemoryWorker,
    ContentsProbeCatalog,
    ContentsProbeWorker,
    ContentsRevalWorker,
)
from vgi._test_fixtures.worker import ExampleWorker
from vgi.catalog import (
    AttachOpaqueData,
    CatalogContentsResult,
    CatalogInterface,
    ReadOnlyCatalogInterface,
    SchemaContentsInfo,
    SchemaInfo,
    SchemaObjectType,
)
from vgi.meta_worker import MetaWorker
from vgi.protocol import CatalogAttachRequest, CatalogContentsResponse, SchemaContents
from vgi.worker import Worker, catalog_contents_digest

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


def _attach(worker: Any, name: str) -> bytes:
    request = CatalogAttachRequest(name=name, options=None, data_version_spec=None, implementation_version=None)
    return bytes(worker.catalog_attach(request).attach_opaque_data)


def _per_schema_items(worker: Worker, attach: bytes, path: list[str]) -> dict[str, list[bytes]]:
    """What the per-schema RPCs return for every kind."""
    fn = worker.catalog_schema_contents_functions
    macros = worker.catalog_schema_contents_macros
    return {
        "tables": worker.catalog_schema_contents_tables(attach, path).items,
        "views": worker.catalog_schema_contents_views(attach, path).items,
        "scalar_functions": fn(attach, path, SchemaObjectType.SCALAR_FUNCTION).items,
        "aggregate_functions": fn(attach, path, SchemaObjectType.AGGREGATE_FUNCTION).items,
        "table_functions": fn(attach, path, SchemaObjectType.TABLE_FUNCTION).items,
        "scalar_macros": macros(attach, path, SchemaObjectType.SCALAR_MACRO).items,
        "table_macros": macros(attach, path, SchemaObjectType.TABLE_MACRO).items,
        "indexes": worker.catalog_schema_contents_indexes(attach, path).items,
    }


def _roundtrip(response: CatalogContentsResponse) -> CatalogContentsResponse:
    """Decode the response from the bytes the RPC layer would put on the wire."""
    return CatalogContentsResponse.deserialize_from_bytes(response.serialize_to_bytes())


# ---------------------------------------------------------------------------
# Wire shape
# ---------------------------------------------------------------------------


class TestShape:
    """The typed response: a struct per schema, items byte-identical to the per-schema RPCs."""

    def test_schemas_is_a_list_of_structs(self) -> None:
        """``schemas`` is ``list<struct>`` with ``path`` first, not a list of IPC blobs."""
        field = CatalogContentsResponse.ARROW_SCHEMA.field("schemas")
        assert field.type.value_type.num_fields == 10
        assert [f.name for f in field.type.value_type] == ["path", "schema", *_KINDS]
        assert CatalogContentsResponse.ARROW_SCHEMA.names == ["catalog_version", "etag", "not_modified", "schemas"]

    def test_items_match_the_per_schema_rpcs(self) -> None:
        """Every kind of every schema is byte-identical to its per-schema RPC answer."""
        worker = ContentsProbeWorker()
        attach = _attach(worker, CATALOG_PROBE)
        response = _roundtrip(worker.catalog_contents(attach))
        assert response.etag is None
        assert not response.not_modified
        assert [entry.path for entry in response.schemas] == [["main"], ["extra"]]
        schema_items = worker.catalog_schemas(attach).items
        assert [entry.schema for entry in response.schemas] == schema_items
        for entry in response.schemas:
            assert SchemaInfo.deserialize_from_bytes(entry.schema).path == entry.path
            expected = _per_schema_items(worker, attach, entry.path)
            for kind in _KINDS:
                assert getattr(entry, kind) == expected[kind], (entry.path, kind)
        main = response.schemas[0]
        # Every seeded kind is present in the fixture.
        for kind in _KINDS[:-1]:
            assert getattr(main, kind), kind

    def test_parents_first_and_path_from_schema_info(self) -> None:
        """Children returned first are reordered after their parents; ``path`` is SchemaInfo.path."""

        class Nested(ContentsProbeCatalog):
            def catalog_contents(self, *, attach_opaque_data: Any, if_none_match: str | None = None) -> Any:
                def info(path: list[str]) -> SchemaContentsInfo:
                    return SchemaContentsInfo(
                        schema=SchemaInfo(attach_opaque_data=attach_opaque_data, path=path, comment=None, tags={})
                    )

                return CatalogContentsResult(schemas=[info(["a", "b", "c"]), info(["a", "b"]), info(["a"])])

        class NestedWorker(ContentsProbeWorker):
            catalog_interface = Nested

        worker = NestedWorker()
        response = _roundtrip(worker.catalog_contents(_attach(worker, CATALOG_PROBE)))
        assert [entry.path for entry in response.schemas] == [["a"], ["a", "b"], ["a", "b", "c"]]
        for entry in response.schemas:
            assert SchemaInfo.deserialize_from_bytes(entry.schema).path == entry.path

    @pytest.mark.parametrize(
        ("paths", "message"),
        [
            ([["a"], ["a"]], "duplicate schema paths"),
            ([["a", "b"]], "without its parent"),
        ],
    )
    def test_invalid_paths_are_rejected(self, paths: list[list[str]], message: str) -> None:
        """Duplicate paths and orphans fail at the wire boundary."""

        class Bad(ContentsProbeCatalog):
            catalog_contents_attach_independent = False

            def catalog_contents(self, *, attach_opaque_data: Any, if_none_match: str | None = None) -> Any:
                return CatalogContentsResult(
                    schemas=[
                        SchemaContentsInfo(
                            schema=SchemaInfo(attach_opaque_data=attach_opaque_data, path=p, comment=None, tags={})
                        )
                        for p in paths
                    ]
                )

        class BadWorker(ContentsProbeWorker):
            catalog_interface = Bad

        worker = BadWorker()
        with pytest.raises(ValueError, match=message):
            worker.catalog_contents(_attach(worker, CATALOG_PROBE))


# ---------------------------------------------------------------------------
# Revalidation
# ---------------------------------------------------------------------------


def _forbid_build(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make any attempt to build a snapshot fail the test."""

    def boom(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("catalog_contents built the snapshot")

    monkeypatch.setattr(InMemoryCatalog, "schemas", boom)
    monkeypatch.setattr(InMemoryCatalog, "schema_contents", boom)


class TestRevalidation:
    """etag / if_none_match / not_modified."""

    def test_cheap_validator_short_circuits_without_building(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A matching generation-counter etag answers not_modified before any build."""
        worker = ContentsRevalWorker()
        attach = _attach(worker, CATALOG_REVAL)
        full = worker.catalog_contents(attach)
        assert full.etag == "gen-1"
        assert not full.not_modified
        assert [entry.path for entry in full.schemas] == [["main"]]

        with monkeypatch.context() as m:
            _forbid_build(m)
            hit = _roundtrip(worker.catalog_contents(attach, if_none_match="gen-1"))
        assert hit.not_modified
        assert hit.etag == "gen-1"
        assert hit.schemas == []
        assert hit.catalog_version == 1

        # A different etag gets the full contents.
        miss = worker.catalog_contents(attach, if_none_match="gen-0")
        assert not miss.not_modified
        assert miss.etag == "gen-1"
        assert miss.schemas == full.schemas

    def test_ddl_changes_the_validator(self) -> None:
        """After a change the old etag no longer matches."""
        worker = ContentsRevalWorker()
        attach = _attach(worker, CATALOG_REVAL)
        cat = worker._get_catalog()
        assert isinstance(cat, InMemoryCatalog)
        plain = worker._unwrap_attach(attach)
        _add_schema(cat, plain, "other")
        cat._increment_version(plain)
        response = worker.catalog_contents(attach, if_none_match="gen-1")
        assert not response.not_modified
        assert response.etag == "gen-2"
        assert response.catalog_version == 2
        assert [entry.path for entry in response.schemas] == [["main"], ["other"]]

    def test_no_etag_ignores_if_none_match(self) -> None:
        """A catalog without an etag always answers in full, with etag null."""
        worker = ContentsMemoryWorker()
        attach = _attach(worker, CATALOG_MEMORY)
        response = worker.catalog_contents(attach, if_none_match="anything")
        assert response.etag is None
        assert not response.not_modified
        assert [entry.path for entry in response.schemas] == [["main"]]
        # contents_memory reports version 0: exercises the client's version-0 rule.
        assert response.catalog_version == 0

    def test_etag_equal_to_if_none_match_becomes_not_modified(self) -> None:
        """A catalog that built anyway but returned the matching etag still yields not_modified."""

        class Built(ContentsProbeCatalog):
            catalog_contents_attach_independent = False

            def catalog_contents(self, *, attach_opaque_data: Any, if_none_match: str | None = None) -> Any:
                built = ReadOnlyCatalogInterface.catalog_contents(self, attach_opaque_data=attach_opaque_data)
                return CatalogContentsResult(schemas=built.schemas, etag="v7")

        class BuiltWorker(ContentsProbeWorker):
            catalog_interface = Built

        worker = BuiltWorker()
        attach = _attach(worker, CATALOG_PROBE)
        response = worker.catalog_contents(attach, if_none_match="v7")
        assert response.not_modified
        assert response.schemas == []
        assert response.etag == "v7"

    @pytest.mark.parametrize(
        ("result", "if_none_match", "message"),
        [
            (CatalogContentsResult(not_modified=True), "x", "not_modified"),
            (CatalogContentsResult(etag="x", not_modified=True), None, "not_modified"),
            (CatalogContentsResult(etag="y", not_modified=True), "x", "not_modified"),
        ],
    )
    def test_invalid_not_modified_is_rejected(
        self, result: CatalogContentsResult, if_none_match: str | None, message: str
    ) -> None:
        """not_modified needs an etag equal to if_none_match."""

        class Liar(ContentsProbeCatalog):
            catalog_contents_attach_independent = False

            def catalog_contents(self, *, attach_opaque_data: Any, if_none_match: str | None = None) -> Any:
                return result

        class LiarWorker(ContentsProbeWorker):
            catalog_interface = Liar

        worker = LiarWorker()
        with pytest.raises(ValueError, match=message):
            worker.catalog_contents(_attach(worker, CATALOG_PROBE), if_none_match=if_none_match)

    def test_not_modified_with_schemas_is_rejected(self) -> None:
        """not_modified must come with no schemas."""

        class Liar(ContentsProbeCatalog):
            catalog_contents_attach_independent = False

            def catalog_contents(self, *, attach_opaque_data: Any, if_none_match: str | None = None) -> Any:
                schema = SchemaInfo(attach_opaque_data=attach_opaque_data, path=["main"], comment=None, tags={})
                return CatalogContentsResult(schemas=[SchemaContentsInfo(schema=schema)], etag="x", not_modified=True)

        class LiarWorker(ContentsProbeWorker):
            catalog_interface = Liar

        worker = LiarWorker()
        with pytest.raises(ValueError, match="with schemas"):
            worker.catalog_contents(_attach(worker, CATALOG_PROBE), if_none_match="x")


def _add_schema(cat: InMemoryCatalog, attach: AttachOpaqueData, name: str) -> None:
    data = cat._get_catalog(attach)
    data.schemas[(name,)] = SchemaData(
        info=SchemaInfo(attach_opaque_data=attach, path=[name], comment=None, tags={}),
    )


# ---------------------------------------------------------------------------
# Content-hash etag
# ---------------------------------------------------------------------------


class TestContentHash:
    """``catalog_contents_etag = "content-hash"``."""

    def test_hash_is_deterministic_and_revalidates(self) -> None:
        """Two builds hash alike; a match is not_modified; a change is not."""
        worker = ContentsHashWorker()
        attach = _attach(worker, CATALOG_HASH)
        first = worker.catalog_contents(attach)
        assert first.etag is not None
        assert re.fullmatch(r"[0-9a-f]{64}", first.etag)
        second = worker.catalog_contents(attach)
        assert second.etag == first.etag
        assert second.schemas == first.schemas
        assert catalog_contents_digest(first.schemas) == first.etag

        hit = worker.catalog_contents(attach, if_none_match=first.etag)
        assert hit.not_modified
        assert hit.schemas == []
        assert hit.etag == first.etag

        cat = worker._get_catalog()
        assert isinstance(cat, InMemoryCatalog)
        _add_schema(cat, worker._unwrap_attach(attach), "other")
        changed = worker.catalog_contents(attach, if_none_match=first.etag)
        assert not changed.not_modified
        assert changed.etag != first.etag
        assert [entry.path for entry in changed.schemas] == [["main"], ["other"]]

    def test_off_by_default(self) -> None:
        """Without the opt-in a catalog gets no etag."""
        assert CatalogInterface.catalog_contents_etag is None
        worker = ContentsMemoryWorker()
        assert worker.catalog_contents(_attach(worker, CATALOG_MEMORY)).etag is None

    def test_catalog_etag_wins_over_the_hash(self) -> None:
        """A catalog that returns its own etag keeps it under content-hash."""

        class Own(ContentsProbeCatalog):
            catalog_contents_etag = "content-hash"
            catalog_contents_attach_independent = False

            def catalog_contents(self, *, attach_opaque_data: Any, if_none_match: str | None = None) -> Any:
                built = ReadOnlyCatalogInterface.catalog_contents(self, attach_opaque_data=attach_opaque_data)
                return CatalogContentsResult(schemas=built.schemas, etag="mine")

        class OwnWorker(ContentsProbeWorker):
            catalog_interface = Own

        worker = OwnWorker()
        assert worker.catalog_contents(_attach(worker, CATALOG_PROBE)).etag == "mine"

    def test_digest_separates_fields(self) -> None:
        """Moving bytes between fields changes the digest."""
        a = [SchemaContents(path=["s"], schema=b"x", tables=[b"ab"])]
        b = [SchemaContents(path=["s"], schema=b"x", tables=[b"a", b"b"])]
        c = [SchemaContents(path=["s"], schema=b"x", views=[b"ab"])]
        assert len({catalog_contents_digest(a), catalog_contents_digest(b), catalog_contents_digest(c)}) == 3


# ---------------------------------------------------------------------------
# Worker-side cache
# ---------------------------------------------------------------------------


def _count_builds(monkeypatch: pytest.MonkeyPatch, cls: type[CatalogInterface]) -> list[int]:
    calls: list[int] = []
    original = cls.catalog_contents

    def counting(self: Any, **kwargs: Any) -> Any:
        calls.append(1)
        return original(self, **kwargs)

    monkeypatch.setattr(cls, "catalog_contents", counting)
    return calls


class TestCache:
    """Version-frozen, attach-independent catalogs build once per catalog version."""

    def test_second_call_does_not_rebuild(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Repeated calls, from different attaches, reuse one build and one encoding."""
        calls = _count_builds(monkeypatch, ContentsProbeCatalog)
        worker = ContentsProbeWorker()
        first = worker.catalog_contents(_attach(worker, CATALOG_PROBE))
        second = worker.catalog_contents(_attach(worker, CATALOG_PROBE))
        assert len(calls) == 1
        assert second is first
        assert second.serialize_to_bytes() is first.serialize_to_bytes()

    def test_cached_bytes_equal_a_fresh_encoding(self) -> None:
        """The pre-serialized response is byte-identical to encoding it normally."""
        worker = ContentsProbeWorker()
        cached = worker.catalog_contents(_attach(worker, CATALOG_PROBE))
        fresh = CatalogContentsResponse(
            catalog_version=cached.catalog_version,
            etag=cached.etag,
            not_modified=cached.not_modified,
            schemas=list(cached.schemas),
        )
        assert cached.serialize_to_bytes() == fresh.serialize_to_bytes()
        assert _roundtrip(cached) == fresh

    def test_new_catalog_version_rebuilds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A different catalog_version is a cache miss."""
        calls = _count_builds(monkeypatch, ContentsProbeCatalog)
        version = [1]
        monkeypatch.setattr(ContentsProbeCatalog, "catalog_version", lambda self, **kw: version[0])
        worker = ContentsProbeWorker()
        attach = _attach(worker, CATALOG_PROBE)
        assert worker.catalog_contents(attach).catalog_version == 1
        worker.catalog_contents(attach)
        version[0] = 2
        assert worker.catalog_contents(attach).catalog_version == 2
        worker.catalog_contents(attach)
        assert len(calls) == 2

    def test_cache_serves_conditional_requests(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A cached content-hash catalog answers a matching if_none_match with not_modified."""

        class Hashed(ContentsProbeCatalog):
            catalog_contents_etag = "content-hash"

        class HashedWorker(ContentsProbeWorker):
            catalog_interface = Hashed

        calls = _count_builds(monkeypatch, Hashed)
        worker = HashedWorker()
        attach = _attach(worker, CATALOG_PROBE)
        full = worker.catalog_contents(attach)
        assert full.etag is not None
        hit = worker.catalog_contents(attach, if_none_match=full.etag)
        assert hit.not_modified
        assert hit.schemas == []
        assert worker.catalog_contents(attach, if_none_match="stale") is full
        assert len(calls) == 1

    @pytest.mark.parametrize(("independent", "frozen"), [(False, True), (True, False)])
    def test_not_cached_without_both_opt_ins(
        self, monkeypatch: pytest.MonkeyPatch, independent: bool, frozen: bool
    ) -> None:
        """Caching needs a frozen version and attach-independent contents."""

        class Uncached(ContentsProbeCatalog):
            catalog_contents_attach_independent = independent
            catalog_version_frozen = frozen

        class UncachedWorker(ContentsProbeWorker):
            catalog_interface = Uncached

        calls = _count_builds(monkeypatch, Uncached)
        worker = UncachedWorker()
        attach = _attach(worker, CATALOG_PROBE)
        worker.catalog_contents(attach)
        worker.catalog_contents(attach)
        assert len(calls) == 2

    def test_dynamic_catalogs_are_not_cached(self) -> None:
        """The DDL catalogs never opt in."""
        for worker_cls in (ContentsMemoryWorker, ContentsRevalWorker, ContentsHashWorker):
            assert not worker_cls.catalog_interface.catalog_contents_attach_independent
        assert ReadOnlyCatalogInterface.catalog_contents_attach_independent


# ---------------------------------------------------------------------------
# MetaWorker routing
# ---------------------------------------------------------------------------


def test_meta_worker_routes_if_none_match() -> None:
    """MetaWorker passes if_none_match through to the attached sub-worker."""
    meta = MetaWorker([ExampleWorker(), *(w() for w in CONTENTS_WORKERS)])
    attach = _attach(meta, CATALOG_REVAL)
    full = meta.catalog_contents(attach_opaque_data=attach)  # type: ignore[attr-defined]
    assert full.etag == "gen-1"
    hit = meta.catalog_contents(attach_opaque_data=attach, if_none_match="gen-1")  # type: ignore[attr-defined]
    assert hit.not_modified


# ---------------------------------------------------------------------------
# Client round trip (pipe / launch and HTTP)
# ---------------------------------------------------------------------------


def test_client_round_trip(client_transport: Any) -> None:
    """``Client.contents`` decodes the struct rows and revalidates over every transport."""
    with client_transport() as client:
        attach = client.catalog_attach(name=CATALOG_PROBE, data_version_spec=None, implementation_version=None)
        assert attach.supports_catalog_contents
        contents = client.contents(attach_opaque_data=attach.attach_opaque_data)
        assert contents.etag is None
        assert not contents.not_modified
        assert [list(s.schema.path) for s in contents.schemas] == [["main"], ["extra"]]
        main = contents.schemas[0]
        assert [t.name for t in main.tables] == ["ten"]
        assert [v.name for v in main.views] == ["answer"]
        assert {f.name for f in main.scalar_functions} >= {"double"}
        assert [m.name for m in main.scalar_macros] == ["contents_triple"]
        assert [m.name for m in main.table_macros] == ["contents_range"]

        reval = client.catalog_attach(name=CATALOG_REVAL, data_version_spec=None, implementation_version=None)
        full = client.contents(attach_opaque_data=reval.attach_opaque_data)
        assert full.etag == "gen-1"
        assert [list(s.schema.path) for s in full.schemas] == [["main"]]
        hit = client.contents(attach_opaque_data=reval.attach_opaque_data, if_none_match=full.etag)
        assert hit.not_modified
        assert hit.schemas == []
        assert hit.etag == "gen-1"
        client.catalog_detach(attach_opaque_data=reval.attach_opaque_data)


def test_item_encoding_sorts_map_keys() -> None:
    """Equal items whose maps were filled in different orders encode identically."""
    from vgi.catalog import SchemaInfo
    from vgi.protocol import _item_ipc_bytes

    a = SchemaInfo(
        attach_opaque_data=AttachOpaqueData(b"x"),
        path=["s"],
        comment=None,
        tags={"b": "2", "a": "1"},
        estimated_object_count={"view": 0, "table": 3},
    )
    b = SchemaInfo(
        attach_opaque_data=AttachOpaqueData(b"x"),
        path=["s"],
        comment=None,
        tags={"a": "1", "b": "2"},
        estimated_object_count={"table": 3, "view": 0},
    )
    assert _item_ipc_bytes(a) == _item_ipc_bytes(b)
    assert SchemaInfo.deserialize_from_bytes(_item_ipc_bytes(a)).tags == {"a": "1", "b": "2"}
