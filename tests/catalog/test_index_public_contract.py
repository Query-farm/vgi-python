# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Typed index wire shape and fail-closed public capability boundaries."""

from dataclasses import replace
from types import SimpleNamespace

import pyarrow as pa
import pytest

from vgi.catalog.capabilities import (
    CONSTRAINT_ENFORCE,
    CONSTRAINT_RECORDS_READ,
    INDEX_DEFINITION_CREATE,
    INDEX_DEFINITION_READ,
    INDEX_DROP,
    ROW_SCALAR_EXPRESSION,
    TRANSACTION_DDL,
    CatalogCapabilities,
)
from vgi.catalog.catalog_interface import IndexInfo, OnConflict
from vgi.catalog.dependencies import CatalogObjectRef
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.indexes import IndexConstraintType, IndexDefinition, IndexKey
from vgi.catalog.options import OptionState
from vgi.catalog.sql import SqlExpression
from vgi.exceptions import CatalogCapabilityError
from vgi.meta_worker import MetaWorker
from vgi.protocol import IndexCreateRequest
from vgi.typed_values import TypedValueBatch
from vgi.worker import Worker

TABLE = CatalogObjectRef(kind="table", schema_path=["a.b", "nested"], name="items")
DEFINITION = IndexDefinition(
    table=TABLE,
    algorithm="test.binary.v1",
    keys=[IndexKey(column_path=["record", "a.b"]).serialize_to_bytes()],
    options=TypedValueBatch.empty(),
    constraint_type=IndexConstraintType.NONE,
)
PROFILES = CatalogCapabilities(
    read=sorted([INDEX_DEFINITION_READ, CONSTRAINT_RECORDS_READ]),
    ddl=sorted([INDEX_DEFINITION_CREATE, INDEX_DROP]),
    write=[CONSTRAINT_ENFORCE],
    transaction=[TRANSACTION_DDL],
)


def info(definition=DEFINITION):
    return IndexInfo(
        name="idx",
        schema_path=["indexes"],
        comment=None,
        tags={},
        definition=definition,
        options=OptionState(values=definition.options),
    )


def request(**changes):
    return replace(
        IndexCreateRequest(attach_opaque_data=b"sealed", schema_path=["indexes"], name="idx", definition=DEFINITION),
        **changes,
    )


def worker(monkeypatch, selected=PROFILES, available=PROFILES):
    instance = object.__new__(Worker)
    calls = []
    provider = SimpleNamespace(
        catalog_capabilities=lambda **kwargs: available,
        index_create=lambda **kwargs: calls.append(kwargs),
        index_drop=lambda **kwargs: calls.append(kwargs),
        index_get=lambda **kwargs: info(),
    )
    monkeypatch.setattr(instance, "_get_catalog", lambda: provider)
    monkeypatch.setattr(instance, "_catalog_agreement", lambda envelope: selected)
    monkeypatch.setattr(instance, "_unwrap_attach", lambda envelope: b"provider")
    monkeypatch.setattr(instance, "_unwrap_tx_opt", lambda tx, attach: None if tx is None else b"tx")
    monkeypatch.setattr(instance, "_enrich_catalog_span", lambda **kwargs: None)
    return instance, provider, calls


def test_typed_public_carriers_replace_legacy_strings_and_require_options():
    assert not {"table_name", "index_type", "expressions", "constraint_type"}.intersection(IndexInfo.ARROW_SCHEMA.names)
    assert not {"table_name", "index_type", "expressions", "options", "constraint_type"}.intersection(
        IndexCreateRequest.ARROW_SCHEMA.names
    )
    for cls, names in [
        (IndexDefinition, ["table", "algorithm", "keys", "options", "constraint_type"]),
        (IndexInfo, ["definition", "options"]),
        (IndexCreateRequest, ["definition"]),
    ]:
        assert all(not cls.ARROW_SCHEMA.field(name).nullable for name in names)
    assert IndexInfo.deserialize_from_bytes(info().serialize_to_bytes()) == info()
    assert IndexCreateRequest.deserialize_from_bytes(request().serialize_to_bytes()) == request()
    assert DEFINITION.key_definitions()[0].column_path == ["record", "a.b"]


@pytest.mark.parametrize("kwargs", [{}, {"column_path": []}, {"column_path": ["x", None]}, {"column_path": [""]}])
def test_invalid_keys_reject(kwargs):
    with pytest.raises((ValueError, TypeError)):
        IndexKey(**kwargs)


def test_missing_options_empty_algorithm_and_key_list_reject():
    for changes in [{"options": None}, {"algorithm": ""}, {"keys": []}, {"keys": [None]}, {"keys": [b"bad"]}]:
        with pytest.raises((ValueError, TypeError, pa.ArrowInvalid)):
            replace(DEFINITION, **changes)


def test_primary_requires_actual_owned_constraint_identity():
    with pytest.raises(ValueError, match="association"):
        replace(DEFINITION, constraint_type=IndexConstraintType.PRIMARY)
    association = CatalogObjectRef(kind="constraint", schema_path=TABLE.schema_path, owner_table="other", name="stable")
    with pytest.raises(ValueError, match="another table"):
        replace(DEFINITION, constraint_type=IndexConstraintType.UNIQUE, associated_constraint=association)


def test_strict_record_rejects_trailing_data_missing_fields_and_mutable_damage():
    with pytest.raises(ValueError):
        IndexDefinition.deserialize_from_bytes(DEFINITION.serialize_to_bytes() + b"trailing")
    batch = TypedValueBatch.deserialize_from_bytes(DEFINITION.serialize_to_bytes()).batch
    with pytest.raises(ValueError):
        IndexDefinition.deserialize_from_batch(batch.drop_columns(["keys"]))
    broken = replace(DEFINITION, keys=list(DEFINITION.keys))
    broken.keys.append(b"")
    with pytest.raises(ValueError):
        broken.serialize_to_bytes()


def test_readback_option_values_must_match_resolved_definition():
    other = TypedValueBatch(pa.record_batch([pa.array([1], type=pa.int64())], names=["limit"]))
    with pytest.raises(ValueError, match="disagree"):
        replace(info(), options=OptionState(values=other, explicit_names=["limit"]))


def test_profiles_require_readback_and_do_not_select_without_offer():
    assert PROFILES.select(None) == CatalogCapabilities()
    assert PROFILES.select(CatalogCapabilities(ddl=[INDEX_DEFINITION_CREATE, INDEX_DROP])).ddl == []


@pytest.mark.parametrize("selected", [CatalogCapabilities(), CatalogCapabilities(read=[INDEX_DEFINITION_READ])])
def test_create_and_drop_without_selected_profiles_have_no_effect(monkeypatch, selected):
    instance, _, calls = worker(monkeypatch, selected=selected)
    with pytest.raises(CatalogCapabilityError):
        instance.catalog_index_create(request())
    with pytest.raises(CatalogCapabilityError):
        instance.catalog_index_drop(b"sealed", ["indexes"], "idx")
    assert calls == []


def test_current_provider_profile_revocation_and_transactions_have_no_effect(monkeypatch):
    instance, _, calls = worker(monkeypatch, available=CatalogCapabilities())
    with pytest.raises(CatalogCapabilityError):
        instance.catalog_index_create(request())
    instance, _, calls = worker(monkeypatch, selected=replace(PROFILES, transaction=[]))
    with pytest.raises(CatalogCapabilityError):
        instance.catalog_index_create(request(transaction_opaque_data=b"tx"))
    assert calls == []


def test_unique_requires_enforcement_and_replace_requires_drop(monkeypatch):
    instance, _, calls = worker(monkeypatch, selected=replace(PROFILES, write=[]))
    with pytest.raises(CatalogCapabilityError):
        instance.catalog_index_create(
            request(definition=replace(DEFINITION, constraint_type=IndexConstraintType.UNIQUE))
        )
    instance, _, calls = worker(monkeypatch, selected=replace(PROFILES, ddl=[INDEX_DEFINITION_CREATE]))
    with pytest.raises(CatalogCapabilityError):
        instance.catalog_index_create(request(on_conflict=OnConflict.REPLACE))
    assert calls == []


def test_definition_is_forwarded_without_raw_name_or_option_changes(monkeypatch):
    instance, _, calls = worker(monkeypatch)
    instance.catalog_index_create(request())
    assert calls[0]["definition"] == DEFINITION
    assert calls[0]["attach_opaque_data"] == b"provider"
    assert calls[0]["schema_path"] == ["indexes"]
    assert calls[0]["definition"].table.schema_path == ["a.b", "nested"]


def test_index_discovery_never_silently_drops_required_definition():
    with pytest.raises(CatalogCapabilityError):
        Worker._index_for_capabilities(info(), CatalogCapabilities())
    assert Worker._index_for_capabilities(info(), PROFILES) == info()


def test_meta_worker_routes_new_index_options(monkeypatch):
    meta = object.__new__(MetaWorker)
    calls = []
    child = SimpleNamespace(catalog_index_option_definitions=lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(meta, "_worker_for_attach", lambda *args, **kwargs: child)
    meta.catalog_index_option_definitions(
        attach_opaque_data=b"sealed", table=TABLE.serialize_to_bytes(), algorithm="test.binary.v1"
    )
    assert calls[0]["table"] == TABLE.serialize_to_bytes()


def test_expression_requires_context_matching_table_and_selected_profile(monkeypatch):
    expression = SqlExpression(
        sql='lower("label")',
        profile=ROW_SCALAR_EXPRESSION,
        schema_path=TABLE.schema_path,
        evaluation_context=QueryEvaluationContext(),
    )
    with pytest.raises(ValueError, match="exactly one"):
        IndexKey(column_path=["label"], expression=expression)
    value = replace(DEFINITION, keys=[IndexKey(expression=expression).serialize_to_bytes()])
    instance, _, calls = worker(monkeypatch)
    with pytest.raises(CatalogCapabilityError):
        instance.catalog_index_create(request(definition=value))
    assert calls == []
    with pytest.raises(ValueError, match="table schema"):
        replace(value, table=replace(TABLE, schema_path=["different"]))
