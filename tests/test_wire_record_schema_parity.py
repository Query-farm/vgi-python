# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Parity between hand-built wire records and their declared ``ARROW_SCHEMA``.

Most wire records in this package derive their schema from the dataclass
(``ArrowSerializableDataclass``), so the batch is built *from* ``ARROW_SCHEMA``
and drift is structurally impossible. A handful do not: they declare
``ARROW_SCHEMA`` by hand *and* build the row by hand, and ``from_pylist`` is
forgiving in both directions — a key the schema does not declare is dropped
without a word, and a column the row omits is written as null. Neither shows up
in the bytes, and to a reader both look like a worker that had nothing to say.

That failure mode is not hypothetical; it has cost the sibling SDKs real
outages:

* TypeScript's ``ScanBranch`` builder supplied 7 of the schema's 9 columns
  because it was never updated when the two ``format_*`` fields were added. The
  missing ``format_locations`` is a LIST, and Arrow dereferences a list's
  children while writing, so EVERY multi-branch table in that worker died with
  ``TypeError: Cannot read properties of undefined (reading 'slice')`` —
  including tables that predated those fields entirely.
* Java's ``PlanResponse`` marked two fields nullable where the protocol says
  non-null, and the client rejected the whole response as an "out-of-date
  Apache Arrow schema".

So for every such record this module builds one with every field populated and
checks three things: the row's key set is exactly the schema's column set, the
serialized batch's schema is exactly ``ARROW_SCHEMA``, and no column came back
null. ``test_every_hand_declared_schema_is_covered`` then walks the package for
hand-declared ``ARROW_SCHEMA`` class attributes, so a record type added later is
either covered here or explicitly excused — never silently untested.
"""

from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.utils import deserialize_record_batch

import vgi
from vgi.catalog.attach_option import AttachOptionSpec
from vgi.catalog.catalog_interface import (
    ScanBranch,
    ScanBranchesResult,
    ScanFunctionResult,
)
from vgi.catalog.secret_type import SecretTypeSpec
from vgi.catalog.setting import SettingSpec


@dataclass(frozen=True)
class WireRecordCase:
    """One record that declares its own ``ARROW_SCHEMA`` and builds its own row.

    ``build`` returns an instance with EVERY field of the schema populated, so a
    null column in the result means the builder dropped the value rather than
    the fixture never supplying one.
    """

    name: str
    build: Callable[[], Any]


def _scalar_args() -> tuple[list[pa.Scalar], dict[str, pa.Scalar]]:
    return (
        [pa.scalar("s3://bucket/x.parquet", pa.string())],
        {"hive_partitioning": pa.scalar(True, pa.bool_())},
    )


def _scan_function_result() -> ScanFunctionResult:
    positional, named = _scalar_args()
    return ScanFunctionResult(
        function_name="read_parquet",
        positional_arguments=positional,
        named_arguments=named,
        required_extensions=["parquet"],
    )


def _scan_branch() -> ScanBranch:
    positional, named = _scalar_args()
    return ScanBranch(
        function_name="read_parquet",
        positional_arguments=positional,
        named_arguments=named,
        branch_filter="ts >= '2026-01-01'",
        writable=True,
        source_catalog="lake",
        source_schema="main",
        source_table="events",
        format_name="parquet",
        format_locations=["s3://bucket/a.parquet"],
    )


def _scan_branches_result() -> ScanBranchesResult:
    return ScanBranchesResult(branches=[_scan_branch()], required_extensions=["parquet"])


CASES: list[WireRecordCase] = [
    WireRecordCase("ScanFunctionResult", _scan_function_result),
    WireRecordCase("ScanBranch", _scan_branch),
    WireRecordCase("ScanBranchesResult", _scan_branches_result),
    WireRecordCase(
        "SettingSpec",
        lambda: SettingSpec(name="greeting", desc="Greeting text", type=pa.string(), default="hello"),
    ),
    WireRecordCase(
        # `required` and `default` are mutually exclusive by construction, so
        # this instance populates `default` and the `required` column carries
        # its meaningful False. The nullable-column check below skips it.
        "AttachOptionSpec",
        lambda: AttachOptionSpec(name="multiplier", desc="Row multiplier", type=pa.int64(), default=2, required=False),
    ),
    WireRecordCase(
        "SecretTypeSpec",
        lambda: SecretTypeSpec(
            name="vgi_example",
            description="Example VGI secret",
            schema=pa.schema([pa.field("api_key", pa.string(), metadata={"redact": "true"})]),
        ),
    ),
]

#: Hand-declared ``ARROW_SCHEMA`` holders that no case builds, and why. Listing
#: them beats ignoring unknown classes: adding a record forces a deliberate
#: choice between covering it and writing down why it needs no coverage.
NOT_COVERED: dict[str, str] = {
    "_SpecBase": (
        "abstract base — SettingSpec and AttachOptionSpec are the concrete records on the wire and both are covered"
    ),
}

CASES_BY_NAME = {case.name: case for case in CASES}


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_row_dict_covers_every_declared_column(case: WireRecordCase) -> None:
    """The builder's row names exactly the columns the schema declares.

    A missing key is the ScanBranch bug: silently null on the wire (or, for a
    list column, a crash inside Arrow's writer). An extra key is the opposite
    failure and just as quiet — ``from_pylist`` drops it, so a value the caller
    set never reaches the peer at all.
    """
    record = case.build()
    schema: pa.Schema = type(record).ARROW_SCHEMA
    row_keys = set(record.to_row_dict())
    declared = set(schema.names)

    assert row_keys == declared, (
        f"{case.name}.to_row_dict() does not match ARROW_SCHEMA: "
        f"missing {sorted(declared - row_keys)}, unexpected {sorted(row_keys - declared)}"
    )


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_serialized_schema_is_the_declared_schema(case: WireRecordCase) -> None:
    """The bytes on the wire carry ``ARROW_SCHEMA``, field for field.

    Names, order, types and nullability all matter: the C++ client matches the
    response schema against its own generated copy and rejects the whole
    response on any difference, which is how Java's two over-nullable fields
    turned into "out-of-date Apache Arrow schema".
    """
    record = case.build()
    schema: pa.Schema = type(record).ARROW_SCHEMA
    batch, _ = deserialize_record_batch(record.serialize())

    assert batch.schema.names == schema.names, (
        f"{case.name}: serialized columns {batch.schema.names} != declared {schema.names}"
    )
    for declared_field in schema:
        actual = batch.schema.field(declared_field.name)
        assert actual.type == declared_field.type, (
            f"{case.name}.{declared_field.name}: serialized as {actual.type}, declared {declared_field.type}"
        )
        assert actual.nullable == declared_field.nullable, (
            f"{case.name}.{declared_field.name}: serialized nullable={actual.nullable}, "
            f"declared nullable={declared_field.nullable}"
        )


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_every_column_is_populated(case: WireRecordCase) -> None:
    """No column comes back null for a record whose every field is set.

    A column that is present but never written is indistinguishable on the wire
    from "the worker has nothing to say", so the peer reads a default and the
    feature quietly does not exist.
    """
    record = case.build()
    schema: pa.Schema = type(record).ARROW_SCHEMA
    batch, _ = deserialize_record_batch(record.serialize())

    row = batch.to_pylist()[0]
    for name in schema.names:
        assert row[name] is not None, (
            f"{case.name}.{name} is null even though the fixture populates every field "
            f"— the builder is not wiring this column"
        )


def _hand_declared_schema_classes() -> dict[str, type]:
    """Classes in the ``vgi`` package that declare ``ARROW_SCHEMA`` themselves.

    Deliberately reads ``__dict__`` rather than ``getattr``: an
    ``ArrowSerializableDataclass`` subclass answers ``ARROW_SCHEMA`` from a
    descriptor that GENERATES it from the dataclass fields, so its batch cannot
    disagree with its schema. Only a class that spells the schema out by hand
    has two things that can drift apart, and only those belong here.
    """
    found: dict[str, type] = {}
    for module_info in pkgutil.walk_packages(vgi.__path__, prefix="vgi."):
        try:
            module = importlib.import_module(module_info.name)
        except ImportError:  # pragma: no cover - optional extras
            continue
        for obj in vars(module).values():
            if not isinstance(obj, type) or obj.__module__ != module_info.name:
                continue
            if isinstance(vars(obj).get("ARROW_SCHEMA"), pa.Schema):
                # Keyed by __qualname__, not the binding name, so a module-level
                # alias (WriteFunctionResult = ScanFunctionResult) does not read
                # as a second, uncovered record type.
                found[obj.__qualname__] = obj
    return found


def test_every_hand_declared_schema_is_covered() -> None:
    """No hand-built record type escapes the parity checks above.

    A new record type FAILS here as unlisted rather than being picked up
    automatically. Auto-coverage would need a generic fully-populated builder
    per record, and there isn't one — the builders take dataclass fields, not
    schemas — so "automatic" would in practice mean "skipped", which recreates
    the gap this test exists to close.
    """
    classes = _hand_declared_schema_classes()
    assert classes, (
        "found no classes declaring their own ARROW_SCHEMA — the discovery walk broke "
        "and this coverage guard is now silently vacuous"
    )

    missing = sorted(set(classes) - set(CASES_BY_NAME) - set(NOT_COVERED))
    assert not missing, (
        f"{missing} declare their own ARROW_SCHEMA but no WireRecordCase builds them. "
        "Add a case, or add the class to NOT_COVERED with the reason it needs none."
    )

    # The excuse list has to stay honest too: an entry for a class that no
    # longer exists reads as coverage that does not exist.
    stale = sorted(set(NOT_COVERED) - set(classes))
    assert not stale, f"NOT_COVERED lists {stale}, which no longer declare an ARROW_SCHEMA — drop them"

    both = sorted(set(NOT_COVERED) & set(CASES_BY_NAME))
    assert not both, f"{both} are both covered by a case and excused in NOT_COVERED"
