# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + behavior tests for `vgi.codegen.cpp_records`.

The deep correctness check — that every generated record encodes with exactly its
`vgi_protocol_schemas.hpp` factory's schema, and round-trips — lives on the C++
side, in the extension's ``test/cpp/test_protocol_records.cpp``: only a C++
compiler can run the generated codecs. From here we check the model the
generator builds and that the checked-in files are current.

The checked-in comparison is skipped when the sibling `vgi` checkout is not
present; ``VGI_EXTENSION_ROOT`` points it at another checkout (e.g. a worktree).
"""

from __future__ import annotations

import io
import os
from pathlib import Path

import pytest

from vgi.codegen import cpp_records
from vgi.codegen.cpp_records import Presence

_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python scripts/regen_generated.py\n"
    "\n"
    "Do not redirect a generator into its destination with `>`: the shell\n"
    "truncates the file before the generator runs, so any failure destroys\n"
    "the checked-in artifact. The script renders to memory first."
)


def _extension_root() -> Path:
    override = os.environ.get("VGI_EXTENSION_ROOT")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi"


def _emitted(which: str) -> str:
    buf = io.StringIO()
    cpp_records.emit(buf, which)
    return buf.getvalue()


@pytest.mark.parametrize("which", ["hpp", "cpp"])
def test_generator_is_deterministic(which: str) -> None:
    """Calling emit() twice produces byte-identical output."""
    assert _emitted(which) == _emitted(which), f"cpp_records {which} is non-deterministic"


@pytest.mark.parametrize(
    ("which", "relative"),
    [("hpp", "src/generated/vgi_protocol_records.hpp"), ("cpp", "src/generated/vgi_protocol_records.cpp")],
)
def test_checked_in_files_are_current(which: str, relative: str) -> None:
    """The extension's checked-in generated files match the generator."""
    path = _extension_root() / relative
    if not path.exists():
        pytest.skip(f"{path} not present (set VGI_EXTENSION_ROOT)")
    assert path.read_text() == _emitted(which), f"{path} is stale.\n{_REGEN_HINT}"


def test_header_is_arrow_free() -> None:
    """The records header only forward-declares Arrow, so it is cheap to include anywhere."""
    header = _emitted("hpp")
    assert "#include <arrow" not in header
    assert "class RecordBatch;" in header


def test_members_follow_arrow_schema_order_and_nullability() -> None:
    """Member order is column order; nullable columns are optional or defaulted, others required."""
    for record in cpp_records.build_model().records.values():
        assert [m.wire_name for m in record.members] == record.schema.names, record.name
        for member, field in zip(record.members, record.schema, strict=True):
            if field.nullable:
                assert member.presence in (Presence.OPTIONAL, Presence.DEFAULTED), (record.name, member.wire_name)
            else:
                assert member.presence is Presence.REQUIRED, (record.name, member.wire_name)
            if member.presence is Presence.OPTIONAL:
                assert member.declared_type.startswith("std::optional<"), (record.name, member.wire_name)


def test_known_shapes() -> None:
    """Spot-check the mapping on records the extension decodes and encodes."""
    model = cpp_records.build_model()
    types = {r.name: {m.wire_name: m.declared_type for m in r.members} for r in model.records.values()}
    attach = types["CatalogAttachResult"]
    assert attach["attach_opaque_data"] == "Bytes"
    assert attach["comment"] == "std::optional<std::string>"
    assert attach["tags"] == "std::map<std::string, std::string>"
    assert attach["settings"] == "std::vector<Bytes>"
    init = types["InitRequest"]
    assert init["phase"] == "std::optional<VgiInitPhase>"
    assert init["pushdown_filters"] == "std::optional<LargeBytes>"
    assert init["join_keys"] == "std::optional<std::vector<LargeBytes>>"
    assert types["CatalogInfo"]["releases"] == "std::vector<CatalogRelease>"
    assert types["CatalogRelease"]["released_at"] == "std::optional<TimestampMicros>"
    assert types["FunctionInfo"]["function_type"] == "FunctionType"
    assert types["FunctionInfo"]["arguments"] == "Bytes"
    assert types["FunctionInfo"]["output_schema"] == "Bytes"
    assert types["FunctionInfo"]["parameter_default_values"] == "std::optional<Bytes>"
    # A wire name that is a C++ keyword gets a trailing underscore.
    assert "namespace_" in {m.ident for m in model.records["FilterFunctionCapability"].members}
    assert "max_" in {m.ident for m in model.records["TableFunctionCardinalityResult"].members}
    # Results are named after their method, matching their schema factory.
    assert "SecretLookupResult" in model.records


def test_enum_wire_values_are_member_names_in_order() -> None:
    """Enumerator order is the wire dictionary order; the wire value is the member name."""
    from vgi.table_function import TableInOutFunctionInitPhase

    phase = cpp_records.build_model().enums["VgiInitPhase"]
    assert [m.wire_name for m in phase.members] == [m.name for m in TableInOutFunctionInitPhase]
    assert [m.ident for m in phase.members] == ["Input", "Finalize", "TableBuffering", "TableBufferingFinalize"]


def test_every_factory_backed_record_gets_a_parity_pair() -> None:
    """Records with a vgi_protocol_schemas factory are listed for the C++ schema-parity test."""
    source = _emitted("cpp")
    factories = cpp_records._factory_names()
    for name in cpp_records.build_model().records:
        if name in factories:
            assert f'{{"{name}", {name}RecordSchema(), {name}Schema()}}' in source, name
