# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + behavior tests for `vgi.codegen.csharp_types` and `vgi.codegen.csharp_schemas`.

vgi-csharp checks in two generated files: the protocol's records and enums as C#
types (its wire schemas are derived from them by reflection), and the protocol's
schemas as C# Arrow schemas, which its test project compares that derivation
against. Both must match what the generators emit right now.

The drift tests compare text, not parsed schemas: the types file is more than
schemas (CLR types, attributes, initializers), and any byte of it changing is a
change to what vgi-csharp compiles.

Skipped when the `vgi-csharp` checkout is not next to vgi-python;
``VGI_CSHARP_ROOT`` overrides the location.
"""

from __future__ import annotations

import io
import os
from pathlib import Path

import pytest

from vgi.codegen import csharp_schemas, csharp_types

_TYPES = Path("src/QueryFarm.Vgi/Protocol/Generated/VgiProtocolTypes.g.cs")
_SCHEMAS = Path("test/QueryFarm.Vgi.Tests/Generated/VgiProtocolSchemas.g.cs")

_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python scripts/regen_generated.py\n"
    "\n"
    "Do not redirect a generator into its destination with `>`: the shell\n"
    "truncates the file before the generator runs, so any failure destroys\n"
    "the checked-in artifact. The script renders to memory first."
)


def _vgi_csharp_root() -> Path:
    override = os.environ.get("VGI_CSHARP_ROOT")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi-csharp"


def _render(module: object) -> str:
    buf = io.StringIO()
    module.emit(buf)  # type: ignore[attr-defined]
    return buf.getvalue()


@pytest.mark.parametrize("module", [csharp_types, csharp_schemas], ids=["types", "schemas"])
def test_generator_is_deterministic(module: object) -> None:
    """Calling emit() twice produces byte-identical output."""
    assert _render(module) == _render(module)


@pytest.mark.parametrize(
    ("module", "relative"),
    [(csharp_types, _TYPES), (csharp_schemas, _SCHEMAS)],
    ids=["types", "schemas"],
)
def test_checked_in_file_matches_generator(module: object, relative: Path) -> None:
    """Drift check: the checked-in C# matches what the generator produces."""
    path = _vgi_csharp_root() / relative
    if not path.exists():
        pytest.skip(f"{path} not found; set VGI_CSHARP_ROOT or check out vgi-csharp")
    assert path.read_text() == _render(module), f"{path} is stale.\n{_REGEN_HINT}"


def test_every_csharp_rename_is_used() -> None:
    """A rename whose Python type no longer exists is dead, and would hide the next rename it needs."""
    model = csharp_types.build_model()
    emitted = set(model.records) | set(model.enums)
    reachable_python_names = {py for py, cs in csharp_types.CSHARP_NAMES.items() if cs in emitted}
    stale = set(csharp_types.CSHARP_NAMES) - reachable_python_names
    assert not stale, f"CSHARP_NAMES entries map to nothing emitted: {sorted(stale)}"


def test_merged_items_responses_share_one_type() -> None:
    """The catalog items responses collapse into one C# type, built from all of them."""
    record = csharp_types.build_model().records["ItemsResponse"]
    methods = {o.removeprefix("method '").split("'")[0] for o in record.origins}
    assert {"catalog_catalogs", "catalog_schemas", "catalog_table_get", "catalog_index_get"} <= methods
    assert [p.wire_name for p in record.props] == ["items"]


def test_large_binary_list_carries_large_width() -> None:
    """`list<large_binary>` is declared on the property; without it the derivation says `list<binary>`."""
    props = {p.wire_name: p for p in csharp_types.build_model().records["InitRequest"].props}
    assert "[global::QueryFarm.VgiRpc.Reflection.LargeWidth]" in props["join_keys"].attrs
    assert props["join_keys"].clr_type == "global::System.Collections.Generic.List<byte[]>?"
    assert "[global::QueryFarm.VgiRpc.Reflection.LargeWidth]" in props["pushdown_filters"].attrs


def test_appended_nullable_flag_is_wire_optional() -> None:
    """A nullable column typed as a plain value with a default is an appended column."""
    props = {p.wire_name: p for p in csharp_types.build_model().records["AttachOptionSpec"].props}
    assert props["required"].clr_type == "bool"
    assert "[global::QueryFarm.Vgi.Internal.WireOptional]" in props["required"].attrs


def test_enum_members_round_trip_to_their_wire_names() -> None:
    """C# derives an enum member's wire name as UPPER_SNAKE of its name; every member must survive that."""
    for e in csharp_types.build_model().enums.values():
        for m in e.members:
            derived = csharp_types._to_snake_case(m.name).upper()
            rendered = csharp_types._render_enum(e)
            assert derived == m.wire_name or f'RpcName("{m.wire_name}")' in rendered, (e.name, m.name)
