# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The generated ``vgi.v2`` registries reproduce the reference protocol hash.

Each SDK's vgi-rpc port derives the wire schemas from the generated interface
by reflection, so the meaningful check is not "the generator read the model"
but "what it *rendered* derives back to the reference". These tests parse the
rendered Java and C#, derive every params / result / header field by that
port's ``SchemaDerivation`` rules, and assert the resulting
``vgi_rpc.protocol_hash.v1`` digest equals the live ``VgiProtocol`` hash --
computed here, never a pinned constant, so a protocol change moves both sides.

Drift: the checked-in ``VgiService.java`` / ``IVgiService.g.cs`` must equal
what the generators emit now (skipped when the sibling checkout is absent;
``VGI_JAVA_ROOT`` / ``VGI_CSHARP_ROOT`` override the location).
"""

from __future__ import annotations

import dataclasses
import os
import re
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.rpc._protocol_hash import compute_protocol_hash
from vgi_rpc.rpc._types import MethodType, rpc_methods  # type: ignore[attr-defined]

from vgi.codegen import csharp_registry, csharp_types, java_registry
from vgi.codegen._registry import RegistryMethod, preimage_hash, protocol_name, registry_methods
from vgi.protocol import VgiProtocol

_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python scripts/regen_generated.py\n"
    "\n"
    "Do not redirect a generator into its destination with `>`: the shell\n"
    "truncates the file before the generator runs, so any failure destroys\n"
    "the checked-in artifact. The script renders to memory first."
)

_LIST_UTF8 = pa.list_(pa.field("item", pa.string(), nullable=True))
_MAP_UTF8 = pa.map_(pa.string(), pa.string())
_DICT_UTF8 = pa.dictionary(pa.int16(), pa.string())


def _live_hash() -> str:
    return str(compute_protocol_hash(protocol_name(), rpc_methods(VgiProtocol)))


def _model() -> dict[str, RegistryMethod]:
    return {m.name: m for m in registry_methods()}


def _header_class(name: str, names: dict[str, str]) -> type:
    """The Python header dataclass a rendered header type name stands for."""
    for m in registry_methods():
        if m.header_type is not None and names.get(m.header_type.__name__, m.header_type.__name__) == name:
            return m.header_type
    raise AssertionError(f"no vgi.v2 header type renders as {name}")


def test_model_reproduces_the_live_hash() -> None:
    """The language-neutral model is lossless: its preimage is the reference's."""
    assert preimage_hash(protocol_name(), registry_methods()) == _live_hash()


# ---------------------------------------------------------------------------
# Java: derive back by vgi-rpc-java's SchemaDerivation rules
# ---------------------------------------------------------------------------

_JAVA_METHOD = re.compile(
    r"((?:    @\w+(?:\([^)]*\))?\n)*)    default (.+?) (\w+)\(\n"
    r"((?:            .+,\n)*)            CallContext ctx\) \{\n"
    r'        throw notImplemented\("(\w+)"\);\n    \}'
)
_JAVA_PARAM = re.compile(r"^((?:@\w+(?:\([^)]*\))? )*)(.+) (\w+)$")
_JAVA_RECORDS = set(java_registry.JAVA_TYPES.values()) | set(java_registry.JAVA_RAW_RESULTS.values())


def _java_type(java_type: str, annotations: str) -> pa.DataType:
    if "@ArrowField(ArrowFieldType.DICT_INT16_UTF8)" in annotations:
        return _DICT_UTF8
    if java_type in _JAVA_RECORDS or java_type == "byte[]":
        return pa.binary()
    simple: dict[str, pa.DataType] = {
        "String": pa.string(),
        "boolean": pa.bool_(),
        "Boolean": pa.bool_(),
        "long": pa.int64(),
        "Long": pa.int64(),
        "List<String>": _LIST_UTF8,
        "Map<String, String>": _MAP_UTF8,
    }
    return simple[java_type]


def _java_derived(text: str | None = None) -> list[RegistryMethod]:
    text = java_registry.render() if text is None else text
    model = _model()
    out: list[RegistryMethod] = []
    for match in _JAVA_METHOD.finditer(text):
        method_ann, ret, name, params_text, stub_name = match.groups()
        assert stub_name == name, f"{name}'s default body reports {stub_name}"
        base = model[name]
        params = []
        for line, p in zip(params_text.splitlines(), base.params, strict=True):
            pm = _JAVA_PARAM.match(line.strip().rstrip(","))
            assert pm is not None, line
            annotations, java_type, pname = pm.groups()
            assert pname == p.name
            field = pa.field(pname, _java_type(java_type, annotations), nullable="@Nullable" in annotations)
            params.append(dataclasses.replace(p, field=field))
        is_stream = ret.startswith("RpcStream<")
        result = None
        if not is_stream and ret != "void":
            result = pa.field("result", _java_type(ret, ""), nullable="@Nullable" in method_ann)
        header_match = re.search(r"@StreamHeader\((\w+)\.class\)", method_ann)
        header = _header_class(header_match.group(1), java_registry.JAVA_TYPES) if header_match else None
        out.append(
            dataclasses.replace(
                base,
                method_type=MethodType.STREAM if is_stream else MethodType.UNARY,
                params=tuple(params),
                result_field=result,
                header_type=header,
            )
        )
    return out


def test_java_registry_derives_the_live_hash() -> None:
    """The rendered VgiService, read back as vgi-rpc-java reads it, is the reference vgi.v2."""
    derived = _java_derived()
    assert sorted(m.name for m in derived) == sorted(_model()), "rendered method set differs from vgi.v2"
    assert preimage_hash(protocol_name(), derived) == _live_hash()


# ---------------------------------------------------------------------------
# C#: derive back by vgi-rpc-csharp's SchemaDerivation rules
# ---------------------------------------------------------------------------

_CS_METHOD = re.compile(
    r"((?:    \[.+\]\n)*)    (Task(?:<.+>)?) (\w+)Async\(\n((?:        .+,\n)*)        ICallContext\? ctx = null\) =>\n"
    r'        throw UnimplementedMethod\.For\("(\w+)"\);'
)
_CS_PARAM = re.compile(r"^((?:\[[^\]]+\] )*)(.+?) (\w+)(?: = .+)?$")


def _cs_type(clr: str, enums: set[str], records: set[str]) -> tuple[pa.DataType, bool]:
    nullable = clr.endswith("?")
    clr = clr.rstrip("?")
    if clr in enums:
        return _DICT_UTF8, nullable
    if clr in records or clr == "byte[]":
        return pa.binary(), nullable
    simple: dict[str, pa.DataType] = {
        "string": pa.string(),
        "bool": pa.bool_(),
        "long": pa.int64(),
        "List<string>": _LIST_UTF8,
        "Dictionary<string, string>": _MAP_UTF8,
    }
    return simple[clr], nullable


def _cs_derived() -> list[RegistryMethod]:
    text = csharp_registry.render()
    cs_model = csharp_types.build_model()
    enums, records = set(cs_model.enums), set(cs_model.records)
    model = _model()
    by_pascal = {csharp_types._pascal(name): name for name in model}
    out: list[RegistryMethod] = []
    for match in _CS_METHOD.finditer(text):
        attrs, ret, pascal, params_text, stub_name = match.groups()
        name = by_pascal[pascal]
        assert csharp_types._to_snake_case(pascal) == name, f"{pascal}Async does not map to {name}"
        assert stub_name == name, f"{name}'s default body reports {stub_name}"
        base = model[name]
        params = []
        for line, p in zip(params_text.splitlines(), base.params, strict=True):
            pm = _CS_PARAM.match(line.strip().rstrip(","))
            assert pm is not None, line
            _, clr, pname = pm.groups()
            assert csharp_types._to_snake_case(pname) == p.name
            dtype, nullable = _cs_type(clr, enums, records)
            params.append(dataclasses.replace(p, field=pa.field(p.name, dtype, nullable=nullable)))
        inner = ret[len("Task<") : -1] if ret.startswith("Task<") else None
        is_stream = inner is not None and inner.startswith("RpcStream<")
        result = None
        if inner is not None and not is_stream:
            dtype, nullable = _cs_type(inner, enums, records)
            result = pa.field("result", dtype, nullable=nullable)
        header_match = re.search(r"\[StreamHeader\(typeof\((\w+)\)\)\]", attrs)
        header = _header_class(header_match.group(1), csharp_types.CSHARP_NAMES) if header_match else None
        out.append(
            dataclasses.replace(
                base,
                method_type=MethodType.STREAM if is_stream else MethodType.UNARY,
                params=tuple(params),
                result_field=result,
                header_type=header,
            )
        )
    return out


def test_csharp_registry_derives_the_live_hash() -> None:
    """The rendered IVgiService, read back as vgi-rpc-csharp reads it, is the reference vgi.v2."""
    derived = _cs_derived()
    assert sorted(m.name for m in derived) == sorted(_model()), "rendered method set differs from vgi.v2"
    assert preimage_hash(protocol_name(), derived) == _live_hash()


# ---------------------------------------------------------------------------
# A changed protocol changes the rendering (the test above is not vacuous)
# ---------------------------------------------------------------------------


def test_a_flipped_nullability_changes_the_derived_hash() -> None:
    """Derivation reads the rendered text: one dropped ``@Nullable`` moves the digest."""
    text = java_registry.render()
    start = text.index("    default ItemsResponse catalog_schemas(")
    end = text.index("CallContext ctx", start)
    target = "@Nullable byte[] transaction_opaque_data"
    assert target in text[start:end]
    tampered = text[:start] + text[start:end].replace(target, "byte[] transaction_opaque_data") + text[end:]
    assert preimage_hash(protocol_name(), _java_derived(tampered)) != _live_hash()


# ---------------------------------------------------------------------------
# Drift
# ---------------------------------------------------------------------------


def _root(env: str, repo: str, marker: str) -> Path:
    override = os.environ.get(env)
    if override:
        return Path(override)
    candidates = [Path(__file__).resolve().parents[2] / repo, Path.home() / repo]
    for candidate in candidates:
        if (candidate / marker).parent.is_dir():
            return candidate
    return candidates[0]


_TARGETS: list[tuple[Any, str, str, str]] = [
    (java_registry, "VGI_JAVA_ROOT", "vgi-java", java_registry.TARGET),
    (csharp_registry, "VGI_CSHARP_ROOT", "vgi-csharp", csharp_registry.TARGET),
]


@pytest.mark.parametrize(("module", "env", "repo", "relative"), _TARGETS, ids=["java", "csharp"])
def test_generator_is_deterministic(module: Any, env: str, repo: str, relative: str) -> None:
    """Rendering twice produces byte-identical output."""
    assert module.render() == module.render()


@pytest.mark.parametrize(("module", "env", "repo", "relative"), _TARGETS, ids=["java", "csharp"])
def test_checked_in_registry_matches_generator(module: Any, env: str, repo: str, relative: str) -> None:
    """Drift check: the checked-in registry matches what the generator produces."""
    path = _root(env, repo, relative) / relative
    if not path.parent.is_dir():
        pytest.skip(f"{path.parent} not found; set {env} or check out {repo}")
    assert path.exists(), f"{path} is missing.\n{_REGEN_HINT}"
    assert path.read_text() == module.render(), f"{path} is stale.\n{_REGEN_HINT}"
