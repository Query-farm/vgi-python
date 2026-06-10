# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Shared machinery for `cpp_schemas` and `ts_schemas` generators.

Both emitters walk the same VgiProtocol + an explicit info-type list and
produce one schema-factory per dataclass or per-method. Only the rendering
differs. This module owns:

- The explicit `INFO_TYPES` and `EXTRA_RESPONSE_TYPES` lists.
- `collect_schemas()` — walks rpc_methods() + the explicit lists and yields
  named `EmittedSchema` records.
- `sanitize_name()` — CamelCase conversion for method names.
- `GeneratorError` for consistent error reporting.
- `provenance_comment()` for content-hashed generator headers.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import pyarrow as pa
from vgi_rpc.rpc._types import MethodType, rpc_methods  # type: ignore[attr-defined]

from vgi.catalog.catalog_interface import (
    CatalogInfo,
    CatalogObject,
    FunctionInfo,
    IndexInfo,
    MacroInfo,
    ScanBranch,
    ScanBranchesResult,
    ScanFunctionResult,
    SchemaInfo,
    TableInfo,
    ViewInfo,
)
from vgi.protocol import VgiProtocol

# Info-object dataclasses that appear as inner items inside `{items: List<Binary>}`
# responses. The wire layer erases them to `binary`, so the method signature
# doesn't reference them — enumerate explicitly.
INFO_TYPES: tuple[type, ...] = (
    CatalogInfo,
    SchemaInfo,
    TableInfo,
    ViewInfo,
    FunctionInfo,
    MacroInfo,
    IndexInfo,
)

# Extra dataclasses whose ARROW_SCHEMA is referenced by methods whose return
# type is `bytes` (raw IPC). Not inferable from method signatures alone.
EXTRA_RESPONSE_TYPES: tuple[type, ...] = (
    ScanFunctionResult,  # catalog_table_{scan,insert,update,delete}_function_get
    ScanBranchesResult,  # catalog_table_scan_branches_get (top-level wrapper)
    ScanBranch,          # one entry inside ScanBranchesResult.branches (binary blob)
)


class GeneratorError(RuntimeError):
    """Raised when an Arrow feature used by vgi-python isn't supported by an emitter."""


@dataclass(frozen=True)
class EmittedSchema:
    """A named Arrow schema emission target.

    `name` is the factory/function/const stem (CamelCase, no trailing `Schema`);
    emitters are free to append whatever suffix their output language idiom uses.
    """

    name: str
    schema: pa.Schema
    origin: str  # e.g. "method 'catalog_catalogs' result"


def sanitize_name(name: str) -> str:
    """Convert a method/class name into a valid CamelCase C++/TS identifier stem."""
    if "_" in name:
        return "".join(part.capitalize() for part in name.split("_"))
    return name[:1].upper() + name[1:]


def _all_info_subclasses() -> set[type]:
    """Transitive subclasses of CatalogObject, excluding CatalogObject itself."""
    result: set[type] = set()
    stack = [CatalogObject]
    while stack:
        cls = stack.pop()
        for sub in cls.__subclasses__():
            if sub not in result:
                result.add(sub)
                stack.append(sub)
    return result


def _resolve_inner_schema(result_type: object, method_name: str) -> pa.Schema | None:
    """Translate a method's return-type annotation to its inner Arrow schema.

    Returns None when the method's return carries raw IPC bytes or ``None``
    (both map to ``dynamic`` in the generated registry and don't need a factory).
    """
    import types
    import typing

    if isinstance(result_type, type):
        arrow_schema = getattr(result_type, "ARROW_SCHEMA", None)
        if isinstance(arrow_schema, pa.Schema):
            return arrow_schema
        if result_type is type(None) or result_type is bytes:
            return None
        raise GeneratorError(
            f"Method '{method_name}' returns {result_type!r}, which isn't an "
            "ArrowSerializableDataclass or raw bytes. Extend _resolve_inner_schema().",
        )

    origin = typing.get_origin(result_type)
    if origin is typing.Union or origin is types.UnionType:
        args = [a for a in typing.get_args(result_type) if a is not type(None)]
        if len(args) == 1:
            return _resolve_inner_schema(args[0], method_name)
        raise GeneratorError(
            f"Method '{method_name}' returns a non-Optional union {result_type!r}; unsupported.",
        )

    raise GeneratorError(
        f"Method '{method_name}' has unrecognized result_type {result_type!r}. "
        "Extend _resolve_inner_schema() in vgi/codegen/_common.py.",
    )


def collect_schemas(
    protocol_cls: type = VgiProtocol,
    *,
    info_types: tuple[type, ...] = INFO_TYPES,
    extra_response_types: tuple[type, ...] = EXTRA_RESPONSE_TYPES,
    check_info_subclasses: bool = True,
) -> list[EmittedSchema]:
    """Enumerate every schema the C++ or TS emitter needs, deduped by name.

    Ordering: info types first (alphabetical by definition order in ``info_types``),
    then per-method result schemas (alphabetical by method name), then per-method
    params schemas (alphabetical). Stable across runs — both emitters rely on
    this for drift tests.

    Parametrized by protocol class so a second protocol (e.g.
    :class:`vgi.secret_protocol.VgiSecretProtocol`) can reuse the same machinery.
    The defaults reproduce the original ``VgiProtocol`` behavior byte-for-byte.
    A protocol with no catalog-object info types passes ``info_types=()``,
    ``extra_response_types=()`` and ``check_info_subclasses=False`` (the
    CatalogObject completeness net only applies to the catalog protocol).
    """
    out: list[EmittedSchema] = []
    seen_names: set[str] = set()

    # Safety net: every subclass of CatalogObject with its own ARROW_SCHEMA
    # must be in info_types. Intermediate ABCs like CatalogSchemaObject are
    # skipped automatically. Only meaningful for the catalog protocol.
    if check_info_subclasses:
        declared = set(info_types)
        missing = {
            c
            for c in _all_info_subclasses()
            if hasattr(c, "ARROW_SCHEMA") and isinstance(getattr(c, "ARROW_SCHEMA", None), pa.Schema)
        } - declared
        if missing:
            names = sorted(c.__name__ for c in missing)
            raise GeneratorError(
                f"Info dataclasses {names} inherit from CatalogObject and have ARROW_SCHEMA "
                "but are not in INFO_TYPES in vgi/codegen/_common.py. Add them to keep "
                "inner-item validation in sync.",
            )

    # 1) Info-type schemas + extra standalone dataclasses.
    for cls in (*info_types, *extra_response_types):
        name = sanitize_name(cls.__name__)
        if name in seen_names:
            continue
        seen_names.add(name)
        schema = getattr(cls, "ARROW_SCHEMA")  # noqa: B009
        out.append(EmittedSchema(name=name, schema=schema, origin=cls.__name__))

    # 2) Per-method unary response schemas (inner dataclass, not the outer envelope).
    methods = rpc_methods(protocol_cls)
    for method_name in sorted(methods.keys()):
        info = methods[method_name]
        if info.method_type != MethodType.UNARY:
            continue
        if not info.has_return:
            continue
        inner = _resolve_inner_schema(info.result_type, method_name)
        if inner is None:
            continue
        name = sanitize_name(method_name) + "Result"
        if name in seen_names:
            continue
        seen_names.add(name)
        out.append(
            EmittedSchema(
                name=name,
                schema=inner,
                origin=f"method '{method_name}' result",
            ),
        )

    # 3) Per-method request schemas (outer wire batch, every method).
    for method_name in sorted(methods.keys()):
        info = methods[method_name]
        name = sanitize_name(method_name) + "Params"
        if name in seen_names:
            continue
        seen_names.add(name)
        out.append(
            EmittedSchema(
                name=name,
                schema=info.params_schema,
                origin=f"method '{method_name}' params",
            ),
        )

    return out


def provenance_comment(
    *,
    generator_module: str,
    generator_command: str,
    generator_version: str,
    regen_command_lines: list[str],
    body: str,
    comment_prefix: str = "//",
) -> str:
    """Return a generator banner whose hash captures only the meaningful body.

    The drift test fires when ``body`` actually changes between regenerations,
    not on every vgi-python commit. Provenance (which generator, which
    version) stays in the comment; ``git log`` on the emitted file fills in
    the "which checkout produced this" question that the SHA stamp used to
    answer.
    """
    h = hashlib.sha256(body.encode("utf-8")).hexdigest()[:12]
    cp = comment_prefix
    out: list[str] = [
        f"{cp} GENERATED by {generator_module}. DO NOT EDIT BY HAND.",
        f"{cp}",
        f"{cp} Generator: {generator_command} v{generator_version}",
        f"{cp} Content hash: {h}",
        f"{cp}",
        f"{cp} To regenerate:",
    ]
    for line in regen_command_lines:
        out.append(f"{cp}   {line}")
    return "\n".join(out) + "\n"
