r"""Emit a typed TS client surface (interfaces + enums) for vgi-typescript.

Sister module to `vgi.codegen.ts_schemas`. Walks the VgiProtocol and emits
one TS interface per referenced dataclass plus one string-literal union per
referenced Enum. The companion `vgi-protocol-schemas.ts` owns the on-the-wire
Arrow schemas; this file owns the *shape* of those requests/responses.

### Multirepo workflow

1. Modify the dataclass in `vgi-python`.
2. Regenerate both generated files:
   ```
   uv run --project ~/Development/vgi-python vgi-gen-ts-schemas \\
       > ~/Development/vgi-typescript/src/generated/vgi-protocol-schemas.ts
   uv run --project ~/Development/vgi-python vgi-gen-ts-client \\
       > ~/Development/vgi-typescript/src/generated/vgi-client.ts
   ```
3. Commit the regenerated files in the `vgi-typescript` repo on the same branch.

`tests/test_generated_ts_client.py` enforces that the checked-in `.ts`
matches what the generator would emit right now.

### Opaque vs describable types

For fields typed as `Annotated[T, ArrowType(binary|large_binary)]`:
- If `T` is a pyarrow `Schema` / `RecordBatch`, or an opaque dataclass
  (`Arguments`, `ArrowSerializableDataclass | None`), emit `Uint8Array` with
  a JSDoc tag noting the semantic source. Serialization stays the caller's
  responsibility (and matches the existing Python side).
- Otherwise walk the inner type and emit its semantic shape.

For the INFO_TYPES response envelopes (CatalogsResponse, TablesResponse, …
FunctionsResponse), promote `items: list[bytes]` to a typed array of the
corresponding Info dataclass (see `INFO_ENVELOPES`).

### Enums

Emitted as string-literal unions of member **names**. Wire encoding for
most enums uses `.name`; if a specific enum encodes by `.value`, override
in `_ENUM_VALUE_OVERRIDES` (empty today; audit per-enum as the need arises).
"""

from __future__ import annotations

import dataclasses
import enum
import io
import sys
import types as pytypes
import typing
from typing import TYPE_CHECKING

from vgi_rpc.rpc._types import rpc_methods

from vgi.codegen._common import INFO_TYPES, GeneratorError, provenance_comment
from vgi.protocol import VgiProtocol

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"


# Map items-envelope response class -> the dataclass each byte element decodes to.
# Explicit so surprises fail loudly instead of silently mis-typing.
def _info(name: str) -> type:
    for c in INFO_TYPES:
        if c.__name__ == name:
            return c
    raise GeneratorError(f"INFO_TYPES missing expected class {name!r}")


INFO_ENVELOPES: dict[str, type] = {
    "CatalogsResponse": _info("CatalogInfo"),
    "SchemasResponse": _info("SchemaInfo"),
    "TablesResponse": _info("TableInfo"),
    "ViewsResponse": _info("ViewInfo"),
    "FunctionsResponse": _info("FunctionInfo"),
    "MacrosResponse": _info("MacroInfo"),
    "IndexesResponse": _info("IndexInfo"),
}


# Qualified names of types that stay terminal `Uint8Array` at the wire boundary.
# The JSDoc tag emitted alongside each documents *what* the bytes actually are,
# so callers know which codec responsibility applies.
_OPAQUE_KINDS: dict[str, str] = {
    "pyarrow.lib.Schema": "@arrow-schema",
    "pyarrow.lib.RecordBatch": "@record-batch",
    # Fully-qualified ASD base — user-defined opaque payloads (*_opaque_data).
    "vgi_rpc.utils.ArrowSerializableDataclass": "@opaque-user-payload",
    # Arguments contains pyarrow Scalar fields — not describable as TS interfaces.
    "vgi.arguments.Arguments": "@vgi-arguments",
}


# If a specific enum needs to be emitted as a union of `.value` instead of
# `.name` (some wire encodings use the value). Empty today.
_ENUM_VALUE_OVERRIDES: set[type] = set()


# --------------------------------------------------------------------------- #
# Walk state
# --------------------------------------------------------------------------- #


@dataclasses.dataclass
class _Ctx:
    dataclasses_seen: dict[type, str] = dataclasses.field(default_factory=dict)
    enums_seen: dict[type, str] = dataclasses.field(default_factory=dict)
    dataclass_order: list[type] = dataclasses.field(default_factory=list)
    enum_order: list[type] = dataclasses.field(default_factory=list)


# --------------------------------------------------------------------------- #
# Type helpers
# --------------------------------------------------------------------------- #


def _unwrap(t: typing.Any) -> tuple[typing.Any, typing.Any]:
    """Strip NewType.__supertype__ and Annotated wrappers. Return (T, ArrowType-or-None)."""
    arrow_meta = None
    while hasattr(t, "__supertype__"):
        t = t.__supertype__
    if typing.get_origin(t) is typing.Annotated:
        args = typing.get_args(t)
        t = args[0]
        for m in args[1:]:
            if type(m).__name__ == "ArrowType":
                arrow_meta = m
    return t, arrow_meta


def _is_optional(t: typing.Any) -> bool:
    origin = typing.get_origin(t)
    if origin is typing.Union or origin is pytypes.UnionType:
        return type(None) in typing.get_args(t)
    return False


def _strip_optional(t: typing.Any) -> typing.Any:
    args = [a for a in typing.get_args(t) if a is not type(None)]
    if len(args) == 1:
        return args[0]
    return typing.Union[tuple(args)]  # noqa: UP007


def _qualname(t: typing.Any) -> str:
    return f"{getattr(t, '__module__', '')}.{getattr(t, '__name__', '')}"


def _opaque_tag_for(inner: typing.Any) -> str | None:
    """Return a JSDoc tag if `inner` is a known-opaque wire payload, else None."""
    if _qualname(inner) in _OPAQUE_KINDS:
        return _OPAQUE_KINDS[_qualname(inner)]
    return None


# --------------------------------------------------------------------------- #
# Type emitter
# --------------------------------------------------------------------------- #


def _emit_type(t: typing.Any, ctx: _Ctx) -> str:
    """Convert a Python type hint to a TS type expression (side-effect: registers dataclasses/enums)."""
    t, arrow_meta = _unwrap(t)

    if arrow_meta is not None:
        inner = _strip_optional(t) if _is_optional(t) else t
        suffix = " | null" if _is_optional(t) else ""

        if _opaque_tag_for(inner) is not None:
            return f"Uint8Array{suffix}"
        if typing.get_origin(inner) is list:
            (li,) = typing.get_args(inner)
            if _opaque_tag_for(li) is not None:
                return f"Uint8Array[]{suffix}"
        # else: fall through and emit the semantic type below

    if _is_optional(t):
        return f"{_emit_type(_strip_optional(t), ctx)} | null"

    origin = typing.get_origin(t)

    if origin is None:
        if t is int or t is float:
            return "number"
        if t is str:
            return "string"
        if t is bool:
            return "boolean"
        if t is bytes:
            return "Uint8Array"
        if t is type(None):
            return "null"
        if t is typing.Any:
            return "unknown"
        if isinstance(t, type) and issubclass(t, enum.Enum):
            return _register_enum(t, ctx)
        if isinstance(t, type) and dataclasses.is_dataclass(t):
            return _register_dataclass(t, ctx)
        # Terminal opaque — bare pa.Schema / pa.RecordBatch / ASD without Annotated
        if _qualname(t) in _OPAQUE_KINDS:
            return "Uint8Array"
        raise GeneratorError(f"Unmapped type: {t!r} (qual={_qualname(t)}). Extend ts_client.")

    if origin is list:
        (inner,) = typing.get_args(t)
        return f"{_emit_type(inner, ctx)}[]"
    if origin is dict:
        k, v = typing.get_args(t)
        return f"Record<{_emit_type(k, ctx)}, {_emit_type(v, ctx)}>"
    if origin is tuple:
        args = typing.get_args(t)
        # Handle `tuple[X, ...]` (homogeneous)
        if len(args) == 2 and args[1] is Ellipsis:
            return f"{_emit_type(args[0], ctx)}[]"
        return "[" + ", ".join(_emit_type(a, ctx) for a in args) + "]"
    if origin is typing.Union or origin is pytypes.UnionType:
        return " | ".join(_emit_type(a, ctx) for a in typing.get_args(t))
    if origin is typing.Literal:
        parts: list[str] = []
        for a in typing.get_args(t):
            parts.append(f'"{a}"' if isinstance(a, str) else repr(a))
        return " | ".join(parts)

    raise GeneratorError(f"Unmapped origin {origin!r} for type {t!r}")


def _ts_name_for(cls: type, ctx: _Ctx) -> str:
    """Assign a TS-unique name for a Python class, disambiguating by module on collision.

    Same bare `__name__` appearing in two modules (e.g. vgi.invocation.FunctionType
    vs vgi.catalog.catalog_interface.FunctionType) would emit a TS duplicate. Use
    the class's last module segment (capitalized) as a prefix for the later one so
    both round-trip cleanly.
    """
    existing = {ctx.dataclasses_seen.get(c) or ctx.enums_seen.get(c): c
                for c in (*ctx.dataclasses_seen, *ctx.enums_seen)}
    name = cls.__name__
    if name not in existing:
        return name
    # Collision: module-qualify
    mod_tail = cls.__module__.rsplit(".", 1)[-1]
    prefix = "".join(p.capitalize() for p in mod_tail.split("_"))
    return f"{prefix}{name}"


def _register_enum(cls: type, ctx: _Ctx) -> str:
    if cls in ctx.enums_seen:
        return ctx.enums_seen[cls]
    ts_name = _ts_name_for(cls, ctx)
    ctx.enums_seen[cls] = ts_name
    ctx.enum_order.append(cls)
    return ts_name


def _register_dataclass(cls: type, ctx: _Ctx) -> str:
    if cls in ctx.dataclasses_seen:
        return ctx.dataclasses_seen[cls]
    ts_name = _ts_name_for(cls, ctx)
    ctx.dataclasses_seen[cls] = ts_name
    hints = typing.get_type_hints(cls, include_extras=True)
    for f in dataclasses.fields(cls):
        t = hints.get(f.name, f.type)
        _emit_type(t, ctx)
    ctx.dataclass_order.append(cls)
    return ts_name


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #


def _render_enum(cls: type, ctx: _Ctx) -> str:
    """Render an enum as a string-literal union export."""
    members = (
        [m.value for m in cls]  # type: ignore[attr-defined]
        if cls in _ENUM_VALUE_OVERRIDES
        else [m.name for m in cls]  # type: ignore[attr-defined]
    )
    body = " | ".join(f'"{m}"' for m in members)
    return f"export type {ctx.enums_seen[cls]} = {body};\n"


def _inner_opaque_semantic(t: typing.Any) -> str | None:
    """For an `Annotated[T, binary]` field returning Uint8Array, return the JSDoc tag."""
    t, arrow_meta = _unwrap(t)
    if arrow_meta is None:
        return None
    inner = _strip_optional(t) if _is_optional(t) else t
    tag = _opaque_tag_for(inner)
    if tag is not None:
        return tag
    if typing.get_origin(inner) is list:
        (li,) = typing.get_args(inner)
        tag = _opaque_tag_for(li)
        if tag is not None:
            return tag
    return None


def _render_dataclass(cls: type, ctx: _Ctx) -> str:
    hints = typing.get_type_hints(cls, include_extras=True)
    ts_name = ctx.dataclasses_seen[cls]
    lines = [f"export interface {ts_name} {{"]
    info_inner: type | None = INFO_ENVELOPES.get(cls.__name__)
    for f in dataclasses.fields(cls):
        # INFO_TYPES envelope: promote items:list[bytes] to typed array
        if info_inner is not None and f.name == "items":
            inner_name = _register_dataclass(info_inner, ctx)
            lines.append(f"  items: {inner_name}[];")
            continue

        t = hints.get(f.name, f.type)
        has_default = (
            f.default is not dataclasses.MISSING
            or f.default_factory is not dataclasses.MISSING
        )
        optional = "?" if has_default else ""
        ts = _emit_type(t, ctx)
        tag = _inner_opaque_semantic(t)
        tag_comment = f" /** {tag} */" if tag else ""
        lines.append(f"  {f.name}{optional}:{tag_comment} {ts};")
    lines.append("}\n")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #


def _collect(ctx: _Ctx) -> None:
    # Pre-register INFO_TYPES so they appear even if only reached via envelopes.
    for cls in INFO_ENVELOPES.values():
        _register_dataclass(cls, ctx)

    methods = rpc_methods(VgiProtocol)
    for name in sorted(methods.keys()):
        info = methods[name]
        for pt in info.param_types.values():
            _emit_type(pt, ctx)
        if info.has_return and info.result_type is not None:
            _emit_type(info.result_type, ctx)


def emit(out: TextIO) -> None:
    """Write the generated TS client source to `out`."""
    ctx = _Ctx()
    _collect(ctx)

    body = io.StringIO()
    # Imports for the codec wrappers emitted at the bottom of the file.
    body.write('import { encodeASD, decodeASD } from "../codec/asd.js";\n')
    info_schema_imports = sorted(
        f"{cls.__name__}Schema" for cls in INFO_ENVELOPES.values()
    )
    body.write("import {\n")
    for s in info_schema_imports:
        body.write(f"  {s},\n")
    body.write('} from "./vgi-protocol-schemas.js";\n\n')

    # Deterministic order: enums first (sorted by name), then dataclasses
    # in registration order (which mirrors Protocol method-walk order + field
    # traversal — already deterministic because rpc_methods + sorted keys).
    for e in sorted(ctx.enum_order, key=lambda c: ctx.enums_seen[c]):
        body.write(_render_enum(e, ctx))
    body.write("\n")
    for cls in ctx.dataclass_order:
        body.write(_render_dataclass(cls, ctx))
        body.write("\n")

    # Codec wrappers for the INFO_TYPES — one encode/decode per Info dataclass.
    # These are what make the typed interface honest at the wire boundary.
    body.write("// ----------------------------------------------------------------------------\n")
    body.write("// Codec wrappers: turn the typed interfaces above into actual wire bytes.\n")
    body.write("// ----------------------------------------------------------------------------\n\n")
    for info_cls in sorted(
        set(INFO_ENVELOPES.values()), key=lambda c: c.__name__,
    ):
        ts_name = ctx.dataclasses_seen[info_cls]
        schema_ref = f"{info_cls.__name__}Schema"
        body.write(
            f"export const encode{ts_name} = (v: {ts_name}): Uint8Array => "
            f"encodeASD({schema_ref}, v);\n",
        )
        body.write(
            f"export const decode{ts_name} = (b: Uint8Array): {ts_name} => "
            f"decodeASD<{ts_name}>({schema_ref}, b);\n",
        )
    body.write("\n")

    out.write("// ============================================================================\n")
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.ts_client",
            generator_command="vgi-gen-ts-client",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[
                "uv run --project ~/Development/vgi-python vgi-gen-ts-client \\",
                "  > ~/Development/vgi-typescript/src/generated/vgi-client.ts",
            ],
            body=body.getvalue(),
        )
    )
    out.write("// ============================================================================\n")
    out.write("\n")
    out.write(body.getvalue())


def main() -> None:
    """CLI entrypoint: write the generated TS client to stdout."""
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
