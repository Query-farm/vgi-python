# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit VGI protocol records as Java records, for vgi-java.

vgi-java's transport (vgi-rpc-java) *derives* every wire schema from a Java
record by reflection — component declaration order is field order, ``long`` is a
non-null ``int64``, ``@Nullable String`` a nullable ``utf8``, ``List<byte[]>`` a
``list<binary>``. A record declared by hand is therefore a second description of
the protocol that can only be checked after the fact (vgi-java's
``WireRecordSchemaConformanceTest``), and a component added in the wrong place
or with the wrong nullability is a wire bug the C++ client rejects at runtime.
This emitter writes those records from each protocol class's ``ARROW_SCHEMA`` —
the authority — instead.

It is data-driven: `JAVA_RECORDS` lists the protocol classes that are
generated, each into its own file (Java allows one public top-level type per
file) in vgi-java's ``farm.query.vgi.protocol`` package, so the generated types
keep the names vgi-java's API already uses. Add a class there to generate it;
`scripts/regen_generated.py` derives its targets from the same list.

Only Arrow shapes vgi-rpc-java's ``SchemaDerivation`` reproduces exactly are
accepted; anything else raises ``GeneratorError`` rather than emitting a record
whose derived schema would silently differ.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py

``tests/test_generated_java_types.py`` fails when a checked-in file and the
generator disagree.
"""

from __future__ import annotations

import argparse
import inspect
import io
import re
import sys
import textwrap
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from vgi.catalog.catalog_interface import CatalogAttachResult
from vgi.codegen._common import GeneratorError, provenance_comment
from vgi.protocol import CatalogContentsResponse, SchemaContents

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"

JAVA_PACKAGE = "farm.query.vgi.protocol"

#: Where the generated files live, relative to the vgi-java checkout.
JAVA_PROTOCOL_DIR = "vgi/src/main/java/farm/query/vgi/protocol"

#: Protocol classes emitted as Java records, one file each.
JAVA_RECORDS: tuple[type, ...] = (
    CatalogAttachResult,
    SchemaContents,
    CatalogContentsResponse,
)

#: The Java name of a protocol class, where it is not the Python one.
JAVA_NAMES: dict[str, str] = {}

_REGEN_LINE = "uv run --project ~/Development/vgi-python python scripts/regen_generated.py"


def java_name(cls: type) -> str:
    """The Java record name for a protocol class."""
    return JAVA_NAMES.get(cls.__name__, cls.__name__)


def _record(name: str) -> type:
    for cls in JAVA_RECORDS:
        if java_name(cls) == name:
            return cls
    raise GeneratorError(f"{name!r} is not a generated Java record; known: {sorted(record_names())}")


def record_names() -> list[str]:
    """The Java names of every generated record, in `JAVA_RECORDS` order."""
    return [java_name(cls) for cls in JAVA_RECORDS]


def targets() -> list[tuple[str, str]]:
    """``(record name, path relative to the vgi-java checkout)`` for every generated file."""
    return [(name, f"{JAVA_PROTOCOL_DIR}/{name}.java") for name in record_names()]


# ---------------------------------------------------------------------------
# Type mapping — mirrors vgi-rpc-java's SchemaDerivation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Component:
    name: str
    java_type: str
    annotations: tuple[str, ...]


#: Arrow type -> (non-null Java type, nullable Java type, @ArrowField override or None).
#: ``int64``/``float64``/``bool``/``utf8``/``binary`` are what the derivation infers
#: from the Java type alone; the rest need an explicit ``@ArrowField``.
_SCALARS: list[tuple[pa.DataType, str, str, str | None]] = [
    (pa.bool_(), "boolean", "Boolean", None),
    (pa.int64(), "long", "Long", None),
    (pa.float64(), "double", "Double", None),
    (pa.string(), "String", "String", None),
    (pa.binary(), "byte[]", "byte[]", None),
    (pa.large_binary(), "byte[]", "byte[]", "LARGE_BINARY"),
]

#: Element types a ``List<T>`` may carry with no override (the derivation infers them).
_ELEMENTS: list[tuple[pa.DataType, str]] = [
    (pa.binary(), "byte[]"),
    (pa.string(), "String"),
    (pa.int64(), "Long"),
    (pa.bool_(), "Boolean"),
    (pa.float64(), "Double"),
]

_DICT_UTF8 = pa.dictionary(pa.int16(), pa.string())


def _element(dtype: pa.DataType, origin: str) -> str:
    for proto, java in _ELEMENTS:
        if dtype.equals(proto):
            return java
    raise GeneratorError(
        f"{origin}: vgi-rpc-java cannot derive a list/map element of type {dtype} without an override; "
        "add a case to vgi/codegen/java_types.py once SchemaDerivation can express it.",
    )


def _component(f: pa.Field[Any], origin: str) -> _Component:
    """Map one Arrow field to the record component vgi-rpc-java derives it back from."""
    where = f"{origin}.{f.name}"
    nullable = ("@Nullable",) if f.nullable else ()
    dtype = f.type

    if dtype.equals(_DICT_UTF8):
        return _Component(f.name, "String", (*nullable, "@ArrowField(ArrowFieldType.DICT_INT16_UTF8)"))

    for proto, plain, boxed, override in _SCALARS:
        if dtype.equals(proto):
            ann = (f"@ArrowField(ArrowFieldType.{override})",) if override else ()
            return _Component(f.name, boxed if f.nullable else plain, (*nullable, *ann))

    if pa.types.is_list(dtype):
        item = dtype.value_field
        if item.name != "item" or not item.nullable:
            raise GeneratorError(f"{where}: vgi-rpc-java derives every list item as a nullable 'item'; got {item}.")
        return _Component(f.name, f"List<{_element(item.type, where + '[]')}>", nullable)

    if pa.types.is_map(dtype):
        key, value = dtype.key_field, dtype.item_field
        if key.nullable or not value.nullable or not pa.map_(key.type, value.type).equals(dtype):
            raise GeneratorError(
                f"{where}: vgi-rpc-java derives map<key not null, value nullable> with default field names; "
                f"got {dtype}.",
            )
        k = _element(key.type, where + "[key]")
        v = _element(value.type, where + "[value]")
        return _Component(f.name, f"Map<{k}, {v}>", nullable)

    raise GeneratorError(
        f"vgi.codegen.java_types: unsupported Arrow type {dtype} at {where}.\n"
        "Add a case to _component() in vgi/codegen/java_types.py — and check that vgi-rpc-java's "
        "SchemaDerivation derives exactly that type from the Java declaration.",
    )


# ---------------------------------------------------------------------------
# Javadoc
# ---------------------------------------------------------------------------

_ATTR_LINE = re.compile(r"^(\w+)(?: \([^)]*\))?:\s*(.*)$")


def _parse_docstring(cls: type) -> tuple[str | None, dict[str, str]]:
    """Split a Google-style docstring into its summary and its ``Attributes:`` entries."""
    raw = inspect.getdoc(cls)
    if not raw:
        return None, {}
    summary: list[str] = []
    attrs: dict[str, list[str]] = {}
    section: str | None = None
    current: str | None = None
    for line in raw.splitlines():
        stripped = line.strip()
        if re.fullmatch(r"[A-Z][A-Za-z ]*:", stripped) and not line.startswith(" "):
            section, current = stripped[:-1], None
            continue
        if section is None:
            summary.append(line)
        elif section == "Attributes":
            if line.startswith("    ") and not line.startswith("        "):
                m = _ATTR_LINE.match(stripped)
                if m:
                    current = m.group(1)
                    attrs[current] = [m.group(2)]
                    continue
            if current is not None and stripped:
                attrs[current].append(stripped)
    text = "\n".join(summary).strip()
    return (text or None), {k: " ".join(v).strip() for k, v in attrs.items()}


def _javadoc_text(text: str) -> str:
    """Render docstring prose as Javadoc: HTML-escaped, code spans as ``{@code}``."""
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("@", "&#64;")
    text = text.replace("*/", "*&#47;")
    text = re.sub(r":class:`([^`]+)`", r"{@code \1}", text)
    text = re.sub(r"\[`([^`]+)`\]\[[^\]]*\]", r"{@code \1}", text)
    text = re.sub(r"``([^`]+)``", r"{@code \1}", text)
    text = re.sub(r"`([^`]+)`", r"{@code \1}", text)
    return text


_NBSP = "\x00"


def _wrap(text: str, first: str, rest: str) -> list[str]:
    """Wrap Javadoc text without breaking inside a ``{@code ...}`` span."""
    text = re.sub(r"\{@code [^}]*\}", lambda m: m.group(0).replace(" ", _NBSP), text)
    lines = textwrap.wrap(
        text,
        width=100,
        initial_indent=first,
        subsequent_indent=rest,
        break_long_words=False,
        break_on_hyphens=False,
    )
    return [line.replace(_NBSP, " ") for line in lines]


def _javadoc(summary: str | None, params: list[tuple[str, str]], origin: str) -> list[str]:
    out = ["/**"]
    paragraphs = [" ".join(p.split()) for p in re.split(r"\n\s*\n", summary or "") if p.strip()]
    for i, para in enumerate(paragraphs):
        if i:
            out.append(" *")
        out += _wrap(("<p>" if i else "") + _javadoc_text(para), " * ", " * ")
    out.append(" *")
    out += _wrap(
        f"<p>Protocol type {{@code {origin}}}, generated from its {{@code ARROW_SCHEMA}}: component order is "
        "column order and {@code @Nullable} is wire nullability.",
        " * ",
        " * ",
    )
    out.append(" *")
    for name, doc in params:
        out += _wrap(_javadoc_text(doc), f" * @param {name} ", " *        ")
    out.append(" */")
    return out


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _render_record(cls: type) -> str:
    name = java_name(cls)
    schema = getattr(cls, "ARROW_SCHEMA", None)
    if not isinstance(schema, pa.Schema):
        raise GeneratorError(f"{cls.__qualname__} has no ARROW_SCHEMA to generate {name} from.")
    components = [_component(schema.field(i), name) for i in range(len(schema))]
    summary, attr_docs = _parse_docstring(cls)
    params = [(c.name, attr_docs.get(c.name) or f"the {{@code {c.name}}} column") for c in components]

    imports = {"farm.query.vgirpc.schema.ArrowSerializableRecord"}
    for c in components:
        if "@Nullable" in c.annotations:
            imports.add("farm.query.vgirpc.schema.Nullable")
        if any(a.startswith("@ArrowField(") for a in c.annotations):
            imports.update({"farm.query.vgirpc.schema.ArrowField", "farm.query.vgirpc.schema.ArrowFieldType"})
    java_util = sorted(
        f"java.util.{t}" for t in ("List", "Map") if any(c.java_type.startswith(f"{t}<") for c in components)
    )

    lines = ["// Copyright 2025, 2026 Query Farm LLC - https://query.farm", "", f"package {JAVA_PACKAGE};", ""]
    lines += [f"import {i};" for i in sorted(imports)]
    if java_util:
        lines.append("")
        lines += [f"import {i};" for i in java_util]
    lines.append("")
    lines += _javadoc(summary, params, f"{cls.__module__}.{cls.__qualname__}")
    lines.append(f"public record {name}(")
    for i, c in enumerate(components):
        sep = "," if i < len(components) - 1 else ") implements ArrowSerializableRecord {"
        decl = " ".join((*c.annotations, c.java_type, c.name))
        lines.append(f"        {decl}{sep}")
    lines.append("}")
    return "\n".join(lines) + "\n"


def emit(out: TextIO, *, record: str) -> None:
    """Emit the generated Java record named *record* to *out*."""
    body = _render_record(_record(record))
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.java_types",
            generator_command=f"python -m vgi.codegen.java_types {record}",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[_REGEN_LINE],
            body=body,
        )
    )
    out.write("\n")
    out.write(body)


def render(record: str) -> str:
    """The full text of the generated file for *record*."""
    buf = io.StringIO()
    emit(buf, record=record)
    return buf.getvalue()


def main() -> None:
    """Console-script entrypoint — write one generated Java record to stdout."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("record", choices=record_names(), help="the Java record to emit")
    args = parser.parse_args()
    try:
        emit(sys.stdout, record=args.record)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
