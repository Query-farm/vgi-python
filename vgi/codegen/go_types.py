# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit VGI protocol records as Go structs, for vgi-go.

vgi-go's transport (vgi-rpc-go) *derives* a record's wire schema from a Go
struct by reflection over ``vgirpc:"name[,option]"`` struct tags: declaration
order is field order, a pointer is a nullable column, ``[]byte`` is ``binary``,
``[][]byte`` is ``list<binary>``, ``map[string]string`` is
``map<string, string>``, a ``,enum`` tag is ``dictionary<int16, utf8>``. Those
structs used to be written by hand next to the schema factories
`vgi.codegen.go_schemas` emits, and nothing but a build-tagged test compared
the two, so a field added to a record in Python had to be mirrored by hand —
in the right position, with the right nullability — or the worker sent a shape
the client rejects.

This emitter writes those structs from each record's ``ARROW_SCHEMA`` (the
authority for field order, types and nullability). Anything vgi-rpc-go's
derivation cannot express raises ``GeneratorError`` rather than emitting a
struct whose derived schema would silently differ; vgi-go's
``vgi/generated/protocol_types_test.go`` checks every emitted struct's derived
schema against the matching ``generated.<Name>Schema`` field for field.

Which records are emitted is data: `GO_TYPE_RECORDS`. Add a record class there
(and, if its Go name differs, to `GO_NAMES`) and regenerate.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py

``tests/test_generated_go_types.py`` fails when the checked-in file and the
generator disagree.
"""

from __future__ import annotations

import enum
import io
import re
import sys
import textwrap
import typing
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from vgi.catalog.catalog_interface import CatalogAttachResult
from vgi.codegen._common import SHARED_CONTRACT_TYPES, GeneratorError, nested_binary_record, provenance_comment
from vgi.codegen.csharp_types import _base, _hints, _parse_docstring
from vgi.protocol import CatalogContentsResponse, SchemaContents

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "2"

#: The records emitted as Go structs, in output order.
#:
#: - ``CatalogAttachResult`` is the ``catalog_attach`` result.
#: - ``CatalogContentsResponse`` is the ``catalog_contents`` result.
#: - ``SchemaContents`` is the inline struct row of
#:   ``CatalogContentsResponse.schemas`` (``[]SchemaContents``, which vgi-rpc-go
#:   derives as ``list<struct<...>>``); it is checked on its own against
#:   ``generated.SchemaContentsSchema``.
GO_TYPE_RECORDS: tuple[type, ...] = (
    *SHARED_CONTRACT_TYPES,
    CatalogAttachResult,
    CatalogContentsResponse,
    SchemaContents,
)

#: The Go name of a record, where it is not the Python one.
GO_NAMES: dict[str, str] = {}

#: Snake-case words spelled as Go initialisms in field names.
_INITIALISMS: dict[str, str] = {
    "id": "ID",
    "ids": "IDs",
    "url": "URL",
    "uri": "URI",
    "sql": "SQL",
    "json": "JSON",
    "uuid": "UUID",
    "http": "HTTP",
    "ipc": "IPC",
    "ttl": "TTL",
}

_SCALARS: list[tuple[pa.DataType, str]] = [
    (pa.bool_(), "bool"),
    (pa.int8(), "int8"),
    (pa.int16(), "int16"),
    (pa.int32(), "int32"),
    (pa.int64(), "int64"),
    (pa.uint8(), "uint8"),
    (pa.uint16(), "uint16"),
    (pa.uint32(), "uint32"),
    (pa.uint64(), "uint64"),
    (pa.float32(), "float32"),
    (pa.float64(), "float64"),
    (pa.string(), "string"),
    (pa.binary(), "[]byte"),
]

#: Arrow types vgi-rpc-go derives only from an explicit tag option.
_TAGGED: list[tuple[pa.DataType, str, str]] = [
    (pa.large_string(), "string", "large_string"),
    (pa.large_binary(), "[]byte", "large_binary"),
]


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


@dataclass
class GoField:
    """One struct field: its wire name, Go name, Go type and tag options."""

    wire_name: str
    name: str
    go_type: str
    options: list[str]
    doc: str | None
    #: The Go record name a raw-preserving `Nested` field decodes to, else ``None``.
    nested: str | None = None
    nullable: bool = False

    @property
    def tag(self) -> str:
        """The ``vgirpc`` struct tag value."""
        return ",".join([self.wire_name, *self.options])


@dataclass
class GoRecord:
    """One emitted struct."""

    name: str
    py_name: str
    schema: pa.Schema
    fields: list[GoField]
    doc: str | None


def go_name(py_name: str) -> str:
    """The Go type name for a record's Python name."""
    return GO_NAMES.get(py_name, py_name)


def go_field_name(snake: str) -> str:
    """``attach_opaque_data`` -> ``AttachOpaqueData``, with Go initialisms."""
    parts = [p for p in snake.split("_") if p]
    if not parts:
        raise GeneratorError(f"cannot derive a Go field name from {snake!r}")
    return "".join(_INITIALISMS.get(p.lower(), p[:1].upper() + p[1:].lower()) for p in parts)


def _map(dtype: pa.DataType, annotation: object, *, origin: str) -> tuple[str, list[str]]:
    """Map one Arrow type to ``(Go type, vgirpc tag options)``."""
    for proto, go in _SCALARS:
        if dtype.equals(proto):
            return go, []
    for proto, go, option in _TAGGED:
        if dtype.equals(proto):
            return go, [option]

    if pa.types.is_dictionary(dtype):
        if not (dtype.index_type.equals(pa.int16()) and dtype.value_type.equals(pa.string()) and not dtype.ordered):
            raise GeneratorError(f"{origin}: vgi-rpc-go derives only dictionary<int16, utf8>, got {dtype}.")
        base, _ = _base(annotation)
        if annotation is not None and not (isinstance(base, type) and issubclass(base, enum.Enum | str)):
            raise GeneratorError(f"{origin}: a dictionary column must be an enum or a string, got {annotation!r}.")
        return "string", ["enum"]

    if pa.types.is_list(dtype):
        item = dtype.value_field
        if item.name != "item" or not item.nullable:
            raise GeneratorError(
                f"{origin}: vgi-rpc-go derives every list item as a nullable field named 'item'; got {item}.",
            )
        base, _ = _base(annotation)
        args = typing.get_args(base) if base is not None else ()
        elem, elem_options = _map(item.type, args[0] if args else None, origin=f"{origin}[item]")
        return f"[]{elem}", _elem_options(elem_options, origin=origin)

    if pa.types.is_map(dtype):
        key, value = dtype.key_field, dtype.item_field
        if key.nullable or not value.nullable or not pa.map_(key.type, value.type).equals(dtype):
            raise GeneratorError(
                f"{origin}: vgi-rpc-go derives map<key not null, value nullable> with the default field names; "
                f"got {dtype}.",
            )
        base, _ = _base(annotation)
        args = typing.get_args(base) if base is not None else ()
        k, k_options = _map(key.type, args[0] if args else None, origin=f"{origin}[key]")
        v, v_options = _map(value.type, args[1] if len(args) > 1 else None, origin=f"{origin}[value]")
        if k_options:
            raise GeneratorError(f"{origin}: a tag option cannot reach a map's key ({k_options}).")
        return f"map[{k}]{v}", _elem_options(v_options, origin=origin)

    if pa.types.is_struct(dtype):
        # vgi-rpc-go derives a nested (non-ArrowSerializable) Go struct as an
        # inline struct of its tagged fields, so a struct column is the emitted
        # record whose own schema is exactly this struct.
        base, _ = _base(annotation)
        if not (isinstance(base, type) and base in GO_TYPE_RECORDS):
            raise GeneratorError(f"{origin}: a struct column must be a record in GO_TYPE_RECORDS, got {annotation!r}.")
        if not pa.struct(list(base.ARROW_SCHEMA)).equals(dtype):  # type: ignore[attr-defined]
            raise GeneratorError(f"{origin}: {base.__name__}.ARROW_SCHEMA does not match the struct {dtype}.")
        return go_name(base.__name__), []

    raise GeneratorError(
        f"vgi.codegen.go_types: unsupported Arrow type {dtype} at {origin}.\n"
        "Add a case to _map() in vgi/codegen/go_types.py — and check that vgi-rpc-go's "
        "goTypeToArrowType derives it.",
    )


def _elem_options(options: list[str], *, origin: str) -> list[str]:
    """Carry an element's tag option down to it with ``elem=``; one level only."""
    if not options:
        return []
    if len(options) > 1 or options[0].startswith("elem="):
        raise GeneratorError(f"{origin}: vgi-rpc-go's elem= reaches only one level; got {options}.")
    return [f"elem={options[0]}"]


def _layout(cls: type) -> GoRecord:
    schema = cls.ARROW_SCHEMA  # type: ignore[attr-defined]
    if not isinstance(schema, pa.Schema):
        raise GeneratorError(f"{cls.__name__} has no ARROW_SCHEMA.")
    name = go_name(cls.__name__)
    hints = _hints(cls)
    summary, attr_docs = _parse_docstring(cls)
    fields: list[GoField] = []
    seen: dict[str, str] = {}
    for f in schema:
        where = f"{name}.{f.name}"
        nested = nested_binary_record(f.type, hints.get(f.name))
        if nested is not None:
            # A nested record carried as binary (large-values spec §3.3): the
            # raw-preserving wrapper, still a `binary` column on the wire.
            if nested not in GO_TYPE_RECORDS:
                raise GeneratorError(f"{where}: nested record {nested.__name__} is not in GO_TYPE_RECORDS.")
            go, options = f"Nested[{go_name(nested.__name__)}]", ["binary"]
        else:
            go, options = _map(f.type, hints.get(f.name), origin=where)
        if f.nullable:
            go = "*" + go
        field_name = go_field_name(f.name)
        if field_name in seen:
            raise GeneratorError(f"{where}: Go name {field_name} also derived from {seen[field_name]!r}.")
        seen[field_name] = f.name
        fields.append(
            GoField(
                f.name,
                field_name,
                go,
                options,
                attr_docs.get(f.name),
                nested=go_name(nested.__name__) if nested is not None else None,
                nullable=f.nullable,
            )
        )
    return GoRecord(name, cls.__name__, schema, fields, summary)


def build_records() -> list[GoRecord]:
    """Lay out every record in `GO_TYPE_RECORDS`, as Go sees them."""
    records = [_layout(cls) for cls in GO_TYPE_RECORDS]
    names = [r.name for r in records]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise GeneratorError(f"two records map to the same Go name: {dupes}")
    return records


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _go_text(text: str) -> str:
    """Docstring prose as Go doc text: cross-reference and code markup removed."""
    text = re.sub(r":[a-z]+:`([^`]+)`", r"\1", text)  # Sphinx roles (:class:`X`)
    text = re.sub(r"\[`([^`]+)`\]\[[^\]]*\]", r"\1", text)
    text = re.sub(r"``([^`]+)``", r"\1", text)
    return re.sub(r"`([^`]+)`", r"\1", text)


def _sentence(text: str) -> str:
    text = text.rstrip()
    return text if text.endswith((".", "!", "?", ":")) else text + "."


def _comment(paragraphs: list[str], indent: str) -> list[str]:
    out: list[str] = []
    for i, para in enumerate(paragraphs):
        if i:
            out.append(f"{indent}//")
        for line in textwrap.wrap(para, width=76, break_long_words=False, break_on_hyphens=False):
            out.append(f"{indent}// {line}")
    return out


def _paragraphs(text: str | None) -> list[str]:
    if not text:
        return []
    return [" ".join(_go_text(p).split()) for p in re.split(r"\n\s*\n", text) if p.strip()]


def _render_record(r: GoRecord) -> str:
    head = f"{r.name} is the protocol record {r.py_name}"
    head += "." if r.name == r.py_name else f" (named {r.name} in Go)."
    paragraphs = [head, *(_sentence(p) for p in _paragraphs(r.doc))]
    lines = _comment(paragraphs, "")
    lines.append(f"type {r.name} struct {{")
    for i, f in enumerate(r.fields):
        # A blank line between fields: each is its own gofmt alignment section,
        # so the single-space layout below is already gofmt's.
        if i:
            lines.append("")
        lines += _comment([_sentence(p) for p in _paragraphs(f.doc)], "\t")
        lines.append(f'\t{f.name} {f.go_type} `vgirpc:"{f.tag}"`')
    lines.append("}")
    for f in r.fields:
        if f.nested is not None:
            lines.append("")
            lines += _render_nested_accessor(r, f)
    return "\n".join(lines) + "\n"


def _render_nested_accessor(r: GoRecord, f: GoField) -> list[str]:
    """``<Field>Value()``: the decoded nested record, its errors naming the field path."""
    path = f"{r.py_name}.{f.wire_name}"
    method = f"{f.name}Value"
    if f.nullable:
        return [
            f"// {method} decodes {f.name} ({path}), or returns nil when the column is null.",
            "// The decode happens once per wrapper; errors name the field path.",
            f"func (r {r.name}) {method}() (*{f.nested}, error) {{",
            f"\tif r.{f.name} == nil {{",
            "\t\treturn nil, nil",
            "\t}",
            f"\tv, err := r.{f.name}.Get()",
            "\tif err != nil {",
            f'\t\treturn nil, fmt.Errorf("{path}: %w", err)',
            "\t}",
            "\treturn &v, nil",
            "}",
        ]
    return [
        f"// {method} decodes {f.name} ({path}). The decode happens once per",
        "// wrapper; errors name the field path.",
        f"func (r {r.name}) {method}() ({f.nested}, error) {{",
        f"\tv, err := r.{f.name}.Get()",
        "\tif err != nil {",
        f'\t\treturn v, fmt.Errorf("{path}: %w", err)',
        "\t}",
        "\treturn v, nil",
        "}",
    ]


#: The raw-preserving wrapper (large-values spec §3.3), emitted once when any
#: record has a nested-binary field.
_NESTED_GO = """\
// Nested is a protocol record carried as a complete Arrow IPC stream in a
// binary column (vgi docs/large-values-3.4.md §3.3), held raw-preserving:
//
//   - it holds the bytes exactly as received (a private copy);
//   - Get decodes them on first use and caches the result; concurrent first
//     use decodes once;
//   - encoding the embedding record writes the held bytes verbatim, accessed or
//     not, so a re-encoded request is byte-identical to the one received;
//   - NestedOf serializes a typed record once and then behaves exactly like a
//     wrapper decoded from those bytes;
//   - equality is over the held bytes, and printing never decodes.
//
// The zero value holds no bytes: Get reports a decode error and encoding fails.
// A nullable column is a *Nested[T]. To change a field, build a new wrapper with
// NestedOf; nothing done to a value returned by Get changes the held bytes.
//
// vgi-rpc-go carries it through encoding.BinaryMarshaler and
// encoding.BinaryUnmarshaler on a `binary`-tagged field.
type Nested[T any] struct {
\traw   []byte
\tcache *nestedCache[T]
}

type nestedCache[T any] struct {
\tonce  sync.Once
\tvalue T
\terr   error
}

// NestedFromBytes wraps bytes received elsewhere (for example a bind call kept
// from an earlier request). The bytes are copied; they are decoded on Get.
func NestedFromBytes[T any](data []byte) Nested[T] {
\treturn Nested[T]{raw: bytes.Clone(nonNil(data)), cache: &nestedCache[T]{}}
}

// NestedOf serializes value once and wraps those bytes.
func NestedOf[T any](value T) (Nested[T], error) {
\tdata, err := vgirpc.MarshalRecord(value)
\tif err != nil {
\t\tvar zero T
\t\treturn Nested[T]{}, fmt.Errorf("encoding nested %T: %w", zero, err)
\t}
\treturn Nested[T]{raw: data, cache: &nestedCache[T]{}}, nil
}

// Get returns the decoded record, decoding the held bytes on first use.
func (n Nested[T]) Get() (T, error) {
\tif n.cache == nil {
\t\tvar zero T
\t\treturn zero, fmt.Errorf("decoding nested %T: the wrapper holds no bytes", zero)
\t}
\tn.cache.once.Do(func() {
\t\tif len(n.raw) == 0 {
\t\t\tn.cache.err = fmt.Errorf("decoding nested %T: empty IPC stream", n.cache.value)
\t\t\treturn
\t\t}
\t\tif err := vgirpc.UnmarshalRecord(n.raw, &n.cache.value); err != nil {
\t\t\tn.cache.err = fmt.Errorf("decoding nested %T: %w", n.cache.value, err)
\t\t}
\t})
\treturn n.cache.value, n.cache.err
}

// Bytes returns a copy of the held bytes.
func (n Nested[T]) Bytes() []byte {
\treturn bytes.Clone(n.raw)
}

// Len is the number of held bytes.
func (n Nested[T]) Len() int {
\treturn len(n.raw)
}

// IsZero reports whether this is the zero wrapper (no bytes at all).
func (n Nested[T]) IsZero() bool {
\treturn n.cache == nil
}

// Equal compares the held bytes.
func (n Nested[T]) Equal(other Nested[T]) bool {
\treturn n.cache != nil && other.cache != nil && bytes.Equal(n.raw, other.raw)
}

// MarshalBinary returns the held bytes verbatim (never a re-encoding).
func (n Nested[T]) MarshalBinary() ([]byte, error) {
\tif n.cache == nil {
\t\tvar zero T
\t\treturn nil, fmt.Errorf("encoding nested %T: the wrapper holds no bytes", zero)
\t}
\treturn n.raw, nil
}

// UnmarshalBinary holds a copy of data, undecoded.
func (n *Nested[T]) UnmarshalBinary(data []byte) error {
\t*n = NestedFromBytes[T](data)
\treturn nil
}

// String describes the wrapper without decoding it.
func (n Nested[T]) String() string {
\tvar zero T
\treturn fmt.Sprintf("Nested[%T](%d bytes)", zero, len(n.raw))
}

// GoString describes the wrapper without decoding it.
func (n Nested[T]) GoString() string {
\treturn n.String()
}

func nonNil(data []byte) []byte {
\tif data == nil {
\t\treturn []byte{}
\t}
\treturn data
}
"""


def emit(out: TextIO) -> None:
    """Emit the generated Go record types to *out*."""
    body = io.StringIO()
    body.write("// Copyright 2025, 2026 Query Farm LLC - https://query.farm\n")
    body.write("\n")
    body.write("package generated\n")
    body.write("\n")
    records = build_records()
    blocks = [_render_record(r) for r in records]
    if any(f.nested for r in records for f in r.fields):
        body.write('import (\n\t"bytes"\n\t"fmt"\n\t"sync"\n\n\t"github.com/Query-farm/vgi-rpc-go/vgirpc"\n)\n\n')
        blocks.append(_NESTED_GO)
    # Blank line BETWEEN blocks, none trailing at EOF (gofmt rejects it).
    body.write("\n".join(blocks))

    out.write(
        provenance_comment(
            generator_module="vgi.codegen.go_types",
            generator_command="python -m vgi.codegen.go_types",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[
                "uv run --project ~/Development/vgi-python python scripts/regen_generated.py",
            ],
            body=body.getvalue(),
        )
    )
    out.write("\n")
    out.write(body.getvalue())


def main() -> None:
    """Console-script entrypoint — write the Go record types to stdout."""
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
