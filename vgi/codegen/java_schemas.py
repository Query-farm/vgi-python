# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Emit the VGI protocol's Arrow schemas as a Java class, for vgi-java.

Unlike the C++ / Go / Rust / TypeScript emitters, whose output is *used* —
those SDKs validate incoming batches against the generated schemas — vgi-java
derives every schema it writes from the record declaration itself, via
vgi-rpc-java's ``RecordCodec``. It therefore has no runtime need for this file
and it is emitted into the TEST source set.

What it is for is the hole that leaves. When one description of a schema exists,
nothing can disagree with it — and nothing can catch it being *wrong*. That is
not hypothetical: ``PlanResponse`` declared two components ``@Nullable`` that the
protocol declares non-null, and because the codec derived the wire bytes from
that same declaration, every test in the SDK passed while the C++ client
rejected the whole response as an "out-of-date Apache Arrow schema". This file
is the second description, taken from the protocol rather than from Java, so
``WireRecordSchemaConformanceTest`` has something to compare against.

Multirepo workflow, same as the other emitters: modify the dataclass here, run
the generator, and commit the regenerated file in the sibling repo.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python -m vgi.codegen.java_schemas \\
     > ~/vgi-java/vgi/src/test/java/farm/query/vgi/generated/VgiProtocolSchemas.java

``tests/test_generated_java_schemas.py`` fails when the checked-in file and the
generator disagree.
"""

from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from vgi.protocol import (
    AggregateBindRequest,
    AggregateCombineRequest,
    AggregateDestructorRequest,
    AggregateFinalizeRequest,
    AggregateUpdateRequest,
    BindRequest,
    CatalogAttachRequest,
    CopyFromContext,
    CopyToContext,
    GlobalInitResponse,
    InitRequest,
    TableBufferingCombineRequest,
    TableBufferingDestructorRequest,
    TableBufferingProcessRequest,
    TableFunctionCardinalityRequest,
    TableFunctionPlanRequest,
)
from vgi.codegen._common import (
    EXTRA_RESPONSE_TYPES,
    EmittedSchema,
    GeneratorError,
    collect_schemas,
    provenance_comment,
)

if TYPE_CHECKING:
    from typing import TextIO


# Dataclasses vgi-java mirrors as wire records but which `collect_schemas()`
# does not reach. Two kinds, both invisible to a walk over method signatures:
#
#   - REQUEST records. A method's params schema is the outer envelope
#     (`request: binary`); the request dataclass itself rides inside it as an
#     opaque blob, so only the endpoints know its shape. C++ writes these and
#     Java reads them, which is exactly the pair that can disagree.
#   - `GlobalInitResponse`, whose method (`init`) is a STREAM and so has no
#     unary result schema, and the two `Copy*Context` records, which appear
#     only as nested struct columns of `BindRequest`.
#
# Emitted for Java alone. The other emitters serve clients that validate
# *responses*, so widening the shared list would churn four checked-in
# generated files for schemas none of them reference.
JAVA_EXTRA_TYPES: tuple[type, ...] = (
    AggregateBindRequest,
    AggregateCombineRequest,
    AggregateDestructorRequest,
    AggregateFinalizeRequest,
    AggregateUpdateRequest,
    BindRequest,
    CatalogAttachRequest,
    CopyFromContext,
    CopyToContext,
    GlobalInitResponse,
    InitRequest,
    TableBufferingCombineRequest,
    TableBufferingDestructorRequest,
    TableBufferingProcessRequest,
    TableFunctionCardinalityRequest,
    TableFunctionPlanRequest,
)


_INT_WIDTHS = (8, 16, 32, 64)


def _emit_type(dtype: pa.DataType, *, origin: str) -> str:
    """Render an Arrow type as an arrow-java ``ArrowType`` expression."""
    if pa.types.is_boolean(dtype):
        return "new ArrowType.Bool()"

    if pa.types.is_integer(dtype):
        width = dtype.bit_width
        if width not in _INT_WIDTHS:
            raise GeneratorError(f"integer at {origin} has unsupported bit width {width}.")
        signed = "true" if pa.types.is_signed_integer(dtype) else "false"
        return f"new ArrowType.Int({width}, {signed})"

    if pa.types.is_float32(dtype):
        return "new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)"
    if pa.types.is_float64(dtype):
        return "new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)"

    if pa.types.is_string(dtype):
        return "new ArrowType.Utf8()"
    if pa.types.is_large_string(dtype):
        return "new ArrowType.LargeUtf8()"
    if pa.types.is_binary(dtype):
        return "new ArrowType.Binary()"
    if pa.types.is_large_binary(dtype):
        return "new ArrowType.LargeBinary()"

    if pa.types.is_timestamp(dtype):
        unit_map = {
            "s": "TimeUnit.SECOND",
            "ms": "TimeUnit.MILLISECOND",
            "us": "TimeUnit.MICROSECOND",
            "ns": "TimeUnit.NANOSECOND",
        }
        unit_expr = unit_map.get(dtype.unit)
        if unit_expr is None:
            raise GeneratorError(
                f"timestamp at {origin} has unknown unit {dtype.unit!r}; expected one of {sorted(unit_map)}.",
            )
        tz = "null" if dtype.tz is None else f'"{dtype.tz}"'
        return f"new ArrowType.Timestamp({unit_expr}, {tz})"

    if pa.types.is_list(dtype):
        return "new ArrowType.List()"
    if pa.types.is_large_list(dtype):
        return "new ArrowType.LargeList()"
    if pa.types.is_struct(dtype):
        return "new ArrowType.Struct()"
    if pa.types.is_map(dtype):
        # keysSorted is false for every map pyarrow builds via pa.map_().
        return "new ArrowType.Map(false)"

    raise GeneratorError(
        f"vgi.codegen.java_schemas: unsupported Arrow type {type(dtype).__name__!r} at {origin} "
        f"(type={dtype!r}).\n"
        "To support this type, add a case to _emit_type() in vgi/codegen/java_schemas.py.",
    )


def _emit_field(field: pa.Field[Any], *, origin: str, indent: str) -> str:
    """Render one field as an arrow-java ``Field`` expression, children inline."""
    path = f"{origin}.{field.name}"
    dtype = field.type
    nullable = "true" if field.nullable else "false"

    if pa.types.is_dictionary(dtype):
        # Arrow names the VALUE type on the field and carries the INDEX type in
        # the encoding; the dictionary id only has to agree with the dictionary
        # batch beside it in the same stream, so it is emitted as 0 and the
        # comparison ignores it.
        value_expr = _emit_type(dtype.value_type, origin=f"{path}[dict value]")
        index_expr = _emit_type(dtype.index_type, origin=f"{path}[dict index]")
        ordered = "true" if dtype.ordered else "false"
        return (
            f'{indent}dict("{field.name}", {nullable}, {value_expr},\n'
            f"{indent}        new DictionaryEncoding(0L, {ordered}, {index_expr}))"
        )

    children = _child_fields(dtype, origin=path)
    type_expr = _emit_type(dtype, origin=path)
    if not children:
        return f'{indent}f("{field.name}", {nullable}, {type_expr})'

    inner = ",\n".join(
        _emit_field(child, origin=path, indent=indent + "        ") for child in children
    )
    return f'{indent}f("{field.name}", {nullable}, {type_expr},\n{inner})'


def _child_fields(dtype: pa.DataType, *, origin: str) -> list[pa.Field[Any]]:
    """The Arrow child fields of a nested type, in wire order.

    A map's single child is the ``entries`` struct that arrow-java requires;
    pyarrow models the same shape as key/item fields, so it is rebuilt here
    rather than read off the type.
    """
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return [dtype.value_field]
    if pa.types.is_struct(dtype):
        return [dtype.field(i) for i in range(dtype.num_fields)]
    if pa.types.is_map(dtype):
        key_field = dtype.key_field
        item_field = dtype.item_field
        if key_field.nullable:
            raise GeneratorError(f"map at {origin} has a nullable key field, which Arrow forbids.")
        entries = pa.field(
            "entries",
            pa.struct([key_field.with_name("key"), item_field.with_name("value")]),
            nullable=False,
        )
        return [entries]
    return []


def _method_name(name: str) -> str:
    """The Java accessor stem for an emitted schema (``BindResult`` -> ``bindResult``)."""
    return name[:1].lower() + name[1:]


def _emit_schema_method(es: EmittedSchema) -> str:
    body = [
        "    /**",
        f"     * The {es.name} schema.",
        "     *",
        f"     * <p>Origin: {es.origin}</p>",
        "     *",
        "     * @return the schema.",
        "     */",
        f"    private static Schema {_method_name(es.name)}() {{",
    ]
    fields = list(es.schema)
    if not fields:
        body.append("        return new Schema(List.of());")
    else:
        rendered = ",\n".join(
            _emit_field(f, origin=es.name, indent="                ") for f in fields
        )
        body.append("        return new Schema(List.of(")
        body.append(rendered + "));")
    body.append("    }")
    return "\n".join(body)


GENERATOR_VERSION = "1"

_PREAMBLE = '''package farm.query.vgi.generated;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;

/**
 * Every Arrow schema the VGI protocol defines, generated from the protocol
 * itself rather than from this SDK's record declarations.
 *
 * <p>vgi-java writes its wire schemas by deriving them from record components,
 * so within the SDK there is only one description of each schema and nothing
 * can contradict it — including when it is wrong. This class is the second
 * description, so {@code WireRecordSchemaConformanceTest} has something to
 * compare a record against. It is test-only; nothing in the published library
 * reads it.</p>
 *
 * <p>Names are the protocol's, not Java's: {@code "BindResult"} is the result
 * record of the {@code bind} method, {@code "InitRequest"} the request record
 * carried inside {@code init}'s params envelope. The test owns the mapping from
 * Java record to schema name, because it is not always one word — Java's
 * {@code CardinalityResponse} is the protocol's
 * {@code TableFunctionCardinalityResult}.</p>
 *
 * <p>Dictionary ids are emitted as 0 and are not meaningful: an id only has to
 * agree between a schema field and the dictionary batch beside it in the same
 * stream.</p>
 */
public final class VgiProtocolSchemas {

    private VgiProtocolSchemas() {
    }

    /**
     * Every generated schema, keyed by its protocol name.
     *
     * @return an immutable map from schema name to schema.
     */
    public static Map<String, Schema> byName() {
        return SCHEMAS;
    }

    /**
     * One generated schema by its protocol name.
     *
     * @param name the protocol schema name, e.g. {@code "BindResult"}.
     * @return the schema.
     * @throws IllegalArgumentException if no schema has that name.
     */
    public static Schema get(String name) {
        Schema schema = SCHEMAS.get(name);
        if (schema == null) {
            throw new IllegalArgumentException("no generated schema named '" + name
                    + "'; known names: " + SCHEMAS.keySet());
        }
        return schema;
    }

    private static Field f(String name, boolean nullable, ArrowType type, Field... children) {
        return new Field(name, new FieldType(nullable, type, null), List.of(children));
    }

    private static Field dict(String name, boolean nullable, ArrowType valueType,
            DictionaryEncoding encoding) {
        return new Field(name, new FieldType(nullable, valueType, encoding), List.of());
    }
'''


def emit(out: TextIO) -> None:
    """Emit the generated Java schema class to *out*."""
    schemas = collect_schemas(
        extra_response_types=(*EXTRA_RESPONSE_TYPES, *JAVA_EXTRA_TYPES),
    )

    body = io.StringIO()
    body.write("// Copyright 2025, 2026 Query Farm LLC - https://query.farm\n")
    body.write("\n")
    body.write(_PREAMBLE)
    body.write("\n")
    for es in schemas:
        body.write(_emit_schema_method(es))
        body.write("\n\n")

    body.write("    private static final Map<String, Schema> SCHEMAS = Map.ofEntries(\n")
    entries = ",\n".join(
        f'            Map.entry("{es.name}", {_method_name(es.name)}())' for es in schemas
    )
    body.write(entries + ");\n")
    body.write("}\n")

    out.write(
        provenance_comment(
            generator_module="vgi.codegen.java_schemas",
            generator_command="python -m vgi.codegen.java_schemas",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[
                "uv run --project ~/Development/vgi-python python -m vgi.codegen.java_schemas \\",
                "  > ~/vgi-java/vgi/src/test/java/farm/query/vgi/generated/VgiProtocolSchemas.java",
            ],
            body=body.getvalue(),
        )
    )
    out.write("\n")
    out.write(body.getvalue())


def main() -> None:
    """Console-script entrypoint — write the Java schema class to stdout."""
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
