# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Complete macro overload families, independent of engine-private ASTs.

Declaration order is preserved; the receiving SQL binder owns overload
resolution, coercions and ambiguity errors. Legacy documentation schemas never
declare parameter types: their types describe literal defaults only.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Annotated, Any, Self, cast

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType
from vgi_rpc.utils import IpcValidation

from vgi.argument_spec import macro_arguments_schema, macro_parameter_docs_from_schema
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.layout import SqlExpression
from vgi.schema_path import SchemaPath, schema_path_key
from vgi.typed_values import TypedValueBatch

MACRO_PROFILE = "duckdb.macro.v1"
MACRO_DEFAULT_PROFILE = "duckdb.macro.default.v1"
MAX_MACRO_BYTES = 8 * 1024 * 1024
MAX_MACRO_OVERLOADS = 256
MAX_MACRO_PARAMETERS = 1024
_ASCII_LOWER = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


def _name(name: str) -> None:
    if not isinstance(name, str) or not name or "\0" in name or len(name.encode()) > 4096:
        raise ValueError("macro identifiers must be nonempty, bounded strings without NUL")


class _MacroRecord(ArrowSerializableDataclass):
    @classmethod
    def deserialize_from_bytes(cls, data: bytes, ipc_validation: IpcValidation = IpcValidation.FULL) -> Self:
        """Reject multiple rows, partial schemas and oversized envelopes."""
        if not isinstance(data, bytes) or not 0 < len(data) <= MAX_MACRO_BYTES:
            raise ValueError("invalid macro record size")
        return cls.deserialize_from_batch(TypedValueBatch.deserialize_from_bytes(data).batch)

    @classmethod
    def deserialize_from_batch(
        cls,
        batch: pa.RecordBatch,
        custom_metadata: pa.KeyValueMetadata | None = None,
        *,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> Self:
        """Validate the complete Arrow shape and required nested fields."""
        if batch.num_rows != 1 or not batch.schema.equals(cls.ARROW_SCHEMA, check_metadata=False):
            raise ValueError(f"{cls.__name__} requires one row with its complete schema")
        if batch.nbytes > MAX_MACRO_BYTES:
            raise ValueError("macro record is too large")
        TypedValueBatch(batch)
        return super().deserialize_from_batch(batch, custom_metadata, ipc_validation=IpcValidation.FULL)

    def serialize_to_bytes(self) -> bytes:
        """Revalidate mutable collections before serializing a complete record."""
        if validate := getattr(self, "__post_init__", None):
            validate()
        result = super().serialize_to_bytes()
        if len(result) > MAX_MACRO_BYTES:
            raise ValueError("macro record is too large")
        return result


@dataclass(frozen=True, kw_only=True)
class MacroParameter(_MacroRecord):
    """One parameter with independent declared type and optional default.

    ``declared_type`` is absent for an untyped parameter; otherwise it is a
    singleton schema with this exact name and nullable field. Its recursive
    metadata is logical type identity, not documentation. Literal default NULL
    is a present one-row value. A parameter cannot have both default forms.
    """

    name: str
    declared_type: Annotated[pa.Schema | None, ArrowType(pa.binary())] = None
    default_value: Annotated[TypedValueBatch | None, ArrowType(pa.binary())] = None
    default_expression: Annotated[SqlExpression | None, ArrowType(pa.binary())] = None
    documentation: str | None = None

    def __post_init__(self) -> None:
        """Preserve absence, typed NULL and explicit declaration separately."""
        _name(self.name)
        if self.declared_type is not None:
            if not isinstance(self.declared_type, pa.Schema) or self.declared_type.names != [self.name]:
                raise ValueError("declared macro type must be a singleton schema named for its parameter")
            if not self.declared_type.field(0).nullable or pa.types.is_null(self.declared_type.field(0).type):
                raise ValueError("declared macro type must be nullable and cannot be the null placeholder type")
            if self.declared_type.serialize().size > 1024 * 1024:
                raise ValueError("declared macro type schema is too large")
        if self.default_value is not None:
            if not isinstance(self.default_value, TypedValueBatch):
                raise TypeError("macro literal defaults require TypedValueBatch")
            self.default_value.__post_init__()
            if self.default_value.schema.names != [self.name]:
                raise ValueError("macro literal default must contain exactly its named parameter")
            if self.default_value.batch.nbytes > MAX_MACRO_BYTES:
                raise ValueError("macro literal default is too large")
        if self.default_expression is not None:
            if not isinstance(self.default_expression, SqlExpression):
                raise TypeError("macro expression defaults require SqlExpression")
            self.default_expression.__post_init__()
            if self.default_expression.profile != MACRO_DEFAULT_PROFILE:
                raise ValueError("unsupported macro default expression profile")
        if self.default_value is not None and self.default_expression is not None:
            raise ValueError("macro default value and default expression are mutually exclusive")
        if self.documentation is not None and (
            not isinstance(self.documentation, str)
            or "\0" in self.documentation
            or len(self.documentation.encode()) > 64 * 1024
        ):
            raise ValueError("invalid macro parameter documentation")

    @property
    def has_default(self) -> bool:
        """Distinguish required parameters from any present default, including NULL."""
        return self.default_value is not None or self.default_expression is not None


@dataclass(frozen=True, kw_only=True)
class MacroOverload(_MacroRecord):
    """One scalar expression or table query in a complete overload family.

    ``parameters`` includes required and defaulted parameters in declaration
    order. ``schema_path`` is raw and attachment-relative; it never contains the
    client's catalog alias. No overload index is a stable remote object handle.
    """

    parameters: list[MacroParameter]
    definition: str
    profile: str
    schema_path: SchemaPath
    macro_type: str
    evaluation_context: QueryEvaluationContext

    def __post_init__(self) -> None:
        """Validate declaration ordering and the selected semantic profile."""
        if self.macro_type not in {"scalar", "table"}:
            raise ValueError("macro_type must be scalar or table")
        if self.profile != MACRO_PROFILE:
            raise ValueError("unsupported macro definition profile")
        if not isinstance(self.evaluation_context, QueryEvaluationContext):
            raise TypeError("macro overload requires an explicit evaluation context")
        self.evaluation_context.__post_init__()
        schema_path_key(self.schema_path)
        if len(self.schema_path) > 32:
            raise ValueError("macro owner schema is too deep")
        for name in self.schema_path:
            _name(name)
        if not isinstance(self.definition, str) or not self.definition.strip() or "\0" in self.definition:
            raise ValueError("macro definition must be nonempty SQL without NUL")
        if len(self.definition.encode()) > 1024 * 1024:
            raise ValueError("macro definition is too large")
        if not isinstance(self.parameters, list) or len(self.parameters) > MAX_MACRO_PARAMETERS:
            raise ValueError("invalid macro parameter collection")
        names: set[str] = set()
        seen_default = False
        for parameter in self.parameters:
            if not isinstance(parameter, MacroParameter):
                raise TypeError("macro parameters require MacroParameter records")
            parameter.__post_init__()
            key = parameter.name.translate(_ASCII_LOWER)
            if key in names:
                raise ValueError("macro parameter names must be unique under ASCII case folding")
            names.add(key)
            if seen_default and not parameter.has_default:
                raise ValueError("required macro parameter cannot follow a defaulted parameter")
            seen_default |= parameter.has_default
            if (
                parameter.default_expression is not None
                and parameter.default_expression.schema_path != self.schema_path
            ):
                raise ValueError("macro default expression must use its overload owner schema")
            if (
                parameter.default_expression is not None
                and parameter.default_expression.evaluation_context != self.evaluation_context
            ):
                raise ValueError("macro default expression must use its overload evaluation context")


def validate_macro_overloads(
    *,
    macro_type: str,
    schema_path: SchemaPath,
    parameters: list[str],
    definition: str,
    parameter_default_values: pa.RecordBatch | None = None,
    arguments_schema: pa.Schema | None = None,
    overloads: list[MacroOverload] | None = None,
) -> None:
    """Reject ambiguous dual forms and validate the complete replacement family.

    ``None`` retains the existing legacy contract. A new collection is never a
    patch and must not carry a duplicate first overload in legacy fields.
    """
    if overloads is None:
        return
    if not isinstance(overloads, list) or not 1 <= len(overloads) <= MAX_MACRO_OVERLOADS:
        raise ValueError("macro overloads must be a nonempty bounded collection")
    if parameters != [] or definition != "" or parameter_default_values is not None or arguments_schema is not None:
        raise ValueError("macro overload collection conflicts with legacy single-definition fields")
    for overload in overloads:
        if not isinstance(overload, MacroOverload):
            raise TypeError("macro overload collection requires MacroOverload records")
        overload.__post_init__()
        if overload.macro_type != macro_type or overload.schema_path != schema_path:
            raise ValueError("macro overload kind and owner must equal the parent macro")


def validate_macro_family(*, macro_type: str, schema_path: SchemaPath, overloads: list[MacroOverload]) -> None:
    """Validate the sole wire representation, including mandatory nonempty family."""
    if overloads is None:
        raise ValueError("macro overloads must be a nonempty bounded collection")
    validate_macro_overloads(
        macro_type=macro_type, schema_path=schema_path, parameters=[], definition="", overloads=overloads
    )


def normalize_macro_overloads(
    *,
    macro_type: str,
    schema_path: SchemaPath,
    parameters: list[str],
    definition: str,
    parameter_default_values: pa.RecordBatch | None = None,
    arguments_schema: pa.Schema | None = None,
    overloads: list[MacroOverload] | None = None,
    evaluation_context: QueryEvaluationContext | None = None,
) -> list[MacroOverload]:
    """Upgrade a legacy definition without inventing declared parameter types.

    Unknown legacy documentation metadata is rejected rather than discarded.
    New collections are copied through the actual wire codec, which validates
    and prevents later caller mutation from changing a stored definition.
    """
    validate_macro_overloads(
        macro_type=macro_type,
        schema_path=schema_path,
        parameters=parameters,
        definition=definition,
        parameter_default_values=parameter_default_values,
        arguments_schema=arguments_schema,
        overloads=overloads,
    )
    if overloads is not None:
        return [MacroOverload.deserialize_from_bytes(item.serialize_to_bytes()) for item in overloads]
    if not isinstance(evaluation_context, QueryEvaluationContext):
        raise ValueError("legacy macro upgrade requires the owner's evaluation context")
    if parameter_default_values is not None:
        TypedValueBatch(parameter_default_values)
        if any(name not in parameters for name in parameter_default_values.schema.names):
            raise ValueError("legacy macro defaults name an unknown parameter")
    docs = macro_parameter_docs_from_schema(arguments_schema) if arguments_schema is not None else {}
    if arguments_schema is not None and not arguments_schema.equals(
        macro_arguments_schema(parameters, parameter_default_values, docs), check_metadata=True
    ):
        raise ValueError("legacy arguments_schema has unsupported or inconsistent documentation metadata")
    result = MacroOverload(
        evaluation_context=evaluation_context,
        macro_type=macro_type,
        schema_path=list(schema_path),
        profile=MACRO_PROFILE,
        definition=definition,
        parameters=[
            MacroParameter(
                name=name,
                documentation=docs.get(name),
                default_value=TypedValueBatch(parameter_default_values.select([name]))
                if parameter_default_values is not None and name in parameter_default_values.schema.names
                else None,
            )
            for name in parameters
        ],
    )
    return [result]


def legacy_macro_fields(
    overloads: list[MacroOverload], *, evaluation_context: QueryEvaluationContext | None = None
) -> dict[str, Any]:
    """Return a lossless legacy definition, or reject unsupported information.

    This compatibility helper never chooses the first overload, evaluates an
    expression, erases a declaration, or converts Arrow values to SQL strings.
    """
    if len(overloads) != 1:
        raise ValueError("legacy macros cannot represent multiple overloads")
    overload = overloads[0]
    overload.__post_init__()
    if evaluation_context != overload.evaluation_context:
        raise ValueError("legacy macros require a matching explicit destination evaluation context")
    if any(parameter.declared_type is not None for parameter in overload.parameters):
        raise ValueError("legacy macros cannot represent declared parameter types")
    if any(parameter.default_expression is not None for parameter in overload.parameters):
        raise ValueError("legacy macros cannot represent expression defaults")
    if any(parameter.documentation == "" for parameter in overload.parameters):
        raise ValueError("legacy macros cannot preserve explicitly empty documentation")
    defaults = [parameter.default_value for parameter in overload.parameters if parameter.default_value is not None]
    default_batch = None
    if defaults:
        source_metadata = defaults[0].schema.metadata
        metadata = cast("dict[bytes | str, bytes | str] | None", source_metadata)
        if any(default.schema.metadata != metadata for default in defaults):
            raise ValueError("legacy macro defaults cannot merge different schema metadata")
        default_batch = pa.RecordBatch.from_arrays(
            [default.batch.column(0) for default in defaults],
            schema=pa.schema([default.schema.field(0) for default in defaults], metadata=metadata),
        )
    names = [parameter.name for parameter in overload.parameters]
    docs = {parameter.name: parameter.documentation for parameter in overload.parameters if parameter.documentation}
    return {
        "macro_type": overload.macro_type,
        "schema_path": list(overload.schema_path),
        "parameters": names,
        "definition": overload.definition,
        "parameter_default_values": default_batch,
        "arguments_schema": macro_arguments_schema(names, default_batch, docs),
    }


def canonicalize_macro_defaults(
    overloads: list[MacroOverload],
    resolve: Callable[[MacroOverload, MacroParameter], TypedValueBatch],
) -> list[MacroOverload]:
    """Resolve defaults into a new complete family before catalog publication.

    The owning SQL binder supplies ``resolve``: it must reject nonconstant or
    unsupported expressions, apply declared-type coercions, and return the
    resolved literal. This helper neither runs SQL nor mutates the original.
    If any default fails, no candidate family is returned for publication.
    """
    if not 1 <= len(overloads) <= MAX_MACRO_OVERLOADS:
        raise ValueError("macro overloads must be a nonempty bounded collection")
    result = []
    for original in overloads:
        overload = MacroOverload.deserialize_from_bytes(original.serialize_to_bytes())
        parameters = [
            replace(parameter, default_value=resolve(overload, parameter), default_expression=None)
            if parameter.has_default
            else parameter
            for parameter in overload.parameters
        ]
        result.append(replace(overload, parameters=parameters))
    return result
