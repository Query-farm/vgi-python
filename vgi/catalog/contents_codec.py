"""Catalog-wide metadata collections using the six-stream codec introduced in 2.2.

The author-facing inventory stays grouped by schema. Collection encoders reuse
the ordinary record conversion, and readers consume every row of every batch.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.utils import new_ipc_stream

from vgi.catalog.catalog_interface import (
    FunctionInfo,
    FunctionType,
    IndexInfo,
    MacroInfo,
    MacroType,
    SchemaContentsInfo,
    SchemaInfo,
    TableInfo,
    ViewInfo,
)
from vgi.schema_path import schema_path_key

if TYPE_CHECKING:
    from vgi.protocol import CatalogContentsResponse

CATEGORY_TYPES: dict[str, type[Any]] = {
    "schemas": SchemaInfo,
    "tables": TableInfo,
    "views": ViewInfo,
    "functions": FunctionInfo,
    "macros": MacroInfo,
    "indexes": IndexInfo,
}


def encode_collection[T: ArrowSerializableDataclass](
    record_type: type[T], records: Iterable[T | bytes], *, batch_size: int = 1024
) -> bytes | None:
    """Encode records directly, accepting legacy singleton IPC bytes as an adapter.

    Each batch builds its own dictionaries; IPC dictionary replacements are
    supported. Raw IPC byte strings are never concatenated.
    """
    from vgi.protocol import _with_sorted_maps

    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    sink = pa.BufferOutputStream()
    rows: list[dict[str, object]] = []
    count = 0
    with new_ipc_stream(sink, record_type.ARROW_SCHEMA) as writer:
        for record in records:
            if isinstance(record, bytes):
                decoded = decode_collection(record_type, record)
                if len(decoded) != 1:
                    raise ValueError("legacy catalog item must contain exactly one row")
                record = decoded[0]
            if not isinstance(record, record_type):
                raise TypeError(f"expected {record_type.__name__} catalog record")
            rows.append(_with_sorted_maps(record)._to_row_dict())
            count += 1
            if len(rows) >= batch_size:
                writer.write_batch(pa.RecordBatch.from_pylist(rows, schema=record_type.ARROW_SCHEMA))
                rows.clear()
        if rows:
            writer.write_batch(pa.RecordBatch.from_pylist(rows, schema=record_type.ARROW_SCHEMA))
    return sink.getvalue().to_pybytes() if count else None


def decode_collection[T: ArrowSerializableDataclass](record_type: type[T], data: bytes | None) -> list[T]:
    """Validate one typed collection and decode all rows, including later batches."""
    if data is None:
        return []
    result: list[T] = []
    source = pa.BufferReader(data)
    with pa.ipc.open_stream(source) as reader:
        if not reader.schema.equals(record_type.ARROW_SCHEMA, check_metadata=False):
            raise ValueError(f"catalog {record_type.__name__} collection has an incompatible schema")
        for batch in reader:
            batch.validate(full=True)
            for column, field in zip(batch.columns, batch.schema, strict=True):
                for value in column.to_pylist():
                    _validate_nullability(value, field, record_type.__name__)
            result.extend(record_type.deserialize_from_batch(batch.slice(i, 1)) for i in range(batch.num_rows))
    if source.tell() != len(data):
        raise ValueError("catalog collection has trailing bytes after its IPC stream")
    return result


def _validate_nullability(value: Any, field: pa.Field[pa.DataType], path: str) -> None:
    if value is None:
        if not field.nullable:
            raise ValueError(f"catalog {path}.{field.name} contains nulls")
        return
    dtype = field.type
    if pa.types.is_dictionary(dtype):
        return
    if pa.types.is_struct(dtype):
        for child in dtype:
            _validate_nullability(value.get(child.name), child, path + "." + field.name)
    elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype):
        for item in value:
            _validate_nullability(item, dtype.value_field, path + "." + field.name)
    elif pa.types.is_map(dtype):
        for key, item in value:
            _validate_nullability(key, dtype.key_field, path + "." + field.name)
            _validate_nullability(item, dtype.item_field, path + "." + field.name)


def encode_inventory(infos: Sequence[SchemaContentsInfo]) -> dict[str, bytes | None]:
    """Validate schema membership and encode all six categories across schemas."""
    ordered = sorted(infos, key=lambda c: len(c.schema.path))
    paths = [schema_path_key(c.schema.path) for c in ordered]
    if len(paths) != len(set(paths)):
        raise ValueError("catalog_contents returned duplicate schema paths")
    for path in paths:
        if len(path) > 1 and path[:-1] not in paths:
            raise ValueError(f"catalog_contents returned schema path {list(path)!r} without its parent")
    grouped: dict[str, list[Any]] = {kind: [] for kind in CATEGORY_TYPES}
    source_kinds = {
        "tables": "tables",
        "views": "views",
        "indexes": "indexes",
        "scalar_functions": "functions",
        "aggregate_functions": "functions",
        "table_functions": "functions",
        "scalar_macros": "macros",
        "table_macros": "macros",
    }
    for info in ordered:
        grouped["schemas"].append(info.schema)
        for source, target in source_kinds.items():
            for item in getattr(info, source):
                if isinstance(item, bytes):
                    decoded = decode_collection(CATEGORY_TYPES[target], item)
                    if len(decoded) != 1:
                        raise ValueError("legacy catalog item must contain exactly one row")
                    item = decoded[0]
                expected_types: dict[str, set[Any]] = {
                    "scalar_functions": {FunctionType.SCALAR},
                    "aggregate_functions": {FunctionType.AGGREGATE},
                    "table_functions": {FunctionType.TABLE, FunctionType.TABLE_BUFFERING},
                    "scalar_macros": {MacroType.SCALAR},
                    "table_macros": {MacroType.TABLE},
                }
                expected = expected_types.get(source)
                actual = getattr(item, "function_type", getattr(item, "macro_type", None))
                if expected is not None and actual not in expected:
                    raise ValueError(f"catalog_contents {source} item has the wrong subtype")
                if schema_path_key(item.schema_path) != schema_path_key(info.schema.path):
                    raise ValueError(f"catalog_contents {source} item belongs to a different schema")
                grouped[target].append(item)
    return {kind: encode_collection(cls, grouped[kind]) for kind, cls in CATEGORY_TYPES.items()}


def decode_inventory(
    response: CatalogContentsResponse, *, if_none_match: str | None = None
) -> list[SchemaContentsInfo]:
    """Validate the response and restore the SDK's grouped inventory."""
    if response.not_modified:
        if response.etag is None or response.etag != if_none_match:
            raise ValueError("catalog_contents not_modified requires a matching non-null ETag")
        if any(getattr(response, kind) is not None for kind in CATEGORY_TYPES):
            raise ValueError("catalog_contents not_modified must have null categories")
        return []
    schemas = decode_collection(SchemaInfo, response.schemas)
    result: dict[tuple[str, ...], SchemaContentsInfo] = {}
    for schema in schemas:
        path = schema_path_key(schema.path)
        if path in result:
            raise ValueError("catalog_contents returned duplicate schema paths")
        if len(path) > 1 and path[:-1] not in result:
            raise ValueError("catalog_contents schema parent must precede its child")
        result[path] = SchemaContentsInfo(
            schema=schema,
            tables=[],
            views=[],
            scalar_functions=[],
            aggregate_functions=[],
            table_functions=[],
            scalar_macros=[],
            table_macros=[],
            indexes=[],
        )
    for category, cls in CATEGORY_TYPES.items():
        if category == "schemas":
            continue
        for item in decode_collection(cls, getattr(response, category)):
            path = schema_path_key(item.schema_path)
            if path not in result:
                raise ValueError(f"catalog_contents {category} references an unknown schema")
            destination = category
            if category == "functions":
                destination = {
                    FunctionType.SCALAR: "scalar_functions",
                    FunctionType.AGGREGATE: "aggregate_functions",
                    FunctionType.TABLE: "table_functions",
                    FunctionType.TABLE_BUFFERING: "table_functions",
                }[item.function_type]
            elif category == "macros":
                destination = {MacroType.SCALAR: "scalar_macros", MacroType.TABLE: "table_macros"}[item.macro_type]
            getattr(result[path], destination).append(item)
    return list(result.values())


def contents_digest(categories: dict[str, bytes | None]) -> str:
    """Hash category identity and complete stream bytes with unambiguous lengths."""
    # This domain versions the unchanged stream framing, not the VGI protocol.
    digest = hashlib.sha256(b"vgi.catalog.contents.2.2\0")
    for kind in CATEGORY_TYPES:
        data = categories[kind]
        digest.update(kind.encode() + b"\0")
        digest.update(b"\0" if data is None else b"\1")
        if data is not None:
            digest.update(len(data).to_bytes(8, "little"))
            digest.update(data)
    return digest.hexdigest()
