# Copyright 2026 Query Farm LLC - https://query.farm
"""Both native C++ runtimes share the V3 model and wire shapes."""

from __future__ import annotations

import io
import re

import pyarrow as pa
import pytest

from scripts import regen_generated
from vgi.codegen import cpp_records, cpp_request_builders, cpp_schemas, cpp_secret_request_builders, cpp_secret_schemas
from vgi.codegen._common import GeneratorError


@pytest.mark.parametrize("module", [cpp_schemas, cpp_request_builders, cpp_secret_schemas, cpp_secret_request_builders])
def test_nanoarrow_inventory_matches_arrow(module: object) -> None:
    """Switching runtimes preserves every schema or builder, including secret RPCs."""
    outputs = []
    for backend in ("arrow", "nanoarrow"):
        out = io.StringIO()
        module.emit(out, backend=backend)
        outputs.append(out.getvalue())
    arrow, nano = outputs
    assert set(re.findall(r"\b(\w+(?:Schema|Params))\(", arrow)) == set(
        re.findall(r"\b(\w+(?:Schema|Params))\(", nano)
    ) - {"MakeSchema"}
    assert "arrow::" not in nano
    assert "#include <arrow" not in nano
    assert "--native-cpp-backend nanoarrow" in nano
    assert "SKIPPED:" not in nano


@pytest.mark.parametrize("which", ["hpp", "cpp"])
def test_nanoarrow_records_share_v3_model(which: str) -> None:
    """Every current record receives Nano codecs without changing the shared model."""
    out = io.StringIO()
    cpp_records.emit(out, which, backend="nanoarrow")
    source = out.getvalue()
    for record in cpp_records.build_model().records.values():
        assert f"ToBatch(const {record.name} &value)" in source
    assert "arrow::" not in source
    assert "using LargeBytes = VgiBufferPtr" in source if which == "hpp" else "wire::CurrentBatchScope" in source


def test_nanoarrow_preserves_nested_nullability_and_dictionary_width() -> None:
    """Nested list nullability and dictionary index width are wire-significant."""
    dtype = pa.struct(
        [
            pa.field("paths", pa.list_(pa.field("item", pa.list_(pa.field("item", pa.string(), False)), False)), False),
            pa.field("payload", pa.dictionary(pa.int32(), pa.binary()), True),
        ]
    )
    expr = cpp_schemas._na_emit_type(dtype, origin="test")
    assert (
        "type::List(type::List(type::Utf8(), /*item_nullable=*/false), "
        '/*item_nullable=*/false).Named("paths", /*nullable=*/false)'
    ) in expr
    assert 'type::Dictionary(type::Int32(), type::Binary()).Named("payload", /*nullable=*/true)' in expr
    with pytest.raises(GeneratorError, match="ordered dictionary"):
        cpp_schemas._na_emit_type(pa.dictionary(pa.int16(), pa.string(), ordered=True), origin="test")


def test_nanoarrow_v3_request_shapes() -> None:
    """V3 nullable aliases and typed capability structs remain supported."""
    out = io.StringIO()
    cpp_request_builders.emit(out, backend="nanoarrow")
    source = out.getvalue()
    assert "BuildOptionalStringListScalar(aliases)" in source
    assert "const VgiColumn& query_pushdown" in source
    assert "BuildStructScalar(query_pushdown, schema->field(6), true)" in source


def test_explicit_backend_is_limited_to_native_targets() -> None:
    """Standalone C++ SDK rendering keeps its existing Arrow runtime."""
    native = regen_generated._render("vgi.codegen.cpp_schemas", "nanoarrow")
    sdk = regen_generated._render("vgi.codegen.cpp_sdk:schemas")
    assert "VgiSchemaPtr" in native
    assert "arrow::Schema" in sdk
    assert "VgiSchemaPtr" not in sdk
