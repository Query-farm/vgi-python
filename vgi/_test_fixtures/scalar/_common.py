# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Shared scalar fixture helpers (numeric type promotion)."""

from __future__ import annotations

import pyarrow as pa

from vgi.exceptions import SchemaValidationError


def _is_addable_type(dtype: pa.DataType) -> bool:
    """Check if a type can be passed to pyarrow.compute.add."""
    return (
        pa.types.is_integer(dtype)
        or pa.types.is_floating(dtype)
        or pa.types.is_decimal(dtype)
        or pa.types.is_temporal(dtype)
    )


def _is_multipliable_type(dtype: pa.DataType) -> bool:
    """Check if a type can be passed to pyarrow.compute.multiply.

    Tighter than ``_is_addable_type`` because pc.multiply has no kernel for
    temporal types (date/time/timestamp/interval) — pc.add does, since
    date + interval is well-defined, but doubling a date is not.
    """
    return pa.types.is_integer(dtype) or pa.types.is_floating(dtype) or pa.types.is_decimal(dtype)


def _promote_for_addition(dtype: pa.DataType) -> pa.DataType:
    """Return the appropriate output type for addition to reduce overflow risk.

    Adding two values of the same type can overflow, so we promote integers
    to the next larger size. For example, int32 + int32 -> int64.
    """
    if pa.types.is_temporal(dtype):
        return dtype
    if pa.types.is_floating(dtype):
        # Promote float32 -> float64 to reduce overflow risk
        if dtype == pa.float16() or dtype == pa.float32():
            return pa.float64()
        return dtype
    if pa.types.is_integer(dtype):
        # Promote to a larger integer type since a + b can overflow
        if dtype == pa.int8():
            return pa.int16()
        if dtype == pa.int16():
            return pa.int32()
        if dtype in (pa.int32(), pa.int64()):
            return pa.int64()
        # Unsigned integers
        if dtype == pa.uint8():
            return pa.uint16()
        if dtype == pa.uint16():
            return pa.uint32()
        if dtype in (pa.uint32(), pa.uint64()):
            return pa.uint64()
        return dtype
    if pa.types.is_decimal(dtype):
        # Adding/doubling a decimal needs +1 digit of precision to avoid
        # overflow (2 * 10^p uses p+1 digits). DuckDB only consumes
        # decimal128 over the Arrow C ABI (no decimal256 reader), so we cap
        # at precision 38; doubling at the cap keeps the same type and
        # accepts that values >= 5e37 will overflow at compute time.
        new_precision = min(dtype.precision + 1, 38)
        return pa.decimal128(new_precision, dtype.scale)
    raise SchemaValidationError(f"Unsupported numeric type for addition: {dtype}")
