"""Filter-pushdown evaluation against canonical Arrow extension types.

Regression test for the issue where DuckDB's VGI extension serialises
filter-literal BOOLEAN values as ``extension<arrow.bool8>`` (per
``vgi/duckdb/src/common/arrow/arrow_type_extension.cpp``), and the worker
attempted to compare them directly against a plain ``pa.bool_()`` column.
PyArrow's binary kernels are type-pair-keyed and have no entry for
``equal(bool, extension<arrow.bool8>)``, so the bare ``pc.equal`` call
raised ``ArrowNotImplementedError``.

These tests pin the worker's normalization of canonical extension types
in ``ConstantFilter.evaluate`` and ``InFilter.evaluate`` so the same bug
can't slip back in for ``arrow.bool8``, ``arrow.uuid``, or any future
canonical extension type that wraps a comparable storage type.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi.table_filter_pushdown import (
    ComparisonOp,
    ConstantFilter,
    InFilter,
)


def _batch_with_bool(name: str, values: list[bool | None]) -> pa.RecordBatch:
    """RecordBatch holding a single plain pa.bool_() column."""
    return pa.RecordBatch.from_pydict({name: pa.array(values, type=pa.bool_())})


def _batch_with_bool8(name: str, values: list[bool | None]) -> pa.RecordBatch:
    """RecordBatch holding a single pa.bool8() (canonical extension) column."""
    storage = pa.array([None if v is None else int(v) for v in values], type=pa.int8())
    ext_arr = pa.ExtensionArray.from_storage(pa.bool8(), storage)
    return pa.RecordBatch.from_arrays([ext_arr], names=[name])


# =============================================================================
# ConstantFilter — bool column, bool8 literal (the trains-server case)
# =============================================================================


class TestBoolColumnBool8Literal:
    """Column is plain ``pa.bool_()``; literal arrives as ``pa.bool8()``.

    This is the case the trains worker hits when DuckDB serialises a
    BOOLEAN constant from ``WHERE has_departures = true``.
    """

    def test_eq_true(self) -> None:
        """EQ against a bool8 literal matches the plain-bool column."""
        batch = _batch_with_bool("flag", [True, False, None, True])
        f = ConstantFilter(
            column_name="flag",
            column_index=0,
            op=ComparisonOp.EQ,
            value=pa.scalar(1, type=pa.bool8()),
        )
        result = f.evaluate(batch)
        assert result.to_pylist() == [True, False, None, True]

    def test_eq_false(self) -> None:
        """EQ against a false bool8 literal."""
        batch = _batch_with_bool("flag", [True, False, None, True])
        f = ConstantFilter(
            column_name="flag",
            column_index=0,
            op=ComparisonOp.EQ,
            value=pa.scalar(0, type=pa.bool8()),
        )
        assert f.evaluate(batch).to_pylist() == [False, True, None, False]

    def test_ne_true(self) -> None:
        """NE against a bool8 literal."""
        batch = _batch_with_bool("flag", [True, False, None, True])
        f = ConstantFilter(
            column_name="flag",
            column_index=0,
            op=ComparisonOp.NE,
            value=pa.scalar(1, type=pa.bool8()),
        )
        assert f.evaluate(batch).to_pylist() == [False, True, None, False]


# =============================================================================
# ConstantFilter — bool8 column, bool8 literal (worker-uses-bool8 case)
# =============================================================================


class TestBool8ColumnBool8Literal:
    """Column is also ``pa.bool8()`` — same story on both sides of the kernel.

    PyArrow has no ``equal(extension<bool8>, extension<bool8>)`` either, so
    we still need to strip the wrapper.
    """

    def test_eq_true(self) -> None:
        """EQ with bool8 on both sides of the kernel."""
        batch = _batch_with_bool8("flag", [True, False, None, True])
        f = ConstantFilter(
            column_name="flag",
            column_index=0,
            op=ComparisonOp.EQ,
            value=pa.scalar(1, type=pa.bool8()),
        )
        assert f.evaluate(batch).to_pylist() == [True, False, None, True]

    def test_eq_false(self) -> None:
        """EQ against a false literal with bool8 on both sides."""
        batch = _batch_with_bool8("flag", [True, False, None, True])
        f = ConstantFilter(
            column_name="flag",
            column_index=0,
            op=ComparisonOp.EQ,
            value=pa.scalar(0, type=pa.bool8()),
        )
        assert f.evaluate(batch).to_pylist() == [False, True, None, False]


# =============================================================================
# Plain types — sanity that normalisation is a no-op (no regression)
# =============================================================================


class TestPlainTypesUnchanged:
    """Non-extension types still flow through pc.equal unchanged.

    The normalisation helper must not break any of the existing happy
    paths that ``test_filter_pushdown.py`` already covers in detail.
    """

    def test_int32_eq(self) -> None:
        """Plain int32 EQ flows through normalisation unchanged."""
        batch = pa.RecordBatch.from_pydict({"n": pa.array([1, 2, 3, 4], type=pa.int32())})
        f = ConstantFilter(column_name="n", column_index=0, op=ComparisonOp.EQ, value=pa.scalar(2, type=pa.int32()))
        assert f.evaluate(batch).to_pylist() == [False, True, False, False]

    def test_string_eq(self) -> None:
        """Plain string EQ flows through normalisation unchanged."""
        batch = pa.RecordBatch.from_pydict({"s": ["a", "b", "c"]})
        f = ConstantFilter(column_name="s", column_index=0, op=ComparisonOp.EQ, value=pa.scalar("b"))
        assert f.evaluate(batch).to_pylist() == [False, True, False]

    def test_plain_bool_eq(self) -> None:
        """No extension on either side — must still work after the fix."""
        batch = _batch_with_bool("flag", [True, False, None, True])
        f = ConstantFilter(column_name="flag", column_index=0, op=ComparisonOp.EQ, value=pa.scalar(True))
        assert f.evaluate(batch).to_pylist() == [True, False, None, True]


# =============================================================================
# InFilter — same kernel-mismatch story applies to pc.is_in
# =============================================================================


class TestInFilterExtension:
    """``WHERE bool_col IN (true)`` hits ``pc.is_in`` with the same type pair.

    DuckDB serialises the values list using the canonical extension types,
    so InFilter.values arrives as ``extension<arrow.bool8>`` while the
    column may be plain ``pa.bool_()``.
    """

    def test_in_bool_with_bool8_values(self) -> None:
        """IN with bool8 values against a plain-bool column."""
        batch = _batch_with_bool("flag", [True, False, None, True])
        # Build the values array as a bool8 extension array
        storage = pa.array([1], type=pa.int8())
        values = pa.ExtensionArray.from_storage(pa.bool8(), storage)
        f = InFilter(column_name="flag", column_index=0, values=values)
        assert f.evaluate(batch).to_pylist() == [True, False, False, True]


# =============================================================================
# Symmetry: plain literal against bool8 column (defensive — unlikely but
# easy enough to keep working).
# =============================================================================


class TestPlainLiteralBool8Column:
    """Defensive symmetry check: plain bool literal against a bool8 column.

    If some future code path emits a plain bool literal but the column
    happens to be bool8, normalisation should still align them.
    """

    def test_plain_bool_literal_bool8_column(self) -> None:
        """Plain bool literal against a bool8 column."""
        batch = _batch_with_bool8("flag", [True, False, None, True])
        f = ConstantFilter(
            column_name="flag",
            column_index=0,
            op=ComparisonOp.EQ,
            value=pa.scalar(True, type=pa.bool_()),
        )
        assert f.evaluate(batch).to_pylist() == [True, False, None, True]


# =============================================================================
# Pre-fix regression check: ensure the bare pyarrow kernel still raises
# without normalisation. If this stops raising in a future PyArrow version,
# the normalisation helper becomes redundant for that case (good news, not
# a failure) — but flag it so we know to revisit.
# =============================================================================


def test_pyarrow_kernel_gap_still_present() -> None:
    """Document the underlying PyArrow gap.

    If this passes in a future PyArrow release the normalisation helper
    is over-defensive but harmless.
    """
    import pyarrow.compute as pc

    col = pa.array([True, False], type=pa.bool_())
    val = pa.scalar(1, type=pa.bool8())
    with pytest.raises(pa.lib.ArrowNotImplementedError, match="extension<arrow.bool8>"):
        pc.equal(col, val)


# =============================================================================
# UUID — same kernel-mismatch story as bool8; second-most-likely bug in the
# wild because UUID primary keys are everywhere.
# =============================================================================


def _uuid_array(uuids_hex: list[str | None]) -> pa.Array:
    """Build an arrow.uuid extension array from a list of 32-char hex strings."""
    storage = pa.array(
        [None if u is None else bytes.fromhex(u) for u in uuids_hex],
        type=pa.binary(16),
    )
    return pa.ExtensionArray.from_storage(pa.uuid(), storage)


def _uuid_scalar(uuid_hex: str) -> pa.Scalar:
    """Build an arrow.uuid extension scalar from a 32-char hex string."""
    storage = pa.scalar(bytes.fromhex(uuid_hex), type=pa.binary(16))
    return storage.cast(pa.uuid())


class TestUuidExtension:
    """Column and literal both arrive as ``extension<arrow.uuid>``.

    Both sides are wrapped because DuckDB's UUID type round-trips through
    the canonical ``arrow.uuid`` extension on the wire. PyArrow has no
    ``equal(extension<arrow.uuid>, extension<arrow.uuid>)`` kernel.
    """

    UUIDS = [
        "00000000000000000000000000000001",
        "00000000000000000000000000000002",
        None,
        "ffffffffffffffffffffffffffffffff",
    ]

    def test_eq(self) -> None:
        """EQ with arrow.uuid on both sides."""
        batch = pa.RecordBatch.from_arrays([_uuid_array(self.UUIDS)], names=["id"])
        f = ConstantFilter(
            column_name="id",
            column_index=0,
            op=ComparisonOp.EQ,
            value=_uuid_scalar("00000000000000000000000000000002"),
        )
        assert f.evaluate(batch).to_pylist() == [False, True, None, False]

    def test_ne(self) -> None:
        """NE with arrow.uuid on both sides."""
        batch = pa.RecordBatch.from_arrays([_uuid_array(self.UUIDS)], names=["id"])
        f = ConstantFilter(
            column_name="id",
            column_index=0,
            op=ComparisonOp.NE,
            value=_uuid_scalar("00000000000000000000000000000002"),
        )
        assert f.evaluate(batch).to_pylist() == [True, False, None, True]

    def test_in(self) -> None:
        """IN with an arrow.uuid values array."""
        batch = pa.RecordBatch.from_arrays([_uuid_array(self.UUIDS)], names=["id"])
        values = _uuid_array(
            [
                "00000000000000000000000000000001",
                "ffffffffffffffffffffffffffffffff",
            ]
        )
        f = InFilter(column_name="id", column_index=0, values=values)
        assert f.evaluate(batch).to_pylist() == [True, False, False, True]


# =============================================================================
# Documented PyArrow gap: no equal kernel for INTERVAL types
# =============================================================================


def test_pyarrow_interval_kernel_gap() -> None:
    """PyArrow has no ``equal`` kernel for any interval type.

    The affected types are ``month_day_nano_interval``,
    ``day_time_interval``, and ``month_interval``.

    This means a filter pushdown like ``WHERE col = INTERVAL '1 day'``
    cannot be evaluated on the worker side regardless of whether we strip
    extension wrappers — there is simply no kernel to dispatch to. The
    pushdown system would need either to refuse INTERVAL filters at
    serialisation (DuckDB extension side) or to implement a custom
    field-by-field comparison in ``ConstantFilter.evaluate``.

    This test documents the gap so we notice if PyArrow ever fills it.
    """
    import pyarrow.compute as pc

    arr = pa.array([(1, 1, 1000), (2, 2, 2000)], type=pa.month_day_nano_interval())
    val = pa.scalar((1, 1, 1000), type=pa.month_day_nano_interval())
    with pytest.raises(pa.lib.ArrowNotImplementedError, match="month_day_nano_interval"):
        pc.equal(arr, val)
