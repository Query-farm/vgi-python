# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Type-introspection scalar fixtures (type_info_*, any_mixed_*, pair_type_*)."""

from __future__ import annotations

from typing import Annotated

import pyarrow as pa

from vgi.arguments import Param, Returns
from vgi.scalar_function import ScalarFunction


def _type_info_result(label: str, v: pa.Array) -> pa.StringArray:  # type: ignore[type-arg]
    """Shared compute logic for all type_info overloads."""
    return pa.array([label if x is not None else None for x in v.to_pylist()], type=pa.string())


class TypeInfoInt32Function(ScalarFunction):
    """Return type name for int32 input."""

    class Meta:
        """Function metadata."""

        name = "type_info"
        description = "Return type name for int32 input"

    @classmethod
    def compute(
        cls,
        v: Annotated[pa.Int32Array, Param(doc="Input value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'int32' for each row."""
        return _type_info_result("int32", v)


class TypeInfoInt64Function(ScalarFunction):
    """Return type name for int64 input."""

    class Meta:
        """Function metadata."""

        name = "type_info"
        description = "Return type name for int64 input"

    @classmethod
    def compute(
        cls,
        v: Annotated[pa.Int64Array, Param(doc="Input value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'int64' for each row."""
        return _type_info_result("int64", v)


class TypeInfoUInt32Function(ScalarFunction):
    """Return type name for uint32 input."""

    class Meta:
        """Function metadata."""

        name = "type_info"
        description = "Return type name for uint32 input"

    @classmethod
    def compute(
        cls,
        v: Annotated[pa.UInt32Array, Param(doc="Input value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'uint32' for each row."""
        return _type_info_result("uint32", v)


class TypeInfoUInt64Function(ScalarFunction):
    """Return type name for uint64 input."""

    class Meta:
        """Function metadata."""

        name = "type_info"
        description = "Return type name for uint64 input"

    @classmethod
    def compute(
        cls,
        v: Annotated[pa.UInt64Array, Param(doc="Input value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'uint64' for each row."""
        return _type_info_result("uint64", v)


class TypeInfoStringFunction(ScalarFunction):
    """Return type name for string input."""

    class Meta:
        """Function metadata."""

        name = "type_info"
        description = "Return type name for string input"

    @classmethod
    def compute(
        cls,
        v: Annotated[pa.StringArray, Param(doc="Input value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'varchar' for each row."""
        return _type_info_result("varchar", v)


def _pair_type_result(label: str, a: pa.Array, b: pa.Array) -> pa.StringArray:  # type: ignore[type-arg]
    """Shared compute logic for all pair_type overloads."""
    return pa.array(
        [
            label if (x is not None and y is not None) else None
            for x, y in zip(a.to_pylist(), b.to_pylist(), strict=True)
        ],
        type=pa.string(),
    )


class PairTypeIntIntFunction(ScalarFunction):
    """Return 'int+int' for two int64 columns."""

    class Meta:
        """Function metadata."""

        name = "pair_type"
        description = "Return type pair name for int+int"

    @classmethod
    def compute(
        cls,
        a: Annotated[pa.Int64Array, Param(doc="First value")],
        b: Annotated[pa.Int64Array, Param(doc="Second value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'int+int' for each row."""
        return _pair_type_result("int+int", a, b)


class PairTypeStrStrFunction(ScalarFunction):
    """Return 'str+str' for two string columns."""

    class Meta:
        """Function metadata."""

        name = "pair_type"
        description = "Return type pair name for str+str"

    @classmethod
    def compute(
        cls,
        a: Annotated[pa.StringArray, Param(doc="First value")],
        b: Annotated[pa.StringArray, Param(doc="Second value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'str+str' for each row."""
        return _pair_type_result("str+str", a, b)


class PairTypeIntStrFunction(ScalarFunction):
    """Return 'int+str' for int64 + string columns."""

    class Meta:
        """Function metadata."""

        name = "pair_type"
        description = "Return type pair name for int+str"

    @classmethod
    def compute(
        cls,
        a: Annotated[pa.Int64Array, Param(doc="First value")],
        b: Annotated[pa.StringArray, Param(doc="Second value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'int+str' for each row."""
        return _pair_type_result("int+str", a, b)


class AnyMixedIntFunction(ScalarFunction):
    """AnyArrow first param, Int64 second param."""

    class Meta:
        """Function metadata."""

        name = "any_mixed"
        description = "Any+int dispatch"

    @classmethod
    def compute(
        cls,
        a: Annotated[pa.Array, Param(doc="Any type value")],  # type: ignore[type-arg]
        b: Annotated[pa.Int64Array, Param(doc="Int value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'any+int: {b}' for each row."""
        return pa.array(
            [f"any+int: {y}" if y is not None else None for y in b.to_pylist()],
            type=pa.string(),
        )


class AnyMixedStrFunction(ScalarFunction):
    """AnyArrow first param, String second param."""

    class Meta:
        """Function metadata."""

        name = "any_mixed"
        description = "Any+str dispatch"

    @classmethod
    def compute(
        cls,
        a: Annotated[pa.Array, Param(doc="Any type value")],  # type: ignore[type-arg]
        b: Annotated[pa.StringArray, Param(doc="String value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return 'any+str: {b}' for each row."""
        return pa.array(
            [f"any+str: {y}" if y is not None else None for y in b.to_pylist()],
            type=pa.string(),
        )
