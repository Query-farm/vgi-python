# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Metadata resolution + foot-gun guards for blended RowTransformFunction (Phase B)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
import pytest

from vgi.arguments import Arg, TableInput
from vgi.invocation import BindResponse
from vgi.metadata import (
    CatalogFunctionType,
    ResolvedMetadata,
    arrow_to_metadata,
    metadata_to_arrow,
    resolve_metadata,
)
from vgi.schema_utils import schema
from vgi.table_function import BindParams
from vgi.table_in_out_function import RowTransformFunction


@dataclass(slots=True, frozen=True, kw_only=True)
class _XYArgs:
    """Two positional input columns + one named option."""

    x: Annotated[float, Arg(0)]
    y: Annotated[float, Arg(1)]
    opt: Annotated[int, Arg("opt", default=1)] = 1


class _GoodBlended(RowTransformFunction[_XYArgs]):
    """A valid blended function."""

    class Meta:
        """Metadata."""

        name = "good_blended"

    @classmethod
    def on_bind(cls, params: BindParams[_XYArgs]) -> BindResponse:
        """Fixed output schema."""
        return BindResponse(output_schema=schema({"z": pa.float64()}))


class TestBlendedResolution:
    """Happy-path resolution of a blended function."""

    def test_resolves_as_blended_table(self) -> None:
        """input_from_args=True, function_type=TABLE, no finalize."""
        m = resolve_metadata(_GoodBlended)
        assert m.function_type is CatalogFunctionType.TABLE
        assert m.input_from_args is True
        assert m.has_finalize is False
        positions = {p.position for p in m.parameters}
        assert 0 in positions and 1 in positions and "opt" in positions

    def test_arrow_roundtrip_preserves_input_from_args(self) -> None:
        """input_from_args survives the Arrow + dict round-trips."""
        m = resolve_metadata(_GoodBlended)
        assert arrow_to_metadata(metadata_to_arrow(m)).input_from_args is True
        assert ResolvedMetadata.from_dict(m.to_dict()).input_from_args is True


class TestBlendedFootguns:
    """resolve_metadata rejects the invalid blended shapes."""

    def test_reject_finalize_override(self) -> None:
        """A blended fn that overrides finish()/finalize() is rejected."""

        class _WithFinalize(RowTransformFunction[_XYArgs]):
            """Illegal: blended with a finalize."""

            class Meta:
                """Metadata."""

                name = "blended_finalize"

            @classmethod
            def finish(cls, params, states):  # noqa: ANN001, ANN206, D102
                return []

        with pytest.raises(TypeError, match="cannot override finalize"):
            resolve_metadata(_WithFinalize)

    def test_reject_table_input(self) -> None:
        """A blended fn that declares a TableInput arg is rejected."""

        @dataclass(slots=True, frozen=True, kw_only=True)
        class _TblArgs:
            """Illegal: a TableInput on a blended fn."""

            x: Annotated[float, Arg(0)]
            data: Annotated[TableInput, Arg(1)]

        class _WithTable(RowTransformFunction[_TblArgs]):
            """Illegal: blended with TableInput."""

            class Meta:
                """Metadata."""

                name = "blended_table"

        with pytest.raises(TypeError, match="must not declare a TableInput"):
            resolve_metadata(_WithTable)

    def test_reject_positional_const(self) -> None:
        """A blended fn with a positional const arg is rejected."""

        @dataclass(slots=True, frozen=True, kw_only=True)
        class _ConstArgs:
            """Illegal: a positional const on a blended fn."""

            k: Annotated[int, Arg(0, const=True)]
            x: Annotated[float, Arg(1)]

        class _WithConst(RowTransformFunction[_ConstArgs]):
            """Illegal: blended with a positional const."""

            class Meta:
                """Metadata."""

                name = "blended_const"

        with pytest.raises(TypeError, match="cannot take a positional const"):
            resolve_metadata(_WithConst)

    def test_reject_zero_positional(self) -> None:
        """A blended fn with no positional args is rejected."""

        @dataclass(slots=True, frozen=True, kw_only=True)
        class _NamedOnlyArgs:
            """Illegal: no positional input columns."""

            opt: Annotated[int, Arg("opt", default=1)] = 1

        class _NamedOnly(RowTransformFunction[_NamedOnlyArgs]):
            """Illegal: blended with no positional args."""

            class Meta:
                """Metadata."""

                name = "blended_named_only"

        with pytest.raises(TypeError, match="at least one positional Arg"):
            resolve_metadata(_NamedOnly)
