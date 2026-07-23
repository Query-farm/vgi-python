# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Same-name-in-two-schemas scalar fixtures (``test_same_name_bind``).

Two distinct :class:`ScalarFunction` classes register under the *same* function
name but live in different catalog schemas (``main`` and ``data``). They exist
to prove that VGI resolves a schema-qualified call to the implementation in that
schema — ``example.main.test_same_name_bind(x)`` must reach the ``main`` class
and ``example.data.test_same_name_bind(x)`` the ``data`` class — rather than
collapsing both into one flat by-name registry entry.

Each returns a VARCHAR tagged with its own schema, so a mis-routed call is
visible in the query result rather than silently plausible.
"""

from __future__ import annotations

from typing import Annotated

import pyarrow as pa
import pyarrow.compute as pc

from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction


def _tag(schema_name: str, value: pa.Int64Array) -> pa.StringArray:
    """Render ``<schema_name>:<value>`` for every row, preserving nulls."""
    rendered = pc.cast(value, pa.string())
    return pa.array(
        [None if v is None else f"{schema_name}:{v}" for v in rendered.to_pylist()],
        type=pa.string(),
    )


class SameNameMainFunction(ScalarFunction):
    """``test_same_name_bind`` as registered in the ``main`` schema."""

    class Meta:
        """Function metadata."""

        name = "test_same_name_bind"
        description = "Schema-disambiguation probe; the main-schema implementation"
        examples = [
            FunctionExample(
                sql="SELECT example.main.test_same_name_bind(1)",
                description="Returns 'main:1'",
            ),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param(doc="Integer value to tag")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Tag each value with the owning schema."""
        return _tag("main", value)


class SameNameDataFunction(ScalarFunction):
    """``test_same_name_bind`` as registered in the ``data`` schema."""

    class Meta:
        """Function metadata."""

        name = "test_same_name_bind"
        description = "Schema-disambiguation probe; the data-schema implementation"
        examples = [
            FunctionExample(
                sql="SELECT example.data.test_same_name_bind(1)",
                description="Returns 'data:1'",
            ),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param(doc="Integer value to tag")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Tag each value with the owning schema."""
        return _tag("data", value)
