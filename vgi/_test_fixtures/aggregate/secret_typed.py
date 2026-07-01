# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Secret-aware aggregate fixture (secret_typed_sum).

Demonstrates the *supported* aggregate x secret intersection: an aggregate
reads a statically-resolved secret in ``on_bind`` (declared via the ``Secret()``
annotation, so the C++ extension resolves it up front and delivers it on
``AggregateBindRequest.secrets``) and uses a secret *value* to shape its output
type. The aggregated value itself is computed normally in ``update`` /
``finalize``.

Boundary (intentional, see ``vgi.worker.aggregate_bind``): secret *values* are
only available at bind time. ``aggregate_update`` / ``aggregate_finalize``
receive an empty ``ResolvedSecrets`` (persisting secrets to shared aggregate
storage would leak them), and a two-phase ``params.secrets.get()`` inside an
aggregate's ``on_bind`` raises ``NotImplementedError``. This fixture therefore
resolves the secret statically and threads the bind-time decision to
``finalize`` via the output schema (which round-trips bind -> finalize).
"""

from __future__ import annotations

from typing import Annotated, Any

import pyarrow as pa

from vgi._test_fixtures.aggregate._common import SumState
from vgi.aggregate_function import AggregateBindParams, AggregateFunction
from vgi.arguments import Param, Returns, Secret
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.schema_utils import schema
from vgi.table_function import ProcessParams


class SecretTypedSumFunction(AggregateFunction[SumState]):
    """Sum an integer column, choosing the result type from a secret.

    ``on_bind`` reads the ``vgi_example`` secret (resolved statically by the C++
    extension). When the secret's ``use_ssl`` field is true the aggregate returns
    the sum as ``DOUBLE``; otherwise as ``BIGINT``. This proves the secret's
    *value* was delivered to the aggregate's bind — an aggregate that never
    declared a secret could not vary its output type this way.

    SQL: ``SELECT secret_typed_sum(n) FROM t GROUP BY g``
    """

    class Meta:
        name = "secret_typed_sum"
        description = "Sum an integer column; the result type is chosen from a secret"
        categories = ["aggregate", "secret"]
        examples = [
            FunctionExample(
                sql="SELECT secret_typed_sum(n) FROM (SELECT 1 AS n UNION ALL SELECT 2)",
                description="Sum a column with the result type selected by the vgi_example secret",
            ),
        ]

    @classmethod
    def on_bind(
        cls,
        params: AggregateBindParams,
        *,
        vgi_example: Annotated[dict[str, pa.Scalar[Any]] | None, Secret("vgi_example")] = None,
        **kwargs: Any,
    ) -> BindResponse:
        """Choose the result type from the statically-resolved secret.

        The ``Secret()`` annotation advertises the requirement so the extension
        pre-resolves the secret and delivers it on the bind request (keyed by the
        DuckDB secret *name*). ``to_dict().of_type()`` matches it by the secret's
        connector-serialized ``type`` field — no ``get()`` (which would register a
        pending lookup and trigger the unsupported two-phase retry).
        """
        secret = next(iter(params.secrets.to_dict().of_type("vgi_example")), {})
        use_ssl = secret.get("use_ssl")
        as_double = bool(use_ssl.as_py()) if use_ssl is not None else False
        result_type = pa.float64() if as_double else pa.int64()
        return BindResponse(output_schema=schema(result=result_type))

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> SumState:
        """Identity element for the sum."""
        return SumState(total=0)

    @classmethod
    def update(
        cls,
        states: dict[int, SumState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.Int64Array, Param(doc="Integer column to sum")],
    ) -> None:
        """Accumulate integer values into per-group totals."""
        for i in range(len(group_ids)):
            gid: int = group_ids[i].as_py()
            v = value[i].as_py()
            if v is not None:
                states[gid] = SumState(total=states[gid].total + v)

    @classmethod
    def combine(cls, source: SumState, target: SumState, params: ProcessParams[None]) -> SumState:
        """Merge two partial sums."""
        return SumState(total=target.total + source.total)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, SumState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns()]:
        """Emit each group's sum, cast to the bind-time (secret-chosen) type."""
        result_field = params.output_schema.field(0)
        results = [s.total if (s := states[gid.as_py()]) is not None else None for gid in group_ids]
        return pa.record_batch({result_field.name: pa.array(results, type=result_field.type)})
