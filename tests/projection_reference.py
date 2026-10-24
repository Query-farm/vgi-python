# Copyright 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D101, D102, D103, D106, D107

"""Finite pure projection proposals with normal bind/init/HTTP stream lifetime."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi import Worker
from vgi.catalog import Catalog, Schema, Table
from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.invocation import BindRepeatability, BindResponse, GlobalInitResponse
from vgi.protocol import TableFunctionProjectionRequest, TableFunctionProjectionResponse
from vgi.table_function import BindParams, InitParams, ProcessParams, TableFunctionGenerator

SOURCE = pa.schema(
    [
        pa.field("n", pa.int32(), metadata={b"source": b"physical"}),
        pa.field("other", pa.int32()),
        pa.field("text", pa.string()),
    ],
    metadata={b"provider": b"projection-reference"},
)
ROWS = [
    {"n": -2147483648, "other": 2, "text": "min"},
    {"n": 0, "other": 3, "text": "zero"},
    {"n": None, "other": None, "text": None},
    {"n": 2147483647, "other": -1, "text": "max"},
]
BASE = b"projection-reference-source-v1"


def event(kind, function, **details):
    filename = os.environ.get("VGI_PROJECTION_TRACE")
    if filename:
        with open(filename, "a") as output:
            output.write(json.dumps({"event": kind, "function": function, **details}) + "\n")


@dataclass
class ProjectionPlan(ArrowSerializableDataclass):
    """Complete portable replacement state; no prepared resource registry."""

    request: bytes
    accepted_indices: list[int]


@dataclass
class ProjectionState(ArrowSerializableDataclass):
    """Continue one fixed source through ordinary HTTP stream serialization."""

    offset: int = 0
    request: bytes | None = None
    accepted_indices: list[int] = field(default_factory=list)


class ProjectionSource(TableFunctionGenerator[None, ProjectionState]):
    """Qualify only total INTEGER-to-BIGINT widening casts against this source."""

    mode: ClassVar[str] = "accepted"
    repeatability: ClassVar[BindRepeatability] = BindRepeatability.UNKNOWN
    source: ClassVar[pa.Schema] = SOURCE
    rows: ClassVar[list[dict]] = ROWS

    class Meta:
        name = "projection_source"
        projection_pushdown = False
        filter_pushdown = False

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        event("bind", cls.Meta.name)
        return BindResponse(
            output_schema=cls.source,
            opaque_data=BASE,
            projection_expression_profiles=[] if cls.mode == "unoffered" else [ROW_SCALAR_EXPRESSION],
            repeatability=cls.repeatability,
        )

    @classmethod
    def candidate(cls, request, index):
        expression = request.expression_records()[index]
        if expression.evaluation_context != QueryEvaluationContext():
            return None
        match = re.fullmatch(r'CAST\s*\(\s*(?:"(n|other)"|(n|other))\s+AS\s+BIGINT\s*\)', expression.sql.strip(), re.I)
        if match is None:
            return None
        name = (match.group(1) or match.group(2)).lower()
        if name not in cls.source.names or cls.source.field(name).type != pa.int32():
            return None
        requested = request.result_schema.field(index)
        if requested.type != pa.int64() or not requested.nullable:
            return None
        return name

    @classmethod
    def on_projection(
        cls, params: BindParams[None], request: TableFunctionProjectionRequest
    ) -> TableFunctionProjectionResponse:
        event("proposal", cls.Meta.name)
        if (
            not request.bind_result.output_schema.equals(cls.source, check_metadata=True)
            or request.bind_result.opaque_data != BASE
        ):
            raise ValueError("proposal changed the actual source bind contract")
        if cls.mode in {"declined", "unoffered"}:
            return TableFunctionProjectionResponse()
        accepted = [i for i in range(len(request.expressions)) if cls.candidate(request, i) is not None]
        if cls.mode == "partial":
            accepted = accepted[:1]
        if not accepted:
            return TableFunctionProjectionResponse()
        schema = pa.schema(
            [*cls.source, *(request.result_schema.field(i) for i in accepted)], metadata=cls.source.metadata
        )
        if cls.mode == "malformed":
            schema = schema.set(len(cls.source), schema.field(len(cls.source)).with_type(pa.string()))
        state = ProjectionPlan(request=request.serialize_to_bytes(), accepted_indices=accepted)
        event("accepted", cls.Meta.name, accepted_indices=accepted, output_fields=schema.names)
        return TableFunctionProjectionResponse(
            accepted_indices=accepted,
            bind_result=BindResponse(
                output_schema=schema,
                opaque_data=state.serialize_to_bytes(),
                projection_expression_profiles=request.bind_result.projection_expression_profiles,
                repeatability=request.bind_result.repeatability,
            ),
        )

    @classmethod
    def plan(cls, params):
        payload = params.init_call.bind_opaque_data
        if payload == BASE:
            if not params.init_call.output_schema.equals(cls.source, check_metadata=True):
                raise ValueError("ordinary source init changed its bind schema")
            return None
        plan = ProjectionPlan.deserialize_from_bytes(payload)
        request = TableFunctionProjectionRequest.deserialize_from_bytes(plan.request)
        result = TableFunctionProjectionResponse(
            accepted_indices=plan.accepted_indices,
            bind_result=BindResponse(
                output_schema=params.init_call.output_schema,
                opaque_data=payload,
                projection_expression_profiles=request.bind_result.projection_expression_profiles,
                repeatability=request.bind_result.repeatability,
            ),
        )
        result.validate_request(request)
        if request.bind_call.serialize_to_bytes() != params.init_call.bind_call.serialize_to_bytes():
            raise ValueError("projection plan belongs to another source bind")
        if any(cls.candidate(request, i) is None for i in plan.accepted_indices):
            raise ValueError("projection plan contains an unqualified expression")
        return plan

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        event("init", cls.Meta.name, output_fields=params.init_call.output_schema.names)
        cls.plan(params)
        if cls.mode == "execution_error":
            raise RuntimeError("projection reference execution failed after acceptance")
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> ProjectionState:
        plan = cls.plan(params)
        return ProjectionState(
            request=None if plan is None else plan.request,
            accepted_indices=[] if plan is None else plan.accepted_indices,
        )

    @classmethod
    def process(cls, params: ProcessParams[None], state: ProjectionState, out: OutputCollector) -> None:
        if state.offset >= len(cls.rows):
            out.finish()
            return
        event("rows", cls.Meta.name)
        source = pa.RecordBatch.from_pylist(cls.rows[state.offset : state.offset + 2], schema=cls.source)
        values = list(source.columns)
        if state.request is not None:
            request = TableFunctionProjectionRequest.deserialize_from_bytes(state.request)
            for index in state.accepted_indices:
                values.append(pc.cast(source.column(cls.candidate(request, index)), pa.int64(), safe=True))
        out.emit(pa.RecordBatch.from_arrays(values, schema=params.output_schema))
        state.offset += source.num_rows


class Accepted(ProjectionSource):
    class Meta(ProjectionSource.Meta):
        name = "projection_accepted"


class Declined(ProjectionSource):
    mode = "declined"

    class Meta(ProjectionSource.Meta):
        name = "projection_declined"


class Partial(ProjectionSource):
    mode = "partial"

    class Meta(ProjectionSource.Meta):
        name = "projection_partial"


class Unoffered(ProjectionSource):
    mode = "unoffered"

    class Meta(ProjectionSource.Meta):
        name = "projection_unoffered"


class Malformed(ProjectionSource):
    mode = "malformed"

    class Meta(ProjectionSource.Meta):
        name = "projection_malformed"


class ExecutionError(ProjectionSource):
    mode = "execution_error"

    class Meta(ProjectionSource.Meta):
        name = "projection_execution_error"


class ProjectionSequence(ProjectionSource):
    repeatability = BindRepeatability.STABLE_WITHIN_QUERY
    source = pa.schema([pa.field("n", pa.int32())])
    rows = [{"n": n} for n in range(10)]

    class Meta(ProjectionSource.Meta):
        name = "projection_sequence"


CLASSES = [Accepted, Declined, Partial, Unoffered, Malformed, ExecutionError]
CATALOG = Catalog(
    name="projection_reference",
    schemas=[Schema(path=["main"], functions=CLASSES, tables=[Table(name=cls.mode, function=cls) for cls in CLASSES])],
)
COMMON = Catalog(
    name="example",
    schemas=[
        Schema(
            path=["main"],
            functions=[ProjectionSequence],
            tables=[Table(name="projection_rows", function=ProjectionSequence)],
        )
    ],
)


class ProjectionReferenceWorker(Worker):
    """Exercise actual function-backed catalog scans without generated columns."""

    catalog = CATALOG


class CommonProjectionWorker(Worker):
    """Share the same small public-RPC source across SDK conformance tests."""

    catalog = COMMON


if __name__ == "__main__":
    (CommonProjectionWorker if os.environ.get("VGI_PROJECTION_COMMON") == "1" else ProjectionReferenceWorker).main()
