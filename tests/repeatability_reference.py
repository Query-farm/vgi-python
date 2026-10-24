# Copyright 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D101, D102, D103, D106, D107

"""Finite SQLite snapshot proof using only ordinary bind and stream state.

VGI_REPEATABILITY_DB names an isolated absolute SQLite path. The native harness
may use seed_database/replace_rows from a separate process to commit mutations.
No resources are retained after bind; stable state contains its complete rows.
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi import Worker
from vgi.catalog import Catalog, Schema, Table, TableInfo
from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.invocation import BindRepeatability, BindResponse, GlobalInitResponse
from vgi.protocol import TableFunctionProjectionRequest, TableFunctionProjectionResponse
from vgi.table_function import BindParams, InitParams, ProcessParams, TableFunctionGenerator

SOURCE = pa.schema([pa.field("n", pa.int32()), pa.field("label", pa.string()), pa.field("generation", pa.int64())])
BASELINE = [(1, "one"), (None, "null"), (3, "three"), (4, "four")]


def database_path() -> Path:
    path = Path(os.environ["VGI_REPEATABILITY_DB"])
    if not path.is_absolute():
        raise ValueError("VGI_REPEATABILITY_DB must be absolute")
    return path


def seed_database(path: Path, rows=BASELINE, *, generation: int = 1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("CREATE TABLE IF NOT EXISTS version (generation INTEGER NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS items (position INTEGER PRIMARY KEY, n INTEGER, label TEXT)")
        db.execute("DELETE FROM version")
        db.execute("INSERT INTO version VALUES (?)", [generation])
        db.execute("DELETE FROM items")
        db.executemany("INSERT INTO items VALUES (?, ?, ?)", [(i, n, label) for i, (n, label) in enumerate(rows)])


def replace_rows(path: Path, rows) -> int:
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        generation = db.execute("SELECT generation FROM version").fetchone()[0] + 1
        db.execute("UPDATE version SET generation = ?", [generation])
        db.execute("DELETE FROM items")
        db.executemany("INSERT INTO items VALUES (?, ?, ?)", [(i, n, label) for i, (n, label) in enumerate(rows)])
    return generation


def event(kind: str, **details) -> None:
    if filename := os.environ.get("VGI_REPEATABILITY_TRACE"):
        with open(filename, "a") as output:
            output.write(json.dumps({"event": kind, **details}) + "\n")


@dataclass
class Snapshot(ArrowSerializableDataclass):
    generation: int
    rows: bytes

    def batch(self) -> pa.RecordBatch:
        with pa.ipc.open_stream(self.rows) as reader:
            batches = list(reader)
        if len(batches) != 1 or not batches[0].schema.equals(SOURCE, check_metadata=True):
            raise ValueError("invalid repeatability snapshot")
        return batches[0]


def read_snapshot(path: Path) -> Snapshot:
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("BEGIN")
        generation = db.execute("SELECT generation FROM version").fetchone()[0]
        rows = db.execute("SELECT n, label FROM items ORDER BY position").fetchall()
    return snapshot_from_rows(rows, generation)


def snapshot_from_rows(rows, generation: int) -> Snapshot:
    batch = pa.RecordBatch.from_pylist(
        [{"n": n, "label": label, "generation": generation} for n, label in rows], schema=SOURCE
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, SOURCE) as writer:
        writer.write_batch(batch)
    return Snapshot(generation=generation, rows=sink.getvalue().to_pybytes())


@dataclass
class BoundState(ArrowSerializableDataclass):
    snapshot: bytes | None = None
    proposal: bytes | None = None
    accepted_indices: list[int] = field(default_factory=list)


@dataclass
class StreamState(ArrowSerializableDataclass):
    snapshot: bytes
    accepted_indices: list[int]
    offset: int = 0


class RepeatabilitySource(TableFunctionGenerator[None, StreamState]):
    repeatability: ClassVar[BindRepeatability] = BindRepeatability.UNKNOWN
    projection_profiles: ClassVar[list[str]] = [ROW_SCALAR_EXPRESSION]

    class Meta:
        name = "repeatability_source"
        projection_pushdown = False
        filter_pushdown = False

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        snapshot = (
            read_snapshot(database_path()) if cls.repeatability == BindRepeatability.STABLE_WITHIN_QUERY else None
        )
        event("bind", function=cls.Meta.name, generation=None if snapshot is None else snapshot.generation)
        return BindResponse(
            output_schema=SOURCE,
            opaque_data=BoundState(
                snapshot=None if snapshot is None else snapshot.serialize_to_bytes()
            ).serialize_to_bytes(),
            projection_expression_profiles=list(cls.projection_profiles),
            repeatability=cls.repeatability,
        )

    @classmethod
    def on_projection(
        cls, params: BindParams[None], request: TableFunctionProjectionRequest
    ) -> TableFunctionProjectionResponse:
        if not request.bind_result.output_schema.equals(SOURCE, check_metadata=True):
            raise ValueError("repeatability proposal changed source schema")
        if request.bind_result.repeatability != cls.repeatability:
            raise ValueError("repeatability proposal changed source promise")
        original = BoundState.deserialize_from_bytes(request.bind_result.opaque_data)
        if original.proposal is not None:
            raise ValueError("repeatability reference negotiates once")
        indices = [
            i
            for i, expression in enumerate(request.expression_records())
            if expression.sql == 'CAST("n" AS BIGINT)'
            and expression.evaluation_context == QueryEvaluationContext()
            and request.result_schema.field(i).type == pa.int64()
            and request.result_schema.field(i).nullable
        ]
        event("proposal", function=cls.Meta.name, accepted_indices=indices)
        if not indices:
            return TableFunctionProjectionResponse()
        state = BoundState(snapshot=original.snapshot, proposal=request.serialize_to_bytes(), accepted_indices=indices)
        return TableFunctionProjectionResponse(
            accepted_indices=indices,
            bind_result=BindResponse(
                output_schema=pa.schema([*SOURCE, *(request.result_schema.field(i) for i in indices)]),
                opaque_data=state.serialize_to_bytes(),
                projection_expression_profiles=request.bind_result.projection_expression_profiles,
                repeatability=request.bind_result.repeatability,
            ),
        )

    @classmethod
    def bound_state(cls, params) -> BoundState:
        state = BoundState.deserialize_from_bytes(params.init_call.bind_opaque_data)
        if (state.snapshot is not None) != (cls.repeatability == BindRepeatability.STABLE_WITHIN_QUERY):
            raise ValueError("repeatability reference requires its complete snapshot state")
        if state.proposal is None:
            if not params.init_call.output_schema.equals(SOURCE, check_metadata=True) or state.accepted_indices:
                raise ValueError("repeatability source init changed its bind schema")
        else:
            proposal = TableFunctionProjectionRequest.deserialize_from_bytes(state.proposal)
            if proposal.bind_call.serialize_to_bytes() != params.init_call.bind_call.serialize_to_bytes():
                raise ValueError("repeatability projection belongs to another bind")
            TableFunctionProjectionResponse(
                accepted_indices=state.accepted_indices,
                bind_result=BindResponse(
                    output_schema=params.init_call.output_schema,
                    opaque_data=params.init_call.bind_opaque_data,
                    projection_expression_profiles=proposal.bind_result.projection_expression_profiles,
                    repeatability=cls.repeatability,
                ),
            ).validate_request(proposal)
        return state

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        cls.bound_state(params)
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> StreamState:
        bound = cls.bound_state(params)
        snapshot = (
            Snapshot.deserialize_from_bytes(bound.snapshot)
            if bound.snapshot is not None
            else read_snapshot(database_path())
        )
        snapshot.batch()
        event("init", function=cls.Meta.name, generation=snapshot.generation)
        return StreamState(snapshot=snapshot.serialize_to_bytes(), accepted_indices=bound.accepted_indices)

    @classmethod
    def process(cls, params: ProcessParams[None], state: StreamState, out: OutputCollector) -> None:
        snapshot = Snapshot.deserialize_from_bytes(state.snapshot)
        batch = snapshot.batch()
        if state.offset >= batch.num_rows:
            out.finish()
            return
        batch = batch.slice(state.offset, 2)
        columns = list(batch.columns)
        for _ in state.accepted_indices:
            columns.append(pc.cast(batch.column("n"), pa.int64(), safe=True))
        out.emit(pa.RecordBatch.from_arrays(columns, schema=params.output_schema))
        state.offset += batch.num_rows
        event("rows", function=cls.Meta.name, generation=snapshot.generation, rows=batch.num_rows)


class Stable(RepeatabilitySource):
    repeatability = BindRepeatability.STABLE_WITHIN_QUERY

    class Meta(RepeatabilitySource.Meta):
        name = "repeatability_stable"


class StableUnoffered(Stable):
    projection_profiles = []

    class Meta(Stable.Meta):
        name = "repeatability_stable_unoffered"


class Unknown(RepeatabilitySource):
    class Meta(RepeatabilitySource.Meta):
        name = "repeatability_unknown"


class SnapshotTable(Table):
    """Declare the fixed source schema without taking a discovery-time snapshot."""

    def _get_resolved_columns(self) -> pa.Schema:
        return SOURCE


class StaleInlineTable(SnapshotTable):
    """Publish an obvious stale snapshot to qualify native fresh-bind bypass."""

    def to_table_info(self, schema_path: list[str]) -> TableInfo:
        stale = snapshot_from_rows([(-99, "stale-inline")], 0)
        bound = BindResponse(
            output_schema=SOURCE,
            opaque_data=BoundState(snapshot=stale.serialize_to_bytes()).serialize_to_bytes(),
            repeatability=BindRepeatability.STABLE_WITHIN_QUERY,
        )
        return replace(super().to_table_info(schema_path), bind_result=bound.serialize_to_bytes())


class RepeatabilityWorker(Worker):
    catalog = Catalog(
        name="repeatability_reference",
        schemas=[
            Schema(
                path=["main"],
                functions=[Stable, StableUnoffered, Unknown],
                tables=[
                    SnapshotTable(name="stable", function=Stable),
                    SnapshotTable(name="unknown", function=Unknown),
                    SnapshotTable(name="stable_unoffered", function=StableUnoffered),
                    StaleInlineTable(name="stable_inline", function=StableUnoffered),
                ],
            )
        ],
    )


if __name__ == "__main__":
    if not database_path().exists():
        seed_database(database_path())
    RepeatabilityWorker.main()
