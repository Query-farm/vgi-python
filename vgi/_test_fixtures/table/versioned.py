# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Versioned data fixtures (versioned_data, versioned_constraints) used by time-travel tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import _OneShotState
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.schema_utils import schema
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)

# ============================================================================

# Version definitions: schema and data per version
_VERSIONED_SCHEMAS: dict[int, pa.Schema] = {
    1: schema(id=pa.int64()),
    2: schema(id=pa.int64(), name=pa.string(), score=pa.float64(), active=pa.bool_()),
    3: schema(id=pa.int64(), score=pa.float64()),
}

_VERSIONED_DATA: dict[int, dict[str, list[Any]]] = {
    1: {"id": [1, 2, 3]},
    2: {
        "id": [1, 2, 3, 4, 5],
        "name": ["alice", "bob", "carol", "dave", "eve"],
        "score": [10.0, 20.0, 30.0, 40.0, 50.0],
        "active": [True, False, True, False, True],
    },
    3: {"id": [1, 2, 3, 4], "score": [15.0, 25.0, 35.0, 45.0]},
}

# Current version (default when no AT clause)
_CURRENT_VERSION = 3


def resolve_version(at_unit: str | None, at_value: str | None) -> int:
    """Resolve AT clause to a version number.

    - ``VERSION``: direct integer version (must exist in ``_VERSIONED_SCHEMAS``)
    - ``TIMESTAMP``: year-based mapping (<=2020→1, <=2021→2, >=2022→3)
    - ``None``: current version (3)

    Raises ``ValueError`` for unknown versions or unsupported AT units.
    """
    if not at_unit:
        return _CURRENT_VERSION

    if at_unit.upper() == "VERSION":
        version = int(at_value)  # type: ignore[arg-type]
        if version not in _VERSIONED_SCHEMAS:
            raise ValueError(f"Unknown version: {version}. Valid versions: {sorted(_VERSIONED_SCHEMAS)}")
        return version

    if at_unit.upper() == "TIMESTAMP":
        # Parse year from timestamp string (e.g. "2020-06-15 00:00:00")
        year = int(str(at_value)[:4])
        if year < 2020:
            raise ValueError(f"No version exists at timestamp {at_value!r}: table did not exist before 2020")
        if year <= 2020:
            return 1
        if year <= 2021:
            return 2
        return 3

    raise ValueError(f"Unsupported at_unit: {at_unit!r}")


@dataclass(slots=True, frozen=True)
class VersionedDataFunctionArgs:
    """Arguments for VersionedDataFunction."""

    version: Annotated[int, Arg(0, doc="Data version to return")]


@dataclass(kw_only=True)
class VersionedDataState(ArrowSerializableDataclass):
    """State for VersionedDataFunction."""

    done: bool = False


@init_single_worker
class VersionedDataFunction(TableFunctionGenerator[VersionedDataFunctionArgs, VersionedDataState]):
    """Returns version-specific data demonstrating time travel with schema evolution.

    Each version has a different schema and different data:

    - **Version 1**: ``(id int64)`` — 3 rows
    - **Version 2**: ``(id int64, name string, score double, active bool)`` — 5 rows
    - **Version 3** (current): ``(id int64, score double)`` — 4 rows

    """

    class Meta:
        """Metadata for VersionedDataFunction."""

        name = "versioned_data_scan"
        description = "Returns versioned data with schema evolution"
        categories = ["generator", "testing"]

    @classmethod
    def on_bind(cls, params: BindParams[VersionedDataFunctionArgs]) -> BindResponse:
        """Return version-specific output schema."""
        version = params.args.version
        if version not in _VERSIONED_SCHEMAS:
            raise ValueError(f"Unknown version: {version}. Valid versions: {sorted(_VERSIONED_SCHEMAS)}")
        return BindResponse(output_schema=_VERSIONED_SCHEMAS[version])

    @classmethod
    def initial_state(cls, params: ProcessParams[VersionedDataFunctionArgs]) -> VersionedDataState:
        """Create initial state."""
        return VersionedDataState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[VersionedDataFunctionArgs],
        state: VersionedDataState,
        out: OutputCollector,
    ) -> None:
        """Emit all rows for the requested version in one batch."""
        if state.done:
            out.finish()
            return
        state.done = True
        version = params.args.version
        data = _VERSIONED_DATA[version]
        out.emit(pa.RecordBatch.from_pydict(data, schema=params.output_schema))


# ============================================================================

# Version 1: simple users table (id, name) — NOT NULL on id only
# Version 2: adds email column, PK on id, UNIQUE on email
# Version 3: adds department_id column, FK to departments

_VERSIONED_CONSTRAINTS_SCHEMAS: dict[int, pa.Schema] = {
    1: schema(id=pa.int64(), name=pa.string()),
    2: schema(id=pa.int64(), name=pa.string(), email=pa.string()),
    3: schema(id=pa.int64(), name=pa.string(), email=pa.string(), department_id=pa.int64()),
}

_VERSIONED_CONSTRAINTS_DATA: dict[int, dict[str, list[Any]]] = {
    1: {"id": [1, 2], "name": ["Alice", "Bob"]},
    2: {"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"], "email": ["a@co", "b@co", "c@co"]},
    3: {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
        "email": ["a@co", "b@co", "c@co"],
        "department_id": [1, 2, 1],
    },
}

_VERSIONED_CONSTRAINTS_CURRENT = 3


def resolve_versioned_constraints_version(at_unit: str | None, at_value: str | None) -> int:
    """Resolve AT clause for versioned_constraints table."""
    if not at_unit:
        return _VERSIONED_CONSTRAINTS_CURRENT

    if at_unit.upper() == "VERSION":
        version = int(at_value)  # type: ignore[arg-type]
        if version not in _VERSIONED_CONSTRAINTS_SCHEMAS:
            raise ValueError(f"Unknown version: {version}. Valid versions: {sorted(_VERSIONED_CONSTRAINTS_SCHEMAS)}")
        return version

    raise ValueError(f"Unsupported at_unit: {at_unit!r}")


@dataclass(slots=True, frozen=True)
class _VersionedConstraintsArgs:
    """Arguments for VersionedConstraintsScanFunction."""

    version: Annotated[int, Arg(0, doc="Data version")]


@init_single_worker
class VersionedConstraintsScanFunction(
    TableFunctionGenerator[_VersionedConstraintsArgs, _OneShotState],
):
    """Returns version-specific data for constraint evolution testing."""

    class Meta:
        """Metadata for VersionedConstraintsScanFunction."""

        name = "versioned_constraints_scan"
        description = "Scan versioned constraints table"

    @classmethod
    def on_bind(cls, params: BindParams[_VersionedConstraintsArgs]) -> BindResponse:
        """Return output schema."""
        version = params.args.version
        if version not in _VERSIONED_CONSTRAINTS_SCHEMAS:
            raise ValueError(f"Unknown version: {version}")
        return BindResponse(output_schema=_VERSIONED_CONSTRAINTS_SCHEMAS[version])

    @classmethod
    def initial_state(cls, params: ProcessParams[_VersionedConstraintsArgs]) -> _OneShotState:
        """Create initial state."""
        return _OneShotState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_VersionedConstraintsArgs],
        state: _OneShotState,
        out: OutputCollector,
    ) -> None:
        """Emit data."""
        if state.done:
            out.finish()
            return
        state.done = True
        version = params.args.version
        data = _VERSIONED_CONSTRAINTS_DATA[version]
        out.emit(pa.RecordBatch.from_pydict(data, schema=params.output_schema))
