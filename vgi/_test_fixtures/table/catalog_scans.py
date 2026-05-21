# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Static catalog scan functions (colors, departments, employees, products, projects)."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import _EmptyArgs, _OneShotState
from vgi.invocation import BindResponse
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)


def _static_scan_function(
    func_name: str,
    func_description: str,
    output_schema: pa.Schema,
    data: dict[str, list[Any]],
) -> type[TableFunctionGenerator[_EmptyArgs, _OneShotState]]:
    """Create a table function that returns static data in one batch.

    This factory eliminates boilerplate for simple scan functions that
    return a fixed dataset. Each generated class is decorated with
    ``@init_single_worker`` and has a unique ``Meta.name``.
    """

    @init_single_worker
    class StaticScanFunction(TableFunctionGenerator[_EmptyArgs, _OneShotState]):
        """Returns static data."""

        class Meta:
            """Function metadata."""

            name = func_name
            description = func_description

        @classmethod
        def on_bind(cls, params: BindParams[_EmptyArgs]) -> BindResponse:
            """Return output schema."""
            return BindResponse(output_schema=output_schema)

        @classmethod
        def initial_state(cls, params: ProcessParams[_EmptyArgs]) -> _OneShotState:
            """Create initial state."""
            return _OneShotState()

        @classmethod
        def process(
            cls,
            params: ProcessParams[_EmptyArgs],
            state: _OneShotState,
            out: OutputCollector,
        ) -> None:
            """Emit data."""
            if state.done:
                out.finish()
                return
            state.done = True
            out.emit(pa.RecordBatch.from_pydict(data, schema=params.output_schema))

    StaticScanFunction.__name__ = func_name.title().replace("_", "") + "Function"
    StaticScanFunction.__qualname__ = StaticScanFunction.__name__

    return StaticScanFunction


DepartmentsScanFunction = _static_scan_function(
    func_name="departments_scan",
    func_description="Scan departments table",
    output_schema=pa.schema(
        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("budget", pa.float64()),
        ]
    ),
    data={
        "id": [1, 2, 3],
        "name": ["Engineering", "Sales", "HR"],
        "budget": [500000.0, 300000.0, 200000.0],
    },
)

EmployeesScanFunction = _static_scan_function(
    func_name="employees_scan",
    func_description="Scan employees table",
    output_schema=pa.schema(
        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("email", pa.string()),
            pa.field("department_id", pa.int64()),
        ]
    ),
    data={
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "email": ["alice@co.com", "bob@co.com", "carol@co.com", "dave@co.com", "eve@co.com"],
        "department_id": [1, 1, 2, 2, 3],
    },
)

ProjectsScanFunction = _static_scan_function(
    func_name="projects_scan",
    func_description="Scan projects table",
    output_schema=pa.schema(
        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
            pa.field("department_id", pa.int64()),
            pa.field("project_code", pa.string()),
            pa.field("title", pa.string()),
        ]
    ),
    data={
        "department_id": [1, 1, 2],
        "project_code": ["P001", "P002", "P003"],
        "title": ["Backend API", "Frontend UI", "Sales Portal"],
    },
)

ProductsScanFunction = _static_scan_function(
    func_name="products_scan",
    func_description="Scan products table",
    output_schema=pa.schema(
        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("quantity", pa.int64()),
            pa.field("price", pa.float64()),
        ]
    ),
    data={
        "id": [1, 2, 3],
        "name": ["Widget", "Gadget", "Doohickey"],
        "quantity": [100, 50, 200],
        "price": [9.99, 24.99, 4.99],
    },
)

ColorsScanFunction = _static_scan_function(
    func_name="colors_scan",
    func_description="Scan colors table (ENUM column)",
    output_schema=pa.schema(
        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
            pa.field("id", pa.int64()),
            pa.field("color", pa.string()),
            pa.field("hex_code", pa.string()),
        ]
    ),
    data={
        "id": [1, 2, 3],
        "color": ["blue", "green", "red"],
        "hex_code": ["#0000FF", "#00FF00", "#FF0000"],
    },
)
