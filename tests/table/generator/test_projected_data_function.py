"""Tests for the ProjectedDataFunction demonstrating projection pushdown."""

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.examples.table import ProjectedDataFunction
from vgi.testing import run_table_function


class TestProjectedDataFunctionInProcess:
    """In-process tests for projection pushdown."""

    def test_generates_all_columns_without_projection(self) -> None:
        """Without projection, all 4 columns should be generated."""
        outputs, logs = run_table_function(ProjectedDataFunction, args=(5,))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 5
        assert table.num_columns == 4
        assert table.schema.names == ["id", "name", "value", "extra"]

        # Verify data content
        assert table.column("id").to_pylist() == [0, 1, 2, 3, 4]
        assert table.column("name").to_pylist() == [
            "item_0",
            "item_1",
            "item_2",
            "item_3",
            "item_4",
        ]
        assert table.column("value").to_pylist() == [0.0, 1.5, 3.0, 4.5, 6.0]
        assert table.column("extra").to_pylist() == [0, 1, 4, 9, 16]

    def test_projection_two_columns(self) -> None:
        """With projection_ids=[0, 2], only id and value columns should be returned."""
        outputs, logs = run_table_function(
            ProjectedDataFunction,
            args=(5,),
            projection_ids=[0, 2],  # id (0) and value (2)
        )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 5
        assert table.num_columns == 2
        assert table.schema.names == ["id", "value"]

        # Verify only projected columns are present and have correct data
        assert table.column("id").to_pylist() == [0, 1, 2, 3, 4]
        assert table.column("value").to_pylist() == [0.0, 1.5, 3.0, 4.5, 6.0]

    def test_projection_single_column(self) -> None:
        """With projection_ids=[1], only name column should be returned."""
        outputs, logs = run_table_function(
            ProjectedDataFunction,
            args=(3,),
            projection_ids=[1],  # name only
        )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3
        assert table.num_columns == 1
        assert table.schema.names == ["name"]

        assert table.column("name").to_pylist() == ["item_0", "item_1", "item_2"]

    def test_projection_reordered_columns(self) -> None:
        """Projection with different order: [2, 0] should return value, id."""
        outputs, logs = run_table_function(
            ProjectedDataFunction,
            args=(3,),
            projection_ids=[2, 0],  # value first, then id
        )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3
        assert table.num_columns == 2
        # Columns should be in the order specified by projection_ids
        assert table.schema.names == ["value", "id"]

        assert table.column("value").to_pylist() == [0.0, 1.5, 3.0]
        assert table.column("id").to_pylist() == [0, 1, 2]

    def test_projection_all_columns_explicit(self) -> None:
        """Explicit projection of all columns should work like no projection."""
        outputs, logs = run_table_function(
            ProjectedDataFunction,
            args=(3,),
            projection_ids=[0, 1, 2, 3],  # All columns explicitly
        )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3
        assert table.num_columns == 4
        assert table.schema.names == ["id", "name", "value", "extra"]

    def test_projection_last_column_only(self) -> None:
        """With projection_ids=[3], only extra column should be returned."""
        outputs, logs = run_table_function(
            ProjectedDataFunction,
            args=(4,),
            projection_ids=[3],  # extra only (id squared)
        )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 4
        assert table.num_columns == 1
        assert table.schema.names == ["extra"]

        # id squared: 0, 1, 4, 9
        assert table.column("extra").to_pylist() == [0, 1, 4, 9]

    def test_zero_count_with_projection(self) -> None:
        """Zero count with projection should produce no rows."""
        outputs, logs = run_table_function(
            ProjectedDataFunction,
            args=(0,),
            projection_ids=[0, 2],
        )

        assert len(outputs) == 0

    def test_large_batch_with_projection(self) -> None:
        """Large count should work correctly with projection."""
        outputs, logs = run_table_function(
            ProjectedDataFunction,
            args=(2500,),  # Larger than BATCH_SIZE of 1000
            projection_ids=[0, 3],  # id and extra
        )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 2500
        assert table.num_columns == 2
        assert table.schema.names == ["id", "extra"]

        # Verify first and last values
        ids = table.column("id").to_pylist()
        extras = table.column("extra").to_pylist()

        assert ids[0] == 0
        assert ids[-1] == 2499
        assert extras[0] == 0  # 0^2
        assert extras[-1] == 2499 * 2499  # 2499^2

    def test_metadata(self) -> None:
        """ProjectedDataFunction should have correct metadata."""
        meta = ProjectedDataFunction.get_metadata()
        assert meta.name == "projected_data"
        assert meta.max_workers == 1
        assert "generator" in meta.categories

    def test_output_schema_reflects_projection(self) -> None:
        """The output_schema property should reflect the projection."""
        import structlog

        from vgi.invocation import Invocation, InvocationType
        from vgi.table_function import TableFunctionInitInput

        invocation = Invocation(
            function_name="projected_data",
            input_schema=None,
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test",
            arguments=Arguments(positional=(pa.scalar(10),)),
        )
        func = ProjectedDataFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )

        # Without init_data, should return full schema
        assert func.output_schema == ProjectedDataFunction.FULL_SCHEMA

        # After setting init_data with projection, should return projected schema
        func.init_data = TableFunctionInitInput(projection_ids=[0, 2])
        schema = func.output_schema
        assert len(schema) == 2
        assert schema.names == ["id", "value"]


class TestProjectedDataFunctionViaClient:
    """Tests that run via Client subprocess."""

    def test_projection_via_client(self) -> None:
        """Projection should work correctly via Client subprocess."""
        from vgi.client import Client

        with Client("vgi-example-worker", max_workers=1) as client:
            outputs = list(
                client.table_function(
                    function_name="projected_data",
                    arguments=Arguments(positional=(pa.scalar(5),)),
                    projection_ids=[0, 2],  # id and value only
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 5
        assert table.num_columns == 2
        assert table.schema.names == ["id", "value"]

        assert table.column("id").to_pylist() == [0, 1, 2, 3, 4]
        assert table.column("value").to_pylist() == [0.0, 1.5, 3.0, 4.5, 6.0]

    def test_all_columns_via_client(self) -> None:
        """All columns should be returned when no projection specified."""
        from vgi.client import Client

        with Client("vgi-example-worker", max_workers=1) as client:
            outputs = list(
                client.table_function(
                    function_name="projected_data",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3
        assert table.num_columns == 4
        assert table.schema.names == ["id", "name", "value", "extra"]
