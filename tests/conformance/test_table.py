"""Conformance stub for ``vgi/test/sql/integration/table/``."""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "table",
    [
        "column_statistics.test",
        "comments.test",
        "constant_columns.test",
        "constant_columns_types.test",
        "constraints.test",
        "constraints_time_travel.test",
        "database_tags.test",
        "defaults.test",
        "double_sequence.test",
        "dynamic_filter.test",
        "expression_filter.test",
        "filter_echo.test",
        "filter_pushdown.test",
        "function_registration.test",
        "generated_columns.test",
        "generator_exception.test",
        "join_keys_pushdown.test",
        "joins.test",
        "logging_generator.test",
        "named_params.test",
        "order_pushdown.test",
        "partitioned_sequence.test",
        "projected_data.test",
        "projection_info.test",
        "rowid.test",
        "sequence.test",
        "set_operations_and_subqueries.test",
        "table_function_statistics.test",
        "tablesample.test",
        "time_travel.test",
    ],
)
