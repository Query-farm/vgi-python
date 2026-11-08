# Copyright 2026 Query Farm LLC - https://query.farm

# ruff: noqa: S101, D101, D102, D103
"""Protocol 3.2 filtered scan planning (docs/protocol/scan-planning.md).

Covers the wire records (``ScanBranch.row_count`` / ``column_statistics``,
``TableInfo.scan_planning``), descriptor validation, the
``ScanPlanningFilters`` helper, and the worker dispatch that passes
``pushdown_filters`` only to hooks that declare the parameter.
"""

from __future__ import annotations

import datetime
import json
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.utils import deserialize_record_batch

from vgi.catalog import ScanBranch, Table, TableInfo
from vgi.catalog.catalog_interface import (
    SCAN_PLANNING_FILTERED,
    ColumnStatistics,
    ScanFunctionResult,
    SerializedSchema,
)
from vgi.filter_v2_builder import build_filter_batch, serialize_filter_batch, value_payload
from vgi.scan_planning import ScanPlanningFilters
from vgi.schema_utils import schema

COLUMNS = schema(event_date=pa.date32(), n=pa.int64())
D = datetime.date


def _filters(*predicates: tuple[str, str, Any]) -> ScanPlanningFilters:
    """Build a filter v2 snapshot of ``column op literal`` conjuncts over COLUMNS."""
    doc_predicates = []
    payloads = []
    for i, (column, op, value) in enumerate(predicates):
        doc_predicates.append(
            {
                "id": f"query:{i}",
                "revision": 0,
                "mode": "advisory",
                "source": "query",
                "expression": {
                    "node": "comparison",
                    "op": op,
                    "left": {
                        "node": "column_ref",
                        "column_index": COLUMNS.get_field_index(column),
                        "column_name": column,
                    },
                    "right": {"node": "literal", "value_ref": i},
                },
            }
        )
        payloads.append(value_payload(i, value, COLUMNS.field(column).type))
    document = {
        "encoding": "vgi.filters.v2",
        "semantics": "vgi.duckdb.standard.v1",
        "kind": "snapshot",
        "predicates": doc_predicates,
    }
    raw = serialize_filter_batch(build_filter_batch(document, payloads))
    return ScanPlanningFilters(raw, lambda: COLUMNS)


def _day_stats(day: datetime.date) -> list[ColumnStatistics]:
    return [ColumnStatistics("event_date", min=pa.scalar(day), max=pa.scalar(day), has_null=False)]


# --------------------------------------------------------------------------- ScanBranch


def test_scan_branch_statistics_round_trip() -> None:
    branch = ScanBranch(
        function_name="read_parquet",
        positional_arguments=[pa.scalar(["a.parquet"])],
        named_arguments={},
        row_count=42,
        column_statistics=_day_stats(D(2026, 10, 1)),
    )
    batch, _ = deserialize_record_batch(branch.serialize())
    assert batch.schema.field("row_count").type == pa.int64()
    assert batch.schema.field("column_statistics").type == pa.binary()
    restored = ScanBranch.deserialize(batch)
    assert restored.row_count == 42
    assert restored.column_statistics is not None
    (stat,) = restored.column_statistics
    assert stat.column_name == "event_date"
    assert stat.min is not None and stat.min.as_py() == D(2026, 10, 1)
    assert stat.has_null is False


def test_scan_branch_statistics_default_to_null() -> None:
    branch = ScanBranch(function_name="sequence", positional_arguments=[pa.scalar(3)], named_arguments={})
    batch, _ = deserialize_record_batch(branch.serialize())
    assert batch.column("row_count")[0].as_py() is None
    assert batch.column("column_statistics")[0].as_py() is None
    restored = ScanBranch.deserialize(batch)
    assert restored.row_count is None and restored.column_statistics is None


def test_scan_branch_rejects_negative_row_count() -> None:
    with pytest.raises(ValueError, match="row_count"):
        ScanBranch(function_name="f", positional_arguments=[], named_arguments={}, row_count=-1)


def test_scan_branch_rejects_duplicate_statistics() -> None:
    with pytest.raises(ValueError, match="more than once"):
        ScanBranch(
            function_name="f",
            positional_arguments=[],
            named_arguments={},
            column_statistics=_day_stats(D(2026, 1, 1)) + _day_stats(D(2026, 1, 2)),
        )


# --------------------------------------------------------------------------- TableInfo / Table


def _table_info(**kwargs: Any) -> TableInfo:
    return TableInfo(
        name="t",
        schema_path=["main"],
        columns=SerializedSchema(COLUMNS.serialize().to_pybytes()),
        constraints=[],
        comment=None,
        tags={},
        **kwargs,
    )


def test_table_info_scan_planning_defaults_to_bind_and_round_trips() -> None:
    assert _table_info().scan_planning == "bind"
    info = _table_info(scan_planning="filtered")
    restored = TableInfo.deserialize_from_batch(deserialize_record_batch(info.serialize_to_bytes())[0])
    assert restored.scan_planning == "filtered"


def test_table_info_rejects_unknown_scan_planning() -> None:
    with pytest.raises(ValueError, match="scan_planning"):
        _table_info(scan_planning="later")


def test_table_info_rejects_filtered_with_inlined_scan_function() -> None:
    inlined = ScanFunctionResult(function_name="sequence", positional_arguments=[], named_arguments={}).serialize()
    with pytest.raises(ValueError, match="inlined scan_function"):
        _table_info(scan_planning="filtered", scan_function=inlined)


def test_table_descriptor_carries_scan_planning() -> None:
    info = Table(name="t", columns=COLUMNS, scan_planning="filtered").to_table_info(["main"])
    assert info.scan_planning == SCAN_PLANNING_FILTERED
    assert info.scan_function is None


def test_table_descriptor_filtered_requires_columns() -> None:
    from vgi._test_fixtures.table.sequence import SequenceFunction

    with pytest.raises(ValueError, match="requires columns="):
        Table(name="t", function=SequenceFunction, scan_planning="filtered")


def test_table_descriptor_rejects_unknown_scan_planning() -> None:
    with pytest.raises(ValueError, match="scan_planning"):
        Table(name="t", columns=COLUMNS, scan_planning="eventually")


# --------------------------------------------------------------------------- ScanPlanningFilters


def test_filters_decode_against_table_columns() -> None:
    decoded = _filters(("event_date", "ge", D(2026, 10, 3))).decode()
    bounds = decoded.get_column_bounds("event_date")
    assert bounds is not None and bounds.min_value is not None
    assert bounds.min_value.as_py() == D(2026, 10, 3)


@pytest.mark.parametrize(
    ("predicates", "matching_days"),
    [
        ((("event_date", "ge", D(2026, 10, 3)),), [3, 4, 5]),
        ((("event_date", "gt", D(2026, 10, 3)),), [4, 5]),
        ((("event_date", "lt", D(2026, 10, 2)),), [1]),
        ((("event_date", "le", D(2026, 10, 2)),), [1, 2]),
        ((("event_date", "eq", D(2026, 10, 4)),), [4]),
        ((("event_date", "ge", D(2026, 10, 2)), ("event_date", "lt", D(2026, 10, 4))), [2, 3]),
        ((("event_date", "eq", D(2027, 1, 1)),), []),
        # A predicate on a column without statistics never prunes.
        ((("n", "eq", 7),), [1, 2, 3, 4, 5]),
    ],
)
def test_may_match_prunes_by_min_max(predicates: Any, matching_days: list[int]) -> None:
    filters = _filters(*predicates)
    days = [d for d in range(1, 6) if filters.may_match(_day_stats(D(2026, 10, d)))]
    assert days == matching_days


def test_may_match_or_and_in() -> None:
    def or_filters(*days: int) -> ScanPlanningFilters:
        exprs = [
            {
                "node": "comparison",
                "op": "eq",
                "left": {"node": "column_ref", "column_index": 0, "column_name": "event_date"},
                "right": {"node": "literal", "value_ref": i},
            }
            for i in range(len(days))
        ]
        document = {
            "encoding": "vgi.filters.v2",
            "semantics": "vgi.duckdb.standard.v1",
            "kind": "snapshot",
            "predicates": [
                {
                    "id": "query:0",
                    "revision": 0,
                    "mode": "advisory",
                    "source": "query",
                    "expression": {"node": "or", "children": exprs},
                }
            ],
        }
        payloads = [value_payload(i, D(2026, 10, d), pa.date32()) for i, d in enumerate(days)]
        return ScanPlanningFilters(serialize_filter_batch(build_filter_batch(document, payloads)), lambda: COLUMNS)

    filters = or_filters(1, 3)
    assert [d for d in range(1, 6) if filters.may_match(_day_stats(D(2026, 10, d)))] == [1, 3]


def test_may_match_keeps_candidates_without_bounds() -> None:
    filters = _filters(("event_date", "eq", D(2026, 10, 4)))
    assert filters.may_match([ColumnStatistics("event_date")])
    assert filters.may_match([])


def test_may_match_entirely_null_column() -> None:
    filters = _filters(("event_date", "eq", D(2026, 10, 4)))
    assert not filters.may_match([ColumnStatistics("event_date", has_null=True, has_not_null=False)])


def test_may_match_spanning_range() -> None:
    filters = _filters(("event_date", "eq", D(2026, 10, 4)))
    stats = [ColumnStatistics("event_date", min=pa.scalar(D(2026, 10, 1)), max=pa.scalar(D(2026, 10, 31)))]
    assert filters.may_match(stats)
    stats = [ColumnStatistics("event_date", min=pa.scalar(D(2026, 11, 1)), max=pa.scalar(D(2026, 11, 30)))]
    assert not filters.may_match(stats)


# --------------------------------------------------------------------------- worker dispatch


@pytest.fixture
def client(client_transport: Any) -> Any:
    with client_transport() as c:
        yield c


def _attach(client: Any) -> Any:
    return client.catalog_attach(name="example", data_version_spec=None, implementation_version=None).attach_opaque_data


def _last_plan(tmp_dir: str) -> dict[str, Any]:
    with open(f"{tmp_dir}/planned_days_last_plan.json", encoding="utf-8") as fh:
        return json.load(fh)  # type: ignore[no-any-return]


def test_filtered_table_info_reaches_the_client(client: Any) -> None:
    info = client.table_get(attach_opaque_data=_attach(client), schema_path=["data"], name="planned_days")
    assert info.scan_planning == "filtered"
    assert info.scan_function is None


def test_worker_hook_receives_filters_and_prunes(client: Any) -> None:
    from vgi._test_fixtures.worker import _BRANCH_DIR

    filters = _filters(("event_date", "ge", D(2026, 10, 4)))
    result = client.table_scan_branches_get(
        attach_opaque_data=_attach(client),
        schema_path=["data"],
        name="planned_days",
        pushdown_filters=filters.raw,
    )
    assert len(result.branches) == 1
    files = result.branches[0].positional_arguments[0].as_py()
    assert [f.rsplit("/", 1)[1] for f in files] == [
        "planned_days_2026-10-04.parquet",
        "planned_days_2026-10-05.parquet",
    ]
    assert result.branches[0].row_count == 20
    plan = _last_plan(_BRANCH_DIR)
    assert plan == {"received_filters": True, "days": ["2026-10-04", "2026-10-05"]}


def test_worker_returns_zero_branches_when_nothing_matches(client: Any) -> None:
    filters = _filters(("event_date", "lt", D(2026, 1, 1)))
    result = client.table_scan_branches_get(
        attach_opaque_data=_attach(client),
        schema_path=["data"],
        name="planned_days",
        pushdown_filters=filters.raw,
    )
    assert result.branches == []


def test_worker_without_filters_returns_everything(client: Any) -> None:
    result = client.table_scan_branches_get(
        attach_opaque_data=_attach(client), schema_path=["data"], name="planned_days"
    )
    assert len(result.branches[0].positional_arguments[0].as_py()) == 5


def test_branch_statistics_reach_the_client(client: Any) -> None:
    result = client.table_scan_branches_get(
        attach_opaque_data=_attach(client), schema_path=["data"], name="planned_days_stats"
    )
    assert len(result.branches) == 5
    for day, branch in zip(range(1, 6), result.branches, strict=True):
        assert branch.row_count == 10
        assert branch.column_statistics is not None
        assert branch.column_statistics[0].min.as_py() == D(2026, 10, day)


def test_hooks_without_the_parameter_still_work(client: Any) -> None:
    """``data.numbers`` is served by the default plan hook, which ignores the predicates."""
    filters = _filters(("n", "eq", 1))
    result = client.table_scan_branches_get(
        attach_opaque_data=_attach(client),
        schema_path=["data"],
        name="numbers",
        pushdown_filters=filters.raw,
    )
    assert len(result.branches) == 1


def test_default_plan_hook_delegates_to_branches_get() -> None:
    """A 3.1-style catalog that only overrides table_scan_branches_get keeps working."""
    from vgi.catalog import ScanBranchesResult
    from vgi.catalog.catalog_interface import ReadOnlyCatalogInterface

    class Old(ReadOnlyCatalogInterface):
        def __init__(self) -> None:
            pass

        def table_scan_branches_get(self, **kwargs: Any) -> ScanBranchesResult:
            return ScanBranchesResult(branches=[], required_extensions=["sentinel"])

    result = Old().table_scan_branches_plan(
        attach_opaque_data=b"",
        transaction_opaque_data=None,
        schema_path=["main"],
        name="t",
        at_unit=None,
        at_value=None,
        pushdown_filters=_filters(("n", "eq", 1)),
    )
    assert result.required_extensions == ["sentinel"]
