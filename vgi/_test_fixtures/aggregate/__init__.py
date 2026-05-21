# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Aggregate-function fixtures.

Originally a single 1,179-line module; split into cohesive sub-modules and
re-exported here so existing import sites (worker.py, tests) keep working
unchanged.

* :mod:`._common`     — shared SumState, ListAggState
* :mod:`.basic`       — count, sum, avg, weighted_sum
* :mod:`.listagg`     — list_agg (order-dependent string concatenation)
* :mod:`.percentile`  — percentile (sorted-quantile demo)
* :mod:`.generic`     — generic_sum (any-type aggregate)
* :mod:`.varargs`     — sum_all (varargs over numeric columns)
* :mod:`.dynamic`     — dynamic_aggregate, dynamic_ml_aggregate
                        (gated on VGI_WORKER_SUPPORTS_DYNAMIC_CODE)
* :mod:`.window`      — window_sum, window_median, window_listagg
* :mod:`.streaming`   — streaming_sum (streaming-partitioned protocol)
"""

from vgi._test_fixtures.aggregate._common import ListAggState, SumState
from vgi._test_fixtures.aggregate.basic import (
    AvgFunction,
    CountFunction,
    SumFunction,
    WeightedSumFunction,
)
from vgi._test_fixtures.aggregate.dynamic import (
    DynamicAggregateFunction,
    DynamicMLAggregateFunction,
)
from vgi._test_fixtures.aggregate.generic import GenericSumFunction
from vgi._test_fixtures.aggregate.listagg import ListAggFunction
from vgi._test_fixtures.aggregate.percentile import PercentileFunction
from vgi._test_fixtures.aggregate.streaming import StreamingSumFunction
from vgi._test_fixtures.aggregate.varargs import SumAllFunction
from vgi._test_fixtures.aggregate.window import (
    WindowListAggFunction,
    WindowMedianFunction,
    WindowSumBatchFunction,
    WindowSumFunction,
)

__all__ = [
    "AvgFunction",
    "CountFunction",
    "DynamicAggregateFunction",
    "DynamicMLAggregateFunction",
    "GenericSumFunction",
    "ListAggFunction",
    "ListAggState",
    "PercentileFunction",
    "StreamingSumFunction",
    "SumAllFunction",
    "SumFunction",
    "SumState",
    "WeightedSumFunction",
    "WindowListAggFunction",
    "WindowMedianFunction",
    "WindowSumBatchFunction",
    "WindowSumFunction",
]
