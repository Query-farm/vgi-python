"""Table-function fixtures.

Originally a single 3,270-line module; split into cohesive sub-modules and
re-exported here so existing import sites (worker.py, tests) keep working
unchanged.

If you're looking for a specific fixture, the module names below should
point you at the right file:

* :mod:`._common`         — ``CountdownState``, ``_BaseSequenceFunction``
* :mod:`.sequence`        — sequence / partitioned / nested / row_id
* :mod:`.make_series`     — make_series_count / range / step / csv / float
* :mod:`.pairs`           — make_pairs_*, repeat_value_*, constant_columns
* :mod:`.settings`        — settings_aware, struct_settings, secret_demo
* :mod:`.filters`         — filter_echo, dynamic_filter_echo, expression_filter,
                            spatial_filter
* :mod:`.catalog_scans`   — colors / departments / employees / products / projects
* :mod:`.versioned`       — versioned_data + versioned_constraints (time travel)
* :mod:`.misc`            — projected_data, generator_exception,
                            logging_generator, order_echo, sample_echo
"""

from vgi._test_fixtures.table.batch_index import (
    PartitionedBatchIndexFunction,
    PartitionedBatchIndexMarkedFunction,
)
from vgi._test_fixtures.table.batch_index_broken import (
    BatchIndexOverflowFunction,
    MissingBatchIndexTagFunction,
    NonMonotoneBatchIndexFunction,
)
from vgi._test_fixtures.table.catalog_scans import (
    ColorsScanFunction,
    DepartmentsScanFunction,
    EmployeesScanFunction,
    ProductsScanFunction,
    ProjectsScanFunction,
)
from vgi._test_fixtures.table.filters import (
    DynamicFilterEchoFunction,
    ExpressionFilterTestFunction,
    FilterEchoFunction,
    FilterEchoPartitionedFunction,
    SpatialFilterExampleFunction,
)
from vgi._test_fixtures.table.make_series import (
    MakeSeriesCountFunction,
    MakeSeriesCsvFunction,
    MakeSeriesFloatFunction,
    MakeSeriesRangeFunction,
    MakeSeriesStepFunction,
)
from vgi._test_fixtures.table.misc import (
    GeneratorExceptionFunction,
    LoggingGeneratorFunction,
    OrderEchoFunction,
    ProjectedDataFunction,
    SampleEchoFunction,
)
from vgi._test_fixtures.table.order_modes import (
    PartitionedFixedOrderFunction,
    PartitionedNoOrderGuaranteeFunction,
    PartitionedPreservesOrderFunction,
)
from vgi._test_fixtures.table.pairs import (
    ConstantColumnsFunction,
    MakePairsIntFunction,
    MakePairsIntStrFunction,
    MakePairsStrFunction,
    RepeatValueIntFunction,
    RepeatValueStrFunction,
)
from vgi._test_fixtures.table.partition_columns import (
    CountryPartitionedSalesFunction,
    DisjointRangePartitionedFunction,
    PartitionedWithExplicitOverrideFunction,
    RegionYearPartitionedFunction,
)
from vgi._test_fixtures.table.partition_columns_broken import (
    BrokenMissingPartitionValuesFunction,
    BrokenPartitionColumnAbsentFromBatchFunction,
    BrokenPartitionMinNeqMaxFunction,
    BrokenPartitionValuesNoAnnotationFunction,
)
from vgi._test_fixtures.table.profiling_example import (
    ProfilingDemoFunction,
)
from vgi._test_fixtures.table.sequence import (
    DoubleSequenceFunction,
    NamedParamsEchoFunction,
    NestedSequenceFunction,
    PartitionedSequenceFunction,
    RowIdSequenceFunction,
    SequenceFunction,
    TenThousandFunction,
)
from vgi._test_fixtures.table.settings import (
    ScopedSecretDemoFunction,
    SecretDemoFunction,
    SettingsAwareFunction,
    StructSettingsFunction,
)
from vgi._test_fixtures.table.transaction_storage import TxCachedValueFunction
from vgi._test_fixtures.table.versioned import (
    _CURRENT_VERSION,
    _VERSIONED_CONSTRAINTS_CURRENT,
    _VERSIONED_CONSTRAINTS_DATA,
    _VERSIONED_CONSTRAINTS_SCHEMAS,
    _VERSIONED_DATA,
    _VERSIONED_SCHEMAS,
    VersionedConstraintsScanFunction,
    VersionedDataFunction,
    resolve_version,
    resolve_versioned_constraints_version,
)

__all__ = [
    "_CURRENT_VERSION",
    "_VERSIONED_CONSTRAINTS_CURRENT",
    "_VERSIONED_CONSTRAINTS_DATA",
    "_VERSIONED_CONSTRAINTS_SCHEMAS",
    "_VERSIONED_DATA",
    "_VERSIONED_SCHEMAS",
    "BatchIndexOverflowFunction",
    "BrokenMissingPartitionValuesFunction",
    "BrokenPartitionColumnAbsentFromBatchFunction",
    "BrokenPartitionMinNeqMaxFunction",
    "BrokenPartitionValuesNoAnnotationFunction",
    "ColorsScanFunction",
    "ConstantColumnsFunction",
    "CountryPartitionedSalesFunction",
    "DisjointRangePartitionedFunction",
    "DepartmentsScanFunction",
    "DoubleSequenceFunction",
    "DynamicFilterEchoFunction",
    "EmployeesScanFunction",
    "ExpressionFilterTestFunction",
    "FilterEchoFunction",
    "FilterEchoPartitionedFunction",
    "GeneratorExceptionFunction",
    "LoggingGeneratorFunction",
    "MakePairsIntFunction",
    "MakePairsIntStrFunction",
    "MakePairsStrFunction",
    "MakeSeriesCountFunction",
    "MakeSeriesCsvFunction",
    "MakeSeriesFloatFunction",
    "MakeSeriesRangeFunction",
    "MakeSeriesStepFunction",
    "MissingBatchIndexTagFunction",
    "NamedParamsEchoFunction",
    "NestedSequenceFunction",
    "NonMonotoneBatchIndexFunction",
    "OrderEchoFunction",
    "PartitionedBatchIndexFunction",
    "PartitionedBatchIndexMarkedFunction",
    "PartitionedFixedOrderFunction",
    "PartitionedNoOrderGuaranteeFunction",
    "PartitionedPreservesOrderFunction",
    "PartitionedSequenceFunction",
    "PartitionedWithExplicitOverrideFunction",
    "ProductsScanFunction",
    "ProfilingDemoFunction",
    "ProjectedDataFunction",
    "ProjectsScanFunction",
    "RegionYearPartitionedFunction",
    "RepeatValueIntFunction",
    "RepeatValueStrFunction",
    "RowIdSequenceFunction",
    "SampleEchoFunction",
    "ScopedSecretDemoFunction",
    "SecretDemoFunction",
    "SequenceFunction",
    "SettingsAwareFunction",
    "SpatialFilterExampleFunction",
    "StructSettingsFunction",
    "TenThousandFunction",
    "TxCachedValueFunction",
    "VersionedConstraintsScanFunction",
    "VersionedDataFunction",
    "resolve_version",
    "resolve_versioned_constraints_version",
]
