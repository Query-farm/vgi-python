# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Scalar-function fixtures.

Originally a single 1,568-line module; split into cohesive sub-modules and
re-exported here so existing import sites (worker.py, tests) keep working
unchanged.

* :mod:`._common`           — numeric type promotion helpers
* :mod:`.arithmetic`        — multiply, double, add_values, sum_values, concat_*
* :mod:`.formatting`        — format_number_*, smart_format_*
* :mod:`.null_handling`     — null_handling, conditional_message
* :mod:`.binary`            — binary_packet, upper_case
* :mod:`.random_demo`       — random_int, random_bytes, bernoulli, hash_seed
* :mod:`.type_info`         — type_info_*, any_mixed_*, pair_type_*
* :mod:`.geo`               — geo_distance_*, geo_centroid_*
* :mod:`.settings_secrets`  — multiply_by_setting, return_secret_value, who_am_i
"""

from vgi._test_fixtures.scalar.arithmetic import (
    AddValuesFunction,
    CachedAddConstScalarFunction,
    CachedDoubleScalarFunction,
    CachedLabelScalarFunction,
    ConcatValuesIntFunction,
    ConcatValuesStrFunction,
    DoubleFunction,
    MultiplyFunction,
    SumValuesFunction,
)
from vgi._test_fixtures.scalar.bench_ladder import (
    CollatzStepsFunction,
    PassthruFunction,
    HashRoundsFunction,
    Sha256HexFunction,
)
from vgi._test_fixtures.scalar.binary import (
    BinaryPacketFunction,
    UpperCaseFunction,
)
from vgi._test_fixtures.scalar.formatting import (
    FormatNumberDefaultFunction,
    FormatNumberFullFunction,
    FormatNumberPrecisionFunction,
    SmartFormatPrefixFunction,
    SmartFormatWidthFunction,
)
from vgi._test_fixtures.scalar.geo import (
    _POINT_STRUCT_TYPE,
    GeoCentroidFixedFunction,
    GeoCentroidListFunction,
    GeoCentroidStructFunction,
    GeoDistanceFixedFunction,
    GeoDistanceListFunction,
    GeoDistanceStructFunction,
)
from vgi._test_fixtures.scalar.null_handling import (
    ConditionalMessageFunction,
    NullHandlingFunction,
)
from vgi._test_fixtures.scalar.random_demo import (
    BernoulliFunction,
    HashSeedFunction,
    QuerySeedFunction,
    RandomBytesFunction,
    RandomIntFunction,
)
from vgi._test_fixtures.scalar.settings_secrets import (
    MultiplyBySettingFunction,
    ReturnSecretValueFunction,
    ScaleBySettingFunction,
    SecretFieldFunction,
    WhoAmIFunction,
)
from vgi._test_fixtures.scalar.type_info import (
    AnyMixedIntFunction,
    AnyMixedStrFunction,
    PairTypeIntIntFunction,
    PairTypeIntStrFunction,
    PairTypeStrStrFunction,
    TypeInfoInt32Function,
    TypeInfoInt64Function,
    TypeInfoStringFunction,
    TypeInfoUInt32Function,
    TypeInfoUInt64Function,
)

__all__ = [
    "_POINT_STRUCT_TYPE",
    "AddValuesFunction",
    "AnyMixedIntFunction",
    "AnyMixedStrFunction",
    "BernoulliFunction",
    "BinaryPacketFunction",
    "CachedAddConstScalarFunction",
    "CachedDoubleScalarFunction",
    "CachedLabelScalarFunction",
    "CollatzStepsFunction",
    "ConcatValuesIntFunction",
    "ConcatValuesStrFunction",
    "ConditionalMessageFunction",
    "DoubleFunction",
    "HashRoundsFunction",
    "PassthruFunction",
    "Sha256HexFunction",
    "FormatNumberDefaultFunction",
    "FormatNumberFullFunction",
    "FormatNumberPrecisionFunction",
    "GeoCentroidFixedFunction",
    "GeoCentroidListFunction",
    "GeoCentroidStructFunction",
    "GeoDistanceFixedFunction",
    "GeoDistanceListFunction",
    "GeoDistanceStructFunction",
    "HashSeedFunction",
    "MultiplyBySettingFunction",
    "MultiplyFunction",
    "NullHandlingFunction",
    "PairTypeIntIntFunction",
    "PairTypeIntStrFunction",
    "PairTypeStrStrFunction",
    "QuerySeedFunction",
    "RandomBytesFunction",
    "RandomIntFunction",
    "ReturnSecretValueFunction",
    "ScaleBySettingFunction",
    "SecretFieldFunction",
    "SmartFormatPrefixFunction",
    "SmartFormatWidthFunction",
    "SumValuesFunction",
    "TypeInfoInt32Function",
    "TypeInfoInt64Function",
    "TypeInfoStringFunction",
    "TypeInfoUInt32Function",
    "TypeInfoUInt64Function",
    "UpperCaseFunction",
    "WhoAmIFunction",
]
