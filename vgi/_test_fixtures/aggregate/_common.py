# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Shared aggregate state classes used across multiple submodules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType


@dataclass(kw_only=True)
class SumState(ArrowSerializableDataclass):
    total: Annotated[int, ArrowType(pa.int64())] = 0


@dataclass(kw_only=True)
class ListAggState(ArrowSerializableDataclass):
    values: Annotated[str, ArrowType(pa.string())] = ""
