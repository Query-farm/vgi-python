# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Example implementations for VGI.

This package contains example functions and servers for testing and reference.
"""

from vgi._test_fixtures.table_in_out import (
    BufferInputFunction,
    EchoFunction,
    RepeatInputsFunction,
    SumAllColumnsFunction,
)

__all__ = [
    "EchoFunction",
    "BufferInputFunction",
    "RepeatInputsFunction",
    "SumAllColumnsFunction",
]
