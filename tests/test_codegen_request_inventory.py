# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Ensure every opaque request body's schema is available to SDK generators."""

from __future__ import annotations

import inspect
import typing

from vgi.codegen._common import EXTRA_RESPONSE_TYPES, REQUEST_TYPES, SHARED_CONTRACT_TYPES
from vgi.codegen.go_types import GO_TYPE_RECORDS
from vgi.codegen.java_types import JAVA_RECORDS
from vgi.codegen.rust_types import RUST_TYPES
from vgi.codegen.ts_types import TS_RECORD_TYPES
from vgi.protocol import VgiProtocol


def test_every_request_dataclass_is_in_codegen_inventory() -> None:
    """An RPC ``request: binary`` body must not be invisible to other SDKs."""
    declared = set(REQUEST_TYPES)
    missing: dict[str, str] = {}
    for method_name, method in inspect.getmembers(VgiProtocol, inspect.isfunction):
        request_type = typing.get_type_hints(method).get("request")
        if isinstance(request_type, type) and hasattr(request_type, "ARROW_SCHEMA") and request_type not in declared:
            missing[method_name] = request_type.__name__

    assert not missing, f"Opaque request dataclasses missing from REQUEST_TYPES: {missing}"


def test_shared_contract_records_have_schemas_and_all_typed_models() -> None:
    """Preparing common types must not leave a worker SDK with only raw bytes."""
    required = set(SHARED_CONTRACT_TYPES)
    assert required <= set(EXTRA_RESPONSE_TYPES)  # C++ schemas and C# models use this inventory.
    for records in (GO_TYPE_RECORDS, JAVA_RECORDS, RUST_TYPES, TS_RECORD_TYPES):
        assert required <= set(records)
