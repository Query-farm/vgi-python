# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Ensure every opaque request body's schema is available to SDK generators."""

from __future__ import annotations

import inspect
import typing

from vgi.codegen._common import REQUEST_TYPES
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
