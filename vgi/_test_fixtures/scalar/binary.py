# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Binary-payload and string-cased scalar fixtures (binary_packet, upper_case)."""

from __future__ import annotations

from typing import Annotated, Any

import pyarrow as pa
import pyarrow.compute as pc

from vgi.arguments import ConstParam, Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

_CONFIG_STRUCT_TYPE = pa.struct([("label", pa.string()), ("version", pa.int64())])


class BinaryPacketFunction(ScalarFunction):
    """Builds binary packets with header, payload, and config metadata.

    This example demonstrates complex ConstParam types:
    - header (binary): Constant prefix bytes at the start
    - payload (binary column): Variable binary data per row
    - config (struct): Constant metadata struct at the end

    The constant parameters bracket the column parameter (first and last).

    The function concatenates: header + payload + config.label encoded + version byte

    Example:
        SQL:    SELECT binary_packet(x'CAFE', data, {label: 'v1', version: 1}) FROM t
        Input:  data=[x'0102', x'0304']
        Args:   header=x'CAFE', config={label: 'v1', version: 1}
        Output: result=[x'CAFE0102763101', x'CAFE0304763101']

    """

    class Meta:
        """Function metadata."""

        name = "binary_packet"
        description = "Build binary packets with header, payload, and config"
        examples = [
            FunctionExample(
                sql="SELECT binary_packet(x'FF', payload, {label: 'msg', version: 1}) FROM t",
                description="Build packets with 0xFF header",
            ),
        ]

    @classmethod
    def compute(
        cls,
        header: Annotated[
            bytes,
            ConstParam("Header bytes to prepend", arrow_type=pa.binary()),
        ],
        payload: Annotated[pa.BinaryArray, Param(doc="Binary payload data")],
        config: Annotated[
            dict[str, Any],
            ConstParam("Config {label, version}", arrow_type=_CONFIG_STRUCT_TYPE),
        ],
    ) -> Annotated[pa.BinaryArray, Returns()]:
        """Build binary packets from header, payload, and config."""
        # Extract config fields
        label: str = config["label"]
        version: int = config["version"]

        # Build suffix from config: label bytes + version as single byte
        suffix = label.encode("utf-8") + bytes([version & 0xFF])

        # Concatenate header + payload + suffix for each row
        results: list[bytes] = []
        for i in range(len(payload)):
            if payload[i].is_valid:
                payload_bytes: bytes = payload[i].as_py()
                results.append(header + payload_bytes + suffix)
            else:
                results.append(header + suffix)  # Empty payload for nulls

        return pa.array(results, type=pa.binary())


class UpperCaseFunction(ScalarFunction):
    """Converts string values to uppercase.

    This example demonstrates type inference with pa.StringArray:
    - pa.StringArray -> pa.string() (inferred from Annotated type)
    - Returns() output type is also inferred from pa.StringArray

    Example:
        SQL:    SELECT upper_case(name) FROM users
        Input:  name=["alice", "bob", "charlie"]
        Output: result=["ALICE", "BOB", "CHARLIE"]

    """

    class Meta:
        """Function metadata."""

        name = "upper_case"
        description = "Converts string values to uppercase"
        examples = [
            FunctionExample(
                sql="SELECT upper_case(name) FROM users",
                description="Convert user names to uppercase",
            ),
            FunctionExample(
                sql="SELECT upper_case(status) FROM orders WHERE id = 1",
                description="Uppercase the status field",
            ),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.StringArray, Param(doc="String value to uppercase")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Convert the string values to uppercase."""
        return pc.utf8_upper(value)
