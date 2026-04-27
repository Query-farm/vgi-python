"""Tests for BinaryPacketFunction via Client."""

from __future__ import annotations

import pyarrow as pa

from vgi import schema
from vgi.arguments import Arguments
from vgi.client import Client


class TestBinaryPacketFunction:
    """Tests for BinaryPacketFunction via Client subprocess."""

    def test_binary_packet_basic(self, fixture_worker: str) -> None:
        """Header + payload + config suffix concatenation."""
        s = schema(payload=pa.binary())
        batch = pa.RecordBatch.from_pydict({"payload": [b"\x01\x02", b"\x03\x04"]}, schema=s)

        # ConstParam args only: header and config (payload is a Param from batch columns)
        header = pa.scalar(b"\xca\xfe", type=pa.binary())
        config = pa.scalar(
            {"label": "v1", "version": 1},
            type=pa.struct([("label", pa.string()), ("version", pa.int64())]),
        )

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="binary_packet",
                    input=iter([batch]),
                    arguments=Arguments(positional=(header, config)),
                )
            )

        assert len(outputs) == 1
        results = outputs[0].column("result").to_pylist()
        # suffix = "v1".encode() + bytes([1]) = b"v1\x01"
        assert results[0] == b"\xca\xfe\x01\x02v1\x01"
        assert results[1] == b"\xca\xfe\x03\x04v1\x01"

    def test_binary_packet_null_payload(self, fixture_worker: str) -> None:
        """Null payload gets header + suffix only."""
        s = schema(payload=pa.binary())
        batch = pa.RecordBatch.from_pydict({"payload": [None]}, schema=s)

        header = pa.scalar(b"\xff", type=pa.binary())
        config = pa.scalar(
            {"label": "x", "version": 2},
            type=pa.struct([("label", pa.string()), ("version", pa.int64())]),
        )

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="binary_packet",
                    input=iter([batch]),
                    arguments=Arguments(positional=(header, config)),
                )
            )

        assert len(outputs) == 1
        results = outputs[0].column("result").to_pylist()
        # No payload bytes, just header + suffix
        assert results[0] == b"\xffx\x02"
