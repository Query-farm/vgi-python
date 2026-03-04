"""Tests for ReturnSecretValueFunction via Client."""

from __future__ import annotations

import json

import pyarrow as pa

from vgi import schema
from vgi.client import Client


class TestReturnSecretValueFunction:
    """Tests for ReturnSecretValueFunction via Client subprocess."""

    def test_return_secret_value_basic(self, example_worker: str) -> None:
        """Pass a secret dict and verify JSON output matches."""
        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": [1, 2, 3]}, schema=s)

        secret_value = {"type": "test_secret", "provider": "config", "secret_string": "s3cr3t"}

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="return_secret_value",
                    input=iter([batch]),
                    secrets={"vgi_example": secret_value},
                )
            )

        assert len(outputs) == 1
        assert outputs[0].num_rows == 3

        # Each row should contain the JSON-encoded secret dict
        results = outputs[0].column("result").to_pylist()
        for result_str in results:
            parsed = json.loads(str(result_str))
            assert parsed == secret_value
