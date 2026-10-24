# Copyright 2026 Query Farm LLC - https://query.farm

"""Exercise optional projection negotiation and execution through public RPCs.

The portable fixture is example/main/projection_rows, backed by
projection_sequence(). It emits nullable int32 n=0..9 and accepts only the
canonical CAST("n" AS BIGINT) expression in the default evaluation context.
"""

from __future__ import annotations

import argparse
import json
from contextlib import closing
from dataclasses import replace

import pyarrow as pa
from vgi_rpc.rpc import RpcError

from vgi.arguments import Arguments
from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.sql import SqlExpression
from vgi.client import Client
from vgi.invocation import BindResponse, FunctionType
from vgi.protocol import (
    BindRequest,
    ClientCapabilities,
    InitRequest,
    TableFunctionProjectionRequest,
)


def proposal(bind_call, bind_result, sql, *, context=None):
    """Construct typed, canonical candidate fields in stable proposal order."""
    return TableFunctionProjectionRequest(
        bind_call=bind_call,
        bind_result=bind_result,
        expressions=[
            SqlExpression(
                sql=text,
                profile=ROW_SCALAR_EXPRESSION,
                schema_path=["main"],
                evaluation_context=context or QueryEvaluationContext(),
            ).serialize_to_bytes()
            for text in sql
        ],
        result_schema=pa.schema(
            [
                pa.field(f"wide_{index}", pa.int64(), metadata={b"projection-fixture": str(index).encode()})
                for index in range(len(sql))
            ]
        ),
    )


def scan(proxy, bind_call, bind_result):
    """Consume the existing producer stream with the selected bind contract."""
    with closing(
        proxy.init(
            request=InitRequest(
                bind_call=bind_call,
                output_schema=bind_result.output_schema,
                bind_opaque_data=bind_result.opaque_data,
            )
        )
    ) as stream:
        batches = [output.batch for output in stream]
    for batch in batches:
        assert batch.schema.equals(bind_result.output_schema, check_metadata=True), batch.schema
    actual = pa.Table.from_batches(batches, schema=bind_result.output_schema)
    assert actual.to_pylist() == [
        {field.name: number for field in bind_result.output_schema} for number in range(10)
    ], actual.to_pylist()


class RawProjectionRequest(TableFunctionProjectionRequest):
    """Send deliberately malformed wire bytes without local model validation."""

    def serialize_to_bytes(self):
        """Return the intentional invalid test payload unchanged."""
        return self.raw_payload


def raw_request(request, field, value):
    """Replace one packed field while retaining the exact outer wire schema."""
    with pa.ipc.open_stream(request.serialize_to_bytes()) as reader:
        batch = reader.read_next_batch()
    index = batch.schema.get_field_index(field)
    columns = list(batch.columns)
    columns[index] = pa.array([value], type=batch.schema.field(index).type)
    return raw_batch(pa.RecordBatch.from_arrays(columns, schema=batch.schema))


def raw_batch(batch):
    """Pack an intentionally invalid row count or schema for wire tests."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    invalid = object.__new__(RawProjectionRequest)
    object.__setattr__(invalid, "raw_payload", sink.getvalue().to_pybytes())
    return invalid


def invalid_cases(request):
    """Exercise handler boundaries independently of Python's local validators."""
    expressions = request.expression_records()
    return [
        ("empty expressions", "expressions", []),
        ("malformed expression IPC", "expressions", [b"invalid", request.expressions[1]]),
        ("wrong result count", "result_schema", pa.schema([]).serialize().to_pybytes()),
        (
            "source/result name collision",
            "result_schema",
            pa.schema([pa.field("N", pa.int64()), pa.field("wide_1", pa.int64())]).serialize().to_pybytes(),
        ),
        (
            "unexpected result schema metadata",
            "result_schema",
            request.result_schema.with_metadata({b"not": b"a new source"}).serialize().to_pybytes(),
        ),
        ("absent owner", "bind_call", replace(request.bind_call, schema_path=None).serialize_to_bytes()),
        (
            "unoffered profile",
            "bind_result",
            replace(request.bind_result, projection_expression_profiles=[]).serialize_to_bytes(),
        ),
        (
            "different expression contexts",
            "expressions",
            [
                request.expressions[0],
                replace(
                    expressions[1], evaluation_context=replace(QueryEvaluationContext(), timezone="Europe/Paris")
                ).serialize_to_bytes(),
            ],
        ),
    ]


def run(worker):
    """Return named successes, failing immediately on any protocol mismatch."""
    client = Client.from_http(worker) if worker.startswith(("http://", "https://")) else Client(worker, pool=None)
    results = []
    with client:
        attached = client.catalog_attach(
            name="example",
            data_version_spec=None,
            implementation_version=None,
            client_capabilities=ClientCapabilities(
                engine="projection-conformance", native_formats=[], catalogs=[], can_stream=True, filter_encodings=[]
            ),
        )
        client.table_get(attach_opaque_data=attached.attach_opaque_data, schema_path=["main"], name="projection_rows")
        bind_call = BindRequest(
            function_name="projection_sequence",
            schema_path=["main"],
            function_type=FunctionType.TABLE,
            arguments=Arguments(),
            attach_opaque_data=attached.attach_opaque_data,
        )
        # The low-level typed proxy intentionally avoids Client.table_function(),
        # which would perform a fresh ordinary bind and hide accepted state.
        assert client._primary is not None
        proxy = client._primary.proxy
        original = proxy.bind(request=bind_call)
        assert original.projection_expression_profiles == [ROW_SCALAR_EXPRESSION]
        assert original.output_schema.equals(pa.schema([pa.field("n", pa.int32())]), check_metadata=True)
        scan(proxy, bind_call, original)
        results.append("ordinary scan before negotiation")

        cases = [
            ("partial acceptance", ['CAST("n" AS BIGINT)', '"n" + 1', 'CAST("n" AS BIGINT)'], [0, 2], None),
            ("complete decline", ['"n" + 1'], [], None),
            (
                "unsupported context decline",
                ['CAST("n" AS BIGINT)'],
                [],
                replace(QueryEvaluationContext(), timezone="Europe/Paris"),
            ),
            ("complete acceptance", ['CAST("n" AS BIGINT)'], [0], None),
        ]
        for name, expressions, expected_indices, context in cases:
            request = proposal(bind_call, original, expressions, context=context)
            response = proxy.table_function_projection(request=request)
            response.validate_request(request)
            assert response.accepted_indices == expected_indices, response
            selected: BindResponse = response.bind_result or original
            scan(proxy, bind_call, selected)
            # Reusing accepted bind bytes must retain the accepted schema/state.
            if expected_indices:
                scan(proxy, bind_call, selected)
            results.append(name)

        valid = proposal(bind_call, original, ['CAST("n" AS BIGINT)', 'CAST("n" AS BIGINT)'])
        for name, field, value in invalid_cases(valid):
            try:
                proxy.table_function_projection(request=raw_request(valid, field, value))
            except RpcError:
                results.append("reject " + name)
            else:
                raise AssertionError("malformed proposal was not rejected: " + name)

        with pa.ipc.open_stream(valid.serialize_to_bytes()) as reader:
            row = reader.read_next_batch()
        malformed = [
            ("zero proposal rows", row.slice(0, 0)),
            (
                "multiple proposal rows",
                pa.RecordBatch.from_arrays([pa.concat_arrays([column, column]) for column in row], schema=row.schema),
            ),
            ("extra proposal field", row.append_column("unexpected", pa.array([1]))),
        ]
        for name, batch in malformed:
            try:
                proxy.table_function_projection(request=raw_batch(batch))
            except RpcError:
                results.append("reject " + name)
            else:
                raise AssertionError("malformed proposal envelope was not rejected: " + name)

        rebound = proxy.bind(request=bind_call)
        assert rebound.output_schema.equals(original.output_schema, check_metadata=True)
        scan(proxy, bind_call, rebound)
        results.append("independent ordinary rebind")
        client.catalog_detach(attach_opaque_data=attached.attach_opaque_data)
    return results


def main():
    """Run the same transport-level proof against any example worker."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", required=True, help="Worker command or HTTP URL")
    args = parser.parse_args()
    results = run(args.worker)
    print(json.dumps({"passed": len(results), "cases": results}))


if __name__ == "__main__":
    main()
