# Scalar projection worker proof

Run `python conformance/projection-expressions/run_worker.py --worker COMMAND`
from the Python checkout. An HTTP URL is accepted as well. The runner uses the
typed public bind, proposal and init RPCs against the common example fixture;
it does not simulate optimizer replacement.

The fixture is `example.main.projection_rows`, backed by
`projection_sequence()` with no arguments. It emits nullable `n: int32` with
values 0 through 9. It offers `vgi.expression.duckdb.scalar.v1` and accepts
exactly `CAST("n" AS BIGINT)` under the default evaluation context. Requested
result fields, including their metadata, append unchanged. Other expressions
or contexts decline. The Python reference is
`env VGI_PROJECTION_COMMON=1 python -m tests.projection_reference`.

The runner verifies ordinary scan, partial acceptance, complete decline,
unsupported-context decline, complete acceptance, reuse of accepted bind
state, and an independent ordinary rebind. It requires exact result schemas
and rows throughout. Eleven malformed wire requests also verify rejection of
bad expression IPC, counts, names, metadata, owners, offers, contexts and outer
row/schema shape. The runner has 17 cases in total.
Native optimizer replacement and malformed-provider
responses have separate tests; this runner does not claim those properties.

Go's RPC dependency owns decoding of the outer wrapped request. Its row/schema
checks pass this corpus; strict rejection of trailing outer IPC data or extra
outer batches still needs dependency qualification before release. The new Go
handler strictly validates the nested bind and expression records it receives.
