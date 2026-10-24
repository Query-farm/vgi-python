# Scalar projection worker proof

Run `python conformance/projection-expressions/run_worker.py --worker COMMAND`
from the Python checkout. An HTTP URL is accepted as well. The runner uses the
typed public bind, proposal and init RPCs against the common example fixture;
it does not simulate optimizer replacement.

The fixture is `example.main.projection_rows`, backed by
`projection_sequence()` with no arguments. It emits nullable `n: int32` with
values 0 through 9 and declares `STABLE_WITHIN_QUERY`. It offers
`vgi.expression.duckdb.scalar.v1` and accepts
exactly `CAST("n" AS BIGINT)` under the default evaluation context. Requested
result fields, including their metadata, append unchanged. Other expressions
or contexts decline. The Python reference is
`env VGI_PROJECTION_COMMON=1 python -m tests.projection_reference`.

The runner verifies ordinary scan, partial acceptance, complete decline,
unsupported-context decline, complete acceptance, reuse of accepted bind
state, and an independent ordinary rebind. It requires exact result schemas
and rows throughout, including preservation of repeatability. Thirteen malformed wire requests also verify rejection of
bad expression IPC, counts, names, metadata, owners, offers, contexts and outer
row/schema shape and null/unknown repeatability. The runner has 19 cases in total.
Native optimizer replacement and malformed-provider
responses have separate tests; this runner does not claim those properties.

Add `--wrapped-framing` to qualify raw outer IPC handling: valid physical EOF
and legacy EOS, extra batches, trailing bytes, repeated EOS, truncated EOS/body
and requests exceeding 8 MiB. This adds eight cases, for 27 total. The new Go
RPC dependency fix must pass this mode over subprocess and HTTP before release;
the dependency's unit tests also check rejection before Arrow decoding. These
new cases are authored but await remote runtime qualification.
