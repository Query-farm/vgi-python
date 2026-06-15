---
description: "How VGI works: the worker process model, transports, the Apache Arrow data model, the call lifecycle, and parallel workers."
---

# Concepts

**What this is:** the mental model behind VGI — enough to design workers correctly and debug them
confidently. **Who it's for:** developers who want the "why," not just the "how." None of this is
required to ship your first worker; do the [tutorial](../tutorial/index.md) for that.

## The worker process model

A VGI worker is an ordinary process — not code loaded into DuckDB. DuckDB launches your worker
(the `LOCATION` in `ATTACH`) and exchanges **Apache Arrow** record batches with it over a
transport. Because your code runs in its own process, it can use any Python library, can't crash
the database, and isn't tied to DuckDB's ABI or release cycle.

## Transports

The same worker runs over two transports without code changes:

- **Subprocess** (default) — DuckDB (or the Python `Client`) spawns the worker and talks to it over
  stdin/stdout. Best for local/co-located use; the simplest path.
- **HTTP** — the worker runs as a network service (`vgi-serve … --http`). Adds authentication,
  externalized payloads, and stateless stream resume. See
  [Serve over HTTP with auth](../how-to/http-auth.md).

Per-call code can branch on the transport, but most workers never need to.

## The Arrow data model

Functions receive and return **columns**, not rows. A scalar function's `compute` gets a
`pa.StringArray`/`pa.Int64Array` (a whole column) and returns one the same length; table functions
emit `pa.RecordBatch`es. Operating on columns with `pyarrow.compute` is what keeps data transfer
and processing fast — there's no per-row Python call overhead, and the columnar bytes move without
re-serialization. Argument and result types are declared with `Annotated[...]`, and VGI derives the
Arrow schema (and thus the SQL signature) from them — see
[Argument Serialization](../argument-serialization.md).

## The call lifecycle

Every call flows through a small set of phases:

- **bind** — declare the output schema from the arguments (and, for table-in-out, the input schema).
- **init** — set up per-call state (`initial_state`).
- **process** — called one or more times to produce output; a generator emits batches until
  `out.finish()`.
- **finalize** — (aggregates / table-in-out) emit final results after all input is seen.

Scalar functions use a simplified version (no `finalize`): each input batch maps to one output
batch until the input ends. The exact phase ordering, including the distributed/multi-worker case,
is diagrammed in the [Function Lifecycle reference](../lifecycle.md).

## Parallel workers

For throughput, the client can run **several worker processes** and distribute input batches across
them round-robin, collecting results. Aggregates merge partial state across workers via their
`combine` phase, which is why generator and aggregate state must be serializable (see
[Persist state across workers](../how-to/state-storage.md)).

## Next steps

- **Phase-by-phase detail** → [Function Lifecycle](../lifecycle.md).
- **How types cross the wire** → [Argument Serialization](../argument-serialization.md).
- **Apply it** → [How-to guides](../how-to/index.md).
