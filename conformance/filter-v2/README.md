# Filter Encoding v2 conformance corpus

This directory is the canonical, language-neutral corpus location for the proposed `vgi.filters.v2` specification.
Every SDK decoder and evaluator will consume the same cases instead of maintaining language-specific interpretations
of the wire contract.

The corpus has two layers:

- deterministic JSON-only structural cases checked against the normative JSON Schema; and
- Arrow IPC runtime cases containing an input batch, the complete Filter v2 batch, optional external key batches,
  and either the expected filtered batch or expected error text.

## Layout

- `filter-v2.schema.json` is the Draft 2020-12 structural schema for snapshot and delta JSON documents.
- `manifest.json` identifies the corpus format and lists cases in stable execution order.
- `cases/positive` and `cases/negative` contain real JSON documents with the expected schema outcome recorded in the
  manifest.
- `runtime-manifest.json` indexes the executable Arrow vectors under `runtime/` and records a SHA-256 digest for every
  file.
- `generate_runtime.py` deterministically regenerates the runtime corpus with DuckDB 1.5.5 reference semantics.
- `run_worker.py` sends the portable subset through the public VGI client to any compatible worker executable. For
  example:

  ```shell
  uv run python conformance/filter-v2/run_worker.py \
    --worker vgi-fixture-worker
  ```

  Retargeting another SDK requires only changing `--worker`; the worker must expose the standard
  `main.filter_echo` auto-filtering fixture used by the VGI integration suite.

Manifest entries should use stable case IDs and cryptographic digests for every referenced file. An incompatible
manifest-layout change increments `manifest_version`; a filter-wire change uses the encoding and protocol versioning
rules in the normative specification.

JSON Schema validation is only the first validation layer. It cannot check Arrow payload references and types,
name-to-index agreement with bind or join-key schemas, expression result types, capability registries, duplicate IDs,
scan-local revision history, evaluation-context metadata, or evaluation semantics. Protocol implementations must
perform those checks in the order required by the normative specification.
