# Filter Encoding v2 conformance corpus

This directory is the canonical, language-neutral corpus location for the proposed `vgi.filters.v2` specification.
Every SDK decoder and evaluator will consume the same cases instead of maintaining language-specific interpretations
of the wire contract.

The initial corpus contains deterministic JSON-only structural cases. It intentionally contains no Arrow or IPC
binaries yet: those vectors must be generated from reviewed normative cases and checked against the reference
evaluator.

## Layout

- `filter-v2.schema.json` is the Draft 2020-12 structural schema for snapshot and delta JSON documents.
- `manifest.json` identifies the corpus format and lists cases in stable execution order.
- `cases/positive` and `cases/negative` contain real JSON documents with the expected schema outcome recorded in the
  manifest.
- Future case directories will contain their Arrow IPC inputs, filter document, optional join-key/artifact payloads,
  and machine-readable expected result or error.

Manifest entries should use stable case IDs and cryptographic digests for every referenced file. An incompatible
manifest-layout change increments `manifest_version`; a filter-wire change uses the encoding and protocol versioning
rules in the normative specification.

JSON Schema validation is only the first validation layer. It cannot check Arrow payload references and types,
name-to-index agreement with bind or join-key schemas, expression result types, capability registries, duplicate IDs,
scan-local revision history, evaluation-context metadata, or evaluation semantics. Protocol implementations must
perform those checks in the order required by the normative specification.
