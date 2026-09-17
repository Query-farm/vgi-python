# VGI protocol

These pages are the canonical protocol 2.0 design and filter documents maintained with `vgi-python`.
Each document declares its own status; “proposed” means the contract is still subject to revision before release.

| Document | Classification | Purpose |
|---|---|---|
| [VGI Filter Encoding v2](vgi-filter-encoding-v2-spec.md) | **Normative, proposed** | Engine-neutral wire format, semantics, validation, and conformance requirements for `vgi.filters.v2`. |
| [VGI Runtime-Filter Artifacts](vgi-runtime-filter-artifacts.md) | **Normative optional extension, proposed** | Additional requirements for capability-gated advisory runtime-pruning artifacts. Base filter-v2 conformance does not require this extension. |
| [VGI DuckDB Filter Adapter](vgi-duckdb-filter-adapter.md) | **Informative implementation guide, proposed** | DuckDB 1.5 and 2.0 mappings into the normative filter-v2 wire contract. |
| [VGI catalog query pushdown](vgi-catalog-query-pushdown-design.md) | **Informative implementation design, proposed** | Optional read-query preparation and streaming, Python provider API, DuckDB 1.5 explicit execution, and DuckDB 2.0 automatic pushdown. |
| [Proposed VGI protocol changes for DuckDB 2.0](vgi-protocol-proposed-changes.md) | **Informative design/audit report** | Rationale, migration inventory, implementation status, and deferred protocol decisions. It is not an implementation specification. |

## Reading order

Protocol implementers should begin with the normative [Filter Encoding v2](vgi-filter-encoding-v2-spec.md). Read the
runtime-artifact extension only when implementing one of its negotiated algorithms. Engine adapters should then use
the relevant implementation guide without treating engine-private classes as wire types.

The [catalog query pushdown design](vgi-catalog-query-pushdown-design.md) describes a separate optional
capability for executing whole eligible read queries at a catalog provider. It is a proposal, not
an implemented extension of the filter-v2 contract.

The language-neutral filter corpus at `conformance/filter-v2/` contains structural JSON cases and executable Arrow
IPC vectors. Its worker runner sends the portable cases through the public VGI client and can target any SDK's
standard example worker.

The [filter-pushdown user guide](../filter-pushdown.md) explains how Python function authors opt in. User-facing APIs
and implementation status belong there; wire-level requirements belong in the normative specification.
