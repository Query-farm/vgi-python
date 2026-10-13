# Shared catalog values and column paths

These definitions are implemented on the DuckDB 2.0 preparation branch in the
normal SDK modules and generator inventories. They are part of the combined
successor contract. The published protocol is still `vgi.v2` / `2.1.0`; the
unpublished development snapshot now carries schema options and capability
negotiation. See the operation contract below.
Completing these codecs does not complete the release gates in the
[combined manifest](duckdb-2.0-manifest.json).

## Typed option changes and readback

Schema, table, and index options use the same literal-value representation.
An option name identifies a column in a `TypedValueBatch`: one Arrow record
batch with exactly one row, retaining the complete schema and field metadata.
Option names are canonical names declared by the provider. The wire layer
performs no case folding, type inference, casts, or SQL expression evaluation.

`OptionChanges` has these fields, in order:

| Field | Arrow type | Meaning |
| --- | --- | --- |
| `set_values` | nullable `binary` | IPC stream containing a `TypedValueBatch` of the options to SET. NULL means no SET operations. |
| `reset_names` | non-null `list<utf8>` | Names whose stored overrides are removed. Empty means no RESET operations. |

The inner batch must have unique, nonempty, non-NUL field names and exactly one
row. More than one IPC batch, a missing batch, and trailing bytes are invalid.
An empty value set still has one row and zero columns. Types, nullability, and
metadata are validated against the provider's declared option schema, including
nested field metadata. Each supplied field must match its declaration exactly;
the selected fields can appear in any order. Schema metadata must also match.

| Request | Result |
| --- | --- |
| A field contains a non-NULL literal | Store that typed value as an explicit override. |
| A field contains a typed NULL | Store an explicit NULL override, if the declared type permits NULL. |
| A name appears in `reset_names` | Remove its override and restore the provider default resolved for this mutation. |
| A name appears nowhere | Leave its value and override status unchanged. |
| A name appears in SET and RESET, RESET repeats a name, or the name is unknown | Reject the entire change before mutation. |

`OptionState` provides readback:

| Field | Arrow type | Meaning |
| --- | --- | --- |
| `values` | non-null `binary` | IPC stream containing the effective value of every exposed option. |
| `explicit_names` | non-null `list<utf8>` | Names with stored overrides, including explicit NULL and values equal to the default. |

Every explicit name must exist in `values`, with no duplicates. Readback is a
snapshot: it must be produced using the same visibility rules as the owning
catalog object. RESET uses defaults from the mutation's transaction/snapshot,
not defaults remembered by the client at attachment time. Resetting a known
option without an override is valid. Provider policy can restrict which known
options are mutable.

`OptionChanges.apply_to(current, defaults=..., mutable_names=...)` is the Python
reference implementation. It validates the entire candidate and returns a new
`OptionState`; it performs no persistence. A provider must validate additional
range, dependency, authorization, and operation-specific constraints before
committing it. The helper alone does not establish durable atomicity, DDL
rollback, or replay safety. A SQL expression string is a string literal here;
expression-valued options require an explicitly supported expression contract.

```python
import pyarrow as pa
from vgi.catalog.options import OptionChanges, OptionState
from vgi.typed_values import TypedValueBatch

schema = pa.schema([pa.field("label", pa.string())])
defaults = TypedValueBatch(pa.RecordBatch.from_pylist([{"label": "default"}], schema=schema))
current = OptionState(values=defaults)
null_value = TypedValueBatch(pa.RecordBatch.from_pylist([{"label": None}], schema=schema))
changed = OptionChanges(set_values=null_value).apply_to(current, defaults=defaults)
assert changed.explicit_names == ["label"]
assert changed.values.batch.column("label")[0].as_py() is None
reset = OptionChanges(reset_names=["label"]).apply_to(changed, defaults=defaults)
assert reset.explicit_names == []
assert reset.values.batch.column("label")[0].as_py() == "default"
```

Schema option definitions, RPC integration and a transactional reference are
implemented below. Broader option-bearing objects, dependency effects and
shipping adapter qualification remain required before the combined freeze.

## Raw column paths

`ColumnPath` is an ordered list of nonempty, non-NUL strings. The first component
is a top-level column, and subsequent components identify nested fields. Dots,
quotes, reserved words, and Unicode characters are literal components. There is
no dotted SQL parsing or case conversion on the wire. An engine resolves SQL
identifiers against catalog metadata and sends the canonical field spelling.

`column_path_key` returns the exact component tuple for identity comparisons.
`resolve_column_path` checks exact names against an Arrow schema and descends
through struct fields. Missing or ambiguous names and non-struct traversal
reject. List-element, map-key, and union-arm traversal is not implicit.
Engine-specific identifier equivalence remains an adapter responsibility.

`RequiredFilterPaths.groups` is a non-null
`list<list<list<utf8>>>`: AND groups, OR alternatives within each group, then
the raw components of each alternative. An empty outer list imposes no
requirements; empty groups and empty paths reject. Duplicate alternatives in
a group reject. Every alternative must resolve against the table schema.

```python
from vgi.column_path import RequiredFilterPaths

required = RequiredFilterPaths(groups=[[["a", "b"], ["other"]], [["a.b"]]])
assert required.is_satisfied_by([["a"], ["a.b"]])
assert not required.is_satisfied_by([["a.b"]])
```

Each group requires at least one alternative to have a present filter path as
a component prefix. A filter on `['a']` satisfies `['a', 'b']`; a filter on
`['a.b']` does not. A child filter does not satisfy a requirement for its parent.
This preserves the existing presence-policy semantics. It does not prove
predicate selectivity, authorization, or remote enforcement. The adapter must
still enforce the independently negotiated filter semantics.

`RequiredFilterPaths.from_legacy(groups, schema=...)` is an explicit migration
helper for the existing dotted `TableInfo.required_filters`. It enumerates
schema paths and accepts a legacy spelling only if it identifies exactly one
path. When both a literal column `a.b` and struct field `a.b` exist, migration
fails and requires the catalog author to choose the intended components. It is
never an automatic fallback when decoding the successor representation.

Replacing the existing dotted metadata and flat column-operation parameters
still requires the combined version decision and engine integration. Existing
2.1 clients retain their current contract until that transition.

## Cross-SDK evidence

`SHARED_CONTRACT_TYPES` in `vgi/codegen/_common.py` drives schema generation and
the Go, Rust, TypeScript, Java, and C# typed-model inventories. Java's generator
supports recursive lists using its existing RPC codec, validating nullable
`item` fields at every list level. C++ receives schema factories.

`vgi.codegen.catalog_contract_fixtures` generates
`conformance/catalog-contracts.json` into every worker SDK. Its populated and
empty records exercise typed NULL payloads, schema and nested field metadata,
explicit overrides, literal dotted names, quotes, and three list levels. Each
SDK decodes Python-written IPC through its real codec and encodes it again;
Go exercises its codec through an HTTP echo service. TypeScript checks both
Arrow implementations. The tests compare opaque value payloads byte-for-byte;
they do not claim those SDKs can interpret every logical-type annotation yet.

Regenerate and require every checkout with
`scripts/regen_generated.py --require-all`, then check drift with
`scripts/regen_generated.py --check --require-all`. The protocol release still
requires the complete manifest, semantic implementations, all operation and
unsupported-path tests, and engine/transport qualification.

## Attachment negotiation and schema-option operations

The development contract now carries an optional `CatalogCapabilities` record
inside `ClientCapabilities.catalog`. Its four non-null string lists are `read`,
`ddl`, `write`, and `transaction`. Each name is a case-sensitive, explicitly
versioned profile. Missing offers select no new profiles. Unknown offered
profiles/versions are ignored, never downgraded to another version.

`CatalogAttachResult.catalog_capabilities` carries the provider's advertisement;
`selected_capabilities` carries the worker's intersection of the offer, current
provider support, and profiles understood by the worker. Selection is sorted
and excludes profiles whose prerequisites were not selected. Clients must use
the selected profiles, not infer permission from the advertisement or the RPC
method inventory. Capabilities do not replace authorization.

The initial implemented profiles are:

| Category | Profile | Contract |
| --- | --- | --- |
| read | `vgi.schema.options.read.v1` | Discover typed definitions and read effective values and explicit overrides. |
| ddl | `vgi.schema.options.create.v1` | Supply literal schema creation options; requires the read profile. |
| ddl | `vgi.schema.options.alter.v1` | Atomically SET/RESET schema options; requires the read profile. |
| transaction | `vgi.transaction.ddl.v1` | Advertised DDL participates in begin/commit/rollback and transaction-local reads. Does not imply mixed DDL/DML or cross-catalog atomicity. |

Python seals the selection inside the attachment alongside its UUID and
provider bytes. MetaWorker routing and authenticated HTTP reconnects retain it;
providers and storage still see their original bytes/UUID. Sealed attachments
remain bound to the principal. Local transports without sealing have the same
trust boundary as their existing opaque attachment values. Selected profiles
are immutable for an attachment: changing the offer requires a new attachment.
A provider downgrade is checked at each new operation. A missing/stale profile
raises `UNIMPLEMENTED` / `catalog_capability_required` before calling the
provider's mutation hook. Transactional option mutations additionally require
the DDL transaction profile. Providers must validate any changing object-level
requirements atomically with their effects; this profile check does not yet
implement trigger or secure-view requirements.

Schema options are omitted from metadata returned to attachments without the
read profile. Capability-dependent catalog snapshots bypass the shared
attachment-independent cache, and their etags hash the effective agreement and
projected contents. An etag from a different capability projection cannot
suppress the required readback. The framework's existing authentication check
still runs before cache access.

`OptionDefinitions` contains `schema` (serialized Arrow schema), `defaults`
(`TypedValueBatch` IPC containing a subset of the declared fields), and explicit
`create_names` and `alter_names` allowlists. Names without defaults are required
at creation, including nullable options, and cannot be RESET. Read-only options
must have provider defaults. Empty allowlists grant no mutation rights. Every
field and all metadata in the defaults and caller values must match the declared
schema. Helpers `create()` and `apply()` construct a complete candidate before
any provider persistence. Defaults are resolved again in the mutation's
transaction, so discovery does not reserve a default value.

| Method/record | Development change |
| --- | --- |
| `catalog_schema_option_definitions` | Attach, raw schema path, optional transaction; returns `OptionDefinitions` for an existing or proposed schema path. Requires the read profile. |
| `catalog_schema_create` | Adds nullable `options` binary containing `TypedValueBatch` IPC. Non-NULL requires the create profile, including an explicitly empty batch. NULL retains baseline creation behavior. |
| `catalog_schema_alter_options` | Attach, raw schema path, non-null `changes` binary containing `OptionChanges` IPC, optional transaction; returns no result. Requires the alter profile. |
| `SchemaInfo.options` | Nullable binary containing `OptionState` IPC. Read back through get, list, or full catalog contents. NULL means unavailable, not an empty option set. |

Flat RPC arguments carry IPC bytes explicitly; Python's public client accepts
the typed helper objects and serializes them. The provider hooks receive typed
objects. Readback and catalog versions follow the existing transaction visibility
rules. A lost mutation/commit response does not grant permission to replay it.
The wire profile does not invent automatic retries or exactly-once execution.

`tests/catalog/schema_options_reference.py` supplies a durable SQLite reference
with persisted transaction snapshots and conflict detection. Public HTTP tests
cover typed NULL, RESET, isolation, rollback, commit on a new worker, and
idempotent completion of that reference's transaction identifiers. Other tests
cover unknown/absent offers, provider downgrade, mandatory transaction profiles,
MetaWorker routing, malformed/tampered tokens, cache projection, and atomic
rejection of a mixed valid/invalid change. This evidence qualifies the Python
reference behavior; it is not native DuckDB 2.0 qualification.

Other SDKs carry the new schemas/codecs and retain conservative absent
agreements. Their current mutation handlers must reject a non-NULL options
payload before handing an unsupported operation to a provider. Native DuckDB
1.5 continues to offer no new catalog profiles. The combined public protocol
version remains undecided until the remaining contract changes are reviewed;
this development wire snapshot must not be published as a stable 2.1 release.
Schema comment/tag alterations, object requirements, broader transaction
semantics, and the other catalog object families remain in the combined plan.
