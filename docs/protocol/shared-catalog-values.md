# Shared catalog values and column paths

These definitions are implemented on the DuckDB 2.0 preparation branch in the
normal SDK modules and generator inventories. They are part of the combined
successor contract. The published protocol is still `vgi.v2` / `2.1.0`; the
unpublished development snapshot now carries typed schema/table options,
layouts, raw column mutation paths, and capability negotiation. See the
operation contracts below.
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

The unpublished successor uses `column_path: list<utf8>` in the nine existing
column mutation RPCs: add, drop, rename, type change, default set/drop,
nullability set/drop, and comment set. ADD and type change also carry an explicit
path before `column_definition`. That IPC schema must contain exactly one field,
whose name equals the last path component, with no schema metadata or trailing
payload. A type-change definition carries the new type and its logical-type
annotations; column defaults, comments, tags, compression, and nullability must
be changed through their own contracts rather than silently discarded.

`["a.b"]` identifies one literal top-level column; `["a", "b"]` identifies a
field in a struct. A rename's `new_column_name` is one raw component and leaves
the owning path and field position unchanged. ADD appends within its immediate
parent. Nested edits preserve parent metadata and NULL parent structs; they do
not materialize a non-NULL struct merely to hold a new defaulted field. The
reference rejects removing the last field of a table or struct, invalid paths,
failed casts, invalid defaults, and nullability violations before publishing any
metadata, data, or version change. Metadata-only edits preserve existing
physical partitions, including after a future-writes-only layout change.

Each nested operation requires its selected attachment profile:
`vgi.column.nested.{add,drop,rename,type,default,nullability,comment}.v1`.
The worker rechecks current provider support before dispatch. Transactional
nested DDL also requires `vgi.transaction.ddl.v1`. One-component paths keep
baseline flat DDL behavior; the pinned 1.5 adapter emits one component without
splitting dots. Flat-only SDK providers reject deeper paths. The CLI continues
to accept a literal column name and encodes it as one component.

These changed parameter types and required targets make the current combined
diff breaking; retaining them requires a major version and corresponding
routing transition. Existing 2.1 deployments retain their published contract.
No duplicate field-add/drop/rename RPC family is needed. Shared expression
profiles for DEFAULT/USING, generated dependencies, full column metadata,
cross-object CASCADE, and the `TableInfo.required_filters` migration remain
part of the combined contract work. The limited reference is not evidence that
those remaining semantics or every logical type are qualified.

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

The corpus also contains 18 column mutation requests: all nine operations with
both literal dotted targets and nested paths. SDK tests preserve the definition,
transaction, nullable comment/expression, rename component, and boolean flags.
TypeScript's public client-to-handler checks also use the raw RPC list-scalar
representation, which can be an Arrow vector rather than a JavaScript array.

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

## Table options, creation metadata, and layout

The unpublished table increment adds nullable `comment`, `tags`, `options`, and
`layout` fields at the end of `TableCreateRequest`. NULL means no new request;
an empty map, empty typed batch, or empty layout is still an explicit request
and requires its selected capability. Existing conflict and constraint fields
retain their meanings. A provider must validate all supplied metadata before
creating or replacing a table.

`TableInfo` appends nullable `options` (`OptionState` IPC) and `layout`
(`TableLayout` IPC). Get, schema listing, and bulk catalog contents project these
fields according to the sealed attachment agreement. Layout metadata is omitted
when any expression profile it requires is unavailable. Projection contributes
to the response etag, so clients with different profiles cannot share an
incompatible cached representation. Layout describes storage policy; it never
promises the order returned by a scan.

| Operation | Required selected profile |
| --- | --- |
| Option definitions and option readback | `vgi.table.options.read.v1` |
| Explicit creation options | `vgi.table.options.create.v1` |
| Table option SET/RESET | `vgi.table.options.alter.v1` |
| Layout readback | `vgi.table.layout.read.v1` |
| Explicit creation layout | `vgi.table.layout.create.v1` |
| Change layout for subsequent writes | `vgi.table.layout.alter_future.v1` |
| Rewrite existing data to a replacement layout | `vgi.table.layout.alter_rewrite.v1` |
| Explicit creation comment or tags | `vgi.table.metadata.create.v1` |

Option and layout mutation profiles require their respective read profiles.
New operations inside a transaction additionally require the selected DDL
transaction profile. Current provider support is checked again before effects.
Absent capability means rejection, including when the request is empty.

The new flat RPCs are `catalog_table_option_definitions`,
`catalog_table_alter_options`, and `catalog_table_alter_layout`. All target an
attachment, raw `schema_path`, table `name`, and optional transaction handle.
Definitions may describe an existing or proposed table. Option alteration takes
`OptionChanges` IPC; layout alteration takes `TableLayoutChange` IPC. Creation
options are a `TypedValueBatch`, using the same validation and RESET rules as
schema options. The Python client accepts typed objects and serializes these
flat binary parameters explicitly.

### Expression and layout records

`SqlExpression` carries nonempty `sql`, a versioned semantic `profile`, and a raw,
attachment-relative owner `schema_path`. The receiving provider binds the SQL
against the target table. No bound expression, private AST, caller catalog alias,
or session search path crosses the wire. The owner must resolve to the target's
schema. Profiles must define grammar, functions, types, NULLs, determinism, and
errors before publication; accepting arbitrary SQL is not implied.

`SortKey` contains `expression` (a `SqlExpression` IPC stream), explicit
`direction`, and explicit `null_order`. Session defaults must be resolved by the
caller. On the Arrow wire, enum dictionaries carry member **names**:
`ASC`/`DESC` and `FIRST`/`LAST`, not Python's lowercase enum values.

`TableLayout` contains ordered, non-null `partition_by` and `sorted_by` lists of
binary IPC records (`SqlExpression` and `SortKey` respectively), plus `coverage`.
Empty lists mean no corresponding policy. `coverage` is `ALL_ROWS` or
`FUTURE_WRITES`; new empty tables require `ALL_ROWS`.

`TableLayoutChange` has a mandatory `mode` (`FUTURE_WRITES` or `REWRITE`) and
nullable replacement lists `partition_by` and `sorted_by`. NULL leaves that
component unchanged; an empty list clears it. Both NULL is invalid. The complete
candidate includes retained keys: their profiles must remain selected and
currently supported too. Providers revalidate the entire candidate under the
mutation's snapshot to handle concurrent changes.

`FUTURE_WRITES` leaves existing data untouched and sets coverage accordingly.
`REWRITE` succeeds only after existing data conforms to the complete replacement
layout and sets coverage to `ALL_ROWS`. Validation, expression evaluation, or
rewrite failure must leave metadata, data, and the visible catalog version
unchanged. A provider unable to perform the requested mode rejects it; neither
client nor server may silently change modes or retry a mutation automatically.
This contract does not provide mixed DDL/DML or cross-catalog atomicity.

### Reference behavior and qualification limits

`tests/catalog/table_layout_reference.py` persists schema/table metadata and
physical partition groups in SQLite transaction snapshots. Its pinned DuckDB
1.5 evaluator binds expressions and actually partitions and sorts fixture rows.
Tests exercise durable readback, nested owner identity, option RESET and typed
NULL, future-write preservation, rewrite commit/rollback, capability withdrawal,
and atomic rejection of a failing rewrite. Fixture row seeding is not a public
VGI data-write capability.

The candidate `vgi.expression.duckdb.scalar.v1` reference currently accepts only
column references, constants, `+`, `-`, `*`, `%`, `abs`, `lower`, `upper`, and
`coalesce`, subject to DuckDB binding and type checks. It rejects subqueries,
extra statements, qualified/custom functions, and other expression classes.
The parser is used locally for validation only. Existing tests establish selected
integer/NULL cases, not the complete logical-type or DuckDB 2.0 semantic matrix;
that profile remains subject to the combined P1 review.

All SDKs receive the records and shared Python-written IPC corpus, including
NULL versus empty lists, explicit ordering, nested binary values, and creation
metadata. SDK workers without these profiles reject new creation payloads before
mutation and register unsupported RPCs with structured UNIMPLEMENTED errors.
The native 1.5 adapter sends NULL for the four new creation fields and continues
to advertise no new catalog profiles. SQL enablement, production provider
implementations, and the native 2.0 adapter remain separate unfinished work.
