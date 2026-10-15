# Shared catalog values and column paths

The operation sections through prepared read queries describe implemented
records and bounded reference behavior on the DuckDB 2.0 preparation branch.
The combined-design section distinguishes implemented SQL/constraint records
and reference proofs from the remaining public API and execution work. Both are
part of one combined successor contract. The published protocol is still `vgi.v2` / `2.1.0`; the
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
helper for the formerly dotted `TableInfo.required_filters`. It enumerates
schema paths and accepts a legacy spelling only if it identifies exactly one
path. When both a literal column `a.b` and struct field `a.b` exist, migration
fails and requires the catalog author to choose the intended components. It is
never an automatic fallback when decoding the successor representation.

The successor `TableInfo.required_filters` carries these groups directly as
non-null `list<list<list<utf8>>>`: AND groups, OR alternatives, raw components.
An explicit empty outer list means no requirements. Missing or NULL metadata,
legacy two-level lists, NULL/empty groups or paths, NULL/empty/NUL components,
duplicate alternatives, and paths missing or ambiguous in the complete table
schema are invalid. A traversal must pass through struct fields; list/map/union
traversal is not implicit. The pinned native 1.5 parser rejects invalid metadata
before scanning instead of silently substituting an empty requirement. Its
`vgi_required_filters` table tag contains the same three-level JSON structure.

The native optimizer retains raw components through table filters and bound
struct-extract chains, using the engine's resolved field indexes for canonical
names. A deep child reference does not additionally register a whole-parent
filter. Diagnostics quote individual components when needed so literal dots and
quotes remain distinguishable from traversal. This is an unpublished breaking
change in the same successor release; it has no automatic legacy wire fallback.
`TableInfo` is nested IPC inside a binary RPC result, so its schema change does
not by itself change the RPC reflection hash. The generated schema inventory,
shared IPC corpus, and coordinated major-version transition remain required.

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
and cross-object CASCADE remain part of the combined contract work. The limited
reference is not evidence that those remaining semantics or every logical type
are qualified.

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
attachment-relative owner `schema_path`, and required `evaluation_context:
QueryEvaluationContext`. The receiving provider binds the SQL
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

## Schema creation metadata

`catalog_schema_create` already carries nullable `comment`, nullable string-map
`tags`, and `on_conflict`. The successor preparation reuses these fields; it
adds no method or field for creation metadata. A NULL comment means absence and
an empty string is a present comment. NULL/omitted tags create an empty map.
Tag keys and values are literal strings, including empty values and dotted keys.
Get, list, and whole-catalog snapshots preserve this metadata; optional
`estimated_object_count` hints may differ between discovery routes.

ERROR leaves an existing schema and its catalog version unchanged. IGNORE also
preserves its complete metadata and version. For replacement of an empty schema,
REPLACE installs the supplied metadata and clears omitted comments/tags. New
schema creation and replacement advance the mutable catalog version. This
checkpoint does not settle replacement of nonempty schemas, descendant schemas,
or cross-object dependencies; their combined lifecycle contract remains open.

All seven bundled mutable `contents_reval` providers run the same public-client
creation/readback/conflict test. It distinguishes a literal dotted component
from a nested path. The durable Python schema-options reference additionally
qualifies metadata together with typed options across HTTP reconnect, committed
replacement, option alteration and rollback. The older Python transactor rejects
schema metadata and REPLACE before executing DDL; Go's older writable provider
rejects unsupported tags and empty comments. These references do not establish
transaction guarantees for every provider.

The Go wire `SchemaInfo.Comment` uses `*string`; static author-facing schema
comment descriptors keep their historical empty-string default. TypeScript's
provider `schemaCreate` receives the existing conflict policy as a final optional
argument, and decodes the Arrow tag map before delegation.

Native DuckDB 1.5 forwards both CreateSchemaInfo metadata fields and distinguishes
NULL from empty comments during either cache-loading path. Because that engine
rejects `COMMENT ON SCHEMA`, the native integration test creates metadata through
DuckDB's parsed CREATE API, then uses SQL database copy and catalog discovery.
Build with `-DBUILD_VGI_CATALOG_TESTS=ON`, set `VGI_TEST_WORKER` to a worker location,
and run `build/release/extension/vgi/vgi_catalog_tests '[catalog-metadata]'`.
The public-client tests are in `tests/catalog/test_view_metadata_cross_sdk.py`
(set `VGI_METADATA_WORKERS` to SDK-name/worker-argv JSON); durable HTTP coverage is
in `tests/catalog/test_schema_metadata_protocol.py`.

At this creation checkpoint, schema metadata mutation was still open; the
following section records its subsequent contract and reference implementation.
Native nested-schema routing and native 2.0 qualification remain open. Neither
checkpoint freezes or releases the protocol.

## Atomic schema metadata mutation

`catalog_schema_alter_metadata(attach_opaque_data, path, changes,
ignore_not_found=false, transaction_opaque_data=NULL)` applies one
`CatalogMetadataChanges` IPC record. The change record is shared so future
object-specific metadata operations can use identical semantics. Its fields are:

| Field | Required wire representation | Meaning |
| --- | --- | --- |
| `set_comment` | non-null bool | False leaves the comment unchanged; true applies `comment`. |
| `comment` | nullable UTF-8 | With `set_comment=true`, NULL clears and `""` stores an empty comment. A non-null value with `set_comment=false` is invalid. |
| `set_tags` | non-null map of UTF-8 to UTF-8 | Set the supplied literal, case-sensitive keys. Empty values and empty keys are valid; map order has no meaning. |
| `remove_tags` | non-null list of UTF-8 | Remove exactly these keys. A missing key is a no-op. Duplicate keys are invalid. |

The IPC payload has exactly one batch, one row and all four fields; additional
batches and trailing bytes reject. Missing fields, wrong types,
NULL non-nullable fields, duplicate SET keys, duplicate removals, and overlap
between SET and REMOVE reject before the provider is called. Tags are metadata
strings, not parsed SQL identifiers; a dotted key remains one key. `path` uses
nonempty raw schema identifier components without NUL. Invalid requests reject
even with `ignore_not_found=true`.

The provider validates the target, authorization and any object restrictions,
then applies the whole patch atomically in the transaction's snapshot. It must
preserve options, child schemas and contained objects. A missing target rejects
unless explicitly ignored; an empty patch does not bypass this existence check.
If the effective metadata is unchanged, versions and etags stay unchanged.
Changes advance the transaction-visible catalog version. Committed changes must
invalidate old catalog snapshots; rollback must preserve committed metadata and
versions. There is no implicit retry after an ambiguous transport failure.

The operation requires selected DDL profile **`vgi.schema.metadata.alter.v1`**.
It needs no additional read profile because schema comments and tags already
have mandatory readback fields. A transaction handle also requires
`vgi.transaction.ddl.v1`. The Python worker checks the immutable attachment
agreement against current provider support before applying the operation;
withdrawn support, unnegotiated support and a transaction owned by another
attachment reject before effects. Negotiation does not grant authorization.

The durable SQLite schema reference implements this profile through the public
HTTP client. It covers readback, literal/nested path isolation, options
preservation, simultaneous comment/tag edits, no-ops, missing targets, malformed
patches, commit, rollback, reconnect, etag invalidation and routed sealed handles.
The seven ordinary SDK fixture catalogs do **not** advertise this new profile;
the public-client test requires their calls to reject with no metadata/version
change. Their generated request inventories and shared codec corpus include the
new method/record. This is P1 reference behavior plus P2 schema preparation,
not qualification of production metadata mutation in every SDK.

`test_metadata_changes.py` checks strict shape and patch semantics. All SDK codec
suites round-trip populated, clear-comment and no-op vectors, including a literal
`__proto__` tag. This exposed and fixed TypeScript's generic map normalizer losing
that key; Arrow-js and Flechette codecs are checked separately. Flechette's
previously recorded stdio transport limitation remains open.

The pinned DuckDB 1.5 engine and refreshed Haybarn 2.0 source both reject SQL
`COMMENT ON SCHEMA` (2.0: `src/parser/peg/transformer/transform_comment.cpp`).
Native registration of this RPC does not enable that grammar or advertise the
new profile. Native SQL wiring, full SDK capability/provider implementations,
nonempty schema replacement/drop dependencies, and the coordinated major
version freeze/release remain open.

## Dependency authority and schema lifecycle

`catalog_dependencies(attach_opaque_data, transaction_opaque_data=NULL)` returns
one `DependencySnapshot` under the selected read profile
`vgi.catalog.dependencies.read.v1`. The provider constructs the complete graph
and its `catalog_version` from one transaction-visible snapshot. A graph is
advisory: a later mutation must resolve and authorize its target and every
cascade member against the authoritative provider state inside the mutation's
transaction. Discovery is neither a reservation nor permission to delete.
When policy prevents complete disclosure, reject this read rather than omit
inaccessible objects or edges. Enforcement still includes those objects.

`CatalogObjectRef` has four fields:

| Field | Meaning |
| --- | --- |
| `kind: string` | `schema`, `table`, `view`, `index`, `scalar_macro`, `table_macro`, `sequence`, `type`, `scalar_function`, `aggregate_function`, `table_function`, `window_function`, or `trigger`. Unknown kinds reject. A kind does not advertise DDL support. |
| `schema_path: list<string>` | Raw owning-schema components. For a schema object, these are its parents; an empty list is valid only for a root schema. |
| `name: string` | The final raw object identifier. Function/macro references name the overload family, not a private engine overload handle. |
| `owner_table: string?` | Required only for a trigger, whose name is unique within its owning table. NULL for all other kinds. |

All identifier components are nonempty strings without NUL. The attachment
supplies catalog identity; references contain no client aliases, pointers or
engine OIDs. Providers resolve input according to their identifier policy and
return canonical spelling consistently throughout the graph. The shared record
preserves spelling, case and component boundaries; it does not split dotted
names or silently fold Unicode. Cross-attachment dependencies are outside this
profile: a provider cannot advertise this complete graph while silently omitting
an external dependency. This does not promise distributed catalog transactions.

`ObjectDependency` carries `dependent` and `subject` as complete
`CatalogObjectRef` IPC records, plus `kind: string`:

- `requires`: dropping the subject is blocked under RESTRICT while a dependent
  would remain. CASCADE includes the dependent. Schema membership and nested
  schemas' parent edges use this policy.
- `owned`: the dependent is automatically included when its owner is dropped,
  including under RESTRICT. It may also be dropped explicitly. Dropping a
  dependent never drops its subject. An object has at most one owner, and
  ownership cycles reject. Indexes/triggers may use this policy only where the
  provider implements its full semantics.

`DependencySnapshot` contains a nonnegative int64 `catalog_version`,
`objects: list<binary>` and `dependencies: list<binary>`. Objects with no edges
are included. All IPC records contain exactly one complete row and one batch;
missing fields, incorrect types/nullability, trailing bytes, duplicate object
identities, dangling endpoints, self edges, duplicate/conflicting edges and
invalid owner graphs reject. List order does not affect graph meaning. Reference
cycles are permitted; CASCADE computes their finite deletion closure.

The drop planner first computes the full closure of owned objects (and all
references for CASCADE), then checks for blocking edges. This prevents an edge's
iteration order from changing RESTRICT behavior. It returns a deletion set, not
an execution order: providers must apply it atomically, authorize all members,
and preserve graph consistency on failure. A provider may impose additional
object-specific restrictions, such as rejecting an internal/default schema.

The optional DDL profile `vgi.schema.lifecycle.v1` qualifies the existing
`catalog_schema_create` and `catalog_schema_drop` RPCs with these guarantees:

- Create resolves the complete raw target and requires existing parents. ERROR
  and IGNORE do not modify existing metadata. REPLACE is an atomic RESTRICT drop
  and creation; it never implicitly cascades, retains descendants, or orphans
  objects. Omitted metadata/options take the new schema's defaults.
- DROP with `cascade=false` rejects a nonempty schema. `cascade=true` removes its
  subtree and dependent objects, including dependents in other schemas of the
  same attachment. Missing targets obey `ignore_not_found`; ignoring a missing
  target changes neither catalog version nor etag.
- Failed validation/authorization/dependency checks leave all objects, metadata
  and versions unchanged. Successful changes advance the transaction-visible
  version once and invalidate committed snapshots only after commit. Rollback
  restores the previous committed graph. A concurrent committed writer cannot
  be overwritten by an older transaction's metadata snapshot.
- Selecting this guarantee requires current provider support at each schema
  mutation. Transaction handles additionally require `vgi.transaction.ddl.v1`.
  Legacy availability of these existing RPCs is retained without claiming this
  profile. Regardless of negotiation, providers must not bypass authorization or
  their dependency enforcement.

At the inspected Haybarn 2 pin, `CreateSchemaInfo::ShouldReplaceOnConflict`,
`DuckCatalog::CreateSchema` and `test/sql/catalog/nested_schema/drop_nested_schema.test`
confirm REPLACE uses a non-cascading drop and nested children block it. No new
REPLACE/CASCADE flag is needed on the schema-create wire request.

The durable SQLite Python reference now implements schema, table and simple-view
dependencies over the public HTTP client, sealed MetaWorker routing and persistent
transactions. It binds only `SELECT * FROM` one table/view (owner-relative or one
explicit schema component); other SQL rejects before creation. It does not
execute view queries or implement table data writes. Ownership/drop-closure
semantics have executable shared-record tests; production index/trigger/macro,
constraint, rename and cross-attachment authority remain unqualified.

All SDKs receive the shared records and the discovery RPC with codec vectors.
Only the reference advertises these new profiles. Ordinary providers reject
unsupported discovery; generated types do not qualify provider enforcement.
The native 1.5 adapter uses transaction-owned schema/object caches for nonempty
worker transaction handles on mutable catalogs. Frozen catalogs promise invariant
metadata and retain shared caches even when transactions scope function state. Committed `catalog_contents` snapshots cannot seed
those caches, and ending the transaction retires entries through the existing
bound-entry retention mechanism. Cross-schema CASCADE invalidates the complete
relevant cache. `make test_dependencies` exercises real native schema lifecycle,
concurrent metadata snapshots, rollback, reconnect and both cache settings.
Full SDK provider coverage, native 2.0 qualification, broader freeze and release
remain open.

## Prepared read queries and typed parameters

This development contract is part of the combined successor release. It does
not reserve a separate minor release. The Python worker/client and finite
reference provider implement this lifecycle. The native DuckDB 1.5 explicit
adapter is opt-in; automatic 2.0 integration and general provider qualification
remain open. Generated SDK records alone do not enable query execution.
`SET vgi_query_enabled=true` before ATTACH offers only explicit read queries;
the default remains false and existing attachments require reattachment.

`ClientCapabilities.query_pushdown` is a nullable inline
`ClientQueryCapabilities` struct. It offers `encodings`, `semantic_profiles`,
the actual `engine_version`, and `max_query_bytes`. Absence selects no query
support. Unknown offers may be ignored. `CatalogAttachResult.query_pushdown`
is a nullable inline `QueryCapabilities` struct containing the selected
`encoding`, `semantic_profiles`, `execution_modes`, `backend_engine`,
`backend_version`, `capability_revision`, `default_evaluation_context`, and
`max_query_bytes`. The selected SQL limit is the minimum of the two limits.
The offer and selection travel inside the authenticated attachment; a later
capability or policy revision requires reattachment before more execution.

The first encoding is `duckdb.sql.v1`. `vgi.remote.readonly.v1` selects
`explicit` provider-dialect execution. `vgi.duckdb.relational.v1` selects
`automatic` execution and requires transaction support plus provider
certification for the actual client engine build. A backend named DuckDB does
not establish semantic equivalence. No current reference advertises automatic
support.

`QueryEvaluationContext` contains `timezone` (default `UTC`), `collation`
(`binary`), `default_order` (`asc`), `default_null_order` (`nulls_last`), and
`preserve_insertion_order` (`true`). Context equality is exact; the provider
must implement or decline the requested context. The context digest is SHA-256
of `vgi.query.context.v1\0`, followed by the encoding, semantic profile, timezone,
collation, order, and null order, each UTF-8 encoded and prefixed by its unsigned
four-byte big-endian length, then one byte (`0`/`1`) for insertion order.
This framing is independent of Arrow IPC serialization details.

### Parameter batches

Parameters use the existing one-row `TypedValueBatch` carrier, with schema
metadata `vgi.query.parameters.v1` set to `named` or `positional`. There is no
parallel JSON type list. Every value retains its Arrow type, nullability, and
recursive field metadata. A typed NULL is a supplied value. Zero parameters
are an explicit one-row, zero-column batch with the mapping metadata.

Named fields contain exact, unique, nonempty placeholder names without a
leading `$`, `:`, `@`, or `?`; numeric-only names are invalid. Positional fields
are exactly `1` through `N`, in order. One repeated SQL placeholder maps to one
field. Names and metadata are not normalized; the provider additionally checks
its dialect's identifier rules. Duplicate metadata keys, omitted values,
multiple rows/batches, trailing IPC data, and unsupported mappings reject.

Preparation receives schema-only IPC. Each execution receives a full one-row
IPC batch with exactly that schema, including field order, names, types,
nullability, schema metadata, and nested metadata. Changes require another
preparation. Values remain bound parameters, never SQL interpolation. A
provider must fix binding to the declared types even when NULL-valued backend
parameters would otherwise produce value-dependent overload resolution.

### Preparation and execution

`catalog_query_prepare(request)` is a unary describe/authorize operation; it
must not execute the result query, including through a LIMIT 0 workaround.
The request fields are:

| Fields | Contract |
| --- | --- |
| `attach_opaque_data`, optional `transaction_opaque_data` | Current authenticated attachment and, when supplied, its transaction. Empty handles reject. |
| `encoding`, `semantic_profile`, `mode`, `capability_revision` | Must match the immutable attachment agreement. |
| `query`, `parameter_schema`, `evaluation_context` | Bounded SQL, schema-only IPC, and inline context. |
| optional `catalog_version`, `catalog_etag` | Metadata snapshot hints; zero and empty etags are preserved rather than treated as absent. |
| `referenced_objects` | Inline `QueryObjectReference` records, each with an inline canonical `CatalogObjectRef` and schema-only `expected_schema`. Explicit mode requires an empty list. |
| optional `expected_output_schema`, `required_ordering` | Exact output contract when supplied; ordering is `unspecified` or `query_order`. |

Automatic mode requires a transaction, metadata version, expected output
schema, and a nonempty table-only object manifest. Raw schema path components
remain attachment-relative. The provider must validate the complete logical
catalog, authorization, and data snapshot, not merely compare metadata versions.

`CatalogQueryPrepareResult` is an exclusive accepted/declined union. A decline
has `status=declined`, a bounded `reason_code`, optional `reason_detail`, and
all acceptance fields NULL. A placement decline occurs before execution;
malformed input, authorization failures and backend errors remain errors.
Acceptance requires a nonempty opaque `prepared_query_token`, output schema,
result ordering, context digest and absolute expiry. Validated catalog
version/etag and row/byte estimates are optional, except automatic acceptance
requires the validated version. Required output schema and ordering must match.

Execution uses TABLE `bind`/`init` with the reserved, schema-less name
`__vgi_catalog_query`, followed by the existing Arrow result stream. Its two
nonnull binary positional arguments are the preparation token and parameter
batch. The SDK owns one executor per Worker, hidden from catalog inventories.
It accepts only the full output projection and one reader (`max_workers=1`);
input batches, splits, secondary initialization, scan pushdown, time travel,
COPY, external settings/secrets, dynamic filters and cache validators reject.
The output schema, including metadata, remains exact throughout execution.

Preparation tokens bind identity, attachment, transaction, capability revision,
SQL, parameter/output schemas and context. Bind resolves them without running
SQL. Each successful primary init starts a new execution; neither an ambiguous
failure nor a lost owner authorizes a retry or local fallback. Continuation
tokens resume an existing execution and must never reconstruct it by rerunning
SQL. The Python client uses `table_scan_continue` for reconnects and rejects the
older rebind-and-discard resumable path for this executor.

`catalog_query_release(attach_opaque_data, prepared_query_token,
transaction_opaque_data=None)` idempotently releases an owner's preparation.
It remains usable for cleanup after expiry or capability withdrawal. Existing
executions have independent handles/deadlines and survive preparation release
or expiry. EOS, errors, cancellation, transaction end, attachment revocation
and execution expiry must release execution resources. Since cancellation is
best effort, providers need a janitor independent of client release calls.

Protocol bounds are SQL 1 MiB, request/result envelopes 8 MiB, parameter batch
8 MiB, schema-only IPC 1 MiB, opaque tokens 64 KiB, 1,024 parameters/referenced
objects, and 32 schema-path components. Recursive parameter metadata validation
is bounded at depth 64. The SDK limits preparation lifetime to one hour and
execution lifetime to 24 hours; providers should choose shorter leases and
enforce resource-count and storage quotas.

The SQLite reference stores immutable preparations and ordered Arrow fragments
in shared storage. It binds only six exact read queries over scalar parameters
and an immutable seven-row relation, with no general SQL sandbox or automatic
equivalence claim. It materializes results during init, uses two-minute leases,
64 resources and a 16 MiB result-storage quota, and exposes an independent sweep
operation for abandoned resources. Public HTTP tests cover typed NULL rebinding,
reconnect to a different worker, cancellation, release, transaction end,
ownership, schema mismatch, lost results, expiry and policy changes. General
provider authorization/snapshot integration, a scheduled production janitor,
native explicit/automatic adapters and performance qualification remain open.


## Combined expression, constraint, transaction and trigger design

**Checkpoint: 2026-10-14. Status: SQL/context and canonical constraint records
generated; bounded public constraint migration verified on pinned 1.5; remaining APIs
specified and not frozen.** The definitions in this
section supersede placeholder records in the manifest and the older trigger
proposal. They belong to the same major transition as the existing development
changes. Engine pins remain unchanged; the public inventory now has 83 methods.
A specified shape does not close P1. The implementation evidence below applies
only to its stated scope; the remaining acceptance cases still require proof.

### SQL and constraint implementation checkpoint

`SqlExpression`, `SqlStatement`, `MacroOverload`'s required context and all six
canonical constraint records are implemented and generated across seven SDKs.
`CatalogObjectRef(kind="constraint")` requires an owning table and preserves
the provider-issued key exactly. Codecs reject malformed or conflicting records;
they do not authorize execution or prove enforcement by a provider.

The durable SQLite-backed constraint reference stores Arrow rows and canonical
records together. It proves create/read/atomic apply, immediate NOT_NULL,
UNIQUE, PRIMARY_KEY, BOOLEAN CHECK and MATCH SIMPLE foreign keys, existing-row
validation, exact referenced-key dependencies, schema nullability, stable keys,
effect-sensitive revisions, rename/recreate, transaction isolation, rollback,
reconnect and failed-publication recovery. DuckDB binds and evaluates SQL rather
than using text matching to simulate checks. Its finite scope is top-level
boolean, signed integer and string columns; only ENFORCED + ALL_ROWS and
NO_ACTION/RESTRICT foreign-key actions execute. Retargeting an existing foreign
key, nested constraints, alternate enforcement modes and broader actions reject.

The shared scalar reference accepts constants, primitive casts, row columns,
arithmetic `+ - * %`, comparisons, AND/OR/NOT, NULL tests and
`abs/lower/upper/coalesce`, under the explicit default evaluation context.
CHECK requires BOOLEAN; TRUE and typed NULL pass. DEFAULT rejects row references.
Generated-column evaluation orders a finite dependency graph and rejects cycles.
Only the existing USING adapter additionally permits its already qualified nested
old-row references and numeric literals. Other contexts, functions and logical
types reject. DEFAULT/generated/USING helpers prove evaluation rules; they do not
establish a new persistent default/generated-column implementation. `SqlStatement`
is a validated carrier only, with no new statement execution permission.

The shared corpus covers all constraint kinds and mutation variants, binary
revisions, nullable fields, raw identities, required contexts and exact expression
IPC preserved inside column metadata across all seven SDKs (both TypeScript
Arrow backends). Arrow metadata is UTF-8: raw IPC there failed Rust decoding, so
the metadata encoding below uses canonical base64. Record fields typed `binary`
continue carrying raw IPC. The native 1.5 adapter rejects these new column
metadata namespaces recursively until their semantics are implemented.

### Public constraint migration checkpoint

The parent table records now use the canonical fields below, and
`catalog_table_constraints_apply` is a public RPC. Python gates creation,
discovery and mutation on selected profiles and current provider support.
Mutation takes the discovered semantic revision; it does not retry an ambiguous
result. Unported SDK providers reject constrained creation and unsupported
mutation before changing state. Read-only providers must explicitly supply
canonical identities and trust states; legacy arrays cannot manufacture those
facts. Generated schemas and registries cover all seven SDKs.

The durable reference exposes public creation, discovery, apply and DDL
transactions. It advertises no public scan/write functions. Its provider-side
row replacement authority continues validating the same persisted constraints.
The native 1.5 subset translates top-level NOT_NULL, UNIQUE, PRIMARY_KEY and
same-schema MATCH SIMPLE NO_ACTION/RESTRICT foreign keys. SET/DROP NOT NULL uses
canonical apply with the discovered revision. CHECK, nested paths, unsupported
trust states and broader FK actions reject explicitly. Native default/generated
metadata translation remains pending.

EC2 validation passed for the Python public lifecycle, all SDK contract/adapter
suites, the bounded native 1.5 adapter and six built-worker discovery lanes.
Java's shared RPC serializer now preserves declared Arrow dictionary encodings
for these embedded records; strict IPC and cross-language tests cover the fix.
SDK fixture declarations are METADATA_ONLY/UNVALIDATED, without an enforcement
or native trusted-materialization claim. Exact counts, skips and logs are in
the manifest's `constraint_public_checkpoint`.

This checkpoint adds one method and changes parent schemas; no version is
published or release gate closed. Generic legacy
write/bind authorization remains provider-owned: preliminary table reads are
not atomic with later operations. Typed indexes, semantic ALTER dependencies,
statement lifecycle, triggers and durable replay remain pending.

### Shared SQL definitions and evaluation phases

Keep `SqlExpression(sql, profile, schema_path)` for exactly one **scalar** SQL
expression and add required `evaluation_context: QueryEvaluationContext`.
Reuse the existing context record and its explicit timezone, collation, order,
NULL order and insertion-order settings. Do not create another settings map or
infer semantics from a backend's name/version. Profiles must specify every
additional relevant engine setting as fixed or unsupported; the context record
is not permission to inherit arbitrary session state.

Add `SqlStatement` with those same four fields for one query/DML statement.
Its selected semantic profile specifies the allowed statement kind. It cannot
contain scripts, transaction control or unrestricted statement execution.
Scalar expressions, table-macro queries and trigger actions have distinct
profiles and bind scopes despite sharing text/context conventions. No private
AST, bound-plan serialization or unversioned engine-to-SQL map crosses the wire.
The context digest used by existing queries is unchanged; reusing the record
for persisted definitions does not make their digests query tokens.

The receiving authority binds the definition in the containing operation's
transaction, under the raw owner `schema_path` and declared row/parameter scope.
The containing request supplies the table/object and target `ColumnPath`.
No client catalog alias, implicit caller search path, cross-attachment target or
temporary dependency is permitted for a persistent definition. Binding resolves
types, casts, permissions and dependencies before effects. A client's claimed
dependency list is neither authoritative nor authorization. Readback preserves
the SQL/profile/context and resolved type without claiming source whitespace
round trips. Renames either rewrite/rebind all affected definitions atomically
without changing meaning, or reject; cache invalidation alone is insufficient.

| Use | Binding and evaluation rule |
| --- | --- |
| Column DEFAULT | Bind at definition time without row references or query parameters. Evaluate for each row requiring a default at INSERT/defaulted UPDATE, not universally at CREATE. Deterministic scalar support does not imply volatile, sequence, subquery, aggregate or window support. |
| ADD COLUMN | Distinguish population of existing rows from the future-write default. Preserve NULL parent structs for nested fields. An engine-expanded ADD/UPDATE/SET DEFAULT is one atomic statement, even if implemented by multiple calls. |
| Generated column | Store generated category, expression and declared/resolved Arrow type. The first profile targets deterministic virtual generation. Bind in dependency order and reject cycles. Stored generation needs its own qualified rewrite profile. |
| CHECK | Bind to the candidate row. FALSE rejects; TRUE or NULL passes. Enforced addition validates existing rows and installs future checks atomically. |
| ALTER TYPE USING | Evaluate once per affected row against the old schema, then apply the specified target-type cast. Validate data, dependent definitions and schema together; never substitute a future DEFAULT for USING. |
| Partition, sort and index key | Deterministic row expressions. Layout retains explicit direction/NULL order and REWRITE/FUTURE_WRITES semantics; an index or physical layout does not imply scan ordering. |
| Macro default | Retain `duckdb.macro.default.v1`: the owner evaluates once at CREATE and discovery returns the resolved typed literal. It is not an invocation-time closure over other parameters. |
| Macro body | Scalar bodies use scalar scope; table bodies use query scope. `MacroOverload` gains required `evaluation_context`; supplied default-expression contexts must match it. Keep overload/parameter identity. |
| Trigger action | One profiled DML statement with explicit transition-relation scope and execution owner. It is not a scalar expression or a general remote SQL execution capability. |

In the combined breaking transition, versioned Arrow field metadata keys
`vgi.column.default.v1` and `vgi.column.generated.v1` carry canonical RFC 4648
standard padded base64 ASCII of single-record IPC `SqlExpression` values;
`vgi.column.generation.v1` carries `virtual` or `stored`. Reject raw binary,
whitespace, noncanonical padding, invalid alphabet and oversized metadata before
decoding; never interpret an alternative encoding by guesswork.
The containing Arrow field remains authoritative for the logical type. Absent
default metadata means no declared default; a SQL expression `NULL` is a
declared default. Removal uses the existing explicit mutation, not an empty
payload. Generated category and expression must occur together. Reject legacy
`default`/generated metadata alongside these authoritative keys rather than
choosing one silently. Exact decoded IPC preservation is covered by the shared
SDK corpus; native translation of existing UTF-8 metadata remains pending.
Existing default/USING mutation payloads will change to `SqlExpression` IPC; no second
column-RPC family is added. CHECK and index definitions use the same record.

The `vgi.expression.duckdb.scalar.v1` reference proves only the finite subset and
roles listed in the implementation checkpoint. It does not qualify broader SQL,
volatile defaults, stored generation, general statements or native translation.

### Canonical constraints and index definitions

Replace the five parallel constraint collections in `TableCreateRequest` and
`TableInfo` in this major transition. There is one authoritative representation:
creation carries `constraint_definitions: list<binary>` of `ConstraintDefinition`;
discovery carries `constraints: list<binary>` of `ConstraintInfo`. Both lists
are required; empty means no constraints. CREATE definitions request
ENFORCED + ALL_ROWS and require the enforcement profile; reject any downgrade.
Metadata-only creation is not implied by this request shape. Native adapters translate engine
logical indices into raw paths at the boundary. They must not transmit physical
indices, synthetic row IDs, backing-index OIDs or both legacy and canonical forms.
An absent read profile cannot be handled by hiding enforced constraints or
returning a false empty list: reject materialization when the client cannot
honor the table's required semantics.

| Record/field | Proposed wire value and invariant |
| --- | --- |
| `ConstraintDefinition.declared_name` | Nullable UTF-8 SQL label. Non-NULL labels are unique within the table under the provider's identifier rules. It is not mutation identity. |
| `kind` | Enum `NOT_NULL`, `UNIQUE`, `PRIMARY_KEY`, `CHECK`, `FOREIGN_KEY`. |
| `column_paths` | Ordered, non-NULL raw `ColumnPath` list. NOT_NULL has one; UNIQUE/PRIMARY/FK have nonempty duplicate-free paths; CHECK has none. |
| `check_expression` | Nullable `SqlExpression` IPC; required exactly for CHECK. |
| `foreign_key` | Nullable `ForeignKeyDefinition` IPC; required exactly for FK. |
| `ForeignKeyDefinition.referenced_table` | `CatalogObjectRef` for a table in the same attachment; no client alias. |
| `referenced_column_paths` | Ordered, duplicate-free paths with the same arity as the local key. |
| `match`, `on_update`, `on_delete` | Explicit enums: SIMPLE/FULL and NO_ACTION/RESTRICT/CASCADE/SET_NULL/SET_DEFAULT. No MATCH PARTIAL or deferred checking is implied. |
| `ConstraintInfo.ref` | Sole identity: `CatalogObjectRef(kind="constraint", schema_path, owner_table, name)`. `name` is a provider-issued persisted logical key, compared exactly and never reused; it is not an engine OID or SQL label. |
| `definition` | Complete canonical `ConstraintDefinition` IPC. |
| `enforcement`, `validation` | ENFORCED/METADATA_ONLY and ALL_ROWS/FUTURE_WRITES/UNVALIDATED. Only ENFORCED + ALL_ROWS may become trusted engine constraint facts. |
| `referenced_constraint` | Nullable canonical constraint ref; required for an FK and NULL otherwise. Provider binds the actual referenced UNIQUE/PRIMARY key at creation. |

Extend `CatalogObjectRef` to accept `constraint` with required `owner_table`,
as it already does for triggers. Changing a declared label preserves the
logical key. Table/schema renames update the owning coordinates and all graph
references atomically. An engine that discarded a SQL label reports NULL;
it cannot invent a preserved user declaration. Providers unable to persist
logical keys cannot select canonical mutation support.

Add one mutation method, `catalog_table_constraints_apply(request)`, replacing
the manifest's provisional separate add/drop methods. Its request contains
attachment, optional transaction, raw table identity, required nonempty
`expected_revision: binary`, and ordered nonempty changes. Read discovery's
`TableInfo.definition_revision` before mutation; a stale revision rejects.
Add this nullable binary field to discovery; it is required nonempty whenever
canonical mutation support is selected. It changes on relevant schema,
constraint, enforcement or dependency changes, including changes through existing
column RPCs, and is never reused across DROP/recreation. No-op mutations retain
it; comment-only changes may change the catalog version without changing this
semantic revision. A change is a strict union:

| Change | Fields and behavior |
| --- | --- |
| ADD | Complete definition; requested enforcement/validation. Allocate identity only when the whole request succeeds. |
| DROP | Canonical constraint ref and explicit CASCADE boolean. Missing identity errors; no silent retargeting by SQL label. |
| RENAME | Ref and nullable replacement declared label; key and semantics remain unchanged. |
| REPLACE | Ref, complete replacement definition and requested enforcement/validation. Retains the key, but revalidates every dependent binding. |

The response is `ConstraintSnapshot(table, definition_revision, constraints)`
from the successful mutation snapshot. All changes validate and publish together;
unsupported kinds/actions reject before mutation. Multiple changes may not
address the same existing key in one request; a newly allocated ADD cannot be
addressed until readback. Existing SET/DROP NOT NULL operations update this same
canonical state and obey the same dependencies. Their native grammar remains
available; they do not maintain a second constraint collection.

ENFORCED + ALL_ROWS requires validation of existing data and enforcement for
all subsequent writes, including COPY, other clients and provider-side writes.
A metadata-only declaration is never an enforcement promise. PRIMARY permits
one primary key and contributes NOT NULL. Dropping it removes a field's NOT NULL
property only when no independent enforced requirement remains. UNIQUE permits
NULL-containing keys under the selected SQL profile. Arrow `nullable=false`
and canonical enforced NOT NULL/PRIMARY semantics must agree in both creation
and discovery. Metadata-only/unvalidated declarations cannot justify narrowing
Arrow nullability. Nested STRUCT paths observe visible values: a NULL parent
hides its children. List indexing/traversal is not implied.

Baseline FK execution is immediate MATCH SIMPLE with NO_ACTION/RESTRICT. Any
NULL local component exempts a SIMPLE row; FULL permits all NULL or no NULL.
FULL and each propagating action require their additional selected profile.
SET_NULL/SET_DEFAULT must still satisfy affected constraints. CASCADE must
atomically apply all effects or fail. Cross-attachment targets reject. Broader
same-attachment cross-schema references can be implemented by providers; the
native pin's narrower same-schema rule still applies. Changing/dropping a
referenced key cannot silently retarget an FK to another available key.

Retain existing index create/get/list/drop RPCs. Replace string expressions and
stringified options in their successor records with an `IndexDefinition`:
`table: CatalogObjectRef`, `algorithm: string`, ordered nonempty `keys`,
`options: TypedValueBatch`, explicit `constraint_type: NONE|UNIQUE|PRIMARY`, and
nullable `associated_constraint: CatalogObjectRef`. Each key contains exactly
one raw column path or `SqlExpression`; implicit ordering/collation is forbidden.
Expression context supplies collation. For a raw column key, the initial profile
admits only explicitly established binary column collation; it rejects a
nonbinary or unresolved column collation. Broader collation support remains
part of the separate candidate review and cannot be inferred from an algorithm
name. Physical key-order options require an algorithm profile. A UNIQUE index does not silently create a logical PRIMARY
constraint. Constraint-associated indexes require an authoritative association;
PRIMARY without one rejects. Add `catalog_index_option_definitions` using the
existing `OptionDefinitions` representation. `IndexInfo` returns effective
options through `OptionState` and the complete resolved definition. Unknown
algorithms, options, types or expressions reject before index construction.

The implemented carrier uses `IndexKey.column_path: list<string>?` and
`IndexKey.expression: binary?`, exactly one present. `IndexDefinition.table`,
`options`, and optional `associated_constraint` are nested IPC records;
`keys: list<binary>` preserves key order. Enum wire values are `NONE`, `UNIQUE`,
and `PRIMARY`. `IndexInfo.definition` and `IndexInfo.options` are required binary
records; the effective values in `OptionState` must equal the definition options,
including Arrow metadata. `IndexCreateRequest` keeps index identity, conflict
mode and attachment/transaction tokens and replaces the legacy expression and
string-option fields with the required definition. Index identity and target
table identity may have different schema paths in the general protocol.

`catalog_index_option_definitions(attach_opaque_data, table, algorithm,
transaction_opaque_data?)` accepts a table-reference IPC record and returns the
existing `OptionDefinitions` record. Algorithm identity is explicit: the bounded
SQLite reference uses `SQLITE.BTREE.V1`, never `ART`. This spelling survives the
pinned DuckDB parser's uppercasing of USING identifiers. The reference creates
actual regular/UNIQUE SQLite indexes and declares an empty option schema. The
native 1.5 adapter supports raw top-level keys in one flat schema with binary
collation; expressions, nested keys, nonempty options, PRIMARY/constraint
associations and CASCADE reject before effects. Richer typed values and keys
have codec coverage without implying provider support.

Profiles use existing `CatalogCapabilities` categories: read
`vgi.constraint.records.read.v1` and `vgi.index.definition.read.v1`; DDL
`vgi.constraint.create.v1`, `vgi.constraint.mutate.v1`,
`vgi.index.definition.create.v1`, `vgi.index.drop.v1`; write
`vgi.constraint.enforce.v1`. DDL profiles require corresponding readback.
Define `vgi.constraint.fk.match_full.v1` and action-specific
`vgi.constraint.fk.{cascade,set_null,set_default}.v1` as additional write
profiles. All accepted requests must satisfy the profile; a provider may reject
unsupported constraint kinds, mutation forms or algorithms before effects.
Constraint read/create/mutate/enforce and index read/create/drop profiles are
implemented. Index create/drop require index readback; REPLACE additionally
requires drop, UNIQUE requires constraint enforcement, and associated-index
mutations require constraint mutation authority. Transactional DDL requires its
existing transaction profile. Additional FK match/action profiles remain pending.

The inspected 2.0 pin supports CREATE-time constraint forms but accepts only
UNIQUE/PRIMARY through general ADD CONSTRAINT; SET/DROP NOT NULL have separate
paths. DROP CONSTRAINT is rejected by its transformer. General provider mutation
support is still included in this protocol design. Native adapters reject forms
the engine cannot express rather than simulate unrelated SQL. Existing native
index codecs alone are not evidence of physical construction; the bounded
reference and native SQL lifecycle checks provide that separate evidence.

### Dependencies for semantic ALTER

`ObjectDependency.kind = requires|owned` controls DROP behavior. Required
`blocks_alter: bool` and nullable, nonempty `subject_column_paths` specify
semantic ALTER protection; NULL protects the whole subject. Harmless comments
or label changes do not count as semantic ALTER. Edge identity is the dependent
ref, nullable dependent column path, and subject ref. Merge subject paths only
within that identity; reject conflicting edge kinds or ALTER flags. A NULL path
set dominates narrower protection. Explicit parent and child paths remain in the
merged set so authoritative schema validation checks every claimed field.
Protecting a parent path also protects its nested descendants, and changing a
parent invalidates dependent child paths. DROP follows dependencies regardless
of `blocks_alter`; clearing the flag cannot permit dangling column references.

Constraint, trigger and index refs participate in the same graph. A constraint
is owned by its table; an FK requires the exact referenced key and referenced
columns. A dedicated backing index is owned by its constraint and the constraint
requires the index. That mixed `requires`/`owned` cycle is legal; ownership cycles
remain invalid. A shared index is independently owned and required by each user.
DROP INDEX RESTRICT cannot disable enforcement. A supported CASCADE or replacement
must authorize and atomically resolve the complete closure, including FKs.

Generated-column dependency ordering uses nullable
`dependent_column_path`. This identifies the column within a dependent table;
it is NULL for object-level dependents. Same-table column edges become legal
only with distinct, valid column targets. Build cycle nodes from full object
and column identity; multiple generated columns in one table do not collapse
into a single self edge. Their column dependency graph must be acyclic.
The Python planner validates column existence against authoritative schemas and
returns separate object and column deletion sets. A generated-column edge cannot
turn a column deletion into deletion of its owning table. Providers must authorize
the complete closure and recheck it in the same transaction that publishes the
change; a client-side graph read is advisory. Whole-object requires cycles and
the backing-index mixed cycle above remain legal, while evaluation cycles
involving a dependent column are invalid. Table-column references traverse
column computations rather than unrelated whole-table lifetime dependencies;
view-column references also traverse the whole view definition. Ownership cycle
checks retain the complete lifetime graph.

The durable metadata reference implements table/column rename, column type/drop,
comments, generated-column chains and simple-view blockers. It derives generated
dependencies by binding canonical row expressions and handles bounded generated
column/view CASCADE atomically. Rename/type changes with dependents reject; SQL
definitions are not rewritten. Supported generated expressions are virtual,
top-level primitive scalar expressions with the default explicit context. This
metadata reference does not convert stored rows or enforce physical generated
values, constraints or indexes. Native 1.5 type conversion and canonical
generated-column materialization remain separately limited. All seven SDK wire
surfaces preserve these fields; unported providers reject dependency discovery.

The added required field is part of the unpublished breaking protocol package.
The top-level method inventory stays at 84, and its schema hash is unchanged:
that hash does not cover the dependency records embedded in binary payloads.
Neither the hash nor generated codecs establish provider semantic support.

Canonical column-expression metadata requires its selected and currently
available expression profile on CREATE, ADD and every table discovery path.
For providers that offer dependency discovery, transactional metadata mutation
also requires selected/current `vgi.transaction.ddl.v1`; omitting dependency
read from the client offer cannot bypass that requirement. Existing providers
without the dependency profile retain their legacy transaction behavior.

### Transaction guarantees and composite writes

Use `CatalogCapabilities.transaction`, not a second `TransactionCapabilities`
boolean record. Retain existing begin/commit/rollback and attachment-bound,
authenticated transaction handles. Specify these additional independent promises:

| Profile | Required guarantee |
| --- | --- |
| `vgi.transaction.ddl.v1` (existing) | Advertised DDL, its metadata/dependencies and transaction-local reads commit or roll back together. |
| `vgi.transaction.dml.v1` | Advertised data mutations share the worker transaction and roll back together. |
| `vgi.transaction.mixed.v1` | DDL plus DML, metadata and dependencies commit atomically; requires DDL and DML profiles. |
| `vgi.transaction.multiple_dml.v1` | All base/trigger operations of an admitted multi-DML statement share one atomic transaction; requires DML and the statement lifecycle below. |
| `vgi.transaction.statement_rollback.v1` | A failed statement restores a provider savepoint while retaining earlier transaction work. Without this profile, failure makes the entire transaction rollback-only. |

A nonempty token or `supports_transactions=true` does not establish any of these
additional promises. Tokens bind principal, attachment and transaction; wrong,
expired, unknown or terminal tokens reject further mutation. Data, schema,
dependency graph and visible catalog versions publish together. Committed state
is unchanged on rollback. No cross-catalog/attachment distributed atomicity is
promised, and sequence allocation or external effects are not silently included.
Reject a definition requiring unqualified external effects before execution.

Preflight the complete expanded statement before its first effect: CTAS,
ADD/UPDATE/SET DEFAULT, follow-up constraints, multi-DML CTEs and engine-owned
trigger actions must not discover missing atomicity after partial mutation.
Once any constituent operation fails, COMMIT cannot publish its partial effects.
Repeated COMMIT of a committed token and repeated ROLLBACK of a rolled-back token
are idempotent within a declared retention window; opposite terminal operations
reject. These stronger completion promises require a provider implementation;
the existing transaction boolean alone is insufficient.

A lost mutation/COMMIT response means unknown outcome. Reconnecting, DETACH or
attempting ROLLBACK does not prove that COMMIT failed. Baseline clients never
replay a possibly dispatched mutation. Audit all generic catalog unary retries:
this checkpoint's source review found that native stale-pool retry remains for
other catalog methods even though query preparation now disables it. Fixing and
testing this is required before advertising the new mutation profiles.

### Trigger definitions and statement ownership

Reuse `CatalogObjectRef(kind="trigger", schema_path, owner_table, name)`.
Persistent definitions are worker-authoritative and survive reconnect/DETACH;
local engine entries are versioned materializations. Temporary status follows
the owning table and its lifetime, never an independent persistent-creation flag.

`TriggerDefinition` contains the ref, `timing: BEFORE|AFTER`, singular
`event: INSERT|UPDATE|DELETE`, `granularity: STATEMENT|ROW`,
`update_columns: list<ColumnPath>`, nullable `new_table_alias`/`old_table_alias`,
`action: SqlStatement`, `execution_owner: ENGINE|BACKEND`, nullable comment and
string tags. `TriggerInfo` adds a nonempty opaque definition revision and complete
resolved dependencies. One definition has one event. Empty UPDATE paths means
any UPDATE; nonempty paths mean columns mentioned in SET, not values that
actually changed. No WHEN clause or arbitrary SQL script is introduced.

Use `catalog_table_triggers`, `catalog_trigger_get`, `catalog_trigger_create`,
`catalog_trigger_drop`, and `catalog_trigger_alter_metadata`. They take existing
attachment/transaction context and raw object identity. CREATE takes a complete
definition plus ERROR/IGNORE/REPLACE; REPLACE atomically changes definition,
dependencies and write requirements. Metadata alteration takes
`CatalogMetadataChanges` plus an expected revision. DROP has explicit
ignore-missing/CASCADE semantics. GET returns optional `TriggerInfo`; listing
returns complete infos, catalog version and table requirements revision from
one authorized snapshot. CREATE/ALTER return canonical readback. Incomplete
permission-filtered listing cannot be used as an executable trigger plan.

The initial `vgi.trigger.duckdb.statement.v1` action profile follows the inspected
pin: one INSERT/UPDATE/DELETE/MERGE action, statement BEFORE/AFTER forms, one
trigger event, and only one-component UPDATE OF paths. A literal identifier
`"a.b"` stays one component. Transition tables are AFTER STATEMENT only, cannot
coexist with UPDATE OF, and respect event applicability (no OLD for INSERT or
NEW for DELETE). INSTEAD OF, BEFORE ROW and UPDATE ROW reject. Row and statement
triggers cannot mix for one table/event. A separate
`vgi.trigger.duckdb.row_after.v1` profile would cover only the pinned AFTER
INSERT/DELETE subset; generated source columns, row transition tables,
unsupported action forms, cascading row triggers, RETURNING and ON CONFLICT
remain rejected. Explicitly reject row UPDATE action bodies, bodies without an
applicable OLD/NEW column reference, row self-writes, and recursive trigger
cycles. Statement INSERT ON CONFLICT DO UPDATE with AFTER NEW transition tables
is also rejected by the pin. Do not silently turn a row trigger into a statement
trigger.

Execution ownership is exclusive for the complete table trigger plan in the
initial profile. ENGINE means the qualified client expands definitions once and
the backend does not also fire them. BACKEND means the provider owns all trigger
execution; clients never install those definitions as executable engine entries.
The initial profile rejects mixed owners rather than leave relative ordering
undefined. Ordering is an explicit provider-resolved list of ref/revision pairs
within each event/timing/granularity phase. The DuckDB profile must match the
pinned engine's case-insensitive alphabetical order and validate that order;
list order is not incidental hash-map order.

Add nullable `write_requirements: WriteRequirements` to `TableInfo` and the
existing write-discovery result (`WriteFunctionResult`, currently an alias of
`ScanFunctionResult`), including inlined results. Non-write discovery uses NULL.
The record carries table identity, nonempty `requirements_revision: binary`,
required profile names, execution owner, per-operation minimum result modes,
and ordered trigger ref/revision lists for each phase. Reuse existing
`count < rows < changes`; ENGINE transition-table/row-image support requires
exact `changes`, including when the caller omitted RETURNING.

Add nullable `catalog_write: CatalogWriteBinding` to `BindRequest`, containing
table identity, operation and expected requirements revision. The worker resolves
that table independently from its registered write function and returns the
validated ownership/revision in existing sealed `bind_opaque_data`; no second
public bind token is needed. Add nullable `write_context: WriteExecutionContext`
to `InitRequest`, containing statement ticket, operation ID, stream ID and
nullable source change-set ID for an action consuming a parent operation's
OLD/NEW relation.
The worker checks the agreement, sealed bind and current transactional
requirements atomically before effects. A cached table, inline function or old
bind is not permission to bypass a newly required profile. Missing writer context
is not grandfathered. Reject incapable writers before mutation, including 1.5
clients facing ENGINE triggers. A backend-owned plan may accept legacy writers
only if it can independently recover and enforce the complete SQL statement
semantics; per-batch exchange execution alone is insufficient.

A TABLE batch, stream EOF, `execution_id`, or transactor exchange is not a SQL
statement boundary. Include these methods and records in the combined contract:

| Method | Proposed request and outcome |
| --- | --- |
| `catalog_write_statement_begin` | Attachment, nonempty transaction, unique statement ID, optional parent statement ticket, complete finite write intents/operation IDs and expected requirement revisions. Return an opaque ticket and canonical pinned plan before any BEFORE action. |
| `catalog_write_statement_complete` | Attachment, transaction, ticket, and terminal receipts for every declared operation/stream and required trigger action. Return a completion receipt only after all base and AFTER effects succeed. |
| `catalog_write_statement_abort` | Same owner/ticket and reason. Restore the statement savepoint if qualified; otherwise mark the whole transaction rollback-only. Repeated abort is idempotent; it cannot undo an already committed transaction. |

A `WriteIntent` identifies a nonempty operation ID, target/event, expected
requirements revision, planned result mode, parent operation where applicable
and phase (BEFORE/base/AFTER). It also contains immutable
`updated_column_paths: list<ColumnPath>`: nonempty for UPDATE, empty for INSERT
or DELETE. These are the SET targets, including assignments leaving values
unchanged; the authority validates them against the bound operation before
selecting UPDATE OF triggers. The canonical pinned plan includes ordered
`predecessor_operation_ids` for every operation, derived by the authority from
BEFORE/base/AFTER and trigger order. Init rejects until all predecessors have
authoritative successful completion records. Merely matching final receipts
cannot justify executing an AFTER action before its base operation. The
predecessor graph must be acyclic. Trigger actions additionally identify their exact
trigger ref and definition revision; base operations have neither. The authority
matches these against the required phase order, rather than treating any write
to the same target as completion of that trigger. The authority validates the full
finite action/dependency plan before BEGIN succeeds. Unknown dynamically added
write targets, unbounded recursion and unqualified parallel plans reject.
Initial execution permits one stream per operation; stream identity is real,
but does not advertise parallel/substream execution. All effects use one provider
transaction. Complete OLD/NEW sets span every input batch, retain duplicates and
handle zero affected rows. A statement trigger still follows the qualified
engine's zero-row behavior. Trigger actions use declared operation identities
and cannot recursively open an unrelated transaction. For the initial ROW
profile, one action operation is the vectorized action over all applicable rows
of its parent operation, not repeated invocations of the same operation ID.
It records the parent's complete change-set identity and preserves multiplicity;
row batches cannot become independently firing statements.

`WriteOperationReceipt` contains the statement ticket, operation ID, stream ID,
requirements revision, nullable trigger ref/revision, nonnegative affected-row
count, nonnegative output-row count, and nullable `change_set_id: binary`.
Trigger ref/revision must occur together and match the pinned intent. A successful
`changes` operation has a nonempty change-set ID, one OLD/NEW pair per affected
row, and output count equal to affected count. Other result modes have no change
set. The ID names the complete transaction-owned image relation already delivered
by that operation; it does not imply an extra fetch API or permission to replay
writes. Actions consuming OLD/NEW include this ID in `WriteExecutionContext`;
the authority checks that it belongs to their completed parent operation.

Deliver exactly one receipt as `vgi.write.operation_receipt.v1` custom metadata
encoded as one complete single-record `WriteOperationReceipt` IPC payload,
on a final zero-row IPC record batch with the agreed output schema, after all
ordinary output batches. No data may follow that batch. Stream EOS alone is not
a completion receipt. The authority durably records completion only after the
operation's effects and full result/change-set production finish; an error
produces no success receipt and aborts the statement or transaction. A zero-row
write must still initialize/finalize its declared operation and emit a receipt;
a client-supplied zero count is not a substitute. Each stream has at most one
terminal receipt; repeated control acknowledgements must match it exactly.
COMPLETE's receipt list must match all authoritative operation records, including
zero-row actions, with no missing or duplicated identities. SDK/native readers
must preserve and inspect this metadata even when they normally skip empty
batches. This transport path needs explicit cross-SDK feasibility tests before
freeze; no new statement capability is inferred from existing stream framing.

Before/base/after phases require authoritative operation completion tracking;
client-supplied receipts alone cannot prove effects occurred. Tickets have
states ACTIVE, COMPLETED and ABORTED. Only ACTIVE can transition to either
terminal state. Repeating the same terminal transition returns the original
outcome; the opposite transition rejects. Aborting a completed historical ticket
must never restore a savepoint and erase later successful statements. COMMIT
rejects while any statement ticket is ACTIVE/incomplete or the transaction is
rollback-only, so omission of COMPLETE cannot publish base effects without AFTER
actions. ROLLBACK terminally aborts remaining ACTIVE tickets.

The tuple `(ticket, operation_id, stream_id)` identifies one stream execution.
The sole stream slot is allocated by `(ticket, operation_id)`; changing the
stream ID cannot allocate another execution for that operation. A
duplicate/reconnected init cannot start it again. Reconnect restores control
and status access only; it does not authorize replay or restart of a consumed
stream without a separately qualified resumability profile. A reconnect resumes
only an existing owner/ticket; it never starts a substitute statement. Cancellation,
expiry, transaction end and DETACH reclaim execution resources and abort or mark
rollback-only as required. They do not drop persistent trigger definitions.
Concurrent trigger/constraint changes either serialize with the statement or
produce a conflict before its effects can commit. The provider must not accept
stale requirements based solely on a caller's expected revision.

Proposed read/DDL profiles are `vgi.trigger.read.v1`,
`vgi.trigger.create.v1`, `vgi.trigger.drop.v1`, `vgi.trigger.metadata.v1`.
SQL/action semantic profiles belong to the read category, as the existing
scalar-expression profile does; they do not independently grant execution.
Proposed write profiles are `vgi.write.statement.v1`,
`vgi.trigger.engine.v1` and `vgi.trigger.backend.v1`; action profiles are additional
requirements. ENGINE requires statement lifecycle, DML atomicity, matching
client execution support and the applicable result mode. Read/create profiles do
not imply execution. Definition changes must also update write requirements
atomically. None of these profiles is currently advertised.

### Durable mutation outcomes and bounded replay

Include an optional durable-replay contract in this combined design, instead of
adding identity fields to every existing mutation method. Ordinary methods keep
their selected successor shapes and the no-automatic-retry rule. A separate
`vgi.mutation.replay.v1` transaction profile permits these two new methods only
when the provider can prove durable admission/effect/outcome tracking:

```text
catalog_mutation_execute(attach_opaque_data, transaction_opaque_data?,
                         operation_id, method, request) -> MutationReceipt
catalog_operation_status(attach_opaque_data, transaction_opaque_data?,
                         operation_id) -> MutationReceipt
```

`request` is the **exact bytes** of one IPC parameters batch for the normal
method, validated against that method's schema before dispatch. Preserve those
bytes for retries; an equivalent re-encoding is not the same request. Inner and
outer attachment/transaction fields must match exactly; a method without a
transaction field requires NULL outside. The outer principal remains the only
principal. Reuse normal authorization, capability checks and mutation handlers;
this wrapper is not a privileged second dispatch route. Authenticate and check
identity conflicts first. For an existing identity, return its retained receipt
without redispatching or reapplying active-transaction/current-definition
preconditions. STATUS likewise accepts the matching completed transaction
identity. Otherwise recovery of a COMMIT receipt or an operation completed
before rollback would be impossible. Only a new admitted operation enters the
normal mutation checks; terminal transactions cannot accept new effects.

The initial allowlist is explicit: transaction begin/commit/rollback;
schema create/drop/alter-options/alter-metadata; table create/drop/rename,
alter-options/alter-layout, comment and column-comment mutations, column
add/drop/rename/default-set/default-drop/type-change/not-null-set/not-null-drop;
view create/drop/rename/comment/column-comment; macro create/drop; index
create/drop; canonical constraint apply; and trigger create/drop/alter-metadata.
These refer to the exact corresponding `catalog_*` methods in the registry.
Unknown/unlisted methods reject. Catalog provisioning, ATTACH/DETACH, query
prepare/execution, discovery, statement lifecycle, stream init/process/finalize
and either wrapper method are excluded. Wrapping one stream init cannot make a
streamed write replayable. A future streaming replay profile needs chunk and
completion identity; it is not advertised by this profile.

`operation_id` is exactly 24 bytes: an 8-byte unsigned big-endian admission
deadline in epoch milliseconds (positive, at most int64 max), followed by 16
cryptographically random bytes. The complete value is immutable. Its identity
is scoped to principal plus attachment across all methods; transaction and
method are conflict-checked attributes, not namespaces permitting reuse.
A new request is admitted only when provider admission time is strictly before
the deadline and the deadline is at most 24 hours ahead. The provider uses a
durable nondecreasing admission-time watermark shared by workers/restarts;
clock rollback cannot reactivate an expired ID. Admission after deadline always
rejects, including a previously known ID; use STATUS to recover its outcome.
The deadline does not cancel an operation already admitted.

| `MutationReceipt` field | Meaning |
| --- | --- |
| `operation_id: binary` | Exact 24-byte identity. |
| `status` | Enum UNKNOWN, IN_PROGRESS, SUCCEEDED, FAILED. |
| `request_digest: binary?` | SHA-256 of the method, transaction presence/value and exact inner request bytes; present for known identities. |
| `result: binary?` | Original successful method-result IPC, NULL for a void method; never a newly executed result. |
| `error: binary?` | Existing structured RPC error encoded as IPC, present exactly for FAILED. |
| `expires_at_epoch_ms: int64?` | Guaranteed terminal receipt retention deadline; present exactly for terminal states. |

Digest bytes are SHA-256 of ASCII `vgi.mutation.request.v1` plus NUL, then
method UTF-8 prefixed by unsigned 64-bit big-endian byte length, one transaction
presence byte (0/1), the transaction bytes prefixed by the same length format
when present, and length-prefixed exact request bytes. Store and compare the
original method/transaction/request as well: hash equality alone is not an
identity-conflict check. UNKNOWN has all four nullable fields NULL; IN_PROGRESS
has only a digest; SUCCEEDED has digest/expiry and optional result but no error;
FAILED has digest/expiry/error and no result. Malformed unions reject.

Admission, duplicate exclusion and durable effect/outcome reconciliation must
share authoritative storage. Concurrent exact duplicates return the same pending
or terminal receipt; differing method/transaction/request bytes reject without
dispatch. Retain unresolved records until reconciled, even past their deadline.
Never evict pending operations to admit new work; reject new admissions at quota.
Keep terminal receipts until at least `max(admission_deadline, terminal_time)`
plus 24 hours. After retention, UNKNOWN does not prove nonexecution. Clients
must not change the ID/deadline or fall back to the original RPC after an
ambiguous response. Status cannot dispatch or resume work as a side effect.

SUCCEEDED inside an existing transaction means the operation completed there,
not that the transaction committed. Its record survives rollback/reconnect so
replay cannot rerun it. Commit/rollback receipts establish transaction outcome.
A transport timeout, cancellation or disconnect alone cannot become FAILED:
uncertain effects remain IN_PROGRESS until authoritative recovery determines
the outcome. A provider without recoverable transactions/journaling cannot
select this profile. No exactly-once promise follows merely from carrying IDs.

This profile's feasibility gate includes concurrent duplicates, conflicting
bytes, crash between effect and receipt, ambiguous COMMIT, rollback after a
successful inner operation, quota, expiry and clock rollback. Its record and
method design is included now; reference implementation and advertisement remain
pending. This decision avoids treating reliable replay as an implicit property
of the existing transaction token or stream execution ID.

### Design acceptance and implementation order

1. Qualify the expression/context carrier and role matrix, including typed NULL,
   generated dependency cycles, old-row USING evaluation and schema metadata
   preservation. Preserve the existing macro-default evaluation distinction.
2. Implement canonical constraint read/create/apply and typed index records in
   a durable reference. Prove real enforcement, existing-data failure rollback,
   FK NULL/action semantics, primary-key nullability and backing-index closure.
3. Implement write-requirement checks and statement transaction state before
   exposing trigger execution. Prove CTAS/expanded ADD and multi-operation
   failures cannot partially commit; include lost replies and stale bind caches.
4. Implement persistent trigger CRUD/readback plus BACKEND and ENGINE reference
   paths independently. Test exclusivity, order, zero/multiple batches, exact
   OLD/NEW, trigger changes after bind, reconnect, cancellation and cleanup.
5. Extend dependency planning to constraints and column/ALTER blockers. Test
   rename/replacement, inaccessible dependents, cross-schema identity and no
   cross-attachment authority assumption.
6. Prove the durable-replay admission/outcome rules independently; never enable
   it as a shortcut around the baseline no-retry rule.
7. Generate the selected records/methods across all seven SDKs, with positive
   reference behavior and safe unsupported paths. Freeze only after these proofs;
   then qualify the native 2.0 adapter separately. No implementation is required
   to advertise every profile, but all must understand the frozen wire inventory.

Source evidence is the pinned catalog field audit and local Haybarn 2.0 checkout
`e1784fa4231072d7152b8aaa20977bdb8ff4ace3`, especially `constraint.hpp`,
`transform_alter.cpp`, `bind_create_table.cpp`, `bind_create.cpp`,
`bind_trigger_expansion.cpp` and `table_catalog_entry.cpp`. Existing implementation
anchors are `vgi/catalog/layout.py`, `query.py`, `dependencies.py`,
`catalog_interface.py`, `vgi/protocol.py`, `vgi/write_results.py`, and the durable
schema/layout/column/query fixtures. Source inspection establishes engine limits;
it does not qualify the proposed runtime contracts.
