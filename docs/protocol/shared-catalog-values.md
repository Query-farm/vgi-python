# Shared catalog values and column paths

The operation sections through prepared read queries describe implemented
records and bounded reference behavior on the DuckDB 2.0 preparation branch.
The combined-design section distinguishes implemented SQL/constraint records
and reference proofs from the remaining public API and execution work. Both are
part of one combined successor contract. Published main now uses `vgi.v2` / `2.2.0`. The
unpublished successor temporarily uses the same development labels but contains incompatible
changes; its final major version and routing name remain unresolved. The development snapshot
carries typed schema/table options,
layouts, raw column mutation paths, and capability negotiation. See the
operation contracts below.
Completing these codecs does not complete the release gates in the
[combined manifest](duckdb-2.0-manifest.json).

The [October 23 combined review](#combined-protocol-review--october-23-2026)
is the current scope decision. In particular, the trigger section below is a
retained **deferred design**, not a requirement to add its RPCs to this release.

## Latest-main integration — October 24, 2026

The [integration checkpoint](duckdb-2.0-main-integration-2026-10-24.md) records exact merged
refs, dependency versions and bounded test evidence. Published 2.2's six batched catalog streams
carry the successor records unchanged, including canonical constraints, typed index definitions,
macro families and column metadata. `SchemaInfo` no longer carries attachment state on the wire.
Where selected capabilities project inventory, Python scopes cached responses, publication keys
and etags to that projection. The discriminator is internal; no new RPC or capability is added.

Identity.v2 and continued table-in-out output are integrated alongside generated native codecs.
Earlier dated version/source/test statements below remain historical. This merge does not freeze
the successor, complete the 2.0 engine port or implement automatic whole-query pushdown.

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
No duplicate field-add/drop/rename RPC family is needed. DEFAULT/USING now use
the mandatory canonical payload described in the October 23 implementation
checkpoint. Generated dependencies, full column metadata and complete
cross-object CASCADE remain part of the combined contract work. The limited
reference does not qualify those remaining semantics or every logical type.

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
1.5 offers no schema-options profiles. The combined public protocol
version remains undecided until the remaining contract changes are reviewed;
this development wire snapshot must not be published as a stable 2.1 release.
Schema comment/tag alterations are implemented in the later checkpoint below;
current operation-specific requirements and remaining work follow the October 23
review. No generic object-requirements carrier is selected.

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
to advertise no table-options/layout profiles. SQL enablement, production provider
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


## Combined protocol review — October 23, 2026

This source review reconciles the complete field inventory with the implemented
development contract. It closes scope/mapping review, not P1 semantic proof,
P2 freeze or P3 release. No runtime shapes, version labels or engine pins change
in this checkpoint. Original audit evidence and historical runtime results remain
intact in the [manifest](duckdb-2.0-manifest.json); its
`combined_protocol_review_2026_10_23` records the remaining work and deferral costs.

### Remaining included column and cleanup changes

| Existing field | Successor contract selected by this review |
| --- | --- |
| `catalog_table_column_default_set.expression` | Required `binary` carrying exactly one canonical `SqlExpression` IPC record, replacing raw SQL text. SQL NULL is an expression value; DROP DEFAULT stays its own existing method. |
| `catalog_table_column_type_change.expression` | Nullable `binary` carrying the same record. NULL requests the qualified ordinary cast path; a present record defines USING against the old row. |
| `CatalogAttachResult.default_schema` | Nullable scalar string. NULL means no implicit default; a present value is one nonempty, non-NUL raw root identifier. Existing providers can still explicitly return `main`. No empty-string coercion, dot splitting, nested search path or invented default. |
| Macro create/read definition | Required nonempty overload family; a single macro is a one-element family. Parent definition, parameters, defaults and arguments-schema fields are removed. Local convenience helpers normalize simple definitions into the canonical family. |

The DEFAULT/USING payload migration is implemented in the development branch,
including generated SDKs and canonical CREATE/ADD default metadata. Per the
October 23 implementation decision, these are **mandatory successor wire
representations**, with no new default/USING capability negotiation. The semantic
`profile` remains part of `SqlExpression`; it identifies the interpretation of
the SQL, not a new permission flag. Unknown profiles, malformed records and
owner mismatches reject before provider effects.

Nullable default schema is now implemented in the development protocol and SDKs.
Python's attach result and declarative `Catalog` accept `default_schema=None`.
Omitting the constructor argument retains the existing `"main"` default;
explicit NULL stays absent through attach responses, catalog discovery, clients
and codecs. A present value must be nonempty and contain no NUL; dots are literal
characters in one root identifier. A catalog without a default must register its
functions in explicit schemas instead of relying on an implicit registration
schema. No capability flag or extra field is needed.

The native 1.5 adapter rejects absent defaults before publishing an attachment
or registering its settings. It also rejects empty/NUL names instead of
coercing them to `main`. Present defaults, including literal dotted identifiers,
retain their behavior. Native 2.0 can later map absence to its optional catalog
API; that execution path is not qualified by the 1.5 tests.

Macro create/read now share the canonical family representation in this same
major transition. The Python client, provider hook and worker use the existing
catalog methods for the complete family. No overload-specific opt-in is needed
for this mandatory wire shape. Types, documentation and ordered parameters
remain per overload; discovery returns resolved literal defaults, including
typed NULLs, and rejects unresolved expression defaults. Function argument
defaults remain a separate, unchanged contract. See the
[macro contract](macro-contract.md) for the native adapter's bounded support
and remaining qualification work.

Successor column CREATE/ADD use canonical base64 IPC field annotations for
defaults. Native 1.5 also translates supported virtual generated definitions
on CREATE and discovery; its parser rejects ALTER ADD of generated columns.
Comment remains `comment`;
tags remain `tags`, encoded as a UTF-8 JSON object with literal string keys and
values. Reject duplicate decoded keys (including escaped-equivalent keys),
malformed UTF-8/JSON, unpaired Unicode surrogates and non-string values before
provider effects or table publication. Preserve explicit empty comments/maps,
literal dots, Unicode and NUL, and unrelated metadata. Validation traverses
container, dictionary and extension children. No generic metadata RPC or new
expression envelope is selected. Schema/table/index option values remain typed
literals.

The existing keys have mandatory meanings in this successor contract; no
`vgi.column.metadata.create.v1` opt-in is required. Python's `ColumnMetadata`,
`column_metadata`, `with_column_metadata` and `validate_column_metadata` helpers
provide authoring and validation without changing the wire schema. CREATE, ADD
and discovery validate the same values. The durable column reference retains
them in its existing transaction snapshots and readback.

Native 1.5 forwards column comments on CREATE/ADD and imports them on discovery,
preserving absent versus empty text. It validates and retains tag annotations in
the authoritative Arrow schema. Its `ColumnDefinition` has no public tags API,
so SQL authoring/introspection of column tags and tag preservation through
reconstruction using only native columns remain native-adapter limitations.
Nested comments likewise remain Arrow metadata where the engine lacks a nested
column-comment API. No engine-private access or engine-pin change is introduced.

The proposed `vgi.column.default.v1`, `vgi.column.metadata.create.v1`,
`vgi.column.type.using.v1` and `vgi.column.generated.virtual.v1` capability flags
are withdrawn: the canonical representations need no additional opt-in.
`vgi.column.default.v1` remains an Arrow metadata key, independently of
capability names. A codec accepting an expression record does not establish
provider execution support.

DEFAULT/USING now use the mandatory canonical representation even with no
catalog-capability offer. Existing nested-operation, transaction and constraint
requirements still apply where they already applied. DROP DEFAULT remains its
own operation. Providers validate their supported grammar/context and return
ordinary unsupported errors for operations they cannot implement. A TYPE-only
column definition cannot carry default/generated metadata at any nesting level.
The virtual generated-column checkpoint below qualifies a bounded CREATE/read
path. Unsupported generated mutations and grammar remain ordinary errors.

The durable column reference preserves canonical SQL/context through CREATE,
ADD, SET/DROP DEFAULT, USING, discovery and transaction snapshots. The native
1.5 adapter supports a bounded deterministic scalar grammar under the explicit
default evaluation context, including numeric division. It materializes supported
defaults as typed literals for later native binding while retaining the original
record in authoritative table metadata. Unsupported contexts and volatile/general
SQL reject; the generated path has its own bounded qualification below.

Existing static `Table.defaults` descriptor discovery retains its legacy read
adapter; it is not silently interpreted as new canonical metadata. The custom
DuckDB transactor fixture cannot preserve canonical expression contexts and now
rejects those DEFAULT/USING operations explicitly. Its plain casts and expression-
free CREATE/ADD remain available. The durable reference supplies the tested
canonical mutation behavior; no transactor or native2 qualification is implied.

Virtual generated definitions pair `vgi.column.generated.v1` (canonical base64
`SqlExpression` IPC) with `vgi.column.generation.v1 = virtual`. The Arrow field
supplies the resolved result type. Authoring conveniences translate their SQL
strings locally using the owning schema and explicit default evaluation context.
Discovery and CREATE reject legacy-only `generated_expression`, mixed legacy
and canonical forms, incomplete pairs, malformed records, wrong owners and
unsupported interpretation profiles. The generic category codec retains
`stored`, but current catalog admission rejects it before provider effects or
publication; it is not an executable feature.

The native 1.5 checkpoint supports top-level deterministic virtual expressions
over signed integer, Boolean and string columns under the default context.
Dependencies, cycles and declared result types are checked before publication.
The worker returns physical fields in their original relative order; DuckDB
computes virtual results. Generated fields must not consume physical indices.
The durable generated reference reuses the existing dependency authority and
transaction snapshots, including physical scan readback after reconnect.

This checkpoint does not qualify native INSERT/UPDATE/DELETE on generated
tables, stored or nested generation, or semantic generated-column rewrites.
Those paths reject before effects. Native ALTER ADD of generated columns remains
rejected by the pinned parser. The durable reference rejects rename/drop/type
column edits unchanged; the earlier metadata-only dependency reference retains
its separately bounded rewrite behavior. The native proof covers a single
worker-function scan; delegated, multiple-branch and partitioned generated scans
reject pending separate qualification. No new RPC, record schema, engine pin,
capability flag or public protocol version is introduced.

Views keep SELECT text, owner-relative late binding and derived output schemas;
no fixed persisted output schema or implicit caller search-path contract is
introduced. Existing aliases, security, comments/tags and column comments remain.
Dependency `owned` edges describe drop lifetime, not principal authorization:
SQL ownership transfer is deferred. Non-default physical compression is also
deferred until a provider defines portable semantics. Existing typed options or
metadata may suffice for a future compression profile.

Keep `ClientCapabilities.filter_encodings`: the historical proposal to remove
it has no demonstrated consumer conflict. Typed function defaults remain
authoritative; display-only `vgi_default` metadata is not a second executable
default contract. Raw paths and canonical constraints already provide the other
selected cleanup; no parallel replacement RPC families are needed.

### Optimizer work that still blocks P1

Projection and repeatability remain included to avoid an immediate second wire
migration, but their final fields are **not frozen**. Existing split tokens,
parallelism limits, ordering and string diagnostics remain sufficient for the
reviewed engine hooks; no typed metric or selected-partition-ID record is selected.

Projection needs a closed scalar profile, exact result types and deterministic
original-input/result-slot mapping. Filter v2 currently requires a Boolean root
and types its calls as Boolean; its grammar cannot simply be relabeled a scalar
projection contract. A bounded first proof can use ordinary CAST of a source
column, keeping TRY_CAST and other functions unsupported until specified.

The support decision must precede optimizer replacement. Current native
`table_function_plan` runs during global initialization, so a decline in its
response arrives too late. Prefer a negotiated closed subset the native callback
can decide locally, then carry the accepted plan through existing plan/init
requests and their exact output schema. If support depends on provider state,
prove a pure preparation round trip before replacement. This lifecycle choice
and the exact record fields remain P1 work. Declined expressions retain local
evaluation and all needed input columns; malformed accepted results are errors,
never execute-then-fallback.

Prove nested paths, reordered outputs, residual/runtime filters, bind copies,
split fingerprints and cache identities use the same mapping. No general SQL
bag, serialized private AST, or speculative result-identity framework is selected.

Repeatability needs a bind-specific claim, initially absent/unknown versus
stable within one query, stored with bind data. Prefer one enum over an empty
wrapper record. The same bind, parameters and transaction/snapshot must yield
the same logical result when reevaluated. Cache TTL, scalar stability, replayable
split tokens and worker count do not prove that guarantee or authorize retries.
Before freezing, demonstrate mutable-provider behavior, prepared bind reuse,
snapshot expiry and repeated evaluation without inventing a new query coordinator.

### Reviewed deferrals and rollout

New trigger CRUD/execution, sequence allocation, named-type catalog APIs,
collation/CRS catalogs, standalone windows, arbitrary mutating statements/CONNECT,
multiple relational input transport and typed metrics are deferred. Existing
aggregate windows, read-query execution, supported Arrow value transport and
transaction/write phases remain. The manifest records each actual missing
semantic guarantee, inclusion proof, user-visible limitation and future version
cost. Deferred candidate names are not part of the included freeze inventory.

No new query parameter carrier is needed: types/values use Arrow batches with
the existing mapping metadata. No additional TUPLE/VARIANT carrier is selected:
keep recursive tuple annotations and canonical unshredded Arrow Parquet Variant,
with explicit engine mapping limits. Adapter qualification does not follow from
opaque metadata/byte preservation alone.

The successor requires a major version and matching routing name; the historical
additive `2.2.0` successor candidate is superseded. Published main has independently reached
`2.2.0`; that release is the integration baseline, not the final successor version. Choose the
number at freeze. Keep exact
major/minor checks and deploy separate versioned endpoints/processes. Drain old
transactions, then reattach and reprepare; never transfer tokens or retry an
ambiguous mutation during migration. Rollback needs old-compatible catalog
objects or a tested restore/migration. P3 must exercise these steps and safe
unsupported behavior. No version-aware router or recovery service is required.

Validation on replacement EC2 `98.81.121.120` passed: `--require-decisions` covers
151 audit rows and 20 areas with zero unresolved scope mappings; all 11 manifest
tests pass. Logs: `logs/protocol-review-20261023-SBCl67`. P0 is complete; these
inventory checks do not close P1 or qualify new runtime behavior.

## Combined expression, constraint, transaction and trigger design

**Checkpoint: 2026-10-14. Status: SQL/context and canonical constraint records
generated; bounded public constraint migration verified on pinned 1.5; remaining APIs
specified and not frozen.** The definitions in this
section supersede placeholder records in the manifest and the older trigger
proposal. They belong to the same major transition as the existing development
changes. Engine pins remain unchanged; the public inventory now has 84 methods.
The 2026-10-15 scope correction below supersedes the earlier transaction and
durable recovery proposals; it changes documentation only.
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
canonical apply with the discovered revision. At that checkpoint CHECK, nested
paths, unsupported trust states and broader FK actions rejected explicitly.
The October 23 checkpoint below adds bounded CHECK discovery. The October 24
nested-NOT_NULL follow-up additionally imports enforced, ALL_ROWS facts for
ordinary STRUCT field paths. It validates exact raw components and recursive
Arrow nullability, preserving the original canonical records. A nonnullable
child does not make its parent nonnullable: only top-level NOT_NULL facts become
DuckDB 1.5 constraints or supply top-level ALTER identities. Writes still require
the negotiated provider enforcement profile. Collection and logical-type storage
children do not become SQL constraint paths. Nested UNIQUE/PRIMARY_KEY/FK, nested
SQL constraint DDL, CHECK creation and other unsupported semantics still reject.

EC2 validation passed for the Python public lifecycle, all SDK contract/adapter
suites, the bounded native 1.5 adapter and six built-worker discovery lanes.
Java's shared RPC serializer now preserves declared Arrow dictionary encodings
for these embedded records; strict IPC and cross-language tests cover the fix.
SDK fixture declarations are METADATA_ONLY/UNVALIDATED, without an enforcement
or native trusted-materialization claim. Exact counts, skips and logs are in
the manifest's `constraint_public_checkpoint`.

The October 24 full-language SQL follow-up supersedes those unqualified example
fixture declarations. The read-only examples validate facts against the same
immutable rows emitted by scans and publish matching Arrow nullability and
version-owned constraint snapshots. Their attach selection advertises and selects
only supported, offered read profiles; it adds no generic enforcement or write
claim. Ordinary unconstrained fixture columns remain nullable. The complete
configured seven-language matrix and explicit skips are recorded under
`full_language_slt_checkpoint_2026_10_24`.

This checkpoint adds one method and changes parent schemas; no version is
published or release gate closed. Generic legacy
write/bind authorization remains provider-owned: preliminary table reads are
not atomic with later operations. Later checkpoints implement typed indexes and
semantic ALTER dependencies. Trigger execution remains pending; the 2026-10-15
scope correction replaces the earlier statement lifecycle/replay proposals.

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
SDK corpus. Native 1.5 now translates supported CREATE/ADD defaults and reads
canonical default metadata; existing static descriptor discovery retains its
legacy adapter. Default/USING mutation payloads now carry `SqlExpression` IPC;
no second column-RPC family is added. CHECK and index definitions use the same record.

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

**Scope correction, 2026-10-15:** reuse VGI's existing transaction and write
lifecycle. The earlier savepoint, general statement-ticket/receipt and durable
mutation recovery proposals are withdrawn from this protocol package. These
were design-only proposals; no corresponding RPCs or profiles were implemented.

Keep `supports_transactions`, the existing begin/commit/rollback RPCs, opaque
attachment-bound transaction handles, and write init/exchange/finalize. Retain
`CatalogCapabilities.transaction` and the implemented `vgi.transaction.ddl.v1`
profile. Existing providers already implement real transactions; the task is to
qualify the DuckDB2 paths that use them, not introduce another coordinator.
Additional DML/mixed/multi-operation profiles are not selected changes: first
identify a concrete interoperability gap that the existing contract cannot cover.

For accepted composite operations, DDL, row writes and trigger effects must use
the same provider transaction. Audit CTAS, expanded ALTER and multi-DML execution
for correct token propagation, finalization and error handling. A failed write
must not permit its partial effects to commit; use the existing whole-transaction
rollback/invalidation path. Do not add savepoints or promise preservation of
previous statements after a partially applied write fails. Preserve existing
provider-specific handling of errors that occur before effects.

Reuse existing commit/rollback semantics, including the interface's existing
idempotency requirement. Retrying completion with the same transaction identifier
must not apply effects twice; retain normal provider results/errors. The current
transactor removes completed transactions and rejects subsequent completion calls
as inactive. That error does not establish whether an earlier COMMIT succeeded,
but it does not require a new recovery mechanism. No retained-outcome journal,
recovery endpoint or retention window is introduced.

The earlier blanket single-attempt recommendation for transaction RPCs was too
broad. Preserve commit/rollback retry behavior; the single-attempt safeguards
apply to non-idempotent mutations. BEGIN now also uses a single attempt because
its identifier arrives in the response: the lost-response probe reproduced a
second allocation and an abandoned first transaction. Healthy subprocess pool
reuse is preserved. The original abandoned transaction remains the provider's
responsibility; the shared transactor has no timed reaper, and a client disconnect
does not reclaim it. Explicit completion or provider shutdown does. No distributed
atomicity across independent attachments is promised.

The 2026-10-15 source audit confirms that pinned Haybarn2 already sequences
statement triggers through BEFORE/base/AFTER CTEs and wraps expanded ALTER in
its existing transaction/error machinery. VGI already passes the provider
transaction through CTAS and write finalization. Its absent engine catalog
version also makes native prepared statements conservatively rebind. These
paths do not demonstrate a need for another coordinator or a new per-write
requirements revision. Provider snapshot/concurrent-metadata behavior still
needs qualification.

The initial checkpoints are 1.5-compatible: local transaction exception cleanup,
repeated completion and failure propagation through the current write phases.
These now have bounded runtime evidence below. The signed HTTP writable fixture's
sealed-token bug was reproduced and fixed with authenticated callback parameters;
the wire token remains sealed. The 2026-10-22 follow-up
narrows the finalization findings: current generic/simple writable fixtures
have no provider finalize callback and return results per input batch. Locked
`vgi-rpc` 0.50.0 buffers `out.emit` until `process` succeeds, so an ordinary
emit-then-raise does not send a success response followed by an error. Raw
pre-FINALIZE drain robustness is not a demonstrated current-provider failure.

Optional final output is allowed by the table-in-out API, but native writable
finalization currently interprets it as count output; deferred RETURNING is
an adapter support gap. Add final-row handling with a concrete provider/test
before enabling that form, using the existing schema and lifecycle. Native
OLD/NEW changes adaptation belongs to the DuckDB2 lane. Exact source anchors
are in the manifest's `write_path_source_audit_2026_10_15` and
`write_finalization_source_audit_2026_10_22`. These are source findings only;
EC2 was unavailable and no new tests were run.

The subsequent EC2 checkpoint on `18.234.138.19` ran seven native 1.5 subprocess
probes: normal INPUT RETURNING, buffered emit/process error, count finalization,
multiple batches, zero input, finalizer error, and expected rejection of deferred
RETURNING. All behaved as expected. This diagnostic provider has no transactions
or stored rows; it does not qualify rollback or signed HTTP. See
`write_finalization_runtime_checkpoint_2026_10_22` for exact evidence.

The subsequent native transaction cleanup checkpoint reproduces the local leak
on provider completion errors (24 failing lifetime assertions before the fix).
The manager now transfers ownership before commit/rollback, and Commit retires
its schema cache before the provider call. Scoped destruction releases the local
transaction even when the call throws; bound catalog entries retain their
existing bounded catalog lifetime. Errors, RPCs, transaction identifiers and
retry policies are unchanged. No compensating rollback is introduced.

On EC2, all 960 assertions pass across six scenarios: commit/rollback success,
provider rejection, and provider error after completion, each repeated three
times. Prepared-statement reuse, fresh transactions, a concurrent connection's
snapshot, metadata lifetime and exact callback traces pass. The existing native
semantic ALTER transaction test also passes rollback, commit and reconnect.
This diagnostic does not mutate rows or simulate transport loss. It does not
qualify duplicate completion, BEGIN allocation cleanup, signed HTTP, or broader
write atomicity by itself. See `transaction_cleanup_checkpoint_2026_10_22` for
that evidence and the subsequent checkpoint below for those follow-ups.

The transaction/write checkpoint completes two further bounded checks on the
same pinned 1.5 engine:

- Six lifecycle tests use real DuckDB transactions through the existing
  transactor's Unix RPC server. Repeated and opposite completion calls reject an
  inactive token without reapplying effects or disturbing another transaction.
  Disconnect leaves an open transaction; explicit provider shutdown rolls back
  its DML and DDL, verified after reopening storage.
- The native lost-BEGIN probe now returns an error after exactly one allocation.
  Three healthy autocommit queries reuse one worker PID. ATTACH primes separate
  concrete HTTP provider parameters for single-attempt calls; reusing that cache
  avoids reentering transaction initialization during HTTP BEGIN. Those
  single-attempt parameters are immutable after publication. Shared-container
  dispatch honors the same retry classification, with readiness retries allowed
  before dispatch; that transport change is compiled and source-reviewed only.
- Signed HTTP callbacks receive authenticated provider transaction bytes in
  `BindParams`, `InitParams` and `ProcessParams`, including continuation turns.
  Requests and serialized continuation state retain sealed tokens. Seven focused
  pytest items cover commit/rollback, count/rows, wrong-attachment rejection and
  Worker/MetaWorker lifecycle callbacks; 124 existing regression items and 74
  callback/storage items pass. No wire fields or protocol version change.
- Seventeen native signed-HTTP scenarios use a real SQLite transactional
  reference: five successful count/RETURNING/rollback/zero-row/CTAS cases and
  twelve INSERT/CTAS failures at initialization, the second batch and finalization
  in autocommit and explicit transactions. The provider does not self-roll-back
  on callback error. Native rollback removes partial writes and CTAS tables,
  including an earlier successful statement when COMMIT follows the error.
  Independent committed readback and exact completion traces verify the result;
  failed RETURNING produces no partial successful rows, and a later write succeeds.

The EC2 Python DuckDB 1.5.5 lacks this project's private `subcursor()` and
`enable_suspended_queries` APIs. Lifecycle tests therefore register real DuckDB
cursors directly, while write tests use the bounded SQLite provider. This proves
the tested native transaction path, not the complete custom DuckDB transactor or
every backend. No timed orphan reaper, savepoints, recovery service or changed
COMMIT/ROLLBACK retry semantics are introduced. Deferred RETURNING, native2
OLD/NEW, triggers, expanded ALTER and multi-DML qualification remain separate.
Exact artifacts and source/binary hashes are recorded in
`transaction_write_checkpoint_2026_10_22` in the manifest.

The October 22 full launcher regression run was non-green: 242 actual passes,
66 failures and 59 skips. Baseline source and representative probes account
for 64 constraint-negotiation failures. Two full-run scan failures pass focused
old/current Python reruns; their cause remains unresolved. Whole-repo lint and
mypy pass; one unchanged baseline file fails formatting. The manifest separates
this broader status from the passing transaction/write checkpoint.

The subsequent October 23 checkpoint corrects native constraint negotiation:
canonical read authority permits discovery of trusted ENFORCED/ALL_ROWS facts
without granting write authority. Native constrained DML, constraint DDL and
unique-index DDL check enforcement explicitly. All four table-loading paths
check the selected read profiles, including time travel. No provider constraint
metadata is removed or downgraded to make discovery succeed.

Native CHECK discovery accepts one numeric comparison (`=`, `<>`, `<`, `>`,
`<=`, `>=`) under `vgi.expression.duckdb.scalar.v1` in the read category. Operands
are primitive signed numeric columns (including FLOAT/DOUBLE) or numeric
constants; the complete expression record must identify the owning flat schema
and explicit default evaluation context. The receiving binder verifies the
actual table columns and Boolean result. Functions, casts, generated or
extension-typed operands, other expression shapes/contexts and extra statements
reject. This bounded read support does not enable CHECK creation or general
scalar expressions. Sixteen C++ cases (415 assertions) and four integration
probes (308 assertions, no skips) verify the boundary, including a real SQL
INSERT rejected before write discovery on the read-only fixture.

The scan investigation also reproduces silent truncation through real launcher
workers: after an early process exit, native returned successful counts of zero
or 100 from an incomplete scan. VGI requires explicit Arrow EOS; physical EOF is
not successful completion. Raw function connections now reject incomplete reads
with an I/O error, including EOF at a valid batch boundary. Twelve launcher and
subprocess cases verify failure without partial successful output after empty,
nonempty and split batches, plus healthy empty-batch, empty-result and pagination
controls. This changes neither the wire format nor retry policy.

The original spontaneous disconnect was not reproduced: predecessor tests and
30 stress cycles passed. A separately reproduced RPC launcher idle race has no
established link to that run and is recorded as a dependency follow-up, with no
dependency change here. The manifest's `regression_checkpoint_2026_10_23` records
the full-suite result and the distinction between injected failures and the
original intermittent observations.

The final October 23 full launcher suite passes 308 executed cases and 11,538
assertions with zero failures; 59 environment-dependent cases remain skipped.
Both previously failing scan tests pass in that run. Whole-repo Python lint and
mypy pass; the same unchanged baseline formatting issue remains. These results
qualify the tested configuration, not the skipped transport/backend matrix or
the DuckDB2 adapter.

### Trigger definitions and statement ownership

**Deferred by the October 23 combined review.** The following candidate design
preserves prior source findings for future work. Its records, methods and
profiles are not selected for the current freeze and must not be advertised.

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
definition plus ERROR/IGNORE/REPLACE; REPLACE atomically changes definition and
dependencies. Metadata alteration takes
`CatalogMetadataChanges` plus an expected revision. DROP has explicit
ignore-missing/CASCADE semantics. GET returns optional `TriggerInfo`; listing
returns complete infos and the existing catalog version from
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

The pinned ROW implementation is set-based and decorrelated: row order is
unspecified, all rows for trigger A execute before trigger B, and an individual
firing does not see writes made by earlier individual firings. A row profile
must preserve these limits rather than promise sequential per-row visibility.

Execution ownership is exclusive for the complete table trigger plan in the
initial profile. ENGINE means the qualified client expands definitions once and
the backend does not also fire them. BACKEND means the provider owns all trigger
execution; clients never install those definitions as executable engine entries.
The initial profile rejects mixed owners rather than leave relative ordering
undefined. Listing order within each event/timing/granularity phase must match
the pinned engine's case-insensitive alphabetical order. Its `ScanTriggers`
consumer preserves the supplied order rather than sorting it; do not use
incidental hash-map order or add a separate ordering record.

Start with ENGINE-owned execution and worker-persisted definitions. Native
`ScanTriggers` currently serves both discovery and executable trigger selection;
it cannot safely expose BACKEND-owned definitions as ordinary engine entries.
Native discovery of those definitions needs an engine filtering hook before
exposing them through `duckdb_triggers()`. RPC-only discovery can avoid that
engine change. The current transactor's per-batch SQL still does not establish
whole-statement backend trigger semantics.

Pinned statement-trigger expansion forces base DML `RETURNING *`, including
when no transition alias is declared, so this path needs the existing rows
mode. UPDATE with OLD capture additionally needs native adaptation of the
existing paired OLD/NEW `changes` result into the engine's NEW-then-OLD columns
and its duplicate-target policy. Returned rows must not be zipped to input OLD
values without guaranteed correspondence. The existing carrier is sufficient;
no additional OLD/NEW wire record has been justified.

For execution, audit the existing table/write discovery, sealed bind state,
transaction context, init/exchange/finalize and OLD/NEW `changes` results first.
Use those paths to carry and validate table identity, execution ownership,
required result mode and current definition revision wherever possible. A cached
binding must not bypass a newly required trigger or constraint; determine the
smallest missing validation or metadata field from the actual execution path
before selecting a new wire record. The earlier `WriteRequirements`,
`CatalogWriteBinding`, `WriteExecutionContext` and intent/ticket/receipt design
is withdrawn as a selected implementation plan.

The remaining concrete trigger questions are execution ownership, ordering,
UPDATE OF targets, complete OLD/NEW results, zero-row behavior and errors across
multiple batches. Existing function finalization may provide the needed boundary;
prove that against the pinned engine. A batch is not necessarily the complete
original SQL statement: the current transactor issues backend SQL per batch.
Do not infer statement-trigger semantics merely from those backend statements,
and do not infer that a new coordinator is required merely because batching
exists. Add only a demonstrated missing boundary or metadata value to the
existing path, with a focused regression. If a trigger form cannot be supported
correctly yet, reject it before effects rather than add a general workflow system.

All supported base and trigger writes use the existing transaction. Propagate
failure through the existing error/rollback path and ensure finalization errors
cannot lead to commit. Trigger/constraint changes concurrent with a write must
serialize or cause a conflict using provider transaction and revision checks.
There is no separate statement-begin/complete/abort RPC family, statement ticket,
operation receipt transport, recovery service or savepoint support in this scope.

Proposed read/DDL profiles remain `vgi.trigger.read.v1`,
`vgi.trigger.create.v1`, `vgi.trigger.drop.v1`, `vgi.trigger.metadata.v1`.
SQL/action semantic profiles belong to the read category, as the existing
scalar-expression profile does; they do not independently grant execution.
Candidate execution profiles `vgi.trigger.engine.v1` and
`vgi.trigger.backend.v1` remain under review. Execution requires matching client
and provider behavior, transaction support and the applicable result mode.
Read/create profiles do not imply execution. None is currently advertised.

### Mutation response failures

Durable mutation replay/status is out of scope for this combined release.
Do not add `catalog_mutation_execute`, `catalog_operation_status`, operation
identity records, receipt journals or automatic write recovery. Ordinary errors
and the existing no-automatic-retry policy are sufficient for this work. Existing
read/query result replay functionality is unaffected by this scope decision.

### Design acceptance and implementation order

1. Qualify the expression/context carrier and role matrix, including typed NULL,
   generated dependency cycles, old-row USING evaluation and schema metadata
   preservation. Preserve the existing macro-default evaluation distinction.
2. Implement canonical constraint read/create/apply and typed index records in
   a durable reference. Prove real enforcement, existing-data failure rollback,
   FK NULL/action semantics, primary-key nullability and backing-index closure.
3. Continue from the native 1.5 cleanup, completion, signed-token and persisted
   INSERT/CTAS rollback checkpoints. Qualify expanded ALTER, multi-DML and the
   custom DuckDB transactor through the existing transaction paths.
   No savepoints or general statement-ticket/receipt lifecycle is required.
4. Complete remaining column semantics and optimizer proofs from the October 23
   review. Canonical macro families, DEFAULT/USING and nullable default-schema
   wire migrations are implemented. Broader macro provider/body qualification
   remains separate. Trigger CRUD/execution is deferred; retain
   its proof requirements above for any future scope change.
5. Extend dependency planning to constraints and column/ALTER blockers. Test
   rename/replacement, inaccessible dependents, cross-schema identity and no
   cross-attachment authority assumption.
6. Generate the selected records/methods across all seven SDKs, with positive
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
