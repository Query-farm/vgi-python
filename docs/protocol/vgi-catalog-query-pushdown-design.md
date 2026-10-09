# VGI 2 catalog query pushdown

**Status:** proposed implementation design, not an implemented or normative wire contract.
**Updated:** 2026-10-08. **Original proposal:** 2026-09-13.
**Scope:** optional, read-only catalog query execution; Python SDK; DuckDB 1.5 explicit-query adapter;
DuckDB 2.0-development automatic-pushdown adapter.

The implementation target is an optional extension of the VGI 2 protocol family, proposed for
wire version **2.2.0** under the existing protocol name **`vgi.v2`**. The current Python SDK and
DuckDB extension already use `2.1.0`; this is no longer a change to an unreleased `2.0.0` contract.
New query APIs in this design remain proposals.

| Component | Current baseline | Role in this design |
|---|---|---|
| `vgi-python` | `2029f09`, package `0.42.1`, wire `2.1.0` | Worker, protocol, client, and generators |
| `vgi` | `de68c17`, wire `2.1.0` | Existing extension and reusable execution machinery |
| `vgi/duckdb` submodule | `105edd31b57f`, `haybarn-v1.5.5-rc1` | Explicit-query adapter's current engine dependency |
| `duckdb-3/duckdb` | `80e17fc252`, `v2.0-cyanoptera`, 2026-10-04 | Automatic adapter and SQL-export baseline; `duckdb-8/duckdb` has the same HEAD |
| Original `duckdb-2/duckdb` checkout | `2284ca3c90`, 2026-09-14 | Older comparison baseline, not the new API target |

“DuckDB 2.0” means the inspected development API, not a released upstream compatibility claim.
Wire versions, SDK package versions, and the two engine targets are separate version axes.

## 1. Decision

Add a negotiated **catalog-query preparation RPC**, then execute accepted queries through VGI's
existing TABLE `bind` / `init` / Arrow-result lifecycle. Provide one SDK-owned reserved executor;
do not make each catalog author implement a second streaming protocol.

Ship two engine entry points:

- An explicit `vgi_query(catalog, sql)` table function, usable with the existing DuckDB 1.5 extension.
  The SQL runs with the remote provider's declared dialect and read-only semantics.
- Automatic replacement of eligible single-catalog read queries in the DuckDB 2.0 adapter.
  This requires a substantially stronger promise: the remote result must satisfy the semantics
  and output contract of the original locally bound DuckDB query.

Keep query pushdown optional and independent of filter-v2 support. A worker that can scan tables,
but cannot execute queries over its logical catalog, continues to work unchanged after upgrading
to the agreed wire version. A remote backend need not be DuckDB, but the first automatic provider
should be DuckDB-backed and pass an explicit compatibility suite. SQL transpilation alone is not
evidence of semantic compatibility.

For automatic mode, first prototype an **early bound-plan optimizer extension using DuckDB's
new `LogicalPlanSQLExporter`**. It can keep the original logical plan while asking the worker to
prepare exported SQL, then replace the plan only on acceptance. This makes a new DuckDB catalog
hook a conditional alternative, not a mandatory first dependency. The existing consuming
`RemoteExecute(QueryNode)` hook still has no safe decline contract; do not route VGI through it
while using the bound-plan path.

### Current integration decisions

| Updated capability | Implementation decision |
|---|---|
| Logical-plan SQL exporter and structured issues | Reuse the exporter in a pre-optimizer feasibility spike; do not build a general SQL decompiler |
| `BoundTableFunction`, explicit parser contexts, ref-counted `LogicalType` | Add a dedicated 2.0 compatibility layer before enabling automatic queries |
| `catalog_contents`, metadata etags, version adoption | Reuse existing metadata caches; distinguish committed metadata from transaction/data snapshots |
| `vgi.v2` routing and wire `2.1.0` | Route new RPCs with generated protocol identifiers; propose coordinated `2.2.0` rollout |
| Typed nested catalog records and six worker SDKs | Use typed structs for new query records and include C# schema/type generation |
| Worker-batch validation, cancellation dispatcher, scan interrupt checks | Extend existing guards and lifecycle hooks rather than recreate them |
| Credential-aware caches and HTTP grants | Preserve current identity isolation; keep query-result caching disabled |

### Why this is worth adding

Filter pushdown reduces rows read from one scan. Catalog query pushdown moves an entire eligible
relational computation to the catalog provider: joins, filters, projections, grouping, aggregates,
sorting, and limits can execute together near the data.

For example, with decimal-valued amounts:

```sql
SELECT c.region, sum(o.amount) AS revenue
FROM warehouse.sales.orders AS o
JOIN warehouse.sales.customers AS c ON c.id = o.customer_id
WHERE o.status = 'paid'
GROUP BY c.region
ORDER BY revenue DESC NULLS LAST;
```

The worker can return one row per region instead of transferring the qualifying order and
customer rows for a local join and aggregation. This is a data-movement optimization, not a
promise to eliminate metadata lookups, binding, or every network round trip.

The proposed explicit entry point exposes the same execution facility on DuckDB 1.5:

```sql
SELECT *
FROM vgi_query('warehouse', '
    SELECT region, count(*) AS customers
    FROM sales.customers
    GROUP BY region
    ORDER BY customers DESC NULLS LAST
');
```

Here the outer query runs locally and the SQL string uses the attached provider's namespace.
This syntax is an implementation target, not an existing registered function.

```text
Original SQL
    -> ordinary binding -> early eligibility and SQL export
         ineligible/unsupported -> original bound plan -> normal optimization and VGI scans
         eligible -> catalog_query_prepare
                         declined -> original bound plan
                         accepted -> one reserved VGI query scan
                                         -> bind -> init -> Arrow batches -> cleanup
```

## 2. What the current code provides, and what it does not

| Layer | Available now | Required addition |
|---|---|---|
| DuckDB development API | Consuming remote-catalog hooks; bound-plan SQL exporter; pre/post optimizer extensions | Early export feasibility, VGI source reconstruction, and accepted-plan replacement |
| VGI attachment | Client capabilities, attachment/transaction tokens, bulk catalog contents and etags | Query capability negotiation and transaction-aware metadata validation |
| VGI function execution | Dynamic bind schema, TABLE streaming, batch/type validation, cancellation dispatcher | Reserved prepared-query dispatch and stricter query execution invariants |
| Python worker | Declarative catalogs, scoped functions, state codecs, storage abstractions | Query-provider interface and transport-safe execution state |
| Python transactor | Attachment-scoped databases, transaction cursors, Arrow scan producers | Reusable query-owner integration, prepare-only metadata, explicit handle lifecycle |
| DuckDB VGI extension | Catalogs, Arrow scans, transaction integration, filter pushdown, result cache | Query binder, query-specific bind data, semantic adapter, cache exclusion |

Important limits of DuckDB's existing **parsed-statement remote-catalog pass** (distinct from the
proposed bound-plan adapter):

1. The pass runs on parsed statements before normal query binding. It identifies references to
   one remote **catalog instance**, not merely one worker URL. Two attachments to the same URL
   can have different credentials, options, and transactions and must not be merged.
2. `RemoteCapability::EXECUTE_QUERY_NODE` is not synonymous with SELECT-only execution. Query
   nodes also represent writes. The adapter must explicitly reject write node kinds as well as
   full-statement DDL and raw-SQL `CONNECT` delegation.
3. The base `SupportsPushdown` overloads default to permissive behavior. The VGI adapter must
   override all relevant overloads with explicit allowlists and reject unknown node kinds.
4. `FinishPushdown` strips catalog qualification, moves the original query into `RemoteExecute`,
   and wraps the returned reference in `SELECT *`. It neither preserves an untouched fallback
   candidate nor checks a nullable returned reference as a decline decision.
5. The wrapper does not retain an outer `ORDER BY`. The replacement must preserve the remote
   query's ordered stream; an ordinary unordered parallel table scan is insufficient.
6. This is not a general distributed subplan optimizer. The inspected finishing sites include
   the statement root, certain INSERT/CTAS sources, and certain set-operation children. An
   arbitrary remote subquery inside a mixed local/remote SELECT is not guaranteed to push.
   Parent CTE references also cause conservative rejection.
7. The pass can fold eligible scalar expressions locally. Do not describe remote pushdown as
   executing all expressions remotely, or assume preparation is the first possible binding work.

The newer `StripCatalogName` preserves every component after the catalog, including schema and
struct-field suffixes; it no longer collapses qualified columns to their last two components.
That fixes a naming hazard but does not change the consuming ownership contract above.

The preferred adapter uses `OptimizerExtension::pre_optimize_function`, which runs after binding
and mandatory aggregate lowering but before built-in logical optimizers. The exporter verifies
the plan and returns an owned SQL AST plus output fields or structured issues. Unsupported export
is an eligibility miss; an invalid plan or malformed extension result is an error. Export success
alone proves neither remote semantic compatibility nor authorization.

The engine's `disabled_optimizers='extension'` disables this adapter. Also explicitly honor
`disabled_optimizers='remote_pushdown'` in the adapter for a consistent operator escape hatch;
that flag does not automatically gate optimizer extensions. Neither flag needs to disable an
explicit `vgi_query` call.

## 3. Non-goals for the first release

No remote DDL, DML, COPY, ATTACH, session-control statements, multi-statements, raw `CONNECT`
delegation, arbitrary code execution, or unbounded streaming queries. Existing VGI write APIs
remain authoritative; this capability does not replace them.

No cross-attachment joins, upload of local temporary tables, distributed shuffle, cost-based
join placement, arbitrary mixed-query subtree extraction, or serialization of DuckDB internal
AST/logical-plan objects. No new Substrait implementation is required.

No automatic pushdown of parameters, correlated/LATERAL queries, recursive CTEs, windows,
sampling, time travel, opaque local macro/UDF dependencies, external table functions, or queries
whose order depends on an unproven physical insertion order. These can be added as independently tested profile
revisions. A remote provider's broader explicit-query language must not silently broaden the
automatic profile.

No query-result caching, split result readers, concurrent reads of one cursor, resumable query
execution after an ambiguous failure, or cross-process cursor pickling in the first release.

## 4. Separate language, semantics, and authorization

The wire contract needs three distinct checks:

- **Encoding:** `duckdb.sql.v1` means UTF-8 SQL in the declared DuckDB dialect family. It does not
  promise that every producer/consumer DuckDB version parses the same syntax.
- **Semantic profile:** `vgi.duckdb.relational.v1` defines the tested subset eligible for automatic
  execution. `vgi.remote.readonly.v1` permits explicitly requested remote SQL with provider-defined
  expression semantics, while still forbidding writes and external side effects.
- **Authority:** the actual principal, attachment, transaction, and provider policy determine
  whether this particular query may access these particular objects.

Negotiate encoding and profile independently of `vgi.filters.v2`. This is not another filter
encoding. Do not overload `filter_encodings`, `pushdown_filters`, `ScanBranches`, or
`table_function_plan` with a serialized query.

Engine version and provider build identity inform compatibility decisions but do not prove
them. A provider advertises automatic support only for tested client versions, function
implementations, type mappings, and supported session contexts. Unknown combinations decline.
Changes to that compatibility policy produce a new capability revision.

## 5. Proposed wire contract

The following records are proposed schema definitions, not currently generated dataclasses.
Use the existing Arrow single-record IPC envelope, with **typed nested structs** for new query
records, following `CatalogContentsResponse.schemas`. Do not add a separate IPC stream per object
reference or evaluation-context record. Existing attachment/transaction tokens remain binary;
output/object schemas use Arrow IPC schema serialization, not an IPC table. Null and empty are
distinct. The existing outer `CatalogAttachRequest.client_capabilities` IPC encoding is unchanged.

Route preparation, bind, and init through `VGI_MAIN_PROTOCOL` with generated name `vgi.v2` and the
selected surface version. HTTP preparation uses `{base}/vgi.v2/catalog_query_prepare`; raw
transports carry `vgi_rpc.protocol`. The SDK-owned executor name is a function argument to normal
`bind`/`init`, not a server-level reserved RPC: the `__transport_options__` routing exception must
not apply to it. Register the new method in request/response schema validation and tracing.

### 5.1 Attachment negotiation

Add nullable `query_pushdown: struct<ClientQueryCapabilities>` to `ClientCapabilities` and
nullable `query_pushdown: struct<CatalogQueryCapability>` to `CatalogAttachResult`. Null means
no query capability; decoders that allow an absent optional field use the same default. This
does not override the protocol's major/minor compatibility check. The records differ by direction:

| ClientQueryCapabilities field | Arrow type | Meaning |
|---|---|---|
| `encodings` | `list<utf8>` | Understood versioned query encodings |
| `semantic_profiles` | `list<utf8>` | Understood versioned semantics |
| `engine_version` | `utf8` | Actual client engine version/build identity |
| `max_query_bytes` | `int64` | Maximum UTF-8 SQL payload the client permits |

| CatalogQueryCapability field | Arrow type | Meaning |
|---|---|---|
| `encoding` | `utf8` | One selected offered encoding |
| `semantic_profiles` | `list<utf8>` | Nonempty intersection with client offers |
| `execution_modes` | `list<utf8>` | Subset of `explicit`, `automatic` |
| `backend_engine` | `utf8` | Provider dialect engine identity |
| `backend_version` | `utf8` | Backend version/build identity |
| `capability_revision` | `utf8` | Opaque revision of provider support and semantic policy |
| `default_evaluation_context` | `struct<QueryEvaluationContext>` | Provider defaults used by explicit mode |
| `max_query_bytes` | `int64` | Positive selected limit, no larger than the client offer |

Automatic mode requires `vgi.duckdb.relational.v1`; explicit mode requires
`vgi.remote.readonly.v1`. Unknown advertised profiles can be ignored before selection. Unknown
selected encodings, modes, or required fields are negotiation errors. Never infer automatic
support merely from `backend_engine == 'duckdb'`.

Capabilities are immutable for an attachment. A provider rolling to an incompatible revision
must reject old preparation tokens and require reattachment or normal version renegotiation;
it must not silently change semantics mid-attachment. An old revision may remain supported during
a rolling deployment. `IS_REMOTE` is an engine catalog property, separate from HTTP versus local
worker transport and separate from whether query execution is enabled.

The bound-plan adapter does not require advertising `IS_REMOTE` or `EXECUTE_QUERY_NODE` just to
trigger optimization. Preserve existing VGI lookup behavior unless a separate change justifies
the generic remote-catalog accommodations. If the optional catalog-hook implementation is used,
keep `IS_REMOTE` stable for the attachment lifetime and test its ambiguous-schema lookup effects.

### 5.2 `catalog_query_prepare(request) -> CatalogQueryPrepareResult`

This is a unary catalog RPC. It parses, authorizes, resolves, and binds the proposed query without
executing it or opening a result cursor. It may perform ordinary metadata work and bounded
backend preparation; it must not fetch query results to discover their schema.

| Request field | Arrow type | Contract |
|---|---|---|
| `attach_opaque_data` | `binary` | Existing authenticated attachment token |
| `transaction_opaque_data` | nullable `binary` | Existing transaction token; required for the initial automatic snapshot-backed profile |
| `encoding` | `utf8` | Selected attachment encoding |
| `semantic_profile` | `utf8` | One selected profile |
| `mode` | `utf8` | `automatic` or `explicit` |
| `capability_revision` | `utf8` | Must match the attachment contract |
| `query` | `utf8` | Exactly one read-only query; canonicalized by the adapter in automatic mode |
| `catalog_version` | nullable `int64` | Attachment-local metadata version; required in automatic mode, with 0 meaning unknown |
| `catalog_etag` | nullable `utf8` | Last committed-metadata validator, if known; never a data snapshot token |
| `referenced_objects` | `list<struct<QueryObjectReference>>` | Canonical object manifest; required and complete in automatic mode |
| `expected_output_schema` | nullable `binary` | Local result contract; required in automatic mode |
| `evaluation_context` | `struct<QueryEvaluationContext>` | Profile-specific evaluation context |
| `required_ordering` | `utf8` | `query_order` or `unspecified` |

Each `QueryObjectReference` contains `schema_path: list<utf8>`, `name: utf8`, `kind: utf8`, and
`expected_schema: binary`. The initial automatic profile permits `kind='table'` only. It refers
to a distinct logical catalog object, not an alias or a raw storage filename. Repeated query
aliases may refer to one manifest entry. The worker validates the query against the manifest;
the manifest is not trusted proof of what the SQL actually references. Explicit requests use
an empty manifest and resolve every object under the provider's authorized attachment namespace.

`QueryEvaluationContext` contains `timezone: utf8`, `collation: utf8`, `default_order: utf8`,
`default_null_order: utf8`, and `preserve_insertion_order: bool`. Automatic mode supplies the
effective client values, even when order defaults were expanded into SQL. The initial profile
pins other semantics-affecting engine settings to its tested defaults; the adapter declines
non-default or unknown relevant settings. Future settings require a profile revision, not an
unchecked arbitrary settings map. Explicit mode supplies the provider defaults established for
the attachment in `default_evaluation_context`. Secrets and connection credentials are not
evaluation-context fields. Use `asc`/`desc` for direction and `nulls_first`, `nulls_last`,
`nulls_first_on_asc_last_on_desc`, or `nulls_last_on_asc_first_on_desc` for NULL-order defaults.
Use `binary` for uncollated strings; additional collation/timezone contexts require provider
certification. Automatic v1 starts with binary collation and UTC context, declining other contexts
until tested. Canonical SQL still spells out each ORDER BY term's effective NULL placement.

The worker also binds table identity, virtual-column behavior, and access policy to the existing
catalog contract. Positive metadata versions are comparable only within the same attachment;
0 means unknown, not "unchanged". A known incompatible metadata generation is a decline before
acceptance, not license to resolve a different table with the same name. With version 0, validate
the referenced objects and schemas directly in the transaction at every preparation and again
as necessary before execution. An etag can help detect committed metadata changes but cannot
replace transaction-local object validation. Neither version nor etag is a data snapshot ID.

| Result field | Arrow type | Contract |
|---|---|---|
| `status` | `utf8` | Exactly `accepted` or `declined` |
| `reason_code` | nullable `utf8` | Required for decline; absent for acceptance |
| `reason_detail` | nullable `utf8` | Bounded, sanitized diagnostic; never parsed for decisions |
| `prepared_query_token` | nullable `binary` | Required for acceptance; absent for decline |
| `output_schema` | nullable `binary` | Required for acceptance; absent for decline |
| `result_ordering` | nullable `utf8` | `query_order` or `unspecified` on acceptance |
| `validated_catalog_version` | nullable `int64` | Required for accepted automatic requests; 0 remains unknown |
| `validated_catalog_etag` | nullable `utf8` | Committed-metadata validator if applicable; null when unavailable |
| `context_digest` | nullable `binary` | Required on acceptance; exactly 32 SHA-256 bytes |
| `expires_at_epoch_ms` | nullable `int64` | Required on acceptance; token expiry, not snapshot expiry |
| `estimated_rows` | nullable `int64` | Optional nonnegative advisory estimate |
| `estimated_bytes` | nullable `int64` | Optional nonnegative advisory estimate |

On decline, all acceptance-only fields and estimates are null. On acceptance, reason fields are
null, the worker must meet `required_ordering`, and the schema must satisfy the requested output
contract. An expected type the provider cannot preserve should produce `unsupported_type`, not
an accepted response that relies on the engine to coerce the result.

Define digest bytes independently of incidental IPC buffer layout. Concatenate the ASCII domain
separator `vgi.query.context.v1` followed by a zero byte; then `encoding`, `semantic_profile`,
`timezone`, `collation`, `default_order`, and `default_null_order`, in that order, each as its
four-byte unsigned big-endian UTF-8 byte length followed by those exact UTF-8 bytes; then one byte,
0 or 1, for `preserve_insertion_order`. SHA-256 that sequence. No locale conversion, case folding,
or Unicode normalization occurs during hashing. Publish cross-language golden vectors.

The digest is an equality check and diagnostic, not an authentication mechanism. Authentication
remains token sealing and the established RPC security model. The accepted token binds the SQL,
object manifest, output schema, capability revision, context, attachment, principal, and
transaction scope, including the automatic/explicit mode. The adapter verifies the returned
context digest against its request before accepting the replacement.

Initial decline codes: `unsupported_query`, `unsupported_expression`, `unsupported_type`,
`unsupported_context`, `unsupported_catalog_object`, `ordering_not_supported`,
`metadata_changed`, and `policy_declined`. These mean ordinary VGI scan execution may still be
valid. Authentication failure, forbidden object access, malformed wire data, and backend failure
are errors, not declines. The engine must not turn an access-denied error into a less restricted
local retry. `policy_declined` is an optimization-placement decision, such as declining an
expensive shape, never an authorization failure. Future well-formed decline codes can use the
same pre-execution fallback policy.

Initial proposed hard limits: 1 MiB UTF-8 query text, 8 MiB decoded request envelope, 1 MiB per
schema, 64 KiB prepared token, 1,024 distinct object references, 32 schema-path components, and
4 KiB diagnostic detail. Capability negotiation can lower the SQL limit. Apply envelope limits
before nested decoding, validate positive bounds and integer overflow, and enforce parser depth,
AST size, planning-time, and concurrent-preparation quotas independently. These are contract
limits, not justification to allocate all maxima simultaneously.

### 5.3 Reserved executor through the existing function lifecycle

Reserve wire name `__vgi_catalog_query` in the SDK dispatcher, with no user schema path and
`function_type=TABLE`. Its sole argument is `prepared_query_token: binary`. Do not publish it in
catalog listings or accept user definitions that shadow it. A reserved name is not a security
boundary: every call must validate capability, token, principal, attachment, and transaction.

The C++ SQL-facing function is `vgi_query`; the automatic adapter uses an engine-private query
scan reference. Both reach this same wire executor. The client does not receive an arbitrary
`FunctionInfo` from the worker and trust it to define the query executor's behavior.

`bind` validates the receipt and returns its exact output schema plus execution recipe in
`BindResponse.opaque_data`. It may rehydrate provider metadata but must not start the query.
Existing attachment and transaction fields in `BindRequest` must match the preparation scope.
Per-function settings/secret discovery cannot silently change that scope; the query provider
uses the already resolved attachment authority and settings. Providers needing additional
per-query semantics must negotiate them before preparation.

`init` starts one fresh execution using that recipe and the current validated transaction.
The existing global initialization response supplies the execution ID. Each new initialization
is a new execution; a prepared token is not a result-cache key or an exactly-once execution ID.

The fixed executor contract is:

- One ordered result reader; `max_workers=1`; no secondary initializations or scan splits.
- No projection, filter, dynamic/runtime-filter, sampling, row-limit, partition, or order-hint
  pushdown into the executor. Those semantics already belong to the accepted SQL. Disable their
  engine flags; reject nonempty wire hints, apart from the protocol's canonical identity/no-op
  representation of a full output projection. The worker never silently applies them twice.
- Every batch matches the accepted output schema and preserves row multiplicity. Empty batches
  are not end-of-stream unless the existing stream protocol marks completion.
- `query_order` requires batch concatenation to preserve the query's result ordering. Even
  unordered queries use one reader initially; the backend may still compute internally in parallel.
- Cancellation, terminal errors, and normal completion use the existing streaming lifecycle,
  with provider cleanup and expiry as described below.
- All VGI query-result cache lookup, storage, conditional revalidation, and partition-cache paths
  are disabled. This is an explicit bind-data property, not an assumption about naming.

No new query-data RPC is needed. New records and the preparation RPC must be in the generated
schema/type inventory. Include nested structs in cross-language schema parity; retain explicit
inventory registration for records that are independently serialized rather than structurally nested.

## 6. Automatic semantic profile

### 6.1 Initial useful subset

Target the high-value combination: base tables from one attachment, ordinary projections and
WHERE predicates, INNER/LEFT equijoins, GROUP BY/HAVING, COUNT, exact decimal SUM, MIN/MAX,
and ORDER BY with constant LIMIT/OFFSET. Ordinary nonrecursive query-local CTEs and uncorrelated
subqueries can be admitted only when their entire containing query is eligible and the binding
and ownership tests pass. Initially reject set operations for automatic mode rather than relying
on partially finished set-operation children. Expand one syntax family at a time.

The expression allowlist includes column references, typed literals, proven exact casts,
comparisons, boolean/null predicates, and individually tested arithmetic/aggregate overloads.
It is an overload/type/context allowlist, not merely a list of function names. Overflow, decimal
scale, division-by-zero, NULL, and collation behavior must match. Backend versions with different
behavior decline even if the SQL parses.

Initially transport only boolean, signed integers through 64 bits, supported fixed decimals
through precision 38, binary-collated VARCHAR, DATE, and supported microsecond TIMESTAMP results.
Admit a type only after the existing Arrow-to-DuckDB conversion is demonstrated to be lossless.
Reject unsupported result types rather than casting them into a convenient Arrow representation.
In particular, do not assume Arrow decimal128 automatically preserves DuckDB HUGEINT identity
and range. Integer SUM may produce HUGEINT; decline that result or require a query that itself
produces a supported output type. Floating-point aggregates, TIMESTAMPTZ/timezone-sensitive
functions, unsigned/128-bit integers, UUID/ENUM/custom types, nested types, and VARIANT wait for
explicit type-profile tests. Internal intermediate types also need semantic compatibility even
when they are not transported.

This deliberately narrower first profile can run useful joins and reductions. It does not
pretend that “read-only SQL” establishes engine equivalence.

### 6.2 Use the bound plan and the SQL exporter

Let ordinary binding establish types, dependencies, resolved functions, casts, and session
requirements. In the early optimizer callback, first restrict eligibility to supported read-only
statement roots whose data sources all belong to one VGI attachment. Do not perform a second
general validation bind. Ordinary binding can already perform metadata discovery and constant
evaluation; this feature must not add query execution during export or preparation.

Use `LogicalPlanSQLExporter::Export(context, candidate, options)` after the source/operator
allowlist checks. It returns an owned `QueryNode` and positional fields with `source_binding`,
semantic `type`, and optional `optimizer_type`. Use those fields to verify the result contract
and build the replacement's output mapping. Do not serialize native bindings on the wire or
assume they are stable across independent exports. Use an explicit projection to preserve the
original local result layout and column bindings; leave statement result labels unchanged.

Assign unique transport-column names through `LogicalPlanSQLExportOptions.output_names`, and
derive `expected_output_schema` from those names and the validated semantic types. Local output
labels and transport names are distinct: the former remain DuckDB's original bound labels, the
latter identify the remote result by ordinal. If this mapping cannot be preserved, decline.

VGI base-table scans must export the logical catalog table, not a private `vgi_table_scan` call
with an opaque receipt. Use the exporter's `get.GetTable()` reconstruction when it retains the
right identity; otherwise add a side-effect-free `TableFunction::to_sql` callback to the ordinary
VGI table scan. Return an owned qualified `BaseTableRef` or a structured unsupported reason.
The exporter applies projection and predicates centrally; the callback must not duplicate them.
The reserved query executor itself should decline export to prevent recursion or token leakage.

On the returned AST, remove only the proven target attachment's catalog component from resolved
base references. Preserve schema components, generated aliases, CTE scopes, system-function
qualification, and struct-field suffixes. Use the AST writer, not SQL text replacement. Validate
that every remaining data reference is in the same logical-object manifest. Reject UDFs, views,
companion sources, process-local inputs, and extension operators not admitted by the profile.
Expanded macros are not automatically safe because their original names disappeared: their
bound expressions and every source still need the same semantic/effect checks.

The exporter is deliberately not a remote-compatibility oracle. Its same-environment contract
does not prove that another DuckDB version or non-DuckDB backend implements the exported query
identically. Retain negotiated profiles, overload checks, worker preparation, and output checking.

Prefer the early callback, not an export of an arbitrarily late optimized plan. The current
exporter does not undo folded statistics, literal `now()`/settings observations, or data-derived
empty relations. It can also reject scans whose consumed projection expressions lost provenance.
Even the early callback follows binding and mandatory aggregate lowering. Therefore initial
automatic mode requires the worker to execute in the same backend transaction/snapshot assumed
by binding and any data-dependent planning. Reject incompatible context rather than switching
silently to a later export point. Nontransactional providers remain eligible for explicit mode.

The worker must implement the same **logical VGI tables**. A raw backing table is not equivalent
if normal VGI scans rename columns, add computed columns, enforce predicates, choose scan branches,
or delegate through a companion catalog. A query provider must reproduce those rules or decline
the affected object. This is especially important for row-level and column-level access policy.

`VgiRequiredFiltersOptimizer` currently checks required filters after built-in optimization.
Removing a scan in the early callback would bypass that check. Initially decline automatic
queries touching objects with `required_filters`, native-delegation placeholders, or multi-branch
markers. Supporting them later requires enforcing the same rules before replacement, not just
trusting worker acceptance. Register the query callback in a tested order relative to
`VgiMultiScanRewriter`; do not let branch expansion hide a source's original policy or attachment.

Reuse `VgiCatalog`'s existing `catalog_contents` seeds and per-kind caches during binding. Do not
fetch a whole inventory solely because a query mentions two tables. The bulk response represents
committed metadata and has no transaction token; it cannot prove the existence/schema of
transaction-local DDL. Preserve per-name transactional lookup and invalidation behavior.
Reuse version adoption, stale-snapshot retry, and conditional etag revalidation; do not introduce
a second independent query metadata cache. Version-0/no-etag catalogs retain lazy reload behavior.
`catalog_contents_attach_independent` permits metadata reuse only, never reuse of query receipts,
authorization decisions, result cursors, or data across principals.

Schema paths are component lists throughout resolution. A component containing a dot is not two
schemas. The current DuckDB 1.5 adapter's depth-one bridge must not flatten a deeper worker path.
Do not silently change the existing attachment `default_schema: str` contract in this feature;
automatic SQL uses resolved object paths and explicit mode uses the provider's declared namespace.

### 6.3 Output, ordering, and settings

Compare the prepared output to the expected transport schema and local ordinal mapping before
accepting a rewrite: column count, order, transport names, and logical types must match without
implicit casts or dropped fields. Preserve the original local result labels separately.
Define the profile's Arrow normalization explicitly; do not compare byte-for-byte IPC envelopes
or disregard type-significant metadata. A batch may not violate accepted nullability. Validate
subsequent batches as well as bind-time metadata.

The legacy `SELECT * FROM <table function>` wrapper can introduce name deduplication. The preferred
bound-plan replacement avoids that wrapper and uses unique transport names plus the local output
mapping. Duplicate local labels are supported only after tests prove they remain unchanged; until
then decline them conservatively. Explicit `vgi_query` follows normal table-function naming rules.

Reuse `ValidateWorkerBatch` and `ValidateProjectedWireBatch` before Arrow conversion, including
externalized/shared-memory batches. They already defend buffer contents and wire column types.
They do not establish result-name, collation, field-metadata, or nullability semantics, and the
global validation setting can disable them. The query adapter's exact schema/semantic checks
remain mandatory independently of that performance setting; do not equate structural validation
with query equivalence. Empty `projection_ids` means the full output schema in the current client.

Make default order direction and NULL placement explicit in canonical SQL. DuckDB exposes these
as settings, so relying on the worker's defaults can change results, especially with LIMIT.
See the official [ORDER BY documentation](https://duckdb.org/docs/current/sql/query_syntax/orderby).
An accepted ordered result must use DuckDB `FIXED_ORDER` and a single effective reader through
all initialization paths. Check parallelism after the worker's init response is applied, not
only in the initial function declaration.

Ordering is distinct from deterministic tie-breaking. A query with nonunique ORDER BY keys can
have multiple legal tie orders; do not invent a tie-breaker. Tests compare ties accordingly.
Unordered LIMIT and order-sensitive aggregates are excluded initially. A plain projection/filter
can preserve insertion order in DuckDB; the provider must not replace that with an arbitrary
backend order. The initial automatic policy skips these low-value plain-scan shapes. General
order behavior is documented in DuckDB's [order-preservation reference](https://duckdb.org/docs/current/sql/dialect/order_preservation).

Do not transplant a whole client session into the backend. Copy only negotiated evaluation
context, validate all other relevant settings against the profile, and use isolated backend
connections so concurrent requests cannot race through SET statements. Queries involving clock,
randomness, sequences, side-effectful functions, environment/connection identity, filesystem or
network access, and remote-only functions are not automatic-profile candidates. The same
side-effect restrictions also apply to explicit mode, even where expression results are otherwise
provider-defined.

## 7. DuckDB integration and the fallback boundary

### 7.1 Preferred bound plan integration

Register a 2.0-only `OptimizerExtension::pre_optimize_function`. Limit the first implementation
to complete eligible SELECT roots; explicitly recognize supported EXPLAIN/PREPARE wrappers or
decline those wrappers until their lifecycle tests pass. Do not traverse arbitrary mixed-query
subtrees, DML sources, or local joins in this milestone. The optimizer owns the original logical
plan throughout export and worker preparation.

```text
Original bound plan remains owned by the optimizer
    -> eligibility and snapshot checks
    -> LogicalPlanSQLExporter::Export
         unsupported -> return without substitution
         invalid plan / malformed callback -> error
    -> canonical AST and expected result mapping
    -> catalog_query_prepare
         declined -> return without substitution
         accepted -> build and verify an independent replacement
                         -> swap only after all checks pass
```

Export can resolve/cache logical types during verification; do not promise byte-identical native
objects after the attempt. The guarantee is no semantic or ownership-changing replacement on
decline. No moved-out original plan, no second local execution, and no catch-all fallback on errors.
Perform ordinary local optimization afterward whether the candidate was retained or replaced.
Ensure that later passes cannot duplicate a nonrepeatable remote execution or push forbidden
hints into the executor.

The **first technical gate** is a compiling prototype against `80e17fc252` proving export of a
VGI join+aggregate at this early stage, output/type/binding preservation, snapshot identity,
and successful normal optimization after a decline. The exporter accepts planned trees but
does not select a safe stage for its caller; its README explicitly calls distributed fragment
export experimental. If this gate fails for a shape, decline that shape. Do not claim all
exportable DuckDB queries are automatically remote-capable.

### 7.2 Optional parsed statement integration

If the early bound-plan route cannot meet the required coverage, a separate engine change can
add the previously proposed `TryRemoteExecute(context, const QueryNode &original)` behavior:
return an accepted `TableRef`, no replacement on decline, or an actual error. The optimizer must
retain the qualified original and call before `StripCatalogName` or any ownership transfer.
Use an explicit opt-in so existing consuming `RemoteExecute` implementations are unchanged.
Both statement and nested-node finishing sites need tests for CTE ownership and error context.

This alternative requires its own local validation bind and output preservation checks. It is
not part of the preferred initial implementation and must not run alongside the bound-plan path
for the same candidate. If neither route passes the integration gate, explicit queries can ship
without automatic pushdown. Reconstructing a consumed query from guessed qualification remains
unacceptable.

### 7.3 VGI adapter responsibilities

The preferred callback identifies attached VGI scans directly and checks the current session's
pushdown setting before making preparation RPCs. It leaves VGI's parsed-statement
`EXECUTE_QUERY_NODE`, `EXECUTE_STATEMENT`, and `CONNECT` delegation disabled. Do not store
per-session settings in shared catalog state. Adding INSERT/CTAS source pushdown later is a
separate read-source optimization; it must not imply atomic cross-database writes.

Port the execution adapter against current 2.0 APIs, not the September headers:

- `TableFunctionBindInput.table_function` is a `BoundTableFunction`; retained call arguments and
  `FunctionSignature` belong to the bound/declaration split. Use `named_argument_map_t` and the
  new serialization callback types in the 2.0 build without changing the portable wire encoding.
- Parser construction is explicit (`Parser(context)`, context plus identifier-case mode, or
  explicit options). Generated SQL must preserve resolved identifier spelling and allowed
  dialect semantics; do not use a nonexistent default constructor or inherit arbitrary dialect
  extensions into the wire profile.
- `LogicalType` now owns intrusive ref-counted type information. Use supported copying and type
  comparison APIs, including collation-sensitive checks where needed; no layout assumptions or
  serialized pointers to type information.
- Use `SupportsNestedSchemas`, `ResolveEntryName`, and schema-path APIs where appropriate, but
  never split a known component on dots. String-path lookup and already-resolved identifiers are
  different inputs. Keep the 1.5 depth-one compatibility rule in its adapter.
- Leave the executor's new `is_repeatable` callback absent/false initially. Read-only, metadata
  frozen, and one prepared recipe do not prove repeated executions return a stable result. Do
  not attach a `TableCatalogEntry` to the synthetic executor that would incorrectly classify it
  as a repeatable ordinary table scan.

Construct a dedicated query bind-data type or an explicit discriminated query variant in the
existing bind data. It owns the attachment reference, canonical preparation request, expected
schema, preparation receipt, context/capability digests, transaction scope, and query-cache
exclusion. Retaining the request permits bounded repreparation without reconstructing SQL from
an opaque receipt. Do not hide correctness state in raw AST pointers, a global map indexed by a
user-visible integer, or a `FunctionInfo` blob with unconstrained feature flags.

The SQL `vgi_query(catalog, sql)` arguments must be bind-time constants in the first implementation.
It prepares in explicit mode and learns output schema from the worker. It does not parse remote
SQL using DuckDB 1.5 and then claim to validate the remote engine's syntax. The worker performs
the authoritative read-only parse and access check. Automatic mode, by contrast, begins with a
locally valid, locally bound DuckDB query.

Register the final scan's database read dependency through DuckDB's normal statement-properties
API. Initially mark queries as always requiring rebind, including prepared SQL statements, so
later execution cannot reuse a receipt from a previous transaction, attachment generation,
metadata version, or session context. Preserve DuckDB's prepared-statement output compatibility
rules when rebinding changes schema. Rebind is not permission to change column types silently.

`Copy()` must preserve owned immutable metadata and reset execution-local state. Persistent plan
serialization must never embed reusable credentials or a live transaction/cursor. Initially
reject persistence of prepared query receipts; if engine plan serialization is required, serialize
only a versioned logical recipe and resolve/authorize/reprepare it in the destination context.
Keep this separate from in-process copies of prepared statements.

### 7.4 Precise fallback and retry policy

| Event | Automatic mode | Explicit mode |
|---|---|---|
| Capability/profile absent | Keep existing local plan | Clear unsupported-capability error |
| Local eligibility fails | Keep existing local plan, no prepare RPC | Not applicable; provider validates remote query |
| SQL exporter reports an unsupported construct | Keep original bound plan; record bounded reason | Not applicable |
| SQL exporter reports invalid bindings/types or a malformed callback | Fail; do not hide a broken native plan | Not applicable |
| Worker returns a valid decline | Keep existing local plan | Error with sanitized decline reason |
| Metadata changed before acceptance | At most one refresh/recheck, then local plan | Refresh/reprepare or report change |
| Permission/authentication error | Fail; do not retry locally | Fail |
| Invalid schema, invalid response, preparation timeout/backend error | Fail with diagnostic | Fail with diagnostic |
| Token expires or becomes invalid after replacement | Rebind/reprepare only under the same verified contract, or fail | Same |
| Init has an ambiguous transport failure | Fail; do not start another execution automatically | Same |
| Error after init or after any output | Abort stream and clean up; never restart locally | Same |

The commit point is installation of the accepted replacement, not receipt of the first result
batch. Eligibility/export misses and explicit worker declines before that point keep execution
local; actual query/protocol/backend errors do not authorize fallback. Repreparing
an expired receipt is bounded, must preserve the already bound output/context/transaction, and
does not mean executing the original query locally after failure.

Bind/preparation are repeatable because they do not execute queries; existing reconnect-and-bind
logic can be reused only under that invariant. Starting a query is a separate operation. The
current `InitRequest.execution_id` distinguishes secondary initialization and must not be
repurposed as a new client idempotency key. If reliable resumable execution is added later, give
it an explicit execution-attempt and batch-sequence contract.

## 8. Python provider and worker implementation

### 8.1 Public provider surface

Add an optional `query_provider` to the declarative catalog, absent by default. The SDK obtains
attachment-specific state through the normal catalog context rather than storing one database
connection on a class shared by every user. A proposed author-facing interface is:

```text
CatalogQueryProvider
    capabilities(attach_context, client_offer) -> capability or None
    prepare(query_context, request) -> PreparedQuery or QueryDeclined
    start(execution_context, prepared_recipe) -> serializable execution handle
    read(execution_context, handle, continuation) -> batch and next continuation or EOS
    close(execution_context, handle, reason) -> None
```

These are SDK methods, not five new wire RPCs. The SDK converts preparation outcomes into the
wire response and adapts `start/read/close` to the reserved TABLE executor. The framework owns
validation, receipt sealing, output checks, cancellation bridging, telemetry, and limits. A
provider owns dialect validation, logical-catalog equivalence, backend transaction integration,
and actual execution resources. Capability advertisement must be cheap and not execute queries.

The provider must enforce logical-table restrictions in explicit mode too: `vgi_query` is not an
escape hatch around required filters, row/column policy, or unsupported scan branches. The reference
provider should decline such objects until it can reproduce their existing contract safely.

`PreparedQuery` contains a portable preparation recipe and output metadata, not a live result
cursor. Recipes may be self-contained sealed data or identifiers into shared, bounded TTL
storage. Large SQL recipes will need storage because token size is capped. No new release RPC
is necessary for recipes: abandoned EXPLAIN/PREPARE attempts expire without executing anything.

Receipt expiry gates starting a new execution. It does not interrupt an already accepted running
execution, whose owner handle has its own deadline and idle lease. Transaction termination,
attachment revocation, and authorization changes can invalidate that handle independently.

Register one SDK-internal executor and route it using the current validated attachment. Do not
dynamically generate a worker function class for each SQL string. The reserved dispatch path
must work in `_resolve_function` during initial bind **and** HTTP producer rehydration, not only
in the first `Worker.bind` call. It must not leak into ordinary global function registration.

### 8.2 Cross-process execution is a launch requirement

The current Python `TableProducerState` serializes user state only when it has an appropriate
codec; otherwise HTTP rehydration falls back to `initial_state()`. A Python database cursor in
generator state can therefore restart the query after a worker/process change. Reusing the
existing generator machinery without fixing this ownership model is incorrect.

The query executor's serialized state should contain only a handle/owner locator, attachment and
execution identifiers, continuation/batch position, and terminal state. Never pickle a connection,
cursor, C pointer, or process-local registry reference as a cross-worker execution handle.

For the reference DuckDB-backed provider, use a bounded execution-owner service/actor holding
the connection and cursor. HTTP turns route through the handle to that owner; a lost owner fails
the stream, it does not cause another process to rerun SQL. For in-process/subprocess transport,
the same abstraction can have a local owner with lifetime tied to the execution. A provider that
cannot route to a cursor owner may materialize ordered Arrow fragments to shared storage and
stream them by position instead; document its latency and storage tradeoff.

The existing `vgi/transactor/` is the first reuse candidate, not a reason to build an unrelated
second connection manager. `TransactorImpl` already owns databases by attachment, cursors by
transaction, per-transaction locks, and Arrow-producing scans. Extend that ownership abstraction
behind the provider where appropriate. Its current `_ScanState` holds an in-process Arrow reader;
it is not itself a portable HTTP continuation, and its public `scan` method is not a general
read-query prepare/execute contract. Private owner RPCs may need extension even though the public
VGI data plane remains TABLE streaming.

The checked-in transactor also depends on the VGI DuckDB-Python fork's `subcursor()` to share a
transaction between read cursors; its compatibility shim explicitly says stock DuckDB/haybarn
builds do not yet provide that API. Gate that implementation on a tested build or a different
proven transaction-owner implementation. Do not claim that installing the ordinary optional
DuckDB package supplies this behavior. Network routing to a shared owner is also additional work,
not a property supplied by an in-process transaction dictionary.

Preparation must use genuine parse/bind/describe functionality. Do not generalize the existing
transactor's table-schema `SELECT ... LIMIT 0` execution into a supposedly execution-free query
prepare. A backend's lazy relation metadata API may be usable after validation, but conversion
to an Arrow reader or result table starts execution and belongs in `start`, not `prepare`.

At most one read is in flight for an execution. Apply bounded batch sizes, backpressure, owner
quotas, and idle/total lifetimes. Where the existing transport can replay a continuation, either
replay the identical previously produced batch from a bounded acknowledgement buffer or reject
the replay cleanly. Never advance a cursor a second time under the same continuation and call
that successful replay. The first release does not promise recovery from ambiguous stream loss.

`close` is idempotent and runs on EOS, cancellation, error, transaction end, and expiry. Cancellation
propagates to the backend's interrupt mechanism and releases the read transaction/cursor. Existing
`on_cancel` is best effort, so owner leases/TTL cleanup are still required for disconnected clients.
Use cancellation-aware blocking I/O; a serial result reader must not serialize cancellation behind
an indefinitely blocked fetch. Coordinate owner lifetime with connection-pool return.

Reuse the extension's `VgiCancelDispatcher` and prefetch-slot ownership rather than create another
destructor-side network path. Early-exit cancellation transfers ownership off-thread; a refused
enqueue drops the connection, not returns it to the pool. Test saturation, shutdown, in-flight
prefetch, disabled cancellation, and the WASM explicit-start path. A query executor must never
pool an incomplete result stream simply because cancellation is disabled or unavailable: close
or poison it and let the owner lease expire. Reuse the scan's new interruption checks, and the
Python client's early-close cleanup, without treating either as a remote deadline guarantee.

### 8.3 Transactions and freshness

For a transactional catalog, preparation and execution use the same authenticated VGI transaction
scope; all query reads see the same backend transaction/snapshot as ordinary catalog reads in
that scope. A fresh independent backend connection is not acceptable unless it joins that exact
transaction/snapshot. This may require the same owner service used by the catalog's transaction
implementation. Validate this integration rather than assuming that echoing an opaque token
establishes a shared transaction.

Multiple query scans can exist in one local statement or transaction, including two explicit
`vgi_query` calls. An owner must not let the second cursor reset the first cursor's connection.
Use backend-supported independent cursors within the same transaction, serialize/materialize
under bounded policy, or reject the unsupported execution context. Do not solve this by opening
an unrelated transaction on a second connection. A provider without correct transaction support
must decline that context rather than advertise snapshot equivalence.

For a nontransactional catalog, the provider must still execute the query with the consistency
its backend promises for one statement. It must not advertise multi-statement snapshot isolation.
Such providers can implement explicit queries, but the initial automatic profile declines their
context because it cannot prove a shared binding/planning/execution snapshot. Capability negotiation
should omit automatic mode for these providers rather than issuing predictable failing preparations.
Concurrent metadata changes between prepare and execution are revalidated before starting; an
incompatible schema/policy change fails rather than returning differently interpreted columns.

Explicit and automatic execution do not bypass the attachment's resolved data version. Time
travel remains excluded until query-level and per-table snapshot bindings have a defined contract.
`catalog_version_frozen` freezes metadata, not table contents. Neither catalog metadata etags nor
versions validate a query result. The current extension also isolates caches per database and keys
secret-dependent results with credential fingerprints/HMACs; those improvements establish identity
isolation, not query-data freshness. Query scans must still bypass lookup, store, revalidation,
and partition-result caches. Reuse the per-database owner for any future query caches rather than
introduce a process-global singleton.

## 9. Security and operational controls

Parse exactly one statement using the provider's real parser or prepared-statement API. A SELECT
prefix check, keyword blacklist, or regular expression is not a read-only validator. Inspect
nested statements, CTE bodies, table functions, macros, and callable overloads too. Use backend
read-only execution and least-privileged credentials as additional enforcement, not replacements
for logical catalog authorization. Read-only database mode alone does not prevent filesystem,
network, extension-loading, or side-effectful function access.

The attachment is the namespace boundary. Fully qualified references to another database, system
objects exposing unauthorized metadata, external paths, and hidden backing tables remain forbidden
unless intentionally exposed through the authorized logical catalog and allowed by the profile.
SQL identifiers are quoted through the engine writer; SQL text is never shell-interpolated.

Receipts and execution handles are unforgeable, bounded, expiring, and bound to principal,
attachment generation, policy revision, transaction, and context. Validate on prepare, bind,
init, read, and cancellation as appropriate. Do not rely on handle possession alone when the
authenticated caller changes. Apply existing token-sealing/storage machinery and review scope
checks for the new dispatcher path explicitly.

Use the established `CallContext.auth` identity and attach-token binding. HTTP now accepts sealed
grants and `resolve_token` bearers through the existing authentication chain; do not add a second
query-specific bearer or identity protocol. A query preparation receipt is not an identity grant.
Capture and revalidate effective authority, including relevant grant restrictions/policy revision,
not just a principal string. The same principal with narrower authority must not redeem a receipt
prepared with broader access. Actual object authorization remains the provider's responsibility.

Declared secret ATTACH options remain the credential input. Preserve their redaction and existing
HMAC-based cache identities; do not add credential values to SQL, evaluation context, diagnostics,
or native serialized plans. The explicit API takes an attached catalog name, not an arbitrary
LOCATION. Reuse its already authorized connection path and existing `vgi_allowed_transports`
policy. Current policy deliberately allows previously attached catalogs to continue after
narrowing; do not silently change that rule or expose a new attach/transport bypass through queries.

Remote SQL can contain personal data and literals. Default EXPLAIN/logging shows a redacted shape,
profile, attachment alias, query fingerprint, acceptance reason, and expected result schema,
not raw SQL or token bytes. Do not use high-cardinality SQL fingerprints as metric labels. An
explicit privileged diagnostic option may expose SQL locally; it must not expose credentials.

Execution deadlines use the remaining query budget, not a fresh full timeout at each stage.
Define how the existing RPC cancellation/deadline mechanism reaches both preparation and the
cursor owner; if it cannot carry remaining budget, add that transport field explicitly before
claiming end-to-end deadlines. Cap worker planning time independently. DuckDB's local execution
timeout alone cannot reliably interrupt all remote planning or a blocked network call.

Bound per-principal concurrency, preparation storage, cursor count, buffered result bytes, spill
space, and backend memory. Remote execution changes where resource costs occur, even for SELECT.
Attachment policy may decline expensive shapes without disguising actual backend failures as
capability misses.

## 10. Planning policy and observability

Propose a `vgi_query_pushdown` engine setting with `off`, `auto`, and `require` values. Initially
default to `off` while compatibility tests and a reference provider mature. `auto` tries only
the tested subset and otherwise keeps normal VGI scans. `require` is a development/diagnostic
mode: a detected single-VGI-catalog candidate must be accepted or produce an actionable error;
it is not an instruction to push writes or mixed-catalog queries. Queries with no such candidate
are unaffected. Explicit `vgi_query` is an intentional separate entry point.

Declare this session setting in `vgi/src/vgi_settings.json` and regenerate with
`scripts/generate_settings.py`, using the existing flat-settings validation and scope rules.
Do not add an unvalidated registration or a second stored setting on `VgiCatalog`.

The first automatic policy is capability/shape based, not cost based. Prefer eligible joins,
aggregations, DISTINCT once supported, and ordered reductions; skip trivial scans. Keep estimates
optional and explicitly advisory. An early bound-plan extension is not a full distributed cost
model, and incomparable local/backend cost units should not decide correctness-sensitive rewrites.
Benchmark before enabling automatic mode by default for a certified provider.

Local EXPLAIN may prepare but must not execute the remote SELECT. Show a `VGI_REMOTE_QUERY` node
with provider/profile, redacted operation summary, result columns, order guarantee, and optional
estimates. EXPLAIN ANALYZE executes normally and adds preparation time, time to first batch,
execution time, rows/bytes received, and cancellation/cleanup outcomes. Query declines appear in
debug diagnostics without flooding ordinary successful query logs.

Useful counters: candidates, local ineligibility by bounded reason, worker declines by bounded
reason, accepted preparations, started/completed/cancelled/failed executions, schema violations,
token expiry, active owners, and transferred rows/bytes. Compare actual traffic with pushdown
disabled; do not invent a “rows avoided” metric when the baseline is unknown.

## 11. Implementation map and sequencing

Start two tracks: prove the early bound-plan exporter against the current 2.0 headers, and define
the portable protocol/execution path. The first usable release remains explicit queries on 1.5;
automatic mode depends on the exporter/snapshot/policy gate, not on optimistic API assumptions.
The seven implementation units below are milestones, not claims that any query API is already built.

### PR 1: contract and executable corpus

Pin the `vgi.v2` / proposed `2.2.0` contract and turn the reviewed wire portion into a normative
optional-extension specification. Specify typed nested records, metadata version-0/etag rules,
and the initial automatic snapshot requirement. Add accepted and declined fixtures, exact Arrow
schemas, malformed payloads, capability intersection cases,
and a language-neutral corpus under `conformance/catalog-query-v1/`. Pin semantic profile cases
and type mappings before advertising automatic support. Add a reference stub provider that can
accept, decline, and return deliberately malformed responses without executing a database.

### PR 2: Python protocol, dispatcher, and generated schemas

Primary files: `vgi/protocol.py`, `vgi/catalog/catalog_interface.py`, `vgi/catalog/descriptors.py`,
`vgi/worker.py`, `vgi/invocation.py` where necessary, and a new `vgi/catalog/query.py` module.
Add capability/prepare dataclasses, RPC method, provider interface, reserved name protection,
and consistent authorization/rehydration routing. Keep general TABLE execution unchanged for
ordinary functions.

Update `vgi/codegen/_common.py`'s inventories and generated protocol/schema artifacts for C++,
TypeScript, Rust, Go, Java, and C#. Regenerate the existing typed model/builders as well as schemas,
protocol versions, and protocol-name artifacts; preserve the routing name `vgi.v2`. Extend
inventory, nested-schema/type parity, protocol-name/version, catalog-auth-binding, and client-catalog
tests. Add a Python client query convenience method that uses the same preparation and TABLE path;
do not bypass the public client lifecycle with a private standalone HTTP call.

Use `scripts/regen_generated.py` for the coordinated generation workflow rather than redirecting
individual generators over checked-in files. Include `tests/test_generated_cpp_protocol_name.py`
and `tests/test_generated_csharp.py` alongside the existing schema/version drift tests.

### PR 3: reference provider and execution ownership

Implement bounded preparation storage, serializable execution handles, owner routing, backend
interrupt/cleanup, and transaction reuse, starting from `vgi/transactor/server.py` and its
transaction-cursor model where it fits. Any private transactor protocol/client extensions remain
behind the provider abstraction. Provide an opt-in DuckDB-backed example using the
SDK's existing optional DuckDB-compatible engine resolver. Do not make DuckDB a mandatory
dependency for all VGI workers. Advertise explicit support first; certify automatic support only
after differential conformance against the actual client engine builds.

### PR 4: DuckDB 1.5 explicit adapter

In the `vgi` repository, extend capability construction/parsing in `src/vgi_rpc_types.cpp` and
`src/include/vgi_catalog_metadata.hpp`, with attachment state in
`src/include/storage/vgi_catalog.hpp` / the storage implementation. Route the preparation method
through `src/vgi_catalog_api.cpp`, `src/include/vgi_catalog_rpc.hpp`, pooled unary RPCs, and
`VGI_MAIN_PROTOCOL`; register generated schema validation in `src/vgi_schema_registry.cpp`.
Register `vgi_query` in `src/vgi_extension.cpp` and implement a dedicated query binder/scan adapter.
Reuse the connection/Arrow scan lifecycle from `src/vgi_function_connection.cpp` and
`src/vgi_table_function_impl.cpp`, batch guards in `src/vgi_batch_validation.cpp`, and cleanup in
`src/vgi_cancel_dispatcher.cpp`, with explicit cache exclusion and fixed execution flags. Reuse
`VgiCatalog` metadata seeds/revalidation rather than introducing another inventory loader.
Audit protocol routing, reconnect behavior, `Copy()`, serialization, transaction dependencies,
secret options, grants, and LOCATION policy. Use the generated settings registry for new controls.

This delivers useful protocol capability independently of the DuckDB 2.0 port. Test it against
the existing scan path and all supported worker transports. Do not retrofit an unrelated
optimizer extension into 1.5 as part of this milestone.

### PR 5: DuckDB 2.0 compatibility and export gate

Run the feasibility spike at the start, then land the proven adapter foundations here. Compile
against `duckdb-3/duckdb` at `80e17fc252`: update the declaration/bound-function split, signature
and argument APIs, explicit parser contexts, type ownership, and serialization callbacks in an
isolated 2.0 compatibility layer. Add VGI table-source SQL reconstruction and structured export
diagnostics. Leave the reserved executor nonrepeatable and nonexportable by default.

Prove whole-root join+aggregate export before built-in optimization, unchanged local behavior on
export/preparation decline, exact output remapping, and transaction snapshot reuse. Test callback
order relative to multi-scan rewriting and required-filter enforcement. Porting the VGI execution
path to the 2.0 API is real work even though the protocol remains portable.

Only if that gate demonstrates a specific unresolvable limitation should a separate engine PR
implement the optional prepare-before-rewrite catalog hook from section 7.2. The October exporter
means a core change is no longer the assumed critical path.

### PR 6: DuckDB 2.0 automatic adapter

Add a separate version-specific adapter, for example
`src/storage/vgi_remote_query.cpp` plus its header. Register the early optimizer callback and
implement bound-source eligibility, SQL export, canonical name/type/context validation, prepare
RPC, exact output mapping, and verified plan replacement. Start with whole SELECT roots, no
required-filter/branch placeholders, and a shared backend transaction. Reuse execution from PR 4
and the compatibility/export layer from PR 5. Keep version-dependent DuckDB headers/hooks outside
the portable VGI wire layer and the 1.5 build. Do not enable the consuming remote-catalog pass too.

### PR 7: hardening, documentation, and staged enablement

Run semantic, cross-process, transaction, security, and fault-injection suites. Document the
provider contract, opt-in catalog example, `vgi_query`, EXPLAIN, decline reasons, and rollout
settings. Publish compatibility claims for tested engine/provider combinations, not just a
minimum semver. Enable `auto` by default only for certified providers after performance evidence.

## 12. Required verification

### Protocol and semantic conformance

- Capability absent, empty intersections, unknown selected IDs, version mismatch, size limits,
  malformed nested structs/schema IPC, duplicate/invalid fields, null versus empty, and invalid
  status unions. Check typed models/builders as well as schemas across all six worker SDKs.
- `vgi.v2` routing on raw transports and HTTP; new-method schema registration; protocol-name and
  surface-version mismatch; the reserved executor still using normal routed `bind`/`init`.
- Bulk metadata enabled/disabled, version 0 with/without etag, stale snapshot retry, etag
  not-modified, transaction-local DDL, and two attachments with coincidentally equal versions.
- Prepared token replay across users, attachments, transactions, capability revisions, and expiry;
  reserved-executor calls without negotiation; user definitions attempting to shadow the name.
- Exact result schema and row multiplicity; empty/all-NULL results; zero-row grouped versus
  ungrouped aggregates; duplicate joins; LEFT JOIN null extension; decimal boundaries and overflow.
- ORDER BY default overrides, ascending/descending combinations, NULL placement, multiple batches,
  constant LIMIT/OFFSET, nonunique ties, quoted aliases, duplicate names, and non-ASCII identifiers.
- Quoted schema components containing dots, different local/remote default schemas, shadowed table
  names, same-URL distinct attachments, views/companions, and logical-versus-backing-table mismatch.
- Unsupported parameters, volatile/UDF/macros, nested write constructs, external table functions,
  unsupported types/settings, CTE scope, local correlations, and two-catalog joins decline safely.
- Remote exception versus legitimate decline: only the latter follows the local path.
- Same principal with narrowed grant authority; secret ATTACH options; token/SQL redaction;
  new query APIs cannot bypass LOCATION restrictions or replay authorization from metadata caches.

Run each accepted automatic query against an immutable fixture with pushdown `off` and `auto`;
compare output names/types and result bags or ordered sequences as specified by the query. For
nonunique order keys, validate the permitted tie groups rather than demanding an arbitrary
identical tie order. Add metamorphic and generated query cases to catch missing overload checks.
Use instrumentation to prove accepted queries execute exactly once and declined queries never
start remotely. On mutable data, use a shared pinned snapshot when comparing paths.

### Engine and lifecycle conformance

- Early bound-plan export on the actual 2.0 headers; unsupported-export decline versus invalid
  binding/type error; exact semantic/optimizer-type mapping; unchanged local result labels.
- Constant/settings/statistics folding and snapshot changes; reject a provider unable to share
  the planning transaction. Never fall back to late optimized SQL as an unvalidated workaround.
- Required-filter rejection survives the new callback; native-delegation and multi-branch markers
  are not hidden; scalar/aggregate and table-buffering rewrites do not acquire new bypasses.
- `TableFunction::to_sql` does not execute, duplicate filters, or serialize receipts; the query
  executor remains nonrepeatable and cannot recursively export itself. Respect both `extension`
  and `remote_pushdown` optimizer opt-outs.
- EXPLAIN and PREPARE do not execute; EXECUTE rebinds under changed transaction/settings/metadata;
  persistent plans do not retain live receipts; detach/reattach invalidates old attachment handles.
- Set `threads=1` and a larger value; ordered streams stay ordered and one effective VGI reader
  remains after init. Backend parallelism does not produce multiple client cursor consumers.
- HTTP prepare on process A, bind on B, and successive reads on C/D do not restart execution.
  Owner loss fails safely; replayed continuation never skips or duplicates acknowledged rows.
- Cancellation during prepare, init, blocked fetch, and after partial output; idle abandonment,
  normal EOS, transaction commit/rollback, and double-close all release resources.
- Cancel-dispatcher refusal/shutdown, WASM explicit start, disabled cancellation, and in-flight
  prefetch never return an incomplete query connection to the pool. Test Python client early close.
- Malformed buffers and wrong wire types through normal/externalized/shared-memory paths; verify
  mandatory query-contract checks still run when general batch validation is configured `none`.
- Transient bind reconnect does not execute twice; ambiguous init and post-output failures never
  trigger local replay or a second remote query. Verify the actual RPC client's retry settings.
- Same transaction observes its own prior writes; separate transactions do not share connection
  state; nontransactional catalogs do not claim snapshots; metadata changes cannot alter types
  between accepted schema and streamed batches.
- Cache enabled globally still produces no query cache lookup/hit/store/revalidation/partition
  activity. Existing ordinary-table cache/filter behavior remains intact.

Prefer DuckDB sqllogictests for adapter behavior and Python tests for provider/protocol contracts.
Use generated cross-language fixtures to check every SDK, not Python serialization alone. Full
engine builds belong to the implementation milestones; this document itself changes no runtime.

### Performance acceptance

Measure aggregate-only, same-catalog join+aggregate, ordered top-K, plain scans, unsupported mixed
queries, and repeated prepared statements. Include both localhost/subprocess and realistic HTTP
latency, and small/large result sets. Report total latency, preparation overhead, time to first
batch, transferred rows/bytes, client/worker CPU and memory, and cleanup latency.

The primary success criterion is that accepted reductions transfer the final result, not all
input rows, with identical semantics. Also quantify the extra metadata/preparation round trips
and the regression budget for declined or small queries. Do not gate shipment on an invented
universal speedup percentage; establish budgets from these measurements before default enablement.

## 13. Compatibility, alternatives, and launch gates

The Python SDK and extension currently declare **2.1.0** under **`vgi.v2`**. The query capability
and new RPC target the next coordinated additive surface revision, **2.2.0**, subject to release
coordination if another feature claims that revision first. Do not append them silently to
2.1.0 or rename the protocol to `vgi.v2.2`.

The [reporting SQL binding](../design/reporting-protocols/README.md#scope-and-compatibility)
now exposes reads only, with mutations on RPC. It uses existing VGI facilities and does not
reserve a surface revision for SQL execution context or depend on query pushdown. Future SQL
mutation support, if needed, requires its own design. A reporting source's attachment reference
still does not authorize merging distinct live VGI attachments in query pushdown.

The framework enforces exact major/minor compatibility. Some catalog-item decoders tolerate
absent optional fields by name, but the generated request/response validators also compare schema
shape; this is not general cross-version interoperability. Update the SDKs, typed models, generated
schemas, C++ protocol identifiers, and integration fixtures together. Capability absence means
unsupported query execution only after surface-version compatibility is established. A rolling
deployment needs matched endpoints/releases or an explicit version-routing strategy; retaining
the same major protocol name alone does not make 2.1 and 2.2 peers interoperable.

Wire version, query-profile version, SDK package version, and DuckDB engine version remain
independent. The feature belongs to the VGI 2 family even though its proposed wire release is 2.2.

Alternatives considered:

- **Only a hidden SQL table function:** sufficient for explicit execution, but lacks a first-class
  decline/result-contract decision for safe automatic substitution. Keep it as the data plane,
  not the entire protocol design.
- **New prepare/execute/fetch/cancel wire family:** offers control but duplicates existing VGI
  streaming, Arrow, authentication, cancellation, and transport work. Not needed for this release.
- **Worker-selected arbitrary executor `FunctionInfo`:** flexible but exposes many irrelevant
  flags and permits split/filter/order combinations that weaken query guarantees. A fixed
  reserved executor has a much smaller conformance surface.
- **DuckDB binary AST/logical-plan serialization:** tightly couples worker and client internals;
  unsuitable as VGI's language-neutral wire contract.
- **Always require a new DuckDB remote-catalog hook:** no longer the first choice now that a
  bound-plan exporter and optimizer-extension ownership can support a non-destructive attempt.
  Retain the hook as a conditional alternative with separate tests, not an assumed prerequisite.
- **Export the final optimized plan without checking context:** can ship folded observations from
  another snapshot or lose source provenance. Use the early-stage/snapshot gate instead. The new
  exporter reduces reconstruction work; it does not make arbitrary distributed plans safe.
- **Substrait or a new engine-neutral relational IR immediately:** possible future encodings,
  but much larger than the required read-query capability and does not eliminate type/function
  semantics or authorization problems. Leave encoding negotiation extensible.
- **Automatic retry locally on any remote error:** can duplicate execution, change snapshots,
  leak side effects through functions, or hide permissions and backend failures. Reject it.

Launch gates, not optional follow-up work: exact supported-type mappings; early-stage SQL export
and safe decline ownership in the target DuckDB build; preserved required-filter/branch policy;
logical-table authorization equivalence; ordered streaming under
parallel client settings; process-safe execution ownership; transaction integration; no accidental
result cache use; and coordinated wire-version/codegen validation.

Deferred design work: parameter binding; additional SQL/function/type profiles; remote writes;
time travel; nested catalog views; cost-based placement; arbitrary partial subplans; query-result
cache validators tied to data snapshots; split result streams with explicit global-order and
partition semantics; and durable resumable execution with acknowledged batch IDs. None should
be advertised through the initial capability before its contract and tests exist.

## 14. Source anchors

All local paths below are relative to the repository named; commit identities are recorded at
the top so future API drift is visible.

| Repository / source | Relevant behavior |
|---|---|
| DuckDB: `src/include/duckdb/catalog/catalog.hpp` | Remote capabilities, execution hooks, support callbacks |
| DuckDB: `src/optimizer/remote_pushdown_optimizer.cpp` | Single-catalog analysis, finishing sites, catalog stripping, wrappers, CTE restrictions |
| DuckDB: `src/planner/planner.cpp`, `src/optimizer/optimizer.cpp` | Pre-binding invocation and optimizer gates |
| DuckDB: `src/include/duckdb/function/table_function.hpp` | Table-function binding, order and parallelism controls |
| DuckDB: `src/include/duckdb/optimizer/optimizer_extension.hpp`, `src/optimizer/optimizer.cpp` | Pre-optimizer callback ownership, ordering, and optimizer opt-outs |
| DuckDB: `src/planner/sql_export/README.md`, `src/include/duckdb/planner/logical_plan_sql_exporter.hpp` | Export contract, folded observations, experimental fragment limits, result bindings/types |
| DuckDB: `src/planner/sql_export/table_function_sql_export.cpp` | Source callbacks, logical-table reconstruction, and unsupported-source reasons |
| DuckDB: `src/include/duckdb/parser/parser.hpp`, `src/include/duckdb/common/types.hpp` | Explicit parser contexts and intrusive logical-type ownership |
| DuckDB: `src/planner/logical_operator_repeatability.cpp` | Conservative executor repeatability and ordinary table classification |
| vgi-python: `vgi/protocol.py` | Client capabilities, bind/init requests, TABLE state rehydration, wire versioning |
| vgi-python: `vgi/catalog/catalog_interface.py` | Attachment metadata, schema paths, function and scan contracts |
| vgi-python: `vgi/catalog/descriptors.py`, `vgi/worker.py` | Declarative catalog construction, function routing, attachment context |
| vgi-python: `vgi/transactor/server.py`, `vgi/transactor/_duckdb_compat.py` | Existing transaction-owner model, Arrow scans, fork-specific shared-transaction cursors |
| vgi-python: `vgi/codegen/_common.py`, `scripts/regen_generated.py` | Schema/type inventories and coordinated generation, including nested records and C# |
| vgi-python: `vgi/auth.py`, `vgi/client/client.py` | Effective identity/grant integration and early-close cleanup |
| vgi: `src/include/storage/vgi_catalog.hpp`, `src/storage/vgi_transaction.cpp` | Catalog state and transaction lifetime |
| vgi: `src/storage/vgi_catalog.cpp`, `src/vgi_catalog_api.cpp` | Bulk metadata seeds, etag/version adoption, and pooled catalog RPC validation |
| vgi: `src/include/vgi_catalog_metadata.hpp`, `src/include/vgi_rpc_client.hpp` | Attachment capability records and generated protocol routing |
| vgi: `src/storage/vgi_table_entry.cpp`, `src/storage/vgi_table_function_set.cpp` | Existing catalog scan binding and metadata |
| vgi: `src/vgi_function_connection.cpp`, `src/vgi_table_function_impl.cpp` | Bind/init transport, copies, ordering, result-cache integration |
| vgi: `src/vgi_batch_validation.cpp`, `src/vgi_cancel_dispatcher.cpp` | Reusable batch guards and off-thread stream teardown |
| vgi: `src/vgi_extension.cpp`, `src/vgi_multi_scan_rewriter.cpp` | Required-filter checks, branch rewriting, and per-database cache ownership |
| vgi: `src/vgi_settings.json`, `scripts/generate_settings.py`, `src/vgi_location_policy.cpp` | Generated setting validation and existing-attachment transport policy |

Related design documents: [VGI 2.0 audit](vgi-protocol-proposed-changes.md),
[filter-v2 specification](vgi-filter-encoding-v2-spec.md), and
[DuckDB filter adapter](vgi-duckdb-filter-adapter.md). Query pushdown complements those scan-level
contracts; it does not change their meaning.
