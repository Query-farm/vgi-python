# VGI 2.0 catalog query pushdown

**Status:** proposed implementation design, not an implemented or normative wire contract.
**Date:** 2026-09-13.
**Scope:** optional, read-only catalog query execution; Python SDK; DuckDB 1.5 explicit-query adapter;
DuckDB 2.0-development automatic-pushdown adapter.

This design is based on `vgi-python` commit `4c5c3b1`, the DuckDB extension in the sibling `vgi`
repository at `e48d659`, and the local DuckDB development tree at `c6032fbf3d`. “DuckDB 2.0” below
means that inspected development API, not a claim about an already released, stable upstream API.
The Python protocol already declares `2.0.0`; the extension's DuckDB 1.5 dependency is a separate
version axis. All new identifiers, APIs, settings, and signatures in this document are proposals.

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

For automatic mode, require a prepare-before-rewrite integration seam in DuckDB. The inspected
hook consumes the original query and does not have a supported decline path. Do not ship
best-effort automatic fallback on top of that behavior.

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
    -> local eligibility, name resolution, and output-contract checks
    -> catalog_query_prepare
         declined -> original DuckDB plan -> existing VGI scans/filter pushdown
         accepted -> one reserved VGI query scan
                         -> bind -> init -> ordered Arrow batches -> cleanup
```

## 2. What the current code provides, and what it does not

| Layer | Available now | Required addition |
|---|---|---|
| DuckDB development catalog API | Remote capabilities, syntax support callbacks, query-to-`TableRef` replacement | Safe preparation/decline seam and VGI-specific eligibility checks |
| VGI attachment | Client capabilities, attachment/transaction tokens, catalog metadata version | Query capability negotiation |
| VGI function execution | Dynamic bind schema, opaque bind state, TABLE streaming, cancellation hooks | Reserved prepared-query dispatch and stricter query execution invariants |
| Python worker | Declarative catalogs, scoped functions, state codecs, storage abstractions | Query-provider interface and transport-safe execution state |
| Python transactor | Attachment-scoped databases, transaction cursors, Arrow scan producers | Reusable query-owner integration, prepare-only metadata, explicit handle lifecycle |
| DuckDB VGI extension | Catalogs, Arrow scans, transaction integration, filter pushdown, result cache | Query binder, query-specific bind data, semantic adapter, cache exclusion |

Important limits of the inspected DuckDB implementation:

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

The engine-wide `disabled_optimizers='remote_pushdown'` setting remains a diagnostic escape hatch
for the automatic path. It does not need to disable explicit `vgi_query` execution.

## 3. Non-goals for the first release

No remote DDL, DML, COPY, ATTACH, session-control statements, multi-statements, raw `CONNECT`
delegation, arbitrary code execution, or unbounded streaming queries. Existing VGI write APIs
remain authoritative; this capability does not replace them.

No cross-attachment joins, upload of local temporary tables, distributed shuffle, cost-based
join placement, arbitrary mixed-query subtree extraction, or serialization of DuckDB internal
AST/logical-plan objects. No new Substrait implementation is required.

No automatic pushdown of parameters, correlated/LATERAL queries, recursive CTEs, windows,
sampling, time travel, local macros/UDFs, external table functions, or queries whose order depends
on an unproven physical insertion order. These can be added as independently tested profile
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
Use the existing Arrow single-record IPC convention. Nested records represented as `binary`
contain exactly one record batch with one row and the declared record schema. Schemas use the
existing Arrow IPC schema serialization, not an IPC table. Null and empty are distinct.

### 5.1 Attachment negotiation

Add nullable `query_pushdown: binary` to both `ClientCapabilities` and `CatalogAttachResult`.
Absent/null means no query capability. The inner records differ by direction:

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
| `default_evaluation_context` | `binary` | `QueryEvaluationContext` containing the provider defaults used by explicit mode |
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

For a VGI virtual catalog participating in the remote optimizer, set `IS_REMOTE` consistently
for the attachment lifetime so DuckDB's remote-catalog count and lookup behavior remain valid.
Do not toggle it when a session enables/disables the optimization. Review the changed ambiguous
schema-lookup behavior as part of the adapter's name-resolution tests.

### 5.2 `catalog_query_prepare(request) -> CatalogQueryPrepareResult`

This is a unary catalog RPC. It parses, authorizes, resolves, and binds the proposed query without
executing it or opening a result cursor. It may perform ordinary metadata work and bounded
backend preparation; it must not fetch query results to discover their schema.

| Request field | Arrow type | Contract |
|---|---|---|
| `attach_opaque_data` | `binary` | Existing authenticated attachment token |
| `transaction_opaque_data` | nullable `binary` | Existing transaction token, when applicable |
| `encoding` | `utf8` | Selected attachment encoding |
| `semantic_profile` | `utf8` | One selected profile |
| `mode` | `utf8` | `automatic` or `explicit` |
| `capability_revision` | `utf8` | Must match the attachment contract |
| `query` | `utf8` | Exactly one read-only query; canonicalized by the adapter in automatic mode |
| `catalog_version` | nullable `int64` | Client's metadata version; required in automatic mode |
| `referenced_objects` | `list<binary>` | Canonical object manifest; required and complete in automatic mode |
| `expected_output_schema` | nullable `binary` | Local result contract; required in automatic mode |
| `evaluation_context` | `binary` | Profile-specific `QueryEvaluationContext` |
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
catalog metadata contract. Metadata version mismatch is a decline before acceptance, not license
to resolve a different table with the same name. Catalog version is **not** a data snapshot ID.

| Result field | Arrow type | Contract |
|---|---|---|
| `status` | `utf8` | Exactly `accepted` or `declined` |
| `reason_code` | nullable `utf8` | Required for decline; absent for acceptance |
| `reason_detail` | nullable `utf8` | Bounded, sanitized diagnostic; never parsed for decisions |
| `prepared_query_token` | nullable `binary` | Required for acceptance; absent for decline |
| `output_schema` | nullable `binary` | Required for acceptance; absent for decline |
| `result_ordering` | nullable `utf8` | `query_order` or `unspecified` on acceptance |
| `validated_catalog_version` | nullable `int64` | Required for accepted automatic requests |
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
schema inventory, including inner records hidden behind IPC `binary` fields.

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

### 6.2 Bind locally before agreeing to substitute

After cheap syntax checks, bind a copy of the original qualified candidate locally with remote
rewriting disabled for that validation bind. Do not execute rows. Use the bound result to obtain
the original names, types, dependencies, resolved functions, casts, and session requirements.
Only permitted base-table binding is allowed; reject arbitrary table-function binding that could
perform user-defined work. Normal metadata RPCs may still occur.

Binding is the semantic authority; serializing an unbound AST alone is not. Validate that all
bound data dependencies belong to the selected catalog instance and that every function/operator
is in the tested profile. Reject local macros, local UDFs, remote objects resolved through an
unexpected companion catalog, and views until expansion/policy equivalence is implemented.
Do not accidentally register dependencies only on a throwaway validation binder: transfer the
required reads and rebind properties to the final statement.

Use the binding information to produce canonical, safely quoted SQL and the object manifest.
Preserve the original aliases and expression types. Resolve unqualified names before removing
only the target attachment's catalog component; qualify base references with their canonical
schema-path components. Do not stringify a bound physical plan or strip identifiers by text
replacement. Alias/CTE references remain references to those scopes, not catalog table names.

The worker must implement the same **logical VGI tables**. A raw backing table is not equivalent
if normal VGI scans rename columns, add computed columns, enforce predicates, choose scan branches,
or delegate through a companion catalog. A query provider must reproduce those rules or decline
the affected object. This is especially important for row-level and column-level access policy.

Schema paths are component lists throughout resolution. A component containing a dot is not two
schemas. The current DuckDB 1.5 adapter's depth-one bridge must not flatten a deeper worker path.
Do not silently change the existing attachment `default_schema: str` contract in this feature;
automatic SQL uses resolved object paths and explicit mode uses the provider's declared namespace.

### 6.3 Output, ordering, and settings

Compare the prepared output to the locally bound output before accepting a rewrite: column count,
ordinal order, names, and logical types must match without implicit casts or dropped fields.
Define the profile's Arrow normalization explicitly; do not compare byte-for-byte IPC envelopes
or disregard type-significant metadata. A batch may not violate accepted nullability. Validate
subsequent batches as well as bind-time metadata.

The `SELECT * FROM <table function>` wrapper can introduce name-deduplication behavior. Initially
decline duplicate output names and any names/types the wrapper fails to preserve. Add support
only with tests proving original result names and engine output types survive the replacement.

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

### 7.1 Required core seam

Add an opt-in **try-prepare hook before destructive rewriting**. Its exact C++ API should be
reviewed against the target engine branch; the behavioral contract is more important than the
illustrative name:

```text
TryRemoteExecute(context, original_query_node_by_const_reference)
    -> accepted replacement TableRef
    -> no replacement (ordinary decline)
    -> exception (actual query/protocol/backend error)
```

The caller must still own an untouched, fully qualified candidate when invoking this hook. The
extension performs validation/canonicalization on copies. On decline the optimizer continues
with the original candidate; on acceptance it installs the replacement. Make ownership explicit
in both statement-root and nested-node finishing sites, and preserve statement properties and
query-location/error context. “Untouched” here means unchanged by this replacement attempt;
normal preceding optimizer constant folding is not rolled back.

Use an explicit engine capability or equivalent opt-in dispatch so legacy `RemoteExecute`
implementations keep their current non-null, consuming contract. The new path must occur before
the current `StripCatalogName` call. A null check added after moving and stripping the original
is not sufficient. Changing the default behavior of every remote extension is unnecessary.

The final ordinary binder still binds the accepted replacement, but it must not recursively
propose that internal scan for remote execution. Guard the validation bind against reentry too.
Test scoped CTE ownership, exceptions during preparation, and statement copies independently of
the VGI worker by using a minimal test catalog.

If this engine change cannot be carried or upstreamed, ship explicit query execution first and
leave automatic pushdown unavailable. An extension-only reconstruction of a consumed query,
using guessed catalog qualification and ad hoc binder fallback, is not the recommended design.

### 7.2 VGI adapter responsibilities

`VgiCatalog` implements the remote capability and syntax checks in the 2.0 build only. Its
context-free `Supports(RemoteCapability)` answers come from immutable attachment capabilities.
The context-aware try-prepare path checks the session's pushdown setting before making RPCs;
do not store per-session settings in shared catalog state. `EXECUTE_STATEMENT` and `CONNECT`
remain false. SELECT-bearing INSERT/CTAS optimization is a read-source optimization only; it
must not claim atomic cross-database writes or bypass DuckDB's existing transaction constraints.

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

### 7.3 Precise fallback and retry policy

| Event | Automatic mode | Explicit mode |
|---|---|---|
| Capability/profile absent | Keep existing local plan | Clear unsupported-capability error |
| Local eligibility fails | Keep existing local plan, no prepare RPC | Not applicable; provider validates remote query |
| Worker returns a valid decline | Keep existing local plan | Error with sanitized decline reason |
| Metadata changed before acceptance | At most one refresh/recheck, then local plan | Refresh/reprepare or report change |
| Permission/authentication error | Fail; do not retry locally | Fail |
| Invalid schema, invalid response, preparation timeout/backend error | Fail with diagnostic | Fail with diagnostic |
| Token expires or becomes invalid after replacement | Rebind/reprepare only under the same verified contract, or fail | Same |
| Init has an ambiguous transport failure | Fail; do not start another execution automatically | Same |
| Error after init or after any output | Abort stream and clean up; never restart locally | Same |

The commit point is installation of the accepted replacement, not receipt of the first result
batch. Only explicit declines before that point authorize automatic local fallback. Repreparing
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
Concurrent metadata changes between prepare and execution are revalidated before starting; an
incompatible schema/policy change fails rather than returning differently interpreted columns.

Explicit and automatic execution do not bypass the attachment's resolved data version. Time
travel remains excluded until query-level and per-table snapshot bindings have a defined contract.
`catalog_version_frozen` freezes metadata, not table contents. Consequently neither it nor the
current cache's SQL/arguments-plus-catalog-version key is sufficient for query-result reuse.

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

The first automatic policy is capability/shape based, not cost based. Prefer eligible joins,
aggregations, DISTINCT once supported, and ordered reductions; skip trivial scans. Keep estimates
optional and explicitly advisory. The pre-binding optimizer hook is not a full distributed cost
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

### PR 1: contract and executable corpus

Turn the reviewed wire portion into a normative optional-extension specification. Add accepted
and declined fixtures, exact Arrow schemas, malformed payloads, capability intersection cases,
and a language-neutral corpus under `conformance/catalog-query-v1/`. Pin semantic profile cases
and type mappings before advertising automatic support. Add a reference stub provider that can
accept, decline, and return deliberately malformed responses without executing a database.

### PR 2: Python protocol, dispatcher, and generated schemas

Primary files: `vgi/protocol.py`, `vgi/catalog/catalog_interface.py`, `vgi/catalog/descriptors.py`,
`vgi/worker.py`, `vgi/invocation.py` where necessary, and a new `vgi/catalog/query.py` module.
Add capability/prepare dataclasses, RPC method, provider interface, reserved name protection,
and consistent authorization/rehydration routing. Keep general TABLE execution unchanged for
ordinary functions.

Update `vgi/codegen/_common.py`'s explicit inner-record inventories and generated protocol/schema
artifacts for C++, TypeScript, Rust, Go, and Java. Extend inventory, schema-parity, protocol-version,
catalog-auth-binding, and client-catalog tests. A schema visible only as `binary` must not be
accidentally omitted from generators.

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
catalog attachment state in `src/include/storage/vgi_catalog.hpp` / the storage implementation.
Register `vgi_query` in `src/vgi_extension.cpp` and implement a dedicated query binder/scan adapter.
Reuse the connection/Arrow scan lifecycle from `src/vgi_function_connection.cpp` and
`src/vgi_table_function_impl.cpp` with explicit cache exclusion and fixed execution flags.
Audit reconnect behavior, `Copy()`, serialization, and transaction dependency registration.

This delivers useful protocol capability independently of the DuckDB 2.0 port. Test it against
the existing scan path and all supported worker transports. Do not retrofit an unrelated
optimizer extension into 1.5 as part of this milestone.

### PR 5: DuckDB prepare-before-rewrite seam

In the inspected engine tree, change the opt-in catalog API and
`src/optimizer/remote_pushdown_optimizer.cpp`. Add a small test catalog proving accepted/declined
ownership behavior, preserved qualification, nonrecursive replacement, and compatible legacy
dispatch. Cover both `FinishPushdown` overloads and retain conservative CTE handling. Do not
advertise general mixed-subtree pushdown as a side effect of this patch.

### PR 6: DuckDB 2.0 automatic adapter

Add a separate version-specific adapter, for example
`src/storage/vgi_remote_query.cpp` plus its header. Implement coarse syntax checks, guarded local
binding, canonical name/type/context validation, prepare RPC, exact output checking, and final
scan replacement. Reuse the explicit execution machinery from PR 4. Keep version-dependent
DuckDB headers/hooks outside the portable VGI wire layer and the 1.5 build.

### PR 7: hardening, documentation, and staged enablement

Run semantic, cross-process, transaction, security, and fault-injection suites. Document the
provider contract, opt-in catalog example, `vgi_query`, EXPLAIN, decline reasons, and rollout
settings. Publish compatibility claims for tested engine/provider combinations, not just a
minimum semver. Enable `auto` by default only for certified providers after performance evidence.

## 12. Required verification

### Protocol and semantic conformance

- Capability absent, empty intersections, unknown selected IDs, version mismatch, size limits,
  malformed inner IPC, duplicate/invalid fields, null versus empty, and invalid status unions.
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

Run each accepted automatic query against an immutable fixture with pushdown `off` and `auto`;
compare output names/types and result bags or ordered sequences as specified by the query. For
nonunique order keys, validate the permitted tie groups rather than demanding an arbitrary
identical tie order. Add metamorphic and generated query cases to catch missing overload checks.
Use instrumentation to prove accepted queries execute exactly once and declined queries never
start remotely. On mutable data, use a shared pinned snapshot when comparing paths.

### Engine and lifecycle conformance

- EXPLAIN and PREPARE do not execute; EXECUTE rebinds under changed transaction/settings/metadata;
  persistent plans do not retain live receipts; detach/reattach invalidates old attachment handles.
- Set `threads=1` and a larger value; ordered streams stay ordered and one effective VGI reader
  remains after init. Backend parallelism does not produce multiple client cursor consumers.
- HTTP prepare on process A, bind on B, and successive reads on C/D do not restart execution.
  Owner loss fails safely; replayed continuation never skips or duplicates acknowledged rows.
- Cancellation during prepare, init, blocked fetch, and after partial output; idle abandonment,
  normal EOS, transaction commit/rollback, and double-close all release resources.
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

The current protocol checks major/minor compatibility and the C++ consumer rejects unexpected
return-schema column counts. Adding optional fields is not automatically wire-compatible with
deployed clients. If these changes are still within the coordinated, unreleased VGI 2.0 contract,
regenerate and release the SDKs/extension together. If `2.0.0` is already frozen/deployed as the
supported contract, make this an additive `2.1.0` revision under the current versioning rules,
or explicitly design tolerant decoding first. Do not silently alter deployed 2.0 schemas and
claim capability gating makes them backward-compatible. Wire version, query-profile version,
Python package version, and DuckDB engine version remain independent.

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
- **Substrait or a new engine-neutral relational IR immediately:** possible future encodings,
  but much larger than the required read-query capability and does not eliminate type/function
  semantics or authorization problems. Leave encoding negotiation extensible.
- **Automatic retry locally on any remote error:** can duplicate execution, change snapshots,
  leak side effects through functions, or hide permissions and backend failures. Reject it.

Launch gates, not optional follow-up work: exact supported-type mappings; safe decline ownership
in the target DuckDB build; logical-table authorization equivalence; ordered streaming under
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
| vgi-python: `vgi/protocol.py` | Client capabilities, bind/init requests, TABLE state rehydration, wire versioning |
| vgi-python: `vgi/catalog/catalog_interface.py` | Attachment metadata, schema paths, function and scan contracts |
| vgi-python: `vgi/catalog/descriptors.py`, `vgi/worker.py` | Declarative catalog construction, function routing, attachment context |
| vgi-python: `vgi/transactor/server.py`, `vgi/transactor/_duckdb_compat.py` | Existing transaction-owner model, Arrow scans, fork-specific shared-transaction cursors |
| vgi-python: `vgi/codegen/_common.py` | Explicit schema inventories for nested IPC records |
| vgi: `src/include/storage/vgi_catalog.hpp`, `src/storage/vgi_transaction.cpp` | Catalog state and transaction lifetime |
| vgi: `src/storage/vgi_table_entry.cpp`, `src/storage/vgi_table_function_set.cpp` | Existing catalog scan binding and metadata |
| vgi: `src/vgi_function_connection.cpp`, `src/vgi_table_function_impl.cpp` | Bind/init transport, copies, ordering, result-cache integration |

Related design documents: [VGI 2.0 audit](vgi-protocol-proposed-changes.md),
[filter-v2 specification](vgi-filter-encoding-v2-spec.md), and
[DuckDB filter adapter](vgi-duckdb-filter-adapter.md). Query pushdown complements those scan-level
contracts; it does not change their meaning.
