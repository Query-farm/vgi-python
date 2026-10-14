# Macro overload contract

Status: development contract for the combined successor protocol. The wire
records and Python validation/reference helpers are implemented while builds
remain on DuckDB 1.5. Native creation/loading of the new overload representation
is a separate adapter checkpoint. Existing `vgi.v2` spelling does not declare
this breaking development diff compatible with released implementations.

## Representation and capabilities

`MacroInfo` and `MacroCreateRequest` append nullable
`overloads: list<struct<MacroOverload>>`. The existing macro get, list, create and
drop methods operate on the whole named family. There is no new per-overload
RPC or opaque overload identifier. Catalog dependency references likewise name
the complete scalar or table macro family.

The read profile is `vgi.macro.overloads.read.v1`; the create profile is
`vgi.macro.overloads.create.v1` and requires read. A worker may advertise create
only when it can validate, atomically store, and return the complete new
representation. Reading records or retaining their bytes does not by itself
qualify execution. A client or worker lacking the applicable negotiated profile
must reject a new collection, never choose its first overload or erase typing.

| Parent field state | Meaning |
| --- | --- |
| `overloads = NULL` | Existing single-definition representation. |
| Nonempty overload list | Authoritative complete family; legacy `parameters = []`, `definition = ""`, defaults and `arguments_schema = NULL`. |
| Empty overload list | Invalid; dropping a family uses the existing drop operation. |
| New list plus any populated legacy definition field | Invalid, even if the first overload appears equivalent. |

The parent `macro_type` and `schema_path` must exactly equal every overload's
kind and raw owner path. Scalar and table definitions cannot share one family.
Replacing a macro replaces the whole family atomically; an overload list is
never an append or patch operation. Ordinary conflict and transaction semantics
remain those of the macro create RPC.

## Field definitions

The [combined expression design](shared-catalog-values.md#shared-sql-definitions-and-evaluation-phases)
requires evaluation context on `SqlExpression` and `MacroOverload` for
this same successor release. Both records and all generated SDKs now carry it.
CREATE-time default folding and literal-default discovery remain unchanged;
the new context must not turn defaults into invocation-time expressions.

`MacroOverload` contains:

| Field | Encoding and meaning |
| --- | --- |
| `parameters` | Ordered inline `MacroParameter` records, including both required and defaulted parameters. Empty is a valid zero-argument signature. |
| `definition` | Nonempty unbound scalar expression or table query SQL, according to `macro_type`. |
| `profile` | `duckdb.macro.v1`; unknown profiles fail before catalog mutation. |
| `schema_path` | Raw attachment-relative identifier components of the owning schema. A name containing a dot remains one component. No client catalog alias or implicit search-path substitution. |
| `macro_type` | Exactly `scalar` or `table`. |
| `evaluation_context` | Required inline `QueryEvaluationContext`; each supplied default expression must have the same context and raw owner schema as its body. |

`MacroParameter` contains:

| Field | Encoding and meaning |
| --- | --- |
| `name` | Nonempty raw parameter name. Names are unique under DuckDB ASCII case folding. |
| `declared_type` | Nullable binary Arrow schema. When present, exactly one nullable field with this parameter's exact name; the physical type and all recursive field/schema metadata define the declaration. The Arrow null placeholder type is invalid here. |
| `default_value` | Nullable binary `TypedValueBatch` IPC. When present, exactly one row and one field with this parameter's name. It is a resolved literal default, including a present typed NULL. |
| `default_expression` | Nullable binary `SqlExpression` IPC. When present, uses `duckdb.macro.default.v1` and the overload's exact owner path. |
| `documentation` | Nullable UTF-8 text. Absent and an explicitly empty string are distinct. It does not alter type identity. |

`declared_type = NULL` means **untyped**, irrespective of a literal default's
Arrow type. Declaring a type neither supplies a default nor makes the parameter
non-nullable. Both default fields absent means required. A literal typed NULL
is a supplied default, not absence. Both default fields present is invalid.
Required parameters precede defaulted parameters. All values and declarations
retain nested metadata; a receiver that cannot honor a logical type rejects it
instead of falling back to a physical Arrow type.

The one-row record envelope is exact: missing fields, unexpected schemas,
multiple batches, trailing bytes and NULL in required fields are errors. Limits
are 8 MiB per encoded record, 1 MiB per SQL body or declared type schema,
1,024 parameters per overload, 256 overloads per family, 32 schema components,
4 KiB per identifier and 64 KiB per documentation string. Transport limits may
be smaller. Python serialization revalidates mutable lists before sending.

## Binding, evaluation and discovery

The attachment's owning SQL binder is responsible for expression parsing,
name resolution, authorization, logical type resolution, body validation,
implicit casts, duplicate-signature checks and overload selection. It binds
against the specified owner schema and the create operation's transaction.
The profile describes DuckDB macro semantics, not arbitrary SQL execution.
No serialized engine AST, bound expression or cached plan crosses this wire.

The ordered overload vector must survive create, discovery and copying, but
order is **not** a first-match priority. For `duckdb.macro.v1`, DuckDB considers
provided parameter names, arity and implicit cast costs; typed candidates beat
otherwise equivalent untyped candidates, and tied best matches fail as
ambiguous. Duplicate resolved type vectors are rejected at creation even when
parameter names, bodies or defaults differ. Type aliases must be resolved by
the owner before this check; a generic Arrow codec must not approximate SQL
overload resolution or silently cast arguments.

For `duckdb.macro.default.v1`, the owner binds defaults as constant expressions
**at creation**. Unsupported or non-foldable expressions fail before
publication. Default values must be compatible with the declared parameter
type according to the engine's binding rules, including integer literal rules;
the request codec deliberately does not substitute Arrow casts for this check.
Defaults that reference parameters are not invocation-time closures.

Discovery returns **resolved literal defaults** in `default_value`, with
`default_expression = NULL`. It must not return submitted expression text as
the authoritative default after folding. Thus copying a discovered family does
not rerun default expressions under another session or later catalog state.
An owner may retain original SQL in private audit data, but that text is not a
second competing default on the wire. Type declarations, parameter and overload
order, documentation, resolved defaults and owner-relative body semantics must
survive discovery/recreation. SQL whitespace normalization is allowed; changing
binding or default behavior is not.

The same read representation is required for get, per-schema list and bulk
catalog contents. A family cannot appear as multiple same-name `MacroInfo`
entries. Existing version/ETag and dependency mechanisms identify a change to
any overload as a change to the family. Drop removes the family.

## Legacy migration and executable reference

`validate_macro_overloads` checks the new collection, parent agreement and
exclusive legacy/new representations. It leaves `overloads = NULL` on the
existing legacy path.

`normalize_macro_overloads` upgrades a legacy definition without inferring
declared parameter types from `arguments_schema`; callers must supply the
explicit owner evaluation context. That schema's existing types
describe literal defaults; its `vgi_doc` metadata carries documentation. Unknown
or inconsistent legacy metadata is rejected rather than discarded. New
collections are copied through the actual wire codec to isolate stored state
from later caller list mutation.

`legacy_macro_fields` permits conversion back only when there is one untyped
overload with literal defaults and fully representable documentation/metadata.
The destination evaluation context must match the overload's context.
It rejects multiple overloads, declared parameter types, expression defaults,
and other loss of information. It never evaluates a default or renders an
Arrow value as a SQL string.

`canonicalize_macro_defaults` accepts the owning binder's resolver, copies the
family, and replaces each supplied default with the resolved literal. It
returns a complete candidate only after every default succeeds. The caller
then publishes the family atomically; it must not publish partial results from
the resolver callback. This helper does not claim to implement a general SQL
binder or catalog transaction store.

`tests/catalog/test_macro_contract.py` verifies actual Arrow round trips,
recursive type metadata, typed NULL/absence, shape rejection, ownership,
ordering, exclusive representations, safe legacy migration and failed-default
atomicity. A finite DuckDB 1.5 reference folds a whitelisted expression, stores
and reads the resulting family, copies it without re-evaluation, and compares
macro invocation results. Real DuckDB tests also cover typed overload selection
and duplicate-signature rejection. These establish the contract and bounded
reference behavior; full native catalog adapters and general provider execution
remain separate qualification work.

`tests/catalog/test_macro_overload_protocol.py` exercises the normal worker and
public HTTP client: negotiated create dispatch, legacy hook compatibility,
get/list/bulk discovery, capability withdrawal, rejection of unresolved defaults
on discovery, and prevention of legacy replacement/drop of an overload family
without the create profile. Its metadata store is deliberately finite and
in-process; it makes no durability or transaction qualification claim.

The shared corpus includes new and legacy parent records, typed/untyped
parameters, expression defaults, and ordered overloads. SDK codec checks retain
the inner schema and literal IPC bytes rather than reconstructing logical types
from JSON or display strings.

The semantics were audited against the pinned Haybarn 2.0 checkout
`e1784fa4231072d7152b8aaa20977bdb8ff4ace3`, specifically
`src/include/duckdb/parser/parsed_data/create_macro_info.hpp`,
`src/include/duckdb/function/macro_function.hpp`,
`src/function/macro_function.cpp`, and
`src/planner/binder/statement/bind_create.cpp`. The binder's default-expression
folding is significant: retaining expression nodes in the parser does not mean
defaults execute afresh on every macro invocation.
