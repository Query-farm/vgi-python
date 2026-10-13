# Shared catalog values and column paths

These definitions are implemented on the DuckDB 2.0 preparation branch in the
normal SDK modules and generator inventories. They are part of the combined
successor contract. The published protocol is still `vgi.v2` / `2.1.0`; existing
catalog RPCs do not yet carry these records or advertise their operations.
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

Still required before the operation contract can freeze: exposed option
definitions and supported names; create/alter/discovery integration; selected
capability checks; conflict/version/dependency effects; and transactional
reference-provider behavior. These common records do not stand in for that work.

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
