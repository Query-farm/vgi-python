# TUPLE and canonical Arrow VARIANT contract

Status: development contract for the combined successor protocol. VARIANT now
uses the canonical Arrow Parquet Variant representation. This supersedes the
unpublished `vgi.type.variant.arrow.v1` per-value IPC proposal and its custom
scalar encodings. Previous checkpoint test counts describe that earlier source
snapshot; they are not evidence for this replacement. TUPLE's separate VGI
annotation remains unchanged. No DuckDB engine upgrade is part of this work.

## Negotiation and failure behavior

The capabilities `vgi.type.tuple.v1` and `vgi.type.variant.parquet.v1` are
independent opt-in entries in `CatalogCapabilities.read`. Selection uses the
existing client/provider intersection. Missing agreement means unsupported.
The superseded `vgi.type.variant.arrow.v1` offer does not select the canonical
profile and has no compatibility alias.

These requirements apply to nested fields, catalog definitions, query schemas,
parameter batches, and literal defaults. Annotation alone does not negotiate
support. A provider may select a profile only for operations whose semantic
mapping it can implement. Standalone operations need an equivalent explicit
agreement before consuming these values. Unqualified native DuckDB 1.5 adapters
reject them before interpreting Arrow storage, including legacy macro defaults.

Opaque transport must preserve field and schema metadata, child names/order,
nullability, bytes and validity. Copying a carrier does not authorize executing,
casting or exposing its storage as the logical value. Unknown versions, malformed
storage and unsupported mappings fail; they must not silently become STRUCT,
BLOB, JSON or strings. Query providers decline unsupported schemas before
execution where possible and fail if an unsupported result is discovered later.

## TUPLE

`vgi.logical_type.v1 = tuple` is a VGI field annotation, **not** a canonical
Arrow extension. It requires `vgi.type.tuple.v1` and uses struct storage. Ordered
members have transport names `element1` through `elementN`; these labels do not
become SQL member names. The empty tuple is an annotated zero-child struct,
distinct from an empty named struct. Parent validity distinguishes SQL NULL from
a present tuple whose members are all NULL.

Member types, order, nullability and recursive metadata remain part of schema
identity. An ordinary struct with generated-looking names is still a struct.
Readers must not infer TUPLE from names or shape. Tuple members can themselves
carry canonical VARIANT fields only when both profiles are selected. This says
nothing about representing a DuckDB tuple *inside* Parquet Variant binary data;
that engine mapping remains a separate qualification question.

The removed draft's `vgi.logical_type.v1 = variant` and special scalar kinds
are rejected. This contract adds no custom binary encoding for HUGEINT,
UHUGEINT, BIGNUM, BIT, TIME_TZ, intervals or geometry.

## Canonical VARIANT storage

The Arrow extension is `arrow.parquet.variant`. Its struct contains Parquet
Variant bytes, has no parameters, and uses empty extension metadata. Children
are resolved by their exact, case-sensitive names. The standard permits binary,
large-binary and binary-view storage, with dictionary/run-end encoding also
permitted for `metadata`.
[Arrow canonical extension specification](https://arrow.apache.org/docs/format/CanonicalExtensions.html#parquet-variant)

VGI's initial `vgi.type.variant.parquet.v1` profile supports the unshredded
subset. Writers use this layout:

| Item | Representation |
| --- | --- |
| Field metadata | `ARROW:extension:name = arrow.parquet.variant`; `ARROW:extension:metadata =` empty bytes |
| Parent | Arrow struct, with its declared nullability and validity preserved |
| First child | `metadata: binary`, non-nullable |
| Second child | `value: binary`, nullable |

Readers accept either child order and the canonical binary storage alternatives
above. Exactly the two named fields must be present. Duplicate or extra fields,
including `typed_value`, are rejected by this initial unshredded profile.
Shredding needs a later qualified profile; this restriction does not redefine
the canonical extension.

A present parent requires non-NULL metadata. The carrier preserves nullable
`value` slots and empty byte strings without asserting that they form a valid
Variant value; that decision belongs to the backend decoder. A NULL parent has
no interpreted value and unused child slots are ignored. Each present cell is
bounded to 8 MiB for the combined metadata and non-NULL value bytes.
Arrow schema traversal is bounded to depth 64 and 4,096 recursive fields.
Enclosing IPC, transport and process memory budgets still apply.

The binary fields contain **Parquet Variant data**, not an Arrow IPC stream for
each cell. Batch-level Arrow IPC still transports the column. No VGI scalar
envelope, custom scalar encoding or JSON conversion is introduced.

## Values, validation and engine mappings

The canonical binary format defines primitives, arrays, objects and null
values. Missing object members and present null members have different binary
representations. A valid parent carrying an encoded root null is also distinct
in transport from an Arrow SQL NULL. These distinctions follow the
[Parquet Variant binary format](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md).
Opaque transport preserves bytes and validity without claiming to decode them.

The Python helpers construct fields and check negotiation,
recursive schema shape, payload presence and byte limits. It does **not**
implement a general Parquet Variant binary parser or prove arbitrary payloads
valid. The backend decoder must validate binary versions/tags, offsets,
dictionaries, object keys, nested values and supported engine conversions before
using values. Structural checks alone do not qualify a provider for execution.

This profile preserves Parquet Variant semantics and bytes; it does not promise
preservation of every original DuckDB logical type, width or signedness. Standard
unsigned mappings, including uint8 to int16, uint16 to int32 and uint32 to int64,
are permitted without a custom side channel. An adapter must reject or defer
DuckDB values outside its supported canonical mapping. Unsupported HUGEINT,
UHUGEINT, BIGNUM, interval, temporal, geometry or tuple cases must not revive the
removed scalar encodings. No claim of exact preservation of the full DuckDB type
domain is made.

The audited Haybarn 2.0 checkout is
`e1784fa4231072d7152b8aaa20977bdb8ff4ace3`:

- `src/common/arrow/arrow_type_extension.cpp`, `ArrowVariant`, already uses the
  canonical extension and currently rejects shredded `typed_value` storage.
- `src/common/types/variant/parquet_variant_encoding.cpp` has incomplete
  DuckDB-to-Parquet mappings; each admitted mapping still needs qualification.
- `src/common/types/variant/variant_iterator.cpp`, `RowIsValid`, collapses root
  variant-null to SQL NULL. Native roundtrip claims must account for that limit.
- `src/common/arrow/arrow_converter.cpp`, `SetArrowStructFormat`, synthesizes
  tuple member names and does not preserve tuple identity by itself.

## PyArrow transport and remaining qualification

The PyArrow 25.0.1 integration carries the canonical extension as ordinary
struct/binary storage plus Arrow field metadata. No public PyArrow Variant API
is available in that pin; this work does not register a replacement extension
implementation. IPC round trips retain the canonical metadata and payload
buffers opaquely. Tests distinguish transport from semantic binary decoding.

`vgi/logical_types.py` owns construction and carrier validation;
`tests/test_logical_types.py` and the shared SDK corpus cover metadata, validity,
child order and malformed storage. Native guard tests cover rejection while
its adapter is unqualified. Complete binary decoding, engine-domain conversion,
native DuckDB import/export, cross-SDK execution and performance remain explicit
implementation gates. Current validation results belong in the manifest's
canonical VARIANT checkpoint, separately from the superseded draft.
