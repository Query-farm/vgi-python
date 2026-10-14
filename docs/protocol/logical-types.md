# Portable TUPLE and VARIANT contract

Status: specified for the combined successor protocol; Python reference codecs
and conformance tests accompany this contract. This does not qualify a DuckDB
engine adapter. In particular, DuckDB 1.5 peers must reject these annotations
before catalog materialization, binding, or execution. No engine upgrade is
required to implement or transport these records.

## Negotiation and failure behavior

The exact capability strings `vgi.type.tuple.v1` and
`vgi.type.variant.arrow.v1` live in the existing `CatalogCapabilities.read`
list. They are independent opt-in profiles, selected through the existing client
catalog offer/provider selection intersection. Missing offers or selections mean
unsupported; unknown strings do not confer support. Readers must check the
selected profiles before interpreting schemas or data. A provider may select a
profile only when every advertised operation that accepts that profile implements
its validation and semantic mapping. Ordinary workers select neither profile.

The contract applies recursively to catalog definitions, result/input schemas,
query parameter batches, defaults and options. A schema's annotations alone do
not negotiate capabilities. Standalone operations without a negotiated catalog
agreement cannot consume these types until their own enclosing operation defines
an equivalent explicit profile agreement. A carrier may copy bytes opaquely with
all metadata intact; that is not permission to evaluate, cast, filter, compare,
or expose the storage type as the logical value.

Unknown annotation versions, unknown kinds, unselected profiles, wrong physical
types and ambiguous metadata must fail before execution. Providers that know a
query would produce an unsupported type decline preparation. If a backend cannot
prove this until execution, it must fail the result, never silently reinterpret
or partially coerce the type. No profile implies support for arbitrary registered
Arrow extensions, opaque engine state or user-defined types.

## Recursive field metadata

`vgi.logical_type.v1` is a UTF-8 Arrow **field** metadata key. Its case-sensitive
ASCII value is one of the kinds below. Field and schema metadata keys must be
unique. The field name, order, nullability, Arrow storage type, schema metadata,
and every child's metadata are part of schema identity. Nested fields retain the
annotation at the exact child, including list elements and tuple members.

Consumers never infer logical types from names such as `element1`, a storage
shape, values, or a default. An unannotated struct remains a named struct. A plain
binary field remains bytes. A tuple with all NULL members remains a present
tuple; the parent's validity bitmap alone distinguishes a NULL tuple.

The Python reference admits the portable Arrow types listed below, structs and
ordinary lists, with the explicit special scalar annotations. Dictionary,
run-end, view and other physical optimizations require normalization to these
representations before transmission. Unknown extensions cannot be normalized by
dropping metadata. Maximum type/value nesting is 64 and each schema has at most
4096 recursive fields. Each outer variant permits at most 65,536 scalar slots
across its nested arrays and scalar IPC envelopes, checked before per-cell
validation, including NULL slots.

## TUPLE

Kind `tuple`, capability `vgi.type.tuple.v1`, uses Arrow struct storage. The
members, in order, have transport names `element1` through `elementN`. These are
positional transport labels, not SQL member names. Members preserve their own
logical annotations, physical types, nullability and metadata. The empty tuple
is an annotated zero-child struct, distinct from an empty named struct. Any
producer with named SQL members must transmit an ordinary struct instead.

Changing a member's type, order, metadata or nullability changes the schema. A
reader must map this annotation explicitly to an unnamed tuple and must never
silently return a named STRUCT when it lacks tuple support.

## VARIANT

Kind `variant`, capability `vgi.type.variant.arrow.v1`, uses ordinary Arrow
`binary` storage. Every present value is a complete, bounded Arrow IPC stream
containing **exactly one record batch, one row and one nullable field named
`value`**. That inner field declares the actual dynamic value's exact type and
metadata. The stream has no trailing bytes or additional batches. An outer NULL
has no interpreted payload. No JSON conversion, engine-private binary layout,
Parquet Variant conversion or parameter-value inference is permitted.

This scalar envelope reuses `TypedValueBatch`. It is intentionally simple and
portable across the existing Arrow SDKs. It adds per-value schema/IPC overhead;
a future optimized representation must have a different negotiated profile.
Writers must cap each payload at 8 MiB; recursive decoding also caps the sum of
all nested decoded payload lengths at 8 MiB per outer variant, preventing deeply
nested repeated decoding from evading the resource budget. The normal enclosing
RPC/batch size limits still apply. Decoded Arrow buffers are also limited to
8 MiB. The reference checks decoded size after Arrow allocation; callers must
apply their transport/process memory budget to hostile compressed IPC as well.
Writers use uncompressed IPC in this profile. A payload cannot directly wrap a second
`variant` field; nested variants occur only inside containers.

### Values and absence

- SQL NULL: the **outer** variant field is NULL, with no interpreted payload.
- Present variant-null: non-NULL outer binary containing `value: null` and its
  one NULL value. A typed NULL inner root is invalid; it cannot impersonate a
  dynamic integer/string/etc. value.
- Object: inner `value` is a struct with unique exact UTF-8 member names. Empty
  names are allowed. A missing member is absent from that object's schema. An
  explicitly present null member uses a nullable NULL field or a NULL nested variant.
  Its complete declared child type is retained even for NULL. These must never be conflated. Member order is preserved in transport; this
  does not introduce ordering semantics into backend object equality.
- Array: inner `value` is a list. Homogeneous arrays retain their exact element
  type; heterogeneous arrays use a list whose child is annotated `variant`.
  Element order and explicit NULL positions are preserved. A NULL child denotes
  a present null element, never a missing array position.
- Nested tuple: allowed only when **both** profiles are selected. Its tuple
  annotation is preserved at the inner field. A backend whose variant domain
  cannot represent a tuple must reject that mapping before accepting the value.

The envelope preserves the storage distinction between root variant-null and
SQL NULL. Pinned Haybarn 2.0 currently collapses root variant-null to SQL NULL in
`VariantIterator::RowIsValid`; an adapter must disclose/reject that non-injective
mapping when preservation is required. It must not advertise exact roundtrip
support for that distinction merely because the wire can express it. Nested
object absence and present null are separate regardless of that engine behavior.

### Numeric, temporal and other scalar identity

Unannotated dynamic scalar values use these exact Arrow representations:

| Logical information | Arrow representation |
| --- | --- |
| Boolean | bool |
| Signed/unsigned integer width | int8/16/32/64 or uint8/16/32/64, unchanged |
| Floating width, NaN payload and signed zero | float16, float32 or float64, buffers unchanged |
| Decimal precision and scale | Arrow decimal with exact declared precision/scale |
| Text / bytes | utf8 / binary |
| Date | date32, raw day count preserved |
| Time precision | time64(us) or time64(ns) |
| Timestamp precision / instant distinction | timestamp(s/ms/us/ns), timezone absent for local timestamp or exactly `UTC` for an instant |

No unsigned-to-signed widening, decimal-to-float conversion, timezone-dependent
rendering, timestamp unit change, JSON stringification or value-based type
inference is allowed. Temporal infinities and out-of-range calendar values must
be retained as raw Arrow integer storage when an adapter has a defined mapping;
an adapter must reject a value it cannot map exactly. Python datetime conversion
is not part of this wire contract. Decimal256/float16 values are portable Arrow
values but are not automatically admitted by a DuckDB adapter with narrower
native types.

The following `vgi.logical_type.v1` annotations also require the variant profile
and provide unambiguous scalar storage inside or outside a variant envelope:

| Kind | Required storage and interpretation |
| --- | --- |
| `hugeint` | fixed-size binary(16), little-endian signed two's complement 128-bit integer |
| `uhugeint` | fixed-size binary(16), little-endian unsigned 128-bit integer |
| `bignum` | utf8 canonical base-10 integer: `0` or optional `-` followed by a nonzero digit and digits; no plus sign, leading zeroes or `-0` |
| `bit` | utf8 sequence of `0` and `1`, including empty; bit length and leading zeroes are significant |
| `uuid` | fixed-size binary(16), UUID network byte order |
| `time_tz` | struct of non-NULL `micros:int64` then `offset_seconds:int32`; local time in `[0,86400000000)`, signed seconds east of UTC in `[-57599,57599]` |
| `interval` | struct of non-NULL `months:int32`, `days:int32`, `micros:int64`, in that order; independent signed components, no normalization |
| `geometry` | binary standard WKB plus required `vgi.geometry.crs` field metadata; empty CRS means unspecified; retain WKB and CRS exactly |

For WKB and CRS, the reference validates carrier shape and required metadata;
geometric validity and recognized CRS interpretation belong to the consuming
adapter. Unknown CRS or unsupported geometric kinds must not be silently
reprojected. Geometry interpretation does not become implemented solely by
copying the carrier. User metadata on the scalar and its children is retained.

## Pinned engine evidence and implementation boundary

The audited Haybarn 2.0 source is commit
`e1784fa4231072d7152b8aaa20977bdb8ff4ace3`:

- `src/common/arrow/arrow_converter.cpp`, `SetArrowStructFormat`, exports TUPLE
  through struct storage with `TupleType::NamedChildren` synthetic names.
- `src/common/arrow/arrow_type_extension.cpp`, `ArrowVariant`, exports VARIANT
  through the canonical `arrow.parquet.variant` extension. The current reader
  rejects shredded `typed_value` storage.
- `src/common/types/variant/parquet_variant_encoding.cpp` widens UINT8/16/32,
  attempts narrowing 128-bit/UINT64 to signed64, and rejects several variant
  kinds such as intervals, BIGNUM and TIME_NS. Therefore canonical Parquet
  Variant is **not** selected as the exact DuckDB type-preserving VGI encoding.
- `src/common/types/variant/variant_iterator.cpp`, `RowIsValid`, collapses a
  root variant-null as described above.

The [Arrow canonical extension specification](https://arrow.apache.org/docs/format/CanonicalExtensions.html)
provides the separate standardized Parquet Variant extension; this VGI profile
does not redefine that extension or attach its name to different storage.

`vgi/logical_types.py` implements field construction, deterministic profile
intersection, recursive schema/value validation and bounded scalar IPC
encode/decode. `tests/test_logical_types.py` covers numeric/time identities,
large unsigned values, decimal precision, recursive metadata, tuple versus
struct, SQL NULL/variant-null/absence, heterogeneous arrays and malformed inputs.
Native DuckDB adapters, cross-SDK semantic qualification and performance evidence
remain implementation gates; the wire representation itself is specified here.
