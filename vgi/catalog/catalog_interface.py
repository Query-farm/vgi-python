# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""VGI Catalog Interface for exposing catalogs, schemas, tables, and views.

This module provides the abstract base class and data types for implementing
catalog interfaces in VGI workers, enabling DuckDB ATTACH support.
"""

import dataclasses
import threading
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Literal,
    NewType,
    Self,
    cast,
    overload,
)

if TYPE_CHECKING:
    from vgi_rpc.rpc import CallContext

    from vgi.catalog.attach_option import AttachOptionSpec
    from vgi.catalog.descriptors import Catalog, Index, Macro, Schema, Table, View
    from vgi.catalog.secret_type import SecretTypeSpec
    from vgi.catalog.setting import SettingSpec

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType
from vgi_rpc.utils import deserialize_record_batch, serialize_record_batch_bytes

from vgi.arguments import SecretLookupEntry
from vgi.exceptions import CatalogReadOnlyError
from vgi.metadata import (
    DistinctDependence,
    FunctionStability,
    NullHandling,
    OrderDependence,
    OrderPreservation,
    PartitionKind,
)

__all__ = [
    # Re-exported from vgi.metadata
    "DistinctDependence",
    "FunctionStability",
    "NullHandling",
    "OrderDependence",
    "OrderPreservation",
    "PartitionKind",
    # Catalog-specific
    "CatalogDataVersionRelease",
    "CatalogExample",
    "CatalogInfo",
    "ColumnStatistics",
    "IndexConstraintType",
    "IndexInfo",
    "SecretLookupEntry",
    "MacroType",
    "SchemaObjectType",
    "TableColumnStatisticsResult",
    "WriteFunctionResult",
]


def _validate_at_params(at_unit: str | None, at_value: str | None) -> None:
    """Validate that at_unit and at_value are both provided or both absent."""
    if bool(at_unit) != bool(at_value):
        raise ValueError("at_unit and at_value must both be provided or both be None")


@dataclass(frozen=True)
class CatalogExample(ArrowSerializableDataclass):
    """An example usage of a function for catalog serialization.

    Attributes:
        sql: SQL query demonstrating the function.
        description: What this example demonstrates.
        expected_output: Optional expected result description.

    """

    sql: str
    description: str = ""
    expected_output: str | None = None


# Type aliases for improved code clarity and type checking.
# At runtime, these are equivalent to their underlying types.
AttachOpaqueData = NewType("AttachOpaqueData", bytes)
TransactionOpaqueData = NewType("TransactionOpaqueData", bytes)
SerializedSchema = NewType("SerializedSchema", bytes)
SqlExpression = NewType("SqlExpression", str)


@dataclass(frozen=True)
class CatalogDataVersionRelease(ArrowSerializableDataclass):
    """One published data version of a catalog.

    ``data_version_spec`` advertises a *compatibility range*; this record
    advertises *what's actually been published*. Together they let a
    client (the describe page, Cupola, programmatic consumers) render a
    discoverable release timeline without scraping the worker's repo.

    Contracts on the ``CatalogInfo.releases`` list this belongs to:

    * **Ordering** — entries MUST appear newest-first. Unspecified order
      would force consumers to sort by ``version`` string, which requires
      a comparator the protocol does not define (semver vs. calver vs.
      date-stamped vs. RC tags are all valid).
    * **Uniqueness** — each ``version`` MUST appear at most once. Mirrors
      the same invariant on ``attach_option_specs``'s ``name``. Consumers
      defend against duplicates (log-and-skip later entries) since Arrow
      cannot enforce key uniqueness at the wire level.

    Long-form release notes do not live here — link to a CHANGELOG anchor,
    GitHub release page, PR, or migration guide via ``notes_url``.

    Attributes:
        version: Concrete version, not a spec. e.g. "1.0.0", "2.4.1". Semver
            carries the breaking-change signal directly — major bumps are
            breaking, minor/patch are not.
        released_at: Release date (UTC). ``None`` when the worker doesn't
            track dates.
        summary: One-line human summary. Empty string when unknown.
        notes_url: Optional per-release link to detailed notes. Distinct from
            ``CatalogInfo.source_url`` (which points at the repo as a whole):
            this points at what changed in *this* release.
    """

    version: str
    released_at: Annotated[datetime | None, ArrowType(pa.timestamp("us", tz="UTC"))] = None
    summary: str = ""
    notes_url: str | None = None


@dataclass(frozen=True)
class CatalogInfo(ArrowSerializableDataclass):
    """Discovery record for a catalog exposed by a worker.

    Returned by `catalog_catalogs()` so clients can inspect per-catalog version
    metadata before attaching.

    Attributes:
        name: Catalog name — pass to catalog_attach() to open it.
        implementation_version: Worker software version (singular per worker).
            ``None`` = worker declares no implementation version.
        data_version_spec: Semver range the catalog serves (e.g.
            ">=1.0.0,<2.0.0"). ``None`` = worker declares no data-version
            opinion.
        attach_option_specs: Attach-time options the catalog accepts (distinct
            from session settings). Each AttachOptionSpec is serialized as bytes
            for Arrow compatibility. Enables pre-attach discovery via the
            catalogs() RPC.
        releases: Concrete published data versions, newest-first. Empty when the
            worker doesn't track release history. See
            ``CatalogDataVersionRelease`` for the per-entry ordering and
            uniqueness contracts.
        source_url: Where this worker's code lives — repo, build, docs. ``None``
            when the worker doesn't advertise a source location.
    """

    name: str
    implementation_version: str | None
    data_version_spec: str | None
    attach_option_specs: list[bytes] = field(default_factory=list)
    releases: list[CatalogDataVersionRelease] = field(default_factory=list)
    source_url: str | None = None


@dataclass(frozen=True)
class CatalogAttachResult(ArrowSerializableDataclass):
    """Result from attaching to a catalog.

    Attributes:
        attach_opaque_data: The unique id for the attached catalog.
        supports_transactions: Indicate if the worker supports transactions or
            not. If false, all transaction related methods will not be called
            and all transaction_opaque_data parameters will be None.
        supports_time_travel: Indicate if tables support time travel.
        catalog_version_frozen: Indicate that the catalog version id is frozen
            and the schema and object information will not change.
        catalog_version: The initial catalog version, it increments when
            schemas, tables or other objects change.
        attach_opaque_data_required: Indicate if the attach_opaque_data must be
            persisted across commands. True: Catalog is stateful;
            attach_opaque_data represents a session. False: Catalog is
            stateless; CLI can auto-attach on each command.
        default_schema: The name of the default schema for this catalog.
        settings: Extension options (settings) exposed by this catalog/worker.
            Each ExtensionOption is serialized as bytes for Arrow compatibility.
        secret_types: Secret types registered with DuckDB's SecretManager. Each
            SecretTypeSpec is serialized as bytes for Arrow compatibility.
        comment: Optional comment describing this catalog/database.
        tags: Optional key-value tags associated with this catalog/database.
        supports_column_statistics: Whether any tables in this catalog can
            provide column statistics. Global gate — if False, GetStatistics()
            returns nullptr for all tables.
        resolved_data_version: Concrete data version the worker resolved for
            this attach. ``None`` = worker has no opinion or the request omitted
            data_version_spec.
        resolved_implementation_version: Concrete implementation version the
            worker resolved for this attach. ``None`` = worker has no opinion or
            the request omitted implementation_version.
    """

    attach_opaque_data: AttachOpaqueData
    supports_transactions: bool
    supports_time_travel: bool
    catalog_version_frozen: bool
    catalog_version: int
    attach_opaque_data_required: bool = True
    default_schema: str = "main"
    settings: list[bytes] = field(default_factory=list)
    secret_types: list[bytes] = field(default_factory=list)
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    supports_column_statistics: bool = False
    resolved_data_version: str | None = field(kw_only=True)
    resolved_implementation_version: str | None = field(kw_only=True)


@dataclass(frozen=True)
class CatalogObject:
    """All objects have the following common properties.

    Attributes:
        comment: This is a generic comment about the object.
        tags: These are key-value tags associated with the object.
    """

    comment: str | None
    tags: dict[str, str]


@dataclass(frozen=True)
class CatalogSchemaObject(CatalogObject):
    """Objects that exist within a schema have the following common properties.

    Attributes:
        name: The name of the object.
        schema_name: The name of the schema containing the object.
    """

    name: str
    schema_name: str


@dataclass(frozen=True)
class SchemaInfo(CatalogObject, ArrowSerializableDataclass):
    """Information about a schema in a catalog.

    Attributes:
        attach_opaque_data: The unique id for the attached catalog.
        name: The name of the schema.
        estimated_object_count: Approximate population per object kind, keyed by
            the same names the C++ extension uses for its set-cache
            instrumentation: ``"table"``, ``"view"``, ``"scalar_function"``,
            ``"aggregate_function"``, ``"table_function"``, ``"macro"``,
            ``"index"``. Used by the client to pick between bulk ``LoadEntries``
            and per-name single-entry RPCs. Workers may omit the field entirely
            or any individual key — the client treats absent counts as 1, so
            unspecified populations bias toward eager bulk-load.

            **The value 0 is a hard guarantee, not an estimate.** When a count
            is exactly 0 the client skips the corresponding
            ``catalog_schema_contents_*`` bulk RPC entirely and short-circuits
            per-name lookups (``catalog_table_get`` / ``catalog_view_get`` /
            ``catalog_index_get``). If a worker reports 0 for a kind that
            actually has entries, ``SELECT … FROM s.x`` silently returns "not
            found" — only declare 0 for kinds the worker knows are empty in its
            current view of the schema. Cross-session DDL on the same catalog
            (another connection creating a view in a schema this connection has
            cached as zero-views) is handled the same way as any other stale
            catalog cache: ``vgi_clear_cache()`` or re-attach. Time-travel
            AT-clause queries do not honor the bypass — they always issue the
            per-name RPC because a historical version may have had entries the
            current view does not.
    """

    attach_opaque_data: AttachOpaqueData
    name: str
    estimated_object_count: dict[str, int] | None = None


@dataclass(frozen=True)
class TableInfo(CatalogSchemaObject, ArrowSerializableDataclass):
    """Information about a table in a schema.

    Attributes:
        columns: The columns of the table as a PyArrow schema that is serialized
            as bytes.
        not_null_constraints: Column indices with a NOT NULL constraint. Uses
            ArrowType to specify int32 instead of the default int64.
        unique_constraints: Column-index groups with a UNIQUE constraint.
        check_constraints: SQL CHECK constraint expressions.
        primary_key_constraints: Column-index groups forming the primary key.
        foreign_key_constraints: Serialized foreign-key constraint specs.
        supports_insert: Write-support flag — whether the table supports INSERT.
        supports_update: Write-support flag — whether the table supports UPDATE.
        supports_delete: Write-support flag — whether the table supports DELETE.
        supports_returning: When False (the default), the C++ extension rejects
            INSERT/UPDATE/DELETE ... RETURNING at plan time with a
            BinderException. Workers that can emit the affected rows from their
            write functions must opt in by setting this to True.
        supports_column_statistics: Statistics capability flag — indicates this
            table can provide column statistics.
        scan_function: Optional inlined function-discovery result. When
            populated, the C++ extension uses the cached value and skips the
            corresponding ``catalog_table_scan_function_get`` RPC. Bytes are the
            IPC payload from ``ScanFunctionResult.serialize()``. Populating this
            freezes the function args for the lifetime of the catalog cache
            (until ``catalog_version`` bumps); workers whose function args change
            more frequently than ``catalog_version`` (rotating credentials,
            presigned URLs, per-transaction snapshots) MUST leave it null so the
            per-bind RPC continues to fire.
        insert_function: Optional inlined INSERT function-discovery result. Same
            caching contract as ``scan_function``.
        update_function: Optional inlined UPDATE function-discovery result. Same
            caching contract as ``scan_function``.
        delete_function: Optional inlined DELETE function-discovery result. Same
            caching contract as ``scan_function``.
        cardinality_estimate: Optional inlined cardinality estimate. When
            populated, the C++ extension uses it directly and skips the
            ``table_function_cardinality`` RPC — saving one round-trip per bind.
            Use for read-only or slow-changing tables where cardinality is
            statically known. Freezes the cardinality for the catalog cache
            lifetime (until ``catalog_version`` bumps); workers whose cardinality
            changes faster (e.g. live counters) MUST leave it null.
        cardinality_max: Optional inlined maximum cardinality. Same caching
            contract as ``cardinality_estimate``.
        column_statistics: Optional inlined column statistics. When populated,
            the C++ extension uses the cached value and skips the per-bind /
            per-table ``catalog_table_column_statistics_get`` RPC and the
            per-scan ``table_function_statistics`` RPC. Bytes are the IPC payload
            from ``serialize_column_statistics(stats, cache_max_age_seconds)``.
            Freezes the resolved stats for the catalog cache lifetime (until
            ``catalog_version`` bumps); workers whose statistics change faster
            than ``catalog_version`` (e.g. live counters, rapidly-mutating
            dimensions) MUST leave this null so the on-demand RPC continues to
            fire.
        bind_result: Optional inlined bind result. Bytes are the IPC payload of
            ``BindResponse.serialize_to_bytes()``. When populated, the C++
            extension uses these bytes verbatim and skips the per-scan ``bind``
            RPC, threading the deserialized BindResult straight into bind_data.
            The catalog framework only populates this for tables marked
            ``Table(inline_bind=True)`` whose function class is
            ``@bind_fixed_schema``-decorated — the decorator's contract (output
            is exactly ``cls.FIXED_SCHEMA``, no per-call inputs, no opaque_data)
            matches what's safe to freeze for the catalog cache lifetime.
            Functions with custom ``on_bind`` are not eligible via the framework
            path; workers can still inline manually inside their own
            ``schema_contents`` override when the bind output is independently
            known to be stable.
        required_field_filter_paths: Dotted-path column references that the VGI
            extension's optimizer pass must verify appear in any scan's WHERE
            expression (top-level column names like ``"country"`` or struct
            subfields like ``"bbox.xmin"``, ``"nested.outer.inner"``). Empty
            (default) means no enforcement — the zero-cost fast path for every
            existing table. Satisfaction is prefix-based: a present filter on a
            shorter dotted path satisfies any required path it's a prefix of. A
            whole-struct filter on ``bbox`` therefore satisfies every required
            ``"bbox.*"`` path. The C++ extension throws ``BinderException``
            listing any unsatisfied paths.
    """

    columns: SerializedSchema

    not_null_constraints: Annotated[list[int], ArrowType(pa.list_(pa.int32()))]
    unique_constraints: Annotated[list[list[int]], ArrowType(pa.list_(pa.list_(pa.int32())))]
    check_constraints: list[str]
    primary_key_constraints: Annotated[list[list[int]], ArrowType(pa.list_(pa.list_(pa.int32())))] = field(
        default_factory=list
    )
    foreign_key_constraints: Annotated[list[bytes], ArrowType(pa.list_(pa.binary()))] = field(default_factory=list)

    supports_insert: bool = False
    supports_update: bool = False
    supports_delete: bool = False
    supports_returning: bool = False

    supports_column_statistics: bool = False

    scan_function: Annotated[bytes | None, ArrowType(pa.binary())] = None
    insert_function: Annotated[bytes | None, ArrowType(pa.binary())] = None
    update_function: Annotated[bytes | None, ArrowType(pa.binary())] = None
    delete_function: Annotated[bytes | None, ArrowType(pa.binary())] = None

    cardinality_estimate: Annotated[int | None, ArrowType(pa.int64())] = None
    cardinality_max: Annotated[int | None, ArrowType(pa.int64())] = None

    column_statistics: Annotated[bytes | None, ArrowType(pa.binary())] = None

    bind_result: Annotated[bytes | None, ArrowType(pa.binary())] = None

    required_field_filter_paths: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ViewInfo(CatalogSchemaObject, ArrowSerializableDataclass):
    """Information about a view in a schema.

    Attributes:
        definition: The definition of the view which is a SQL query string.
        column_comments: Per-column comments, keyed by the view's output column
            name. Unlike tables (whose column comments ride along as Arrow field
            metadata on the serialized ``columns`` schema), a view ships only its
            SQL ``definition`` — DuckDB binds that query to derive the columns —
            so view column comments need their own channel. The C++ extension
            aligns these by name against the bound output columns and feeds them
            into ``CreateViewInfo.column_comments_map``; names that don't match a
            bound column are ignored.
    """

    definition: str
    column_comments: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class MacroInfo(CatalogSchemaObject, ArrowSerializableDataclass):
    """Information about a macro in a schema.

    Attributes:
        macro_type: Whether this is a scalar or table macro.
        parameters: Ordered list of parameter names.
        parameter_default_values: One-row `RecordBatch` where column names are parameter
            names and values are typed defaults. None if no defaults.
            Serialized as IPC bytes over the wire.
        definition: The SQL expression (scalar) or query (table).
        arguments_schema: Optional Arrow schema (serialized as IPC bytes) with one
            nullable field per parameter, in ``parameters`` order. Each field's type
            is the parameter's default value type when known (else null), and the
            ``vgi_doc`` field metadata key carries the parameter's description (UTF-8,
            presence-only — omitted when undocumented). Mirrors the per-argument doc
            channel functions expose via ``FunctionInfo.arguments``. None means the
            worker did not supply per-parameter docs (older workers); the extension
            falls back to ``parameters`` for names. Built with
            ``vgi.argument_spec.macro_arguments_schema``.

    """

    macro_type: "MacroType"
    parameters: list[str]
    parameter_default_values: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None
    definition: str = ""
    arguments_schema: Annotated[pa.Schema | None, ArrowType(pa.binary())] = None


class FunctionType(Enum):
    """The type of function in a schema.

    Attributes:
        SCALAR: A scalar function.
        TABLE: A table function.
        TABLE_BUFFERING: A table-buffering (table-in-out) function.
        AGGREGATE: An aggregate function.
    """

    SCALAR = "scalar"
    TABLE = "table"
    TABLE_BUFFERING = "table_buffering"
    AGGREGATE = "aggregate"


class MacroType(Enum):
    """The type of macro in a schema.

    Attributes:
        SCALAR: A scalar macro.
        TABLE: A table macro.
    """

    SCALAR = "scalar"
    TABLE = "table"


class IndexConstraintType(Enum):
    """The constraint type of an index.

    Attributes:
        NONE: Regular index (no constraint enforcement).
        UNIQUE: Index enforces a UNIQUE constraint.
        PRIMARY: Index enforces a PRIMARY KEY constraint.
    """

    NONE = "none"
    UNIQUE = "unique"
    PRIMARY = "primary"


@dataclass(frozen=True)
class IndexInfo(CatalogSchemaObject, ArrowSerializableDataclass):
    """Information about an index in a schema.

    Attributes:
        table_name: The name of the table this index is on.
        index_type: The index type string (e.g., "ART", or empty for default).
        constraint_type: The constraint enforcement type (NONE, UNIQUE, PRIMARY).
        expressions: SQL expression strings defining the indexed expressions.
            For column-based indexes, these are column references (e.g., "col_a").
            For expression indexes, these are arbitrary SQL (e.g., "lower(col_a)").
        options: Key-value index options (WITH clause).

    """

    table_name: str
    index_type: str = ""
    constraint_type: IndexConstraintType = IndexConstraintType.NONE
    expressions: list[str] = field(default_factory=list)
    options: dict[str, str] = field(default_factory=dict)


class SchemaObjectType(Enum):
    """The type of object that can exist within a schema.

    Used to filter results from schema_contents().

    Attributes:
        TABLE: A table.
        VIEW: A view.
        SCALAR_FUNCTION: A scalar function.
        TABLE_FUNCTION: A table function.
        AGGREGATE_FUNCTION: An aggregate function.
        SCALAR_MACRO: A scalar macro.
        TABLE_MACRO: A table macro.
        INDEX: An index.
    """

    TABLE = "table"
    VIEW = "view"
    SCALAR_FUNCTION = "scalar_function"
    TABLE_FUNCTION = "table_function"
    AGGREGATE_FUNCTION = "aggregate_function"
    SCALAR_MACRO = "scalar_macro"
    TABLE_MACRO = "table_macro"
    INDEX = "index"


class OnConflict(Enum):
    """Behavior when a conflict occurs during creation of an object.

    Attributes:
        ERROR: Raise an error if the object already exists.
        IGNORE: Do nothing if the object already exists.
        REPLACE: Replace the existing object if it already exists.
    """

    ERROR = "error"
    IGNORE = "ignore"
    REPLACE = "replace"


@dataclass(frozen=True)
class FunctionInfo(CatalogSchemaObject, ArrowSerializableDataclass):
    """Information about a function in a schema.

    Attributes:
        function_type: The type of function from VGI.
        arguments: The arguments as a serialized Apache arrow schema using
            ``schema.serialize().to_pybytes()``.
        output_schema: The output schema as a serialized Apache arrow schema
            using ``schema.serialize().to_pybytes()``.
        stability: Scalar function behavior field (None for non-scalar
            functions).
        null_handling: Scalar function behavior field (None for non-scalar
            functions).
        description: Intrinsic documentation from function metadata
            (``Meta.description``). The user-settable ``comment`` (via COMMENT ON
            FUNCTION) is inherited from the base object.
        examples: Usage examples for the function.
        categories: Category labels for the function.
        projection_pushdown: Table-function capability (None for scalar
            functions).
        filter_pushdown: Table-function capability (None for scalar functions).
        sampling_pushdown: Table-function capability (None for scalar
            functions).
        late_materialization: True if the table participates in DuckDB's
            late-materialization optimizer (``Meta.late_materialization``). The
            DuckDB extension only honours this when the table also exposes a
            rowid virtual column plus filter/projection pushdown — see
            GetScanFunctionImpl in the C++ vgi_table_entry.cpp.
        supported_expression_filters: Expression-filter classes the function can
            accept pushed down.
        order_preservation: Whether the function preserves input ordering.
        max_workers: Maximum parallel workers. Uses ArrowType to specify int32
            instead of the default int64.
        supports_batch_index: True if the function opts in to per-batch
            ``vgi_batch_index`` tagging: the worker emits an integer partition id
            in each Arrow batch's KeyValueMetadata; the DuckDB extension threads
            it through ``TableFunction::get_partition_data`` so ordered sinks
            (BatchCollector, BatchInsert, BatchCopyToFile, Limit) can reassemble
            parallel output in partition-id order. Opting in also skips the
            FIXED_ORDER MaxThreads=1 clamp; the source stays parallel and the
            sink does the ordering.
        partition_kind: Partition shape declared by the function over its
            ``vgi.partition_column``-annotated bind-schema fields. When
            non-``NOT_PARTITIONED``, the DuckDB extension installs
            ``TableFunction::get_partition_info`` returning the corresponding
            ``TablePartitionInfo`` value so the planner can pick
            ``PhysicalPartitionedAggregate`` for ``GROUP BY`` queries (today,
            only ``SINGLE_VALUE_PARTITIONS`` materially changes planner
            behavior). Per-column annotation lives in the bind schema's
            field-level metadata — see ``vgi.schema_utils.partition_field``.
        order_dependent: Aggregate function field (future).
        distinct_dependent: Aggregate function field (future).
        supports_window: True if the aggregate implements the window() callback.
        streaming_partitioned: True if the aggregate opts into the
            streaming-partitioned protocol — ``aggregate_streaming_open`` /
            ``_chunk`` / ``_close``. The DuckDB extension's optimizer rule may
            rewrite eligible LogicalWindow nodes to use this path.
        has_finalize: True if a table-in-out function declares a finalize/finish
            stage. The C++ extension uses this to conditionally register
            ``in_out_function_final``; DuckDB rejects LATERAL with correlated
            input on functions that register a finalize callback.
        source_order_dependent: Only meaningful when ``function_type ==
            FunctionType.TABLE_BUFFERING`` (i.e. the function is registered
            through the Sink+Source path). When true, the source phase is
            single-threaded and ``finalize_state_id``s drain in combine-returned
            order. Default false enables parallel finalize.
        sink_order_dependent: Only meaningful when ``function_type ==
            FunctionType.TABLE_BUFFERING``. When true, the SINK phase runs
            single-threaded — every process() call arrives in source order on one
            worker. Mutually exclusive with ``requires_input_batch_index``.
        requires_input_batch_index: Only meaningful when ``function_type ==
            FunctionType.TABLE_BUFFERING``. When true, the C++ Sink operator
            declares ``RequiredPartitionInfo()=BatchIndex()``; each process() RPC
            carries a globally-unique monotonic batch_index from DuckDB's source.
            Workers can sort by it in combine() to reconstruct source order under
            parallel ingest. Mutually exclusive with ``sink_order_dependent``.
        required_settings: Settings required by the function.
        required_secrets: Secrets required by the function (each entry has
            secret_type, optional secret_name, optional scope).
    """

    function_type: FunctionType

    arguments: SerializedSchema

    output_schema: SerializedSchema

    stability: FunctionStability | None = None
    null_handling: NullHandling | None = None

    description: str = ""
    examples: list[CatalogExample] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)

    projection_pushdown: bool | None = None
    filter_pushdown: bool | None = None
    sampling_pushdown: bool | None = None
    late_materialization: bool | None = None
    supported_expression_filters: list[str] = field(default_factory=list)
    order_preservation: OrderPreservation | None = None
    max_workers: Annotated[int | None, ArrowType(pa.int32())] = None
    supports_batch_index: bool = False
    partition_kind: PartitionKind = PartitionKind.NOT_PARTITIONED

    order_dependent: OrderDependence = OrderDependence.NOT_ORDER_DEPENDENT
    distinct_dependent: DistinctDependence = DistinctDependence.NOT_DISTINCT_DEPENDENT
    supports_window: bool = False
    streaming_partitioned: bool = False

    has_finalize: bool = False

    source_order_dependent: bool = False

    sink_order_dependent: bool = False

    requires_input_batch_index: bool = False

    required_settings: list[str] = field(default_factory=list)

    required_secrets: list[SecretLookupEntry] = field(default_factory=list)


@dataclass(frozen=True)
class CopyFromFormatInfo(CatalogObject, ArrowSerializableDataclass):
    """A custom ``COPY ... FROM`` format advertised by a VGI catalog.

    The VGI DuckDB extension registers one DuckDB ``CopyFunction`` per advertised
    format (into the system catalog, keyed by ``format_name``) so users can run
    ``COPY target FROM 'path' (FORMAT <format_name>, opt val, ...)`` and have a
    worker function parse the source and stream rows into a local table. Discovery
    is catalog-level via :meth:`VgiProtocol.catalog_copy_from_formats`; see the
    C++ ``vgi_copy_from_impl.cpp`` and ``docs/copy_from.md``.

    Inherits ``comment`` and ``tags`` from :class:`CatalogObject`.

    Attributes:
        format_name: The ``FORMAT`` identifier users type. Lives in a single
            global namespace shared with built-ins (``csv``/``parquet``/``json``)
            and every other attached catalog's formats — collisions are rejected
            at ATTACH by the extension.
        handler: Registered name of the worker function that performs the read.
        options: Serialized Arrow schema of the format's options, built from the
            handler's ``Arg``-annotated arguments (same encoding as
            :attr:`FunctionInfo.arguments`); each field's metadata carries the
            option type / default / ``vgi_doc`` description. The reserved
            ``file_path`` positional is excluded.
        direction: ``"from"`` — the only direction supported today. Reserved
            ``"to"`` for a future ``COPY ... TO``; surfaced so the C++
            ``vgi_copy_formats()`` diagnostic can split FROM vs TO.
        description: Intrinsic documentation from the handler's
            ``Meta.description``.
    """

    format_name: str
    handler: str
    options: SerializedSchema
    direction: str = "from"
    description: str = ""


@dataclass(frozen=True)
class ScanFunctionResult:
    """Result from getting a table scan function.

    This result tells the VGI DuckDB extension which DuckDB function to call
    to obtain the data for a table. This enables catalogs to delegate scanning
    to any DuckDB function (e.g., read_parquet, iceberg_scan, or a custom VGI
    table function) with appropriate arguments.

    Attributes:
        function_name: The DuckDB function to call (e.g., "read_parquet").
        positional_arguments: Positional arguments as PyArrow scalars.
        named_arguments: Named arguments as PyArrow scalars.
        required_extensions: DuckDB extensions to load before calling.
        ARROW_SCHEMA: Arrow IPC schema used to (de)serialize this result over the wire.

    """

    function_name: str
    positional_arguments: list[pa.Scalar]  # type: ignore[type-arg]
    named_arguments: dict[str, pa.Scalar]  # type: ignore[type-arg]
    required_extensions: list[str] = field(default_factory=list)

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("function_name", pa.string(), nullable=False),
            pa.field("arguments", pa.binary(), nullable=False),
            pa.field("required_extensions", pa.list_(pa.string()), nullable=False),
        ]  # type: ignore[arg-type]
    )

    def to_row_dict(self) -> dict[str, Any]:
        """Convert to a dictionary for batch construction.

        The arguments field is serialized as nested Arrow IPC bytes.
        """
        # Build arguments as nested batch
        argument_values: dict[str, pa.Scalar] = {}  # type: ignore[type-arg]
        argument_schema = []
        for index, arg in enumerate(self.positional_arguments):
            argument_schema.append(pa.field(f"arg_{index}", arg.type))
            argument_values[f"arg_{index}"] = arg
        for name, value in self.named_arguments.items():
            argument_schema.append(pa.field(name, value.type))
            argument_values[name] = value

        argument_batch = pa.RecordBatch.from_pylist(
            [argument_values],
            schema=pa.schema(argument_schema),
        )

        return {
            "function_name": self.function_name,
            "arguments": serialize_record_batch_bytes(argument_batch),
            "required_extensions": list(self.required_extensions) if self.required_extensions is not None else None,
        }

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [self.to_row_dict()],
            schema=self.ARROW_SCHEMA,
        )
        return serialize_record_batch_bytes(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from Arrow RecordBatch."""
        from vgi_rpc.utils import _validate_single_row_batch

        row = _validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=["function_name", "arguments"],
        )

        # Deserialize the nested arguments batch.
        # row["arguments"] is already bytes (_validate_single_row_batch returns
        # Python values, not PyArrow scalars).
        arguments_bytes = cast(bytes, row["arguments"])
        arguments_batch, _ = deserialize_record_batch(arguments_bytes)

        # Extract positional and named arguments from the batch
        positional_arguments: list[pa.Scalar] = []  # type: ignore[type-arg]
        named_arguments: dict[str, pa.Scalar] = {}  # type: ignore[type-arg]

        for arg_field in arguments_batch.schema:
            value = arguments_batch.column(arg_field.name)[0]
            if arg_field.name.startswith("arg_"):
                positional_arguments.append(value)
            else:
                named_arguments[arg_field.name] = value

        return cls(
            function_name=cast(str, row["function_name"]),
            positional_arguments=positional_arguments,
            named_arguments=named_arguments,
            required_extensions=list(cast("list[str]", row.get("required_extensions") or [])),
        )


# Write function discovery uses the same wire format as scan function discovery.
WriteFunctionResult = ScanFunctionResult


# ============================================================================
# Multi-branch scan (catalog_table_scan_branches_get)
# ============================================================================
#
# A table whose data spans multiple physical sources (canonical example:
# hot rows in Kafka + historical rows in Iceberg/Delta/parquet) declares
# one ``ScanBranch`` per source. The VGI DuckDB extension's optimizer-
# extension rewrites a placeholder ``LogicalGet`` into a
# ``LogicalSetOperation(UNION_ALL, ...)`` with one arm per branch, each
# binding its own ``TableFunction`` (a VGI function, or a native reader
# like ``iceberg_scan`` / ``read_parquet``).
#
# This is **wire-compat with single-branch workers**: the new RPC
# ``catalog_table_scan_branches_get`` is additive; old workers that don't
# implement it cause the C++ side to fall back to
# ``catalog_table_scan_function_get`` and synthesise a one-branch result.
#
# The rewriter semantics, ``branch_filter`` model, and current scope
# decisions (INSERT-only on writable arm, UPDATE/DELETE/MERGE refused,
# AT-clause refused, fail-fast error semantics) are documented with the
# relevant methods below.


@dataclass(frozen=True)
class ScanBranch:
    """One physical source backing a multi-branch scan.

    Attributes:
        function_name: The DuckDB function to call for this branch
            (e.g., ``"read_parquet"``, ``"iceberg_scan"``, or a VGI
            table function). The C++ rewriter resolves this name against
            DuckDB's function catalog and binds it at optimize time.
        positional_arguments: Positional arguments as PyArrow scalars,
            passed through to the function's ``bind``.
        named_arguments: Named arguments as PyArrow scalars.
        branch_filter: Optional SQL expression text (parsed by DuckDB's
            parser, bound against the branch's bound column list). The
            rewriter AND's this into every scan of this branch BEFORE
            filter pushdown, so the branch only ever sees rows in its
            declared scope. Used to make overlapping physical sources
            (Kafka 7d retention + Iceberg nightly batches with ~24h
            overlap) non-overlapping at scan time, without changing the
            worker code. ``None`` means unconstrained.
        writable: Declares this branch as the INSERT target for the
            multi-branch table. At most one branch per table may set
            this true (enforced at catalog-load by the C++ extension —
            multiple writable arms would violate DuckDB's single-
            writable-catalog-per-transaction rule). When no branch is
            writable, the table is read-only. UPDATE/DELETE/MERGE
            remain refused on multi-branch tables regardless of this
            flag; the contract is INSERT-only until cross-arm
            semantics have customer-driven evidence.
        ARROW_SCHEMA: Arrow IPC schema used to (de)serialize this branch over the wire.

    """

    function_name: str
    positional_arguments: list[pa.Scalar]  # type: ignore[type-arg]
    named_arguments: dict[str, pa.Scalar]  # type: ignore[type-arg]
    branch_filter: str | None = None
    writable: bool = False

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("function_name", pa.string(), nullable=False),
            pa.field("arguments", pa.binary(), nullable=False),
            pa.field("branch_filter", pa.string(), nullable=True),
            pa.field("writable", pa.bool_(), nullable=False),
        ]  # type: ignore[arg-type]
    )

    def to_row_dict(self) -> dict[str, Any]:
        """Convert to a dictionary for batch construction.

        Arguments are serialized as nested Arrow IPC bytes (same trick as
        :class:`ScanFunctionResult`).
        """
        argument_values: dict[str, pa.Scalar] = {}  # type: ignore[type-arg]
        argument_schema: list[pa.Field] = []  # type: ignore[type-arg]
        for index, arg in enumerate(self.positional_arguments):
            argument_schema.append(pa.field(f"arg_{index}", arg.type))
            argument_values[f"arg_{index}"] = arg
        for name, value in self.named_arguments.items():
            argument_schema.append(pa.field(name, value.type))
            argument_values[name] = value
        argument_batch = pa.RecordBatch.from_pylist(
            [argument_values],
            schema=pa.schema(argument_schema),
        )
        return {
            "function_name": self.function_name,
            "arguments": serialize_record_batch_bytes(argument_batch),
            "branch_filter": self.branch_filter,
            "writable": self.writable,
        }

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes (1-row batch using ARROW_SCHEMA)."""
        batch = pa.RecordBatch.from_pylist(
            [self.to_row_dict()],
            schema=self.ARROW_SCHEMA,
        )
        return serialize_record_batch_bytes(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from a 1-row Arrow RecordBatch."""
        from vgi_rpc.utils import _validate_single_row_batch

        row = _validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=["function_name", "arguments"],
        )

        arguments_bytes = cast(bytes, row["arguments"])
        arguments_batch, _ = deserialize_record_batch(arguments_bytes)

        positional_arguments: list[pa.Scalar] = []  # type: ignore[type-arg]
        named_arguments: dict[str, pa.Scalar] = {}  # type: ignore[type-arg]
        for arg_field in arguments_batch.schema:
            value = arguments_batch.column(arg_field.name)[0]
            if arg_field.name.startswith("arg_"):
                positional_arguments.append(value)
            else:
                named_arguments[arg_field.name] = value

        branch_filter_value = row.get("branch_filter")
        return cls(
            function_name=cast(str, row["function_name"]),
            positional_arguments=positional_arguments,
            named_arguments=named_arguments,
            branch_filter=cast("str | None", branch_filter_value) if branch_filter_value is not None else None,
            # writable is non-nullable on the wire — trust the schema.
            writable=bool(row["writable"]),
        )


@dataclass(frozen=True)
class ScanBranchesResult:
    """Result from getting the list of scan branches for a multi-branch table.

    The result tells the VGI DuckDB extension which DuckDB function(s) to
    call to obtain the data for the table. Each branch is bound independently
    and the rewriter unions their output.

    Attributes:
        branches: One `[`ScanBranch`][]` per physical source. Order is meaningful
            for stable diagnostic output (``vgi_table_branches()``) but not
            for query semantics (UNION ALL is unordered).
        required_extensions: Union of all DuckDB extensions needed across all
            branches (e.g., ``["iceberg", "httpfs"]``). The C++ side auto-loads
            unloaded entries before running the rewrite; missing extensions
            surface the existing extension-load diagnostic. Hoisted to the
            top level so workers don't repeat ``"iceberg"`` on every branch
            that uses it.
        ARROW_SCHEMA: Arrow IPC schema used to (de)serialize this result over the wire.

    """

    branches: list[ScanBranch]
    required_extensions: list[str] = field(default_factory=list)

    # On the wire each branch is serialized as its own IPC stream (bytes),
    # carried in a list<binary> column. The C++ side parses each entry via
    # ScanBranch::deserialize, matching the nested-IPC trick used for the
    # arguments field on ScanFunctionResult/ScanBranch themselves.
    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("branches", pa.list_(pa.binary()), nullable=False),
            pa.field("required_extensions", pa.list_(pa.string()), nullable=False),
        ]  # type: ignore[arg-type]
    )

    def to_row_dict(self) -> dict[str, Any]:
        """Convert to a dictionary for batch construction."""
        return {
            "branches": [branch.serialize() for branch in self.branches],
            "required_extensions": list(self.required_extensions),
        }

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes (1-row batch using ARROW_SCHEMA)."""
        batch = pa.RecordBatch.from_pylist(
            [self.to_row_dict()],
            schema=self.ARROW_SCHEMA,
        )
        return serialize_record_batch_bytes(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from a 1-row Arrow RecordBatch.

        Empty branches list is rejected — workers must return at least one
        branch. (See the design memo's "loud at attach" rule.)
        """
        from vgi_rpc.utils import _validate_single_row_batch

        row = _validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=["branches"],
        )

        branch_blobs = cast("list[bytes]", row["branches"])
        if not branch_blobs:
            raise ValueError(f"{cls.__name__}: branches list must not be empty")

        branches: list[ScanBranch] = []
        for blob in branch_blobs:
            branch_batch, _ = deserialize_record_batch(blob)
            branches.append(ScanBranch.deserialize(branch_batch))

        return cls(
            branches=branches,
            required_extensions=list(cast("list[str]", row.get("required_extensions") or [])),
        )


# ============================================================================
# Column Statistics
# ============================================================================


@dataclass(frozen=True)
class ColumnStatistics:
    """Statistics for a single column in a table.

    Workers provide these to help DuckDB's optimizer make cost-based decisions
    (filter elimination, join reordering, etc.).

    Attributes:
        column_name: Name of the column these statistics describe.
        min: Minimum value as a typed PyArrow scalar (e.g., ``pa.scalar(0, pa.int64())``),
            or ``None`` if unknown.
        max: Maximum value as a typed PyArrow scalar, or ``None`` if unknown.
            Must have the same Arrow type as ``min``.
        has_null: Whether the column contains any null values.
        has_not_null: Whether the column contains any non-null values.
        distinct_count: Approximate count of distinct values, or ``None`` if unknown.
        contains_unicode: String/binary columns only — whether values contain non-ASCII
            characters. ``None`` for non-string columns.
        max_string_length: String/binary columns only — maximum byte length of values.
            ``None`` for non-string columns.

    """

    column_name: str
    min: pa.Scalar | None = None  # type: ignore[type-arg]
    max: pa.Scalar | None = None  # type: ignore[type-arg]
    has_null: bool = True
    has_not_null: bool = True
    distinct_count: int | None = None
    contains_unicode: bool | None = None
    max_string_length: int | None = None


@dataclass(frozen=True)
class TableColumnStatisticsResult:
    """Result from ``table_column_statistics_get`` with optional cache control.

    Attributes:
        statistics: Per-column statistics for the table.
        cache_max_age_seconds: How long the client may cache these statistics
            (in seconds). ``None`` means cache indefinitely (static data).
            ``0`` means do not cache (live/volatile data).

    """

    statistics: list[ColumnStatistics]
    cache_max_age_seconds: int | None = None


def _infer_stat_type(stat: ColumnStatistics) -> pa.DataType:
    """Infer the Arrow type for a ColumnStatistics entry from its min/max scalars."""
    if stat.min is not None and stat.min.is_valid:
        return stat.min.type  # type: ignore[no-any-return]
    if stat.max is not None and stat.max.is_valid:
        return stat.max.type  # type: ignore[no-any-return]
    return pa.null()


def serialize_column_statistics(
    stats: list[ColumnStatistics],
    cache_max_age_seconds: int | None = None,
) -> bytes:
    """Serialize column statistics into a single RecordBatch with sparse union min/max.

    The ``min`` and ``max`` columns use an Arrow sparse union whose child types
    are the distinct column types present in *stats*.  This keeps everything in
    a single IPC stream regardless of how many column types the table has.

    Args:
        stats: Per-column statistics to serialize.
        cache_max_age_seconds: Optional cache TTL embedded in schema metadata.

    Returns:
        IPC-serialized bytes of the statistics RecordBatch.

    """
    n = len(stats)
    if n == 0:
        # Return a minimal empty batch — must construct empty union arrays manually
        # since pa.array([], type=sparse_union) is not supported
        union_fields: list[pa.Field[Any]] = [pa.field("0", pa.null())]
        union_type = pa.sparse_union(union_fields)
        empty_union = pa.UnionArray.from_sparse(
            pa.array([], type=pa.int8()),
            [pa.array([], type=pa.null())],
            field_names=["0"],
            type_codes=[0],  # type: ignore[arg-type]
        )
        schema = pa.schema(
            [
                pa.field("column_name", pa.utf8()),
                pa.field("min", union_type),
                pa.field("max", union_type),
                pa.field("has_null", pa.bool_()),
                pa.field("has_not_null", pa.bool_()),
                pa.field("distinct_count", pa.int64()),
                pa.field("contains_unicode", pa.bool_()),
                pa.field("max_string_length", pa.uint64()),
            ]
        )
        batch = pa.record_batch(
            [
                pa.array([], type=pa.utf8()),
                empty_union,
                empty_union,
                pa.array([], type=pa.bool_()),
                pa.array([], type=pa.bool_()),
                pa.array([], type=pa.int64()),
                pa.array([], type=pa.bool_()),
                pa.array([], type=pa.uint64()),
            ],
            schema=schema,
        )
        return serialize_record_batch_bytes(batch)

    # 1. Collect distinct Arrow types, assign type codes
    type_map: dict[pa.DataType, int] = {}
    row_type_codes: list[int] = []
    for s in stats:
        arrow_type = _infer_stat_type(s)
        if arrow_type not in type_map:
            type_map[arrow_type] = len(type_map)
        row_type_codes.append(type_map[arrow_type])

    # 2. Build sparse union child arrays (each child is length N)
    union_fields = []
    field_names: list[str] = []
    type_codes: list[int] = []
    min_children: list[pa.Array[Any]] = []
    max_children: list[pa.Array[Any]] = []
    for arrow_type, code in sorted(type_map.items(), key=lambda x: x[1]):
        union_fields.append(pa.field(str(code), arrow_type))
        field_names.append(str(code))
        type_codes.append(code)
        min_vals = [s.min if row_type_codes[i] == code else None for i, s in enumerate(stats)]
        max_vals = [s.max if row_type_codes[i] == code else None for i, s in enumerate(stats)]
        min_children.append(pa.array(min_vals, type=arrow_type))
        max_children.append(pa.array(max_vals, type=arrow_type))

    # 3. Build sparse union arrays
    codes_arr = pa.array(row_type_codes, type=pa.int8())
    min_union = pa.UnionArray.from_sparse(
        codes_arr,
        min_children,
        field_names=field_names,
        type_codes=type_codes,  # type: ignore[arg-type]
    )
    max_union = pa.UnionArray.from_sparse(
        codes_arr,
        max_children,
        field_names=field_names,
        type_codes=type_codes,  # type: ignore[arg-type]
    )

    # 4. Build schema and batch
    union_type = pa.sparse_union(union_fields)
    schema = pa.schema(
        [
            pa.field("column_name", pa.utf8()),
            pa.field("min", union_type),
            pa.field("max", union_type),
            pa.field("has_null", pa.bool_()),
            pa.field("has_not_null", pa.bool_()),
            pa.field("distinct_count", pa.int64()),
            pa.field("contains_unicode", pa.bool_()),
            pa.field("max_string_length", pa.uint64()),
        ],
    )

    batch = pa.record_batch(
        [
            pa.array([s.column_name for s in stats], type=pa.utf8()),
            min_union,
            max_union,
            pa.array([s.has_null for s in stats], type=pa.bool_()),
            pa.array([s.has_not_null for s in stats], type=pa.bool_()),
            pa.array([s.distinct_count for s in stats], type=pa.int64()),
            pa.array([s.contains_unicode for s in stats], type=pa.bool_()),
            pa.array([s.max_string_length for s in stats], type=pa.uint64()),
        ],
        schema=schema,
    )

    # 5. Serialize with cache TTL as IPC batch custom_metadata (not schema metadata)
    custom_metadata = None
    if cache_max_age_seconds is not None:
        custom_metadata = pa.KeyValueMetadata({b"cache_max_age_seconds": str(cache_max_age_seconds).encode()})
    return serialize_record_batch_bytes(batch, custom_metadata=custom_metadata)


class CatalogInterface(ABC):
    """Provides an interface to manage catalogs, schemas, tables, and views for VGI.

    This interface defines methods for creating, dropping, and managing catalogs,
    schemas, tables, and views. It also supports transactions and provides methods
    for discovering catalog contents.

    Implementors of this interface should provide concrete implementations for
    all abstract methods and properties.

    API limitations:
        - Functions are not able to be created or dropped.
        - Tags are not able to be updated on catalog objects.
        - Comments and tags are not updatable on schemas ([`SchemaInfo`][]).
        - Constraints cannot be added/dropped (except NOT NULL).

    A VGI worker will offer a single implementation of this interface to clients
    to manage their catalogs.
    """

    @property
    def interface_feature_flags(self) -> set[str]:
        """Get the feature flags supported by this [`CatalogInterface`][].

        Feature flags indicate optional capabilities of the implementation.
        The default implementation returns an empty set.
        """
        return set()

    def loggable_attach_options(self, options: Mapping[str, Any]) -> Mapping[str, Any]:
        """Return a redacted view of attach/create options safe for logs and Sentry breadcrumbs.

        Called by the worker when emitting catalog lifecycle events
        (``catalog.attach``, ``catalog.create``).  Override to opt in to
        logging the option fields you know are safe — host names, regions,
        bucket names, etc.  Never return credentials such as passwords,
        tokens, or connection strings containing secrets.

        Default returns an empty mapping, so by default **nothing** from the
        ``options`` dict is logged.  This fail-closed behaviour avoids
        leaking credentials when an implementer has not explicitly chosen
        which fields are safe to emit.

        Args:
            options: The raw options dict the client passed to ATTACH /
                CREATE (the same ``dict`` handed to :meth:`catalog_attach`
                or :meth:`catalog_create`).

        Returns:
            A mapping of safe-to-log key/value pairs.  Returning an empty
            mapping (the default) suppresses the ``options`` field from
            lifecycle events entirely.

        """
        del options
        return {}

    @abstractmethod
    def catalogs(self) -> list[CatalogInfo]:
        """Get a list of catalog discovery records provided by the VGI worker.

        Each record carries the catalog name and — if the worker has opinions —
        its implementation_version and data_version_spec, so clients can
        prevalidate ATTACH requests.

        This is a discovery only method.
        """

    def catalog_create(self, *, name: str, on_conflict: OnConflict, options: dict[str, Any]) -> None:
        """Create a new catalog with the given name.

        If on_conflict is IGNORE and the catalog already exists, do nothing.
        If on_conflict is REPLACE and the catalog already exists, replace it.
        If on_conflict is ERROR and the catalog already exists, raise an error.

        """
        raise NotImplementedError("Catalog create not implemented.")

    # Drop a catalog
    def catalog_drop(self, *, name: str) -> None:
        """Drop the catalog with the given name."""
        raise NotImplementedError("Catalog drop not implemented.")

    # Transactions are initiated and driven by DuckDB it is rare for CatalogInterface
    # implementors to implement them, but I want to support them.
    #
    # Transaction Guarantees
    # - Transactions MAY span multiple worker processes
    # - Workers MUST treat transaction_opaque_data as opaque
    # - Workers MUST ensure idempotency of commit/rollback

    def catalog_transaction_begin(self, *, attach_opaque_data: AttachOpaqueData) -> TransactionOpaqueData | None:
        """Begin a new transaction for the given attach_opaque_data.

        If the implementation does not support transactions, it can return None.
        """
        raise NotImplementedError("Catalog transactions not implemented.")

    def catalog_transaction_commit(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        """Commit the transaction for the given attachment.

        If the transaction cannot be committed, an exception should be raised.
        """
        raise NotImplementedError("Catalog transactions not implemented.")

    def catalog_transaction_rollback(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        """Rollback the transaction for the given attachment.

        If the transaction cannot be rolled back, an exception should be raised.
        """
        raise NotImplementedError("Catalog transactions not implemented.")

    @abstractmethod
    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        ctx: "CallContext | None" = None,
    ) -> CatalogAttachResult:
        """Attach to a catalog with the given name and options.

        ``data_version_spec`` and ``implementation_version`` carry the
        semver constraints the client requested at ATTACH time. Pass-through
        strings — subclasses interpret and validate them. ``None`` means
        the client did not constrain that dimension. Implementations that
        cannot satisfy a requested version MUST raise an exception with a
        human-readable message; the error surfaces on the client as the
        ATTACH failure.

        ``ctx`` is injected by the RPC dispatcher when available. Over HTTP it
        enables setting a per-session routing cookie via ``ctx.set_cookie()``;
        over subprocess it may be ``None`` or have empty cookie support.

        Returns a [`CatalogAttachResult`][] containing the attach ID, other catalog
        metadata, and the resolved concrete versions chosen by the worker.
        """

    def catalog_detach(self, *, attach_opaque_data: AttachOpaqueData) -> None:
        """Detach from the catalog with the given attach_opaque_data.

        Any open transactions should be rolled back.
        The default implementation does nothing.
        """
        return  # Default no-op

    def catalog_version(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        ctx: "CallContext | None" = None,
    ) -> int:
        """Get the current catalog version for the given attach_opaque_data and transaction_opaque_data.

        Returns an integer representing the current catalog version.

        Changes to schemas, tables, and objects increment this version. It is used to
        expire cached catalog/schema/object information inside a VGI client or process.

        ``ctx`` is injected by the RPC dispatcher when available. Subclasses that use
        HTTP-session cookies can consult ``ctx.cookies`` to verify routing
        stickiness.

        The default implementation returns 0.
        """
        del ctx
        return 0

    def schemas(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None
    ) -> list[SchemaInfo]:
        """Get a list of schemas for the given attach_opaque_data and transaction_opaque_data.

        The default returns a schema called "main" with no comment or tags.
        """
        return [
            SchemaInfo(
                attach_opaque_data=attach_opaque_data,
                name="main",
                comment=None,
                tags={},
            )
        ]

    def schema_create(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        on_conflict: OnConflict = OnConflict.ERROR,
        comment: str | None,
        tags: dict[str, str],
    ) -> None:
        """Create a new schema with the given name, comment, and tags."""
        raise NotImplementedError("Schema create not implemented.")

    def schema_drop(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        ignore_not_found: bool,
        cascade: bool,
    ) -> None:
        """Drop the schema with the given name."""
        raise NotImplementedError("Schema drop not implemented.")

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Literal[SchemaObjectType.TABLE],
    ) -> Sequence[TableInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Literal[SchemaObjectType.VIEW],
    ) -> Sequence[ViewInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Literal[
            SchemaObjectType.SCALAR_FUNCTION,
            SchemaObjectType.TABLE_FUNCTION,
            SchemaObjectType.AGGREGATE_FUNCTION,
        ],
    ) -> Sequence[FunctionInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Literal[SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO],
    ) -> Sequence[MacroInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Literal[SchemaObjectType.INDEX],
    ) -> Sequence[IndexInfo]: ...

    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo]:
        """Get the contents of the schema with the given name.

        Schemas can contain tables, views, functions, macros, and indexes.

        Args:
            attach_opaque_data: The attachment identifier.
            transaction_opaque_data: The transaction identifier, if any.
            name: The name of the schema.
            type: The type of objects to return. Must be a [`SchemaObjectType`][] enum:
                - `SchemaObjectType.TABLE`: Return only tables
                - `SchemaObjectType.VIEW`: Return only views
                - `SchemaObjectType.SCALAR_FUNCTION`: Scalar functions
                - `SchemaObjectType.TABLE_FUNCTION`: Table functions
                - `SchemaObjectType.SCALAR_MACRO`: Scalar macros
                - `SchemaObjectType.TABLE_MACRO`: Table macros
                - `SchemaObjectType.INDEX`: Indexes

        Returns:
            A list of [`TableInfo`][], [`ViewInfo`][], [`FunctionInfo`][], or [`MacroInfo`][] objects
            depending on the type parameter.

        """
        raise NotImplementedError("Schema contents not implemented.")

    @abstractmethod
    def schema_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
    ) -> SchemaInfo | None:
        """Get information about the schema with the given name.

        Returns a [`SchemaInfo`][] object if the schema exists, or None if it does not.
        """

    @abstractmethod
    def table_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        """Get information about the table with the given name in the specified schema.

        When ``at_unit`` / ``at_value`` are provided the implementation should
        return the table schema for the requested point in time (time travel).

        Returns a [`TableInfo`][] object if the table exists, or None if it does not.
        """

    def table_create(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        # The contents of the table is a serialized PyArrow schema
        # the nullability for each field is ignored.
        # schema.serialize().to_pybytes()
        columns: SerializedSchema,
        on_conflict: OnConflict,
        # These are constraints listed by field index
        not_null_constraints: list[int],  # [] = no not null constraints
        unique_constraints: list[list[int]],  # [] = no unique constraints
        # These are general check constraints specified as SQL expressions.
        check_constraints: list[str],  # [] = no check constraints
        # Primary key constraints as column index groups
        primary_key_constraints: list[list[int]] | None = None,
        # Foreign key constraints as IPC-serialized bytes (same format as TableInfo)
        foreign_key_constraints: list[bytes] | None = None,
    ) -> None:
        """Create a new table with the given name and schema.

        Comments and tags are not supported on table creation.
        """
        raise NotImplementedError("Table create not implemented.")

    def table_drop(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
        cascade: bool = False,
    ) -> None:
        """Drop the table with the given name."""
        raise NotImplementedError("Table drop not implemented.")

    def table_comment_set(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        comment: str | None,
        ignore_not_found: bool,
    ) -> None:
        """Set the comment for the table with the given name."""
        raise NotImplementedError("Table comment set not implemented.")

    def table_column_comment_set(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        column_name: str,
        comment: str | None,
        ignore_not_found: bool,
    ) -> None:
        """Set the comment for a column in the table."""
        raise NotImplementedError("Table column comment set not implemented.")

    def table_rename(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Rename the table with the given name to the new name."""
        raise NotImplementedError("Table rename not implemented.")

    def table_column_add(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        # Arrow schema with single field for column to add.
        # Serialized via schema.serialize().to_pybytes()
        column_definition: SerializedSchema,
        ignore_not_found: bool,
        if_column_not_exists: bool,
    ) -> None:
        """Add a column to the table with the given name."""
        raise NotImplementedError("Table column add not implemented.")

    def table_column_drop(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool,
        if_column_exists: bool,
        cascade: bool,
    ) -> None:
        """Drop the column from the table with the given name."""
        raise NotImplementedError("Table column drop not implemented.")

    def table_column_rename(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        column_name: str,
        new_column_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Rename the column in the table with the given name."""
        raise NotImplementedError("Table column rename not implemented.")

    def table_column_default_set(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        column_name: str,
        expression: SqlExpression,
        ignore_not_found: bool,
    ) -> None:
        """Set the default expression for the column."""
        raise NotImplementedError("Table column default set not implemented.")

    def table_column_default_drop(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Drop the default expression for the column."""
        raise NotImplementedError("Table column default drop not implemented.")

    def table_column_type_change(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        # Arrow schema with single field for the new column type.
        # Serialized via schema.serialize().to_pybytes()
        column_definition: SerializedSchema,
        expression: SqlExpression | None,
        ignore_not_found: bool,
    ) -> None:
        """Change the type of the column in the table with the given name.

        The name of the column to change is taken from the field in the provided schema.
        """
        raise NotImplementedError("Table column type change not implemented.")

    def table_not_null_drop(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Drop the NOT NULL constraint from the column."""
        raise NotImplementedError("Table NOT NULL drop not implemented.")

    def table_not_null_set(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Set the NOT NULL constraint on the column."""
        raise NotImplementedError("Table NOT NULL set not implemented.")

    def table_scan_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        # Time travel fields (iceberg style)
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        """Get the `ScanFunctionResult` for scanning the table.

        Returns information about the VGI table function to call when scanning
        this table. The at_unit and at_value support time travel queries.
        """
        raise NotImplementedError("Table scan function get not implemented.")

    def table_scan_branches_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanBranchesResult:
        """Get the list of scan branches for a multi-source table.

        Multi-branch tables compose a logical scan from N physical sources
        (canonical case: Kafka hot tier + Iceberg cold tier). The VGI DuckDB
        extension's optimizer-extension rewrites the placeholder ``LogicalGet``
        into ``LogicalSetOperation(UNION_ALL, ...)``, one arm per branch.

        Default implementation: delegate to :meth:`table_scan_function_get`
        and wrap the single ``ScanFunctionResult`` as a one-branch list.
        This makes every existing single-source worker automatically
        compatible with the new branches-aware C++ side, while letting
        workers that genuinely need multi-source override this method.

        Workers that override should NOT also raise from
        :meth:`table_scan_function_get` — the legacy method must keep
        working for old C++ extensions that don't yet probe for the new
        branches RPC. Common pattern: a worker implements both, where
        :meth:`table_scan_function_get` returns ``branches[0]`` (the
        primary branch) and :meth:`table_scan_branches_get` returns the
        full list.

        Args:
            attach_opaque_data: Per-attach session token.
            transaction_opaque_data: Optional transaction token.
            schema_name: Schema containing the table.
            name: Table name.
            at_unit: Optional time-travel unit (e.g., ``"VERSION"`` /
                ``"TIMESTAMP"``). The VGI C++ side refuses ``AT(...)`` on
                multi-branch tables (>1 branch) at bind time, so workers
                returning multiple branches should expect ``at_unit`` /
                ``at_value`` to always be ``None``; single-branch returns
                still honour them.
            at_value: Optional time-travel value matching ``at_unit``.

        Returns:
            A :class:`ScanBranchesResult` carrying one or more
            :class:[`ScanBranch`][] entries plus the union of required
            extensions across all branches.

        """
        legacy = self.table_scan_function_get(
            attach_opaque_data=attach_opaque_data,
            transaction_opaque_data=transaction_opaque_data,
            schema_name=schema_name,
            name=name,
            at_unit=at_unit,
            at_value=at_value,
        )
        return ScanBranchesResult(
            branches=[
                ScanBranch(
                    function_name=legacy.function_name,
                    positional_arguments=list(legacy.positional_arguments),
                    named_arguments=dict(legacy.named_arguments),
                    branch_filter=None,
                ),
            ],
            required_extensions=list(legacy.required_extensions),
        )

    def table_column_statistics_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> TableColumnStatisticsResult | None:
        """Get column statistics for all columns in a table.

        Returns a :class:`TableColumnStatisticsResult` containing per-column
        statistics and an optional cache TTL, or ``None`` if statistics are not
        available for this table.

        The default implementation returns ``None`` (no statistics).
        Workers that provide statistics should override this method.
        """
        return None

    def table_insert_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        writable_branch_function_name: str | None = None,
    ) -> ScanFunctionResult:
        """Get the write function for INSERT operations on the table.

        Returns a `ScanFunctionResult` identifying the [`TableInOutGenerator`][] function
        to call for inserting rows into this table.

        ``writable_branch_function_name`` is set by the C++ extension when the
        table is multi-branch and a branch declared ``writable=True``: the value
        is the writable arm's ``ScanBranch.function_name``. Workers serving
        multi-branch tables can use this to dispatch the INSERT to the correct
        underlying storage without re-resolving the writable arm internally.
        For single-branch tables this is ``None`` (or unset for legacy
        overrides).
        """
        raise NotImplementedError("Table insert not supported.")

    def table_update_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        """Get the write function for UPDATE operations on the table.

        Returns a `ScanFunctionResult` identifying the [`TableInOutGenerator`][] function
        to call for updating rows in this table. Input batches will include a
        rowid column plus the columns being updated.
        """
        raise NotImplementedError("Table update not supported.")

    def table_delete_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        """Get the write function for DELETE operations on the table.

        Returns a `ScanFunctionResult` identifying the [`TableInOutGenerator`][] function
        to call for deleting rows from this table. Input batches will contain
        a rowid column identifying the rows to delete.
        """
        raise NotImplementedError("Table delete not supported.")

    def view_create(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        definition: str,
        on_conflict: OnConflict,
    ) -> None:
        """Create a new view with the given definition."""
        raise NotImplementedError("View create not implemented.")

    def view_drop(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
        cascade: bool = False,
    ) -> None:
        """Drop the view with the given name."""
        raise NotImplementedError("View drop not implemented.")

    def view_rename(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Rename the view to the new name."""
        raise NotImplementedError("View rename not implemented.")

    @abstractmethod
    def view_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> ViewInfo | None:
        """Get information about the view with the given name.

        Returns a [`ViewInfo`][] object if the view exists, or None if it does not.
        """

    def view_comment_set(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        comment: str | None,
        ignore_not_found: bool,
    ) -> None:
        """Set the comment for the view with the given name."""
        raise NotImplementedError("View comment set not implemented.")

    # ---- Macros ----

    @abstractmethod
    def macro_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> MacroInfo | None:
        """Get information about the macro with the given name.

        Returns a [`MacroInfo`][] object if the macro exists, or None if it does not.
        """

    def macro_create(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        macro_type: "MacroType",
        parameters: list[str],
        definition: str,
        on_conflict: OnConflict,
        parameter_default_values: pa.RecordBatch | None = None,
        arguments_schema: pa.Schema | None = None,
    ) -> None:
        """Create a new macro with the given definition.

        Args:
            attach_opaque_data: Per-attach catalog session token.
            transaction_opaque_data: Optional transaction handle.
            schema_name: Schema to create the macro in.
            name: Name for the new macro.
            macro_type: Whether this is a scalar or table macro.
            parameters: Ordered list of parameter names.
            definition: SQL expression (scalar) or query (table).
            on_conflict: Behavior if the macro already exists.
            parameter_default_values: One-row ``RecordBatch`` with typed defaults.
            arguments_schema: Optional Arrow schema (one nullable field per
                parameter, in ``parameters`` order) carrying per-parameter
                descriptions via the ``vgi_doc`` field metadata key. ``None`` when
                no per-parameter docs are supplied.
        """
        raise NotImplementedError("Macro create not implemented.")

    def macro_drop(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
    ) -> None:
        """Drop the macro with the given name."""
        raise NotImplementedError("Macro drop not implemented.")

    # ---- Indexes ----

    def index_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> IndexInfo | None:
        """Get information about the index with the given name.

        Returns an [`IndexInfo`][] object if the index exists, or None if it does not.
        The default implementation returns None (no indexes).
        """
        return None

    def index_create(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        table_name: str,
        index_type: str,
        constraint_type: IndexConstraintType,
        expressions: list[str],
        on_conflict: OnConflict,
        options: dict[str, str] | None = None,
    ) -> None:
        """Create a new index on the specified table."""
        raise NotImplementedError("Index create not implemented.")

    def index_drop(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
        cascade: bool = False,
    ) -> None:
        """Drop the index with the given name."""
        raise NotImplementedError("Index drop not implemented.")

    def copy_from_formats(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
    ) -> list[CopyFromFormatInfo]:
        """List custom ``COPY ... FROM`` formats advertised by this catalog.

        Catalog-level (not schema-scoped). The default returns an empty list, so
        catalogs that don't define COPY formats are unaffected. The VGI extension
        registers one DuckDB ``CopyFunction`` per returned entry at ATTACH time.
        """
        return []


def _read_only(operation: str) -> Any:
    """Create a [`CatalogInterface`][] method that raises [`CatalogReadOnlyError`][]."""

    def method(self: Any, **kwargs: Any) -> Any:
        raise CatalogReadOnlyError(f"Cannot {operation}: catalog is read-only")

    method.__doc__ = "Not supported — raises `CatalogReadOnlyError`."
    return method


def _inline_bind_result_for(func_cls: type) -> bytes | None:
    """Pre-built ``bind_result`` bytes for a ``@bind_fixed_schema`` function.

    Returns the IPC-serialized ``BindResponse(output_schema=cls.FIXED_SCHEMA)``
    that the worker would have produced from a regular bind RPC. Cached on a
    private class attribute so subsequent ``schema_contents`` calls (per
    attach, per cache invalidation) reuse the bytes instead of re-serializing.

    Returns ``None`` if the class isn't safely pre-bind-able — either it
    isn't ``@bind_fixed_schema``-decorated (no ``_inline_bind_safe`` marker),
    or a subclass has overridden ``on_bind`` (escaping the decorator's
    contract — see the eligibility comment on ``bind_fixed_schema``).
    """
    if not getattr(func_cls, "_inline_bind_safe", False):
        return None
    # If the class has its own on_bind in __dict__, it's either the decorator's
    # injection (marked) or a subclass override (unmarked). Reject overrides.
    on_bind_attr = func_cls.__dict__.get("on_bind")
    if on_bind_attr is not None:
        underlying = getattr(on_bind_attr, "__func__", on_bind_attr)
        if not getattr(underlying, "_is_bind_fixed_schema", False):
            return None
    cached = func_cls.__dict__.get("_cached_inline_bind_result")
    if cached is not None:
        return cached  # type: ignore[no-any-return]
    from vgi.invocation import BindResponse

    response = BindResponse(output_schema=func_cls.FIXED_SCHEMA, opaque_data=None)  # type: ignore[attr-defined]
    blob = response.serialize_to_bytes()
    # Set on the class itself so subclasses don't pollute their parents'
    # cache with each other's serialized blobs (FIXED_SCHEMA may differ).
    func_cls._cached_inline_bind_result = blob  # type: ignore[attr-defined]
    return blob


class ReadOnlyCatalogInterface(CatalogInterface):
    """A read-only catalog interface that does not support DDL operations.

    This is a convenience base class for catalogs that only support reading
    metadata and data, not creating or modifying objects.

    There are two ways to use this class:

    1. Subclass and implement abstract methods:
       - catalogs() - List available catalogs
       - catalog_attach() - Attach to a catalog
       - schema_get() - Get schema information
       - table_get() - Get table information (return None for function-only catalogs)
       - view_get() - Get view information (return None for function-only catalogs)

    2. Use with functions list (simpler for function-only catalogs):
       Set the `functions` class attribute to expose VGI functions:
       - catalog_name - Name of the catalog (default: "functions")
       - functions - List of function classes to expose in the "main" schema

       This provides automatic implementations of catalogs(), catalog_attach(),
       schema_get(), table_get(), view_get(), and schema_contents().

    Optional methods that can be overridden:
    - catalog_detach() - Custom detach logic
    - schemas() - Custom schema listing (default returns 'main')
    - schema_contents() - List schema contents
    - table_scan_function_get() - Get scan function for tables

    All DDL operations (create, drop, rename, modify) will raise
    [`CatalogReadOnlyError`][].

    Attributes:
        supports_transactions: Always ``False`` -- read-only catalogs do not
            support transactions.
        catalog_version_frozen: Always ``True`` -- the catalog version never
            changes.
        catalog_name: Name of the catalog exposed when using the functions-list
            mode (default ``"functions"``).
        functions: Function classes to expose in the ``"main"`` schema.
        settings: DuckDB setting specs the catalog declares.
        secret_types: Secret type specs the catalog declares.
        attach_option_specs: Attach option specs accepted at ``ATTACH`` time.
        catalog: Optional declarative `[`Catalog`][]` object describing the
            catalog's schemas, tables, and views.
        catalog_create: DDL stub that raises `[`CatalogReadOnlyError`][].
        catalog_drop: DDL stub that raises `[`CatalogReadOnlyError`][].
        catalog_transaction_begin: DDL stub that raises `[`CatalogReadOnlyError`][].
        catalog_transaction_commit: DDL stub that raises `[`CatalogReadOnlyError`][].
        catalog_transaction_rollback: DDL stub that raises `[`CatalogReadOnlyError`][].
        schema_create: DDL stub that raises `[`CatalogReadOnlyError`][].
        schema_drop: DDL stub that raises `[`CatalogReadOnlyError`][].
        table_create: DDL stub that raises `[`CatalogReadOnlyError`][].
        table_drop: DDL stub that raises `[`CatalogReadOnlyError`][].
        table_comment_set: DDL stub that raises `[`CatalogReadOnlyError`][].
        table_column_comment_set: DDL stub that raises `[`CatalogReadOnlyError`][].
        table_rename: DDL stub that raises `[`CatalogReadOnlyError`][].
        table_column_add: DDL stub that raises `[`CatalogReadOnlyError`][].
        table_column_drop: DDL stub that raises `[`CatalogReadOnlyError`][].
        table_column_rename: DDL stub that raises `[`CatalogReadOnlyError`][].
        table_column_default_set: DDL stub that raises `[`CatalogReadOnlyError`][].
        table_column_default_drop: DDL stub that raises `[`CatalogReadOnlyError`][].
        table_column_type_change: DDL stub that raises `[`CatalogReadOnlyError`][].
        table_not_null_drop: DDL stub that raises `[`CatalogReadOnlyError`][].
        table_not_null_set: DDL stub that raises `[`CatalogReadOnlyError`][].
        view_create: DDL stub that raises `[`CatalogReadOnlyError`][].
        view_drop: DDL stub that raises `[`CatalogReadOnlyError`][].
        view_rename: DDL stub that raises `[`CatalogReadOnlyError`][].
        view_comment_set: DDL stub that raises `[`CatalogReadOnlyError`][].
        macro_create: DDL stub that raises `[`CatalogReadOnlyError`][].
        macro_drop: DDL stub that raises `[`CatalogReadOnlyError`][].
        index_create: DDL stub that raises `[`CatalogReadOnlyError`][].
        index_drop: DDL stub that raises `[`CatalogReadOnlyError`][].
    """

    supports_transactions = False
    catalog_version_frozen = True

    # Class attributes for function-based catalogs
    catalog_name: str = "functions"
    functions: list[type] = []
    settings: list["SettingSpec"] = []
    secret_types: list["SecretTypeSpec"] = []
    attach_option_specs: list["AttachOptionSpec"] = []

    # NEW: Optional Catalog object for declarative definition
    catalog: "Catalog | None" = None

    # Fixed attach_opaque_data for read-only catalogs (no need for unique IDs)
    _FIXED_ATTACH_ID: AttachOpaqueData = AttachOpaqueData(b"readonly-catalog-")

    # Instance-level registry caches (built lazily)
    # Keys are LOWERCASE for case-insensitive lookup
    _schema_registry: "dict[str, Schema] | None" = None
    _table_registry: "dict[tuple[str, str], Table] | None" = None
    _view_registry: "dict[tuple[str, str], View] | None" = None
    _function_registry: "dict[tuple[str, str], list[type]] | None" = None
    _macro_registry: "dict[tuple[str, str], Macro] | None" = None
    _index_registry: "dict[tuple[str, str], Index] | None" = None
    # Lazy registry build is one-time but the fixture HTTP server is
    # multi-threaded and shares one catalog instance, so concurrent
    # first-requests can race the build. Serialize it under a lock and flip
    # ``_registries_built`` only AFTER population so readers never observe a
    # half-built (mutating) registry. (Shared across instances — fine; the
    # build is one-time and infrequent.)
    _build_lock = threading.Lock()
    _registries_built: bool = False

    def _build_registries(self) -> None:
        """Build the lookup registries lazily, once, and thread-safely.

        Double-checked locking: the fast path is a lock-free flag read; the
        actual build runs under ``_build_lock`` and sets ``_registries_built``
        only after population completes. A concurrent reader either builds
        (under the lock) or waits for the builder, so it never iterates a
        registry that another thread is still mutating.
        """
        if self._registries_built:
            return
        with self._build_lock:
            if self._registries_built:
                return
            self._build_registries_locked()

    def _build_registries_locked(self) -> None:
        """Populate the registries. Caller must hold ``_build_lock``.

        All registry keys are lowercase for case-insensitive lookups.
        Raises ValueError if duplicate names detected within same schema.
        """
        # Import here to avoid circular imports
        from vgi.catalog.descriptors import Schema

        self._schema_registry = {}
        self._table_registry = {}
        self._view_registry = {}
        self._function_registry = {}
        self._macro_registry = {}
        self._index_registry = {}

        def _register_table(schema_key: str, table: "Table") -> None:
            key = (schema_key, table.name.lower())
            if key in self._table_registry:  # type: ignore[operator]
                raise ValueError(f"Duplicate table '{table.name}' in schema '{schema_key}'")
            self._table_registry[key] = table  # type: ignore[index]

        def _register_view(schema_key: str, view: "View") -> None:
            key = (schema_key, view.name.lower())
            if key in self._view_registry:  # type: ignore[operator]
                raise ValueError(f"Duplicate view '{view.name}' in schema '{schema_key}'")
            self._view_registry[key] = view  # type: ignore[index]

        def _register_function(schema_key: str, func_cls: type) -> None:
            meta = func_cls.get_metadata()  # type: ignore[attr-defined]
            key = (schema_key, meta.name.lower())
            if key not in self._function_registry:  # type: ignore[operator]
                self._function_registry[key] = []  # type: ignore[index]
            self._function_registry[key].append(func_cls)  # type: ignore[index]

        def _register_macro(schema_key: str, macro: "Macro") -> None:
            key = (schema_key, macro.name.lower())
            if key in self._macro_registry:  # type: ignore[operator]
                raise ValueError(f"Duplicate macro '{macro.name}' in schema '{schema_key}'")
            self._macro_registry[key] = macro  # type: ignore[index]

        def _register_index(schema_key: str, index: "Index") -> None:
            key = (schema_key, index.name.lower())
            if key in self._index_registry:  # type: ignore[operator]
                raise ValueError(f"Duplicate index '{index.name}' in schema '{schema_key}'")
            self._index_registry[key] = index  # type: ignore[index]

        if self.catalog is not None:
            # Build from Catalog object
            for schema in self.catalog.schemas:
                schema_key = schema.name.lower()
                self._schema_registry[schema_key] = schema

                for table in schema.tables:
                    _register_table(schema_key, table)
                for view in schema.views:
                    _register_view(schema_key, view)
                for func_cls in schema.functions:
                    _register_function(schema_key, func_cls)
                for macro in schema.macros:
                    _register_macro(schema_key, macro)
                for index in schema.indexes:
                    _register_index(schema_key, index)
        else:
            # Backward compat: create "main" schema from legacy `functions` list.
            # The schema must carry the functions so its SchemaInfo reports
            # accurate estimated_object_count — the C++ extension treats a
            # zero count as a hard "no objects of this type" guarantee and
            # skips the enumeration RPC entirely, so an empty schema here
            # silently hides every legacy function from ATTACH.
            main_schema = Schema(name="main", tables=(), views=(), functions=tuple(self.functions))
            self._schema_registry["main"] = main_schema

            for func_cls in self.functions:
                _register_function("main", func_cls)

        # Publish last: only now may a concurrent reader skip the build and
        # iterate these registries (they are fully populated and no longer
        # mutated).
        self._registries_built = True

    @property
    def _effective_catalog_name(self) -> str:
        """Get catalog name from Catalog object or class attribute."""
        if self.catalog is not None:
            return self.catalog.name
        return self.catalog_name

    @property
    def _default_schema_name(self) -> str:
        """Get default schema name."""
        if self.catalog is not None:
            return self.catalog.default_schema
        return "main"

    def catalogs(self) -> list[CatalogInfo]:
        """Return the list of available catalogs.

        Default discovery record carries just the catalog name — subclasses
        that want to advertise version metadata should override.
        """
        return [
            CatalogInfo(
                name=self._effective_catalog_name,
                implementation_version=None,
                data_version_spec=None,
                attach_option_specs=[spec.serialize() for spec in self.attach_option_specs],
                source_url=self.catalog.source_url if self.catalog is not None else None,
            )
        ]

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        ctx: "CallContext | None" = None,
    ) -> CatalogAttachResult:
        """Attach to the catalog. Version constraints are ignored by default."""
        del data_version_spec, implementation_version, ctx
        effective_name = self._effective_catalog_name
        if name != effective_name:
            raise ValueError(f"Unknown catalog: {name!r}. Available: {effective_name}")

        # Serialize settings and secret types for the attach result
        serialized_settings = [s.serialize() for s in self.settings]
        serialized_secret_types = [st.serialize() for st in self.secret_types]

        # Auto-derive supports_time_travel and supports_column_statistics from tables
        self._build_registries()
        assert self._table_registry is not None
        has_time_travel = any(t.supports_time_travel for t in self._table_registry.values())
        has_column_statistics = any(bool(t.statistics) for t in self._table_registry.values())

        return CatalogAttachResult(
            attach_opaque_data=self._FIXED_ATTACH_ID,
            supports_transactions=getattr(self, "supports_transactions", False),
            supports_time_travel=has_time_travel,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_opaque_data_required=False,
            default_schema=self._default_schema_name,
            settings=serialized_settings,
            secret_types=serialized_secret_types,
            comment=self.catalog.comment if self.catalog is not None else None,
            tags=dict(self.catalog.tags) if self.catalog is not None else {},
            supports_column_statistics=has_column_statistics,
            resolved_data_version=None,
            resolved_implementation_version=None,
        )

    def schemas(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None
    ) -> list[SchemaInfo]:
        """Get a list of schemas for the given attach_opaque_data."""
        self._build_registries()
        assert self._schema_registry is not None
        return [s.to_schema_info(attach_opaque_data) for s in self._schema_registry.values()]

    def schema_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
    ) -> SchemaInfo | None:
        """Get information about a schema (case-insensitive lookup)."""
        self._build_registries()
        assert self._schema_registry is not None
        schema = self._schema_registry.get(name.lower())
        return schema.to_schema_info(attach_opaque_data) if schema else None

    def table_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        """Get information about a table (case-insensitive lookup).

        When ``at_unit`` / ``at_value`` are provided, the default implementation
        returns the same table info (no schema evolution). Override this method
        to return version-specific schemas for time-travel queries.
        """
        _validate_at_params(at_unit, at_value)

        self._build_registries()
        assert self._table_registry is not None
        assert self._schema_registry is not None
        table = self._table_registry.get((schema_name.lower(), name.lower()))
        if table is None:
            return None

        # If AT clause present but table doesn't support time travel, error
        if at_unit and not table.supports_time_travel:
            raise ValueError(f"Table '{schema_name}.{name}' does not support time travel queries")

        schema = self._schema_registry.get(schema_name.lower())
        return table.to_table_info(schema.name if schema else schema_name)

    def view_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> ViewInfo | None:
        """Get information about a view (case-insensitive lookup)."""
        self._build_registries()
        assert self._view_registry is not None
        assert self._schema_registry is not None
        view = self._view_registry.get((schema_name.lower(), name.lower()))
        if view:
            schema = self._schema_registry.get(schema_name.lower())
            return view.to_view_info(schema.name if schema else schema_name)
        return None

    def macro_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> MacroInfo | None:
        """Get information about a macro (case-insensitive lookup)."""
        self._build_registries()
        assert self._macro_registry is not None
        assert self._schema_registry is not None
        macro = self._macro_registry.get((schema_name.lower(), name.lower()))
        if macro:
            schema = self._schema_registry.get(schema_name.lower())
            return macro.to_macro_info(schema.name if schema else schema_name)
        return None

    def index_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> IndexInfo | None:
        """Get information about an index (case-insensitive lookup)."""
        self._build_registries()
        assert self._index_registry is not None
        assert self._schema_registry is not None
        index = self._index_registry.get((schema_name.lower(), name.lower()))
        if index is not None:
            schema = self._schema_registry.get(schema_name.lower())
            return index.to_index_info(schema.name if schema else schema_name)
        return None

    def table_column_statistics_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> TableColumnStatisticsResult | None:
        """Get column statistics from the [`Table`][] descriptor's ``statistics`` dict.

        Automatically resolves plain Python values to typed PyArrow scalars
        using the column's Arrow type from the table schema.
        Override this method for dynamic or computed statistics.
        """
        self._build_registries()
        assert self._table_registry is not None
        table = self._table_registry.get((schema_name.lower(), name.lower()))
        if table is None:
            return None
        return table.resolve_column_statistics()

    def table_scan_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        """Get scan function for a table.

        For function-backed tables (`Table.function` is set), automatically returns
        a `ScanFunctionResult` that invokes the linked function.

        For tables with explicit columns, override this method in your Worker
        to provide scan functions.
        """
        _validate_at_params(at_unit, at_value)

        self._build_registries()
        assert self._table_registry is not None
        assert self._schema_registry is not None

        # Validate AT clause against table's supports_time_travel
        table = self._table_registry.get((schema_name.lower(), name.lower()))
        if table is not None and at_unit and not table.supports_time_travel:
            raise ValueError(f"Table '{schema_name}.{name}' does not support time travel queries")

        # Check if table exists and is function-backed
        if table is not None and table.function is not None:
            # Auto-implement for function-backed tables
            func_meta = table.function.get_metadata()
            return ScanFunctionResult(
                function_name=func_meta.name,
                positional_arguments=[],
                named_arguments={},
                required_extensions=[],
            )

        # No auto-implementation available - provide helpful error
        available = [
            f"{self._effective_catalog_name}.{s.name}.{t.name}"
            for s in self._schema_registry.values()
            for t in s.tables
        ]
        available_str = ", ".join(sorted(available)) if available else "(none)"

        raise NotImplementedError(
            f"table_scan_function_get not implemented for table "
            f"'{self._effective_catalog_name}.{schema_name}.{name}'. "
            f"Available tables: {available_str}. "
            f"Either use Table(function=...) for automatic scanning, "
            f"or override table_scan_function_get in your Worker."
        )

    def _write_function_get(
        self,
        *,
        schema_name: str,
        name: str,
        operation: str,
        attr_name: str,
    ) -> ScanFunctionResult:
        """Shared implementation for table_{insert,update,delete}_function_get."""
        self._build_registries()
        assert self._table_registry is not None

        table = self._table_registry.get((schema_name.lower(), name.lower()))
        if table is None:
            raise NotImplementedError(f"Table '{schema_name}.{name}' not found in catalog.")

        write_func = getattr(table, attr_name, None)
        if write_func is None:
            raise CatalogReadOnlyError(f"Table '{schema_name}.{name}' does not support {operation}.")

        func_meta = write_func.get_metadata()
        return ScanFunctionResult(
            function_name=func_meta.name,
            positional_arguments=[],
            named_arguments={},
            required_extensions=[],
        )

    def table_insert_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        writable_branch_function_name: str | None = None,
    ) -> ScanFunctionResult:
        """Get insert function for a table."""
        # ReadOnlyCatalogInterface tables are single-branch — writable arm
        # disambiguation is not relevant here. Discard the hint.
        del writable_branch_function_name
        return self._write_function_get(
            schema_name=schema_name,
            name=name,
            operation="INSERT",
            attr_name="insert_function",
        )

    def table_update_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        """Get update function for a table."""
        return self._write_function_get(
            schema_name=schema_name,
            name=name,
            operation="UPDATE",
            attr_name="update_function",
        )

    def table_delete_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        """Get delete function for a table."""
        return self._write_function_get(
            schema_name=schema_name,
            name=name,
            operation="DELETE",
            attr_name="delete_function",
        )

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Literal[SchemaObjectType.TABLE],
    ) -> Sequence[TableInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Literal[SchemaObjectType.VIEW],
    ) -> Sequence[ViewInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Literal[
            SchemaObjectType.SCALAR_FUNCTION,
            SchemaObjectType.TABLE_FUNCTION,
            SchemaObjectType.AGGREGATE_FUNCTION,
        ],
    ) -> Sequence[FunctionInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Literal[SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO],
    ) -> Sequence[MacroInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Literal[SchemaObjectType.INDEX],
    ) -> Sequence[IndexInfo]: ...

    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo]:
        """List contents of a schema.

        Returns tables, views, functions, macros, or indexes based on the type parameter.
        Uses case-insensitive schema name lookup.

        Args:
            attach_opaque_data: The attachment identifier.
            transaction_opaque_data: The transaction identifier, if any.
            name: The name of the schema.
            type: The type of objects to return. Must be a [`SchemaObjectType`][] enum.

        Returns:
            A list of [`TableInfo`][], [`ViewInfo`][], [`FunctionInfo`][], [`MacroInfo`][], or [`IndexInfo`][] objects.

        """
        self._build_registries()
        assert self._schema_registry is not None
        assert self._table_registry is not None
        assert self._view_registry is not None
        assert self._function_registry is not None
        assert self._macro_registry is not None
        assert self._index_registry is not None

        # Case-insensitive schema lookup
        name_lower = name.lower()
        schema = self._schema_registry.get(name_lower)
        if schema is None:
            return []

        schema_name = schema.name

        # Normalize type parameter (may be string from wire protocol)
        type_enum = type if isinstance(type, SchemaObjectType) else SchemaObjectType(type)

        results: list[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo] = []

        if type_enum == SchemaObjectType.TABLE:
            for (sn, _), table in self._table_registry.items():
                if sn == name_lower:
                    info = table.to_table_info(schema_name)
                    # Inline-bind post-pass: descriptors with inline_bind=True
                    # backed by @bind_fixed_schema-decorated functions get a
                    # pre-built BindResponse inlined onto TableInfo.bind_result.
                    # The C++ extension uses these bytes verbatim and skips
                    # the per-scan bind RPC.
                    if table.inline_bind and table.function is not None:
                        bind_bytes = _inline_bind_result_for(table.function)
                        if bind_bytes is not None:
                            info = dataclasses.replace(info, bind_result=bind_bytes)
                    results.append(info)
        elif type_enum == SchemaObjectType.VIEW:
            for (sn, _), view in self._view_registry.items():
                if sn == name_lower:
                    results.append(view.to_view_info(schema_name))
        elif type_enum == SchemaObjectType.INDEX:
            for (sn, _), index in self._index_registry.items():
                if sn == name_lower:
                    results.append(index.to_index_info(schema_name))
        elif type_enum in (SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO):
            target_macro_type = MacroType.SCALAR if type_enum == SchemaObjectType.SCALAR_MACRO else MacroType.TABLE
            for (sn, _), macro in self._macro_registry.items():
                if sn == name_lower and macro.macro_type == target_macro_type:
                    results.append(macro.to_macro_info(schema_name))
        else:
            # SCALAR_FUNCTION or TABLE_FUNCTION
            for (sn, _), func_classes in self._function_registry.items():
                if sn != name_lower:
                    continue
                for func_cls in func_classes:
                    func_info = self._function_to_info(func_cls, schema_name)
                    # Filter by function type
                    if type_enum == SchemaObjectType.SCALAR_FUNCTION and func_info.function_type != FunctionType.SCALAR:
                        continue
                    if type_enum == SchemaObjectType.TABLE_FUNCTION and func_info.function_type not in (
                        FunctionType.TABLE,
                        FunctionType.TABLE_BUFFERING,
                    ):
                        continue
                    if (
                        type_enum == SchemaObjectType.AGGREGATE_FUNCTION
                        and func_info.function_type != FunctionType.AGGREGATE
                    ):
                        continue
                    results.append(func_info)

        return results

    def copy_from_formats(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
    ) -> list[CopyFromFormatInfo]:
        """Advertise every ``CopyFromFunction`` registered in this catalog.

        Introspects the catalog's function list for
        :class:`vgi.copy_from_function.CopyFromFunction` subclasses and converts
        each into a :class:`CopyFromFormatInfo`. The option schema reuses the same
        argument serialization as :meth:`_function_to_info`, so option types /
        defaults / ``doc`` descriptions surface identically to
        ``vgi_function_arguments()``.
        """
        from vgi.argument_spec import argument_specs_to_schema, extract_argument_specs
        from vgi.copy_from_function import CopyFromFunction
        from vgi.metadata import resolve_metadata

        self._build_registries()
        assert self._function_registry is not None

        formats: list[CopyFromFormatInfo] = []
        seen: set[str] = set()
        for (_, _), func_classes in self._function_registry.items():
            for func_cls in func_classes:
                if not (isinstance(func_cls, type) and issubclass(func_cls, CopyFromFunction)):
                    continue
                fmt = getattr(func_cls, "COPY_FROM_FORMAT", None)
                if not fmt or fmt in seen:
                    continue
                seen.add(fmt)
                meta = resolve_metadata(func_cls)
                args_schema = argument_specs_to_schema(extract_argument_specs(func_cls))
                formats.append(
                    CopyFromFormatInfo(
                        comment=getattr(func_cls, "COPY_FROM_COMMENT", None),
                        tags=meta.tags,
                        format_name=fmt,
                        handler=meta.name,
                        options=SerializedSchema(args_schema.serialize().to_pybytes()),
                        direction=getattr(func_cls, "COPY_FROM_DIRECTION", "from"),
                        description=meta.description or "",
                    )
                )
        return formats

    def _function_to_info(self, func_cls: type, schema_name: str) -> FunctionInfo:
        """Convert a function class to [`FunctionInfo`][]."""
        # Import here to avoid circular imports
        from vgi.argument_spec import (
            argument_specs_to_schema,
            extract_argument_specs,
        )
        from vgi.metadata import CatalogFunctionType as MetadataFunctionType
        from vgi.metadata import resolve_metadata

        meta = resolve_metadata(func_cls)

        # Map metadata function type to catalog function type
        func_type_map = {
            MetadataFunctionType.SCALAR: FunctionType.SCALAR,
            MetadataFunctionType.TABLE: FunctionType.TABLE,
            MetadataFunctionType.TABLE_BUFFERING: FunctionType.TABLE_BUFFERING,
            MetadataFunctionType.AGGREGATE: FunctionType.AGGREGATE,
        }
        func_type = func_type_map.get(meta.function_type, FunctionType.TABLE)

        # Extract argument specs with proper Arrow types
        arg_specs = extract_argument_specs(func_cls)
        args_schema = argument_specs_to_schema(arg_specs)
        args_bytes = SerializedSchema(args_schema.serialize().to_pybytes())

        # Get output schema from catalog introspection methods if available
        output_schema: pa.Schema = pa.schema([])
        has_catalog_schema = hasattr(func_cls, "catalog_output_schema")
        if func_type in (FunctionType.SCALAR, FunctionType.AGGREGATE) and has_catalog_schema:
            # ScalarFunction/AggregateFunction has catalog_output_schema() classmethod
            output_schema = func_cls.catalog_output_schema()  # type: ignore[attr-defined]
        output_bytes = SerializedSchema(output_schema.serialize().to_pybytes())

        is_scalar = func_type == FunctionType.SCALAR
        is_aggregate = func_type == FunctionType.AGGREGATE

        return FunctionInfo(
            name=meta.name,
            schema_name=schema_name,
            function_type=func_type,
            arguments=args_bytes,
            output_schema=output_bytes,
            comment=None,  # Functions don't use comment; use description instead
            tags=meta.tags,
            # Scalar/aggregate function behavior fields
            stability=meta.stability if is_scalar else None,
            null_handling=meta.null_handling if (is_scalar or is_aggregate) else None,
            # Documentation fields
            description=meta.description or "",  # Intrinsic from Meta.description
            examples=[
                CatalogExample(
                    sql=ex.sql,
                    description=ex.description,
                    expected_output=ex.expected_output,
                )
                for ex in meta.examples
            ],
            categories=meta.categories,
            # Table function capabilities (None for scalar)
            projection_pushdown=None if is_scalar else meta.projection_pushdown,
            filter_pushdown=None if is_scalar else meta.filter_pushdown,
            sampling_pushdown=None if is_scalar else meta.sampling_pushdown,
            late_materialization=None if is_scalar else meta.late_materialization,
            supported_expression_filters=[] if is_scalar else meta.supported_expression_filters,
            order_preservation=None if is_scalar else meta.preserves_order,
            max_workers=None if is_scalar else meta.max_workers,
            supports_batch_index=False if is_scalar else meta.supports_batch_index,
            partition_kind=PartitionKind.NOT_PARTITIONED if is_scalar else meta.partition_kind,
            # Aggregate function fields
            order_dependent=meta.order_dependent,
            distinct_dependent=meta.distinct_dependent,
            supports_window=meta.supports_window,
            streaming_partitioned=meta.streaming_partitioned,
            has_finalize=meta.has_finalize,
            source_order_dependent=meta.source_order_dependent,
            sink_order_dependent=meta.sink_order_dependent,
            requires_input_batch_index=meta.requires_input_batch_index,
            # Settings
            required_settings=meta.required_settings,
            # Secrets
            required_secrets=list(meta.required_secrets),
        )

    # ========== DDL operations (not supported — read-only catalog) ==========

    catalog_create = _read_only("create catalog")
    catalog_drop = _read_only("drop catalog")
    catalog_transaction_begin = _read_only("begin transaction")
    catalog_transaction_commit = _read_only("commit transaction")
    catalog_transaction_rollback = _read_only("rollback transaction")
    schema_create = _read_only("create schema")
    schema_drop = _read_only("drop schema")
    table_create = _read_only("create table")
    table_drop = _read_only("drop table")
    table_comment_set = _read_only("set table comment")
    table_column_comment_set = _read_only("set column comment")
    table_rename = _read_only("rename table")
    table_column_add = _read_only("add column")
    table_column_drop = _read_only("drop column")
    table_column_rename = _read_only("rename column")
    table_column_default_set = _read_only("set column default")
    table_column_default_drop = _read_only("drop column default")
    table_column_type_change = _read_only("change column type")
    table_not_null_drop = _read_only("drop NOT NULL constraint")
    table_not_null_set = _read_only("set NOT NULL constraint")
    view_create = _read_only("create view")
    view_drop = _read_only("drop view")
    view_rename = _read_only("rename view")
    view_comment_set = _read_only("set view comment")
    macro_create = _read_only("create macro")
    macro_drop = _read_only("drop macro")
    index_create = _read_only("create index")
    index_drop = _read_only("drop index")
