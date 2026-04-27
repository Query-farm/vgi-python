"""VGI Worker base class for hosting user-defined functions and catalogs.

A worker is a subprocess that communicates via stdin/stdout using Arrow IPC.
Workers are spawned by a client as needed and terminate once they detect their
input stream has been closed.

SUPPORTED FUNCTION TYPES
------------------------
The worker supports three function types, dispatched based on class inheritance:

1. ScalarFunction / ScalarFunctionGenerator: Transforms input batches to
   single-column output with 1:1 row mapping. Use for per-row computations.

2. TableInOutFunction / TableInOutGenerator: Reads input batches, produces
   output batches. Use for transforming, filtering, or aggregating input.

3. TableFunctionGenerator: Generates output batches without reading input.
   Use for data generation functions like sequence(), range(), etc.

QUICK START
-----------
Create a worker by subclassing Worker and listing your functions:

    from vgi.worker import Worker
    from vgi.scalar_function import ScalarFunction
    from vgi.table_in_out_function import TableInOutGenerator
    from vgi.table_function import TableFunctionGenerator

    class DoubleColumn(ScalarFunction):
        # Single-column output with 1:1 row mapping
        ...

    class EchoFunction(TableInOutGenerator):
        # Transforms input batches
        ...

    class SequenceFunction(TableFunctionGenerator):
        # Generates output without input
        ...

    class MyWorker(Worker):
        functions = [DoubleColumn, EchoFunction, SequenceFunction]

    if __name__ == "__main__":
        MyWorker().run()

Function names are derived from metadata (Meta.name or class name converted to
snake_case). No manual name mapping required.

KEY CLASSES
-----------
    Worker      - Base class to subclass (set functions attribute)

See Also
--------
vgi.client.Client : Spawns workers and sends data to them
vgi.function.Function : Base class for all functions
vgi._test_fixtures.worker : Example worker with built-in functions

"""

from __future__ import annotations

import importlib.metadata
import logging
import os
import sys
import uuid
from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Any, cast, final

import pyarrow as pa
from vgi_rpc.rpc import CallContext, RpcServer, Stream, serve_stdio

from vgi.aggregate_function import AggregateBindParams, AggregateFunction
from vgi.argument_spec import ArgumentSpec, extract_argument_specs
from vgi.arguments import Arguments
from vgi.catalog import CatalogInterface
from vgi.catalog.attach_option import AttachOptionSpec, extract_attach_option_specs
from vgi.catalog.catalog_interface import (
    AttachId,
    CatalogAttachResult,
    OnConflict,
    SchemaObjectType,
    SerializedSchema,
    SqlExpression,
    TransactionId,
    _validate_at_params,
    serialize_column_statistics,
)
from vgi.catalog.secret_type import SecretTypeSpec
from vgi.catalog.setting import SettingSpec, extract_setting_specs
from vgi.function import (
    Function,
)
from vgi.function_storage import BoundStorage
from vgi.invocation import (
    BindResponse,
    GlobalInitResponse,
)
from vgi.logging_config import LogFormat, LogLevel
from vgi.otel import VgiTracer, get_noop_tracer
from vgi.protocol import (
    BindRequest,
    CatalogAttachRequest,
    CatalogCreateRequest,
    CatalogsResponse,
    CatalogVersionResponse,
    FunctionsResponse,
    IndexCreateRequest,
    IndexesResponse,
    InitRequest,
    MacroCreateRequest,
    MacrosResponse,
    ProcessState,
    ScalarExchangeState,
    SchemasResponse,
    TableCreateRequest,
    TableFunctionCardinalityRequest,
    TableFunctionStatisticsRequest,
    TableInOutExchangeState,
    TableInOutFinalizeState,
    TableProducerState,
    TablesResponse,
    TransactionBeginResponse,
    VgiProtocol,
    ViewsResponse,
)
from vgi.scalar_function import ScalarFunctionGenerator
from vgi.table_function import (
    ProcessParams,
    SecretsAccessor,
    TableCardinality,
    TableFunctionGenerator,
    TableInOutFunctionInitPhase,
    _batch_to_scalar_dict,
    _effective_projection_ids,
    project_schema,
)
from vgi.table_in_out_function import (
    TableInOutGenerator,
)

if TYPE_CHECKING:
    from vgi.catalog.descriptors import Catalog
    from vgi.protocol import (
        AggregateBindRequest,
        AggregateBindResponse,
        AggregateCombineRequest,
        AggregateCombineResponse,
        AggregateDestructorRequest,
        AggregateDestructorResponse,
        AggregateFinalizeRequest,
        AggregateFinalizeResponse,
        AggregateUpdateRequest,
        AggregateUpdateResponse,
        AggregateWindowBatchRequest,
        AggregateWindowBatchResponse,
        AggregateWindowDestructorRequest,
        AggregateWindowDestructorResponse,
        AggregateWindowInitRequest,
        AggregateWindowInitResponse,
        AggregateWindowRequest,
        AggregateWindowResponse,
    )

_logger = logging.getLogger("vgi.worker")

_vgi_version_cache: str | None = None


def _write_port_file(path: str, port: int) -> None:
    """Write `port` to `path` atomically (tmp + rename).

    Callers (typically test harnesses) watch for the target path to appear,
    so a partially-written file would race the reader. Using rename means
    the file either doesn't exist or has the full port number.
    """
    import os
    import tempfile

    parent = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".port.", dir=parent)
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(f"{port}\n")
        os.replace(tmp, path)
    except BaseException:
        # Best-effort cleanup of the tmp on any failure before the rename.
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _get_vgi_version() -> str:
    """Return the installed vgi package version (cached)."""
    global _vgi_version_cache  # noqa: PLW0603
    if _vgi_version_cache is None:
        try:
            _vgi_version_cache = importlib.metadata.version("vgi")
        except importlib.metadata.PackageNotFoundError:
            _vgi_version_cache = "unknown"
    return _vgi_version_cache


def _format_arguments_for_error(args: Arguments) -> str:
    """Format Arguments for error messages, showing values and types.

    Produces output like:
        const_args=[3 (int64), "hello" (string)], named_args={sep: "," (string)}

    Args:
        args: The Arguments instance to format.

    Returns:
        Human-readable string showing argument values and types.

    """

    def format_scalar(scalar: Any) -> str:
        """Format a single scalar value with its type."""
        if scalar is None:
            return "null"
        elif not scalar.is_valid:
            return f"null ({scalar.type})"
        else:
            value = scalar.as_py()
            type_name = str(scalar.type)
            if isinstance(value, str):
                return f"{value!r} ({type_name})"
            elif isinstance(value, bytes):
                if len(value) > 20:
                    return f"<{len(value)} bytes> ({type_name})"
                else:
                    return f"{value!r} ({type_name})"
            else:
                return f"{value} ({type_name})"

    parts = []

    # Format positional constant arguments
    if args.positional:
        pos_strs = [format_scalar(s) for s in args.positional]
        parts.append(f"const_args=[{', '.join(pos_strs)}]")
    else:
        parts.append("const_args=[]")

    # Format named constant arguments
    if args.named:
        named_strs = [f"{name}: {format_scalar(scalar)}" for name, scalar in sorted(args.named.items())]
        parts.append(f"named_args={{{', '.join(named_strs)}}}")
    else:
        parts.append("named_args={}")

    return ", ".join(parts)


# ---------------------------------------------------------------------------
# Window partition in-process cache
# ---------------------------------------------------------------------------
# The storage layer (SQLite / Azure SQL / Cloudflare DO) is authoritative and
# makes the window path correct across multi-process deployments. But a
# single `aggregate_window` call does a BLOB read + Arrow IPC deserialize,
# and we make that call once per output row. For a 1000-row partition that's
# ~200ms of pure storage+deserialize overhead on top of the actual aggregate
# work — enough to make the window path slower than DuckDB's segment-tree
# fallback for many aggregates.
#
# Layer an in-memory cache on top of storage: populated on ``window_init``,
# read first on ``window``, invalidated on ``window_destructor`` and on the
# top-level ``aggregate_destructor`` safety sweep. Storage remains the
# authoritative source — if the cache misses (different worker process, LRU
# eviction, or a crashed-and-restarted worker) we fall through to storage.

# Cap the cache so a missed destructor in a long-running worker can't grow
# memory without bound. Eviction is correctness-safe because storage is
# authoritative.
_WINDOW_PARTITION_CACHE_MAX = 256


@dataclass(slots=True)
class _CachedWindowPartition:
    """Fully-decoded partition ready to hand to the user's ``window()``."""

    partition: Any  # vgi.aggregate_function.WindowPartition (avoid import cycle)
    output_schema: pa.Schema
    window_state: Any  # _WindowStatePlaceholder | None


class _WindowPartitionCache:
    """Process-local, thread-safe LRU of decoded window partitions.

    Keyed by ``(execution_id, partition_id)``. Kept small on purpose — a
    missed destructor bounds at ``_WINDOW_PARTITION_CACHE_MAX`` entries.
    """

    def __init__(self, max_size: int = _WINDOW_PARTITION_CACHE_MAX) -> None:
        self._entries: OrderedDict[tuple[bytes, int], _CachedWindowPartition] = OrderedDict()
        self._lock = Lock()
        self._max_size = max_size

    def get(self, execution_id: bytes, partition_id: int) -> _CachedWindowPartition | None:
        key = (execution_id, partition_id)
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                self._entries.move_to_end(key)
            return entry

    def put(self, execution_id: bytes, partition_id: int, entry: _CachedWindowPartition) -> None:
        key = (execution_id, partition_id)
        with self._lock:
            self._entries[key] = entry
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_size:
                self._entries.popitem(last=False)

    def delete(self, execution_id: bytes, partition_id: int) -> None:
        key = (execution_id, partition_id)
        with self._lock:
            self._entries.pop(key, None)

    def clear_execution(self, execution_id: bytes) -> None:
        with self._lock:
            to_drop = [k for k in self._entries if k[0] == execution_id]
            for k in to_drop:
                del self._entries[k]


_window_partition_cache = _WindowPartitionCache()


def _unpack_bool_mask(data: bytes, length: int) -> pa.BooleanArray:
    """Decode a packed-bit filter mask into a BooleanArray of the given length."""
    if not data:
        return pa.array([True] * length, type=pa.bool_())
    buf = pa.py_buffer(data)
    return cast(pa.BooleanArray, pa.Array.from_buffers(pa.bool_(), length, [None, buf]))  # type: ignore[list-item]


def _unpack_frame_stats(data: bytes) -> tuple[tuple[int, int], tuple[int, int]]:
    """Decode 4× little-endian int64 into FrameStats tuple-of-tuples."""
    if not data or len(data) < 32:
        return ((0, 0), (0, 0))
    import struct

    b0, e0, b1, e1 = struct.unpack("<qqqq", data[:32])
    return ((b0, e0), (b1, e1))


def _unpack_all_valid(data: bytes, column_count: int) -> list[bool]:
    """Decode 1-byte-per-column validity bools."""
    if not data:
        return [True] * column_count
    return [bool(b) for b in data[:column_count]]


def _serialize_schema_bytes(schema: pa.Schema) -> bytes:
    """Serialize an Arrow Schema to IPC bytes (stream format, schema only)."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema):
        pass
    return sink.getvalue().to_pybytes()


# Arrow schema for the serialized window-partition cache payload stored in
# FunctionStorage. One row per partition, all fields binary/int64.
_WINDOW_PARTITION_CACHE_FIELDS: list[pa.Field[Any]] = [
    pa.field("partition_batch", pa.binary(), nullable=False),
    pa.field("output_schema", pa.binary(), nullable=False),
    pa.field("filter_mask", pa.binary(), nullable=False),
    pa.field("frame_stats", pa.binary(), nullable=False),
    pa.field("all_valid", pa.binary(), nullable=False),
    pa.field("row_count", pa.int64(), nullable=False),
    pa.field("window_state", pa.binary(), nullable=True),
    pa.field("window_state_class_name", pa.string(), nullable=False),
]
_WINDOW_PARTITION_CACHE_SCHEMA = pa.schema(_WINDOW_PARTITION_CACHE_FIELDS)


def _encode_window_partition_cache(
    *,
    partition_batch_bytes: bytes,
    output_schema_bytes: bytes,
    filter_mask_bytes: bytes,
    frame_stats_bytes: bytes,
    all_valid_bytes: bytes,
    row_count: int,
    window_state_bytes: bytes | None,
    window_state_class_name: str,
) -> bytes:
    batch = pa.record_batch(
        {
            "partition_batch": [partition_batch_bytes],
            "output_schema": [output_schema_bytes],
            "filter_mask": [filter_mask_bytes],
            "frame_stats": [frame_stats_bytes],
            "all_valid": [all_valid_bytes],
            "row_count": [row_count],
            "window_state": [window_state_bytes],
            "window_state_class_name": [window_state_class_name],
        },
        schema=_WINDOW_PARTITION_CACHE_SCHEMA,
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _decode_window_partition_cache(data: bytes) -> dict[str, Any]:
    batch = pa.ipc.open_stream(data).read_next_batch()
    if batch.num_rows != 1:
        raise ValueError(f"Expected 1 cache row, got {batch.num_rows}")
    row = batch.to_pylist()[0]
    return row


class _WindowStatePlaceholder:
    """Lazy window-state holder passed to user's ``window()``.

    Carries the raw bytes and class name from ``window_init``'s return value.
    The user's ``window()`` implementation typically calls ``.deserialize(cls)``
    to rebuild a real dataclass instance, or inspects ``.raw_bytes`` directly.
    """

    __slots__ = ("raw_bytes", "class_name")

    def __init__(self, raw_bytes: bytes, class_name: str) -> None:
        self.raw_bytes = raw_bytes
        self.class_name = class_name

    def deserialize(self, cls: type[Any]) -> Any:
        """Deserialize the stored bytes via ``cls.deserialize_from_bytes``."""
        return cls.deserialize_from_bytes(self.raw_bytes)


def _build_scalar_result_batch(result_value: Any, output_schema: pa.Schema) -> pa.RecordBatch:
    """Build a one-row RecordBatch containing the scalar window result.

    If ``result_value`` is already a RecordBatch/Array with the right shape,
    convert it; otherwise wrap the scalar in a one-element array of the
    output column's type.
    """
    if isinstance(result_value, pa.RecordBatch):
        if result_value.num_rows != 1:
            raise ValueError(f"window() must return a scalar or a 1-row RecordBatch, got {result_value.num_rows} rows")
        return result_value

    if len(output_schema) != 1:
        raise ValueError(f"Window aggregate output_schema must have 1 field, got {len(output_schema)}")
    output_type = output_schema.field(0).type
    col_name = output_schema.field(0).name
    if isinstance(result_value, pa.Array):
        if len(result_value) != 1:
            raise ValueError(f"window() array result must have length 1, got {len(result_value)}")
        arr = result_value
    else:
        arr = pa.array([result_value], type=output_type)
    return pa.record_batch({col_name: arr}, schema=output_schema)


def _build_batch_result(results: list[Any], output_schema: pa.Schema) -> pa.RecordBatch:
    """Build a count-row RecordBatch containing the batched window results."""
    if len(output_schema) != 1:
        raise ValueError(f"Window aggregate output_schema must have 1 field, got {len(output_schema)}")
    output_type = output_schema.field(0).type
    col_name = output_schema.field(0).name
    arr = pa.array(results, type=output_type)
    return pa.record_batch({col_name: arr}, schema=output_schema)


class Worker:
    """Base class for VGI workers that host user-defined functions.

    Subclass this and define a `functions` class attribute listing your function
    classes. Function names are derived from metadata (Meta.name or snake_case
    of class name). The worker handles the VGI protocol via vgi_rpc.RpcServer.

    Multiple functions can share the same name if they have different argument
    signatures (function overloading). The worker will select the appropriate
    function based on the invocation's arguments.

    Catalog Interface:
        If `catalog_interface` is not set but `functions` is non-empty, a default
        read-only catalog interface is created automatically. This exposes the
        worker's functions via the catalog protocol, allowing clients to discover
        available functions.

        To customize the catalog, set `catalog_interface` to a CatalogInterface
        subclass. To disable the catalog entirely, set `catalog_interface = None`
        and `catalog_name = None`.

    """

    functions: Sequence[type[Function]] = []
    catalog_interface: type[CatalogInterface] | None = None
    catalog_name: str | None = "functions"  # Set to None to disable default catalog
    catalog: Catalog | None = None
    _registry: dict[str, list[type[Function]]] | None = None
    _default_catalog_interface: type[CatalogInterface] | None = None
    _setting_specs: list[SettingSpec] = []  # Extracted from Settings inner class
    _secret_type_specs: list[SecretTypeSpec] = []  # Secret types to register
    _attach_option_specs: list[AttachOptionSpec] = []  # Extracted from AttachOptions inner class

    @final
    @staticmethod
    def _validate_required_settings(func_cls: type[Function], request: BindRequest) -> None:
        """Validate required settings for a bind request."""
        meta = func_cls.get_metadata()
        if not meta.required_settings:
            return

        settings: set[str] = set()
        if request.settings is not None and request.settings.schema is not None:
            settings = set(list(request.settings.schema.names))

        missing = [s for s in meta.required_settings if s not in settings]
        if missing:
            raise ValueError(f"Function '{request.function_name}' requires settings: {missing}")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Process Settings inner class when subclassing Worker."""
        super().__init_subclass__(**kwargs)

        # Process Settings inner class if present
        if hasattr(cls, "Settings") and isinstance(cls.Settings, type):
            cls._setting_specs = extract_setting_specs(cls.Settings)
        else:
            cls._setting_specs = []

        # Process AttachOptions inner class if present
        if hasattr(cls, "AttachOptions") and isinstance(cls.AttachOptions, type):
            cls._attach_option_specs = extract_attach_option_specs(cls.AttachOptions)
        else:
            cls._attach_option_specs = []

        # Process secret_types class attribute if present
        if hasattr(cls, "secret_types") and isinstance(cls.secret_types, list):
            cls._secret_type_specs = list(cls.secret_types)
        else:
            cls._secret_type_specs = []

        # Inject settings/secret_types/attach_option_specs into explicit
        # catalog_interface if set, so catalogs()/catalog_attach() can
        # serialize them. Done once at class definition.
        if cls.catalog_interface is not None:
            if cls._setting_specs and hasattr(cls.catalog_interface, "settings"):
                cls.catalog_interface.settings = list(cls._setting_specs)
            if cls._secret_type_specs and hasattr(cls.catalog_interface, "secret_types"):
                cls.catalog_interface.secret_types = list(cls._secret_type_specs)
            if cls._attach_option_specs and hasattr(cls.catalog_interface, "attach_option_specs"):
                cls.catalog_interface.attach_option_specs = list(cls._attach_option_specs)

    @classmethod
    def _build_registry(cls) -> dict[str, list[type[Function]]]:
        """Build function name -> list of classes mapping from functions list.

        Multiple functions can share the same name if they have different
        argument signatures (overloading).

        Supports both patterns:
        - Legacy: cls.functions list
        - Declarative: cls.catalog.schemas[*].functions
        """
        if cls._registry is not None:
            return cls._registry

        registry: dict[str, list[type[Function]]] = {}

        seen: set[type[Function]] = set()

        def add_function(func_cls: type[Function]) -> None:
            if func_cls in seen:
                return
            seen.add(func_cls)
            meta = func_cls.get_metadata()
            if meta.name not in registry:
                registry[meta.name] = []
            registry[meta.name].append(func_cls)

        # Legacy pattern: functions list
        for func_cls in cls.functions:
            add_function(func_cls)

        # Declarative pattern: functions in catalog schemas
        if cls.catalog is not None:
            for schema in cls.catalog.schemas:
                for func_cls in schema.functions:
                    add_function(func_cls)

                # Auto-register functions referenced by table descriptors
                for table in schema.tables:
                    # Scan function (Table.function)
                    if table.function is not None:
                        add_function(table.function)
                    # Write functions
                    for attr in ("insert_function", "update_function", "delete_function"):
                        write_func = getattr(table, attr, None)
                        if write_func is not None:
                            add_function(write_func)

        cls._registry = registry
        return registry

    @classmethod
    def _get_catalog_interface(cls) -> type[CatalogInterface] | None:
        """Get the catalog interface to use for this worker.

        Returns the explicitly set catalog_interface if present. Otherwise:
        - If `catalog` attribute is set (new pattern), creates a default
          ReadOnlyCatalogInterface using the Catalog object.
        - If `catalog_name` and `functions` are set (legacy pattern), creates
          a default ReadOnlyCatalogInterface exposing the functions.

        Returns:
            CatalogInterface class to instantiate, or None if no catalog.

        """
        # Use explicit catalog_interface if set (settings injected in __init_subclass__)
        if cls.catalog_interface is not None:
            return cls.catalog_interface

        # Check for new Catalog object or legacy patterns
        catalog_obj = cls.catalog
        has_catalog = catalog_obj is not None
        has_legacy = cls.catalog_name is not None and cls.functions

        if not has_catalog and not has_legacy:
            return None

        # Create default catalog interface if not already created
        if cls._default_catalog_interface is None:
            from vgi.catalog import ReadOnlyCatalogInterface

            attrs: dict[str, Any] = {
                "settings": list(cls._setting_specs),
                "secret_types": list(cls._secret_type_specs),
                "attach_option_specs": list(cls._attach_option_specs),
            }

            if has_catalog:
                # New pattern: use Catalog object
                assert catalog_obj is not None
                attrs["catalog"] = catalog_obj
                attrs["catalog_name"] = catalog_obj.name
            else:
                # Legacy pattern: use class attributes
                attrs["catalog_name"] = cls.catalog_name
                attrs["functions"] = list(cls.functions)

            cls._default_catalog_interface = cast(
                type[CatalogInterface],
                type(
                    f"{cls.__name__}Catalog",
                    (ReadOnlyCatalogInterface,),
                    attrs,
                ),
            )

        return cls._default_catalog_interface

    @final
    @classmethod
    def main(cls) -> None:
        """Run this worker as a CLI application with logging options.

        By default, serves over stdin/stdout (pipe transport).
        Pass ``--http`` to serve over HTTP instead.

        Supports ``--quiet``, ``--debug``, ``--log-level``,
        ``--log-logger``, and ``--log-format`` for logging control.

        HTTP-specific options (only used with ``--http``):
        ``--host``, ``--port``, ``--prefix``, ``--cors-origins``,
        ``--describe/--no-describe``.

        Requires the ``http`` extra for HTTP mode: ``pip install vgi[http]``
        """
        import typer

        from vgi.logging_config import configure_worker_logging

        app = typer.Typer(add_completion=False)

        @app.command()
        def _run(
            quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress startup warning"),
            debug: bool = typer.Option(False, "--debug", help="Enable DEBUG on all vgi + vgi_rpc loggers"),
            log_level: LogLevel = typer.Option(LogLevel.INFO, "--log-level", help="Set log level"),  # noqa: B008
            log_logger: list[str] | None = typer.Option(  # noqa: B008
                None, "--log-logger", help="Target specific logger(s)"
            ),
            log_format: LogFormat = typer.Option(  # noqa: B008
                LogFormat.text, "--log-format", help="Stderr log format"
            ),
            # HTTP transport options
            http: bool = typer.Option(False, "--http", help="Serve over HTTP instead of stdin/stdout"),
            host: str = typer.Option("127.0.0.1", "--host", help="HTTP bind address"),
            port: int = typer.Option(0, "--port", "-p", help="HTTP port (0 = auto-select)"),
            prefix: str = typer.Option("", "--prefix", help="URL prefix for RPC endpoints"),
            cors_origins: str = typer.Option("*", "--cors-origins", help="Allowed CORS origins"),
            describe: bool = typer.Option(  # noqa: B008
                True, "--describe/--no-describe", help="Enable description pages (worker + RPC API)"
            ),
            port_file: str | None = typer.Option(
                None,
                "--port-file",
                help=(
                    "Write the bound port number (one line, no prefix) to this file before starting "
                    "to serve. For test harnesses / process managers that need the port side-channel "
                    "without parsing stdout."
                ),
            ),
        ) -> None:
            env_debug = os.environ.get("VGI_WORKER_DEBUG", "").lower() in ("1", "true", "yes")
            effective_debug = debug or env_debug
            effective_level = configure_worker_logging(
                debug=effective_debug,
                log_level=log_level,
                log_loggers=log_logger,
                log_format=log_format,
            )

            if http:
                from vgi.serve import (
                    _resolve_authenticate,
                    _resolve_oauth_resource_metadata,
                    _resolve_otel_config,
                )

                authenticate = _resolve_authenticate()
                oauth_metadata = _resolve_oauth_resource_metadata()
                otel_config = _resolve_otel_config()
                cls._run_http(
                    effective_level=effective_level,
                    host=host,
                    port=port,
                    prefix=prefix,
                    cors_origins=cors_origins,
                    describe=describe,
                    authenticate=authenticate,
                    oauth_resource_metadata=oauth_metadata,
                    otel_config=otel_config,
                    port_file=port_file,
                )
            else:
                from vgi.serve import _resolve_otel_config

                otel_config = _resolve_otel_config()
                cls(quiet=quiet, log_level=effective_level).run(otel_config=otel_config)

        app()

    @final
    @classmethod
    def main_http(cls) -> None:
        """Run this worker as a dedicated HTTP server with logging options.

        Prefer using ``main()`` with ``--http`` instead — it provides the
        same HTTP capabilities while also supporting pipe transport as the
        default.  This method is kept for backward compatibility and for
        entry points that are always HTTP-only.

        Requires the ``http`` extra: ``pip install vgi[http]``
        """
        import typer

        from vgi.logging_config import configure_worker_logging

        app = typer.Typer(add_completion=False)

        @app.command()
        def _run(
            host: str = typer.Option("127.0.0.1", "--host", "-h", help="Bind address"),
            port: int = typer.Option(0, "--port", "-p", help="Bind port (0 = auto-select)"),
            prefix: str = typer.Option("", "--prefix", help="URL prefix for RPC endpoints"),
            cors_origins: str = typer.Option("*", "--cors-origins", help="Allowed CORS origins"),
            describe: bool = typer.Option(  # noqa: B008
                True, "--describe/--no-describe", help="Enable description pages (worker + RPC API)"
            ),
            debug: bool = typer.Option(False, "--debug", help="Enable DEBUG on all vgi + vgi_rpc loggers"),
            log_level: LogLevel = typer.Option(LogLevel.INFO, "--log-level", help="Set log level"),  # noqa: B008
            log_logger: list[str] | None = typer.Option(  # noqa: B008
                None, "--log-logger", help="Target specific logger(s)"
            ),
            log_format: LogFormat = typer.Option(  # noqa: B008
                LogFormat.text, "--log-format", help="Stderr log format"
            ),
        ) -> None:
            env_debug = os.environ.get("VGI_WORKER_DEBUG", "").lower() in ("1", "true", "yes")
            effective_debug = debug or env_debug
            effective_level = configure_worker_logging(
                debug=effective_debug,
                log_level=log_level,
                log_loggers=log_logger,
                log_format=log_format,
            )

            from vgi.serve import (
                _resolve_authenticate,
                _resolve_oauth_resource_metadata,
                _resolve_otel_config,
            )

            authenticate = _resolve_authenticate()
            oauth_metadata = _resolve_oauth_resource_metadata()
            otel_config = _resolve_otel_config()
            cls._run_http(
                effective_level=effective_level,
                host=host,
                port=port,
                prefix=prefix,
                cors_origins=cors_origins,
                describe=describe,
                authenticate=authenticate,
                oauth_resource_metadata=oauth_metadata,
                otel_config=otel_config,
            )

        app()

    @classmethod
    def _run_http(
        cls,
        *,
        effective_level: int,
        host: str,
        port: int,
        prefix: str,
        cors_origins: str,
        describe: bool,
        authenticate: Any = None,
        oauth_resource_metadata: Any = None,
        otel_config: Any = None,
        port_file: str | None = None,
    ) -> None:
        """Start the worker as an HTTP server (shared by ``main`` and ``main_http``)."""
        import socket

        try:
            import waitress  # type: ignore[import-untyped]
        except ImportError:
            sys.stderr.write(
                "Error: waitress not installed.\nInstall with: pip install vgi[http]  (or: uv sync --extra http)\n"
            )
            sys.exit(1)

        if port == 0:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((host, 0))
                port = int(s.getsockname()[1])

        from vgi.serve import _resolve_describe, _resolve_signing_key, create_app

        describe = _resolve_describe(describe)
        signing_key = _resolve_signing_key()

        wsgi_app = create_app(
            cls,
            prefix=prefix,
            cors_origins=cors_origins,
            describe=describe,
            signing_key=signing_key,
            log_level=effective_level,
            authenticate=authenticate,
            oauth_resource_metadata=oauth_resource_metadata,
            otel_config=otel_config,
        )

        # Side-channel port publication for test harnesses: write the port
        # atomically (tmp + rename) so readers can watch for the file
        # appearing without racing a partial write.
        if port_file is not None:
            _write_port_file(port_file, port)

        # Machine-readable port for process managers and test harnesses
        print(f"PORT:{port}", flush=True)
        _logger.info("http_server_starting host=%s port=%d prefix=%s", host, port, prefix)
        sys.stderr.write(f"Serving {cls.__name__} on http://{host}:{port}{prefix}\n")
        sys.stderr.flush()

        waitress.serve(wsgi_app, host=host, port=port, _quiet=True)

    @staticmethod
    def _match_function_arguments(
        *,
        function_name: str,
        arguments: Arguments,
        input_schema: pa.Schema | None,
        candidates: Sequence[type[Function]],
    ) -> type[Function]:
        """Find the function that matches the invocation's arguments.

        Compares the positional and named arguments against each
        the candidate functions' arguments to find a match.  This is
        useful if a function can take different list of arguments or
        argument types.

        Args:
            function_name: The name of the candidate function
            arguments: The arguments that were used to call the function
            input_schema: The input_schema that is passed to the function,
            candidates: Sequence of function classes with the same name.

        Returns:
            The matching function class.

        Raises:
            ValueError: If no function matches or multiple functions match.

        """
        args = arguments
        num_positional = len(args.positional)
        named_keys = set(args.named.keys()) if args.named else set()

        matches: list[type[Function]] = []

        for func_cls in candidates:
            meta = func_cls.get_metadata()

            # Scalar functions vs Table functions have different argument passing:
            # - Scalar functions: column params come from input batches, only
            #   ConstParams (is_const=True) come from invocation.arguments
            # - Table functions: all params come from invocation.arguments
            is_scalar = issubclass(func_cls, ScalarFunctionGenerator)

            # Split parameters into positional and named (excluding TableInput)
            positional_params = [p for p in meta.parameters if isinstance(p.position, int) and not p.is_table_input]
            named_params = [p for p in meta.parameters if isinstance(p.position, str)]

            # Check positional arguments
            if is_scalar:
                # Scalar functions have two calling conventions:
                #
                # 1. New API (Param/ConstParam on compute()):
                #    - Column Params: bound from input batch columns by position
                #    - ConstParams: passed via invocation.arguments
                #    - Only count ConstParams for argument matching
                #
                # 2. Legacy API (no Param/ConstParam):
                #    - Column NAMES passed as positional args to specify bindings
                #    - All params come from invocation.arguments
                #
                # All scalar params are always required (no defaults).
                # Scalar functions don't support named arguments.

                # Only ConstParams come from arguments
                # Column params come from input batch
                const_params = [p for p in positional_params if p.is_const]
                expected_positional = len(const_params)
                has_varargs = any(p.is_varargs for p in const_params)

                if has_varargs:
                    # With varargs, need at least expected params
                    if num_positional < expected_positional:
                        continue
                else:
                    if num_positional != expected_positional:
                        continue  # Must match exactly

                # Scalar functions don't support named arguments
                if named_keys:
                    continue
            else:
                # Table functions: all params come from invocation.arguments
                required_positional = [p for p in positional_params if p.required]
                min_positional = len(required_positional)
                max_positional = len(positional_params)
                has_varargs = any(p.is_varargs for p in positional_params)

                if has_varargs:
                    if num_positional < min_positional:
                        continue  # Too few positional arguments
                else:
                    if not (min_positional <= num_positional <= max_positional):
                        continue  # Wrong number of positional arguments

                # Check named arguments
                valid_named_keys = {p.position for p in named_params}
                required_named_keys = {p.position for p in named_params if p.required}

                # All provided named args must be valid
                if not named_keys.issubset(valid_named_keys):
                    continue  # Unknown named argument

                # All required named args must be provided
                if not required_named_keys.issubset(named_keys):
                    continue  # Missing required named argument

            matches.append(func_cls)

        # Secondary type-based filtering when multiple overloads match by count
        if len(matches) > 1:
            matches = Worker._filter_by_argument_types(
                matches, args, input_schema, is_scalar=issubclass(matches[0], ScalarFunctionGenerator)
            )

        if len(matches) == 0:
            # Build helpful error message
            param_summaries = []
            for func_cls in candidates:
                meta = func_cls.get_metadata()
                params = [p for p in meta.parameters if not p.is_table_input]
                param_str = ", ".join(
                    f"{p.name}: {p.type_name or '?'}" + ("" if p.required else f" = {p.default}") for p in params
                )
                param_summaries.append(f"  {func_cls.__name__}({param_str})")

            # Format input schema for scalar functions
            input_schema_str = ""
            if input_schema is not None:
                cols = [f"{f.name}: {f.type}" for f in input_schema]
                input_schema_str = f"input_columns=[{', '.join(cols)}], "

            raise ValueError(
                f"No matching function '{function_name}' for arguments: "
                f"{input_schema_str}{_format_arguments_for_error(args)}. "
                f"Available overloads:\n" + "\n".join(param_summaries)
            )

        if len(matches) > 1:
            match_names = [m.__name__ for m in matches]
            raise ValueError(f"Ambiguous function call '{function_name}': multiple overloads match: {match_names}")

        return matches[0]

    @staticmethod
    def _types_compatible(actual: pa.DataType, declared: pa.DataType) -> bool:
        """Check if an actual argument type is compatible with a declared type.

        Uses type-family matching: integers match integers, strings match strings,
        etc. This handles DuckDB sending narrower types (e.g., int32 for a literal
        that fits, decimal for numeric literals) when the function declares a wider
        type.

        """
        if actual == declared:
            return True
        # Integer family: int8/16/32/64/uint8/16/32/64
        if pa.types.is_integer(actual) and pa.types.is_integer(declared):
            return True
        # Float/decimal family: float16/32/64, decimal
        if (pa.types.is_floating(actual) or pa.types.is_decimal(actual)) and (
            pa.types.is_floating(declared) or pa.types.is_decimal(declared)
        ):
            return True
        # String family: string, large_string, utf8
        if (pa.types.is_string(actual) or pa.types.is_large_string(actual)) and (
            pa.types.is_string(declared) or pa.types.is_large_string(declared)
        ):
            return True
        # Binary family: binary, large_binary
        if (pa.types.is_binary(actual) or pa.types.is_large_binary(actual)) and (
            pa.types.is_binary(declared) or pa.types.is_large_binary(declared)
        ):
            return True
        # Boolean
        return pa.types.is_boolean(actual) and pa.types.is_boolean(declared)

    _EXACT_MATCH_SCORE = 2
    _FAMILY_MATCH_SCORE = 1

    @staticmethod
    def _score_types(
        specs: list[ArgumentSpec],
        actual_types: Sequence[pa.DataType | None],
    ) -> tuple[int, bool]:
        """Score how well actual argument types match declared specs.

        Compares each spec's declared arrow_type against the corresponding
        actual type.  Elements beyond ``len(specs)`` are scored against the
        varargs spec (if any).

        Args:
            specs: Declared argument specs (ordered by position).
            actual_types: Actual types aligned 1-to-1 with *specs*, with any
                additional varargs tail elements appended.

        Returns:
            ``(score, matched)`` — cumulative score and whether all types
            were compatible.

        """
        score = 0
        varargs_spec: ArgumentSpec | None = None

        for i, spec in enumerate(specs):
            if spec.is_varargs:
                varargs_spec = spec
            if i >= len(actual_types):
                break
            if spec.is_any_type or spec.arrow_type == pa.null():
                continue
            actual = actual_types[i]
            if actual is None:
                continue
            if actual == spec.arrow_type:
                score += Worker._EXACT_MATCH_SCORE
            elif Worker._types_compatible(actual, spec.arrow_type):
                score += Worker._FAMILY_MATCH_SCORE
            else:
                return score, False

        # Score remaining varargs tail elements beyond declared specs
        if varargs_spec is not None and not varargs_spec.is_any_type and varargs_spec.arrow_type != pa.null():
            for i in range(len(specs), len(actual_types)):
                actual = actual_types[i]
                if actual is None:
                    continue
                if actual == varargs_spec.arrow_type:
                    score += Worker._EXACT_MATCH_SCORE
                elif Worker._types_compatible(actual, varargs_spec.arrow_type):
                    score += Worker._FAMILY_MATCH_SCORE
                else:
                    return score, False

        return score, True

    @staticmethod
    def _filter_by_argument_types(
        matches: list[type[Function]],
        arguments: Arguments,
        input_schema: pa.Schema | None,
        *,
        is_scalar: bool,
    ) -> list[type[Function]]:
        """Narrow overload candidates by comparing argument types.

        Called when count-based filtering leaves multiple matches.
        Uses extract_argument_specs to get declared arrow_type for each
        parameter and compares against actual argument types.

        Args:
            matches: Candidate function classes (same arg count).
            arguments: The invocation arguments.
            input_schema: Input schema for scalar functions (column types).
            is_scalar: Whether the candidates are scalar functions.

        Returns:
            Filtered list of matching candidates.

        """
        scored: list[tuple[int, type[Function]]] = []

        for func_cls in matches:
            specs = extract_argument_specs(func_cls)
            score = 0
            matched = True

            if is_scalar:
                # For scalar functions:
                # - ConstParam specs: compare against arguments.positional types
                # - Column Param specs: compare against input_schema field types
                const_specs = [s for s in specs if s.is_const]
                col_specs = [s for s in specs if not s.is_const and isinstance(s.position, int)]

                # Score ConstParam types against positional arguments
                const_types: list[pa.DataType | None] = [
                    arg.type if arg is not None else None for arg in arguments.positional
                ]
                delta, matched = Worker._score_types(const_specs, const_types)
                score += delta

                # Score column Param types against input_schema
                if matched and input_schema is not None:
                    col_types: list[pa.DataType | None] = []
                    varargs_col_spec: ArgumentSpec | None = None
                    for spec in col_specs:
                        if spec.is_varargs:
                            varargs_col_spec = spec
                        pos = spec.position
                        assert isinstance(pos, int)
                        if pos < len(input_schema):
                            col_types.append(input_schema.field(pos).type)
                        else:
                            col_types.append(None)
                    # Append varargs tail from input_schema
                    if varargs_col_spec is not None:
                        assert isinstance(varargs_col_spec.position, int)
                        varargs_start = varargs_col_spec.position + 1
                        for i in range(varargs_start, len(input_schema)):
                            col_types.append(input_schema.field(i).type)
                    delta, matched = Worker._score_types(col_specs, col_types)
                    score += delta
            else:
                # For table functions: compare arguments.positional types
                pos_specs = sorted(
                    [s for s in specs if isinstance(s.position, int) and not s.is_table_input],
                    key=lambda s: s.position,
                )
                pos_types: list[pa.DataType | None] = [
                    arg.type if arg is not None else None for arg in arguments.positional
                ]
                delta, matched = Worker._score_types(pos_specs, pos_types)
                score += delta

            if matched:
                scored.append((score, func_cls))

        if not scored:
            return []

        # Prefer candidates with highest score (most exact type matches)
        max_score = max(s for s, _ in scored)
        return [func_cls for s, func_cls in scored if s == max_score]

    @staticmethod
    def _suggest_similar_names(name: str, candidates: list[str]) -> list[str]:
        """Find function names similar to the given name.

        Uses prefix matching, substring matching, and character overlap to
        suggest likely alternatives for typos.

        Args:
            name: The unknown function name.
            candidates: List of valid function names.

        Returns:
            List of similar names, sorted by relevance.

        """
        if not candidates:
            return []

        name_lower = name.lower()
        scored: list[tuple[int, str]] = []

        for candidate in candidates:
            candidate_lower = candidate.lower()

            # Exact prefix match (highest priority)
            if candidate_lower.startswith(name_lower):
                scored.append((0, candidate))
            elif name_lower.startswith(candidate_lower):
                scored.append((1, candidate))
            # Substring matches
            elif name_lower in candidate_lower or candidate_lower in name_lower:
                scored.append((2, candidate))
            else:
                # Character overlap score (for typos)
                name_chars = set(name_lower)
                candidate_chars = set(candidate_lower)
                overlap = len(name_chars & candidate_chars)
                # Require at least half the characters to match
                if overlap > len(name_lower) // 2:
                    scored.append((10 - overlap, candidate))

        scored.sort(key=lambda x: (x[0], x[1]))
        return [candidate for _, candidate in scored]

    def _resolve_function(self, request: BindRequest) -> type[Function]:
        """Look up and disambiguate function class from registry.

        Args:
            request: The BindRequest containing function_name and arguments.

        Returns:
            The matching function class.

        Raises:
            ValueError: If function not found or ambiguous.

        """
        registry = self._build_registry()
        if request.function_name not in registry:
            available = sorted(registry.keys())
            suggestions = self._suggest_similar_names(request.function_name, available)
            msg_lines = [f"Unknown function: '{request.function_name}'"]
            if suggestions:
                msg_lines.append("  Did you mean:")
                for suggestion in suggestions[:3]:
                    msg_lines.append(f"    - {suggestion}")
            msg_lines.append(f"  Available functions: {available}")
            raise ValueError("\n".join(msg_lines))

        candidates = registry[request.function_name]
        if len(candidates) == 1:
            return candidates[0]

        return self._match_function_arguments(
            function_name=request.function_name,
            arguments=request.arguments,
            input_schema=request.input_schema,
            candidates=candidates,
        )

    def _resolve_function_by_name(
        self,
        function_name: str,
        attach_id: bytes | None = None,
        function_type: type[Function] | None = None,
    ) -> type[Function]:
        """Look up a function by name only (no argument disambiguation).

        Args:
            function_name: The name of the function to look up.
            attach_id: Optional attach ID (reserved for future catalog use).
            function_type: Optional base class to filter candidates by type.

        """
        registry = self._build_registry()
        if function_name not in registry:
            available = sorted(registry.keys())
            raise ValueError(f"Unknown function: '{function_name}'. Available: {available}")
        candidates = registry[function_name]
        if function_type is not None:
            candidates = [c for c in candidates if issubclass(c, function_type)]
            if not candidates:
                raise ValueError(
                    f"No {function_type.__name__} named '{function_name}' found. "
                    f"Candidates exist but are not {function_type.__name__}."
                )
        if len(candidates) == 1:
            return candidates[0]
        # For aggregates with overloads, return the first match
        # (overload disambiguation happens at bind time on the C++ side)
        return candidates[0]

    # ---------------------------------------------------------------------------
    # Catalog helpers
    # ---------------------------------------------------------------------------

    _catalog_instance: CatalogInterface | None = None

    def _get_catalog(self) -> CatalogInterface:
        """Get the CatalogInterface instance for this worker.

        The instance is created on first access and cached for the lifetime
        of the worker, so that state (attach IDs, created schemas, etc.)
        persists across RPC calls.

        Returns:
            CatalogInterface instance.

        Raises:
            ValueError: If no catalog interface is available.

        """
        if self._catalog_instance is not None:
            return self._catalog_instance
        catalog_class = self._get_catalog_interface()
        if catalog_class is None:
            raise ValueError(
                "CatalogInterface invocation received but no catalog is available. "
                "Either set catalog_interface class attribute to a CatalogInterface "
                "subclass, or ensure functions are defined and catalog_name is set."
            )
        self._catalog_instance = catalog_class()
        return self._catalog_instance

    @staticmethod
    def _options_batch_to_dict(batch: pa.RecordBatch | None) -> dict[str, Any]:
        """Convert an options RecordBatch (1 row, mixed types) to a dict."""
        if batch is None or batch.num_rows == 0:
            return {}
        return batch.to_pylist()[0]

    # ---------------------------------------------------------------------------
    # VgiProtocol implementation - bind/init
    # ---------------------------------------------------------------------------

    def bind(self, request: BindRequest, ctx: CallContext) -> BindResponse:
        """Resolve output schema and validate arguments.

        Implements VgiProtocol.bind().
        """
        self._vgi_tracer.set_current_span_attributes(
            {
                "vgi.function.name": request.function_name,
                "vgi.function.type": request.function_type.value,
                "vgi.principal": ctx.auth.principal,
                "vgi.auth_domain": ctx.auth.domain,
                "vgi.authenticated": ctx.auth.authenticated,
            }
        )
        func_cls = self._resolve_function(request)
        self._validate_required_settings(func_cls, request)

        instance = func_cls(logger=_logger)
        return instance.bind(request, ctx=ctx)  # type: ignore[attr-defined, no-any-return]

    def table_function_cardinality(
        self, request: TableFunctionCardinalityRequest, ctx: CallContext
    ) -> TableCardinality:
        """Estimate the cardinality of a table function's output.

        Implements VgiProtocol.table_function_cardinality().
        """
        func_cls = self._resolve_function(request.bind_call)
        if not issubclass(func_cls, TableFunctionGenerator):
            raise ValueError(
                "Cardinality estimation is only supported for table"
                f" functions, but '{func_cls.__name__}' is not a TableFunctionGenerator."
            )
        return func_cls.cardinality(func_cls._make_bind_params(request.bind_call, auth_context=ctx.auth))

    def table_function_statistics(self, request: TableFunctionStatisticsRequest, ctx: CallContext) -> bytes | None:
        """Return per-column statistics for a table function's output.

        Implements VgiProtocol.table_function_statistics(). Returns IPC bytes
        of the serialized ColumnStatistics batch (same wire shape as
        catalog_table_column_statistics_get), or None when stats are unknown.
        """
        func_cls = self._resolve_function(request.bind_call)
        if not issubclass(func_cls, TableFunctionGenerator):
            return None
        stats = func_cls.statistics(func_cls._make_bind_params(request.bind_call, auth_context=ctx.auth))
        if not stats:
            return None
        return serialize_column_statistics(stats, cache_max_age_seconds=None)

    # ========== Aggregate Function Methods ==========

    def _load_aggregate_const_args(
        self,
        func_cls: type[AggregateFunction],  # type: ignore[type-arg]
        storage: BoundStorage,
    ) -> Arguments | None:
        """Load const arguments stored during aggregate_bind (group_id=-2)."""
        from vgi.arguments import Arguments

        result = storage.aggregate_get([-2])
        if result[0] is not None:
            return Arguments.deserialize_from_bytes(result[0][1])
        return None

    def aggregate_bind(
        self,
        request: AggregateBindRequest,
        ctx: CallContext,
    ) -> AggregateBindResponse:
        """Bind an aggregate function, return output schema and execution_id."""
        from vgi.protocol import AggregateBindResponse

        func_cls = self._resolve_function_by_name(
            request.function_name, request.attach_id, function_type=AggregateFunction
        )
        if not issubclass(func_cls, AggregateFunction):
            raise TypeError(f"Function '{request.function_name}' is not an AggregateFunction (got {func_cls.__name__})")

        execution_id = uuid.uuid4().bytes
        bind_params = AggregateBindParams(
            args=request.arguments,
            input_schema=request.input_schema,
            settings=_batch_to_scalar_dict(request.settings),
            secrets=SecretsAccessor(request.secrets),
            auth_context=ctx.auth,
        )
        result = func_cls.on_bind(bind_params)

        if bind_params.secrets.needs_resolution:
            raise NotImplementedError(
                f"Aggregate function '{request.function_name}' requires secret resolution, "
                "which is not yet supported for aggregate functions."
            )

        # Store const arguments in FunctionStorage for later callbacks (group_id=-2).
        if request.arguments and request.arguments.positional:
            storage = BoundStorage(func_cls.storage, execution_id)
            storage.aggregate_put([(-2, request.arguments.serialize_to_bytes())])

        return AggregateBindResponse(
            output_schema=result.output_schema,
            execution_id=execution_id,
        )

    def aggregate_update(
        self,
        request: AggregateUpdateRequest,
        ctx: CallContext,
    ) -> AggregateUpdateResponse:
        """Accumulate rows from a DataChunk into per-group state."""
        from vgi.aggregate_function import GROUP_COLUMN_NAME
        from vgi.protocol import AggregateUpdateResponse

        func_cls = self._resolve_function_by_name(
            request.function_name, request.attach_id, function_type=AggregateFunction
        )
        if not issubclass(func_cls, AggregateFunction):
            raise TypeError(f"Function '{request.function_name}' is not an AggregateFunction (got {func_cls.__name__})")

        batch = pa.ipc.open_stream(request.input_batch).read_next_batch()
        storage = BoundStorage(func_cls.storage, request.execution_id)

        # Strip __vgi_group_id and extract group_ids
        gid_col_idx = batch.schema.get_field_index(GROUP_COLUMN_NAME)
        group_ids: pa.Int64Array = batch.column(gid_col_idx).cast(pa.int64())  # type: ignore[assignment]
        clean_batch = batch.remove_column(gid_col_idx)

        # Load existing states, create initial_state for new groups
        unique_gids: list[int] = [v.as_py() for v in group_ids.unique()]

        if func_cls.state_class is None:
            raise ValueError(f"Aggregate function '{request.function_name}' has no state_class defined")
        const_args = self._load_aggregate_const_args(func_cls, storage)
        params = ProcessParams(
            args=const_args,
            init_call=None,
            init_response=None,
            output_schema=pa.schema([]),
            settings={},
            secrets={},
            storage=storage,
            auth_context=ctx.auth,
        )
        states: dict[int, Any] = {}
        new_gids: set[int] = set()  # Track groups created in this batch
        initial_bytes: dict[int, bytes] = {}  # Snapshot initial state for new groups
        stored = storage.aggregate_get(unique_gids)
        for i, gid in enumerate(unique_gids):
            result = stored[i]
            if result is not None:
                states[gid] = func_cls.state_class.deserialize_from_bytes(result[1])
            else:
                state = func_cls.initial_state(params)
                states[gid] = state
                new_gids.add(gid)
                initial_bytes[gid] = state.serialize_to_bytes()

        # Call user's update() with column arrays and const scalars as kwargs
        kwargs: dict[str, Any] = {"states": states, "group_ids": group_ids}
        compute_params = getattr(func_cls, "_compute_params", {})
        for name, arg in compute_params.items():
            col_idx = getattr(arg, "_resolution_index", None)
            if col_idx is not None and col_idx < clean_batch.num_columns:
                if getattr(arg, "varargs", False):
                    # Varargs: collect all columns from this index onward as a list
                    kwargs[name] = [clean_batch.column(i) for i in range(col_idx, clean_batch.num_columns)]
                else:
                    kwargs[name] = clean_batch.column(col_idx)
        # Extract const values from stored arguments
        const_params = getattr(func_cls, "_const_params", {})
        const_phases = getattr(func_cls, "_const_param_phases", {})
        if const_args and const_args.positional and const_params:
            for name, arg in const_params.items():
                phase = const_phases.get(name, "all")
                if phase not in ("all", "update"):
                    continue  # Skip finalize-only params during update
                arg_idx = getattr(arg, "_resolution_index", None)
                if arg_idx is not None and arg_idx < len(const_args.positional):
                    scalar = const_args.positional[arg_idx]
                    kwargs[name] = scalar.as_py() if scalar is not None else None
        # Inject params for functions that declare it
        import inspect

        update_sig = inspect.signature(func_cls.update)
        if "params" in update_sig.parameters:
            kwargs["params"] = params
        func_cls.update(**kwargs)

        # Save updated states. Skip new groups whose state wasn't modified —
        # this ensures groups that received only NULL values (NullHandling.DEFAULT
        # skips them) don't get stored, so finalize() sees None → returns NULL.
        state_data: list[tuple[int, bytes]] = []
        for gid in states:
            serialized = states[gid].serialize_to_bytes()
            if gid in new_gids and serialized == initial_bytes[gid]:
                continue  # New group, not modified by update() — don't persist
            state_data.append((gid, serialized))
        if state_data:
            storage.aggregate_put(state_data)

        return AggregateUpdateResponse()

    def aggregate_combine(
        self,
        request: AggregateCombineRequest,
        ctx: CallContext,
    ) -> AggregateCombineResponse:
        """Merge source states into target states."""
        from vgi.protocol import AggregateCombineResponse

        func_cls = self._resolve_function_by_name(
            request.function_name, request.attach_id, function_type=AggregateFunction
        )
        if not issubclass(func_cls, AggregateFunction):
            raise TypeError(f"Function '{request.function_name}' is not an AggregateFunction (got {func_cls.__name__})")
        merge_batch = pa.ipc.open_stream(request.merge_batch).read_next_batch()
        storage = BoundStorage(func_cls.storage, request.execution_id)

        if merge_batch.num_rows == 0:
            return AggregateCombineResponse()

        source_ids: list[int] = merge_batch.column("source_group_id").to_pylist()  # type: ignore[assignment]
        target_ids: list[int] = merge_batch.column("target_group_id").to_pylist()  # type: ignore[assignment]

        all_gids: list[int] = list(set(source_ids) | set(target_ids))

        if func_cls.state_class is None:
            raise ValueError(f"Aggregate function '{request.function_name}' has no state_class defined")
        const_args = self._load_aggregate_const_args(func_cls, storage)
        params = ProcessParams(
            args=const_args,
            init_call=None,
            init_response=None,
            output_schema=pa.schema([]),
            settings={},
            secrets={},
            storage=storage,
            auth_context=ctx.auth,
        )
        states: dict[int, Any] = {}
        stored = storage.aggregate_get(all_gids)
        for i, gid in enumerate(all_gids):
            result = stored[i]
            if result is not None:
                states[gid] = func_cls.state_class.deserialize_from_bytes(result[1])
            # else: group was never updated — leave absent from states dict

        # Apply merges. Skip pairs where both source and target were never
        # updated (not in storage). If only one side exists, use
        # initial_state() for the missing side so combine() has two states.
        for src_gid, tgt_gid in zip(source_ids, target_ids, strict=True):
            src = states.get(src_gid)
            tgt = states.get(tgt_gid)
            if src is None and tgt is None:
                continue  # Neither side was ever updated — nothing to merge
            if src is None:
                src = func_cls.initial_state(params)
            if tgt is None:
                tgt = func_cls.initial_state(params)
            states[tgt_gid] = func_cls.combine(src, tgt, params)

        # Save updated targets back to storage.
        updated_targets = set(target_ids)
        state_data = [(gid, states[gid].serialize_to_bytes()) for gid in updated_targets if gid in states]
        if state_data:
            storage.aggregate_put(state_data)

        return AggregateCombineResponse()

    def aggregate_finalize(
        self,
        request: AggregateFinalizeRequest,
        ctx: CallContext,
    ) -> AggregateFinalizeResponse:
        """Produce results for a chunk of group_ids."""
        from vgi.protocol import AggregateFinalizeResponse

        func_cls = self._resolve_function_by_name(
            request.function_name, request.attach_id, function_type=AggregateFunction
        )
        if not issubclass(func_cls, AggregateFunction):
            raise TypeError(f"Function '{request.function_name}' is not an AggregateFunction (got {func_cls.__name__})")
        group_ids_batch = pa.ipc.open_stream(request.group_ids_batch).read_next_batch()
        group_ids: pa.Int64Array = group_ids_batch.column("group_id").cast(pa.int64())  # type: ignore[assignment]
        gid_list: list[int] = group_ids.to_pylist()  # type: ignore[assignment]

        storage = BoundStorage(func_cls.storage, request.execution_id)

        if func_cls.state_class is None:
            raise ValueError(f"Aggregate function '{request.function_name}' has no state_class defined")
        const_args = self._load_aggregate_const_args(func_cls, storage)
        params = ProcessParams(
            args=const_args,
            init_call=None,
            init_response=None,
            output_schema=request.output_schema,
            settings={},
            secrets={},
            storage=storage,
            auth_context=ctx.auth,
        )
        states: dict[int, Any] = {}
        stored = storage.aggregate_get(gid_list)
        for i, gid in enumerate(gid_list):
            result = stored[i]
            if result is not None:
                states[gid] = func_cls.state_class.deserialize_from_bytes(result[1])
            else:
                # Group was never updated — no entry in FunctionStorage.
                # Pass None so finalize() can return NULL (SQL standard for
                # SUM/AVG/MIN/MAX over zero rows). COUNT handles None → 0.
                states[gid] = None

        # Call user's finalize()
        result_batch = func_cls.finalize(group_ids, states, params)

        # Validate
        if result_batch.num_rows != len(gid_list):
            raise ValueError(
                f"finalize() returned {result_batch.num_rows} rows but expected {len(gid_list)} (one per group_id)"
            )

        # Serialize result batch to IPC stream bytes
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, result_batch.schema) as writer:
            writer.write_batch(result_batch)
        return AggregateFinalizeResponse(result_batch=sink.getvalue().to_pybytes())

    def aggregate_destructor(
        self,
        request: AggregateDestructorRequest,
        ctx: CallContext,
    ) -> AggregateDestructorResponse:
        """Best-effort cleanup of aggregate states."""
        from vgi.protocol import AggregateDestructorResponse

        func_cls = self._resolve_function_by_name(
            request.function_name, request.attach_id, function_type=AggregateFunction
        )
        if not issubclass(func_cls, AggregateFunction):
            raise TypeError(f"Function '{request.function_name}' is not an AggregateFunction (got {func_cls.__name__})")

        # Called once when all states have been destroyed (C++ tracks with
        # destroy_counter == group_id_counter). Clear all FunctionStorage state.
        storage = BoundStorage(func_cls.storage, request.execution_id)
        storage.aggregate_clear()
        # Safety sweep for windowed aggregates — in case a window_destructor
        # RPC was dropped mid-query.
        storage.aggregate_window_partition_clear()
        _window_partition_cache.clear_execution(request.execution_id)

        return AggregateDestructorResponse()

    # ========== Windowed Aggregate Methods ==========

    def aggregate_window_init(
        self,
        request: AggregateWindowInitRequest,
        ctx: CallContext,
    ) -> AggregateWindowInitResponse:
        """Cache a partition on the worker for windowed aggregation."""
        from vgi.aggregate_function import WindowPartition
        from vgi.protocol import AggregateWindowInitResponse

        func_cls = self._resolve_function_by_name(
            request.function_name, request.attach_id, function_type=AggregateFunction
        )
        if not issubclass(func_cls, AggregateFunction):
            raise TypeError(f"Function '{request.function_name}' is not an AggregateFunction (got {func_cls.__name__})")

        storage = BoundStorage(func_cls.storage, request.execution_id)
        const_args = self._load_aggregate_const_args(func_cls, storage)
        params = ProcessParams(
            args=const_args,
            init_call=None,
            init_response=None,
            output_schema=request.output_schema,
            settings={},
            secrets={},
            storage=storage,
            auth_context=ctx.auth,
        )

        partition_batch = pa.ipc.open_stream(request.partition_batch).read_next_batch()
        filter_mask = _unpack_bool_mask(request.filter_mask, request.row_count)
        frame_stats = _unpack_frame_stats(request.frame_stats)
        all_valid = _unpack_all_valid(request.all_valid, partition_batch.num_columns)

        partition = WindowPartition(
            inputs=partition_batch,
            row_count=request.row_count,
            filter_mask=filter_mask,
            frame_stats=frame_stats,
            all_valid=all_valid,
        )

        window_state = func_cls.window_init(partition, params)
        window_state_bytes: bytes | None = None
        if window_state is not None:
            if not hasattr(window_state, "serialize_to_bytes"):
                raise TypeError(
                    f"{func_cls.__name__}.window_init() must return an ArrowSerializableDataclass "
                    f"or None, got {type(window_state).__name__}"
                )
            window_state_bytes = window_state.serialize_to_bytes()

        payload = _encode_window_partition_cache(
            partition_batch_bytes=request.partition_batch,
            output_schema_bytes=_serialize_schema_bytes(request.output_schema),
            filter_mask_bytes=request.filter_mask,
            frame_stats_bytes=request.frame_stats,
            all_valid_bytes=request.all_valid,
            row_count=request.row_count,
            window_state_bytes=window_state_bytes,
            window_state_class_name=type(window_state).__name__ if window_state is not None else "",
        )
        storage.aggregate_window_partition_put(request.partition_id, payload)

        # Populate the in-process cache with the already-decoded partition
        # so aggregate_window() can skip the storage read + deserialize.
        cache_window_state: Any = None
        if window_state is not None and window_state_bytes is not None:
            cache_window_state = _WindowStatePlaceholder(
                raw_bytes=window_state_bytes,
                class_name=type(window_state).__name__,
            )
        _window_partition_cache.put(
            request.execution_id,
            request.partition_id,
            _CachedWindowPartition(
                partition=partition,
                output_schema=request.output_schema,
                window_state=cache_window_state,
            ),
        )

        return AggregateWindowInitResponse()

    def aggregate_window(
        self,
        request: AggregateWindowRequest,
        ctx: CallContext,
    ) -> AggregateWindowResponse:
        """Compute one output row for a windowed aggregate."""
        from vgi.protocol import AggregateWindowResponse

        func_cls = self._resolve_function_by_name(
            request.function_name, request.attach_id, function_type=AggregateFunction
        )
        if not issubclass(func_cls, AggregateFunction):
            raise TypeError(f"Function '{request.function_name}' is not an AggregateFunction (got {func_cls.__name__})")

        storage = BoundStorage(func_cls.storage, request.execution_id)
        cached = self._load_cached_window_partition(
            func_cls, request.execution_id, request.partition_id, storage, request.function_name
        )
        partition = cached.partition
        output_schema = cached.output_schema
        window_state = cached.window_state

        const_args = self._load_aggregate_const_args(func_cls, storage)
        params = ProcessParams(
            args=const_args,
            init_call=None,
            init_response=None,
            output_schema=output_schema,
            settings={},
            secrets={},
            storage=storage,
            auth_context=ctx.auth,
        )

        subframes = list(zip(request.frame_starts, request.frame_ends, strict=True))
        result_value = func_cls.window(request.rid, subframes, partition, window_state, params)

        # Build a one-row result batch matching output_schema
        result_batch = _build_scalar_result_batch(result_value, output_schema)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, result_batch.schema) as writer:
            writer.write_batch(result_batch)
        return AggregateWindowResponse(result_batch=sink.getvalue().to_pybytes())

    def _load_cached_window_partition(
        self,
        func_cls: type,
        execution_id: bytes,
        partition_id: int,
        storage: BoundStorage,
        function_name: str,
    ) -> _CachedWindowPartition:
        """Fetch the decoded partition from the in-process cache.

        Falls back to storage on a cache miss (multi-process HTTP, LRU
        eviction, or worker restart). Raises IOError if the partition is
        unknown — window_init never ran, or the destructor already fired.
        """
        from vgi.aggregate_function import WindowPartition

        cached = _window_partition_cache.get(execution_id, partition_id)
        if cached is not None:
            return cached

        payload = storage.aggregate_window_partition_get(partition_id)
        if payload is None:
            raise OSError(
                f"aggregate_window called for unknown partition_id={partition_id} "
                f"(function {function_name}); window_init never ran or destructor already fired"
            )
        decoded = _decode_window_partition_cache(payload)
        partition_batch = pa.ipc.open_stream(decoded["partition_batch"]).read_next_batch()
        output_schema = pa.ipc.open_stream(decoded["output_schema"]).schema
        filter_mask = _unpack_bool_mask(decoded["filter_mask"], decoded["row_count"])
        frame_stats = _unpack_frame_stats(decoded["frame_stats"])
        all_valid = _unpack_all_valid(decoded["all_valid"], partition_batch.num_columns)

        partition = WindowPartition(
            inputs=partition_batch,
            row_count=decoded["row_count"],
            filter_mask=filter_mask,
            frame_stats=frame_stats,
            all_valid=all_valid,
        )
        window_state: Any = None
        if decoded["window_state"] is not None:
            window_state = _WindowStatePlaceholder(
                raw_bytes=decoded["window_state"],
                class_name=decoded["window_state_class_name"],
            )
        cached = _CachedWindowPartition(
            partition=partition,
            output_schema=output_schema,
            window_state=window_state,
        )
        _window_partition_cache.put(execution_id, partition_id, cached)
        return cached

    def aggregate_window_batch(
        self,
        request: AggregateWindowBatchRequest,
        ctx: CallContext,
    ) -> AggregateWindowBatchResponse:
        """Compute ``count`` window output rows in a single batched RPC."""
        from vgi.protocol import AggregateWindowBatchResponse

        func_cls = self._resolve_function_by_name(
            request.function_name, request.attach_id, function_type=AggregateFunction
        )
        if not issubclass(func_cls, AggregateFunction):
            raise TypeError(f"Function '{request.function_name}' is not an AggregateFunction (got {func_cls.__name__})")

        storage = BoundStorage(func_cls.storage, request.execution_id)
        cached = self._load_cached_window_partition(
            func_cls, request.execution_id, request.partition_id, storage, request.function_name
        )
        partition = cached.partition
        output_schema = cached.output_schema
        window_state = cached.window_state

        const_args = self._load_aggregate_const_args(func_cls, storage)
        params = ProcessParams(
            args=const_args,
            init_call=None,
            init_response=None,
            output_schema=output_schema,
            settings={},
            secrets={},
            storage=storage,
            auth_context=ctx.auth,
        )

        # Unflatten subframes: frame_starts/frame_ends are concatenated across
        # all rows, frames_per_row[i] gives the slice length for row i.
        starts = request.frame_starts
        ends = request.frame_ends
        frames_per_row = request.frames_per_row
        if request.count != len(frames_per_row):
            raise ValueError(
                f"aggregate_window_batch: count={request.count} but frames_per_row has {len(frames_per_row)} entries"
            )

        offset = 0
        results: list[Any] = []
        for i in range(request.count):
            n = frames_per_row[i]
            subframes = [(starts[offset + k], ends[offset + k]) for k in range(n)]
            offset += n
            rid = request.row_idx + i
            results.append(func_cls.window(rid, subframes, partition, window_state, params))

        result_batch = _build_batch_result(results, output_schema)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, result_batch.schema) as writer:
            writer.write_batch(result_batch)
        return AggregateWindowBatchResponse(result_batch=sink.getvalue().to_pybytes())

    def aggregate_window_destructor(
        self,
        request: AggregateWindowDestructorRequest,
        ctx: CallContext,
    ) -> AggregateWindowDestructorResponse:
        """Evict a cached partition from storage."""
        from vgi.protocol import AggregateWindowDestructorResponse

        func_cls = self._resolve_function_by_name(
            request.function_name, request.attach_id, function_type=AggregateFunction
        )
        if not issubclass(func_cls, AggregateFunction):
            raise TypeError(f"Function '{request.function_name}' is not an AggregateFunction (got {func_cls.__name__})")

        storage = BoundStorage(func_cls.storage, request.execution_id)
        storage.aggregate_window_partition_delete(request.partition_id)
        _window_partition_cache.delete(request.execution_id, request.partition_id)
        return AggregateWindowDestructorResponse()

    # ========== Function Invocation ==========

    def init(self, request: InitRequest, ctx: CallContext) -> Stream[ProcessState, GlobalInitResponse]:
        """Initialize a function execution and return a processing stream.

        Implements VgiProtocol.init(). Creates the appropriate state object
        based on function type and creates the appropriate state object.
        """
        self._vgi_tracer.set_current_span_attributes(
            {
                "vgi.function.name": request.bind_call.function_name,
                "vgi.function.type": request.bind_call.function_type.value,
                "vgi.init.is_secondary": request.is_secondary,
                "vgi.principal": ctx.auth.principal,
                "vgi.auth_domain": ctx.auth.domain,
                "vgi.authenticated": ctx.auth.authenticated,
            }
        )
        func_cls = self._resolve_function(request.bind_call)
        instance = func_cls(logger=_logger)

        # Determine if this is a secondary init
        if request.is_secondary:
            assert request.execution_id is not None
            init_response = GlobalInitResponse(
                execution_id=request.execution_id,
                opaque_data=request.init_opaque_data,
            )
        else:
            if isinstance(instance, (TableFunctionGenerator, TableInOutGenerator)):
                init_response = instance.global_init(request, ctx=ctx)
            else:
                init_response = instance.global_init(request)  # type: ignore[attr-defined]

        self._vgi_tracer.set_current_span_attributes(
            {
                "vgi.init.execution_id": init_response.execution_id.hex(),
            }
        )
        if request.phase is not None:
            self._vgi_tracer.set_current_span_attributes(
                {
                    "vgi.init.phase": request.phase.value,
                }
            )

        # Build common ProcessParams for table/table-in-out functions
        proj_ids = _effective_projection_ids(func_cls, request.projection_ids)
        output_schema = project_schema(proj_ids, request.output_schema)

        # Determine state and input_schema based on function type
        state: ProcessState
        input_schema: pa.Schema | None

        if isinstance(instance, ScalarFunctionGenerator) and not isinstance(instance, TableInOutGenerator):
            # Scalar function: exchange state with per-batch process()
            state = ScalarExchangeState(
                _func_cls=type(instance),
                _init_call=request,
                _init_response=init_response,
                _vgi_tracer=self._vgi_tracer,
            )
            input_schema = request.bind_call.input_schema

        elif isinstance(instance, TableInOutGenerator):
            # Table-in-out function: separate INPUT and FINALIZE phases
            params = ProcessParams(
                args=type(instance)._parse_arguments(type(instance).FunctionArguments, request.bind_call.arguments),
                init_call=request,
                init_response=init_response,
                output_schema=output_schema,
                settings=_batch_to_scalar_dict(request.bind_call.settings),
                secrets=SecretsAccessor(request.bind_call.secrets).to_dict(),
                storage=BoundStorage(type(instance).storage, init_response.execution_id),
                auth_context=ctx.auth,
            )

            if request.phase == TableInOutFunctionInitPhase.INPUT:
                user_state = type(instance).initial_state(params)
                state = TableInOutExchangeState(
                    _init_call=request,
                    _init_response=init_response,
                    _func_cls=type(instance),
                    _params=params,
                    _user_state=user_state,
                    _vgi_tracer=self._vgi_tracer,
                )
                input_schema = request.bind_call.input_schema
            elif request.phase == TableInOutFunctionInitPhase.FINALIZE:
                # Pre-compute finalize batches
                finalize_batches = type(instance).finalize(params)
                state = TableInOutFinalizeState(
                    _batches=finalize_batches,
                )
                input_schema = None  # Producer — no input
            else:
                raise ValueError(f"Unknown init phase for table-in-out function: {request.phase}")

        elif isinstance(instance, TableFunctionGenerator):
            # Table function: producer state with per-tick process()
            params = ProcessParams(
                args=type(instance)._parse_arguments(type(instance).FunctionArguments, request.bind_call.arguments),
                init_call=request,
                init_response=init_response,
                output_schema=output_schema,
                settings=_batch_to_scalar_dict(request.bind_call.settings),
                secrets=SecretsAccessor(request.bind_call.secrets).to_dict(),
                storage=BoundStorage(type(instance).storage, init_response.execution_id),
                auth_context=ctx.auth,
            )
            user_state = type(instance).initial_state(params)
            state = TableProducerState(
                _init_call=request,
                _init_response=init_response,
                _func_cls=type(instance),
                _params=params,
                _user_state=user_state,
                _vgi_tracer=self._vgi_tracer,
            )
            input_schema = None  # Producer — no input

        else:
            raise ValueError(f"Unknown function type: {type(instance).__name__}")

        return Stream(
            output_schema=output_schema,
            state=state,
            input_schema=input_schema or pa.schema([]),
            header=init_response,
        )

    # ---------------------------------------------------------------------------
    # VgiProtocol implementation - Catalog Discovery
    # ---------------------------------------------------------------------------

    def _enrich_catalog_span(self, **attrs: Any) -> None:
        """Add catalog-specific attributes to the current vgi_rpc span."""
        self._vgi_tracer.set_current_span_attributes(attrs)

    def catalog_catalogs(self) -> CatalogsResponse:
        """List available catalog discovery records."""
        cat = self._get_catalog()
        return CatalogsResponse.from_infos(list(cat.catalogs()))

    # ---------------------------------------------------------------------------
    # VgiProtocol implementation - Catalog Lifecycle
    # ---------------------------------------------------------------------------

    def catalog_attach(
        self,
        request: CatalogAttachRequest,
        *,
        ctx: CallContext | None = None,
    ) -> CatalogAttachResult:
        """Attach to a catalog with options."""
        self._enrich_catalog_span(vgi_catalog_name=request.name)
        cat = self._get_catalog()
        options = self._options_batch_to_dict(request.options)
        return cat.catalog_attach(
            name=request.name,
            options=options,
            data_version_spec=request.data_version_spec,
            implementation_version=request.implementation_version,
            ctx=ctx,
        )

    def catalog_detach(self, attach_id: bytes) -> None:
        """Detach from a catalog."""
        cat = self._get_catalog()
        cat.catalog_detach(attach_id=AttachId(attach_id))

    def catalog_create(self, request: CatalogCreateRequest) -> None:
        """Create a new catalog."""
        self._enrich_catalog_span(vgi_catalog_name=request.name)
        cat = self._get_catalog()
        options = self._options_batch_to_dict(request.options)
        cat.catalog_create(name=request.name, on_conflict=request.on_conflict, options=options)

    def catalog_drop(self, name: str) -> None:
        """Drop a catalog."""
        self._enrich_catalog_span(vgi_catalog_name=name)
        cat = self._get_catalog()
        cat.catalog_drop(name=name)

    def catalog_version(
        self,
        attach_id: bytes,
        transaction_id: bytes | None = None,
        *,
        ctx: CallContext | None = None,
    ) -> CatalogVersionResponse:
        """Get the current catalog version."""
        cat = self._get_catalog()
        version = cat.catalog_version(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            ctx=ctx,
        )
        return CatalogVersionResponse(version=version)

    # ---------------------------------------------------------------------------
    # VgiProtocol implementation - Catalog Transactions
    # ---------------------------------------------------------------------------

    def catalog_transaction_begin(self, attach_id: bytes) -> TransactionBeginResponse:
        """Begin a new transaction."""
        cat = self._get_catalog()
        tx_id = cat.catalog_transaction_begin(attach_id=AttachId(attach_id))
        return TransactionBeginResponse(transaction_id=bytes(tx_id) if tx_id else None)

    def catalog_transaction_commit(self, attach_id: bytes, transaction_id: bytes) -> None:
        """Commit a transaction."""
        cat = self._get_catalog()
        cat.catalog_transaction_commit(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id),
        )

    def catalog_transaction_rollback(self, attach_id: bytes, transaction_id: bytes) -> None:
        """Rollback a transaction."""
        cat = self._get_catalog()
        cat.catalog_transaction_rollback(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id),
        )

    # ---------------------------------------------------------------------------
    # VgiProtocol implementation - Catalog Schemas
    # ---------------------------------------------------------------------------

    def catalog_schemas(self, attach_id: bytes, transaction_id: bytes | None = None) -> SchemasResponse:
        """List schemas in the catalog."""
        cat = self._get_catalog()
        infos = cat.schemas(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
        )
        return SchemasResponse.from_infos(list(infos))

    def catalog_schema_get(self, attach_id: bytes, name: str, transaction_id: bytes | None = None) -> SchemasResponse:
        """Get information about a schema. Returns 0 or 1 items."""
        self._enrich_catalog_span(vgi_schema_name=name)
        cat = self._get_catalog()
        info = cat.schema_get(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            name=name,
        )
        return SchemasResponse.from_optional(info)

    def catalog_schema_create(
        self,
        attach_id: bytes,
        name: str,
        on_conflict: OnConflict = OnConflict.ERROR,
        comment: str | None = None,
        tags: dict[str, str] | None = None,
        transaction_id: bytes | None = None,
    ) -> None:
        """Create a new schema."""
        self._enrich_catalog_span(vgi_schema_name=name)
        cat = self._get_catalog()
        cat.schema_create(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            name=name,
            on_conflict=on_conflict,
            comment=comment,
            tags=tags or {},
        )

    def catalog_schema_drop(
        self,
        attach_id: bytes,
        name: str,
        ignore_not_found: bool = False,
        cascade: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Drop a schema."""
        self._enrich_catalog_span(vgi_schema_name=name)
        cat = self._get_catalog()
        cat.schema_drop(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            name=name,
            ignore_not_found=ignore_not_found,
            cascade=cascade,
        )

    def catalog_schema_contents_tables(
        self,
        attach_id: bytes,
        name: str,
        transaction_id: bytes | None = None,
    ) -> TablesResponse:
        """List tables in a schema."""
        self._enrich_catalog_span(vgi_schema_name=name)
        cat = self._get_catalog()
        infos = cat.schema_contents(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            name=name,
            type=SchemaObjectType.TABLE,
        )
        return TablesResponse.from_infos(list(infos))

    def catalog_schema_contents_views(
        self,
        attach_id: bytes,
        name: str,
        transaction_id: bytes | None = None,
    ) -> ViewsResponse:
        """List views in a schema."""
        self._enrich_catalog_span(vgi_schema_name=name)
        cat = self._get_catalog()
        infos = cat.schema_contents(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            name=name,
            type=SchemaObjectType.VIEW,
        )
        return ViewsResponse.from_infos(list(infos))

    def catalog_schema_contents_functions(
        self,
        attach_id: bytes,
        name: str,
        type: SchemaObjectType,
        transaction_id: bytes | None = None,
    ) -> FunctionsResponse:
        """List functions in a schema (scalar or table)."""
        self._enrich_catalog_span(vgi_schema_name=name)
        cat = self._get_catalog()
        infos = cat.schema_contents(  # type: ignore[call-overload]
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            name=name,
            type=type,
        )
        return FunctionsResponse.from_infos(list(infos))

    # ---------------------------------------------------------------------------
    # VgiProtocol implementation - Catalog Tables
    # ---------------------------------------------------------------------------

    def catalog_table_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
        transaction_id: bytes | None = None,
    ) -> TablesResponse:
        """Get information about a table. Returns 0 or 1 items."""
        _validate_at_params(at_unit, at_value)
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        info = cat.table_get(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            at_unit=at_unit,
            at_value=at_value,
        )
        return TablesResponse.from_optional(info)

    def catalog_table_create(self, request: TableCreateRequest) -> None:
        """Create a new table."""
        self._enrich_catalog_span(vgi_schema_name=request.schema_name, vgi_table_name=request.name)
        cat = self._get_catalog()
        cat.table_create(
            attach_id=AttachId(request.attach_id),
            transaction_id=TransactionId(request.transaction_id) if request.transaction_id else None,
            schema_name=request.schema_name,
            name=request.name,
            columns=SerializedSchema(request.columns),
            on_conflict=request.on_conflict,
            not_null_constraints=list(request.not_null_constraints),
            unique_constraints=[list(c) for c in request.unique_constraints],
            check_constraints=list(request.check_constraints),
            primary_key_constraints=(
                [list(c) for c in request.primary_key_constraints] if request.primary_key_constraints else None
            ),
            foreign_key_constraints=(
                list(request.foreign_key_constraints) if request.foreign_key_constraints else None
            ),
        )

    def catalog_table_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        cascade: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Drop a table."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        cat.table_drop(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            ignore_not_found=ignore_not_found,
            cascade=cascade,
        )

    def catalog_table_scan_function_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
        transaction_id: bytes | None = None,
    ) -> bytes:
        """Get the scan function for a table. Returns ScanFunctionResult as IPC bytes."""
        _validate_at_params(at_unit, at_value)
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        result = cat.table_scan_function_get(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            at_unit=at_unit,
            at_value=at_value,
        )
        return result.serialize()

    def catalog_table_column_statistics_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        transaction_id: bytes | None = None,
    ) -> bytes | None:
        """Get column statistics for a table. Returns IPC bytes or None."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        result = cat.table_column_statistics_get(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
        )
        if result is None:
            return None
        return serialize_column_statistics(result.statistics, result.cache_max_age_seconds)

    def catalog_table_insert_function_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        transaction_id: bytes | None = None,
    ) -> bytes:
        """Get the insert function for a table. Returns WriteFunctionResult as IPC bytes."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        result = cat.table_insert_function_get(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
        )
        return result.serialize()

    def catalog_table_update_function_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        transaction_id: bytes | None = None,
    ) -> bytes:
        """Get the update function for a table. Returns WriteFunctionResult as IPC bytes."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        result = cat.table_update_function_get(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
        )
        return result.serialize()

    def catalog_table_delete_function_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        transaction_id: bytes | None = None,
    ) -> bytes:
        """Get the delete function for a table. Returns WriteFunctionResult as IPC bytes."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        result = cat.table_delete_function_get(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
        )
        return result.serialize()

    def catalog_table_comment_set(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        comment: str | None = None,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Set or clear the comment on a table."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        cat.table_comment_set(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            comment=comment,
            ignore_not_found=ignore_not_found,
        )

    def catalog_table_column_comment_set(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        comment: str | None = None,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Set or clear the comment on a table column."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        cat.table_column_comment_set(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            column_name=column_name,
            comment=comment,
            ignore_not_found=ignore_not_found,
        )

    def catalog_table_rename(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Rename a table."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        cat.table_rename(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            new_name=new_name,
            ignore_not_found=ignore_not_found,
        )

    def catalog_table_column_add(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_definition: bytes,
        ignore_not_found: bool = False,
        if_column_not_exists: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Add a new column to a table."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        cat.table_column_add(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            column_definition=SerializedSchema(column_definition),
            ignore_not_found=ignore_not_found,
            if_column_not_exists=if_column_not_exists,
        )

    def catalog_table_column_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
        if_column_exists: bool = False,
        cascade: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Drop a column from a table."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        cat.table_column_drop(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            column_name=column_name,
            ignore_not_found=ignore_not_found,
            if_column_exists=if_column_exists,
            cascade=cascade,
        )

    def catalog_table_column_rename(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        new_column_name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Rename a column."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        cat.table_column_rename(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            column_name=column_name,
            new_column_name=new_column_name,
            ignore_not_found=ignore_not_found,
        )

    def catalog_table_column_default_set(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        expression: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Set the default value expression for a column."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        cat.table_column_default_set(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            column_name=column_name,
            expression=SqlExpression(expression),
            ignore_not_found=ignore_not_found,
        )

    def catalog_table_column_default_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Remove the default value from a column."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        cat.table_column_default_drop(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            column_name=column_name,
            ignore_not_found=ignore_not_found,
        )

    def catalog_table_column_type_change(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_definition: bytes,
        expression: str | None = None,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Change the type of a column."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        cat.table_column_type_change(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            column_definition=SerializedSchema(column_definition),
            expression=SqlExpression(expression) if expression else None,
            ignore_not_found=ignore_not_found,
        )

    def catalog_table_not_null_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Remove NOT NULL constraint from a column."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        cat.table_not_null_drop(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            column_name=column_name,
            ignore_not_found=ignore_not_found,
        )

    def catalog_table_not_null_set(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Add NOT NULL constraint to a column."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_table_name=name)
        cat = self._get_catalog()
        cat.table_not_null_set(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            column_name=column_name,
            ignore_not_found=ignore_not_found,
        )

    # ---------------------------------------------------------------------------
    # VgiProtocol implementation - Catalog Views
    # ---------------------------------------------------------------------------

    def catalog_view_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        transaction_id: bytes | None = None,
    ) -> ViewsResponse:
        """Get information about a view. Returns 0 or 1 items."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_view_name=name)
        cat = self._get_catalog()
        info = cat.view_get(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
        )
        return ViewsResponse.from_optional(info)

    def catalog_view_create(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        definition: str,
        on_conflict: OnConflict,
        transaction_id: bytes | None = None,
    ) -> None:
        """Create a new view."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_view_name=name)
        cat = self._get_catalog()
        cat.view_create(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            definition=definition,
            on_conflict=on_conflict,
        )

    def catalog_view_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        cascade: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Drop a view."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_view_name=name)
        cat = self._get_catalog()
        cat.view_drop(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            ignore_not_found=ignore_not_found,
            cascade=cascade,
        )

    def catalog_view_rename(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Rename a view."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_view_name=name)
        cat = self._get_catalog()
        cat.view_rename(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            new_name=new_name,
            ignore_not_found=ignore_not_found,
        )

    def catalog_view_comment_set(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        comment: str | None = None,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Set or clear the comment on a view."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_view_name=name)
        cat = self._get_catalog()
        cat.view_comment_set(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            comment=comment,
            ignore_not_found=ignore_not_found,
        )

    # ---------------------------------------------------------------------------
    # VgiProtocol implementation - Catalog Macros
    # ---------------------------------------------------------------------------

    def catalog_macro_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        transaction_id: bytes | None = None,
    ) -> MacrosResponse:
        """Get information about a macro. Returns 0 or 1 items."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_macro_name=name)
        cat = self._get_catalog()
        info = cat.macro_get(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
        )
        return MacrosResponse.from_optional(info)

    def catalog_macro_create(self, request: MacroCreateRequest) -> None:
        """Create a new macro."""
        self._enrich_catalog_span(vgi_schema_name=request.schema_name, vgi_macro_name=request.name)
        cat = self._get_catalog()
        cat.macro_create(
            attach_id=AttachId(request.attach_id),
            transaction_id=TransactionId(request.transaction_id) if request.transaction_id else None,
            schema_name=request.schema_name,
            name=request.name,
            macro_type=request.macro_type,
            parameters=request.parameters,
            definition=request.definition,
            on_conflict=request.on_conflict,
            parameter_default_values=request.parameter_default_values,
        )

    def catalog_macro_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Drop a macro."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_macro_name=name)
        cat = self._get_catalog()
        cat.macro_drop(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            ignore_not_found=ignore_not_found,
        )

    def catalog_schema_contents_macros(
        self,
        attach_id: bytes,
        name: str,
        type: SchemaObjectType,
        transaction_id: bytes | None = None,
    ) -> MacrosResponse:
        """List macros in a schema (scalar or table)."""
        self._enrich_catalog_span(vgi_schema_name=name)
        cat = self._get_catalog()
        infos = cat.schema_contents(  # type: ignore[call-overload]
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            name=name,
            type=type,
        )
        return MacrosResponse.from_infos(list(infos))

    # ---------------------------------------------------------------------------
    # VgiProtocol implementation - Catalog Indexes
    # ---------------------------------------------------------------------------

    def catalog_index_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        transaction_id: bytes | None = None,
    ) -> IndexesResponse:
        """Get information about an index. Returns 0 or 1 items."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_index_name=name)
        cat = self._get_catalog()
        info = cat.index_get(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
        )
        return IndexesResponse.from_optional(info)

    def catalog_index_create(self, request: IndexCreateRequest) -> None:
        """Create a new index."""
        self._enrich_catalog_span(vgi_schema_name=request.schema_name, vgi_index_name=request.name)
        cat = self._get_catalog()
        cat.index_create(
            attach_id=AttachId(request.attach_id),
            transaction_id=TransactionId(request.transaction_id) if request.transaction_id else None,
            schema_name=request.schema_name,
            name=request.name,
            table_name=request.table_name,
            index_type=request.index_type,
            constraint_type=request.constraint_type,
            expressions=list(request.expressions),
            on_conflict=request.on_conflict,
            options=dict(request.options) if request.options else None,
        )

    def catalog_index_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        cascade: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Drop an index."""
        self._enrich_catalog_span(vgi_schema_name=schema_name, vgi_index_name=name)
        cat = self._get_catalog()
        cat.index_drop(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            ignore_not_found=ignore_not_found,
            cascade=cascade,
        )

    def catalog_schema_contents_indexes(
        self,
        attach_id: bytes,
        name: str,
        transaction_id: bytes | None = None,
    ) -> IndexesResponse:
        """List indexes in a schema."""
        self._enrich_catalog_span(vgi_schema_name=name)
        cat = self._get_catalog()
        infos = cat.schema_contents(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            name=name,
            type=SchemaObjectType.INDEX,
        )
        return IndexesResponse.from_infos(list(infos))

    # ---------------------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------------------

    def __init__(self, *, quiet: bool = False, log_level: int = logging.INFO) -> None:
        """Initialize the worker with logging.

        Args:
            quiet: If True, suppress startup logging output. Can also be enabled
                by setting the VGI_QUIET=1 environment variable.
            log_level: Numeric logging level for the ``vgi`` logger hierarchy.

        """
        self._quiet = quiet or os.environ.get("VGI_QUIET") == "1"
        self._vgi_tracer: VgiTracer = get_noop_tracer()
        logging.getLogger("vgi").setLevel(log_level)

    def run(self, otel_config: Any = None) -> None:
        """Run the worker, reading from stdin and writing to stdout.

        Args:
            otel_config: Optional ``OtelConfig`` for OpenTelemetry instrumentation.
                When provided, instruments the RPC server and creates a VGI tracer.

        """
        # Warn if stdin is a terminal - user likely ran worker directly
        if sys.stdin.isatty() and not self._quiet:
            sys.stderr.write(
                "\n"
                "Warning: This worker expects Arrow IPC binary data on stdin.\n"
                "It is not meant to be run interactively in a terminal.\n"
                "\n"
                "Usage:\n"
                "  - Use vgi-client to invoke functions\n"
                "  - Use DuckDB with VGI extension\n"
                "\n"
                "To suppress this warning: --quiet or VGI_QUIET=1\n"
                "\n"
            )
            sys.stderr.flush()

        _logger.info("worker_starting")

        try:
            server = RpcServer(VgiProtocol, self, server_version=_get_vgi_version())
            if otel_config is not None:
                from vgi_rpc.otel import instrument_server

                instrument_server(server, otel_config)
                self._vgi_tracer = VgiTracer.create(otel_config)
            serve_stdio(server)
        except KeyboardInterrupt:
            _logger.debug("worker_interrupted")
            sys.exit(130)
