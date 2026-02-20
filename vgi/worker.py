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
vgi.examples.worker : Example worker with built-in functions

"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast, final

import pyarrow as pa
import structlog
import structlog.stdlib
from vgi_rpc.rpc import RpcServer, Stream, serve_stdio

from vgi.arguments import Arguments
from vgi.catalog import CatalogInterface
from vgi.catalog.catalog_interface import (
    AttachId,
    CatalogAttachResult,
    OnConflict,
    SchemaObjectType,
    SerializedSchema,
    SqlExpression,
    TransactionId,
)
from vgi.catalog.setting import SettingSpec, extract_setting_specs
from vgi.function import (
    Function,
)
from vgi.function_storage import BoundStorage
from vgi.invocation import (
    BindResponse,
    GlobalInitResponse,
)
from vgi.protocol import (
    BindRequest,
    CatalogAttachRequest,
    CatalogCreateRequest,
    CatalogsResponse,
    CatalogVersionResponse,
    FunctionsResponse,
    InitRequest,
    ProcessState,
    ScalarExchangeState,
    SchemasResponse,
    TableCreateRequest,
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
    TableFunctionGenerator,
    TableInOutFunctionInitPhase,
    _batch_to_scalar_dict,
    _batch_to_secret_dict,
    project_schema,
)
from vgi.table_in_out_function import (
    TableInOutGenerator,
)

if TYPE_CHECKING:
    from vgi.catalog.descriptors import Catalog


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

    @final
    @staticmethod
    def _validate_required_secrets(func_cls: type[Function], request: BindRequest) -> None:
        """Validate required secrets for a bind request."""
        meta = func_cls.get_metadata()
        if not meta.required_secrets:
            return

        secrets: set[str] = set()
        if request.secrets is not None and request.secrets.schema is not None:
            secrets = set(list(request.secrets.schema.names))

        missing = [s for s in meta.required_secrets if s not in secrets]
        if missing:
            raise ValueError(f"Function '{request.function_name}' requires secrets: {missing}")

    def table_scan_function_get(
        self,
        *,
        attach_id: Any,
        transaction_id: Any,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> Any:
        """Override this method to provide custom scan functions for tables.

        This method is called when a table defined with explicit columns
        (not function-backed) needs to be scanned. Override in your Worker
        subclass to return a ScanFunctionResult.

        For function-backed tables (Table(function=...)), scanning is handled
        automatically and this method is not called.

        Args:
            attach_id: The attachment identifier.
            transaction_id: The transaction identifier, if any.
            schema_name: The schema name.
            name: The table name.
            at_unit: Time travel unit (optional).
            at_value: Time travel value (optional).

        Returns:
            ScanFunctionResult describing how to scan the table.

        Raises:
            NotImplementedError: Always (base implementation).

        """
        raise NotImplementedError(
            f"table_scan_function_get not implemented for table "
            f"'{schema_name}.{name}'. Override this method in your Worker "
            f"subclass to provide scan functions for tables with explicit columns."
        )

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Process Settings inner class when subclassing Worker."""
        super().__init_subclass__(**kwargs)

        # Process Settings inner class if present
        if hasattr(cls, "Settings") and isinstance(cls.Settings, type):
            cls._setting_specs = extract_setting_specs(cls.Settings)
        else:
            cls._setting_specs = []

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

        def add_function(func_cls: type[Function]) -> None:
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
        # Use explicit catalog_interface if set
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

            # Copy table_scan_function_get from Worker if overridden
            # This allows Worker subclasses to define the scan method directly
            if (
                hasattr(cls, "table_scan_function_get")
                and cls.table_scan_function_get is not Worker.table_scan_function_get
            ):
                attrs["table_scan_function_get"] = cls.table_scan_function_get

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
    def create_argument_parser(cls) -> argparse.ArgumentParser:
        """Create an argument parser with standard worker options.

        Returns an ArgumentParser configured with common worker flags.
        Subclasses can extend the returned parser with additional arguments.

        The parser includes:
            --quiet, -q: Suppress startup logging output

        Returns:
            Configured ArgumentParser instance.

        """
        parser = argparse.ArgumentParser(
            description=cls.__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument(
            "-q",
            "--quiet",
            action="store_true",
            help="Suppress startup logging output",
        )
        return parser

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

                # Check if function uses new Param/ConstParam API
                const_params = [p for p in positional_params if p.is_const]
                has_const_params = len(const_params) > 0

                if has_const_params:
                    # New API: only ConstParams come from arguments
                    # Column params come from input batch
                    expected_positional = len(const_params)
                    has_varargs = any(p.is_varargs for p in const_params)
                else:
                    # Legacy API: all params (column names) come from arguments
                    expected_positional = len(positional_params)
                    has_varargs = any(p.is_varargs for p in positional_params)

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

    def bind(self, request: BindRequest) -> BindResponse:
        """Resolve output schema and validate arguments.

        Implements VgiProtocol.bind().
        """
        func_cls = self._resolve_function(request)
        self._validate_required_settings(func_cls, request)
        self._validate_required_secrets(func_cls, request)

        instance = func_cls(logger=self.log)
        return instance.bind(request)  # type: ignore[attr-defined, no-any-return]

    def init(self, request: InitRequest) -> Stream[ProcessState, GlobalInitResponse]:
        """Initialize a function execution and return a processing stream.

        Implements VgiProtocol.init(). Creates the appropriate state object
        based on function type and creates the appropriate state object.
        """
        func_cls = self._resolve_function(request.bind_call)
        instance = func_cls(logger=self.log)

        # Determine if this is a secondary init
        if request.is_secondary:
            assert request.execution_id is not None
            init_response = GlobalInitResponse(
                execution_id=request.execution_id,
                opaque_data=request.init_opaque_data,
            )
        else:
            init_response = instance.global_init(request)  # type: ignore[attr-defined]

        # Build common ProcessParams for table/table-in-out functions
        output_schema = project_schema(request.projection_ids, request.output_schema)

        # Determine state and input_schema based on function type
        state: ProcessState
        input_schema: pa.Schema | None

        if isinstance(instance, ScalarFunctionGenerator) and not isinstance(instance, TableInOutGenerator):
            # Scalar function: exchange state with per-batch process()
            state = ScalarExchangeState(
                _func_cls=type(instance),
                _init_call=request,
                _init_response=init_response,
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
                secrets=_batch_to_secret_dict(request.bind_call.secrets),
                storage=BoundStorage(type(instance).storage, init_response.execution_id),
            )

            if request.phase == TableInOutFunctionInitPhase.INPUT:
                user_state = type(instance).initial_state(params)
                state = TableInOutExchangeState(
                    _func_cls=type(instance),
                    _params=params,
                    _user_state=user_state,
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
                secrets=_batch_to_secret_dict(request.bind_call.secrets),
                storage=BoundStorage(type(instance).storage, init_response.execution_id),
            )
            user_state = type(instance).initial_state(params)
            state = TableProducerState(
                _func_cls=type(instance),
                _params=params,
                _user_state=user_state,
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

    def catalog_catalogs(self) -> CatalogsResponse:
        """List available catalog names."""
        cat = self._get_catalog()
        return CatalogsResponse(items=cat.catalogs())

    # ---------------------------------------------------------------------------
    # VgiProtocol implementation - Catalog Lifecycle
    # ---------------------------------------------------------------------------

    def catalog_attach(self, request: CatalogAttachRequest) -> CatalogAttachResult:
        """Attach to a catalog with options."""
        cat = self._get_catalog()
        options = self._options_batch_to_dict(request.options)
        return cat.catalog_attach(name=request.name, options=options)

    def catalog_detach(self, attach_id: bytes) -> None:
        """Detach from a catalog."""
        cat = self._get_catalog()
        cat.catalog_detach(attach_id=AttachId(attach_id))

    def catalog_create(self, request: CatalogCreateRequest) -> None:
        """Create a new catalog."""
        cat = self._get_catalog()
        options = self._options_batch_to_dict(request.options)
        cat.catalog_create(name=request.name, on_conflict=request.on_conflict, options=options)

    def catalog_drop(self, name: str) -> None:
        """Drop a catalog."""
        cat = self._get_catalog()
        cat.catalog_drop(name=name)

    def catalog_version(self, attach_id: bytes, transaction_id: bytes | None = None) -> CatalogVersionResponse:
        """Get the current catalog version."""
        cat = self._get_catalog()
        version = cat.catalog_version(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
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
        return SchemasResponse.from_schema_infos(list(infos))

    def catalog_schema_get(self, attach_id: bytes, name: str, transaction_id: bytes | None = None) -> SchemasResponse:
        """Get information about a schema. Returns 0 or 1 items."""
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
        comment: str | None = None,
        tags: dict[str, str] | None = None,
        transaction_id: bytes | None = None,
    ) -> None:
        """Create a new schema."""
        cat = self._get_catalog()
        cat.schema_create(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            name=name,
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
        cat = self._get_catalog()
        infos = cat.schema_contents(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            name=name,
            type=SchemaObjectType.TABLE,
        )
        return TablesResponse.from_table_infos(list(infos))

    def catalog_schema_contents_views(
        self,
        attach_id: bytes,
        name: str,
        transaction_id: bytes | None = None,
    ) -> ViewsResponse:
        """List views in a schema."""
        cat = self._get_catalog()
        infos = cat.schema_contents(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            name=name,
            type=SchemaObjectType.VIEW,
        )
        return ViewsResponse.from_view_infos(list(infos))

    def catalog_schema_contents_functions(
        self,
        attach_id: bytes,
        name: str,
        type: SchemaObjectType,
        transaction_id: bytes | None = None,
    ) -> FunctionsResponse:
        """List functions in a schema (scalar or table)."""
        cat = self._get_catalog()
        infos = cat.schema_contents(  # type: ignore[call-overload]
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            name=name,
            type=type,
        )
        return FunctionsResponse.from_function_infos(list(infos))

    # ---------------------------------------------------------------------------
    # VgiProtocol implementation - Catalog Tables
    # ---------------------------------------------------------------------------

    def catalog_table_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        transaction_id: bytes | None = None,
    ) -> TablesResponse:
        """Get information about a table. Returns 0 or 1 items."""
        cat = self._get_catalog()
        info = cat.table_get(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
        )
        return TablesResponse.from_optional(info)

    def catalog_table_create(self, request: TableCreateRequest) -> None:
        """Create a new table."""
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
        )

    def catalog_table_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Drop a table."""
        cat = self._get_catalog()
        cat.table_drop(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            ignore_not_found=ignore_not_found,
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
        cat = self._get_catalog()
        cat.table_comment_set(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
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
        transaction_id: bytes | None = None,
    ) -> None:
        """Drop a view."""
        cat = self._get_catalog()
        cat.view_drop(
            attach_id=AttachId(attach_id),
            transaction_id=TransactionId(transaction_id) if transaction_id else None,
            schema_name=schema_name,
            name=name,
            ignore_not_found=ignore_not_found,
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
    # Lifecycle
    # ---------------------------------------------------------------------------

    def __init__(self, *, quiet: bool = False) -> None:
        """Initialize the worker with structured logging.

        Args:
            quiet: If True, suppress startup logging output. Can also be enabled
                by setting the VGI_QUIET=1 environment variable.

        """
        self._quiet = quiet or os.environ.get("VGI_QUIET") == "1"
        structlog.configure(
            processors=[
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(0),
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        )
        self.log: structlog.stdlib.BoundLogger = structlog.get_logger().bind(component="worker", pid=os.getpid())

    def run(self) -> None:
        """Run the worker, reading from stdin and writing to stdout."""
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

        self.log.info("worker_starting")

        try:
            server = RpcServer(VgiProtocol, self)
            serve_stdio(server)
        except KeyboardInterrupt:
            self.log.debug("worker_interrupted")
            sys.exit(130)
