"""Scalar functions: per-row transforms with single-column output.

Scalar functions are the simplest function type in VGI. They transform each
input row into exactly one output value, producing a single column of results.

Key characteristics:
- **1:1 row mapping**: Output has exactly the same number of rows as input
- **Single column output**: Output schema has exactly one column named "result"
- **No finish()**: Processing ends when the caller closes the input stream.

Common use cases:
- Mathematical operations: multiply, add, abs
- String transforms: upper, lower, concat, trim
- Type conversions: cast, parse
- Field extraction: get nested values, parse JSON fields

This module provides two base classes:

    ScalarFunction (recommended)
        Declarative API using Param/ConstParam/Returns annotations on compute().
        Also supports Setting, Secret, and OutputLength annotations.
        Override output_type() only if the output type depends on input schema.

    ScalarFunctionGenerator (advanced)
        Per-batch callback API for fine-grained control.
        Override output_type() and process().

"""

from __future__ import annotations

import contextlib
import inspect
import logging
import uuid
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast, final, get_args, get_origin, get_type_hints

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

import vgi.function
from vgi.arguments import (
    _PYTHON_TO_ARROW,
    ARRAY_CLASS_TO_DATATYPE,
    COMPLEX_ARRAY_CLASSES,
    Arg,
    Arguments,
    ConstParam,
    OutputLength,
    Param,
    Returns,
    Secret,
    SecretLookupEntry,
    _extract_setting_secret_params,
)
from vgi.function_storage import BoundStorage
from vgi.invocation import (
    BaseInitResponse,
    BindResponse,
    GlobalInitResponse,
)
from vgi.schema_utils import schema
from vgi.table_function import SecretsAccessor, _struct_scalar_to_dict

if TYPE_CHECKING:
    from vgi.protocol import BindRequest, InitRequest

logger = logging.getLogger(__name__)

__all__ = [
    "BindParameters",
    "BindResult",
    "RowCountMismatchError",
    "ScalarFunction",
    "ScalarFunctionGenerator",
    "TypeMismatchError",
]


@dataclass(slots=True, frozen=True)
class BindResult(ArrowSerializableDataclass):
    """Result of calling bind() on a scalar function.

    Unlike table functions which return a full schema, scalar functions
    return a single output type since they produce one value per row.

    Attributes:
        output_type: Arrow data type for the output value.
        opaque_data: Optional serialized data, opaque to the caller,
            that will be passed to global_init() and process().

    """

    output_type: pa.DataType
    opaque_data: ArrowSerializableDataclass | None = None


@dataclass(slots=True, frozen=True)
class BindParameters:
    """Parameters passed to a scalar function's bind() method.

    Attributes:
        constant_arguments: Constant arguments provided at query planning time.
        arguments_schema: Schema describing the input columns.
        settings: DuckDB settings as a single-row RecordBatch, or None.
        secrets: SecretsAccessor for accessing resolved and dynamic secrets.

    """

    constant_arguments: Arguments
    arguments_schema: pa.Schema
    settings: pa.RecordBatch | None
    secrets: SecretsAccessor


def _resolve_explicit_arrow_type(arrow_type: pa.DataType | type) -> pa.DataType:
    """Resolve an explicit arrow_type value to a pa.DataType.

    Handles pa.DataType instances and Python types (int/str/float/bool/bytes).

    Raises:
        TypeError: If the type cannot be converted to Arrow.

    """
    if isinstance(arrow_type, pa.DataType):
        return arrow_type
    if arrow_type in _PYTHON_TO_ARROW:
        return _PYTHON_TO_ARROW[arrow_type]
    raise TypeError(
        f"Cannot convert type '{arrow_type}' to Arrow type. "
        f"Use pa.DataType, Python type (int/str/float/bool/bytes), "
        f"or None for AnyArrow."
    )


def _param_to_arg(param: Param, base_type: type, position: int) -> Arg[Any]:
    """Convert Param dataclass to internal Arg object with type inference.

    Supports hybrid type inference:
    1. Explicit arrow_type in Param() takes priority
    2. Simple array classes (pa.Int64Array, etc.) are inferred automatically
    3. Complex/parameterized types (pa.StructArray, etc.) require explicit arrow_type
    4. pa.Array or pa.Array[Any] indicates AnyArrow (dynamic type)

    Args:
        param: The Param metadata from an Annotated type hint.
        base_type: The type from the Annotated first argument (e.g., pa.Int64Array
            from Annotated[pa.Int64Array, Param(...)]).
        position: The parameter's position in the compute() signature.

    Returns:
        Arg instance configured for columnar input.

    Raises:
        TypeError: If type cannot be determined (complex type without explicit
            arrow_type).

    """
    is_any = False
    arrow_type: pa.DataType

    # For varargs params, unwrap list[X] to get the element type X
    infer_type = base_type
    if param.varargs and get_origin(base_type) is list:
        type_args = get_args(base_type)
        if type_args:
            infer_type = type_args[0]

    if param.arrow_type is not None:
        arrow_type = _resolve_explicit_arrow_type(param.arrow_type)
    # Infer from simple array class (pa.Int64Array -> pa.int64())
    elif infer_type in ARRAY_CLASS_TO_DATATYPE:
        arrow_type = ARRAY_CLASS_TO_DATATYPE[infer_type]
    # Complex types require explicit arrow_type
    elif infer_type in COMPLEX_ARRAY_CLASSES:
        raise TypeError(
            f"{base_type.__name__} requires explicit arrow_type in Param(). "
            f"Example: Param(arrow_type=pa.list_(pa.int64()), doc='...')"
        )
    # pa.Array or generic -> AnyArrow
    else:
        # Covers pa.Array, pa.Array[Any], Any, and other generic types
        is_any = True
        arrow_type = pa.null()  # Placeholder for AnyArrow

    return Arg[Any](
        position,
        doc=param.doc,
        arrow_type=arrow_type,
        type_bound=param.type_bound,
        varargs=param.varargs,
        is_any=is_any,
    )


def _const_param_to_arg(const_param: ConstParam, base_type: type, position: int) -> Arg[Any]:
    """Convert ConstParam dataclass to internal Arg object.

    Args:
        const_param: The ConstParam metadata from an Annotated type hint.
        base_type: The type from the Annotated first argument (e.g., int from
            Annotated[int, ConstParam(...)]).
        position: The parameter's position in the const arguments.

    Returns:
        Arg instance configured for constant (scalar) input.

    Raises:
        TypeError: If the Arrow type cannot be determined.

    """
    arrow_type: pa.DataType

    if const_param.arrow_type is not None:
        arrow_type = _resolve_explicit_arrow_type(const_param.arrow_type)
    elif base_type in _PYTHON_TO_ARROW:
        # Infer from Annotated first argument
        arrow_type = _PYTHON_TO_ARROW[base_type]
    else:
        raise TypeError(
            f"Cannot infer Arrow type from {base_type}. "
            f"Use a supported type (int/str/float/bool/bytes) or specify arrow_type."
        )

    return Arg[Any](position, doc=const_param.doc, arrow_type=arrow_type, const=True)


# =============================================================================
# Descriptors for Param/ConstParam Arguments
# =============================================================================


class _ArgDescriptor:
    """Descriptor for Param arguments.

    On class access, returns the Arg metadata (position, doc, type_bound, etc.).
    On instance access, returns the resolved column value via Arg._resolve().
    """

    __slots__ = ("arg", "name")

    def __init__(self, arg: Arg[Any], name: str) -> None:
        self.arg = arg
        self.name = name

    def __get__(self, obj: object | None, _objtype: type | None = None) -> Any:
        if obj is None:
            return self.arg
        # For instance access, return the resolved value from arguments
        # This allows accessing self.param_name to get the column name/value
        return self.arg._resolve(obj)


class _ConstArgDescriptor:
    """Descriptor for constant-folded ConstParam arguments.

    Provides access to the scalar value (not array) for const parameters.
    The value is resolved from invocation.arguments and converted to Python.

    These must be separate classes because their __get__ methods return different types.
        - _ArgDescriptor returns the column value (array) for regular Param
        - _ConstArgDescriptor returns the scalar value for ConstParam
    """

    __slots__ = ("arg", "name")

    def __init__(self, arg: Arg[Any], name: str) -> None:
        self.arg = arg
        self.name = name

    def __get__(self, obj: object | None, _objtype: type | None = None) -> Any:
        if obj is None:
            return self.arg
        # For instance access, return the resolved scalar value
        return self.arg._resolve(obj)


class RowCountMismatchError(Exception):
    """Raised when scalar function output row count doesn't match input.

    Scalar functions must produce exactly one output row for each input row.
    This error indicates the compute() method returned an array with the
    wrong number of elements.

    Attributes:
        input_rows: Number of rows in the input batch.
        output_rows: Number of rows in the output batch.
        function_name: Name of the function that produced the mismatch.

    """

    def __init__(
        self,
        message: str,
        *,
        input_rows: int | None = None,
        output_rows: int | None = None,
        function_name: str = "",
    ) -> None:
        """Initialize with row count details.

        Args:
            message: Base error message.
            input_rows: Number of input rows.
            output_rows: Number of output rows.
            function_name: Name of the function class.

        """
        self.input_rows = input_rows
        self.output_rows = output_rows
        self.function_name = function_name

        if input_rows is not None and output_rows is not None:
            full_message = self._build_detailed_message(message, input_rows, output_rows)
        else:
            full_message = message

        super().__init__(full_message)

    def _build_detailed_message(self, base_message: str, input_rows: int, output_rows: int) -> str:
        """Build a detailed, helpful error message."""
        lines = [base_message, ""]

        if self.function_name:
            lines.append(f"  Function: {self.function_name}")

        lines.append(f"  Input rows:  {input_rows}")
        lines.append(f"  Output rows: {output_rows}")

        # Provide specific guidance based on the mismatch type
        lines.append("")
        if output_rows < input_rows:
            lines.append("  Problem: Output has fewer rows than input.")
            lines.append("")
            lines.append("  Possible causes:")
            lines.append("    - compute() is filtering rows (not allowed in scalar)")
            lines.append("    - compute() is aggregating (not allowed in scalar)")
            lines.append("    - Bug in array construction")
            lines.append("")
            lines.append("  Scalar functions require 1:1 row mapping.")
            lines.append("  For filtering or aggregation, use a table function.")
        else:
            lines.append("  Problem: Output has more rows than input.")
            lines.append("")
            lines.append("  Possible causes:")
            lines.append("    - compute() is expanding rows (not allowed in scalar)")
            lines.append("    - compute() is unnesting arrays")
            lines.append("    - Bug in array construction")
            lines.append("")
            lines.append("  Scalar functions require 1:1 row mapping.")
            lines.append("  For row expansion (1→N), use a table function.")

        return "\n".join(lines)


class TypeMismatchError(TypeError):
    """Raised when array type doesn't match declared parameter or return type.

    This error indicates a mismatch between the declared type in Param() or Returns()
    and the actual array type at runtime.

    Attributes:
        param_name: Name of the parameter with the type mismatch.
        expected_type: The declared Arrow type.
        actual_type: The actual Arrow type found.
        function_name: Name of the function class.

    """

    def __init__(
        self,
        message: str,
        *,
        param_name: str = "",
        expected_type: pa.DataType | None = None,
        actual_type: pa.DataType | None = None,
        function_name: str = "",
    ) -> None:
        """Initialize with type mismatch details.

        Args:
            message: Base error message.
            param_name: Name of the parameter.
            expected_type: Expected Arrow type.
            actual_type: Actual Arrow type found.
            function_name: Name of the function class.

        """
        self.param_name = param_name
        self.expected_type = expected_type
        self.actual_type = actual_type
        self.function_name = function_name

        if expected_type is not None and actual_type is not None:
            full_message = self._build_detailed_message(message, param_name, expected_type, actual_type)
        else:
            full_message = message

        super().__init__(full_message)

    def _build_detailed_message(
        self,
        base_message: str,
        param_name: str,
        expected_type: pa.DataType,
        actual_type: pa.DataType,
    ) -> str:
        """Build a detailed, helpful error message."""
        lines = [base_message, ""]

        if self.function_name:
            lines.append(f"  Function: {self.function_name}")
        if param_name:
            lines.append(f"  Parameter: {param_name}")

        lines.append(f"  Expected type: {expected_type}")
        lines.append(f"  Actual type:   {actual_type}")

        return "\n".join(lines)


class ScalarFunctionGenerator(vgi.function.Function):
    """Per-batch callback base class for scalar functions.

    This is the advanced API for scalar functions. For most use cases,
    use ScalarFunction instead, which provides a simpler compute() callback.

    Scalar functions have these constraints:
    - **1:1 row mapping**: Output row count must equal input row count
    - **Single value output**: Produces one value per input row
    - **No finalization**: Processing ends when input is exhausted

    Methods to Override
    -------------------
    output_type(params) -> pa.DataType
        Return the Arrow type for the output value. Required.

    process(...) -> pa.RecordBatch
        Process one input batch. Must return output with same row count.
        Required.

    on_bind(params) -> BindResult
        Optional. Override to perform custom bind-time logic.

    on_init(...) -> GlobalInitResponse
        Optional. Override to perform custom initialization.

    Protocol Entry Points (called by worker, do not override)
    ---------------------------------------------------------
    bind(input) -> BindResponse
        Handles the bind API call

    global_init(input) -> GlobalInitResponse
        Handles the global_init API call

    """

    @final
    @classmethod
    def _validate_row_count(cls, output_batch: pa.RecordBatch, input_batch: pa.RecordBatch) -> None:
        """Validate that output row count matches input row count."""
        if output_batch.num_rows != input_batch.num_rows:
            raise RowCountMismatchError(
                "Scalar function output must have same row count as input.",
                input_rows=input_batch.num_rows,
                output_rows=output_batch.num_rows,
                function_name=cls.__name__,
            )

    @classmethod
    @abstractmethod
    def output_type(cls, params: BindParameters) -> pa.DataType:
        """Return the Arrow type for the output value.

        Args:
            params: Bind parameters including arguments and input schema.

        """
        ...

    @classmethod
    def on_bind(
        cls,
        params: BindParameters,
    ) -> BindResult:
        """Produce the output type during the bind API call.

        Override to perform custom bind-time logic such as validating
        arguments or computing a dynamic output type.

        Args:
            params: Bind parameters including arguments and schema.

        Returns:
            BindResult with output_type and optional opaque_data.

        """
        return BindResult(cls.output_type(params))

    @final
    @classmethod
    def _validate_param_type_bounds(cls, input_schema: pa.Schema) -> None:
        """Validate type bounds for AnyArrow Param parameters at bind time.

        Checks each Param with type_bound against the input schema field types.
        This provides early error detection before any data is processed.

        Only applies to ScalarFunction subclasses that define _compute_params
        (via the Param/ConstParam annotation API). For ScalarFunctionGenerator
        subclasses that don't use annotations, this is a no-op.

        Args:
            input_schema: The input schema from the bind call.

        Raises:
            SchemaValidationError: If any column type fails type_bound.

        """
        compute_params: dict[str, Arg[Any]] | None = getattr(cls, "_compute_params", None)
        if not compute_params:
            return
        for _name, arg in compute_params.items():
            if not arg.is_any or arg.type_bound is None:
                continue
            col_idx = cast(int, arg._resolution_index)
            if arg.varargs:
                for i in range(col_idx, len(input_schema)):
                    arg.validate_type_bound(input_schema.field(i).type)
            else:
                arg.validate_type_bound(input_schema.field(col_idx).type)

    @final
    @classmethod
    def bind(
        cls,
        input: BindRequest,
    ) -> BindResponse:
        """Bind protocol entry point. Do not override; use on_bind() instead.

        Constructs BindParameters, validates type bounds, calls on_bind(),
        and wraps the result for transmission to global_init. If on_bind()
        triggers dynamic secret lookups or if compute() declares Secret()
        annotations that haven't been resolved, returns a secret scope request.

        """
        assert input.input_schema is not None
        cls._validate_param_type_bounds(input.input_schema)

        # Auto-request secrets declared via Secret() annotations on compute()
        # when they haven't been resolved yet (first bind call).
        # _secret_params is only defined on ScalarFunction, not ScalarFunctionGenerator.
        secret_params: dict[str, Secret] = getattr(cls, "_secret_params", {})
        if secret_params and not input.resolved_secrets_provided and input.secrets is None:
            entries = [
                SecretLookupEntry(
                    secret_type=secret.secret_type,
                    scope=secret.scope,
                    secret_name=secret.name,
                )
                for secret in secret_params.values()
            ]
            return BindResponse.secret_scope_request(entries)

        secrets_accessor = SecretsAccessor(input.secrets, is_retry=input.resolved_secrets_provided)
        bind_params = BindParameters(input.arguments, input.input_schema, input.settings, secrets_accessor)
        result = cls.on_bind(bind_params)

        # Check if on_bind() registered pending secret lookups
        if secrets_accessor.needs_resolution:
            return BindResponse.secret_scope_request(secrets_accessor.pending_lookups)

        return BindResponse(
            output_schema=pa.schema([pa.field("result", result.output_type)]),
            opaque_data=result.opaque_data,
        )

    @classmethod
    def on_init(
        cls,
        *,
        bind_call: BindRequest,
        opaque_data: ArrowSerializableDataclass | None,
        storage: BoundStorage,
    ) -> GlobalInitResponse:
        """Initialize the function during the init API call.

        Override to perform one-time setup that should happen after bind
        but before processing batches. The default returns max_processes=1.

        Args:
            bind_call: The original BindCall with arguments and schema.
            opaque_data: Data from on_bind(), if any was returned.
            storage: BoundStorage for storing data across calls.

        Returns:
            GlobalInitResponse with max_processes and optional opaque data.

        """
        return GlobalInitResponse()

    @final
    @classmethod
    def global_init(cls, input: InitRequest) -> GlobalInitResponse:
        """Global init protocol entry point. Do not override; use on_init() instead.

        Deserializes the wrapped bind data, calls on_init(), and
        wraps the result for transmission to process().

        """
        execution_id = uuid.uuid4().bytes
        result = cls.on_init(
            bind_call=input.bind_call,
            opaque_data=input.bind_opaque_data,
            storage=BoundStorage(cls.storage, execution_id),
        )

        return GlobalInitResponse(
            max_workers=result.max_workers,
            execution_id=execution_id,
            opaque_data=result.opaque_data,
        )

    @classmethod
    @abstractmethod
    def process(
        cls,
        *,
        batch: pa.RecordBatch,
        init_call: InitRequest,
        init_response: BaseInitResponse,
        storage: BoundStorage,
    ) -> pa.RecordBatch:
        """Process one input batch.

        Override this method to implement your scalar transformation.
        Must return an output RecordBatch with exactly the same number
        of rows as the input batch.

        Args:
            batch: The input RecordBatch to process.
            init_call: The parameters from global_init.
            init_response: The response from the init call.
            storage: BoundStorage for storing data across calls.

        Returns:
            Output RecordBatch with same row count as input.

        """
        ...


class ScalarFunction(ScalarFunctionGenerator):
    """Base class for scalar functions (1:1 row mapping, single output column).

    Scalar functions transform each input row to exactly one output value.
    Use Param/ConstParam/Returns annotations on compute() to declare types.

    Type Validation
    ---------------
    Input and output types are validated at runtime:
    - Param types are checked against actual array types
    - Returns type is checked against compute() result
    - AnyArrow parameters skip validation
    - TypeMismatchError is raised on mismatch

    Methods to Override
    -------------------
    compute(self, ...) -> pa.Array
        Transform input arrays to output. Use Param/ConstParam annotations.

    output_type(params) -> pa.DataType (classmethod)
        Override when output type depends on input schema or arguments.

    """

    # For TYPE_CHECKING, allow dynamic attribute access for Param/ConstParam
    if TYPE_CHECKING:

        def __getattr__(self, _name: str) -> Any:
            """Allow dynamic attribute access for Param/ConstParam descriptors."""
            ...

    _compute_params: dict[str, Arg[Any]]  # Regular Param() arguments (arrays)
    _const_params: dict[str, Arg[Any]]  # ConstParam() arguments (scalars)
    _setting_params: dict[str, str]  # Setting params: param_name -> setting_key
    _secret_params: dict[str, Secret]  # Secret params: param_name -> Secret instance
    _output_length_param: str | None  # OutputLength param name (batch row count)
    _returns_output_type: pa.DataType | None  # Output type from Returns()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Extract annotations from compute() signature.

        Extracts Param, ConstParam, Setting, Secret, OutputLength, and
        Returns type information from compute() parameter annotations.
        """
        super().__init_subclass__(**kwargs)

        # Skip abstract classes
        if inspect.isabstract(cls):
            return

        # Get compute method
        compute_method = getattr(cls, "compute", None)
        if compute_method is None:
            raise TypeError(f"{cls.__name__} must define a compute() method.\n\n")

        sig = inspect.signature(compute_method)

        # Try to get type hints for the compute method
        # This handles both regular annotations and PEP 563 string annotations
        hints: dict[str, Any] = {}
        with contextlib.suppress(Exception):
            hints = get_type_hints(compute_method, include_extras=True)

        # If get_type_hints failed or returned empty, try to evaluate annotations
        # manually. This handles cases where Param/ConstParam are used with
        # `from __future__ import annotations` which stores annotations as strings.
        if not hints:
            raw_annotations = getattr(compute_method, "__annotations__", {})
            # Build namespace with imports from vgi.arguments for evaluation
            from vgi import arguments as vgi_args

            # Create a mock pa module with subscriptable Array for eval
            # (pa.Array[Any] isn't subscriptable in PyArrow)
            class _MockArray:
                def __class_getitem__(cls, _item: Any) -> Any:
                    return Any

            class _MockPa:
                Array = _MockArray

                def __getattr__(self, name: str) -> Any:
                    return getattr(pa, name)

            from typing import Annotated

            eval_namespace = {
                **getattr(compute_method, "__globals__", {}),
                "Annotated": Annotated,
                "Param": vgi_args.Param,
                "ConstParam": vgi_args.ConstParam,
                "Setting": vgi_args.Setting,
                "Secret": vgi_args.Secret,
                "OutputLength": vgi_args.OutputLength,
                "Returns": vgi_args.Returns,
                "AnyArrow": vgi_args.AnyArrow,
                "pa": _MockPa(),
            }
            for name, annotation in raw_annotations.items():
                if isinstance(annotation, str):
                    with contextlib.suppress(Exception):
                        hints[name] = eval(annotation, eval_namespace)  # noqa: S307
                else:
                    hints[name] = annotation

        compute_params: dict[str, Arg[Any]] = {}
        const_params: dict[str, Arg[Any]] = {}
        output_length_param: str | None = None  # param that receives batch row count
        returns_output_type: pa.DataType | None = None

        # Check return type for Returns() annotation
        return_hint = hints.get("return")
        if return_hint is not None and hasattr(return_hint, "__metadata__"):
            # Extract Returns from Annotated[..., Returns(...)]
            for meta in return_hint.__metadata__:
                if isinstance(meta, Returns):
                    # Priority 1: Explicit arrow_type in Returns()
                    if meta.arrow_type is not None:
                        returns_output_type = meta.arrow_type
                    else:
                        # Priority 2: Infer from Annotated first argument
                        type_args = get_args(return_hint)
                        if type_args:
                            return_base_type = type_args[0]
                            if return_base_type in ARRAY_CLASS_TO_DATATYPE:
                                returns_output_type = ARRAY_CLASS_TO_DATATYPE[return_base_type]
                            elif return_base_type in COMPLEX_ARRAY_CLASSES:
                                raise TypeError(
                                    f"{return_base_type.__name__} requires explicit "
                                    f"arrow_type in Returns(). "
                                    f"Example: Returns(arrow_type=pa.list_(pa.int64()))"
                                )
                            # Else: AnyArrow (returns_output_type remains None)
                    break

        # Extract Param/ConstParam from parameter annotations
        # Track overall position (call order) for metadata/client use.
        # For const params, track resolution_index (index into Args.positional)
        # For column params, track column_index (index into batch columns)
        overall_position = 0  # Overall call order position for metadata
        column_index = 0  # Index in batch columns (for column params)
        const_index = 0  # Index in invocation.arguments.positional (for const params)
        for name in sig.parameters:
            if name == "self":
                continue

            hint = hints.get(name)
            if hint is None:
                continue

            # Check for Annotated[..., Param/ConstParam/...] pattern
            if hasattr(hint, "__metadata__"):
                for meta in hint.__metadata__:
                    # Param: column input (array)
                    if isinstance(meta, Param):
                        # Get base type from Annotated first argument for inference
                        type_args = get_args(hint)
                        base_type = type_args[0] if type_args else pa.Array
                        # Use overall position for metadata, column_index for resolution
                        arg = _param_to_arg(meta, base_type, overall_position)
                        arg._name = name
                        # Store column_index in _resolution_index for batch lookup
                        arg._resolution_index = column_index
                        compute_params[name] = arg
                        setattr(cls, name, _ArgDescriptor(arg, name))
                        overall_position += 1
                        column_index += 1
                        break

                    # ConstParam: constant input (scalar)
                    if isinstance(meta, ConstParam):
                        # Get base type from Annotated first argument
                        type_args = get_args(hint)
                        base_type = cast(type, type_args[0] if type_args else Any)
                        # Use overall position for metadata
                        arg = _const_param_to_arg(meta, base_type, overall_position)
                        arg._name = name
                        # _resolution_index points to Arguments.positional index
                        arg._resolution_index = const_index
                        const_params[name] = arg
                        setattr(cls, name, _ConstArgDescriptor(arg, name))
                        overall_position += 1
                        const_index += 1
                        break

                    # OutputLength: receives batch row count
                    if isinstance(meta, OutputLength):
                        output_length_param = name
                        # Don't increment overall_position - not a call argument
                        break

        # Extract Setting/Secret params using shared helper
        setting_params, secret_params = _extract_setting_secret_params(compute_method)

        cls._compute_params = compute_params
        cls._const_params = const_params
        cls._setting_params = setting_params
        cls._secret_params = secret_params
        cls._output_length_param = output_length_param
        cls._returns_output_type = returns_output_type

    @final
    @classmethod
    def catalog_output_schema(cls) -> pa.Schema:
        """Return output schema for catalog introspection.

        Returns the output schema with a single "result" field using the
        type from the Returns() annotation. If no explicit type was declared
        (dynamic type), returns null() with metadata indicating "any" type.
        """
        returns_type = getattr(cls, "_returns_output_type", None)
        if returns_type is None:
            # Dynamic type (no explicit Returns type)
            field = pa.field("result", pa.null(), metadata={b"vgi:any": b"true"})
            return pa.schema([field])
        return schema({"result": returns_type})

    @classmethod
    def output_type(cls, params: BindParameters) -> pa.DataType:
        """Return the Arrow type for the output column.

        Default implementation uses _returns_output_type from Returns()
        annotation. Override when the output type depends on input schema
        or arguments (use params.arguments_schema, params.constant_arguments).

        Args:
            params: Bind parameters including arguments and input schema.

        """
        if cls._returns_output_type is not None:
            return cls._returns_output_type

        raise NotImplementedError(
            f"{cls.__name__}.output_type must be overridden when using Returns() "
            f"without an explicit type (dynamic output type)."
        )

    # Note: compute() is NOT defined here. Subclasses define it with their own
    # keyword-only signature. This avoids mypy override errors for users.
    # See class docstring for compute() signature requirements.
    # Validated at class definition time by __init_subclass__.

    @final
    @classmethod
    def _extract_compute_kwargs(cls, batch: pa.RecordBatch, bind_call: BindRequest) -> dict[str, Any]:
        """Extract columns/values for compute() parameters.

        Returns dict[str, Any] because values are a mix of arrays, lists of
        arrays, and scalar values, keyed by compute() parameter names.

        Args:
            batch: Input RecordBatch.
            bind_call: The BindCall with arguments, settings, and secrets.

        Returns:
            Dict mapping parameter names to their resolved values.

        """
        kwargs: dict[str, Any] = {}

        # Regular params: extract arrays by _resolution_index (batch column index)
        for name, arg in cls._compute_params.items():
            # Use _resolution_index for batch column lookup
            col_idx = cast(int, arg._resolution_index)
            if arg.varargs:
                # Varargs: collect all remaining columns from position
                kwargs[name] = [batch.column(i) for i in range(col_idx, batch.num_columns)]
            else:
                # Regular param: extract column by index
                kwargs[name] = batch.column(col_idx)

        # Const params: extract scalar values from arguments
        for name, arg in cls._const_params.items():
            # Use _resolution_index for Arguments.positional lookup
            arg_idx = cast(int, arg._resolution_index)
            # Get the scalar value from arguments
            scalar = bind_call.arguments.positional[arg_idx]
            # Convert to Python value
            kwargs[name] = scalar.as_py() if scalar is not None else None

        # Setting params: extract pa.Scalar from settings RecordBatch
        if bind_call.settings is not None and cls._setting_params:
            settings_schema = bind_call.settings.schema
            for name, setting_key in cls._setting_params.items():
                col_idx = settings_schema.get_field_index(setting_key)
                kwargs[name] = bind_call.settings.column(col_idx)[0] if col_idx >= 0 else None

        # Secret params: extract dict[str, pa.Scalar] from secrets RecordBatch
        if bind_call.secrets is not None and cls._secret_params:
            secrets_schema = bind_call.secrets.schema
            for name, secret in cls._secret_params.items():
                col_idx = secrets_schema.get_field_index(secret.secret_type)
                kwargs[name] = _struct_scalar_to_dict(bind_call.secrets.column(col_idx)[0]) if col_idx >= 0 else None

        # OutputLength param: pass the batch row count
        if cls._output_length_param is not None:
            kwargs[cls._output_length_param] = batch.num_rows

        return kwargs

    @final
    @classmethod
    def _validate_single_param_type(cls, arg: Arg[Any], arr: pa.Array[Any], display_name: str) -> pa.Array[Any]:
        """Validate a single parameter's array type against its declaration.

        If the array type doesn't match exactly but is castable (e.g. int32→int64,
        decimal128→double), the array is cast to the expected type and returned.

        Args:
            arg: The Arg metadata for the parameter.
            arr: The actual array to validate.
            display_name: Name used in error messages (e.g. "x" or "x[0]").

        Returns:
            The (possibly cast) array.

        Raises:
            TypeMismatchError: If array type doesn't match and cannot be cast.

        """
        if arg.is_any:
            if arg.type_bound is not None:
                arg.validate_type_bound(arr.type)
            return arr
        if arg.arrow_type is not None and arr.type != arg.arrow_type:
            try:
                casted = arr.cast(arg.arrow_type)
                logger.debug("Cast parameter '%s' from %s to %s", display_name, arr.type, arg.arrow_type)
                return casted
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                raise TypeMismatchError(
                    f"Input type mismatch for parameter '{display_name}'.",
                    param_name=display_name,
                    expected_type=arg.arrow_type,
                    actual_type=arr.type,
                    function_name=cls.__name__,
                ) from None
        return arr

    @final
    @classmethod
    def _validate_param_types(cls, kwargs: dict[str, Any]) -> None:
        """Validate that input array types match declared Param types.

        For the Param/ConstParam API:
        - Validates exact type match for params with declared arrow_type
        - Validates type_bound predicates for AnyArrow params with type_bound

        Args:
            kwargs: Dict of parameter names to arrays (from _extract_compute_kwargs).

        Raises:
            TypeMismatchError: If any array type doesn't match its declared type.
            SchemaValidationError: If any array type fails type_bound validation.

        """
        for name, arg in cls._compute_params.items():
            if arg.varargs:
                kwargs[name] = [
                    cls._validate_single_param_type(arg, arr, f"{name}[{i}]") for i, arr in enumerate(kwargs[name])
                ]
            else:
                kwargs[name] = cls._validate_single_param_type(arg, kwargs[name], name)

    @final
    @classmethod
    def _validate_output_type(cls, result: pa.Array[Any]) -> None:
        """Validate that output array type matches declared Returns type.

        Args:
            result: The output array from compute().

        Raises:
            TypeMismatchError: If output type doesn't match declared type.

        """
        if cls._returns_output_type is None:
            return  # AnyArrow or not specified

        if result.type != cls._returns_output_type:
            raise TypeMismatchError(
                "Output type mismatch.",
                param_name="return",
                expected_type=cls._returns_output_type,
                actual_type=result.type,
                function_name=cls.__name__,
            )

    @classmethod
    def on_bind(cls, params: BindParameters) -> BindResult:
        """Produce the output type during the bind phase.

        Override to perform custom bind-time logic such as validating
        arguments, examining input schema, or computing a dynamic output type.

        Args:
            params: Bind parameters including arguments, input schema,
                settings, and secrets.

        Returns:
            BindResult with output_type and optional opaque_data.

        Note:
            Constant arguments needed during process() are automatically
            serialized by the protocol. The opaque_data field is for
            additional bind-time state you need to pass forward.

        """
        return BindResult(output_type=cls.output_type(params), opaque_data=None)

    @final
    @classmethod
    def process(
        cls,
        *,
        batch: pa.RecordBatch,
        init_call: InitRequest,
        init_response: BaseInitResponse,
        storage: BoundStorage,
    ) -> pa.RecordBatch:
        """Convert compute() to per-batch callback.

        This method calls your compute() method for the input batch.
        Keyword-only parameters in compute() are automatically populated
        from the batch columns.

        """
        output_schema = init_call.output_schema

        # Extract columns for keyword-only parameters
        kwargs = cls._extract_compute_kwargs(batch, init_call.bind_call)

        # Validate input types match declared Param types
        cls._validate_param_types(kwargs)

        # Call compute() defined by subclass. Cast to Any to avoid
        # attr-defined error since compute() isn't on base class.
        # and the arguments of compute() vary by subclass.
        result = cast(Any, cls).compute(**kwargs)

        # Validate output type matches declared Returns type
        cls._validate_output_type(result)

        # Create output batch from result array
        return pa.RecordBatch.from_arrays([result], schema=output_schema)
