"""Scalar functions: per-row transforms with single-column output.

Scalar functions are the simplest function type in VGI. They transform each
input row into exactly one output value, producing a single column of results.

Key characteristics:
- **1:1 row mapping**: Output has exactly the same number of rows as input
- **Single column output**: Output schema has exactly one column named "result"
- **Finalize message**: Processing ends when client sends finalize (no finish() method)

Common use cases:
- Mathematical operations: multiply, add, abs
- String transforms: upper, lower, concat, trim
- Type conversions: cast, parse
- Field extraction: get nested values, parse JSON fields

This module provides two base classes:

    ScalarFunction (recommended)
        Simple callback-based API. Define Meta.output_type and compute().
        Override output_type property only if output depends on input schema.

    ScalarFunctionGenerator (advanced)
        Generator-based API for fine-grained control over logging.
        Override output_schema and process().

Example (static output type)::

    from typing import Annotated

    class UpperCase(ScalarFunction):
        class Meta:
            output_type = pa.string()  # Always returns string

        column: Annotated[str, Arg(0, doc="String value to uppercase")]

        def compute(self, *, column: pa.Array) -> pa.Array:
            # Keyword-only params match Arg attribute names
            return pc.utf8_upper(column)

Example (dynamic output type - depends on input)::

    from typing import Annotated

    class DoubleValue(ScalarFunction):
        class Meta:
            output_type = AnyArrow  # Output type depends on input

        column: Annotated[AnyArrowValue, Arg(0, doc="Numeric value to double")]

        @property
        def output_type(self) -> pa.DataType:
            return self.input_schema.field(self.column.value).type

        def compute(self, *, column: pa.Array) -> pa.Array:
            return pc.multiply(column, 2)

"""

from __future__ import annotations

import contextlib
import inspect
from abc import abstractmethod
from collections.abc import Generator
from typing import TYPE_CHECKING, Any, cast, final, get_type_hints

import pyarrow as pa
import structlog

import vgi.function
import vgi.log
from vgi.arguments import AnyArrow, Arg, _OutputType
from vgi.output_complete import OutputComplete
from vgi.protocol_types import ProtocolInput
from vgi.table_function import Output, ProtocolOutput

__all__ = [
    "ProtocolInput",
    "RowCountMismatchError",
    "ScalarFunction",
    "ScalarFunctionGenerator",
    "ScalarOutputGenerator",
    "ScalarOutputType",
    "TypeMismatchError",
]

# Type alias for scalar function output type declarations.
# Use pa.DataType for static output types, or AnyArrow for dynamic types
# that depend on input schema.
ScalarOutputType = pa.DataType | type[AnyArrow]


# =============================================================================
# Descriptors for Param/ConstParam Arguments
# =============================================================================


class _ArgDescriptor:
    """Descriptor for regular (per-batch) Param arguments.

    Provides access to the Arg metadata. The actual array value is passed
    directly to compute() - this descriptor is for accessing the Arg's
    metadata like position, doc, type_bound, etc.
    """

    __slots__ = ("arg", "name")

    def __init__(self, arg: Arg[Any], name: str) -> None:
        self.arg = arg
        self.name = name

    def __get__(self, obj: object | None, objtype: type | None = None) -> Any:
        if obj is None:
            return self.arg
        # For instance access, return the resolved value from arguments
        # This allows accessing self.param_name to get the column name/value
        return self.arg._resolve(obj)


class _ConstArgDescriptor:
    """Descriptor for constant-folded ConstParam arguments.

    Provides access to the scalar value (not array) for const parameters.
    The value is resolved from invocation.arguments and converted to Python.
    """

    __slots__ = ("arg", "name")

    def __init__(self, arg: Arg[Any], name: str) -> None:
        self.arg = arg
        self.name = name

    def __get__(self, obj: object | None, objtype: type | None = None) -> Any:
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
            full_message = self._build_detailed_message(
                message, input_rows, output_rows
            )
        else:
            full_message = message

        super().__init__(full_message)

    def _build_detailed_message(
        self, base_message: str, input_rows: int, output_rows: int
    ) -> str:
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
            full_message = self._build_detailed_message(
                message, param_name, expected_type, actual_type
            )
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


# Generator type for scalar function output.
# Must yield Output or Message (never None) since scalars always produce output.
ScalarOutputGenerator = Generator[vgi.log.Message | Output, pa.RecordBatch | None, None]


# ProtocolInput imported from vgi.protocol_types


class ScalarFunctionGenerator(vgi.function.Function[vgi.function.FunctionInitInput]):
    """Generator-based base class for scalar functions.

    This is the advanced API for scalar functions. For most use cases,
    use ScalarFunction instead, which provides a simpler compute() callback.

    Scalar functions have these constraints:
    - **1:1 row mapping**: Output row count must equal input row count
    - **Single column**: Output schema has exactly one column
    - **No finalization**: Processing ends when input is exhausted

    Override process() to implement the generator protocol:

        def process(self, batch: pa.RecordBatch) -> ScalarOutputGenerator:
            _ = yield Output(self.empty_output_batch)  # Priming yield
            while True:
                # Optional: yield log messages
                yield Message(Level.INFO, f"Processing {batch.num_rows} rows")

                result_array = compute_result(batch)
                output_batch = pa.RecordBatch.from_arrays(
                    [result_array], schema=self.output_schema
                )
                batch = yield Output(output_batch)
                if batch is None:
                    break

    Methods to Override
    -------------------
    output_schema : pa.Schema (property)
        Define the single-column output schema.

    process(batch) : ScalarOutputGenerator
        Generator that processes batches. Must yield Output or Message.

    setup() : None
        Called before processing. Acquire resources here.

    teardown() : None
        Called after processing. Release resources here.

    Available Attributes
    --------------------
    self.invocation : Invocation
        The complete invocation request with function name and arguments.

    self.input_schema : pa.Schema
        Schema of input batches (from invocation).

    self.output_schema : pa.Schema
        Schema of output batches (single column).

    self.empty_output_batch : pa.RecordBatch
        Empty batch conforming to output_schema, useful for priming yields.

    """

    # InitInputType inferred from generic parameter Function[FunctionInitInput]

    def __init__(
        self,
        invocation: vgi.invocation.Invocation,
        logger: structlog.stdlib.BoundLogger,
    ):
        """Initialize the scalar function with invocation data and logger."""
        super().__init__(invocation=invocation, logger=logger)

    def _validate_input_schema_requirement(self) -> None:
        """Validate that input_schema is provided for scalar functions."""
        if self.invocation.input_schema is None:
            raise ValueError(
                f"{type(self).__name__} requires an input schema, but none was "
                f"provided. ScalarFunction processes input batches and requires "
                f"input_schema to be set in the Invocation."
            )

    # input_schema property and _validate_input_schema inherited from Function

    @final
    def _validate_row_count(
        self, output_batch: pa.RecordBatch, input_batch: pa.RecordBatch
    ) -> None:
        """Validate that output row count matches input row count."""
        if output_batch.num_rows != input_batch.num_rows:
            raise RowCountMismatchError(
                "Scalar function output must have same row count as input.",
                input_rows=input_batch.num_rows,
                output_rows=output_batch.num_rows,
                function_name=type(self).__name__,
            )

    @final
    def _process_and_validate(
        self,
        generator: ScalarOutputGenerator,
        input_batch: pa.RecordBatch,
    ) -> OutputComplete:
        """Process a batch and validate schemas and row count.

        Args:
            generator: The user's process() generator.
            input_batch: The input RecordBatch to process.

        Returns:
            OutputComplete with validated output batch.

        Raises:
            SchemaValidationError: If input or output batch schema doesn't match.
            RowCountMismatchError: If output row count doesn't match input.

        """
        self._validate_input_schema(input_batch)
        result: OutputComplete = OutputComplete.from_process_result(
            generator.send(input_batch),
            self.empty_output_batch,
        )
        self._validate_output_schema(result.batch)
        # Validate row count for actual output (not log messages)
        if result.log_message is None:
            self._validate_row_count(result.batch, input_batch)
        return result

    @final
    def _process_with_exception_handling(
        self,
        generator: ScalarOutputGenerator,
        input_batch: pa.RecordBatch,
    ) -> OutputComplete:
        """Process a batch with exception handling.

        Wraps _process_and_validate to catch exceptions and convert them
        to OutputComplete with an error log message.
        """
        try:
            return self._process_and_validate(generator, input_batch)
        except Exception as e:
            return self._create_error_output(e)

    # _should_terminate inherited from Function

    @abstractmethod
    def process(self, batch: pa.RecordBatch) -> ScalarOutputGenerator:
        """Process input batches.

        Override this method to implement your scalar transformation.

        Args:
            batch: First input batch (subsequent batches via yield return).

        Yields:
            Output: Batch with same row count as input.
            Message: Log message (input will be re-sent).

        """
        ...

    @final
    def run(self) -> Generator[ProtocolOutput, ProtocolInput | None, None]:
        """Run the scalar function protocol. Do not override.

        This generator implements the SETUP -> DATA -> TEARDOWN lifecycle.
        The generator is closed by the caller when input is exhausted.

        Protocol:
            - Caller primes with next() or send(None)
            - Caller sends ProtocolInput for each batch
            - When input exhausted, caller closes the generator
        """
        # Priming yield - caller calls next() or send(None)
        input: ProtocolInput | None = yield ProtocolOutput(batch=None)
        if input is None:
            raise ValueError("Expected ProtocolInput, got None")

        # Acquire resources before processing
        self.setup()

        generator = self.process(input.batch)
        # Prime the process() generator past the initial yield
        generator.send(None)

        try:
            # DATA phase - process batches until generator is closed
            while True:
                result = self._process_with_exception_handling(generator, input.batch)

                input = yield ProtocolOutput(
                    batch=result.batch,
                    log_message=result.log_message,
                )
                if input is None:
                    raise ValueError("Expected ProtocolInput, got None")
                if self._should_terminate(result):
                    return
        finally:
            generator.close()
            # Release resources after processing completes
            self.teardown()


class ScalarFunction(ScalarFunctionGenerator):
    """Base class for scalar functions (1:1 row mapping, single output column).

    Scalar functions transform each input row to exactly one output value.
    Use Param/ConstParam/Returns annotations on compute() to declare types.

    Minimal Example
    ---------------
    ::

        class Upper(ScalarFunction):
            def compute(
                self,
                col: Param(pa.string(), "Input string"),
            ) -> Returns(pa.string()):
                return pc.utf8_upper(col)

    With Constant Argument
    ----------------------
    Use ConstParam for values known at planning time (not per-row arrays)::

        class Multiply(ScalarFunction):
            def compute(
                self,
                col: Param(pa.int64(), "Column to multiply"),
                factor: ConstParam(int, "Multiplication factor"),
            ) -> Returns(pa.int64()):
                # factor is Python int, not pa.Array
                return pc.multiply(col, factor)

    With Dynamic Output Type (AnyArrow)
    -----------------------------------
    Use AnyArrow when output type depends on input schema::

        class Double(ScalarFunction):
            _output_type: pa.DataType

            def bind(self) -> None:
                self._output_type = self.input_schema.field(0).type

            @property
            def output_type(self) -> pa.DataType:
                return self._output_type

            def compute(
                self,
                column: Param(AnyArrow, "Numeric value"),
            ) -> Returns(AnyArrow):
                return pc.multiply(column, 2)

    Type Validation
    ---------------
    Input and output types are validated at runtime:
    - Param types are checked against actual array types
    - Returns type is checked against compute() result
    - AnyArrow parameters skip validation
    - TypeMismatchError is raised on mismatch

    Legacy API (Arg Descriptors)
    ----------------------------
    The older Arg descriptor API is still supported::

        class UpperCase(ScalarFunction):
            class Meta:
                output_type = pa.string()

            column: Annotated[str, Arg(0, doc="String to uppercase")]

            def compute(self, *, column: pa.Array) -> pa.Array:
                return pc.utf8_upper(column)

    Methods to Override
    -------------------
    compute(self, ...) -> pa.Array
        Transform input arrays to output. Use Param/ConstParam annotations.

    bind(self) -> None
        Called after init. Use for schema-dependent initialization.

    output_type -> pa.DataType (property)
        Override when using Returns(AnyArrow) to return actual type.

    setup(self) -> None
        Called before processing. Acquire resources here.

    teardown(self) -> None
        Called after processing. Release resources here.

    Available Attributes
    --------------------
    self.input_schema : pa.Schema
        Schema of input batches.

    self.output_schema : pa.Schema
        Schema of output (single column named "result").

    self.invocation : Invocation
        The invocation request with function name and arguments.

    """

    # For TYPE_CHECKING, allow dynamic attribute access for Param/ConstParam
    if TYPE_CHECKING:

        def __getattr__(self, name: str) -> Any:
            """Allow dynamic attribute access for Param/ConstParam descriptors."""
            ...

    _pending_messages: list[vgi.log.Message]
    _compute_kwonly_params: dict[str, Arg[Any]]
    # New API: separate tracking for const params
    _compute_params: dict[str, Arg[Any]]  # Regular Param() arguments (arrays)
    _const_params: dict[str, Arg[Any]]  # ConstParam() arguments (scalars)
    _returns_output_type: pa.DataType | None  # Output type from Returns()
    _uses_new_param_api: bool  # True if using Param/ConstParam annotations

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Extract Param/ConstParam annotations from compute() signature.

        Supports two APIs:
        1. New API: Param/ConstParam annotations on compute() parameters
        2. Legacy API: Arg descriptors as class attributes

        The new API is detected by checking if any compute() parameter has
        an Arg annotation from Param() or ConstParam().
        """
        super().__init_subclass__(**kwargs)

        # Skip abstract classes
        if inspect.isabstract(cls):
            return

        # Get compute method
        compute_method = getattr(cls, "compute", None)
        if compute_method is None:
            raise TypeError(
                f"{cls.__name__} must define a compute() method.\n\n"
                f"Example using Param/ConstParam (recommended):\n"
                f"    def compute(\n"
                f"        self,\n"
                f"        value: Param(pa.int64(), 'Input value'),\n"
                f"    ) -> Returns(pa.int64()):\n"
                f"        return pc.multiply(value, 2)\n\n"
                f"Example using Arg descriptors (legacy):\n"
                f"    column: Annotated[str, Arg(0, doc='Column name')]\n"
                f"    def compute(self, *, column: pa.Array) -> pa.Array:\n"
                f"        return pc.multiply(column, 2)"
            )

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
                def __class_getitem__(cls, item: Any) -> Any:
                    return Any

            class _MockPa:
                Array = _MockArray

                def __getattr__(self, name: str) -> Any:
                    return getattr(pa, name)

            eval_namespace = {
                **getattr(compute_method, "__globals__", {}),
                "Param": vgi_args.Param,
                "ConstParam": vgi_args.ConstParam,
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

        # Check if using new Param/ConstParam API by looking for Arg in annotations
        uses_new_api = False
        compute_params: dict[str, Arg[Any]] = {}
        const_params: dict[str, Arg[Any]] = {}
        returns_output_type: pa.DataType | None = None

        # Check return type for Returns() annotation
        return_hint = hints.get("return")
        if return_hint is not None and hasattr(return_hint, "__metadata__"):
            # Extract _OutputType from Annotated[..., _OutputType(...)]
            for meta in return_hint.__metadata__:
                if isinstance(meta, _OutputType):
                    returns_output_type = meta.arrow_type
                    uses_new_api = True
                    break

        # Extract Param/ConstParam from parameter annotations
        # Track separate positions for column params (from batch) and const params
        # (from invocation arguments)
        column_position = 0  # Position in batch columns
        const_position = 0  # Position in invocation.arguments.positional
        for name in sig.parameters:
            if name == "self":
                continue

            hint = hints.get(name)
            if hint is None:
                continue

            # Check for Annotated[..., Arg(...)] pattern
            if hasattr(hint, "__metadata__"):
                for meta in hint.__metadata__:
                    if isinstance(meta, Arg):
                        uses_new_api = True
                        meta._name = name

                        if meta.const:
                            # Const params use their own position counter
                            # (position into invocation.arguments.positional)
                            if meta.position == -1:  # _INFER_POSITION
                                meta.position = const_position
                            const_params[name] = meta
                            # Create descriptor for const param
                            setattr(cls, name, _ConstArgDescriptor(meta, name))
                            const_position += 1
                        else:
                            # Column params use their own position counter
                            # (position into batch columns)
                            if meta.position == -1:  # _INFER_POSITION
                                meta.position = column_position
                            compute_params[name] = meta
                            # Create descriptor for regular param
                            setattr(cls, name, _ArgDescriptor(meta, name))
                            column_position += 1

                        break

        cls._uses_new_param_api = uses_new_api
        cls._compute_params = compute_params
        cls._const_params = const_params
        cls._returns_output_type = returns_output_type

        if uses_new_api:
            # New API: combine for _compute_kwonly_params
            # (used by _extract_compute_kwargs)
            cls._compute_kwonly_params = {**compute_params, **const_params}
        else:
            # Legacy API: validate Arg descriptors match compute() keyword-only params
            kwonly_params: dict[str, Arg[Any]] = {}

            for name, param in sig.parameters.items():
                if param.kind == inspect.Parameter.KEYWORD_ONLY:
                    arg = getattr(cls, name, None)
                    if not isinstance(arg, Arg):
                        raise TypeError(
                            f"{cls.__name__}.compute() has keyword-only parameter "
                            f"'{name}' but no matching Arg descriptor.\n\n"
                            f"Option 1 - Add Arg descriptor:\n"
                            f"    {name}: Annotated[str, Arg(0, doc='...')]\n\n"
                            f"Option 2 - Use Param() annotation (recommended):\n"
                            f"    def compute(\n"
                            f"        self,\n"
                            f"        {name}: Param(pa.int64(), 'description'),\n"
                            f"    ) -> Returns(pa.int64()):\n"
                            f"        ..."
                        )
                    kwonly_params[name] = arg

            # Validate varargs is last if present
            vararg_names = [n for n, a in kwonly_params.items() if a.varargs]
            if vararg_names:
                param_names = list(kwonly_params.keys())
                if vararg_names[0] != param_names[-1]:
                    raise TypeError(
                        f"{cls.__name__}.compute() varargs parameter "
                        f"'{vararg_names[0]}' must be the last keyword-only parameter"
                    )

            cls._compute_kwonly_params = kwonly_params

    def __init__(
        self,
        invocation: vgi.invocation.Invocation,
        logger: structlog.stdlib.BoundLogger,
    ):
        """Initialize the scalar function."""
        # Initialize pending messages before super().__init__ because
        # output_schema property may be accessed during init
        self._pending_messages = []
        super().__init__(invocation=invocation, logger=logger)

    def log(self, level: vgi.log.Level, message: str) -> None:
        """Queue a log message to be emitted with the output.

        Messages are yielded before the compute() result.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR).
            message: Log message text.

        Example:
            def compute(self, batch: pa.RecordBatch) -> pa.Array:
                self.log(Level.INFO, f"Processing {batch.num_rows} rows")
                return pc.multiply(batch.column(self.column), 2)

        """
        self._pending_messages.append(vgi.log.Message(level=level, message=message))

    @classmethod
    def _get_meta_output_type(cls) -> ScalarOutputType:
        """Get output_type from Meta class.

        Walks the MRO to find a Meta class with output_type defined.

        Returns:
            The output_type value from Meta (pa.DataType or AnyArrow).

        Raises:
            TypeError: If Meta.output_type is not defined in the class hierarchy.

        """
        for klass in cls.__mro__:
            if "Meta" in klass.__dict__:
                meta = klass.__dict__["Meta"]
                if hasattr(meta, "output_type"):
                    return meta.output_type  # type: ignore[no-any-return]
        raise TypeError(
            f"{cls.__name__} must define Meta.output_type "
            f"(pa.DataType for static type, or AnyArrow for dynamic)"
        )

    @classmethod
    @final
    def catalog_output_schema(cls) -> pa.Schema:
        """Return output schema for catalog introspection.

        Returns the output schema with a single "result" field. If
        Meta.output_type is AnyArrow, the field has type null() with
        metadata indicating it's a dynamic "any" type.
        """
        output_type = cls._get_meta_output_type()
        if output_type is AnyArrow:
            # Use null type with metadata to indicate "any" type
            field = pa.field("result", pa.null(), metadata={b"vgi:any": b"true"})
            return pa.schema([field])
        # Type is narrowed to pa.DataType after AnyArrow check
        assert isinstance(output_type, pa.DataType)
        return pa.schema([pa.field("result", output_type)])

    @property
    def output_type(self) -> pa.DataType:
        """Return the Arrow type for the output column.

        Default implementation checks (in order):
        1. _returns_output_type from Returns() annotation (new API)
        2. Meta.output_type (legacy API)

        Override only if using AnyArrow/dynamic types that depend on input schema.

        Example:
            @property
            def output_type(self) -> pa.DataType:
                return self.input_schema.field(self.column).type

        """
        # New API: use _returns_output_type from Returns() annotation
        if self._uses_new_param_api and self._returns_output_type is not None:
            return self._returns_output_type

        # Legacy API or AnyArrow: use Meta.output_type
        result = self._get_meta_output_type()
        if result is AnyArrow:
            raise NotImplementedError(
                f"{type(self).__name__}.output_type must be overridden when "
                f"Meta.output_type is AnyArrow or Returns(AnyArrow) is used"
            )
        # Type is narrowed to pa.DataType after AnyArrow check
        assert isinstance(result, pa.DataType)
        return result

    @property
    @final
    def output_schema(self) -> pa.Schema:
        """Return single-column output schema. Do not override."""
        return pa.schema([pa.field("result", self.output_type)])

    # Note: compute() is NOT defined here. Subclasses define it with their own
    # keyword-only signature. This avoids mypy override errors for users.
    # See class docstring for compute() signature requirements.
    # Validated at class definition time by __init_subclass__.

    @final
    def _extract_compute_kwargs(self, batch: pa.RecordBatch) -> dict[str, Any]:
        """Extract columns/values for compute() parameters.

        Handles both APIs:
        - New API (Param/ConstParam): Extract arrays by position, scalars from args
        - Legacy API (Arg descriptors): Extract arrays by column name

        Args:
            batch: Input RecordBatch.

        Returns:
            Dict mapping parameter names to arrays, lists of arrays, or scalar values.

        """
        kwargs: dict[str, Any] = {}

        if self._uses_new_param_api:
            # New Param/ConstParam API
            # Regular params: extract arrays by position
            for name, arg in self._compute_params.items():
                # Position is always int for new API (set in __init_subclass__)
                pos = cast(int, arg.position)
                if arg.varargs:
                    # Varargs: collect all remaining columns from position
                    kwargs[name] = [
                        batch.column(i) for i in range(pos, batch.num_columns)
                    ]
                else:
                    # Regular param: extract column by position
                    kwargs[name] = batch.column(pos)

            # Const params: extract scalar values from arguments
            for name, arg in self._const_params.items():
                # Position is always int for new API
                pos = cast(int, arg.position)
                # Get the scalar value from invocation arguments
                scalar = self.invocation.arguments.positional[pos]
                # Convert to Python value
                kwargs[name] = scalar.as_py() if scalar is not None else None
        else:
            # Legacy Arg descriptor API
            for name, arg in self._compute_kwonly_params.items():
                arg_value = getattr(self, name)

                if arg.varargs:
                    # Varargs: tuple of column names -> list of arrays
                    column_names: tuple[str, ...] = arg_value
                    kwargs[name] = [batch.column(col) for col in column_names]
                else:
                    # Regular arg: column name -> single array
                    col_name = (
                        arg_value.value if hasattr(arg_value, "value") else arg_value
                    )
                    kwargs[name] = batch.column(col_name)

        return kwargs

    @final
    def _validate_param_types(self, kwargs: dict[str, Any]) -> None:
        """Validate that input array types match declared Param types.

        Only validates for the new Param/ConstParam API, and only for params
        that have a declared arrow_type (not is_any=True).

        Args:
            kwargs: Dict of parameter names to arrays (from _extract_compute_kwargs).

        Raises:
            TypeMismatchError: If any array type doesn't match its declared type.

        """
        if not self._uses_new_param_api:
            return  # Legacy API doesn't have explicit type declarations

        for name, arg in self._compute_params.items():
            if arg.is_any:
                continue  # Skip AnyArrow params

            if arg.arrow_type is None:
                continue  # No type declared (shouldn't happen with new API)

            if arg.varargs:
                # Validate all arrays in varargs
                arrays = kwargs[name]
                for i, arr in enumerate(arrays):
                    if arr.type != arg.arrow_type:
                        raise TypeMismatchError(
                            f"Input type mismatch for vararg parameter '{name}' "
                            f"at index {i}.",
                            param_name=f"{name}[{i}]",
                            expected_type=arg.arrow_type,
                            actual_type=arr.type,
                            function_name=type(self).__name__,
                        )
            else:
                arr = kwargs[name]
                if arr.type != arg.arrow_type:
                    raise TypeMismatchError(
                        f"Input type mismatch for parameter '{name}'.",
                        param_name=name,
                        expected_type=arg.arrow_type,
                        actual_type=arr.type,
                        function_name=type(self).__name__,
                    )

    @final
    def _validate_output_type(self, result: pa.Array) -> None:
        """Validate that output array type matches declared Returns type.

        Only validates for the new Param/ConstParam API with explicit Returns().

        Args:
            result: The output array from compute().

        Raises:
            TypeMismatchError: If output type doesn't match declared type.

        """
        if not self._uses_new_param_api:
            return  # Legacy API uses output_schema validation

        if self._returns_output_type is None:
            return  # AnyArrow or not specified

        if result.type != self._returns_output_type:
            raise TypeMismatchError(
                "Output type mismatch.",
                param_name="return",
                expected_type=self._returns_output_type,
                actual_type=result.type,
                function_name=type(self).__name__,
            )

    @final
    def _yield_pending_messages(self) -> ScalarOutputGenerator:
        """Yield all pending log messages. Helper for process()."""
        while self._pending_messages:
            msg = self._pending_messages.pop(0)
            _ = yield msg

    @final
    def process(self, batch: pa.RecordBatch) -> ScalarOutputGenerator:
        """Convert compute() to generator protocol. Do not override.

        This method implements the generator protocol by calling your compute()
        method for each input batch. Keyword-only parameters in compute() are
        automatically populated from the batch columns.
        """
        # Priming yield
        _ = yield Output(self.empty_output_batch)

        while True:
            # Extract columns for keyword-only parameters
            kwargs = self._extract_compute_kwargs(batch)

            # Validate input types match declared Param types
            self._validate_param_types(kwargs)

            # Call compute() defined by subclass. Cast to Any to avoid
            # attr-defined error since compute() isn't on base class.
            result = cast(Any, self).compute(**kwargs)

            # Validate output type matches declared Returns type
            self._validate_output_type(result)

            # Yield any pending log messages first
            yield from self._yield_pending_messages()

            # Create output batch from result array
            output = pa.RecordBatch.from_arrays([result], schema=self.output_schema)
            received = yield Output(output)

            if received is None:
                break
            batch = received
