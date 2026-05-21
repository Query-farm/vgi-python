# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Core data structures for VGI function calls and bind results.

This module defines the foundational classes used during function binding
in the VGI protocol. When a client invokes a function, it sends the
function name, arguments, input schema, and function type.

Classes:
    Function: Base class for all VGI functions.

See Also:
    vgi.scalar_function: Scalar functions with 1:1 row transforms.
    vgi.table_function: Table functions with cardinality hints.
    vgi.table_in_out_function: Streaming table functions for batch transforms.
    vgi_rpc.log: Level and Message for in-band function diagnostics.

"""

from __future__ import annotations

import logging
import os
from abc import ABC
from typing import (
    Annotated,
    Any,
    ClassVar,
    final,
    get_args,
    get_origin,
)

import pyarrow as pa

from vgi.exceptions import SchemaValidationError
from vgi.function_storage import FunctionStorage, FunctionStorageSqlite
from vgi.metadata import MetadataMixin, ResolvedMetadata


def _resolve_storage() -> FunctionStorage:
    """Resolve the default FunctionStorage backend from environment."""
    backend = os.environ.get("VGI_WORKER_SHARED_STORAGE", "sqlite").lower()
    if backend == "sqlite":
        # VGI_WORKER_SQLITE_PATH=":memory:" picks the in-process shared-cache
        # in-memory backend. Used by single-process test fixtures (notably
        # the test fixture HTTP server) to avoid per-op WAL fsync cost.
        db_path = os.environ.get("VGI_WORKER_SQLITE_PATH") or None
        return FunctionStorageSqlite(db_path=db_path)
    if backend == "azure-sql":
        from vgi.function_storage_azure_sql import FunctionStorageAzureSql

        return FunctionStorageAzureSql.from_env()
    if backend == "cloudflare-do":
        from vgi.function_storage_cf_do import FunctionStorageCfDo

        return FunctionStorageCfDo.from_env()
    raise ValueError(
        f"Unknown VGI_WORKER_SHARED_STORAGE backend: {backend!r}. Supported: 'sqlite', 'azure-sql', 'cloudflare-do'"
    )


class _DefaultStorageDescriptor:
    """Resolve FunctionStorage lazily on first attribute access.

    This avoids evaluating environment variables at import time. When a
    subclass explicitly sets ``storage = SomeStorage(...)``, the plain
    attribute shadows this descriptor — no interference.
    """

    _resolved: FunctionStorage | None = None

    def __get__(self, obj: object | None, objtype: type | None = None) -> FunctionStorage:
        if self._resolved is None:
            self._resolved = _resolve_storage()
        return self._resolved


# Default max_workers when not explicitly specified (effectively unlimited)
DEFAULT_MAX_WORKERS = 99999

__all__ = [
    "Function",
    "DEFAULT_MAX_WORKERS",
]


class Function(ABC, MetadataMixin):
    """Base class for all VGI functions.

    Provides shared infrastructure (metadata, storage, Arg descriptor extraction,
    schema validation) for all function types. Since the child classes have very
    different APIs, there are not many standard methods here.

    Subclasses can define a nested Meta class to provide metadata.

    Available Meta attributes:
        name: Function name for registration (default: class name to snake_case)
        description: Human-readable description (default: docstring first line)
        categories: Classification tags
        examples: List of SQL examples
        See vgi.metadata for all available attributes.

    Attributes:
        logger: Structured logger for function diagnostics.

    See Also:
        vgi.scalar_function.ScalarFunction: Scalar 1:1 row transforms.
        vgi.table_function.TableFunctionGenerator: Table functions.
        vgi.table_in_out_function.TableInOutFunction: Table-in-out batch transforms.
        vgi.metadata: Metadata documentation for functions.

    """

    storage: ClassVar[FunctionStorage] = _DefaultStorageDescriptor()  # type: ignore[assignment]

    # Cache for resolved metadata
    _metadata_cache: ClassVar[ResolvedMetadata | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Extract Arg descriptors from Annotated type hints.

        The Arg is extracted from the annotation metadata and installed
        as a class attribute (descriptor).
        """
        super().__init_subclass__(**kwargs)

        # Import here to avoid circular imports
        from vgi.arguments import AnyArrowValue, Arg

        # Get type hints with include_extras=True to access Annotated metadata
        # We only look at the class's own annotations (not inherited) to avoid
        # issues with forward references that can't be resolved in this module
        annotations = getattr(cls, "__annotations__", {})
        if not annotations:
            return

        # Build evaluation namespace from module globals
        module = __import__(cls.__module__, fromlist=[""])
        globalns = getattr(module, "__dict__", {})
        # Add common typing imports that might be needed
        globalns.setdefault("Annotated", Annotated)

        for attr_name, annotation in annotations.items():
            # Evaluate string annotation if needed (from __future__ import annotations)
            if isinstance(annotation, str):
                try:
                    hint = eval(annotation, globalns)  # noqa: S307
                except Exception:
                    # Can't evaluate this annotation, skip it
                    continue
            else:
                hint = annotation
            # Skip if not Annotated
            if get_origin(hint) is not Annotated:
                continue

            # Get the base type and metadata from Annotated[BaseType, metadata...]
            args = get_args(hint)
            if not args:
                continue

            base_type = args[0]
            metadata = args[1:]

            # Look for Arg in the metadata
            for meta in metadata:
                if isinstance(meta, Arg):
                    # Check if an Arg descriptor already exists for this name
                    # (could be from a parent class or explicit assignment)
                    existing = getattr(cls, attr_name, None)
                    if isinstance(existing, Arg):
                        continue

                    # Set the name on the Arg (normally done by __set_name__)
                    meta._name = attr_name

                    # Set _returns_any_arrow_value based on the annotated type
                    meta._returns_any_arrow_value = base_type is AnyArrowValue

                    # Infer _type_param from the base type for metadata extraction
                    # and type_bound validation
                    if base_type is AnyArrowValue or meta.type_bound is not None:
                        # AnyArrowValue or type_bound means this is an AnyArrow arg
                        from vgi.arguments import AnyArrow

                        meta._type_param = AnyArrow
                    elif meta._type_param is None:
                        # Use the annotation type as the type param
                        meta._type_param = base_type

                    # Install the Arg as a class attribute
                    setattr(cls, attr_name, meta)
                    break

    def __init__(
        self,
        *,
        logger: logging.Logger,
    ):
        """Initialize the function with invocation data and logger.

        Args:
            logger: Logger for function diagnostics.

        """
        self.logger = logger

    @final
    @classmethod
    def _validate_output_schema(cls, batch: pa.RecordBatch, output_schema: pa.Schema) -> None:
        """Validate that a batch conforms to the expected output schema."""
        if batch.schema != output_schema:
            raise SchemaValidationError(
                "Output batch schema does not match expected output_schema.",
                expected=output_schema,
                actual=batch.schema,
                context=f"output from {cls.__name__}",
            )

    @final
    @classmethod
    def _validate_input_schema(cls, batch: pa.RecordBatch, input_schema: pa.Schema) -> None:
        """Validate that a batch conforms to the expected input schema."""
        if batch.schema != input_schema:
            raise SchemaValidationError(
                "Input batch schema does not match expected input_schema.",
                expected=input_schema,
                actual=batch.schema,
                context=f"input to {cls.__name__}",
            )
