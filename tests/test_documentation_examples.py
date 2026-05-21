# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Test Python code examples in documentation files.

Uses pytest-examples to validate that code examples in markdown files
are syntactically correct and (where possible) executable.
"""

import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pytest_examples import CodeExample, EvalExample, find_examples
from vgi_rpc.log import Level, Message

import vgi
from vgi import (
    Arg,
    ScalarFunction,
    ScalarFunctionGenerator,
    TableInOutFunction,
    TableInOutGenerator,
    Worker,
    schema,
    schema_like,
)
from vgi.arguments import AnyArrow, Arguments, TableInput
from vgi.catalog import Catalog, Schema, Table, View
from vgi.client import Client
from vgi.metadata import OrderPreservation, ResolvedMetadata
from vgi.table_function import TableCardinality, TableFunctionGenerator

# Pre-built globals for running documentation examples
# These provide common imports so partial snippets can run
DOC_EXAMPLE_GLOBALS: dict[str, Any] = {
    # PyArrow
    "pa": pa,
    "pc": pc,
    # VGI core
    "vgi": vgi,
    "Arg": Arg,
    "Worker": Worker,
    "schema": schema,
    "schema_like": schema_like,
    # Function types
    "ScalarFunction": ScalarFunction,
    "ScalarFunctionGenerator": ScalarFunctionGenerator,
    "TableFunctionGenerator": TableFunctionGenerator,
    "TableInOutFunction": TableInOutFunction,
    "TableInOutGenerator": TableInOutGenerator,
    # Arguments
    "AnyArrow": AnyArrow,
    "Arguments": Arguments,
    "TableInput": TableInput,
    # Catalog descriptors
    "Catalog": Catalog,
    "Schema": Schema,
    "Table": Table,
    "View": View,
    # Other
    "Client": Client,
    "ResolvedMetadata": ResolvedMetadata,
    "TableCardinality": TableCardinality,
    "Level": Level,
    "Message": Message,
    "logging": logging,
    # Typing
    "Iterable": Iterable,
    "Iterator": Iterator,
    "Any": Any,
    "dataclass": dataclass,
    # Metadata
    "OrderPreservation": OrderPreservation,
}

# All documentation files to test
DOC_FILES = [
    "CLAUDE.md",
    "README.md",
    "docs/generator-api.md",
    "docs/catalog-interface.md",
    "docs/argument-serialization.md",
    "docs/lifecycle.md",
    "docs/metadata.md",
    "docs/cli.md",
]


def _should_skip(example: CodeExample) -> bool:
    """Check if example should be skipped entirely."""
    # Skip non-Python examples (prefix_tags returns set like {'python'})
    tags = example.prefix_tags()
    if "python" not in tags and "py" not in tags:
        return True

    # Check for skip marker in code block settings
    # e.g., ```python test="skip"
    settings = example.prefix_settings()
    if settings.get("test") == "skip":
        return True

    # Skip examples with intentionally invalid syntax
    # These are partial snippets that can't even be linted
    source = example.source
    invalid_syntax_markers = [
        '"settings": (',  # Dict entry without dict context
        "attach_opaque_data=...,",  # Placeholder arguments with trailing comma
        "# ... implement",  # Placeholder comment instead of implementation
        "...,",  # Ellipsis with comma (placeholder in arg list)
        ") -> ...:",  # Ellipsis as return type placeholder
    ]
    return any(marker in source for marker in invalid_syntax_markers)


def _is_lint_only(example: CodeExample) -> bool:
    """Check if example should only be linted (not executed).

    Examples are lint-only if they:
    - Have test="lint" marker
    - Reference external files (my_worker, my_catalog_worker, etc.)
    - Contain intentionally broken code (Common Mistakes examples)
    - Import from non-existent modules
    """
    settings = example.prefix_settings()
    if settings.get("test") == "lint":
        return True

    source = example.source

    # Examples referencing external worker files or resources
    external_refs = [
        "my_worker",
        "my_catalog_worker",
        "from my_lib import",
        "./my_worker.py",
        "vgi-fixture-worker",
        # Database/file access
        "sqlite3.connect(",
        'open("',
        "open('",
        # Client operations that need running worker
        "client.table_function(",
        "client.scalar_function(",
        # Catalog storage that needs file access
        "CatalogStorage(",
    ]
    if any(ref in source for ref in external_refs):
        return True

    # Examples that are intentionally incomplete or broken
    broken_markers = [
        "# ❌ WRONG",
        "# ⚠️ PROBLEMATIC",
        "expensive_computation(",
        "# ✅ CORRECT",  # Often paired with WRONG in same block
    ]
    if any(marker in source for marker in broken_markers):
        return True

    # Examples that reference undefined classes/variables (partial snippets)
    # These typically show method implementations without full class context
    partial_refs = [
        "MyFunction",
        "AnotherFunction",
        "ReadOnlyCatalogInterface",
        "SumValuesFunction",
        "EchoFunction",
        "Function",  # Generic function references
        "specs",  # Schema specs variable
        "schema_bytes",  # Serialized schema variable
        "AttachOpaqueData",  # Catalog attach ID
    ]
    if any(ref in source for ref in partial_refs):
        return True

    # Examples that use 'self' outside a method definition (partial snippets)
    # These are method bodies shown without the enclosing class
    return "self." in source and "def " not in source and "class " not in source


@pytest.mark.parametrize("example", find_examples(*DOC_FILES), ids=str)
def test_documentation_examples(example: CodeExample, eval_example: EvalExample) -> None:
    """Test that documentation examples are valid Python.

    - All Python examples are linted for syntax correctness
    - Self-contained examples are also executed
    - Examples with external dependencies are lint-only
    """
    if _should_skip(example):
        pytest.skip(f"Skipping non-Python or marked example: {example.path}")

    # Check if we should update examples or validate them
    if eval_example.update_examples:
        eval_example.format(example)
        if not _is_lint_only(example):
            eval_example.run_print_update(example, module_globals=DOC_EXAMPLE_GLOBALS)
    else:
        # Lint with ruff only (not black) - we care about code errors, not formatting
        # Black formatting would require every example to have exact formatting
        eval_example.lint_ruff(example)

        # Run examples that are self-contained, providing common globals
        if not _is_lint_only(example):
            eval_example.run(example, module_globals=DOC_EXAMPLE_GLOBALS)
