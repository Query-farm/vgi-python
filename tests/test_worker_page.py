"""Tests for the worker description page HTML generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa

from vgi.catalog import Catalog, Schema, Setting, Table, View
from vgi.examples.worker import ExampleWorker
from vgi.http.worker_page import (
    WorkerPageResource,
    _display_function_type,
    build_worker_page,
)
from vgi.metadata import FunctionStability, resolve_metadata
from vgi.scalar_function import ScalarFunction
from vgi.table_function import TableFunctionGenerator
from vgi.table_in_out_function import TableInOutGenerator
from vgi.worker import Worker

# ---------------------------------------------------------------------------
# Fixtures: minimal worker classes for testing
# ---------------------------------------------------------------------------


@dataclass
class _SeqArgs:
    """Arguments for _SeqFunc."""

    n: int = 10


@dataclass
class _EmptyArgs:
    """Empty arguments."""


class _AddFunc(ScalarFunction):
    class Meta:
        name = "add"
        description = "Add two integers"
        examples = ["SELECT add(a, b) FROM t"]
        stability = FunctionStability.VOLATILE

    def compute(self, a: pa.Int64Array, b: pa.Int64Array) -> pa.Int64Array:
        return pa.compute.add(a, b)


class _SeqFunc(TableFunctionGenerator[_SeqArgs]):
    class Meta:
        name = "seq"
        description = "Generate a sequence"
        filter_pushdown = True
        projection_pushdown = True
        max_workers = 4

    def output_schema(self) -> pa.Schema:
        """Return output schema."""
        return pa.schema([("i", pa.int64())])

    def process(self, out: Any) -> None:  # type: ignore[override]
        """Emit a sequence."""
        out.emit(pa.record_batch([pa.array(range(self.args.n), type=pa.int64())], schema=self.output_schema()))  # type: ignore[attr-defined]


class _EchoFunc(TableInOutGenerator[_EmptyArgs]):
    class Meta:
        name = "echo_tio"
        description = "Echo input batches"

    def output_schema(self) -> pa.Schema:
        """Return output schema."""
        return pa.schema([("x", pa.int64())])

    def process(self, batch: Any, out: Any) -> None:  # type: ignore[override]
        """Echo batch."""
        out.emit(batch)


class _MinimalWorker(Worker):
    """A minimal test worker."""

    catalog = Catalog(
        name="test",
        schemas=[
            Schema(
                name="main",
                comment="Main schema",
                functions=[_AddFunc, _SeqFunc, _EchoFunc],
                tables=[
                    Table(
                        name="numbers",
                        columns=pa.schema([("value", pa.int64())]),
                        comment="Integer table",
                    ),
                ],
                views=[
                    View(
                        name="evens",
                        definition="SELECT * FROM numbers WHERE value % 2 = 0",
                        comment="Even numbers",
                    ),
                ],
            ),
        ],
    )


class _WorkerWithSettings(Worker):
    """Worker with settings."""

    class Settings:
        """Settings exposed via catalog_attach."""

        verbose: Annotated[bool, Setting(desc="Verbose mode")] = False
        limit: Annotated[int, Setting(desc="Row limit")] = 100

    catalog = Catalog(
        name="settings_test",
        schemas=[
            Schema(name="main", functions=[_AddFunc]),
        ],
    )


class _LegacyWorker(Worker):
    """Worker using legacy functions list."""

    functions = [_AddFunc, _SeqFunc]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDisplayFunctionType:
    """Tests for _display_function_type classification."""

    def test_scalar(self) -> None:
        """Scalar functions are labelled 'scalar'."""
        meta = resolve_metadata(_AddFunc)
        assert _display_function_type(_AddFunc, meta) == "scalar"

    def test_table(self) -> None:
        """Table generator functions are labelled 'table'."""
        meta = resolve_metadata(_SeqFunc)
        assert _display_function_type(_SeqFunc, meta) == "table"

    def test_table_in_out(self) -> None:
        """Table-in-out functions are labelled 'table-in-out'."""
        meta = resolve_metadata(_EchoFunc)
        assert _display_function_type(_EchoFunc, meta) == "table-in-out"


class TestBuildWorkerPage:
    """Tests for build_worker_page HTML generation."""

    def test_returns_bytes(self) -> None:
        """Result is UTF-8 encoded bytes."""
        result = build_worker_page(_MinimalWorker, "/vgi")
        assert isinstance(result, bytes)

    def test_contains_worker_name(self) -> None:
        """Worker class name appears in the page."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "_MinimalWorker" in html

    def test_contains_worker_docstring(self) -> None:
        """Worker docstring appears in the page."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "A minimal test worker." in html

    def test_contains_function_names(self) -> None:
        """All function names appear in the page."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "add" in html
        assert "seq" in html
        assert "echo_tio" in html

    def test_contains_function_descriptions(self) -> None:
        """Function descriptions appear in the page."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "Add two integers" in html
        assert "Generate a sequence" in html
        assert "Echo input batches" in html

    def test_contains_type_badges(self) -> None:
        """Badge CSS classes for all three types appear."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "badge-scalar" in html
        assert "badge-table" in html
        assert "badge-table-in-out" in html

    def test_contains_schema_name(self) -> None:
        """Schema name and comment appear."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "main" in html
        assert "Main schema" in html

    def test_contains_table_info(self) -> None:
        """Table name, comment, and column types appear."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "numbers" in html
        assert "Integer table" in html
        assert "int64" in html

    def test_contains_view_info(self) -> None:
        """View name, comment, and SQL definition appear."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "evens" in html
        assert "Even numbers" in html
        assert "SELECT * FROM numbers" in html

    def test_contains_examples(self) -> None:
        """Function examples appear."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "SELECT add(a, b) FROM t" in html

    def test_contains_capabilities(self) -> None:
        """Table function capabilities appear."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "filter pushdown" in html
        assert "projection pushdown" in html
        assert "max_workers=4" in html

    def test_contains_stability_badge(self) -> None:
        """Non-default stability badge appears."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "badge-stability" in html
        assert "volatile" in html

    def test_contains_prefix_links(self) -> None:
        """Footer links use the prefix."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "/vgi" in html
        assert "/vgi/describe" in html

    def test_contains_vgi_version(self) -> None:
        """VGI version string appears."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "vgi</code> v" in html

    def test_valid_html_structure(self) -> None:
        """Output is well-formed HTML."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert html.startswith("<!DOCTYPE html>")
        assert "</html>" in html
        assert "<head>" in html
        assert "<body>" in html


class TestBuildWorkerPageWithSettings:
    """Tests for worker page with settings."""

    def test_contains_settings_section(self) -> None:
        """Settings section shows all settings."""
        html = build_worker_page(_WorkerWithSettings, "/vgi").decode()
        assert "Settings" in html
        assert "verbose" in html
        assert "limit" in html
        assert "Verbose mode" in html
        assert "Row limit" in html

    def test_contains_setting_defaults(self) -> None:
        """Setting default values appear."""
        html = build_worker_page(_WorkerWithSettings, "/vgi").decode()
        assert "False" in html
        assert "100" in html


class TestBuildWorkerPageLegacy:
    """Tests for worker page with legacy functions list."""

    def test_legacy_functions_list(self) -> None:
        """Legacy functions list renders function names."""
        html = build_worker_page(_LegacyWorker, "/vgi").decode()
        assert "add" in html
        assert "seq" in html

    def test_no_schema_heading(self) -> None:
        """Legacy workers have no schema grouping."""
        html = build_worker_page(_LegacyWorker, "/vgi").decode()
        # No <h2 class="schema-heading"> elements (CSS class exists in style block)
        assert '<h2 class="schema-heading">' not in html


class TestExampleWorkerPage:
    """Tests for the real ExampleWorker page."""

    def test_example_worker_builds(self) -> None:
        """Smoke test: the real ExampleWorker page renders without error."""
        result = build_worker_page(ExampleWorker, "/vgi")
        assert isinstance(result, bytes)
        html = result.decode()
        assert "ExampleWorker" in html

    def test_example_worker_has_settings(self) -> None:
        """ExampleWorker settings appear in the page."""
        html = build_worker_page(ExampleWorker, "/vgi").decode()
        assert "vgi_verbose_mode" in html
        assert "greeting" in html
        assert "multiplier" in html

    def test_example_worker_has_schemas(self) -> None:
        """ExampleWorker schemas appear in the page."""
        html = build_worker_page(ExampleWorker, "/vgi").decode()
        assert "data" in html


class TestWorkerPageResource:
    """Tests for the Falcon resource class."""

    def test_on_get(self) -> None:
        """WorkerPageResource returns pre-rendered body."""
        body = b"<html>test</html>"
        resource = WorkerPageResource(body)

        class FakeResp:
            """Fake response object."""

            content_type: str = ""
            data: bytes = b""

        resp = FakeResp()
        resource.on_get(None, resp)  # type: ignore[arg-type]
        assert resp.content_type == "text/html; charset=utf-8"
        assert resp.data == body
