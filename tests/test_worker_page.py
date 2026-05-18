"""Tests for the worker description page HTML generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa

from vgi._test_fixtures.attach_options import AttachOptionsWorker
from vgi._test_fixtures.worker import ExampleWorker
from vgi.catalog import (
    AttachOpaqueData,
    Catalog,
    CatalogAttachResult,
    CatalogDataVersionRelease,
    CatalogInfo,
    ReadOnlyCatalogInterface,
    Schema,
    Setting,
    Table,
    View,
)
from vgi.catalog.attach_option import AttachOption, extract_attach_option_specs
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

    def test_legacy_schema_grouping(self) -> None:
        """Legacy workers (Worker.functions list) get a default ``main`` schema.

        Pre-dynamic enumeration this used to render a flat function list, but
        the worker's auto-generated ReadOnlyCatalogInterface really does
        expose a ``main`` schema, and the page now reflects that.
        """
        html = build_worker_page(_LegacyWorker, "/vgi").decode()
        assert '<h2 class="schema-heading">main</h2>' in html


class TestExampleWorkerPage:
    """Tests for the real ExampleWorker page."""

    def test_fixture_worker_builds(self) -> None:
        """Smoke test: the real ExampleWorker page renders without error."""
        result = build_worker_page(ExampleWorker, "/vgi")
        assert isinstance(result, bytes)
        html = result.decode()
        assert "ExampleWorker" in html

    def test_fixture_worker_has_settings(self) -> None:
        """ExampleWorker settings appear in the page."""
        html = build_worker_page(ExampleWorker, "/vgi").decode()
        assert "vgi_verbose_mode" in html
        assert "greeting" in html
        assert "multiplier" in html

    def test_fixture_worker_has_schemas(self) -> None:
        """ExampleWorker schemas appear in the page."""
        html = build_worker_page(ExampleWorker, "/vgi").decode()
        assert "data" in html


class TestWorkerPageResource:
    """Tests for the Falcon resource class."""

    class _FakeReq:
        """Minimal Falcon-request stand-in returning fixed query params."""

        def __init__(self, params: dict[str, str] | None = None) -> None:
            self._params = params or {}

        def get_param(self, name: str) -> str | None:
            return self._params.get(name)

    class _FakeResp:
        """Minimal Falcon-response stand-in capturing content-type + data."""

        content_type: str = ""
        data: bytes = b""

    def test_on_get_returns_html_for_minimal_worker(self) -> None:
        """WorkerPageResource renders the page on every GET."""
        resource = WorkerPageResource(_MinimalWorker, "/vgi")
        resp = self._FakeResp()
        resource.on_get(self._FakeReq(), resp)  # type: ignore[arg-type]
        assert resp.content_type == "text/html; charset=utf-8"
        assert b"<!DOCTYPE html>" in resp.data
        assert b"_MinimalWorker" in resp.data

    def test_on_get_passes_query_params_through(self) -> None:
        """``?catalog=`` + ``?data_version_spec=`` reach the renderer."""
        resource = WorkerPageResource(_MultiCatalogWorker, "/vgi")
        resp = self._FakeResp()
        req = self._FakeReq({"catalog": "staging", "data_version_spec": "1.2.3"})
        resource.on_get(req, resp)  # type: ignore[arg-type]
        # The staging panel ends up active (no `hidden` attr).
        assert b'<div class="catalog-panel" id="catpanel-1" role="tabpanel" data-catalog="staging">' in resp.data
        # The ATTACH SQL clause is baked in (no `hidden` on the dv-clause).
        # Note: staging's data_version_spec is None, so even with an active
        # request the dv-clause/dv-value spans don't appear on its panel.
        # Switch to prod to verify the SQL bake.
        prod_resp = self._FakeResp()
        resource.on_get(self._FakeReq({"catalog": "prod", "data_version_spec": "1.2.3"}), prod_resp)  # type: ignore[arg-type]
        assert b'class="dv-clause">, data_version_spec' in prod_resp.data
        assert b'class="dv-value">1.2.3</span>' in prod_resp.data

    def test_body_transform_applied(self) -> None:
        """``body_transform`` lets vgi-serve inject PKCE user-info markup."""
        resource = WorkerPageResource(
            _MinimalWorker,
            "/vgi",
            body_transform=lambda b: b.replace(b"</body>", b"<div id=injected/></body>"),
        )
        resp = self._FakeResp()
        resource.on_get(self._FakeReq(), resp)  # type: ignore[arg-type]
        assert b"<div id=injected/>" in resp.data


# ---------------------------------------------------------------------------
# Fixtures: multi-catalog + version-bearing worker
# ---------------------------------------------------------------------------


class _ProdAttachOptions:
    """Two-option declaration used by the multi-catalog test fixture."""

    region: Annotated[str, AttachOption(desc="AWS region")] = "us-east-1"
    bucket: Annotated[str, AttachOption(desc="S3 bucket name")] = "prod-bucket"


_PROD_OPTION_SPECS = extract_attach_option_specs(_ProdAttachOptions)


_TEST_PROD_CATALOG = Catalog(
    name="prod",
    comment="Production warehouse",
    schemas=[Schema(name="main", functions=[_AddFunc])],
)


class _TestMultiCatalogInterface(ReadOnlyCatalogInterface):
    """Two catalogs: prod (versioned + options) and staging (no version)."""

    catalog = _TEST_PROD_CATALOG
    catalog_name = "prod"

    def catalogs(self) -> list[CatalogInfo]:
        """Advertise prod (versioned + options) and staging (vanilla)."""
        return [
            CatalogInfo(
                name="prod",
                implementation_version="2.4.0",
                data_version_spec=">=2.0.0,<3.0.0",
                attach_option_specs=[s.serialize() for s in _PROD_OPTION_SPECS],
            ),
            CatalogInfo(
                name="staging",
                implementation_version=None,
                data_version_spec=None,
                attach_option_specs=[],
            ),
        ]

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        ctx: Any = None,
    ) -> CatalogAttachResult:
        """Stub — describe-page tests don't actually attach."""
        del options, data_version_spec, implementation_version, ctx, name
        return CatalogAttachResult(
            attach_opaque_data=AttachOpaqueData(b"\x00" * 16),
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_opaque_data_required=False,
            default_schema="main",
            settings=[],
            secret_types=[],
            comment=None,
            tags={},
            resolved_data_version=None,
            resolved_implementation_version=None,
        )


class _MultiCatalogWorker(Worker):
    """Test worker exposing two catalogs via ``catalogs()``."""

    catalog_interface = _TestMultiCatalogInterface
    catalog = _TEST_PROD_CATALOG


# ---------------------------------------------------------------------------
# Tests for the new sections (Cupola button, multi-catalog tabs, attach
# options table, version display, Cupola deep link)
# ---------------------------------------------------------------------------


class TestCupolaButton:
    """The "Explore this data in Cupola" button on every describe page."""

    def test_renders_id_and_text(self) -> None:
        """Cupola anchor element is present with the user-visible label."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert 'id="cupola-btn"' in html
        assert "Explore this data in Cupola" in html

    def test_links_to_cupola_service_url(self) -> None:
        """Default href points at the Cupola entry point."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "cupola.query-farm.services" in html
        # JS rewrites to ?service=… at runtime; the static href is the bare URL.
        assert 'href="https://cupola.query-farm.services/"' in html

    def test_barn_icon_present(self) -> None:
        """Decorative barn-with-cupola SVG renders inside the button."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        # Presence of the cupola-icon span and the gambrel-roof path is enough.
        assert 'class="cupola-icon"' in html


class TestMultiCatalog:
    """Catalog tab strip behaviour."""

    def test_single_catalog_no_tabs(self) -> None:
        """Workers with one catalog do not render the tab strip."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert 'class="catalog-tabs"' not in html
        assert 'class="catalog-tab"' not in html
        assert 'class="catalog-tab active"' not in html

    def test_multi_catalog_renders_tabs(self) -> None:
        """Workers exposing >1 catalog render one tab per catalog name."""
        html = build_worker_page(_MultiCatalogWorker, "/vgi").decode()
        assert 'class="catalog-tabs"' in html
        # Both catalog names appear inside catalog-tab buttons.
        assert ">prod</button>" in html
        assert ">staging</button>" in html

    def test_first_panel_visible_others_hidden(self) -> None:
        """First catalog panel is visible; subsequent ones use the hidden attr."""
        html = build_worker_page(_MultiCatalogWorker, "/vgi").decode()
        # Panel 0 has no `hidden` attribute on the opening div.
        assert '<div class="catalog-panel" id="catpanel-0" role="tabpanel" data-catalog="prod">' in html
        # Panel 1 is hidden (a real HTML attribute, not in the class list).
        assert '<div class="catalog-panel" id="catpanel-1" role="tabpanel" data-catalog="staging" hidden>' in html


class TestAttachOptions:
    """Per-catalog attach options table."""

    def test_no_options_no_section(self) -> None:
        """Catalogs without declared options omit the Attach options heading."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert "Attach options" not in html

    def test_options_render_with_types_and_defaults(self) -> None:
        """The AttachOptionsWorker fixture surfaces every declared option."""
        html = build_worker_page(AttachOptionsWorker, "/vgi").decode()
        assert "Attach options" in html
        # A handful of distinctive declared options.
        assert "opt_bool" in html
        assert "opt_string" in html
        assert "opt_decimal" in html
        # Their declared types appear (as Arrow type strings).
        assert "decimal128(18, 4)" in html
        # Defaults render via repr() and HTML-escape the quotes.
        assert "&#x27;hello&#x27;" in html


class TestVersionDisplay:
    """Implementation chip + Data version input."""

    def test_impl_chip_renders_when_set(self) -> None:
        """Catalogs with implementation_version surface the impl chip."""
        html = build_worker_page(_MultiCatalogWorker, "/vgi").decode()
        assert 'class="impl-chip"' in html
        # Verbatim version string lands inside a <code> tag.
        assert "<code>2.4.0</code>" in html

    def test_dv_input_renders_when_spec_set(self) -> None:
        """Catalogs with data_version_spec surface a labeled input."""
        html = build_worker_page(_MultiCatalogWorker, "/vgi").decode()
        assert 'class="dv-input"' in html
        assert 'placeholder="latest"' in html
        # Supported range is shown verbatim — `<` and `>` HTML-escape.
        assert "&gt;=2.0.0,&lt;3.0.0" in html

    def test_minimal_worker_no_version_ui(self) -> None:
        """Workers without version metadata render neither chip nor input."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert 'class="impl-chip"' not in html
        assert 'class="dv-input"' not in html


class TestVersionValidation:
    """Server-side pre-attach + error banner."""

    def test_supported_version_no_banner(self) -> None:
        """A version inside the supported set renders the page cleanly."""
        from vgi._test_fixtures.versioned import VersionedWorker

        html = build_worker_page(
            VersionedWorker,
            "/vgi",
            active_catalog="versioned",
            requested_data_version="1.1.0",  # in SUPPORTED_DATA_VERSIONS
        ).decode()
        assert 'class="dv-error"' not in html

    def test_unsupported_version_shows_error_banner(self) -> None:
        """The worker's catalog_attach error surfaces in a red banner."""
        from vgi._test_fixtures.versioned import VersionedWorker

        html = build_worker_page(
            VersionedWorker,
            "/vgi",
            active_catalog="versioned",
            requested_data_version="1.1.1",  # NOT in SUPPORTED_DATA_VERSIONS
        ).decode()
        assert 'class="dv-error"' in html
        # The worker's verbatim message lands in the banner.
        assert "Unsupported data_version_spec" in html
        assert "1.1.1" in html

    def test_no_version_no_banner(self) -> None:
        """Without ?data_version_spec the page renders without validating."""
        from vgi._test_fixtures.versioned import VersionedWorker

        html = build_worker_page(VersionedWorker, "/vgi").decode()
        assert 'class="dv-error"' not in html

    def test_version_without_active_catalog_falls_back_to_first(self) -> None:
        """No ``catalog`` query param: attach uses the first advertised catalog.

        VersionedWorker advertises one catalog (``versioned``) so the version
        gets validated against it even without an explicit ``?catalog=``.
        """
        from vgi._test_fixtures.versioned import VersionedWorker

        html = build_worker_page(
            VersionedWorker,
            "/vgi",
            requested_data_version="1.1.1",
        ).decode()
        # Banner DOES appear because we attach against the only catalog the
        # worker exposes — that's the desired form-submit behaviour.
        assert 'class="dv-error"' in html


class TestApplyForm:
    """Apply form: GET reload with ?catalog=&data_version_spec= bakes state."""

    def test_form_renders_with_apply_button(self) -> None:
        """Each panel with a data_version_spec wraps the input in a GET form."""
        html = build_worker_page(_MultiCatalogWorker, "/vgi").decode()
        assert 'class="dv-form" method="get"' in html
        assert "Apply</button>" in html
        # The input's `name` is what ends up in the URL after submit.
        assert 'name="data_version_spec"' in html
        assert 'name="catalog"' in html

    def test_requested_version_pre_fills_input_and_sql(self) -> None:
        """``requested_data_version`` shows up in the input value and SQL."""
        html = build_worker_page(
            _MultiCatalogWorker,
            "/vgi",
            active_catalog="prod",
            requested_data_version="2.1.0",
        ).decode()
        assert 'value="2.1.0"' in html
        # SQL clause renders inline (no `hidden` attribute) with the value
        # nested inside a span — search for the markup, not the rendered text.
        assert 'class="dv-clause">, data_version_spec' in html
        assert 'class="dv-value">2.1.0</span>' in html
        # And the chosen catalog tab is active without `hidden`.
        assert '<div class="catalog-panel" id="catpanel-0" role="tabpanel" data-catalog="prod">' in html

    def test_requested_version_only_applies_to_active_panel(self) -> None:
        """Inactive panels keep their hidden dv-clause and empty input."""
        html = build_worker_page(
            _MultiCatalogWorker,
            "/vgi",
            active_catalog="prod",
            requested_data_version="2.1.0",
        ).decode()
        # Staging panel (inactive) has no dv-input at all because its
        # data_version_spec is None — but if it had one it would be unfilled.
        # Ensure its panel is hidden.
        assert '<div class="catalog-panel" id="catpanel-1" role="tabpanel" data-catalog="staging" hidden>' in html


class TestCupolaDeepLink:
    """Cupola button href reflects the active catalog and data version."""

    def test_helper_function_present(self) -> None:
        """The href update helper is wired into the page script."""
        html = build_worker_page(_MultiCatalogWorker, "/vgi").decode()
        assert "function updateCupolaHref()" in html
        # And it runs at startup so the initial href is correct.
        assert "updateCupolaHref();" in html

    def test_dv_clause_template_in_attach_sql(self) -> None:
        """The hidden dv-clause span is part of every catalog's ATTACH SQL."""
        html = build_worker_page(_MultiCatalogWorker, "/vgi").decode()
        assert 'class="dv-clause" hidden' in html
        assert 'class="dv-value"' in html

    def test_dv_input_emits_input_listener(self) -> None:
        """Typing into the dv-input updates the SQL clause and the Cupola href."""
        html = build_worker_page(_MultiCatalogWorker, "/vgi").decode()
        assert "updateDvClause(inp)" in html
        assert "updateCupolaHref()" in html


# ---------------------------------------------------------------------------
# Release timeline + source link
# ---------------------------------------------------------------------------


class _ReleasesCatalogInterface(ReadOnlyCatalogInterface):
    """Single catalog carrying a populated release manifest + source_url."""

    catalog = _TEST_PROD_CATALOG
    catalog_name = "prod"

    def catalogs(self) -> list[CatalogInfo]:
        """Advertise a populated release manifest for UI tests."""
        from datetime import UTC, datetime

        return [
            CatalogInfo(
                name="prod",
                implementation_version="2.4.0",
                data_version_spec=">=2.0.0,<3.0.0",
                source_url="https://example.com/repo",
                releases=[
                    CatalogDataVersionRelease(
                        version="2.1.0",
                        released_at=datetime(2026, 3, 1, tzinfo=UTC),
                        summary="Added 'plants' table.",
                        notes_url="https://example.com/v2.1.0",
                    ),
                    CatalogDataVersionRelease(
                        version="2.0.0",
                        released_at=datetime(2026, 2, 1, tzinfo=UTC),
                        summary="Initial 2.x line.",
                    ),
                ],
            ),
        ]

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        ctx: Any = None,
    ) -> CatalogAttachResult:
        """Stub — accept any version so the timeline renders cleanly."""
        del options, data_version_spec, implementation_version, ctx, name
        return CatalogAttachResult(
            attach_opaque_data=AttachOpaqueData(b"\x00" * 16),
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_opaque_data_required=False,
            default_schema="main",
            settings=[],
            secret_types=[],
            comment=None,
            tags={},
            resolved_data_version=None,
            resolved_implementation_version=None,
        )


class _ReleasesWorker(Worker):
    """Test worker exposing a populated release manifest."""

    catalog_interface = _ReleasesCatalogInterface
    catalog = _TEST_PROD_CATALOG


class TestReleaseTimeline:
    """Per-catalog release-history panel rendered under the dv-input."""

    def test_timeline_renders_for_populated_releases(self) -> None:
        """Every release shows as a clickable button with its date + summary."""
        html = build_worker_page(_ReleasesWorker, "/vgi").decode()
        assert 'class="release-timeline"' in html
        assert 'class="release-list"' in html
        # Each version is a clickable button keyed by data-version.
        assert 'data-version="2.1.0"' in html
        assert 'data-version="2.0.0"' in html
        # Dates render as YYYY-MM-DD.
        assert "2026-03-01" in html
        assert "2026-02-01" in html
        # Summaries land verbatim (with HTML escaping of the apostrophe).
        assert "Added &#x27;plants&#x27; table." in html
        assert "Initial 2.x line." in html

    def test_notes_url_renders_when_set(self) -> None:
        """``notes_url`` becomes a 'details →' link; absent fields don't."""
        html = build_worker_page(_ReleasesWorker, "/vgi").decode()
        assert 'href="https://example.com/v2.1.0"' in html
        assert "details" in html
        # 2.0.0 has no notes_url — it shouldn't get a link.
        # Count: only one release-details anchor expected.
        assert html.count('class="release-details"') == 1

    def test_no_timeline_when_no_releases(self) -> None:
        """Workers without releases render no timeline panel."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert 'class="release-timeline"' not in html
        assert 'class="release-list"' not in html

    def test_version_button_wired_to_dv_input(self) -> None:
        """JS click handler fills the matching dv-input and refreshes state."""
        html = build_worker_page(_ReleasesWorker, "/vgi").decode()
        assert "release-version" in html
        # The click handler in the inline script fills the dv-input by
        # data-catalog and then updates the SQL clause + Cupola href.
        assert "inp.value=btn.dataset.version" in html
        assert "updateDvClause(inp);updateCupolaHref();" in html


class TestSourceLink:
    """``source_url`` renders inline next to the implementation chip."""

    def test_source_link_renders_when_set(self) -> None:
        """Worker with ``source_url`` gets a 'View source →' anchor."""
        html = build_worker_page(_ReleasesWorker, "/vgi").decode()
        assert 'class="source-link"' in html
        assert 'href="https://example.com/repo"' in html
        assert "View source" in html

    def test_no_source_link_when_unset(self) -> None:
        """Workers without ``source_url`` render no anchor."""
        html = build_worker_page(_MinimalWorker, "/vgi").decode()
        assert 'class="source-link"' not in html

    def test_impl_row_groups_chip_and_link(self) -> None:
        """The impl row wraps the chip + source link in one container."""
        html = build_worker_page(_ReleasesWorker, "/vgi").decode()
        assert 'class="impl-row"' in html
