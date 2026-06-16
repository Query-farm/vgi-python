# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Worker description page for VGI HTTP servers.

Generates a pre-rendered HTML page showing worker metadata:
- Worker identity and description
- Functions (scalar, table, table-in-out) with parameters and examples
- Catalog structure (schemas, tables, views)
- Settings

The page is built once at startup (zero per-request cost) and served
as a Falcon resource at ``{prefix}/worker``.
"""

from __future__ import annotations

import html as _html
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from importlib.metadata import version as _pkg_version
from typing import TYPE_CHECKING, Any

import falcon

from vgi.metadata import (
    CatalogFunctionType,
    FunctionStability,
    ResolvedMetadata,
    resolve_metadata,
)

if TYPE_CHECKING:
    from vgi.catalog.attach_option import AttachOptionSpec
    from vgi.catalog.catalog_interface import (
        AttachOpaqueData,
        CatalogDataVersionRelease,
        CatalogInterface,
        FunctionInfo,
        SchemaInfo,
        TableInfo,
        ViewInfo,
    )
    from vgi.worker import Worker

__all__ = [
    "WorkerPageResource",
    "build_worker_page",
]

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_FONT_IMPORTS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700'
    '&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">'
)

# The three display-level function types we show on the page.
_TABLE_IN_OUT_CLASS_NAMES = frozenset(
    {
        "TableInOutGenerator",
        "TableInOutFunction",
    }
)


def _display_function_type(func_cls: type, meta: ResolvedMetadata) -> str:
    """Return a human-readable function type label.

    Differentiates table-in-out from plain table functions by walking the MRO.
    """
    if meta.function_type == CatalogFunctionType.SCALAR:
        return "scalar"
    if meta.function_type == CatalogFunctionType.AGGREGATE:
        return "aggregate"
    # TABLE — check if it's actually a table-in-out
    for klass in func_cls.__mro__:
        if klass.__name__ in _TABLE_IN_OUT_CLASS_NAMES:
            return "table-in-out"
    return "table"


_BADGE_CSS_CLASS = {
    "scalar": "badge-scalar",
    "table": "badge-table",
    "table-in-out": "badge-table-in-out",
    "aggregate": "badge-aggregate",
}


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------


def _esc(value: object) -> str:
    return _html.escape(str(value))


def _build_param_row(p) -> str:  # type: ignore[no-untyped-def]  # ParameterInfo
    """Build a table row for a single function parameter."""
    badges = ""
    if p.is_const:
        badges += ' <span class="mini-badge mini-const">const</span>'
    if p.is_varargs:
        badges += ' <span class="mini-badge mini-varargs">varargs</span>'
    if p.is_table_input:
        badges += ' <span class="mini-badge mini-table-input">table input</span>'

    type_str = _esc(p.type_name) if p.type_name else "&mdash;"
    default_str = _esc(repr(p.default)) if p.default is not None else "&mdash;"
    desc_str = _esc(p.description) if p.description else "&mdash;"

    return (
        f"<tr>"
        f"<td><code>{_esc(p.name)}</code>{badges}</td>"
        f"<td><code>{type_str}</code></td>"
        f"<td>{default_str}</td>"
        f"<td>{desc_str}</td>"
        f"</tr>"
    )


def _build_function_card(func_cls: type, meta: ResolvedMetadata) -> str:
    """Build the HTML card for a single function."""
    display_type = _display_function_type(func_cls, meta)
    badge_cls = _BADGE_CSS_CLASS.get(display_type, "badge-table")

    parts: list[str] = [
        '<div class="card">',
        '<div class="card-header">',
        f'<span class="method-name">{_esc(meta.name)}</span>',
        f'<span class="badge {badge_cls}">{_esc(display_type)}</span>',
    ]

    # Stability badge (only if not the default CONSISTENT)
    if meta.stability != FunctionStability.CONSISTENT:
        parts.append(f'<span class="badge badge-stability">{_esc(meta.stability.name.lower())}</span>')

    parts.append("</div>")  # close card-header

    # Description
    if meta.description:
        parts.append(f'<p class="docstring">{_esc(meta.description)}</p>')

    # Parameters table (skip table_input params for cleaner display)
    visible_params = [p for p in meta.parameters if not p.is_table_input]
    if visible_params:
        parts.append('<div class="section-label">Parameters</div>')
        parts.append("<table><tr><th>Name</th><th>Type</th><th>Default</th><th>Description</th></tr>")
        for p in visible_params:
            parts.append(_build_param_row(p))
        parts.append("</table>")
    else:
        parts.append('<p class="no-params">No parameters</p>')

    # Examples
    if meta.examples:
        parts.append('<div class="section-label">Examples</div>')
        for ex in meta.examples:
            if ex.description:
                parts.append(f'<p class="example-desc">{_esc(ex.description)}</p>')
            parts.append(f"<pre><code>{_esc(ex.sql)}</code></pre>")

    # Capabilities (for table functions)
    caps: list[str] = []
    if meta.filter_pushdown:
        caps.append("filter pushdown")
    if meta.projection_pushdown:
        caps.append("projection pushdown")
    if meta.max_workers is not None:
        caps.append(f"max_workers={meta.max_workers}")
    if caps:
        parts.append('<div class="section-label">Capabilities</div>')
        parts.append(
            '<div class="caps">' + " ".join(f'<span class="cap-tag">{_esc(c)}</span>' for c in caps) + "</div>"
        )

    parts.append("</div>")  # close card
    return "\n".join(parts)


def _build_table_card(table) -> str:  # type: ignore[no-untyped-def]  # catalog Table
    """Build an HTML card for a catalog table."""
    parts: list[str] = [
        '<div class="card">',
        '<div class="card-header">',
        f'<span class="method-name">{_esc(table.name)}</span>',
        '<span class="badge badge-table-obj">table</span>',
    ]
    if table.function is not None:
        parts.append('<span class="mini-badge mini-func-backed">function-backed</span>')
    parts.append("</div>")

    if table.comment:
        parts.append(f'<p class="docstring">{_esc(table.comment)}</p>')

    # Columns
    try:
        cols = table.resolved_columns
    except Exception:
        cols = None

    if cols is not None and len(cols) > 0:
        parts.append('<div class="section-label">Columns</div>')
        parts.append("<table><tr><th>Name</th><th>Type</th></tr>")
        for field in cols:
            parts.append(
                f"<tr><td><code>{_esc(field.name)}</code></td><td><code>{_esc(str(field.type))}</code></td></tr>"
            )
        parts.append("</table>")

    parts.append("</div>")
    return "\n".join(parts)


def _build_view_card(view) -> str:  # type: ignore[no-untyped-def]  # catalog View
    """Build an HTML card for a catalog view."""
    parts: list[str] = [
        '<div class="card">',
        '<div class="card-header">',
        f'<span class="method-name">{_esc(view.name)}</span>',
        '<span class="badge badge-view">view</span>',
        "</div>",
    ]
    if view.comment:
        parts.append(f'<p class="docstring">{_esc(view.comment)}</p>')
    parts.append('<div class="section-label">Definition</div>')
    parts.append(f"<pre><code>{_esc(view.definition)}</code></pre>")
    parts.append("</div>")
    return "\n".join(parts)


@dataclass(frozen=True)
class _CatalogPanel:
    """Per-catalog descriptor used to render the connect section."""

    name: str
    implementation_version: str | None = None
    data_version_spec: str | None = None
    attach_option_specs: tuple[AttachOptionSpec, ...] = ()
    comment: str | None = None
    # Published data-version releases, newest-first. Duplicates by
    # ``version`` are dropped at deserialization (uniqueness contract).
    releases: tuple[CatalogDataVersionRelease, ...] = ()
    # Optional link to where the worker's code lives.
    source_url: str | None = None


def _collect_catalog_panels(worker_cls: type[Worker]) -> list[_CatalogPanel]:
    """Enumerate the catalogs this worker exposes, with their attach options.

    Tries to instantiate the worker's catalog interface and call ``catalogs()``,
    so workers that override discovery to advertise multiple catalogs surface
    correctly. Falls back to the static ``cls.catalog`` descriptor (or the
    legacy ``catalog_name``) when instantiation isn't viable at page-build time.
    """
    from vgi_rpc.utils import deserialize_record_batch

    from vgi.catalog.attach_option import AttachOptionSpec

    static_catalog = getattr(worker_cls, "catalog", None)
    static_name = static_catalog.name if static_catalog is not None else None
    static_comment = static_catalog.comment if static_catalog is not None else None

    iface_cls = None
    try:
        iface_cls = worker_cls._get_catalog_interface()
    except Exception:  # noqa: BLE001 — best-effort discovery
        _logger.debug("could not resolve catalog interface for %s", worker_cls.__name__, exc_info=True)

    if iface_cls is not None:
        try:
            iface = iface_cls()
            infos = list(iface.catalogs())
        except Exception:  # noqa: BLE001 — fall back to static descriptor
            _logger.debug("catalog_interface.catalogs() failed for %s", worker_cls.__name__, exc_info=True)
            infos = []

        panels: list[_CatalogPanel] = []
        for info in infos:
            specs: list[AttachOptionSpec] = []
            for raw in info.attach_option_specs or ():
                try:
                    batch, _ = deserialize_record_batch(bytes(raw))
                    specs.append(AttachOptionSpec.deserialize(batch))
                except Exception:  # noqa: BLE001 — drop unparseable spec, keep the page
                    _logger.debug("failed to deserialize attach option spec", exc_info=True)

            # Defend against duplicate ``version`` entries — the contract is
            # one entry per version, but Arrow can't enforce that.
            seen_versions: set[str] = set()
            releases: list[CatalogDataVersionRelease] = []
            for release in info.releases or ():
                if release.version in seen_versions:
                    _logger.warning(
                        "duplicate version %r in releases for catalog %r; dropping later entry",
                        release.version,
                        info.name,
                    )
                    continue
                seen_versions.add(release.version)
                releases.append(release)

            panels.append(
                _CatalogPanel(
                    name=info.name,
                    implementation_version=info.implementation_version,
                    data_version_spec=info.data_version_spec,
                    attach_option_specs=tuple(specs),
                    comment=static_comment if info.name == static_name else None,
                    releases=tuple(releases),
                    source_url=info.source_url,
                )
            )
        if panels:
            return panels

    # Fallback: single catalog from the static descriptor or legacy attribute.
    fallback_name = static_name or getattr(worker_cls, "catalog_name", None) or worker_cls.__name__.lower()
    static_specs = tuple(getattr(worker_cls, "_attach_option_specs", ()) or ())
    return [
        _CatalogPanel(
            name=fallback_name,
            attach_option_specs=static_specs,
            comment=static_comment,
        )
    ]


def _attach_for_describe(
    worker_cls: type[Worker],
    catalog_name: str,
    requested_data_version: str | None,
) -> tuple[CatalogInterface | None, AttachOpaqueData | None, str | None]:
    """Pre-attach to validate the requested version and capture an attach_opaque_data.

    Returns ``(iface, attach_opaque_data, error)``:

    * ``iface`` is the instantiated catalog interface (or ``None`` if the
      worker has none).
    * ``attach_opaque_data`` is non-``None`` when the attach succeeded; the page uses
      it to dynamically enumerate schemas/tables/views/functions for the
      resolved data version.
    * ``error`` is non-``None`` when the worker rejected the requested
      version (e.g. ``"Unsupported data_version_spec '1.1.1'; this worker
      serves one of ['1.0.0', '1.1.0', '1.2.0']"``). The describe page
      surfaces it as a red banner; the Apply form stays interactive so the
      user can fix the value and resubmit.
    """
    iface_cls = None
    try:
        iface_cls = worker_cls._get_catalog_interface()
    except Exception:  # noqa: BLE001 — best-effort; absence of interface is fine
        _logger.debug("could not resolve catalog interface for %s", worker_cls.__name__, exc_info=True)
    if iface_cls is None:
        return None, None, None
    try:
        iface = iface_cls()
    except Exception:  # noqa: BLE001 — instantiation failure shouldn't kill the page
        _logger.debug("catalog interface instantiation failed for %s", worker_cls.__name__, exc_info=True)
        return None, None, None
    try:
        result = iface.catalog_attach(
            name=catalog_name,
            options={},
            data_version_spec=requested_data_version,
            implementation_version=None,
        )
    except Exception as exc:  # noqa: BLE001 — surface whatever the worker raises
        return iface, None, str(exc) or exc.__class__.__name__
    return iface, result.attach_opaque_data, None


def _build_dynamic_table_card(t: TableInfo) -> str:
    """Render a [`TableInfo`][] (from `iface.schema_contents`) as a card."""
    import pyarrow as pa

    parts: list[str] = [
        '<div class="card">',
        '<div class="card-header">',
        f'<span class="method-name">{_esc(t.name)}</span>',
        '<span class="badge badge-table-obj">table</span>',
        "</div>",
    ]
    if t.comment:
        parts.append(f'<p class="docstring">{_esc(t.comment)}</p>')
    schema = None
    try:
        schema = pa.ipc.read_schema(pa.py_buffer(bytes(t.columns)))
    except Exception:  # noqa: BLE001
        _logger.debug("failed to deserialize table columns for %s", t.name, exc_info=True)
    if schema is not None and len(schema) > 0:
        parts.append('<div class="section-label">Columns</div>')
        parts.append("<table><tr><th>Name</th><th>Type</th></tr>")
        for fld in schema:
            parts.append(f"<tr><td><code>{_esc(fld.name)}</code></td><td><code>{_esc(str(fld.type))}</code></td></tr>")
        parts.append("</table>")
    parts.append("</div>")
    return "\n".join(parts)


def _build_dynamic_view_card(v: ViewInfo) -> str:
    """Render a [`ViewInfo`][] as a card."""
    parts: list[str] = [
        '<div class="card">',
        '<div class="card-header">',
        f'<span class="method-name">{_esc(v.name)}</span>',
        '<span class="badge badge-view">view</span>',
        "</div>",
    ]
    if v.comment:
        parts.append(f'<p class="docstring">{_esc(v.comment)}</p>')
    parts.append('<div class="section-label">Definition</div>')
    parts.append(f"<pre><code>{_esc(v.definition)}</code></pre>")
    parts.append("</div>")
    return "\n".join(parts)


def _build_dynamic_function_card(fn: FunctionInfo) -> str:
    """Render a [`FunctionInfo`][] as a card.

    `FunctionInfo` doesn't preserve the table-vs-table-in-out distinction —
    the protocol-level ``function_type`` lumps them together. Use
    ``has_finalize`` as a heuristic for table-in-out so the badge stays
    accurate where possible.
    """
    import pyarrow as pa

    from vgi.catalog.catalog_interface import FunctionType

    if fn.function_type == FunctionType.SCALAR:
        display = "scalar"
    elif fn.function_type == FunctionType.AGGREGATE:
        display = "aggregate"
    elif fn.has_finalize:
        display = "table-in-out"
    else:
        display = "table"
    badge_cls = _BADGE_CSS_CLASS.get(display, "badge-table")

    parts: list[str] = [
        '<div class="card">',
        '<div class="card-header">',
        f'<span class="method-name">{_esc(fn.name)}</span>',
        f'<span class="badge {badge_cls}">{_esc(display)}</span>',
    ]
    if fn.stability is not None and fn.stability != FunctionStability.CONSISTENT:
        parts.append(f'<span class="badge badge-stability">{_esc(fn.stability.name.lower())}</span>')
    parts.append("</div>")

    if fn.description:
        parts.append(f'<p class="docstring">{_esc(fn.description)}</p>')

    args_schema = None
    try:
        args_schema = pa.ipc.read_schema(pa.py_buffer(bytes(fn.arguments)))
    except Exception:  # noqa: BLE001
        _logger.debug("failed to deserialize function arguments for %s", fn.name, exc_info=True)
    if args_schema is not None and len(args_schema) > 0:
        parts.append('<div class="section-label">Parameters</div>')
        parts.append("<table><tr><th>Name</th><th>Type</th></tr>")
        for fld in args_schema:
            parts.append(f"<tr><td><code>{_esc(fld.name)}</code></td><td><code>{_esc(str(fld.type))}</code></td></tr>")
        parts.append("</table>")
    else:
        parts.append('<p class="no-params">No parameters</p>')

    if fn.examples:
        parts.append('<div class="section-label">Examples</div>')
        for ex in fn.examples:
            if ex.description:
                parts.append(f'<p class="example-desc">{_esc(ex.description)}</p>')
            parts.append(f"<pre><code>{_esc(ex.sql)}</code></pre>")

    caps: list[str] = []
    if fn.filter_pushdown:
        caps.append("filter pushdown")
    if fn.projection_pushdown:
        caps.append("projection pushdown")
    if fn.max_workers is not None:
        caps.append(f"max_workers={fn.max_workers}")
    if caps:
        parts.append('<div class="section-label">Capabilities</div>')
        parts.append(
            '<div class="caps">' + " ".join(f'<span class="cap-tag">{_esc(c)}</span>' for c in caps) + "</div>"
        )

    parts.append("</div>")
    return "\n".join(parts)


def _render_dynamic_schemas(iface: CatalogInterface, attach_opaque_data: AttachOpaqueData) -> list[str]:
    """Enumerate schemas/tables/views/functions from the attached catalog.

    Returns a flat list of HTML fragments (matching the static path's
    ``body_parts`` shape). Best-effort: any exception from the interface
    falls through to an empty list so the page still renders.
    """
    from vgi.catalog.catalog_interface import SchemaObjectType

    try:
        schemas: Sequence[SchemaInfo] = iface.schemas(
            attach_opaque_data=attach_opaque_data, transaction_opaque_data=None
        )
    except Exception:  # noqa: BLE001
        _logger.debug("iface.schemas() failed", exc_info=True)
        return []

    def _safe_contents(name: str, kind: SchemaObjectType) -> list[Any]:
        # The overloads on schema_contents key off Literal[...] values;
        # passing a runtime variable defeats the dispatch, so we cast to
        # Any to fall through to the implementation method.
        contents: Any = iface.schema_contents
        try:
            result = contents(attach_opaque_data=attach_opaque_data, transaction_opaque_data=None, name=name, type=kind)
        except Exception:  # noqa: BLE001
            _logger.debug("schema_contents(%s) failed for %s", kind, name, exc_info=True)
            return []
        return list(result)

    out: list[str] = []
    for schema_info in schemas:
        out.append(f'<h2 class="schema-heading">{_esc(schema_info.name)}</h2>')
        if schema_info.comment:
            out.append(f'<p class="schema-comment">{_esc(schema_info.comment)}</p>')

        # Functions (scalar / table / aggregate, sorted by type then name).
        funcs: list[FunctionInfo] = []
        funcs.extend(_safe_contents(schema_info.name, SchemaObjectType.SCALAR_FUNCTION))
        funcs.extend(_safe_contents(schema_info.name, SchemaObjectType.TABLE_FUNCTION))
        funcs.extend(_safe_contents(schema_info.name, SchemaObjectType.AGGREGATE_FUNCTION))
        if funcs:
            out.append('<div class="section-label">Functions</div>')
            for fn in sorted(funcs, key=lambda f: (f.function_type.value, f.name)):
                out.append(_build_dynamic_function_card(fn))

        # Tables.
        tables = list(_safe_contents(schema_info.name, SchemaObjectType.TABLE))
        if tables:
            out.append('<div class="section-label">Tables</div>')
            for t in tables:
                out.append(_build_dynamic_table_card(t))

        # Views.
        views = list(_safe_contents(schema_info.name, SchemaObjectType.VIEW))
        if views:
            out.append('<div class="section-label">Views</div>')
            for v in views:
                out.append(_build_dynamic_view_card(v))

    return out


def _build_attach_options_table(specs: tuple[AttachOptionSpec, ...]) -> str:
    """Render the attach-options table for a single catalog."""
    if not specs:
        return ""
    parts: list[str] = [
        '<div class="section-label">Attach options</div>',
        "<table><tr><th>Name</th><th>Type</th><th>Default</th><th>Description</th></tr>",
    ]
    for spec in specs:
        default_str = _esc(repr(spec.default)) if spec.default is not None else "&mdash;"
        desc_str = _esc(spec.desc) if spec.desc else "&mdash;"
        parts.append(
            f"<tr><td><code>{_esc(spec.name)}</code></td>"
            f"<td><code>{_esc(str(spec.type))}</code></td>"
            f"<td>{default_str}</td>"
            f"<td>{desc_str}</td></tr>"
        )
    parts.append("</table>")
    return "\n".join(parts)


def _build_attach_sql(catalog_name: str, panel_id: str, requested_version: str | None) -> str:
    """Render the ATTACH SQL block (with copy button) for one catalog.

    When ``requested_version`` is provided (the user clicked Apply), the
    ``data_version_spec`` clause renders inline so the SQL the user copies
    matches the version they submitted. Otherwise the clause is hidden and
    JS toggles it as the user edits the input.
    """
    if requested_version:
        clause = (
            '<span class="dv-clause">, data_version_spec \''
            f'<span class="dv-value">{_esc(requested_version)}</span>\'</span>'
        )
    else:
        clause = '<span class="dv-clause" hidden>, data_version_spec \'<span class="dv-value"></span>\'</span>'
    return (
        '<div class="connect-label">Connect with DuckDB'
        f'<button class="copy-btn" data-copy-target="sql-{panel_id}" title="Copy to clipboard">'
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5">'
        '<rect x="5.5" y="5.5" width="9" height="9" rx="1.5"/>'
        '<path d="M10.5 5.5V2.5a1 1 0 00-1-1h-7a1 1 0 00-1 1v7a1 1 0 001 1h3"/>'
        "</svg></button></div>"
        f'<pre><code id="sql-{panel_id}" class="connect-sql" data-catalog="{_esc(catalog_name)}">'
        f"ATTACH '{_esc(catalog_name)}' AS {_esc(catalog_name)}"
        " (TYPE vgi, LOCATION '<span class=\"connect-url\">{location}</span>'"
        f"{clause}"
        ");</code></pre>"
    )


def _build_release_timeline(panel: _CatalogPanel) -> str:
    """Render the data-version release timeline for one catalog.

    Each row carries the version (clickable to fill the catalog's
    ``.dv-input``), the release date, the one-line summary, and an
    optional ``details →`` link when ``notes_url`` is set. The catalog
    enforces newest-first ordering on the wire; we render in the order
    received.
    """
    parts: list[str] = [
        '<div class="release-timeline" '
        f'data-catalog="{_esc(panel.name)}">'
        '<div class="section-label">Releases</div>'
        '<ul class="release-list">'
    ]
    for release in panel.releases:
        date_str = release.released_at.strftime("%Y-%m-%d") if release.released_at is not None else ""
        notes = (
            f' <a class="release-details" href="{_esc(release.notes_url)}" '
            'target="_blank" rel="noopener">details &rarr;</a>'
            if release.notes_url
            else ""
        )
        summary = f'<span class="release-summary">{_esc(release.summary)}</span>' if release.summary else ""
        parts.append(
            "<li>"
            f'<button type="button" class="release-version" '
            f'data-catalog="{_esc(panel.name)}" data-version="{_esc(release.version)}">'
            f"{_esc(release.version)}</button>"
            f'<span class="release-date">{_esc(date_str)}</span>'
            f"{summary}{notes}"
            "</li>"
        )
    parts.append("</ul></div>")
    return "\n".join(parts)


def _build_connect_section(
    panels: list[_CatalogPanel],
    prefix: str,
    *,
    active_catalog: str | None = None,
    requested_data_version: str | None = None,
) -> str:
    """Build the Connect section: optional catalog selector + per-catalog panel.

    ``active_catalog`` and ``requested_data_version`` come from the request
    query string. When ``active_catalog`` matches a panel, that panel is the
    one rendered without ``hidden``; the data version input on that panel is
    pre-filled with ``requested_data_version`` and the ATTACH SQL bakes the
    clause inline.
    """
    active_index = 0
    if active_catalog is not None:
        for i, panel in enumerate(panels):
            if panel.name == active_catalog:
                active_index = i
                break

    parts: list[str] = ['<div class="connect-box">']

    multi = len(panels) > 1
    if multi:
        parts.append('<div class="catalog-tabs" role="tablist">')
        for i, panel in enumerate(panels):
            active = " active" if i == active_index else ""
            parts.append(
                f'<button class="catalog-tab{active}" role="tab" data-panel="catpanel-{i}">{_esc(panel.name)}</button>'
            )
        parts.append("</div>")

    for i, panel in enumerate(panels):
        hidden_attr = "" if i == active_index else " hidden"
        parts.append(
            f'<div class="catalog-panel" id="catpanel-{i}" role="tabpanel" '
            f'data-catalog="{_esc(panel.name)}"{hidden_attr}>'
        )

        if panel.comment:
            parts.append(f'<p class="catalog-comment">{_esc(panel.comment)}</p>')

        # Implementation chip + optional source link. We render both inline
        # in one row so they line up visually; the source link can appear on
        # its own when there's no implementation version.
        impl_bits: list[str] = []
        if panel.implementation_version:
            impl_bits.append(
                f'<span class="impl-chip">Implementation <code>{_esc(panel.implementation_version)}</code></span>'
            )
        if panel.source_url:
            impl_bits.append(
                f'<a class="source-link" href="{_esc(panel.source_url)}" target="_blank" rel="noopener">'
                "View source &rarr;</a>"
            )
        if impl_bits:
            parts.append('<div class="impl-row">' + " ".join(impl_bits) + "</div>")

        # Apply form: GET reload with ?catalog=&data_version_spec= so the URL
        # is the source of truth for the user's chosen version.
        is_active_panel = i == active_index
        prefilled = requested_data_version if is_active_panel else None
        sql_version = prefilled if prefilled else None
        value_attr = f' value="{_esc(prefilled)}"' if prefilled else ""

        if panel.data_version_spec:
            parts.append(
                f'<form class="dv-form" method="get" data-catalog="{_esc(panel.name)}">'
                f'<input type="hidden" name="catalog" value="{_esc(panel.name)}">'
                '<label class="dv-row">'
                '<span class="dv-label">Data version</span>'
                f'<input type="text" class="dv-input" name="data_version_spec" placeholder="latest"'
                f' data-catalog="{_esc(panel.name)}"'
                f' aria-label="Data version for {_esc(panel.name)}"{value_attr}>'
                '<button type="submit" class="dv-apply">Apply</button>'
                f'<span class="dv-hint">supported: <code>{_esc(panel.data_version_spec)}</code></span>'
                "</label>"
                "</form>"
            )

        if panel.releases:
            parts.append(_build_release_timeline(panel))

        parts.append(_build_attach_sql(panel.name, str(i), sql_version))
        parts.append(_build_attach_options_table(panel.attach_option_specs))

        parts.append("</div>")  # /catalog-panel

    # JS: substitute {location}, wire copy buttons + tab switching + data
    # version input, and keep the Cupola button href in sync with the active
    # catalog and (when set) data_version_spec. Wrapped in DOMContentLoaded
    # because the Cupola button element lives outside this connect-section
    # (rendered later by build_worker_page).
    parts.append(
        '<script>document.addEventListener("DOMContentLoaded",function(){'
        f'var u=location.origin+"{_esc(prefix)}";'
        'document.querySelectorAll(".connect-sql").forEach(function(el){'
        'el.innerHTML=el.innerHTML.replace("{location}",u);});'
        # Copy buttons
        'document.querySelectorAll(".copy-btn").forEach(function(btn){'
        'btn.addEventListener("click",function(){'
        "var t=document.getElementById(btn.dataset.copyTarget);if(!t)return;"
        "navigator.clipboard.writeText(t.textContent).then(function(){"
        'btn.classList.add("copied");'
        'setTimeout(function(){btn.classList.remove("copied")},1500);});});});'
        # Data version input — toggle SQL clause
        "function updateDvClause(inp){"
        "var name=inp.dataset.catalog;"
        "var sql=document.querySelector('.connect-sql[data-catalog=\"'+name+'\"]');"
        "if(!sql)return;"
        'var clause=sql.querySelector(".dv-clause");'
        'var slot=sql.querySelector(".dv-value");'
        "var v=inp.value.trim();"
        "if(v){slot.textContent=v;clause.hidden=false;}"
        "else{clause.hidden=true;}"
        "}"
        'document.querySelectorAll(".dv-input").forEach(function(inp){'
        'inp.addEventListener("input",function(){updateDvClause(inp);updateCupolaHref();});});'
        # Release timeline: clicking a version button fills the matching
        # dv-input and refreshes the SQL clause + Cupola href.
        'document.querySelectorAll(".release-version").forEach(function(btn){'
        'btn.addEventListener("click",function(){'
        "var name=btn.dataset.catalog;"
        "var inp=document.querySelector('.dv-input[data-catalog=\"'+name+'\"]');"
        "if(!inp)return;"
        "inp.value=btn.dataset.version;"
        "updateDvClause(inp);updateCupolaHref();"
        "inp.focus();});});"
        # Tab switching
        'document.querySelectorAll(".catalog-tab").forEach(function(tab){'
        'tab.addEventListener("click",function(){'
        'document.querySelectorAll(".catalog-tab").forEach(function(t){t.classList.remove("active");});'
        'document.querySelectorAll(".catalog-panel").forEach(function(p){p.hidden=true;});'
        'tab.classList.add("active");'
        "var p=document.getElementById(tab.dataset.panel);if(p)p.hidden=false;"
        "updateCupolaHref();});});"
        # Cupola deep-link — reflects active panel + dv input
        "function updateCupolaHref(){"
        'var cb=document.getElementById("cupola-btn");if(!cb)return;'
        'var active=document.querySelector(".catalog-panel:not([hidden])");'
        'var url="https://cupola.query-farm.services/?service="+encodeURIComponent(u);'
        "if(active){"
        'url+="&catalog="+encodeURIComponent(active.dataset.catalog);'
        'var dv=active.querySelector(".dv-input");'
        "if(dv&&dv.value.trim()){"
        'url+="&data_version_spec="+encodeURIComponent(dv.value.trim());'
        "}}"
        "cb.href=url;"
        "}"
        "updateCupolaHref();"
        "});</script>"
    )
    parts.append("</div>")  # /connect-box
    return "\n".join(parts)


def _build_settings_section(setting_specs: list) -> str:  # type: ignore[type-arg]
    """Build the settings section HTML."""
    if not setting_specs:
        return ""
    parts: list[str] = [
        "<h2>Settings</h2>",
        "<table><tr><th>Name</th><th>Type</th><th>Default</th><th>Description</th></tr>",
    ]
    for spec in setting_specs:
        default_str = _esc(repr(spec.default)) if spec.default is not None else "&mdash;"
        parts.append(
            f"<tr><td><code>{_esc(spec.name)}</code></td>"
            f"<td><code>{_esc(str(spec.type))}</code></td>"
            f"<td>{default_str}</td>"
            f"<td>{_esc(spec.desc)}</td></tr>"
        )
    parts.append("</table>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_worker_page(
    worker_cls: type[Worker],
    prefix: str,
    *,
    active_catalog: str | None = None,
    requested_data_version: str | None = None,
) -> bytes:
    """Render the worker description page HTML.

    Extracts metadata from the worker class (functions, catalog, settings)
    and generates a complete HTML page.

    ``active_catalog`` and ``requested_data_version`` come from the request
    query string and let the page reflect the user's submitted form state:
    the named catalog tab is opened, the data-version input is pre-filled,
    and the ATTACH SQL bakes the version clause inline.

    Args:
        worker_cls: The Worker subclass to describe.
        prefix: URL prefix (e.g. ``/api``).
        active_catalog: Catalog name to mark active (from ``?catalog=``).
        requested_data_version: Data version to pre-fill (from
            ``?data_version_spec=``) on the active catalog only.

    Returns:
        UTF-8 encoded HTML bytes.

    """
    worker_name = worker_cls.__name__
    worker_doc = ""
    if worker_cls.__doc__:
        worker_doc = worker_cls.__doc__.strip().split("\n")[0]

    try:
        vgi_version = _pkg_version("vgi-python")
    except Exception:
        vgi_version = "unknown"

    # Collect sections
    body_parts: list[str] = []

    # Connect section: per-catalog ATTACH SQL + attach options.
    panels = _collect_catalog_panels(worker_cls)

    # Pick the catalog to attach against. When the user submitted via Apply
    # we honour ``active_catalog``; otherwise we fall back to the first
    # advertised catalog so the schema/tables list always reflects a real
    # attach (the only way to surface version-dependent content).
    target_catalog = active_catalog or (panels[0].name if panels else None)

    attach_iface: CatalogInterface | None = None
    attach_opaque_data: AttachOpaqueData | None = None
    attach_error: str | None = None
    if target_catalog is not None:
        attach_iface, attach_opaque_data, raw_error = _attach_for_describe(
            worker_cls, target_catalog, requested_data_version
        )
        # Only surface the error in the UI if the user explicitly requested a
        # version. A failure with no version requested is silent (defaults
        # might just be unsupported under unusual configurations).
        if raw_error is not None and requested_data_version:
            attach_error = raw_error

    if attach_error is not None:
        body_parts.append(
            '<div class="dv-error" role="alert">'
            "<strong>Cannot attach with <code>data_version_spec</code> "
            f"<code>{_esc(requested_data_version)}</code>:</strong> "
            f"{_esc(attach_error)}"
            "</div>"
        )

    body_parts.append(
        _build_connect_section(
            panels,
            prefix,
            active_catalog=active_catalog,
            requested_data_version=requested_data_version,
        )
    )

    # Cupola explore button
    body_parts.append(
        '<a class="cupola-box" id="cupola-btn" href="https://cupola.query-farm.services/"'
        ' target="_blank" rel="noopener">'
        '<span class="cupola-icon" aria-hidden="true">'
        '<svg viewBox="0 0 40 40" width="36" height="36" fill="none" stroke="currentColor"'
        ' stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round">'
        '<circle cx="20" cy="3" r="0.9" fill="currentColor" stroke="none"/>'
        '<line x1="20" y1="3.8" x2="20" y2="6"/>'
        '<path d="M16 11 C 16 7 17 6 20 6 C 23 6 24 7 24 11"/>'
        '<rect x="15.5" y="11" width="9" height="4.5"/>'
        '<path d="M17.3 14.8 V13 a0.9 0.9 0 0 1 1.8 0 V14.8"/>'
        '<path d="M20.9 14.8 V13 a0.9 0.9 0 0 1 1.8 0 V14.8"/>'
        '<path d="M20 15.5 L 10 21 L 6 26"/>'
        '<path d="M20 15.5 L 30 21 L 34 26"/>'
        '<line x1="6" y1="26" x2="34" y2="26"/>'
        '<rect x="6" y="26" width="28" height="12"/>'
        '<rect x="18" y="20.5" width="4" height="3"/>'
        '<rect x="15.5" y="29" width="9" height="9"/>'
        '<line x1="20" y1="29" x2="20" y2="38"/>'
        '<line x1="15.5" y1="29" x2="20" y2="33.5"/>'
        '<line x1="24.5" y1="29" x2="20" y2="33.5"/>'
        "</svg>"
        "</span>"
        '<span class="cupola-text">'
        '<span class="cupola-title">Explore this data in Cupola</span>'
        '<span class="cupola-subtitle">Browse schemas, tables, views, and functions interactively'
        " &mdash; no install required.</span>"
        "</span>"
        '<span class="cupola-arrow" aria-hidden="true">'
        '<svg viewBox="0 0 16 16" width="18" height="18" fill="none" stroke="currentColor"'
        ' stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">'
        '<path d="M5 3l6 5-6 5"/>'
        "</svg>"
        "</span>"
        "</a>"
    )

    # Settings
    if worker_cls._setting_specs:
        body_parts.append(_build_settings_section(list(worker_cls._setting_specs)))

    # Schema content. Prefer dynamic enumeration via the attached catalog —
    # that's the only way version-dependent tables (e.g. versioned_tables)
    # show up. Fall back to the static descriptor when the worker has no
    # catalog interface at all (legacy ``Worker.functions`` pattern).
    if attach_iface is not None and attach_opaque_data is not None:
        body_parts.extend(_render_dynamic_schemas(attach_iface, attach_opaque_data))
    elif (catalog := getattr(worker_cls, "catalog", None)) is not None:
        for schema in catalog.schemas:
            body_parts.append(f'<h2 class="schema-heading">{_esc(schema.name)}</h2>')
            if schema.comment:
                body_parts.append(f'<p class="schema-comment">{_esc(schema.comment)}</p>')

            # Functions grouped by display type
            if schema.functions:
                func_cards: list[tuple[str, str, str]] = []
                for func_cls in schema.functions:
                    meta = resolve_metadata(func_cls)
                    display_type = _display_function_type(func_cls, meta)
                    card_html = _build_function_card(func_cls, meta)
                    func_cards.append((display_type, meta.name, card_html))

                # Sort: scalar, then table, then table-in-out, then alpha
                type_order = {"scalar": 0, "table": 1, "table-in-out": 2, "aggregate": 3}
                func_cards.sort(key=lambda t: (type_order.get(t[0], 99), t[1]))

                body_parts.append('<div class="section-label">Functions</div>')
                for _dt, _name, card in func_cards:
                    body_parts.append(card)

            # Tables
            if schema.tables:
                body_parts.append('<div class="section-label">Tables</div>')
                for table in schema.tables:
                    body_parts.append(_build_table_card(table))

            # Views
            if schema.views:
                body_parts.append('<div class="section-label">Views</div>')
                for view in schema.views:
                    body_parts.append(_build_view_card(view))
    else:
        # Legacy: worker.functions list (no schema grouping)
        functions = getattr(worker_cls, "functions", None) or []
        if functions:
            func_cards = []
            for func_cls in functions:
                meta = resolve_metadata(func_cls)
                display_type = _display_function_type(func_cls, meta)
                card_html = _build_function_card(func_cls, meta)
                func_cards.append((display_type, meta.name, card_html))

            type_order = {"scalar": 0, "table": 1, "table-in-out": 2, "aggregate": 3}
            func_cards.sort(key=lambda t: (type_order.get(t[0], 99), t[1]))

            body_parts.append('<div class="section-label">Functions</div>')
            for _dt, _name, card in func_cards:
                body_parts.append(card)

    body_html = "\n".join(body_parts)

    page = _PAGE_TEMPLATE.format(
        worker_name=_esc(worker_name),
        worker_doc=_esc(worker_doc),
        vgi_version=_esc(vgi_version),
        prefix=_esc(prefix),
        body=body_html,
    )
    return page.encode("utf-8")


class WorkerPageResource:
    """Falcon resource serving the worker description page.

    Renders per request so the page can reflect ``?catalog=`` and
    ``?data_version_spec=`` query params submitted via the Apply form.
    Optional ``body_transform`` is applied to the rendered bytes (used by
    ``vgi-serve`` to inject the OAuth/PKCE user-info script).
    """

    __slots__ = ("_body_transform", "_prefix", "_worker_cls")

    def __init__(
        self,
        worker_cls: type[Worker],
        prefix: str,
        body_transform: Callable[[bytes], bytes] | None = None,
    ) -> None:
        """Store the worker class + prefix + optional post-render transform."""
        self._worker_cls = worker_cls
        self._prefix = prefix
        self._body_transform = body_transform

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Render the worker description page, honouring query params."""
        active_catalog = req.get_param("catalog") if req is not None else None
        requested_dv = req.get_param("data_version_spec") if req is not None else None
        body = build_worker_page(
            self._worker_cls,
            self._prefix,
            active_catalog=active_catalog,
            requested_data_version=requested_dv,
        )
        if self._body_transform is not None:
            body = self._body_transform(body)
        resp.content_type = "text/html; charset=utf-8"
        resp.data = body


# ---------------------------------------------------------------------------
# Page template
# ---------------------------------------------------------------------------

_PAGE_TEMPLATE = (
    """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{worker_name} &mdash; VGI Worker</title>
"""
    + _FONT_IMPORTS
    + """
<style>
  body {{ font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 900px;
         margin: 0 auto; padding: 40px 20px 0; color: #2c2c1e; background: #faf8f0; }}
  .header {{ text-align: center; margin-bottom: 40px; }}
  .header .logo img {{ width: 80px; height: 80px; border-radius: 50%;
                       box-shadow: 0 3px 16px rgba(0,0,0,0.10); }}
  .header h1 {{ margin-bottom: 4px; color: #2d5016; font-weight: 700; }}
  .header .subtitle {{ color: #6b6b5a; font-size: 1.1em; margin-top: 0; }}
  .header .meta {{ color: #6b6b5a; font-size: 0.9em; }}
  .header .meta a {{ color: #2d5016; font-weight: 600; }}
  .header .meta a:hover {{ color: #4a7c23; }}
  code {{ font-family: 'JetBrains Mono', monospace; background: #f0ece0;
          padding: 2px 6px; border-radius: 3px; font-size: 0.85em; color: #2c2c1e; }}
  pre {{ background: #f0ece0; padding: 12px 16px; border-radius: 6px;
         overflow-x: auto; font-size: 0.85em; }}
  pre code {{ background: none; padding: 0; }}
  a {{ color: #2d5016; text-decoration: none; }}
  a:hover {{ color: #4a7c23; }}
  h2 {{ color: #2d5016; font-weight: 700; margin-top: 36px; margin-bottom: 16px;
        border-bottom: 2px solid #f0ece0; padding-bottom: 8px; }}
  .schema-heading {{ font-size: 1.3em; }}
  .schema-comment {{ color: #6b6b5a; margin-top: -8px; margin-bottom: 16px; }}
  .card {{ border: 1px solid #f0ece0; border-radius: 8px; padding: 20px;
           margin-bottom: 16px; background: #fff; }}
  .card:hover {{ border-color: #c8a43a; }}
  .card-header {{ display: flex; align-items: center; gap: 10px; margin-bottom: 12px;
                  flex-wrap: wrap; }}
  .method-name {{ font-family: 'JetBrains Mono', monospace; font-size: 1.1em;
                  font-weight: 600; color: #2d5016; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px;
            font-size: 0.75em; font-weight: 600; text-transform: uppercase;
            letter-spacing: 0.03em; }}
  .badge-scalar {{ background: #e8f5e0; color: #2d5016; }}
  .badge-table {{ background: #e0ecf5; color: #1a4a6b; }}
  .badge-table-in-out {{ background: #f5e6f0; color: #6b234a; }}
  .badge-aggregate {{ background: #f5eee0; color: #6b4423; }}
  .badge-stability {{ background: #fff3cd; color: #856404; }}
  .badge-table-obj {{ background: #e0f0f5; color: #1a5a6b; }}
  .badge-view {{ background: #f0e8f5; color: #4a236b; }}
  .mini-badge {{ display: inline-block; padding: 1px 6px; border-radius: 3px;
                 font-size: 0.7em; font-weight: 600; vertical-align: middle;
                 margin-left: 4px; }}
  .mini-const {{ background: #e8f5e0; color: #2d5016; }}
  .mini-varargs {{ background: #f5eee0; color: #6b4423; }}
  .mini-table-input {{ background: #e0ecf5; color: #1a4a6b; }}
  .mini-func-backed {{ background: #f0ece0; color: #6b6b5a; }}
  .connect-box {{ border: 1px solid #e0dcd0; border-radius: 8px; padding: 16px 20px;
                   margin-bottom: 32px; background: #fff; }}
  .connect-box pre {{ margin: 8px 0 0; }}
  .connect-label {{ font-size: 0.8em; font-weight: 600; text-transform: uppercase;
                     letter-spacing: 0.05em; color: #6b6b5a;
                     display: flex; align-items: center; gap: 8px; }}
  .copy-btn {{ background: none; border: 1px solid #e0dcd0; border-radius: 4px;
               padding: 3px 5px; cursor: pointer; color: #6b6b5a;
               transition: all 0.15s ease; display: inline-flex; align-items: center; }}
  .copy-btn:hover {{ color: #2d5016; border-color: #4a7c23; }}
  .copy-btn.copied {{ color: #4a7c23; border-color: #4a7c23; }}
  .connect-url {{ color: #4a7c23; }}
  .catalog-tabs {{ display: flex; gap: 4px; margin-bottom: 12px;
                    border-bottom: 1px solid #e0dcd0; flex-wrap: wrap; }}
  .catalog-tab {{ background: none; border: none; padding: 8px 14px;
                   margin-bottom: -1px; cursor: pointer; font-family: inherit;
                   font-size: 0.9em; font-weight: 600; color: #6b6b5a;
                   border-bottom: 2px solid transparent;
                   transition: color 0.15s, border-color 0.15s; }}
  .catalog-tab:hover {{ color: #2d5016; }}
  .catalog-tab.active {{ color: #2d5016; border-bottom-color: #2d5016; }}
  .catalog-panel[hidden] {{ display: none; }}
  .catalog-comment {{ color: #6b6b5a; margin: 0 0 8px; font-size: 0.9em; }}
  .impl-row {{ display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
                margin: 0 0 10px; font-size: 0.85em; }}
  .impl-chip {{ display: inline-block; font-size: 0.85em; color: #6b6b5a;
                 background: #f0ece0; padding: 3px 10px; border-radius: 999px; }}
  .impl-chip code {{ background: none; padding: 0; color: #2d5016; font-weight: 600; }}
  .source-link {{ color: #2d5016; font-weight: 600; text-decoration: none; }}
  .source-link:hover {{ color: #4a7c23; text-decoration: underline; }}
  .dv-row {{ display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
              margin: 0 0 10px; font-size: 0.9em; }}
  .dv-label {{ font-weight: 600; color: #2c2c1e; }}
  .dv-input {{ font-family: 'JetBrains Mono', monospace; font-size: 0.9em;
                padding: 4px 8px; border: 1px solid #e0dcd0; border-radius: 4px;
                background: #faf8f0; color: #2c2c1e; min-width: 140px; }}
  .dv-input:focus {{ outline: none; border-color: #4a7c23; }}
  .dv-apply {{ font-family: inherit; font-size: 0.85em; font-weight: 600;
                background: #2d5016; color: #faf8f0; border: 1px solid #2d5016;
                padding: 5px 14px; border-radius: 4px; cursor: pointer;
                transition: background 0.15s, border-color 0.15s; }}
  .dv-apply:hover {{ background: #4a7c23; border-color: #4a7c23; }}
  .dv-form {{ margin: 0 0 10px; }}
  .dv-hint {{ color: #6b6b5a; font-size: 0.85em; }}
  .dv-hint code {{ color: #2d5016; }}
  .dv-error {{ background: #fdecea; border: 1px solid #c33; color: #6b1414;
                border-radius: 6px; padding: 12px 16px; margin-bottom: 18px;
                font-size: 0.95em; }}
  .dv-error code {{ background: rgba(195,51,51,0.10); color: #6b1414; }}
  .dv-error strong {{ font-weight: 700; }}
  .release-timeline {{ margin: 0 0 16px; }}
  .release-list {{ list-style: none; padding: 0; margin: 6px 0 0;
                    border: 1px solid #e0dcd0; border-radius: 6px;
                    background: #fff; }}
  .release-list li {{ display: flex; align-items: center; gap: 10px;
                       padding: 8px 12px; border-bottom: 1px solid #f0ece0;
                       font-size: 0.9em; flex-wrap: wrap; }}
  .release-list li:last-child {{ border-bottom: none; }}
  .release-version {{ font-family: 'JetBrains Mono', monospace; font-weight: 600;
                       color: #2d5016; background: #f0ece0; border: 1px solid #e0dcd0;
                       padding: 3px 10px; border-radius: 4px; cursor: pointer;
                       font-size: 0.9em; transition: background 0.15s, color 0.15s; }}
  .release-version:hover {{ background: #2d5016; color: #faf8f0; }}
  .release-date {{ color: #6b6b5a; font-size: 0.85em;
                    font-family: 'JetBrains Mono', monospace; }}
  .release-summary {{ color: #2c2c1e; flex: 1 1 auto; min-width: 180px; }}
  .release-details {{ color: #2d5016; font-weight: 600; font-size: 0.85em;
                       text-decoration: none; }}
  .release-details:hover {{ text-decoration: underline; }}
  .cupola-box {{ display: flex; align-items: center; gap: 16px;
                  border: 1px solid #2d5016; border-radius: 8px; padding: 16px 20px;
                  margin-bottom: 32px; background: #2d5016; color: #faf8f0;
                  text-decoration: none; transition: background 0.15s ease,
                  border-color 0.15s ease, transform 0.15s ease; }}
  .cupola-box:hover {{ background: #3d6a1f; border-color: #4a7c23;
                        color: #faf8f0; transform: translateY(-1px); }}
  .cupola-icon {{ flex: 0 0 auto; display: inline-flex; align-items: center;
                   justify-content: center; width: 56px; height: 56px;
                   border-radius: 50%; background: rgba(255,255,255,0.10);
                   color: #faf8f0; }}
  .cupola-text {{ flex: 1 1 auto; display: flex; flex-direction: column; gap: 2px; }}
  .cupola-title {{ font-size: 1.05em; font-weight: 700; letter-spacing: 0.01em; }}
  .cupola-subtitle {{ font-size: 0.88em; color: #cfd8be; line-height: 1.4; }}
  .cupola-arrow {{ flex: 0 0 auto; color: #cfd8be; transition: transform 0.15s ease; }}
  .cupola-box:hover .cupola-arrow {{ transform: translateX(3px); color: #faf8f0; }}
  .docstring {{ color: #6b6b5a; margin-bottom: 12px; line-height: 1.5; }}
  .example-desc {{ color: #6b6b5a; font-size: 0.9em; margin-bottom: 4px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.9em; }}
  th {{ text-align: left; padding: 8px 10px; background: #f0ece0; color: #2c2c1e;
        font-weight: 600; border-bottom: 2px solid #e0dcd0; }}
  td {{ padding: 8px 10px; border-bottom: 1px solid #f0ece0; }}
  td code {{ font-size: 0.85em; }}
  .no-params {{ color: #6b6b5a; font-style: italic; font-size: 0.9em; }}
  .section-label {{ font-size: 0.8em; font-weight: 600; text-transform: uppercase;
                    letter-spacing: 0.05em; color: #6b6b5a; margin-top: 14px;
                    margin-bottom: 6px; }}
  .caps {{ display: flex; gap: 6px; flex-wrap: wrap; }}
  .cap-tag {{ display: inline-block; padding: 2px 8px; border-radius: 4px;
              font-size: 0.8em; background: #f0ece0; color: #6b6b5a; }}
  footer {{ text-align: center; margin-top: 48px; padding: 20px 0;
            border-top: 1px solid #f0ece0; color: #6b6b5a; font-size: 0.85em; }}
  footer a {{ color: #2d5016; font-weight: 600; }}
  footer a:hover {{ color: #4a7c23; }}
</style>
</head>
<body>
<div class="header">
  <div class="logo">
    <img src="https://vgi-rpc-python.query.farm/assets/logo-hero.png" alt="vgi logo">
  </div>
  <h1>{worker_name}</h1>
  <p class="subtitle">VGI Worker</p>
  <p class="meta">{worker_doc}</p>
  <p class="meta"><code>vgi</code> v{vgi_version}</p>
</div>
{body}
<footer>
  <a href="{prefix}">vgi-rpc endpoint</a>
  &middot;
  <a href="{prefix}/describe">vgi-rpc API reference</a>
  &middot;
  <a href="https://vgi-rpc.query.farm">About <code>vgi-rpc</code></a>
  &middot;
  &copy; 2026 &#x1F69C; <a href="https://query.farm">Query.Farm LLC</a>
</footer>
</body>
</html>"""
)
