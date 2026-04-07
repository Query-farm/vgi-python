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
from importlib.metadata import version as _pkg_version
from typing import TYPE_CHECKING

import falcon

from vgi.metadata import (
    CatalogFunctionType,
    FunctionStability,
    ResolvedMetadata,
    resolve_metadata,
)

if TYPE_CHECKING:
    from vgi.worker import Worker

__all__ = [
    "WorkerPageResource",
    "build_worker_page",
]


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


def build_worker_page(worker_cls: type[Worker], prefix: str) -> bytes:
    """Pre-render the worker description page HTML.

    Extracts metadata from the worker class (functions, catalog, settings)
    and generates a complete HTML page.  The result is UTF-8 encoded bytes
    suitable for serving as a static response.

    Args:
        worker_cls: The Worker subclass to describe.
        prefix: URL prefix (e.g. ``/api``).

    Returns:
        UTF-8 encoded HTML bytes.

    """
    worker_name = worker_cls.__name__
    worker_doc = ""
    if worker_cls.__doc__:
        worker_doc = worker_cls.__doc__.strip().split("\n")[0]

    try:
        vgi_version = _pkg_version("vgi")
    except Exception:
        vgi_version = "unknown"

    # Collect sections
    body_parts: list[str] = []

    # Connection snippet
    catalog = getattr(worker_cls, "catalog", None)
    catalog_name = catalog.name if catalog is not None else worker_name.lower()
    body_parts.append(
        f'<div class="connect-box">'
        f'<div class="connect-label">Connect with DuckDB'
        f'<button class="copy-btn" id="copy-btn" title="Copy to clipboard">'
        f'<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5">'
        f'<rect x="5.5" y="5.5" width="9" height="9" rx="1.5"/>'
        f'<path d="M10.5 5.5V2.5a1 1 0 00-1-1h-7a1 1 0 00-1 1v7a1 1 0 001 1h3"/>'
        f"</svg>"
        f"</button>"
        f"</div>"
        f'<pre><code id="connect-sql">'
        f"ATTACH '{_esc(catalog_name)}' AS {_esc(catalog_name)}"
        f" (TYPE vgi, LOCATION '<span class=\"connect-url\">{{location}}</span>');"
        f"</code></pre>"
        f"<script>"
        f'(function(){{var u=location.origin+"{_esc(prefix)}";'
        f'var el=document.getElementById("connect-sql");'
        f'if(el)el.innerHTML=el.innerHTML.replace("{{location}}",u);'
        f'var btn=document.getElementById("copy-btn");'
        f"if(btn)btn.onclick=function(){{var t=el.textContent;"
        f'navigator.clipboard.writeText(t).then(function(){{btn.classList.add("copied");'
        f'setTimeout(function(){{btn.classList.remove("copied")}},1500)}})}};}})();'
        f"</script>"
        f"</div>"
    )

    # Settings
    if worker_cls._setting_specs:
        body_parts.append(_build_settings_section(list(worker_cls._setting_specs)))

    # Build per-schema content
    catalog = getattr(worker_cls, "catalog", None)
    if catalog is not None:
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
    """Falcon resource serving the pre-rendered worker description page."""

    __slots__ = ("_body",)

    def __init__(self, body: bytes) -> None:
        """Store pre-rendered HTML body."""
        self._body = body

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Return the worker description page HTML."""
        resp.content_type = "text/html; charset=utf-8"
        resp.data = self._body


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
