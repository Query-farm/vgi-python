"""Build the versioned ``describe.json`` landing contract for a :class:`Worker`.

The shared static landing page — one self-contained ``landing.html`` served
byte-identically by every VGI language worker — fetches ``GET
{prefix}/describe.json`` (same origin) and renders it. This module is the Python
**reference producer** for that contract; the other language implementations must
emit an equivalent document (guarded by the cross-language conformance harness).

The contract is versioned by ``landing_schema_version`` independently of the VGI
wire protocol: additive fields do not bump it, breaking changes do. Table/view
columns are **lazy** — the document carries only a column count, and the page
fetches per-object detail from :func:`build_columns_json`
(``GET {prefix}/describe/{catalog}/{schema}/{table}.json``) on first expand.

See ``~/Development/vgi/docs/http-landing-contract.md`` for the normative spec.
"""

from __future__ import annotations

import contextlib
import json
import logging
from importlib.metadata import version as _pkg_version
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from vgi.argument_spec import (
    VGI_ARG_KEY,
    VGI_ARG_NAMED,
    VGI_DEFAULT_KEY,
    VGI_DOC_KEY,
    VGI_TYPE_KEY,
    VGI_TYPE_TABLE,
)
from vgi.catalog.catalog_interface import (
    CatalogAttachResult,
    CatalogInterface,
    FunctionInfo,
    FunctionType,
    MacroInfo,
    MacroType,
    SchemaObjectType,
    TableInfo,
    ViewInfo,
)
from vgi.http.worker_page import _CatalogPanel, _collect_catalog_panels

if TYPE_CHECKING:
    from vgi.catalog.catalog_interface import AttachOpaqueData
    from vgi.worker import Worker

logger = logging.getLogger(__name__)

LANDING_SCHEMA_VERSION = 1
CUPOLA_BASE = "https://cupola.query-farm.services"

# ``duckdb_databases().tags`` keys (the reserved ``vgi.*`` namespace, per
# vgi-lint-check TAGS.md) surfaced in the landing page's catalog card.
_STRING_TAGS = {
    "title": "vgi.title",
    "doc_md": "vgi.doc_md",
    "source_url": "vgi.source_url",
    "license": "vgi.license",
    "author": "vgi.author",
    "copyright": "vgi.copyright",
    "support_contact": "vgi.support_contact",
    "support_policy_url": "vgi.support_policy_url",
}
_KEYWORDS_TAG = "vgi.keywords"  # JSON array encoded as a string in the tags MAP


def _pkg_ver() -> str:
    try:
        return _pkg_version("vgi-python")
    except Exception:  # noqa: BLE001
        return "unknown"


# ---------------------------------------------------------------------------
# Attach (capture the full result so we can read tags + resolved versions)
# ---------------------------------------------------------------------------


def _attach(
    worker_cls: type[Worker], catalog_name: str, data_version: str | None
) -> tuple[CatalogInterface | None, CatalogAttachResult | None]:
    """Instantiate the catalog interface and attach; return ``(iface, result)``.

    Unlike ``worker_page._attach_for_describe`` (which discards everything but the
    opaque handle), this keeps the whole :class:`CatalogAttachResult` so we can
    read ``tags``, ``comment``, and the resolved versions. Best-effort: any
    failure yields ``(iface_or_none, None)`` so the endpoint still responds.
    """
    try:
        iface_cls = worker_cls._get_catalog_interface()
    except Exception:  # noqa: BLE001
        logger.debug("no catalog interface for %s", worker_cls.__name__, exc_info=True)
        return None, None
    if iface_cls is None:
        return None, None
    try:
        iface = iface_cls()
    except Exception:  # noqa: BLE001
        logger.debug("catalog interface instantiation failed for %s", worker_cls.__name__, exc_info=True)
        return None, None
    try:
        result = iface.catalog_attach(
            name=catalog_name,
            options={},
            data_version_spec=data_version,
            implementation_version=None,
        )
    except Exception:  # noqa: BLE001 — unsupported version etc.; still render structure
        logger.debug("catalog_attach failed for %s/%s", worker_cls.__name__, catalog_name, exc_info=True)
        return iface, None
    return iface, result


# ---------------------------------------------------------------------------
# Field / schema helpers
# ---------------------------------------------------------------------------


def _meta(field: pa.Field[Any], key: bytes) -> str | None:
    md = field.metadata or {}
    raw = md.get(key)
    return raw.decode("utf-8") if raw is not None else None


def _column(field: pa.Field[Any]) -> dict[str, Any]:
    col: dict[str, Any] = {"name": field.name, "type": str(field.type)}
    comment = _meta(field, b"comment") or _meta(field, VGI_DOC_KEY)
    if comment:
        col["comment"] = comment
    return col


def _read_schema(raw: bytes) -> pa.Schema | None:
    try:
        return pa.ipc.read_schema(pa.py_buffer(bytes(raw)))
    except Exception:  # noqa: BLE001
        logger.debug("failed to deserialize an Arrow schema", exc_info=True)
        return None


def _function_display_type(fn: FunctionInfo) -> str:
    if fn.function_type == FunctionType.SCALAR:
        return "scalar"
    if fn.function_type == FunctionType.AGGREGATE:
        return "aggregate"
    if fn.has_finalize:
        return "table_in_out"
    return "table"


def _function_returns(fn: FunctionInfo) -> str | None:
    schema = _read_schema(bytes(fn.output_schema))
    if schema is None or len(schema) == 0:
        return None
    if fn.function_type in (FunctionType.SCALAR, FunctionType.AGGREGATE):
        # Scalar/aggregate output is a single "result" column.
        return str(schema.field(0).type)
    cols = ", ".join(f"{f.name} {f.type}" for f in schema)
    return f"TABLE({cols})"


def _function_args(fn: FunctionInfo) -> list[dict[str, Any]]:
    schema = _read_schema(bytes(fn.arguments))
    if schema is None:
        return []
    args: list[dict[str, Any]] = []
    for field in schema:
        # Skip the piped input relation of a table-in-out function; it's not a
        # user-supplied argument.
        if _meta(field, VGI_TYPE_KEY) == VGI_TYPE_TABLE.decode():
            continue
        arg: dict[str, Any] = {"name": field.name, "type": str(field.type)}
        md = field.metadata or {}
        if md.get(VGI_ARG_KEY) == VGI_ARG_NAMED:
            arg["named"] = True
        doc = _meta(field, VGI_DOC_KEY)
        if doc:
            arg["desc"] = doc
        default = _meta(field, VGI_DEFAULT_KEY)
        if default is not None:
            # Stored as a JSON scalar; render the decoded value for display.
            try:
                arg["default"] = json.dumps(json.loads(default))
            except Exception:  # noqa: BLE001
                arg["default"] = default
        args.append(arg)
    return args


def _macro_display_type(m: MacroInfo) -> str:
    # A scalar macro is invoked exactly like a scalar function in SQL, and a
    # table macro like a table function; surface them in the same buckets so the
    # landing page lists a catalog's full callable surface (VGI workers commonly
    # expose their "functions" as declarative macros — see vgi-volcanos).
    return "scalar" if m.macro_type == MacroType.SCALAR else "table"


def _macro_args(m: MacroInfo) -> list[dict[str, Any]]:
    # Defaulted parameters are optional and callable by name in DuckDB, so we
    # present them as named args (with their default); the rest are positional.
    defaults: dict[str, Any] = {}
    if m.parameter_default_values is not None:
        batch = m.parameter_default_values
        for col_name in batch.schema.names:
            with contextlib.suppress(Exception):
                defaults[col_name] = batch.column(col_name)[0].as_py()

    raw = m.arguments_schema
    if raw is None:
        schema = None
    elif isinstance(raw, pa.Schema):
        # In-process catalog interfaces hand back a live schema; only the
        # serialized wire form is IPC bytes.
        schema = raw
    else:
        schema = _read_schema(bytes(raw))
    fields = list(schema) if schema is not None else None

    args: list[dict[str, Any]] = []
    names = [f.name for f in fields] if fields is not None else list(m.parameters)
    field_by_name = {f.name: f for f in fields} if fields is not None else {}
    for name in names:
        field = field_by_name.get(name)
        # Macro parameters are untyped unless a typed default pins them; show
        # ANY rather than the Arrow null placeholder.
        if field is not None and not pa.types.is_null(field.type):
            arg: dict[str, Any] = {"name": name, "type": str(field.type)}
        else:
            arg = {"name": name, "type": "ANY"}
        doc = _meta(field, VGI_DOC_KEY) if field is not None else None
        if doc:
            arg["desc"] = doc
        if name in defaults:
            arg["named"] = True
            try:
                arg["default"] = json.dumps(defaults[name])
            except Exception:  # noqa: BLE001
                arg["default"] = str(defaults[name])
        args.append(arg)
    return args


def _macro_to_dict(m: MacroInfo) -> dict[str, Any]:
    return {
        "name": m.name,
        "type": _macro_display_type(m),
        "doc": m.comment or "",
        "args": _macro_args(m),
    }


# ---------------------------------------------------------------------------
# Catalog contents
# ---------------------------------------------------------------------------


def _schema_contents(
    iface: CatalogInterface, attach_opaque_data: AttachOpaqueData, name: str, kind: SchemaObjectType
) -> list[Any]:
    contents: Any = iface.schema_contents  # runtime kind defeats the Literal overloads
    try:
        return list(contents(attach_opaque_data=attach_opaque_data, transaction_opaque_data=None, name=name, type=kind))
    except Exception:  # noqa: BLE001
        logger.debug("schema_contents(%s) failed for %s", kind, name, exc_info=True)
        return []


def _build_schemas(
    iface: CatalogInterface, attach_opaque_data: AttachOpaqueData
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Return ``(schemas, counts)`` for the attached catalog."""
    try:
        schema_infos = list(iface.schemas(attach_opaque_data=attach_opaque_data, transaction_opaque_data=None))
    except Exception:  # noqa: BLE001
        logger.debug("iface.schemas() failed", exc_info=True)
        return [], {"schemas": 0, "tables": 0, "views": 0, "functions": 0}

    schemas: list[dict[str, Any]] = []
    totals = {"schemas": 0, "tables": 0, "views": 0, "functions": 0}
    # Deterministic ordering so describe.json (and the conformance golden) is
    # stable across platforms / Python versions / worker iteration order.
    for si in sorted(schema_infos, key=lambda s: s.name):
        tables_raw: list[TableInfo] = _schema_contents(iface, attach_opaque_data, si.name, SchemaObjectType.TABLE)
        views_raw: list[ViewInfo] = _schema_contents(iface, attach_opaque_data, si.name, SchemaObjectType.VIEW)
        funcs_raw: list[FunctionInfo] = []
        for kind in (
            SchemaObjectType.SCALAR_FUNCTION,
            SchemaObjectType.TABLE_FUNCTION,
            SchemaObjectType.AGGREGATE_FUNCTION,
        ):
            funcs_raw.extend(_schema_contents(iface, attach_opaque_data, si.name, kind))
        macros_raw: list[MacroInfo] = []
        for kind in (SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO):
            macros_raw.extend(_schema_contents(iface, attach_opaque_data, si.name, kind))

        tables = []
        for t in sorted(tables_raw, key=lambda x: x.name):
            schema = _read_schema(bytes(t.columns))
            tables.append(
                {"name": t.name, "cols": len(schema) if schema is not None else 0, "comment": t.comment or ""}
            )

        views = []
        for v in sorted(views_raw, key=lambda x: x.name):
            views.append(
                {
                    "name": v.name,
                    "cols": len(v.column_comments),
                    "comment": v.comment or "",
                    "def": v.definition,
                }
            )

        functions = [
            {
                "name": fn.name,
                "type": _function_display_type(fn),
                "doc": fn.description or "",
                "args": _function_args(fn),
                **({"returns": r} if (r := _function_returns(fn)) else {}),
            }
            for fn in funcs_raw
        ]
        functions.extend(_macro_to_dict(m) for m in macros_raw)
        # Deterministic ordering across functions + macros (both fold into the
        # same scalar/table/aggregate/table_in_out buckets on the landing page).
        functions.sort(key=lambda d: (d["type"], d["name"]))

        schema_entry: dict[str, Any] = {"name": si.name, "tables": tables, "views": views, "functions": functions}
        if si.comment:
            schema_entry["doc"] = si.comment
        schemas.append(schema_entry)
        totals["schemas"] += 1
        totals["tables"] += len(tables)
        totals["views"] += len(views)
        totals["functions"] += len(functions)
    return schemas, totals


def _catalog_tags(result: CatalogAttachResult | None) -> dict[str, Any]:
    if result is None or not result.tags:
        return {}
    tags = result.tags
    out: dict[str, Any] = {}
    for key, tag in _STRING_TAGS.items():
        val = tags.get(tag)
        if val:
            out[key] = val
    kw = tags.get(_KEYWORDS_TAG)
    if kw:
        try:
            parsed = json.loads(kw)
            if isinstance(parsed, list):
                out["keywords"] = [str(k) for k in parsed]
        except Exception:  # noqa: BLE001
            logger.debug("could not parse %s tag %r", _KEYWORDS_TAG, kw, exc_info=True)
    return out


def _build_catalog(worker_cls: type[Worker], panel: _CatalogPanel) -> dict[str, Any]:
    iface, result = _attach(worker_cls, panel.name, None)

    attach_options = [
        {"name": s.name, "type": str(s.type), "default": _default_str(s), "description": getattr(s, "description", "")}
        for s in panel.attach_option_specs
    ]
    data_versions = [{"spec": r.version, **({"label": r.summary} if r.summary else {})} for r in (panel.releases or ())]

    impl = panel.implementation_version
    dvs = panel.data_version_spec
    if result is not None:
        impl = result.resolved_implementation_version or impl
        dvs = result.resolved_data_version or dvs

    catalog: dict[str, Any] = {
        "name": panel.name,
        "implementation_version": impl,
        "data_version_spec": dvs,
        "data_versions": data_versions,
        "attach_options": attach_options,
        "tags": _catalog_tags(result),
    }

    if iface is not None and result is not None:
        schemas, counts = _build_schemas(iface, result.attach_opaque_data)
    else:
        schemas, counts = [], {"schemas": 0, "tables": 0, "views": 0, "functions": 0}
    catalog["counts"] = counts
    catalog["schemas"] = schemas
    return catalog


def _default_str(spec: Any) -> str:
    default = getattr(spec, "default", None)
    return "" if default is None else str(default)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_describe_json(
    worker_cls: type[Worker],
    *,
    oauth: bool = False,
    server_id: str = "",
    cupola_base: str = CUPOLA_BASE,
) -> dict[str, Any]:
    """Build the full ``describe.json`` document for ``worker_cls``.

    ``oauth`` and ``server_id`` are supplied by the HTTP server (they depend on
    runtime config, not the worker class).
    """
    doc_line = ""
    if worker_cls.__doc__:
        doc_line = worker_cls.__doc__.strip().split("\n")[0]

    catalogs = [_build_catalog(worker_cls, panel) for panel in _collect_catalog_panels(worker_cls)]

    return {
        "landing_schema_version": LANDING_SCHEMA_VERSION,
        "worker": {
            "name": worker_cls.__name__,
            "doc": doc_line,
            "version": _pkg_ver(),
            "lang": "python",
        },
        "server_id": server_id,
        "oauth": oauth,
        "cupola_base": cupola_base,
        "catalogs": catalogs,
    }


def build_columns_json(worker_cls: type[Worker], catalog: str, schema: str, table: str) -> dict[str, Any] | None:
    """Build the lazy per-object column payload.

    Returns ``{"columns": [{"name", "type", "comment"?}]}`` for the given table or
    view, or ``None`` if the object can't be found. Tables deserialize their Arrow
    column schema; views expose their declared column comments (types are only
    known after binding the SQL, which the worker does not do here).
    """
    iface, result = _attach(worker_cls, catalog, None)
    if iface is None or result is None:
        return None
    aod = result.attach_opaque_data
    for t in _schema_contents(iface, aod, schema, SchemaObjectType.TABLE):
        if t.name == table:
            arrow = _read_schema(bytes(t.columns))
            return {"columns": [_column(f) for f in (arrow or [])]}
    for v in _schema_contents(iface, aod, schema, SchemaObjectType.VIEW):
        if v.name == table:
            return {"columns": [{"name": n, "type": "", "comment": c} for n, c in v.column_comments.items()]}
    return None
