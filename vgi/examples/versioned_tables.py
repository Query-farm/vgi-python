"""Example VGI worker whose exposed tables vary per requested data version.

Complements :mod:`vgi.examples.versioned` (which validates versions but shows
no tables) by exercising the *payload-shaping* side of ATTACH-time versioning.

The catalog advertises data_version_spec ``>=1.0.0,<4.0.0`` and supports four
concrete versions with distinct table sets::

    1.0.0 -> animals                      (name, legs, sound)
    1.1.0 -> animals (with color column)  (name, legs, sound, color)
    2.0.0 -> animals + plants
    3.0.0 -> plants

Clients can also request a version by spec rather than exact match. The
resolver accepts:

* exact ``X.Y.Z`` (e.g. ``1.0.0``) — must be in the supported set.
* bare ``X`` (e.g. ``1``) — resolves to the newest ``X.y.z`` supported.
* bare ``X.Y`` (e.g. ``1.0``) — pins to exact ``X.Y.0``.
* npm ``^X.Y.Z`` — newest ``X.y.z >= X.Y.Z``.
* npm ``~X.Y.Z`` — newest ``X.Y.z >= X.Y.Z``.

Registered as the ``vgi-example-versioned-tables-worker`` entry point.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.catalog import (
    AttachId,
    CatalogAttachResult,
    CatalogInfo,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    TableInfo,
    TransactionId,
)
from vgi.invocation import BindResponse
from vgi.table_function import BindParams, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi.worker import Worker

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vgi.catalog.catalog_interface import FunctionInfo, IndexInfo, MacroInfo, ViewInfo
    from vgi_rpc.rpc import CallContext


CATALOG_NAME = "versioned_tables"
DATA_VERSION_SPEC = ">=1.0.0,<4.0.0"
SUPPORTED_VERSIONS: tuple[str, ...] = ("1.0.0", "1.1.0", "2.0.0", "3.0.0")
DEFAULT_VERSION = "3.0.0"

# Implementation versions use a distinctly different numbering (10.x / 11.x)
# from data versions (1.x / 2.x / 3.x) so test assertions can't confuse the
# two dimensions. The advertised implementation_version in vgi_catalogs() is
# the default (newest) — clients that want an older impl pass a spec.
SUPPORTED_IMPLEMENTATION_VERSIONS: tuple[str, ...] = ("10.0.0", "10.1.0", "11.0.0")
DEFAULT_IMPLEMENTATION_VERSION = "11.0.0"

STICKY_COOKIE_NAME = "vgi_sticky"


# ============================================================================
# Static table data
# ============================================================================

_ANIMALS_ROWS = {
    "name": ["chicken", "cow", "horse", "pig", "sheep"],
    "legs": [2, 4, 4, 4, 4],
    "sound": ["cluck", "moo", "neigh", "oink", "baa"],
}

# 1.1.0 adds the color column. Rows are kept in the same order for test
# stability.
_ANIMALS_COLORS = ["red", "brown", "black", "pink", "white"]

_PLANTS_ROWS = {
    "name": ["oak", "pine", "rose", "tomato", "wheat"],
    "kind": ["tree", "tree", "flower", "vegetable", "grass"],
    "height_m": [20.0, 25.0, 0.6, 1.5, 1.0],
}

_ANIMALS_SCHEMA_V1 = pa.schema(
    [
        pa.field("name", pa.string()),
        pa.field("legs", pa.int64()),
        pa.field("sound", pa.string()),
    ]
)

_ANIMALS_SCHEMA_V1_1 = pa.schema(
    [
        pa.field("name", pa.string()),
        pa.field("legs", pa.int64()),
        pa.field("sound", pa.string()),
        pa.field("color", pa.string()),
    ]
)

_PLANTS_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string()),
        pa.field("kind", pa.string()),
        pa.field("height_m", pa.float64()),
    ]
)


# ============================================================================
# Table functions — one per (table, version-variant)
# ============================================================================


@dataclass(slots=True, frozen=True)
class _NoArgs:
    """Scan functions here take no arguments."""


@dataclass(kw_only=True)
class _EmitOnceState(ArrowSerializableDataclass):
    done: bool = False


@init_single_worker
class AnimalsScanFunction(TableFunctionGenerator[_NoArgs, _EmitOnceState]):
    """Scan the 1.0.0 animals table: (name, legs, sound)."""

    class Meta:
        """Function metadata."""

        name = "versioned_tables_animals_scan"
        description = "Animals table for data_version 1.0.0"

    @classmethod
    def on_bind(cls, params: BindParams[_NoArgs]) -> BindResponse:
        """Return fixed schema."""
        return BindResponse(output_schema=_ANIMALS_SCHEMA_V1)

    @classmethod
    def initial_state(cls, params: ProcessParams[_NoArgs]) -> _EmitOnceState:
        """Fresh state per scan."""
        return _EmitOnceState()

    @classmethod
    def process(cls, params: ProcessParams[_NoArgs], state: _EmitOnceState, out: OutputCollector) -> None:
        """Emit rows once, then finish."""
        if state.done:
            out.finish()
            return
        state.done = True
        out.emit(pa.RecordBatch.from_pydict(_ANIMALS_ROWS, schema=params.output_schema))


@init_single_worker
class AnimalsWithColorScanFunction(TableFunctionGenerator[_NoArgs, _EmitOnceState]):
    """Scan the 1.1.0 animals table: same rows plus a ``color`` column."""

    class Meta:
        """Function metadata."""

        name = "versioned_tables_animals_color_scan"
        description = "Animals table for data_version 1.1.0 (with color)"

    @classmethod
    def on_bind(cls, params: BindParams[_NoArgs]) -> BindResponse:
        """Return fixed schema with color."""
        return BindResponse(output_schema=_ANIMALS_SCHEMA_V1_1)

    @classmethod
    def initial_state(cls, params: ProcessParams[_NoArgs]) -> _EmitOnceState:
        """Fresh state per scan."""
        return _EmitOnceState()

    @classmethod
    def process(cls, params: ProcessParams[_NoArgs], state: _EmitOnceState, out: OutputCollector) -> None:
        """Emit rows once, then finish."""
        if state.done:
            out.finish()
            return
        state.done = True
        rows = {**_ANIMALS_ROWS, "color": _ANIMALS_COLORS}
        out.emit(pa.RecordBatch.from_pydict(rows, schema=params.output_schema))


@init_single_worker
class PlantsScanFunction(TableFunctionGenerator[_NoArgs, _EmitOnceState]):
    """Scan the plants table: (name, kind, height_m)."""

    class Meta:
        """Function metadata."""

        name = "versioned_tables_plants_scan"
        description = "Plants table for data_version 2.0.0 and 3.0.0"

    @classmethod
    def on_bind(cls, params: BindParams[_NoArgs]) -> BindResponse:
        """Return fixed schema."""
        return BindResponse(output_schema=_PLANTS_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[_NoArgs]) -> _EmitOnceState:
        """Fresh state per scan."""
        return _EmitOnceState()

    @classmethod
    def process(cls, params: ProcessParams[_NoArgs], state: _EmitOnceState, out: OutputCollector) -> None:
        """Emit rows once, then finish."""
        if state.done:
            out.finish()
            return
        state.done = True
        out.emit(pa.RecordBatch.from_pydict(_PLANTS_ROWS, schema=params.output_schema))


# ============================================================================
# Per-version table spec
# ============================================================================


@dataclass(frozen=True, slots=True)
class _VersionedTable:
    """A (function_name, arrow_schema) tuple for one table variant."""

    function_name: str
    columns: pa.Schema


_ANIMALS_V1 = _VersionedTable(function_name=AnimalsScanFunction.Meta.name, columns=_ANIMALS_SCHEMA_V1)
_ANIMALS_V1_1 = _VersionedTable(function_name=AnimalsWithColorScanFunction.Meta.name, columns=_ANIMALS_SCHEMA_V1_1)
_PLANTS = _VersionedTable(function_name=PlantsScanFunction.Meta.name, columns=_PLANTS_SCHEMA)

VERSION_TABLES: dict[str, dict[str, _VersionedTable]] = {
    "1.0.0": {"animals": _ANIMALS_V1},
    "1.1.0": {"animals": _ANIMALS_V1_1},
    "2.0.0": {"animals": _ANIMALS_V1, "plants": _PLANTS},
    "3.0.0": {"plants": _PLANTS},
}


# ============================================================================
# Version spec resolver (npm-ish) — generic over (supported, default, label)
# ============================================================================

_EXACT_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
_MAJOR_RE = re.compile(r"^(\d+)$")
_MAJOR_MINOR_RE = re.compile(r"^(\d+)\.(\d+)$")
_CARET_RE = re.compile(r"^\^(\d+)\.(\d+)\.(\d+)$")
_TILDE_RE = re.compile(r"^~(\d+)\.(\d+)\.(\d+)$")


def _parse(version: str) -> tuple[int, int, int]:
    """Parse an exact ``X.Y.Z`` version string into a tuple."""
    m = _EXACT_RE.match(version)
    if not m:
        raise ValueError(f"Not a valid version: {version!r}")
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _resolve_against(spec: str, supported: tuple[str, ...], default: str, *, label: str) -> str:
    """Resolve an npm-style spec to a concrete supported version.

    Accepts exact ``X.Y.Z``, bare ``X`` (latest in major), bare ``X.Y``
    (pinned to ``X.Y.0``), caret ``^X.Y.Z`` (newest in major with
    ``(y,z) >= (Y,Z)``), and tilde ``~X.Y.Z`` (newest in major.minor with
    ``z >= Z``). Raises ``ValueError`` with ``label`` in the message when
    nothing matches; the extension surfaces that as the ATTACH failure.
    """
    if not spec:
        return default

    sorted_supported: list[tuple[tuple[int, int, int], str]] = sorted(
        (_parse(v), v) for v in supported
    )

    # Exact X.Y.Z
    if _EXACT_RE.match(spec):
        if spec in supported:
            return spec
        raise ValueError(f"Unsupported {label} {spec!r}; this worker serves {list(supported)}")

    # Bare major `X`: latest X.y.z
    m = _MAJOR_RE.match(spec)
    if m:
        major = int(m.group(1))
        candidates = [v for t, v in sorted_supported if t[0] == major]
        if not candidates:
            raise ValueError(f"Unsupported {label} {spec!r}; no major {major} version available")
        return candidates[-1]

    # Bare major.minor `X.Y`: pinned to X.Y.0
    m = _MAJOR_MINOR_RE.match(spec)
    if m:
        pinned = f"{m.group(1)}.{m.group(2)}.0"
        if pinned in supported:
            return pinned
        raise ValueError(f"Unsupported {label} {spec!r}; {pinned!r} not in {list(supported)}")

    # Caret `^X.Y.Z`
    m = _CARET_RE.match(spec)
    if m:
        base = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
        candidates = [v for t, v in sorted_supported if t[0] == base[0] and t >= base]
        if not candidates:
            raise ValueError(f"Unsupported {label} {spec!r}; no match in major {base[0]}")
        return candidates[-1]

    # Tilde `~X.Y.Z`
    m = _TILDE_RE.match(spec)
    if m:
        base = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
        candidates = [v for t, v in sorted_supported if t[0] == base[0] and t[1] == base[1] and t >= base]
        if not candidates:
            raise ValueError(f"Unsupported {label} {spec!r}; no match in {base[0]}.{base[1]}.x")
        return candidates[-1]

    raise ValueError(f"Unsupported {label} {spec!r}; accepted forms: X.Y.Z, X, X.Y, ^X.Y.Z, ~X.Y.Z")


def _resolve_data_version(spec: str) -> str:
    return _resolve_against(spec, SUPPORTED_VERSIONS, DEFAULT_VERSION, label="data_version_spec")


def _resolve_impl_version(spec: str) -> str:
    return _resolve_against(
        spec, SUPPORTED_IMPLEMENTATION_VERSIONS, DEFAULT_IMPLEMENTATION_VERSION, label="implementation_version"
    )


# ============================================================================
# Catalog interface
# ============================================================================


class VersionedTablesCatalog(ReadOnlyCatalogInterface):
    """Catalog whose visible tables depend on the resolved data version."""

    catalog_name = CATALOG_NAME

    # Worker.functions also registers these, but listing them on the catalog
    # interface is harmless and keeps the class self-describing.
    functions = [AnimalsScanFunction, AnimalsWithColorScanFunction, PlantsScanFunction]

    # The resolved data version is encoded directly into the attach_id rather
    # than kept in per-instance state. The worker pool can dispatch RPCs for
    # a single catalog across multiple subprocesses, so any state kept on
    # `self` would be invisible to sibling workers. Embedding the version in
    # the attach_id sidesteps that entirely.
    #
    # Wire format: ``<resolved_version>\x00<uuid16>`` — the null byte splits
    # the decodable version prefix from uuid entropy that keeps attach_ids
    # unique per call.
    _ATTACH_ID_SEP = b"\x00"

    # ------------------------------------------------------------------ catalogs

    def catalogs(self) -> list[CatalogInfo]:
        """Advertise catalog name and version metadata for discovery.

        ``implementation_version`` advertises the default (newest) impl. Clients
        that want an older impl pass a spec (``^10.0.0``, ``~10.0.0``, etc.)
        via the ATTACH ``implementation_version`` option.
        """
        return [
            CatalogInfo(
                name=CATALOG_NAME,
                implementation_version=DEFAULT_IMPLEMENTATION_VERSION,
                data_version_spec=DATA_VERSION_SPEC,
            ),
        ]

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str = "",
        implementation_version: str = "",
        ctx: "CallContext | None" = None,
    ) -> CatalogAttachResult:
        """Validate versions, record the resolved data version, return attach result."""
        del options
        if name != CATALOG_NAME:
            raise ValueError(f"Unknown catalog: {name!r}. Available: {CATALOG_NAME}")

        resolved_impl = _resolve_impl_version(implementation_version)
        resolved = _resolve_data_version(data_version_spec)

        attach_id = AttachId(resolved.encode("utf-8") + self._ATTACH_ID_SEP + uuid.uuid4().bytes)

        # Pin HTTP sessions. Ignored by subprocess transport.
        if ctx is not None:
            try:
                ctx.set_cookie(STICKY_COOKIE_NAME, uuid.uuid4().hex)
            except RuntimeError:
                pass

        return CatalogAttachResult(
            attach_id=attach_id,
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_id_required=True,
            default_schema="main",
            resolved_data_version=resolved,
            resolved_implementation_version=resolved_impl,
        )

    # ------------------------------------------------------------------ helpers

    def _tables_for(self, attach_id: AttachId) -> dict[str, _VersionedTable]:
        raw = bytes(attach_id)
        sep = raw.find(self._ATTACH_ID_SEP)
        if sep <= 0:
            return {}
        version = raw[:sep].decode("utf-8", errors="replace")
        return VERSION_TABLES.get(version, {})

    @staticmethod
    def _make_table_info(name: str, table: _VersionedTable) -> TableInfo:
        return TableInfo(
            comment=None,
            tags={},
            name=name,
            schema_name="main",
            columns=SerializedSchema(table.columns.serialize().to_pybytes()),
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

    # ------------------------------------------------------------------ schemas / tables

    def schemas(self, *, attach_id: AttachId, transaction_id: TransactionId | None) -> list[SchemaInfo]:
        """Single ``main`` schema, regardless of version."""
        del transaction_id
        return [SchemaInfo(attach_id=attach_id, name="main", comment=None, tags={})]

    def schema_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
    ) -> SchemaInfo | None:
        """Return the ``main`` schema only."""
        del transaction_id
        if name.lower() != "main":
            return None
        return SchemaInfo(attach_id=attach_id, name="main", comment=None, tags={})

    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: SchemaObjectType,
    ) -> "Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo]":
        """List objects in the schema — tables filtered by attach's resolved version."""
        del transaction_id
        if name.lower() != "main":
            return []
        if type == SchemaObjectType.TABLE:
            return [self._make_table_info(n, t) for n, t in sorted(self._tables_for(attach_id).items())]
        return []

    def table_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        """Return table info only if it exists at this attach's resolved version."""
        del transaction_id, at_unit, at_value
        if schema_name.lower() != "main":
            return None
        table = self._tables_for(attach_id).get(name.lower())
        if table is None:
            return None
        return self._make_table_info(name.lower(), table)

    def view_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> None:
        """No views exposed."""
        del attach_id, transaction_id, schema_name, name
        return None

    def table_scan_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        """Dispatch to the function backing this table at the attach's version."""
        del transaction_id, at_unit, at_value
        if schema_name.lower() != "main":
            raise ValueError(f"Unknown schema: {schema_name}")
        table = self._tables_for(attach_id).get(name.lower())
        if table is None:
            raise ValueError(f"Table {schema_name}.{name} not visible at this data version")
        return ScanFunctionResult(
            function_name=table.function_name,
            positional_arguments=[],
            named_arguments={},
            required_extensions=[],
        )

    def catalog_version(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        ctx: "CallContext | None" = None,
    ) -> int:
        """Assert cookie stickiness on HTTP and return a constant version."""
        del transaction_id
        if ctx is not None and ctx.cookies and STICKY_COOKIE_NAME not in ctx.cookies:
            raise ValueError(
                f"expected cookie {STICKY_COOKIE_NAME!r} on follow-up request; got {sorted(ctx.cookies)}",
            )
        # Non-zero int; table set only changes across ATTACHes, not within one.
        return 1


# ============================================================================
# Worker + entry point
# ============================================================================


class VersionedTablesWorker(Worker):
    """Worker exposing :class:`VersionedTablesCatalog`."""

    catalog_interface = VersionedTablesCatalog
    catalog_name = CATALOG_NAME
    functions = [AnimalsScanFunction, AnimalsWithColorScanFunction, PlantsScanFunction]


def main() -> None:
    """Run the versioned-tables worker process."""
    VersionedTablesWorker.main()


if __name__ == "__main__":
    main()
