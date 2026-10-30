# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""vgi.v3 regressions: old override signatures, silent read-only tables, ATTACH rules.

Each test pins a failure a worker hit after moving to vgi.v3: overrides
written for the 2.x ``CatalogInterface`` raising TypeError on every ATTACH,
constrained writable tables silently served read-only, declared indexes
failing catalog discovery, and records the extension rejects at ATTACH being
served without complaint.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any, ClassVar, cast

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient
from vgi_rpc.rpc import OutputCollector

from vgi import Worker
from vgi.catalog import (
    AttachOpaqueData,
    Catalog,
    CatalogAttachResult,
    ConstraintInfo,
    Index,
    OnConflict,
    ReadOnlyCatalogInterface,
    ReferencedKey,
    Schema,
    SchemaObjectType,
    SerializedSchema,
    SqlExpression,
    Table,
    TableInfo,
    catalog_capabilities_for,
    constraint_record,
    index_definition,
    served_table_schema,
    validate_constraint_records,
    validate_foreign_key_targets,
)
from vgi.catalog._dispatch import call_catalog
from vgi.catalog._expression_grammar import _parse
from vgi.catalog.capabilities import (
    CONSTRAINT_ENFORCE,
    CONSTRAINT_RECORDS_READ,
    INDEX_DEFINITION_READ,
    ROW_SCALAR_EXPRESSION,
    CatalogCapabilities,
)
from vgi.catalog.sql import (
    GENERATED_EXPRESSION_KEY,
    canonical_generated_columns,
    column_expressions,
    validate_generated_columns,
)
from vgi.client import DEFAULT_CLIENT_CAPABILITIES, Client
from vgi.serve import create_app
from vgi.table_function import ProcessParams, TableFunctionGenerator, bind_fixed_schema, init_single_worker
from vgi.table_in_out_function import TableInOutGenerator

KEY = b"v3-regressions-signing-key"
TRUSTED: dict[str, Any] = {"enforcement": "ENFORCED", "validation": "ALL_ROWS"}
HAS_ENGINE = _parse("SELECT 1") is not None
needs_engine = pytest.mark.skipif(not HAS_ENGINE, reason="no DuckDB engine installed for grammar checks")


def _client(worker: type[Worker]) -> Client:
    return Client.from_http(
        "http://vgi.test", httpx_client=_SyncTestClient(create_app(worker, describe=False, signing_key=KEY))
    )


def _record(table: str, name: str, kind: str, columns: list[list[str]], **kwargs: Any) -> ConstraintInfo:
    return constraint_record(["main"], table, name, kind, columns, **{**TRUSTED, **kwargs})


# --- signature-tolerant dispatch to pre-v3 overrides ---


class _OldSignatureCatalog(ReadOnlyCatalogInterface):
    """Overrides written against the 2.x signatures: explicit parameters, no **kwargs."""

    catalog = Catalog(name="old", default_schema="main", schemas=[Schema(path=["main"])])
    calls: ClassVar[list[tuple[str, dict[str, Any]]]] = []

    def catalog_attach(  # type: ignore[override]
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        ctx: Any = None,
    ) -> CatalogAttachResult:
        self.calls.append(("catalog_attach", {"name": name}))
        return super().catalog_attach(
            name=name, options=options, data_version_spec=data_version_spec, implementation_version=None
        )

    def schema_create(  # type: ignore[override]
        self,
        *,
        attach_opaque_data: bytes,
        transaction_opaque_data: bytes | None,
        path: list[str],
        on_conflict: OnConflict,
        comment: str | None,
        tags: dict[str, str],
    ) -> None:
        self.calls.append(("schema_create", {"path": path}))

    def view_create(  # type: ignore[override]
        self,
        *,
        attach_opaque_data: bytes,
        transaction_opaque_data: bytes | None,
        schema_path: list[str],
        name: str,
        definition: str,
        on_conflict: OnConflict,
    ) -> None:
        self.calls.append(("view_create", {"name": name}))

    def table_column_drop(  # type: ignore[override]
        self,
        *,
        attach_opaque_data: bytes,
        transaction_opaque_data: bytes | None,
        schema_path: list[str],
        name: str,
        column_name: str,
        ignore_not_found: bool,
        if_column_exists: bool,
        cascade: bool,
    ) -> None:
        self.calls.append(("table_column_drop", {"column_name": column_name}))

    def table_create(  # type: ignore[override]
        self,
        *,
        attach_opaque_data: bytes,
        transaction_opaque_data: bytes | None,
        schema_path: list[str],
        name: str,
        columns: bytes,
        on_conflict: OnConflict,
        not_null_constraints: list[int],
        unique_constraints: list[list[int]],
        check_constraints: list[str],
    ) -> None:
        self.calls.append(("table_create", {"not_null_constraints": not_null_constraints}))


class _OldSignatureWorker(Worker):
    catalog_interface = _OldSignatureCatalog


def test_old_catalog_attach_override_attaches_with_a_client_offer() -> None:
    """vgi-adbc/ducklake/email/hardshell: TypeError on every ATTACH before the fix."""
    _OldSignatureCatalog.calls.clear()
    with warnings.catch_warnings(record=True) as caught, _client(_OldSignatureWorker) as client:
        warnings.simplefilter("always")
        client.catalog_attach(name="old", data_version_spec=None, implementation_version=None)
    assert _OldSignatureCatalog.calls == [("catalog_attach", {"name": "old"})]
    assert any(issubclass(w.category, DeprecationWarning) and "client_capabilities" in str(w.message) for w in caught)


def test_old_overrides_receive_only_parameters_they_accept() -> None:
    catalog = _OldSignatureCatalog()
    _OldSignatureCatalog.calls.clear()
    common = {"attach_opaque_data": b"a", "transaction_opaque_data": None}
    with pytest.warns(DeprecationWarning, match="options="):
        call_catalog(
            catalog, "schema_create", **common, path=["s"], on_conflict=OnConflict.ERROR, comment=None, tags={},
            options=None,
        )  # fmt: skip
    with pytest.warns(DeprecationWarning):
        call_catalog(
            catalog, "view_create", **common, schema_path=["main"], name="v", definition="SELECT 1",
            on_conflict=OnConflict.ERROR, aliases=[], security="regular", comment=None, tags={}, column_comments={},
        )  # fmt: skip
    with pytest.warns(DeprecationWarning, match="column_name"):
        call_catalog(
            catalog, "table_column_drop", **common, schema_path=["main"], name="t", column_path=["c"],
            ignore_not_found=False, if_column_exists=False, cascade=False,
        )  # fmt: skip
    with pytest.warns(DeprecationWarning, match="removed not_null_constraints"):
        call_catalog(
            catalog, "table_create", **common, schema_path=["main"], name="t", columns=b"",
            on_conflict=OnConflict.ERROR, constraint_definitions=[],
        )  # fmt: skip
    assert [call[0] for call in _OldSignatureCatalog.calls] == [
        "schema_create",
        "view_create",
        "table_column_drop",
        "table_create",
    ]
    assert _OldSignatureCatalog.calls[2][1] == {"column_name": "c"}
    assert _OldSignatureCatalog.calls[3][1] == {"not_null_constraints": []}


def test_meaningful_new_arguments_are_never_silently_dropped() -> None:
    catalog = _OldSignatureCatalog()
    common = {"attach_opaque_data": b"a", "transaction_opaque_data": None}
    with pytest.raises(NotImplementedError, match="security='barrier'"):
        call_catalog(
            catalog, "view_create", **common, schema_path=["main"], name="v", definition="SELECT 1",
            on_conflict=OnConflict.ERROR, security="barrier",
        )  # fmt: skip
    with pytest.raises(NotImplementedError, match="nested column path"):
        call_catalog(
            catalog, "table_column_drop", **common, schema_path=["main"], name="t", column_path=["s", "c"],
            ignore_not_found=False, if_column_exists=False, cascade=False,
        )  # fmt: skip
    with pytest.raises(NotImplementedError, match="constraint_definitions"):
        call_catalog(
            catalog, "table_create", **common, schema_path=["main"], name="t", columns=b"",
            on_conflict=OnConflict.ERROR, constraint_definitions=[b"x"],
        )  # fmt: skip


def test_kwargs_overrides_receive_everything() -> None:
    received: dict[str, Any] = {}

    class Catalog_:
        def schema_create(self, **kwargs: Any) -> None:
            received.update(kwargs)

    call_catalog(Catalog_(), "schema_create", path=["s"], options=None)
    assert received == {"path": ["s"], "options": None}


# --- constrained writable tables negotiate enforcement instead of going read-only ---


@dataclass(frozen=True)
class _NoArgs:
    """No arguments."""


@init_single_worker
@bind_fixed_schema
class _Messages(TableFunctionGenerator[_NoArgs]):
    """Function-backed table whose output schema is non-nullable."""

    class Meta:  # noqa: D106
        name = "messages"

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        cast(Any, [pa.field("id", pa.int64(), nullable=False), pa.field("subject", pa.string(), nullable=False)])
    )

    @classmethod
    def process(cls, params: ProcessParams[_NoArgs], state: None, out: OutputCollector) -> None:  # noqa: D102
        out.finish()


class _MessagesInsert(TableInOutGenerator[None, None]):
    """Stub insert function."""

    class Meta:  # noqa: D106
        name = "messages_insert"


_MESSAGES = Table(
    name="messages",
    function=_Messages,
    insert_function=_MessagesInsert,
    constraints=(_record("messages", "pk", "PRIMARY_KEY", [["id"]]),),
)


class _MailCatalog(ReadOnlyCatalogInterface):
    catalog = Catalog(
        name="mail",
        default_schema="main",
        schemas=[
            Schema(
                path=["main"],
                tables=[_MESSAGES],
                indexes=[
                    Index(
                        name="messages_id", definition=index_definition(["main"], "messages", [["id"]], algorithm="x")
                    )
                ],
            )
        ],
    )


class _MailWorker(Worker):
    catalog_interface = _MailCatalog


def test_constrained_writable_table_advertises_write_enforcement_and_index_read() -> None:
    available = _MailCatalog().catalog_capabilities(attach_opaque_data=AttachOpaqueData(b""))
    assert available.write == [CONSTRAINT_ENFORCE]
    assert {CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION, INDEX_DEFINITION_READ} <= set(available.read)


def test_constrained_writable_table_stays_writable_over_the_wire() -> None:
    """vgi-email: ENFORCED records + insert_function were silently served read-only."""
    with _client(_MailWorker) as client:
        attach = client.catalog_attach(name="mail", data_version_spec=None, implementation_version=None)
        assert attach.selected_capabilities is not None
        assert CONSTRAINT_ENFORCE in attach.selected_capabilities.write
        tables = client.schema_contents(
            attach_opaque_data=attach.attach_opaque_data, path=["main"], type=SchemaObjectType.TABLE
        )
        (table,) = tables
        assert table.insert_function is not None
        assert table.write_result_modes == {"insert": "count"}
        served = pa.ipc.read_schema(pa.py_buffer(table.columns))
        # Function-backed output: the PK column stays non-nullable, the rest is relaxed.
        assert not served.field("id").nullable
        assert served.field("subject").nullable
        # Declared indexes no longer break whole-catalog discovery.
        contents = client.load_catalog(attach=attach)
        assert contents is not None


def test_unnegotiated_writes_log_instead_of_silently_disappearing(caplog: pytest.LogCaptureFixture) -> None:
    info = _MESSAGES.to_table_info(["main"])
    with caplog.at_level("WARNING", logger="vgi.worker"):
        served = Worker._table_for_capabilities(
            info, CatalogCapabilities(read=sorted([CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION]))
        )
    assert served.insert_function is None
    assert "served read-only" in caplog.text


def test_catalog_capabilities_for_custom_catalogs() -> None:
    info = _MESSAGES.to_table_info(["main"])
    index = Index(name="i", definition=index_definition(["main"], "messages", [["id"]], algorithm="x"))
    capabilities = catalog_capabilities_for([info], [index.to_index_info(["main"])])
    assert capabilities.write == [CONSTRAINT_ENFORCE]
    assert INDEX_DEFINITION_READ in capabilities.read
    assert catalog_capabilities_for([]) == CatalogCapabilities()
    assert CatalogCapabilities(read=[INDEX_DEFINITION_READ]).union(capabilities) == capabilities


def test_index_definition_builder_encodes_options() -> None:
    definition = index_definition(
        ["main"], "t", [["a"], ["b"]], algorithm="art", options={"fill": 70}, constraint_type="UNIQUE"
    )
    assert [key.column_path for key in definition.key_definitions()] == [["a"], ["b"]]
    assert definition.options.batch.to_pylist() == [{"fill": 70}]
    with pytest.raises(TypeError, match="component paths"):
        index_definition(["main"], "t", cast(Any, ["a"]), algorithm="art")


def test_default_client_offer_matches_decodable_profiles() -> None:
    assert DEFAULT_CLIENT_CAPABILITIES.catalog is not None
    assert {CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION, INDEX_DEFINITION_READ} <= set(
        DEFAULT_CLIENT_CAPABILITIES.catalog.read
    )
    assert DEFAULT_CLIENT_CAPABILITIES.catalog.write == [CONSTRAINT_ENFORCE]


# --- legacy constructors fail with the replacement named ---


def test_legacy_table_info_and_sql_expression_name_the_replacement() -> None:
    with pytest.raises(TypeError, match=r"not_null_constraints=\.\.\.\) was removed in vgi.v3: .*constraint_record"):
        TableInfo(
            name="t", schema_path=["main"], comment=None, tags={}, columns=SerializedSchema(b""),
            not_null_constraints=[0],  # type: ignore[call-arg]
        )  # fmt: skip
    with pytest.raises(TypeError, match=r"SqlExpression.scalar\(sql, schema_path\)"):
        SqlExpression("a + 1")  # type: ignore[call-arg]
    assert SqlExpression.scalar("a + 1", ["main"]).profile == ROW_SCALAR_EXPRESSION


def test_legacy_foreign_key_classes_are_deprecated() -> None:
    from vgi.catalog import ForeignKeyDef

    with pytest.warns(DeprecationWarning, match="constraint_record"):
        ForeignKeyDef(columns=("a",), referenced_table="t", referenced_columns=("id",))


# --- ATTACH rules checked SDK-side ---

COLUMNS = pa.schema(
    cast(
        Any,
        [
            pa.field("id", pa.int64()),
            pa.field("n", pa.int32()),
            pa.field("s", pa.string()),
            pa.field("bbox", pa.struct([("x", pa.int64())])),
        ],
    )
)


@pytest.mark.parametrize(
    ("records", "message"),
    [
        ([_record("t", "a", "NOT_NULL", [["id"]]), _record("t", "a", "NOT_NULL", [["n"]])], "duplicate constraint id"),
        (
            [
                _record("t", "a", "UNIQUE", [["id"]], declared_name="Key"),
                _record("t", "b", "UNIQUE", [["n"]], declared_name="key"),
            ],
            "duplicate declared name",
        ),
        ([_record("t", "a", "PRIMARY_KEY", [["id"]]), _record("t", "b", "PRIMARY_KEY", [["n"]])], "more than one"),
        ([_record("t", "a", "NOT_NULL", [["missing"]])], r"constraint 'a': column path \['missing'\]"),
        ([_record("t", "a", "UNIQUE", [["bbox", "x"]])], "only NOT_NULL may name a nested column path"),
        ([_record("t", "a", "NOT_NULL", [["bbox", "y"]])], "missing or ambiguous nested field"),
        (
            [
                constraint_record(
                    ["main"],
                    "t",
                    "fk",
                    "FOREIGN_KEY",
                    [["id"]],
                    references=ReferencedKey(schema_path=["other"], table="p", columns=[["id"]], constraint_id="pk"),
                    **TRUSTED,
                )  # fmt: skip
            ],
            "same schema",
        ),
    ],
)
def test_records_duckdb_rejects_at_attach_fail_in_process(records: list[ConstraintInfo], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        validate_constraint_records("t", COLUMNS, records, schema_path=["main"])


def test_record_schema_path_must_match_the_table() -> None:
    record = constraint_record(["other"], "t", "a", "NOT_NULL", [["id"]], **TRUSTED)
    with pytest.raises(ValueError, match=r"schema_path \['other'\]"):
        validate_constraint_records("t", COLUMNS, [record], schema_path=["main"])


def test_foreign_key_targets_are_checked_across_tables() -> None:
    fk = constraint_record(
        ["main"], "t", "fk", "FOREIGN_KEY", [["id"]],
        references=ReferencedKey(schema_path=["main"], table="p", columns=[["id"]], constraint_id="p.pk"),
        **TRUSTED,
    )  # fmt: skip
    pk = constraint_record(["main"], "p", "p.pk", "PRIMARY_KEY", [["id"]], **TRUSTED)
    validate_foreign_key_targets({(("main",), "t"): [fk], (("main",), "p"): [pk]})
    with pytest.raises(ValueError, match="is not served"):
        validate_foreign_key_targets({(("main",), "t"): [fk]})
    with pytest.raises(ValueError, match="not a PRIMARY_KEY or UNIQUE record"):
        validate_foreign_key_targets({(("main",), "t"): [fk], (("main",), "p"): []})
    catalog = type(
        "_FkCatalog",
        (ReadOnlyCatalogInterface,),
        {
            "catalog": Catalog(
                name="c",
                default_schema="main",
                schemas=[Schema(path=["main"], tables=[Table(name="t", columns=COLUMNS, constraints=(fk,))])],
            )
        },
    )()
    with pytest.raises(ValueError, match="is not served"):
        catalog.catalog_capabilities(attach_opaque_data=AttachOpaqueData(b""))


@needs_engine
@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("n >= 0 AND n < 10", "Boolean numeric comparisons"),
        ("s = 'x'", "CHECK column 's' is string"),
        ("n > 'a'", "numeric operands"),
        ("abs(n) > 0", "only comparisons between numeric columns and constants"),
        ("t.n > 0", "unqualified"),
        ("q > 0", "missing or synthetic"),
    ],
)
def test_check_outside_the_extension_grammar_fails(sql: str, message: str) -> None:
    record = _record("t", "c", "CHECK", [], check_sql=sql)
    with pytest.raises(ValueError, match=message):
        validate_constraint_records("t", COLUMNS, [record])


@needs_engine
def test_check_in_the_extension_grammar_passes() -> None:
    validate_constraint_records("t", COLUMNS, [_record("t", "c", "CHECK", [], check_sql="N >= -1.5")])


# --- generated columns ---


def test_legacy_generated_expression_is_translated() -> None:
    legacy = pa.schema(
        [pa.field("a", pa.int64()), pa.field("b", pa.int64(), metadata={b"generated_expression": b"a + 1"})]
    )
    canonical = canonical_generated_columns(legacy, ["main"])
    expressions = column_expressions(canonical.field("b"))
    assert expressions.generated is not None and expressions.generated.sql == "a + 1"
    assert expressions.generation == "virtual"
    assert b"generated_expression" not in (canonical.field("b").metadata or {})
    # The worker serves the canonical form instead of rejecting the table.
    info = TableInfo(
        name="t", schema_path=["main"], comment=None, tags={},
        columns=SerializedSchema(legacy.serialize().to_pybytes()), constraints=[],
    )  # fmt: skip
    served = Worker._table_for_capabilities(info, CatalogCapabilities())
    assert GENERATED_EXPRESSION_KEY in (pa.ipc.read_schema(pa.py_buffer(served.columns)).field("b").metadata or {})


def _generated(sql: str, dtype: pa.DataType | None = None, *, nullable: bool = True) -> pa.Schema:
    field = pa.field("g", dtype or pa.int64(), nullable=nullable, metadata={b"generated_expression": sql.encode()})
    return canonical_generated_columns(pa.schema([pa.field("a", pa.int64()), field]), ["main"])


def test_generated_column_rules() -> None:
    validate_generated_columns(_generated("a + 1"), ["main"])
    with pytest.raises(ValueError, match="must be nullable"):
        validate_generated_columns(_generated("a + 1", nullable=False), ["main"])
    with pytest.raises(ValueError, match="bool, int8-64 or string"):
        validate_generated_columns(_generated("a + 1", pa.float64()), ["main"])
    with pytest.raises(ValueError, match="owned by the table's schema"):
        validate_generated_columns(_generated("a + 1"), ["other"])
    stored = _generated("a + 1")
    metadata: dict[bytes | str, bytes | str] = dict(cast(Any, stored.field("g").metadata))
    metadata[b"vgi.column.generation.v1"] = b"stored"
    with pytest.raises(NotImplementedError, match="VIRTUAL"):
        validate_generated_columns(stored.set(1, stored.field("g").with_metadata(metadata)), ["main"])
    # A non-nullable generated field that no record names is relaxed, not rejected.
    Table(name="t", columns=pa.schema([pa.field("a", pa.int64()), pa.field("g", pa.int64(), nullable=False)]),
          generated_columns={"g": "a + 1"})  # fmt: skip
    with pytest.raises(ValueError, match="must be nullable"):
        Table(
            name="t",
            columns=pa.schema([pa.field("a", pa.int64()), pa.field("g", pa.int64())]),
            generated_columns={"g": "a + 1"},
            constraints=(_record("t", "nn", "NOT_NULL", [["g"]]),),
        )


@needs_engine
@pytest.mark.parametrize("sql", ["a / 2", "a + 1.5", "random()", "TRY_CAST(a AS INTEGER)", "a IN (1, 2)"])
def test_generated_expression_outside_the_grammar_fails(sql: str) -> None:
    with pytest.raises(ValueError, match="generated column 'g'"):
        validate_generated_columns(_generated(sql), ["main"])


@needs_engine
@pytest.mark.parametrize("sql", ["a + 1", "CAST(a AS VARCHAR) || 'x'", "coalesce(a, 0) % 3", "NOT (a IS NULL)"])
def test_generated_expression_in_the_grammar_passes(sql: str) -> None:
    dtype = pa.string() if "||" in sql else pa.bool_() if "NOT" in sql else pa.int64()
    validate_generated_columns(_generated(sql, dtype), ["main"])


def test_served_table_schema_is_public_and_idempotent() -> None:
    record = _record("t", "nn", "NOT_NULL", [["bbox", "x"]])
    once = served_table_schema(COLUMNS, [record])
    assert served_table_schema(once, [record]).equals(once)
    assert not once.field("bbox").type.field("x").nullable
