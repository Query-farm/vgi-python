# Copyright 2026 Query Farm LLC - https://query.farm

"""Secret-lookup conformance fixtures.

Implements the language-neutral fixture contract that the shared
``test/sql/integration/secret/secret_lookup_*.test`` files run against every
SDK's example worker. The contract (function names, declared secret
requirements, output columns, value rendering) is written down in the vgi repo
at ``docs/secret_lookup_fixtures.md``; keep this module in step with it.

Every function reads secrets of the ``vgi_example`` type registered by the
example worker. Value-returning functions emit exactly one row
``(found BOOLEAN, secret_name VARCHAR, value VARCHAR)``.

* ``secret_value_by_type(field)`` — requirement declared statically in the
  simplest form (the bare type string); selected by TYPE, so an unnamed
  ``__default_vgi_example`` and a secret named unlike its type both match.
* ``secret_value_by_name(name, field)`` — requested by NAME at bind (two-phase).
* ``secret_value_for_scope(path, field)`` — three static requirements of one
  type (unscoped, ``vgi-conformance://a/``, ``vgi-conformance://a/b/``); the
  worker selects among the delivered secrets by longest scope prefix of
  ``path``, falling back to the unscoped one.
* ``secret_value_required(field)`` — like ``secret_value_by_type`` but the
  secret is required: bind fails when it is absent.
* ``secret_types_seen()`` — requires ``vgi_example`` (bare string) and ``s3``
  scoped to ``s3://vgi-conformance/``; emits one row per secret delivered.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg, SecretLookupEntry
from vgi.function import Function
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.schema_utils import schema
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

#: The secret type every value fixture reads (registered by the example worker).
SECRET_TYPE = "vgi_example"

#: Static scopes declared by ``secret_value_for_scope`` (nested on purpose, so
#: longest-prefix selection is observable).
SCOPE_A = "vgi-conformance://a/"
SCOPE_AB = "vgi-conformance://a/b/"

#: The second, foreign secret type ``secret_types_seen`` requires, and its scope.
#: DuckDB's ``s3`` secrets default to the scope ``s3://``, so they must be asked
#: for by scope (an empty-scope lookup only matches unscoped secrets).
S3_SCOPE = "s3://vgi-conformance/"

_VALUE_SCHEMA = schema(found=pa.bool_(), secret_name=pa.string(), value=pa.string())


def _render(value: Any) -> str | None:
    """Render a secret field as text: NULL stays NULL, booleans are lowercase."""
    py = value.as_py() if isinstance(value, pa.Scalar) else value
    if py is None:
        return None
    if isinstance(py, bool):
        return "true" if py else "false"
    return str(py)


@dataclass(kw_only=True)
class _ValueState(ArrowSerializableDataclass):
    """The single ``(found, secret_name, value)`` row to emit."""

    found: bool = False
    secret_name: str | None = None
    value: str | None = None
    done: bool = False


def _state_for(secret: dict[str, Any] | None, field_name: str) -> _ValueState:
    """Build the output row for a selected secret (or its absence)."""
    if not secret:
        return _ValueState()
    return _ValueState(found=True, secret_name=_render(secret.get("name")), value=_render(secret.get(field_name)))


def _emit_value_row(params: ProcessParams[Any], state: _ValueState, out: OutputCollector) -> None:
    """Emit the one ``(found, secret_name, value)`` row, then finish."""
    if state.done:
        out.finish()
        return
    out.emit(
        pa.RecordBatch.from_pydict(
            {"found": [state.found], "secret_name": [state.secret_name], "value": [state.value]},
            schema=params.output_schema,
        )
    )
    state.done = True


# ---------------------------------------------------------------------------
# secret_value_by_type
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class SecretFieldArgs:
    """A single secret field name."""

    field: Annotated[str, Arg(0, doc="Secret field to return")]


@init_single_worker
@bind_fixed_schema
class SecretValueByTypeFunction(TableFunctionGenerator[SecretFieldArgs, _ValueState]):
    """Select the ``vgi_example`` secret by TYPE from a static, bare-string requirement."""

    class Meta:
        """Metadata for SecretValueByTypeFunction."""

        name = "secret_value_by_type"
        description = "Conformance: a field of the vgi_example secret, selected by type"
        categories = ["secret", "testing"]
        # The simplest declaration form: a bare type string.
        required_secrets = [SECRET_TYPE]
        examples = [FunctionExample(sql="SELECT * FROM secret_value_by_type('api_key')", description="By type")]

    FunctionArguments = SecretFieldArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = _VALUE_SCHEMA

    @classmethod
    def initial_state(cls, params: ProcessParams[SecretFieldArgs]) -> _ValueState:
        """Select by type among the secrets the static requirement delivered."""
        return _state_for(params.secrets.lookup(SECRET_TYPE), params.args.field)

    @classmethod
    def process(cls, params: ProcessParams[SecretFieldArgs], state: _ValueState, out: OutputCollector) -> None:
        """Emit the single row."""
        _emit_value_row(params, state, out)


# ---------------------------------------------------------------------------
# secret_value_required
# ---------------------------------------------------------------------------
@init_single_worker
class SecretValueRequiredFunction(TableFunctionGenerator[SecretFieldArgs, _ValueState]):
    """Like ``secret_value_by_type``, but bind fails when the secret is absent."""

    class Meta:
        """Metadata for SecretValueRequiredFunction."""

        name = "secret_value_required"
        description = "Conformance: a field of the required vgi_example secret; errors when absent"
        categories = ["secret", "testing"]
        required_secrets = [SECRET_TYPE]
        examples = [FunctionExample(sql="SELECT * FROM secret_value_required('api_key')", description="Required")]

    FunctionArguments = SecretFieldArgs

    @classmethod
    def on_bind(cls, params: BindParams[SecretFieldArgs]) -> BindResponse:
        """Fail the bind when no ``vgi_example`` secret was resolved."""
        params.secrets.get(SECRET_TYPE, required=True)
        return BindResponse(output_schema=_VALUE_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[SecretFieldArgs]) -> _ValueState:
        """Select by type."""
        return _state_for(params.secrets.lookup(SECRET_TYPE), params.args.field)

    @classmethod
    def process(cls, params: ProcessParams[SecretFieldArgs], state: _ValueState, out: OutputCollector) -> None:
        """Emit the single row."""
        _emit_value_row(params, state, out)


# ---------------------------------------------------------------------------
# secret_value_by_name
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class SecretNameFieldArgs:
    """A secret name and a field name."""

    name: Annotated[str, Arg(0, doc="Secret name to look up")]
    field: Annotated[str, Arg(1, doc="Secret field to return")]


@init_single_worker
class SecretValueByNameFunction(TableFunctionGenerator[SecretNameFieldArgs, _ValueState]):
    """Request a ``vgi_example`` secret by NAME at bind and return one of its fields."""

    class Meta:
        """Metadata for SecretValueByNameFunction."""

        name = "secret_value_by_name"
        description = "Conformance: a field of the named vgi_example secret"
        categories = ["secret", "testing"]
        examples = [
            FunctionExample(sql="SELECT * FROM secret_value_by_name('my_secret', 'api_key')", description="By name")
        ]

    FunctionArguments = SecretNameFieldArgs

    @classmethod
    def on_bind(cls, params: BindParams[SecretNameFieldArgs]) -> BindResponse:
        """Request the named secret (two-phase bind)."""
        params.secrets.get(SECRET_TYPE, name=params.args.name)
        return BindResponse(output_schema=_VALUE_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[SecretNameFieldArgs]) -> _ValueState:
        """Select the delivered secret by name (and type)."""
        return _state_for(params.secrets.lookup(SECRET_TYPE, name=params.args.name), params.args.field)

    @classmethod
    def process(cls, params: ProcessParams[SecretNameFieldArgs], state: _ValueState, out: OutputCollector) -> None:
        """Emit the single row."""
        _emit_value_row(params, state, out)


# ---------------------------------------------------------------------------
# secret_value_for_scope
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class SecretPathFieldArgs:
    """A path to select a secret for, and a field name."""

    path: Annotated[str, Arg(0, doc="Path to select a secret for (longest scope prefix)")]
    field: Annotated[str, Arg(1, doc="Secret field to return")]


@init_single_worker
@bind_fixed_schema
class SecretValueForScopeFunction(TableFunctionGenerator[SecretPathFieldArgs, _ValueState]):
    """Select among several same-type secrets by the longest scope prefix of ``path``."""

    class Meta:
        """Metadata for SecretValueForScopeFunction."""

        name = "secret_value_for_scope"
        description = "Conformance: a field of the vgi_example secret whose scope best matches a path"
        categories = ["secret", "testing"]
        required_secrets = [
            SECRET_TYPE,
            SecretLookupEntry(secret_type=SECRET_TYPE, scope=SCOPE_A),
            SecretLookupEntry(secret_type=SECRET_TYPE, scope=SCOPE_AB),
        ]
        examples = [
            FunctionExample(
                sql="SELECT * FROM secret_value_for_scope('vgi-conformance://a/b/x', 'api_key')",
                description="Longest-prefix scope selection",
            )
        ]

    FunctionArguments = SecretPathFieldArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = _VALUE_SCHEMA

    @classmethod
    def initial_state(cls, params: ProcessParams[SecretPathFieldArgs]) -> _ValueState:
        """Select worker-side by scope among the delivered ``vgi_example`` secrets."""
        return _state_for(params.secrets.for_scope_of_type(params.args.path, SECRET_TYPE), params.args.field)

    @classmethod
    def process(cls, params: ProcessParams[SecretPathFieldArgs], state: _ValueState, out: OutputCollector) -> None:
        """Emit the single row."""
        _emit_value_row(params, state, out)


# ---------------------------------------------------------------------------
# secret_types_seen
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class SecretTypesSeenArgs:
    """No arguments."""


@dataclass(kw_only=True)
class _SeenState(ArrowSerializableDataclass):
    """One ``(secret_name, secret_type, scope)`` row per delivered secret."""

    names: list[str] = field(default_factory=list)
    types: list[str | None] = field(default_factory=list)
    scopes: list[str | None] = field(default_factory=list)
    done: bool = False


@init_single_worker
@bind_fixed_schema
class SecretTypesSeenFunction(TableFunctionGenerator[SecretTypesSeenArgs, _SeenState]):
    """List every secret delivered to a function that requires two secret types."""

    class Meta:
        """Metadata for SecretTypesSeenFunction."""

        name = "secret_types_seen"
        description = "Conformance: the secrets delivered for a vgi_example + s3 requirement"
        categories = ["secret", "testing"]
        required_secrets = [
            SECRET_TYPE,
            SecretLookupEntry(secret_type="s3", scope=S3_SCOPE),
        ]
        examples = [FunctionExample(sql="SELECT * FROM secret_types_seen()", description="Delivered secrets")]

    FunctionArguments = SecretTypesSeenArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(secret_name=pa.string(), secret_type=pa.string(), scope=pa.string())

    @classmethod
    def initial_state(cls, params: ProcessParams[SecretTypesSeenArgs]) -> _SeenState:
        """Snapshot the delivered secrets, ordered by name."""
        names = sorted(params.secrets)
        return _SeenState(
            names=names,
            types=[params.secrets.secret_type(n) for n in names],
            scopes=[_render(params.secrets[n].get("scope")) for n in names],
        )

    @classmethod
    def process(cls, params: ProcessParams[SecretTypesSeenArgs], state: _SeenState, out: OutputCollector) -> None:
        """Emit all rows once, then finish."""
        if state.done or not state.names:
            out.finish()
            return
        out.emit(
            pa.RecordBatch.from_pydict(
                {"secret_name": state.names, "secret_type": state.types, "scope": state.scopes},
                schema=params.output_schema,
            )
        )
        state.done = True


SECRET_LOOKUP_FUNCTIONS: list[type[Function]] = [
    SecretValueByTypeFunction,
    SecretValueByNameFunction,
    SecretValueForScopeFunction,
    SecretValueRequiredFunction,
    SecretTypesSeenFunction,
]
