# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Settings/secrets fixtures: settings_aware, struct_settings, secret_demo, scoped_secret_demo."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import (
    _cardinality_from_count,
)
from vgi.arguments import Arg, Secret, Setting
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.schema_utils import schema
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)


@dataclass(slots=True, frozen=True)
class SettingsAwareFunctionArguments:
    """Arguments for SettingsAwareFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]


@dataclass(kw_only=True)
class SettingsAwareState(ArrowSerializableDataclass):
    """Mutable state for SettingsAwareFunction with typed settings."""

    remaining: int
    current_index: int = 0
    verbose: bool = False
    greeting: str = "Hello"
    multiplier: int = 1


@init_single_worker
@_cardinality_from_count
class SettingsAwareFunction(TableFunctionGenerator[SettingsAwareFunctionArguments, SettingsAwareState]):
    """Generates data demonstrating that settings are passed to functions.

    USE CASE
    --------
    Demonstrates how functions can declare required settings via
    Setting() annotations and access them via state (resolved once
    in initial_state()). The output includes columns showing the actual
    setting values that were passed.

    This function uses three settings:
    - vgi_verbose_mode: bool - when true, adds a details column
    - greeting: str - a custom greeting message echoed in output
    - multiplier: int - multiplies the value column

    Settings are typed: the C++ extension sends Arrow scalars with proper
    types (bool, int64, string). For backward compatibility, string values
    like "true" are also accepted for vgi_verbose_mode.

    SCHEMA
    ------
    Base output: {"id": int64, "greeting": string, "value": float64}
    With vgi_verbose_mode=true: adds "details": string column

    Example:
    -------
    With settings={vgi_verbose_mode: true, greeting: "Hi", multiplier: 2}:
    Returns: [{"id": 0, "greeting": "Hi", "value": 0.0, "details": "row_0"}, ...]

    Attributes:
        BATCH_SIZE: Number of rows emitted per output batch.

    """

    class Meta:
        """Metadata for SettingsAwareFunction."""

        name = "settings_aware"
        description = "Generates data demonstrating settings are passed"
        categories = ["generator", "settings"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM settings_aware(5)",
                description="Generate 5 rows showing setting values",
            )
        ]

    BATCH_SIZE: ClassVar[int] = 1000

    @staticmethod
    def _is_verbose(val: object) -> bool:
        """Check if verbose mode is enabled, handling both bool and string values."""
        return val is True or val == "true"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[SettingsAwareFunctionArguments],
        *,
        vgi_verbose_mode: Annotated[pa.Scalar[Any] | None, Setting()] = None,
        greeting: Annotated[pa.Scalar[Any] | None, Setting()] = None,
        multiplier: Annotated[pa.Scalar[Any] | None, Setting()] = None,
    ) -> BindResponse:
        """Return output schema based on vgi_verbose_mode setting.

        Always includes id, greeting (from setting), and value (multiplied).
        When vgi_verbose_mode is true, includes an extra "details" column.
        """
        fields: list[pa.Field[pa.DataType]] = [
            pa.field("id", pa.int64()),
            pa.field("greeting", pa.string()),
            pa.field("value", pa.float64()),
        ]

        # Add details column if verbose mode is enabled (handles bool and string)
        if vgi_verbose_mode is not None and cls._is_verbose(vgi_verbose_mode.as_py()):
            fields.append(pa.field("details", pa.string()))

        return BindResponse(output_schema=pa.schema(fields))

    @classmethod
    def initial_state(cls, params: ProcessParams[SettingsAwareFunctionArguments]) -> SettingsAwareState:
        """Create initial state with typed settings resolved once."""
        verbose_val = params.settings.get("vgi_verbose_mode", pa.scalar(False)).as_py()
        greeting_val = params.settings.get("greeting", pa.scalar("Hello")).as_py()
        multiplier_val = params.settings.get("multiplier", pa.scalar(1)).as_py()

        return SettingsAwareState(
            remaining=params.args.count,
            verbose=cls._is_verbose(verbose_val),
            greeting=str(greeting_val),
            multiplier=int(multiplier_val),
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[SettingsAwareFunctionArguments],
        state: SettingsAwareState,
        out: OutputCollector,
    ) -> None:
        """Generate data based on settings stored in state."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, cls.BATCH_SIZE)
        ids = list(range(state.current_index, state.current_index + size))

        data: dict[str, list[int] | list[float] | list[str]] = {
            "id": ids,
            "greeting": [state.greeting] * size,
            "value": [float(i) * 2.5 * state.multiplier for i in ids],
        }

        if state.verbose:
            data["details"] = [f"row_{i}" for i in ids]

        out.emit(pa.RecordBatch.from_pydict(data, schema=params.output_schema))

        state.current_index += size
        state.remaining -= size


@dataclass(slots=True, frozen=True)
class StructSettingsFunctionArguments:
    """Arguments for StructSettingsFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]


@dataclass(kw_only=True)
class StructSettingsState(ArrowSerializableDataclass):
    """Mutable state for StructSettingsFunction."""

    remaining: int
    current_index: int = 0
    start: int = 0
    step: int = 1
    label: str = "item"


@init_single_worker
@_cardinality_from_count
class StructSettingsFunction(TableFunctionGenerator[StructSettingsFunctionArguments, StructSettingsState]):
    """Generates a sequence configured by a struct setting.

    USE CASE
    --------
    Demonstrates how a single struct setting can configure multiple aspects
    of a function's behavior. The config setting is a struct with fields:
    - start: int64 - starting value for the sequence
    - step: int64 - step between values
    - label: string - prefix for label column

    SCHEMA
    ------
    Output: {"n": int64, "label": string}

    Example:
    -------
    With config={'start': 10, 'step': 5, 'label': 'item'} and count=3:
    Returns: [{"n": 10, "label": "item_0"}, {"n": 15, "label": "item_1"}, {"n": 20, "label": "item_2"}]

    Attributes:
        FIXED_SCHEMA: The fixed Arrow output schema this function always produces.

    """

    class Meta:
        """Metadata for StructSettingsFunction."""

        name = "struct_settings"
        description = "Generate a sequence configured by a struct setting"
        categories = ["generator", "settings"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM struct_settings(5)",
                description="Generate 5 rows configured by the config setting",
            )
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema({"n": pa.int64(), "label": pa.string()})

    @classmethod
    def on_bind(
        cls,
        params: BindParams[StructSettingsFunctionArguments],
        *,
        config: Annotated[pa.Scalar[Any] | None, Setting()] = None,
    ) -> BindResponse:
        """Return output schema. Config declared here for required_settings registration."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[StructSettingsFunctionArguments]) -> StructSettingsState:
        """Create initial state with struct setting values resolved once."""
        config = params.settings["config"]  # pa.StructScalar
        cfg = config.as_py()  # dict
        return StructSettingsState(
            remaining=params.args.count,
            start=cfg["start"],
            step=cfg["step"],
            label=cfg["label"],
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[StructSettingsFunctionArguments],
        state: StructSettingsState,
        out: OutputCollector,
    ) -> None:
        """Generate rows with values derived from the struct setting."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, 1000)
        data: dict[str, list[int] | list[str]] = {
            "n": [state.start + (state.current_index + i) * state.step for i in range(size)],
            "label": [f"{state.label}_{state.current_index + i}" for i in range(size)],
        }
        out.emit(pa.RecordBatch.from_pydict(data, schema=params.output_schema))
        state.current_index += size
        state.remaining -= size


# =============================================================================


@dataclass(kw_only=True)
class SecretDemoState(ArrowSerializableDataclass):
    """State for SecretDemoFunction."""

    keys: list[str] = field(default_factory=list)
    values: list[str] = field(default_factory=list)
    types: list[str] = field(default_factory=list)


@init_single_worker
class SecretDemoFunction(TableFunctionGenerator[None, SecretDemoState]):
    """Table function that outputs secret key-value pairs as rows.

    Demonstrates basic secret access via Secret() annotation.
    """

    class Meta:
        """Metadata for SecretDemoFunction."""

        name = "secret_demo"
        description = "Outputs secret contents as key-value rows"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[None],
    ) -> BindResponse:
        """Bind with secret request via SecretsAccessor."""
        # Request the secret via the accessor — triggers two-phase bind
        # so the resolved secret is available in initial_state().
        params.secrets.get("vgi_example")
        return BindResponse(
            output_schema=schema(
                {
                    "key": pa.string(),
                    "value": pa.string(),
                    "arrow_type": pa.string(),
                }
            )
        )

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> SecretDemoState:
        """Build initial state from secret key-value pairs."""
        secret = params.secrets.get("vgi_example", {})
        keys = list(secret.keys())
        values = [str(v.as_py()) for v in secret.values()]
        types = [str(v.type) for v in secret.values()]
        return SecretDemoState(keys=keys, values=values, types=types)

    @classmethod
    def process(
        cls,
        params: ProcessParams[None],
        state: SecretDemoState,
        out: OutputCollector,
    ) -> None:
        """Emit secret entries as rows."""
        if not state.keys:
            out.finish()
            return
        batch = pa.RecordBatch.from_pydict(
            {"key": state.keys, "value": state.values, "arrow_type": state.types},
            schema=params.output_schema,
        )
        out.emit(batch)
        state.keys = []
        state.values = []
        state.types = []


@dataclass(frozen=True)
class ScopedSecretDemoArgs:
    """Arguments for ScopedSecretDemoFunction."""

    path: Annotated[str, Arg(0, doc="Scope path for secret lookup")]


@dataclass(kw_only=True)
class ScopedSecretDemoState(ArrowSerializableDataclass):
    """State for ScopedSecretDemoFunction."""

    found: bool = False
    secret_keys: str = ""


@init_single_worker
class ScopedSecretDemoFunction(TableFunctionGenerator[ScopedSecretDemoArgs, ScopedSecretDemoState]):
    """Demonstrates automatic two-phase bind with scoped secrets.

    Requests a secret with a dynamic scope computed from the function argument.
    The framework automatically handles the two-phase bind retry.
    """

    class Meta:
        """Metadata for ScopedSecretDemoFunction."""

        name = "scoped_secret_demo"
        description = "Demo: resolves scoped secret based on argument"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[ScopedSecretDemoArgs],
        *,
        vgi_example: Annotated[dict[str, pa.Scalar[Any]] | None, Secret("vgi_example")] = None,
    ) -> BindResponse:
        """Bind with dynamic scoped secret lookup."""
        # Request secret with dynamic scope — framework handles retry automatically.
        # The get() call registers a pending scoped lookup; the return value is
        # unused because the framework will trigger a two-phase bind retry.
        params.secrets.get("vgi_example", scope=params.args.path)

        # On first call: secret is None (pending), framework triggers retry
        # On retry: secret is dict (found) or None (genuinely not found)

        return BindResponse(
            output_schema=schema(
                {
                    "scope": pa.string(),
                    "found": pa.bool_(),
                    "secret_keys": pa.string(),
                }
            )
        )

    @classmethod
    def initial_state(cls, params: ProcessParams[ScopedSecretDemoArgs]) -> ScopedSecretDemoState:
        """Build state from resolved secrets."""
        secret = params.secrets.get("vgi_example", {})
        return ScopedSecretDemoState(
            found=bool(secret),
            secret_keys=",".join(secret.keys()) if secret else "",
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[ScopedSecretDemoArgs],
        state: ScopedSecretDemoState,
        out: OutputCollector,
    ) -> None:
        """Emit scope info and resolved secret keys."""
        batch = pa.RecordBatch.from_pydict(
            {
                "scope": [params.args.path],
                "found": [state.found],
                "secret_keys": [state.secret_keys],
            },
            schema=params.output_schema,
        )
        out.emit(batch)
        out.finish()
