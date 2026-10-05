# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for the AttachOption descriptor, its ``required`` and ``secret`` flags, and validation."""

from typing import Annotated

import pyarrow as pa
import pytest
from vgi_rpc.utils import deserialize_record_batch, serialize_record_batch_bytes

from vgi.catalog._descriptor_spec import _SpecBase
from vgi.catalog.attach_option import (
    AttachOption,
    AttachOptionSpec,
    MissingAttachOptionsError,
    extract_attach_option_specs,
    validate_required_attach_options,
)
from vgi.catalog.catalog_interface import ReadOnlyCatalogInterface
from vgi.catalog.setting import SettingSpec


class _Options:
    """Attach options mixing defaulted and required entries."""

    region: Annotated[str, AttachOption(desc="AWS region")] = "us-east-1"
    # No class-level assignment: nothing to fall back to, so the caller must supply it.
    api_key: Annotated[str, AttachOption(desc="API key", required=True, secret=True)]
    # A credential that may be omitted, with no default (the recommended shape).
    token: Annotated[str | None, AttachOption(desc="Optional token", arrow_type=pa.string(), secret=True)] = None
    # Allowed, though discouraged: a secret option with a default.
    password: Annotated[str, AttachOption(desc="Password", secret=True)] = "changeme"


def _roundtrip(spec: AttachOptionSpec) -> AttachOptionSpec:
    batch, _ = deserialize_record_batch(spec.serialize())
    return AttachOptionSpec.deserialize(batch)


class TestRequiredExtraction:
    """Extraction of the ``required`` flag from Annotated declarations."""

    def test_defaults_to_false(self) -> None:
        """An option that isn't declared required isn't required."""
        specs = {s.name: s for s in extract_attach_option_specs(_Options)}
        assert specs["region"].required is False
        assert specs["region"].default == "us-east-1"

    def test_required_option_extracted(self) -> None:
        """``required=True`` survives extraction, with no default."""
        specs = {s.name: s for s in extract_attach_option_specs(_Options)}
        assert specs["api_key"].required is True
        assert specs["api_key"].default is None

    def test_required_with_default_rejected(self) -> None:
        """A required option that also defaults is contradictory."""
        with pytest.raises(ValueError, match="required but also declares a default"):
            AttachOptionSpec(name="api_key", desc="", type=pa.string(), default="fallback", required=True)


class TestWireFormat:
    """Serialization of the added ``required`` column."""

    @pytest.mark.parametrize("required", [True, False], ids=["required", "optional"])
    def test_roundtrip_preserves_required(self, required: bool) -> None:
        """``required`` survives an Arrow IPC round trip in both states."""
        spec = AttachOptionSpec(name="api_key", desc="API key", type=pa.string(), default=None, required=required)
        assert _roundtrip(spec).required is required

    def test_roundtrip_preserves_shared_fields(self) -> None:
        """Widening the schema doesn't disturb the four shared columns."""
        spec = AttachOptionSpec(name="region", desc="AWS region", type=pa.string(), default="us-east-1")
        restored = _roundtrip(spec)
        assert (restored.name, restored.desc, restored.type, restored.default) == (
            "region",
            "AWS region",
            pa.string(),
            "us-east-1",
        )

    def test_setting_wire_format_unchanged(self) -> None:
        """``Setting`` keeps the original four columns; only AttachOption widened."""
        assert SettingSpec.ARROW_SCHEMA.names == ["name", "description", "type", "default_value"]
        assert AttachOptionSpec.ARROW_SCHEMA.names == [
            "name",
            "description",
            "type",
            "default_value",
            "required",
            "secret",
        ]

    def test_reads_batch_without_required_column(self) -> None:
        """A peer that predates the column yields ``required=False``, not an error."""
        type_bytes = pa.schema([pa.field("value", pa.string())]).serialize().to_pybytes()
        legacy = pa.RecordBatch.from_pylist(
            [{"name": "region", "description": "AWS region", "type": type_bytes, "default_value": None}],
            schema=_SpecBase.ARROW_SCHEMA,
        )
        restored = AttachOptionSpec.deserialize(legacy)
        assert restored.required is False
        assert restored.name == "region"

    def test_older_reader_ignores_required_column(self) -> None:
        """A four-column reader still reads a five-column batch (forward compatible)."""
        spec = AttachOptionSpec(name="api_key", desc="API key", type=pa.string(), default=None, required=True)
        batch, _ = deserialize_record_batch(spec.serialize())
        legacy_read = SettingSpec.deserialize(batch)
        assert legacy_read.name == "api_key"
        assert legacy_read.default is None

    def test_null_required_reads_as_false(self) -> None:
        """An explicit null in the nullable column means "not required"."""
        type_bytes = pa.schema([pa.field("value", pa.string())]).serialize().to_pybytes()
        batch = pa.RecordBatch.from_pylist(
            [
                {
                    "name": "region",
                    "description": "",
                    "type": type_bytes,
                    "default_value": None,
                    "required": None,
                }
            ],
            schema=AttachOptionSpec.ARROW_SCHEMA,
        )
        # Round-trip through IPC the way a real peer's bytes would arrive.
        restored, _ = deserialize_record_batch(serialize_record_batch_bytes(batch))
        assert AttachOptionSpec.deserialize(restored).required is False


class TestSecretExtraction:
    """Extraction of the ``secret`` flag from Annotated declarations."""

    def test_defaults_to_false(self) -> None:
        """An option that isn't declared secret isn't secret."""
        specs = {s.name: s for s in extract_attach_option_specs(_Options)}
        assert specs["region"].secret is False

    def test_secret_with_required(self) -> None:
        """``secret`` and ``required`` combine: a credential the caller must supply."""
        specs = {s.name: s for s in extract_attach_option_specs(_Options)}
        assert specs["api_key"].secret is True
        assert specs["api_key"].required is True
        assert specs["api_key"].default is None

    def test_secret_without_required(self) -> None:
        """A secret option need not be required."""
        specs = {s.name: s for s in extract_attach_option_specs(_Options)}
        assert specs["token"].secret is True
        assert specs["token"].required is False

    def test_secret_with_default_allowed(self) -> None:
        """A secret option may declare a default (discouraged, but not an error)."""
        specs = {s.name: s for s in extract_attach_option_specs(_Options)}
        assert specs["password"].secret is True
        assert specs["password"].default == "changeme"

    def test_secret_required_and_default_still_rejected(self) -> None:
        """``secret`` doesn't relax the required + default contradiction."""
        with pytest.raises(ValueError, match="required but also declares a default"):
            AttachOptionSpec(name="k", desc="", type=pa.string(), default="x", required=True, secret=True)


class TestSecretWireFormat:
    """Serialization of the added ``secret`` column."""

    @pytest.mark.parametrize("secret", [True, False], ids=["secret", "plain"])
    def test_roundtrip_preserves_secret(self, secret: bool) -> None:
        """``secret`` survives an Arrow IPC round trip in both states."""
        spec = AttachOptionSpec(name="api_key", desc="API key", type=pa.string(), default=None, secret=secret)
        restored = _roundtrip(spec)
        assert restored.secret is secret
        assert restored.required is False

    @pytest.mark.parametrize(
        ("required", "secret"),
        [(True, True), (True, False), (False, True), (False, False)],
    )
    def test_roundtrip_required_and_secret_independent(self, required: bool, secret: bool) -> None:
        """The two flags occupy separate columns and never bleed into each other."""
        spec = AttachOptionSpec(
            name="api_key", desc="API key", type=pa.string(), default=None, required=required, secret=secret
        )
        restored = _roundtrip(spec)
        assert (restored.required, restored.secret) == (required, secret)

    def test_secret_column_follows_required(self) -> None:
        """``secret`` is a nullable bool appended after ``required``."""
        schema = AttachOptionSpec.ARROW_SCHEMA
        assert schema.get_field_index("secret") == schema.get_field_index("required") + 1
        assert schema.get_field_index("secret") == len(schema) - 1
        field = schema.field("secret")
        assert field.type == pa.bool_()
        assert field.nullable

    def test_reads_batch_without_secret_column(self) -> None:
        """A peer that has ``required`` but predates ``secret`` yields ``secret=False``."""
        type_bytes = pa.schema([pa.field("value", pa.string())]).serialize().to_pybytes()
        older_schema = pa.schema([*_SpecBase.ARROW_SCHEMA, pa.field("required", pa.bool_(), nullable=True)])
        older = pa.RecordBatch.from_pylist(
            [{"name": "api_key", "description": "", "type": type_bytes, "default_value": None, "required": True}],
            schema=older_schema,
        )
        restored_batch, _ = deserialize_record_batch(serialize_record_batch_bytes(older))
        restored = AttachOptionSpec.deserialize(restored_batch)
        assert restored.secret is False
        assert restored.required is True

    def test_reads_four_column_batch_as_not_secret(self) -> None:
        """A peer that predates both columns reads as neither required nor secret."""
        type_bytes = pa.schema([pa.field("value", pa.string())]).serialize().to_pybytes()
        legacy = pa.RecordBatch.from_pylist(
            [{"name": "region", "description": "", "type": type_bytes, "default_value": None}],
            schema=_SpecBase.ARROW_SCHEMA,
        )
        restored = AttachOptionSpec.deserialize(legacy)
        assert (restored.required, restored.secret) == (False, False)

    def test_null_secret_reads_as_false(self) -> None:
        """An explicit null in the nullable column means "not secret"."""
        type_bytes = pa.schema([pa.field("value", pa.string())]).serialize().to_pybytes()
        batch = pa.RecordBatch.from_pylist(
            [
                {
                    "name": "region",
                    "description": "",
                    "type": type_bytes,
                    "default_value": None,
                    "required": None,
                    "secret": None,
                }
            ],
            schema=AttachOptionSpec.ARROW_SCHEMA,
        )
        restored, _ = deserialize_record_batch(serialize_record_batch_bytes(batch))
        assert AttachOptionSpec.deserialize(restored).secret is False

    def test_older_reader_ignores_secret_column(self) -> None:
        """A four-column reader still reads a six-column batch (forward compatible)."""
        spec = AttachOptionSpec(name="api_key", desc="", type=pa.string(), default=None, required=True, secret=True)
        batch, _ = deserialize_record_batch(spec.serialize())
        assert SettingSpec.deserialize(batch).name == "api_key"


class TestValidateRequiredAttachOptions:
    """The shared validation helper."""

    SPECS = [
        AttachOptionSpec(name="region", desc="", type=pa.string(), default="us-east-1"),
        AttachOptionSpec(name="api_key", desc="", type=pa.string(), default=None, required=True),
    ]

    def test_missing_required_raises(self) -> None:
        """Omitting a required option raises with the names attached."""
        with pytest.raises(MissingAttachOptionsError) as exc:
            validate_required_attach_options("demo", self.SPECS, {"region": "eu-west-1"})
        assert exc.value.missing == ["api_key"]
        assert "'api_key'" in str(exc.value)

    def test_supplied_required_passes(self) -> None:
        """Supplying the required option is enough; defaults cover the rest."""
        validate_required_attach_options("demo", self.SPECS, {"api_key": "secret"})

    def test_case_insensitive_match(self) -> None:
        """DuckDB lowercases ATTACH option keys; matching follows."""
        validate_required_attach_options("demo", self.SPECS, {"API_KEY": "secret"})

    def test_no_required_specs_always_passes(self) -> None:
        """A catalog with no required options accepts an empty mapping."""
        validate_required_attach_options("demo", [self.SPECS[0]], {})

    def test_multiple_missing_reported_together(self) -> None:
        """Every missing option is reported in one error, not one at a time."""
        specs = [
            AttachOptionSpec(name="api_key", desc="", type=pa.string(), default=None, required=True),
            AttachOptionSpec(name="tenant", desc="", type=pa.string(), default=None, required=True),
        ]
        with pytest.raises(MissingAttachOptionsError) as exc:
            validate_required_attach_options("demo", specs, {})
        assert exc.value.missing == ["api_key", "tenant"]


class _GatedCatalog(ReadOnlyCatalogInterface):
    """Read-only catalog that cannot be attached without ``api_key``."""

    catalog_name = "gated"
    attach_option_specs = [
        AttachOptionSpec(name="api_key", desc="API key", type=pa.string(), default=None, required=True),
    ]


class TestCatalogAttachEnforcement:
    """The default ``catalog_attach`` refuses an attach missing required options."""

    def test_attach_without_required_option_raises(self) -> None:
        """The attach fails loudly rather than yielding an empty-looking catalog."""
        with pytest.raises(MissingAttachOptionsError, match="api_key"):
            _GatedCatalog().catalog_attach(
                name="gated", options={}, data_version_spec=None, implementation_version=None
            )

    def test_attach_with_required_option_succeeds(self) -> None:
        """Supplying the option attaches normally."""
        result = _GatedCatalog().catalog_attach(
            name="gated",
            options={"api_key": "secret"},
            data_version_spec=None,
            implementation_version=None,
        )
        assert result.attach_opaque_data is not None
