# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""AttachOption descriptor for declarative worker attach-time options.

This module provides the AttachOption descriptor class for declaring options
that workers accept at ATTACH time (delivered once via catalog_attach, distinct
from session-level Settings resent on every call).

The declaration mirrors ``vgi.catalog.setting.Setting`` — same Arrow IPC spec
format, same Python type → Arrow mapping, same extractor shape — so both share
the machinery in ``vgi.catalog._descriptor_spec``.

Credential options (API keys, tokens, passwords) MUST be declared
``secret=True``. The caller passes them inline as attach options; the DuckDB
extension redacts them from ``duckdb_databases()``, keeps only a salted hash of
them in its result-cache key, and never logs them. To keep a credential out of
the SQL text, write the option as an expression::

    ATTACH 'mydb' (TYPE vgi, LOCATION 'https://worker.example.com', api_key getenv('MYDB_API_KEY'));
"""

from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
from vgi_rpc.errors import Code
from vgi_rpc.utils import deserialize_record_batch, serialize_record_batch_bytes

from vgi.catalog._descriptor_spec import _DescriptorBase, _extract_specs, _SpecBase

__all__ = [
    "AttachOption",
    "AttachOptionSpec",
    "RESERVED_ATTACH_OPTION",
    "MissingAttachOptionsError",
    "attach_options",
    "extract_attach_option_specs",
    "pack_attach_options",
    "unpack_attach_options",
    "validate_required_attach_options",
]


#: Reserved for attach tickets (see ``vgi.attach_ticket``); no catalog may
#: declare an attach option with this name, compared case-insensitively.
RESERVED_ATTACH_OPTION = "vgi_attach_ticket"


@dataclass(frozen=True)
class AttachOptionSpec(_SpecBase):
    """Extracted attach-option metadata for catalog discovery serialization.

    The resolved form of an `AttachOption`. See
    `_SpecBase` (in `vgi.catalog._descriptor_spec`) for the shared fields and
    wire format.

    Attributes:
        required: The caller must supply this option at ``ATTACH`` time. A
            catalog that cannot be attached without it advertises that fact at
            discovery, so a client can say so before attempting the attach
            rather than surfacing a failure that reads like an empty catalog.
            Mutually exclusive with ``default`` — an option that falls back to
            a value is by definition satisfiable without the caller.
        secret: The option carries a credential (an API key, token or
            password). It is still passed inline at ``ATTACH``, but clients
            mask it, and the DuckDB extension redacts it from
            ``duckdb_databases()``, keeps only a salted hash of it in its
            result-cache key, and never logs it. Keep it out of the SQL text
            with an expression, e.g. ``api_key getenv('MYDB_API_KEY')``.
            Credential options MUST set this.
            Compatible with ``required``. Allowed alongside a ``default``, but a
            secret option should normally have none: a default credential is
            advertised in plain text to every client at discovery.
        ARROW_SCHEMA: The shared four columns plus ``required`` and ``secret``.

    """

    required: bool = False
    secret: bool = False

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            *_SpecBase.ARROW_SCHEMA,
            # Nullable, and appended after the shared columns: a peer that predates this column
            # reads the batch by name and simply doesn't see it.
            pa.field("required", pa.bool_(), nullable=True),
            # Same rules as ``required``: nullable, appended after it, and an
            # absent column reads as False.
            pa.field("secret", pa.bool_(), nullable=True),
        ]
    )

    def __post_init__(self) -> None:
        """Reject the reserved name and the contradictory ``required`` + ``default`` combination."""
        if self.name.lower() == RESERVED_ATTACH_OPTION:
            raise ValueError(
                f"Attach option {self.name!r} uses the reserved name {RESERVED_ATTACH_OPTION!r}: the framework "
                "reads it as an attach ticket before any catalog code runs. Rename the option."
            )
        if self.required and self.default is not None:
            raise ValueError(
                f"Attach option {self.name!r} is required but also declares a default "
                f"({self.default!r}); an option with a default is always satisfiable "
                "without the caller. Drop one."
            )

    def _extra_row(self) -> dict[str, Any]:
        return {"required": self.required, "secret": self.secret}

    @classmethod
    def _extra_kwargs(cls, row: dict[str, object]) -> dict[str, Any]:
        # Absent column (older peer) and explicit null both mean "not required"
        # / "not secret".
        return {
            "required": bool(row.get("required") or False),
            "secret": bool(row.get("secret") or False),
        }


@dataclass
class AttachOption(_DescriptorBase):
    """Descriptor for declarative attach-option definitions using Annotated.

    Use with Annotated type hints to declare options in a Worker's
    AttachOptions inner class. The Arrow type is resolved from the base type
    in the Annotated hint. See
    `_DescriptorBase` (in `vgi.catalog._descriptor_spec`) for the
    ``desc`` and ``arrow_type`` attributes.

    The class-level assignment supplies the option's default. Omitting it makes
    the option *optional with no default* -- the caller may leave it out, and
    ``catalog_attach`` then simply doesn't find it in ``options``. Only
    ``required=True`` makes the caller supply it (``ATTACH`` without it fails
    with [`MissingAttachOptionsError`][vgi.catalog.attach_option.MissingAttachOptionsError]).
    A ``| None`` on the type is accepted and documents that the value may be
    absent; it does not change the option's type::

        class AttachOptions:
            region: Annotated[str, AttachOption(desc="AWS region")] = "us-east-1"
            endpoint: Annotated[str | None, AttachOption(desc="Override endpoint")] = None
            api_key: Annotated[str, AttachOption(desc="API key", required=True, secret=True)]

    Credential options (API keys, tokens, passwords) MUST be declared
    ``secret=True``; see `AttachOptionSpec.secret`.

    Attributes:
        required: The caller must supply this option at ``ATTACH`` time.
        secret: The option carries a credential. Clients and the DuckDB
            extension mask it, keep only a salted hash of it in cache keys, and
            never log it. Should normally have no default.

    """

    required: bool = False
    secret: bool = False

    def extra_spec_kwargs(self) -> dict[str, Any]:
        """Carry ``required`` and ``secret`` through to the `AttachOptionSpec`."""
        return {"required": self.required, "secret": self.secret}


class MissingAttachOptionsError(ValueError):
    """Raised when ``catalog_attach`` omits options declared ``required``.

    Carries the option names in :attr:`missing` so a caller can act on them
    without parsing the message.

    Attributes:
        missing: Names of the required options that were not supplied.
        error_code: ``INVALID_ARGUMENT``.

    """

    missing: list[str]
    error_code: ClassVar[Code] = Code.INVALID_ARGUMENT

    def __init__(self, catalog_name: str, missing: list[str]) -> None:
        """Build the error from the catalog name and the missing option names."""
        self.missing = missing
        joined = ", ".join(repr(name) for name in missing)
        super().__init__(
            f"Catalog {catalog_name!r} cannot be attached without the required "
            f"option{'s' if len(missing) > 1 else ''} {joined}."
        )


def validate_required_attach_options(
    catalog_name: str,
    specs: list[AttachOptionSpec],
    options: dict[str, Any],
) -> None:
    """Raise if any ``required`` spec has no corresponding entry in ``options``.

    Option names are matched case-insensitively, mirroring DuckDB's handling of
    ``ATTACH`` option keys.

    Args:
        catalog_name: Catalog being attached, for the error message.
        specs: The attach-option specs the catalog declares.
        options: Options the caller supplied at ``ATTACH`` time.

    Raises:
        MissingAttachOptionsError: If a required option was not supplied.

    """
    supplied = {key.lower() for key in options}
    missing = [spec.name for spec in specs if spec.required and spec.name.lower() not in supplied]
    if missing:
        raise MissingAttachOptionsError(catalog_name, missing)


def extract_attach_option_specs(options_cls: type) -> list[AttachOptionSpec]:
    """Extract AttachOptionSpec objects from an AttachOptions class."""
    return _extract_specs(options_cls, descriptor_type=AttachOption, spec_factory=AttachOptionSpec)


# Prefix of attach_opaque_data built by pack_attach_options; versioned so the
# encoding can change without misreading older bytes. Layout after the magic:
# 16 reserved bytes (zero), then the Arrow IPC bytes of a one-row batch of the
# non-secret options.
_PACKED_OPTIONS_MAGIC = b"vgi.attach-options.v1\x00"
_RESERVED_LEN = 16


def _one_row(values: dict[str, Any], types: dict[str, pa.DataType]) -> pa.RecordBatch:
    fields = [
        pa.field(name, types[name] if name in types else (pa.scalar(v).type if v is not None else pa.null()))
        for name, v in values.items()
    ]
    return pa.RecordBatch.from_pylist([values] if fields else [], schema=pa.schema(fields))


def attach_options(attach_opaque_data: bytes | None) -> dict[str, Any]:
    """Return the ATTACH options of an attach, secret options included.

    The standard way for functions and catalog hooks to read the options an
    ``ATTACH`` supplied. The worker carries them inside ``attach_opaque_data``
    itself (so they reach every worker process, on every transport), with
    options declared ``secret=True`` encrypted under a key the client never
    has (see ``docs/protocol/vgi-opaque-data-sealing.md``, rule 5). Pass the
    ``attach_opaque_data`` a function (``params.attach_opaque_data``; or just
    use ``params.attach_options``) or a ``catalog_transaction_*`` hook
    received. Catalog authors never put secrets into their own attach bytes.

    Args:
        attach_opaque_data: The attach bytes a function or hook received.

    Returns:
        Lower-cased option names to values; an option the caller left out is
        absent (declared defaults are not filled in). ``{}`` when the bytes
        carry no options (e.g. a value that has been copied or serialized:
        the options never travel with the catalog's bytes).

    Raises:
        OpaqueDataNotRecognizedError: The encrypted part does not open (it was
            moved from another attach, tampered with, or the key is gone).

    """
    from vgi.attach_secret_options import options_of

    return options_of(attach_opaque_data)


def pack_attach_options(options: dict[str, Any], specs: list[AttachOptionSpec] | None = None) -> bytes:
    """Encode the non-secret ATTACH options as catalog ``attach_opaque_data`` bytes.

    Rarely needed since the framework carries every ATTACH option itself (read
    them with [`attach_options`][vgi.catalog.attach_option.attach_options]).
    Use it when a catalog wants its own attach bytes to depend on the options.
    Options whose spec is declared ``secret=True`` are left out: secrets reach
    functions through ``attach_options`` (encrypted by the framework), never
    through the catalog's own bytes.

    Args:
        options: The options ``catalog_attach`` received.
        specs: The catalog's attach-option specs. Options are typed by their
            spec, declared defaults fill in options the caller left out, and
            ``secret`` options are dropped.

    Returns:
        The bytes to use as ``attach_opaque_data``.

    """
    values = {str(key).lower(): value for key, value in options.items()}
    by_name = {spec.name.lower(): spec for spec in specs or []}
    plain: dict[str, Any] = {}
    for name, spec in by_name.items():
        if spec.secret or (name not in values and spec.default is None):
            continue
        plain[name] = values.get(name, spec.default)
    for name, value in values.items():
        if name not in by_name:
            plain[name] = value
    types = {name: spec.type for name, spec in by_name.items()}
    return _PACKED_OPTIONS_MAGIC + bytes(_RESERVED_LEN) + serialize_record_batch_bytes(_one_row(plain, types))


def unpack_attach_options(attach_opaque_data: bytes | None) -> dict[str, Any]:
    """Return the attach's options: those the framework carries, plus any packed by ``pack_attach_options``.

    Equivalent to [`attach_options`][vgi.catalog.attach_option.attach_options]
    for catalogs that do not pack their own bytes.

    Args:
        attach_opaque_data: The attach bytes a function or hook received.

    Returns:
        Lower-cased option names to values.

    """
    values = attach_options(attach_opaque_data)
    if attach_opaque_data is None:
        return values
    raw = bytes(attach_opaque_data)
    head = len(_PACKED_OPTIONS_MAGIC) + _RESERVED_LEN
    if raw.startswith(_PACKED_OPTIONS_MAGIC) and len(raw) > head:
        batch, _ = deserialize_record_batch(raw[head:])
        if batch.num_rows:
            for key, value in batch.slice(0, 1).to_pylist()[0].items():
                values.setdefault(key, value)
    return values
