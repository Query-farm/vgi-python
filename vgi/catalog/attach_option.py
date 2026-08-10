# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""AttachOption descriptor for declarative worker attach-time options.

This module provides the AttachOption descriptor class for declaring options
that workers accept at ATTACH time (delivered once via catalog_attach, distinct
from session-level Settings resent on every call).

The declaration mirrors ``vgi.catalog.setting.Setting`` — same Arrow IPC spec
format, same Python type → Arrow mapping, same extractor shape — so both share
the machinery in ``vgi.catalog._descriptor_spec``.
"""

from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa

from vgi.catalog._descriptor_spec import _DescriptorBase, _extract_specs, _SpecBase

__all__ = [
    "AttachOption",
    "AttachOptionSpec",
    "MissingAttachOptionsError",
    "extract_attach_option_specs",
    "validate_required_attach_options",
]


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
        ARROW_SCHEMA: The shared four columns plus ``required``.

    """

    required: bool = False

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            *_SpecBase.ARROW_SCHEMA,
            # Nullable, and appended last: a peer that predates this column
            # reads the batch by name and simply doesn't see it.
            pa.field("required", pa.bool_(), nullable=True),
        ]
    )

    def __post_init__(self) -> None:
        """Reject the contradictory ``required`` + ``default`` combination."""
        if self.required and self.default is not None:
            raise ValueError(
                f"Attach option {self.name!r} is required but also declares a default "
                f"({self.default!r}); an option with a default is always satisfiable "
                "without the caller. Drop one."
            )

    def _extra_row(self) -> dict[str, Any]:
        return {"required": self.required}

    @classmethod
    def _extra_kwargs(cls, row: dict[str, object]) -> dict[str, Any]:
        # Absent column (older peer) and explicit null both mean "not required".
        return {"required": bool(row.get("required") or False)}


@dataclass
class AttachOption(_DescriptorBase):
    """Descriptor for declarative attach-option definitions using Annotated.

    Use with Annotated type hints to declare options in a Worker's
    AttachOptions inner class. The Arrow type is resolved from the base type
    in the Annotated hint. See
    `_DescriptorBase` (in `vgi.catalog._descriptor_spec`) for the
    ``desc`` and ``arrow_type`` attributes.

    Declare a required option by omitting the class-level assignment that would
    otherwise supply its default::

        class AttachOptions:
            region: Annotated[str, AttachOption(desc="AWS region")] = "us-east-1"
            api_key: Annotated[str, AttachOption(desc="API key", required=True)]

    Attributes:
        required: The caller must supply this option at ``ATTACH`` time.

    """

    required: bool = False

    def extra_spec_kwargs(self) -> dict[str, Any]:
        """Carry ``required`` through to the `AttachOptionSpec`."""
        return {"required": self.required}


class MissingAttachOptionsError(ValueError):
    """Raised when ``catalog_attach`` omits options declared ``required``.

    Carries the option names in :attr:`missing` so a caller can act on them
    without parsing the message.

    Attributes:
        missing: Names of the required options that were not supplied.

    """

    missing: list[str]

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
