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

from vgi.catalog._descriptor_spec import _DescriptorBase, _extract_specs, _SpecBase

__all__ = [
    "AttachOption",
    "AttachOptionSpec",
    "extract_attach_option_specs",
]


@dataclass(frozen=True)
class AttachOptionSpec(_SpecBase):
    """Extracted attach-option metadata for catalog discovery serialization.

    The resolved form of an `AttachOption`. See
    `_SpecBase` (in `vgi.catalog._descriptor_spec`) for the field and
    wire-format definition.
    """


@dataclass
class AttachOption(_DescriptorBase):
    """Descriptor for declarative attach-option definitions using Annotated.

    Use with Annotated type hints to declare options in a Worker's
    AttachOptions inner class. The Arrow type is resolved from the base type
    in the Annotated hint. See
    `_DescriptorBase` (in `vgi.catalog._descriptor_spec`) for the
    ``desc`` and ``arrow_type`` attributes.
    """


def extract_attach_option_specs(options_cls: type) -> list[AttachOptionSpec]:
    """Extract AttachOptionSpec objects from an AttachOptions class."""
    return _extract_specs(options_cls, descriptor_type=AttachOption, spec_factory=AttachOptionSpec)
