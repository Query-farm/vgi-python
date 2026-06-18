# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Setting descriptor for declarative worker settings.

This module provides the `Setting` descriptor class for defining worker settings
using Python's `Annotated` type hints, similar to how [`Arg`][] works for function arguments.

"""

from dataclasses import dataclass

from vgi.catalog._descriptor_spec import _DescriptorBase, _extract_specs, _SpecBase

__all__ = [
    "Setting",
    "SettingSpec",
    "extract_setting_specs",
]


@dataclass(frozen=True)
class SettingSpec(_SpecBase):
    """Extracted setting metadata for catalog serialization.

    This is the resolved form of a `Setting`, with all types inferred and
    ready for serialization. See [`_SpecBase`][vgi.catalog._descriptor_spec._SpecBase]
    for the field and wire-format definition.
    """


@dataclass
class Setting(_DescriptorBase):
    """Descriptor for declarative setting definitions using Annotated.

    Use with `Annotated` type hints to declare settings in a [`Worker`][]'s Settings class.
    The Arrow type is resolved from the base type in the `Annotated` hint. See
    [`_DescriptorBase`][vgi.catalog._descriptor_spec._DescriptorBase] for the
    ``desc`` and ``arrow_type`` attributes.
    """


def extract_setting_specs(settings_cls: type) -> list[SettingSpec]:
    """Extract [`SettingSpec`][] objects from a Settings class.

    Parses a Settings class with `Annotated[type, Setting(...)]` attributes and
    extracts a `SettingSpec` for each setting definition.

    Args:
        settings_cls: A class with `Annotated[type, Setting(...)]` attributes.

    Returns:
        List of `SettingSpec` objects extracted from the class.

    Raises:
        TypeError: If a setting's Arrow type cannot be resolved.

    """
    return _extract_specs(settings_cls, descriptor_type=Setting, spec_factory=SettingSpec)
