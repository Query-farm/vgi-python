# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Canonical Variant negotiation cannot select the retired custom encoding."""

import pytest

from vgi.catalog.capabilities import LOGICAL_TYPE_VARIANT, CatalogCapabilities
from vgi.exceptions import CatalogCapabilityError
from vgi.logical_types import VARIANT_PROFILE


def test_canonical_variant_profile_requires_matching_offer_and_provider() -> None:
    """Neither a missing offer nor the old unpublished encoding confers support."""
    assert LOGICAL_TYPE_VARIANT == VARIANT_PROFILE == "vgi.type.variant.parquet.v1"
    available = CatalogCapabilities(read=[LOGICAL_TYPE_VARIANT])
    retired = CatalogCapabilities(read=["vgi.type.variant.arrow.v1"])
    assert available.select(None) == CatalogCapabilities()
    assert available.select(retired) == CatalogCapabilities()
    assert retired.select(available) == CatalogCapabilities()
    assert available.select(available) == available


def test_canonical_variant_profile_withdrawal_rejects_existing_agreement() -> None:
    """An attachment cannot continue consuming variants after support disappears."""
    selected = CatalogCapabilities(read=[LOGICAL_TYPE_VARIANT])
    with pytest.raises(CatalogCapabilityError):
        selected.require(LOGICAL_TYPE_VARIANT, available=CatalogCapabilities(), operation="read VARIANT")
