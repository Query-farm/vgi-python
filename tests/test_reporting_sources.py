# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Admission constraints for portable sources and incremental progress decisions."""

from typing import Literal

import pytest

from vgi.reporting.sources import (
    ObjectRef,
    QuerySource,
    RevisionSelection,
    SourceProgressDecision,
    validate_progress_decision,
    validate_query_source,
)


@pytest.mark.parametrize("mode", ["head", "published"])
def test_incremental_sources_must_pin(mode: Literal["head", "published"]) -> None:
    """A floating query is valid for a stateless consumer, never an incremental task."""
    source = QuerySource(
        saved=ObjectRef(service_url="https://example.test/library", object_id="orders"),
        selection=RevisionSelection(mode=mode),
    )
    validate_query_source(source)
    with pytest.raises(ValueError, match="pinned"):
        validate_query_source(source, incremental=True)


def test_incremental_source_pin_roundtrip_and_progress_choice() -> None:
    """Pinned sources preserve identity; no source transition silently defaults."""
    source = QuerySource(
        saved=ObjectRef(service_url="https://example.test/library", object_id="orders"),
        selection=RevisionSelection(mode="pinned", revision_id="revision-1"),
    )
    validate_query_source(QuerySource.deserialize_from_bytes(source.serialize_to_bytes()), incremental=True)
    with pytest.raises(ValueError, match="requires"):
        validate_progress_decision(None, incremental_kind="stored")
    modes: tuple[Literal["retain", "reset", "rebuild"], ...] = ("retain", "reset", "rebuild")
    for mode in modes:
        validate_progress_decision(SourceProgressDecision(mode=mode), incremental_kind="stored")
    with pytest.raises(ValueError, match="Derived"):
        validate_progress_decision(SourceProgressDecision(mode="reset"), incremental_kind="derived")


def test_inline_context_cannot_use_an_undeclared_default() -> None:
    """A shared source cannot acquire the recipient's ambient default catalog."""
    from vgi.reporting.sources import ExecutionContext, InlineQuery, QueryDefinition

    source = QuerySource(
        inline=InlineQuery(
            definition=QueryDefinition(
                query="SELECT * FROM orders", context=ExecutionContext(default_source_alias="other_tenant")
            )
        )
    )
    with pytest.raises(ValueError, match="declared attachment"):
        validate_query_source(source)


@pytest.mark.parametrize(
    "selection", [RevisionSelection(mode="head", revision_id="old"), RevisionSelection(mode="pinned")]
)
def test_saved_selector_is_unambiguous(selection: RevisionSelection) -> None:
    """Do not silently choose between a floating pointer and supplied revision."""
    source = QuerySource(saved=ObjectRef(service_url="https://example.test", object_id="q"), selection=selection)
    with pytest.raises(ValueError):
        validate_query_source(source)
