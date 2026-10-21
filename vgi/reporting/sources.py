# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Portable saved-source identities, query definitions and revision selection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Literal

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.reporting._arrow import Json, non_null_list
from vgi.reporting.common import DataSource, ParameterSpec


@dataclass(frozen=True, slots=True, kw_only=True)
class ObjectRef(ArrowSerializableDataclass):
    """Object identity; service_url is the normalized complete service location."""

    service_url: str
    object_id: str


@dataclass(frozen=True, slots=True, kw_only=True)
class RevisionSelection(ArrowSerializableDataclass):
    """Exactly pinned carries a nonempty revision_id; floating modes carry None."""

    mode: Annotated[Literal["head", "published", "pinned"], ArrowType(pa.string())]
    revision_id: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class ResolvedSource(ArrowSerializableDataclass):
    """Frozen source provenance, without credentials or document content."""

    source: ObjectRef
    revision_id: str
    body_sha256: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionContext(ArrowSerializableDataclass):
    """Portable name resolution. Alias must identify a declared source attachment.

    None means the engine's isolated local database, never the recipient's
    workspace default. A schema without a source alias is invalid.
    """

    default_source_alias: str | None = None
    default_schema: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class QueryDefinition(ArrowSerializableDataclass):
    """The JSON body of cupola.query/1; bindings and parameters live in its envelope."""

    query: str
    setup_sql: str = ""
    dialect: str = "duckdb"
    context: ExecutionContext = field(default_factory=ExecutionContext)


@dataclass(frozen=True, slots=True, kw_only=True)
class InlineQuery(ArrowSerializableDataclass):
    """Inline equivalent of a saved query revision's body and public inputs."""

    definition: QueryDefinition
    data_sources: Annotated[list[DataSource], non_null_list(pa.struct(DataSource.ARROW_SCHEMA))] = field(
        default_factory=list
    )
    parameters: Annotated[list[ParameterSpec], non_null_list(pa.struct(ParameterSpec.ARROW_SCHEMA))] = field(
        default_factory=list
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class QuerySource(ArrowSerializableDataclass):
    """Exactly one of inline or saved; selection is required exactly for saved."""

    inline: InlineQuery | None = None
    saved: ObjectRef | None = None
    selection: RevisionSelection | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SourceProgressDecision(ArrowSerializableDataclass):
    """Explicit source-change decision; workers validate authority and target safety.

    reset_watermark_json is used only for reset of stored progress; None clears
    it. rebuild gates ordinary admission until confirmed full-refresh completion.
    """

    mode: Annotated[Literal["retain", "reset", "rebuild"], ArrowType(pa.string())]
    reset_watermark_json: Json | None = None


def validate_selection(selection: RevisionSelection) -> None:
    """Reject malformed selectors before resolution or dispatch."""
    if selection.mode not in {"head", "published", "pinned"}:
        raise ValueError("Unknown revision selection mode")
    if (selection.mode == "pinned" and not selection.revision_id) or (
        selection.mode != "pinned" and selection.revision_id is not None
    ):
        raise ValueError("Only pinned selection requires a nonempty revision ID")


def validate_query_source(source: QuerySource, *, incremental: bool = False) -> None:
    """Validate exclusive source payloads and pinned incremental-source admission."""
    if (source.inline is None) == (source.saved is None):
        raise ValueError("Choose exactly one inline or saved query")
    if source.saved is not None:
        if not source.saved.service_url or not source.saved.object_id or source.selection is None:
            raise ValueError("Saved queries require service, object and revision selection")
        validate_selection(source.selection)
        if incremental and source.selection.mode != "pinned":
            raise ValueError("Incremental tasks require pinned saved-query revisions")
    elif source.selection is not None:
        raise ValueError("Inline queries do not accept revision selection")
    if source.inline is not None:
        context = source.inline.definition.context
        aliases = [item.alias for item in source.inline.data_sources]
        if len(aliases) != len(set(aliases)):
            raise ValueError("Source attachment aliases must be distinct")
        if context.default_source_alias is not None and context.default_source_alias not in aliases:
            raise ValueError("Default source alias must name a declared attachment")
        if context.default_schema is not None and (not context.default_source_alias or not context.default_schema):
            raise ValueError("A nonempty default schema requires a source alias")


def validate_progress_decision(decision: SourceProgressDecision | None, *, incremental_kind: str) -> None:
    """Require a compatible explicit decision when an incremental source changes."""
    if decision is None or decision.mode not in {"retain", "reset", "rebuild"}:
        raise ValueError("Changing an incremental source requires retain, reset or rebuild")
    if decision.mode == "reset" and incremental_kind != "stored":
        raise ValueError("Derived progress must be reset through a full rebuild")
    if decision.mode != "reset" and decision.reset_watermark_json is not None:
        raise ValueError("Only reset accepts a watermark value")
