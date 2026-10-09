# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Proposed vgi.report_render.v1 records and RPC interface.

These are importable protocol definitions, not a service implementation.
Behavior and worker policy are documented in docs/design/reporting-protocols/.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, ClassVar, Protocol

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

from vgi.reporting._arrow import Instant, non_null_list
from vgi.reporting._enums import RenderStatus
from vgi.reporting._metadata import sql_read
from vgi.reporting.common import (
    Artifact,
    DataSource,
    Delegation,
    ParamValue,
    RunError,
    ServiceInfo,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class RenderRequest(ArrowSerializableDataclass):
    """Inline report body, job-scoped catalog credentials and output preferences."""

    body_format: str
    body: bytes
    data_sources: Annotated[list[DataSource], non_null_list(pa.struct(DataSource.ARROW_SCHEMA))]
    delegations: Annotated[list[Delegation], non_null_list(pa.struct(Delegation.ARROW_SCHEMA))] = field(
        default_factory=list
    )
    parameter_values: Annotated[list[ParamValue], non_null_list(pa.struct(ParamValue.ARROW_SCHEMA))] = field(
        default_factory=list
    )
    outputs: Annotated[list[str], non_null_list(pa.string())] = field(default_factory=lambda: ["application/pdf"])
    locale: str = "en-US"
    time_zone: str = "UTC"
    timeout_seconds: int = 600


@dataclass(frozen=True, slots=True, kw_only=True)
class RenderJob(ArrowSerializableDataclass):
    """Versioned render progress and downloadable artifacts."""

    job_id: str
    version: int
    status: RenderStatus
    created_at: Instant
    started_at: Instant | None
    finished_at: Instant | None
    poll_after_seconds: int
    artifacts: Annotated[list[Artifact], non_null_list(pa.struct(Artifact.ARROW_SCHEMA))]
    error: RunError | None
    allowed_actions: Annotated[list[str], non_null_list(pa.string())]


@dataclass(frozen=True, slots=True, kw_only=True)
class RendererInfo(ServiceInfo):
    """Supported formats, outputs, limits and authorized source locations."""

    body_formats: Annotated[list[str], non_null_list(pa.string())]
    output_media_types: Annotated[list[str], non_null_list(pa.string())]
    allowed_source_locations: Annotated[list[str], non_null_list(pa.string())]


class ReportRenderProtocol(Protocol):
    """Proposed vgi.report_render.v1 records and RPC interface; implementations supply all methods."""

    protocol_name: ClassVar[str] = "vgi.report_render.v1"
    protocol_version: ClassVar[str] = "1.0.0"

    @sql_read(table="info")
    def get_renderer_info(self) -> RendererInfo:
        """Body formats, output media types, timeout ceiling, allowed source locations."""
        ...

    def start_render(self, request_id: str, request: RenderRequest) -> RenderJob:
        """Returns job_id, status queued."""
        ...

    @sql_read()
    def get_render(self, job_id: str) -> RenderJob:
        """Get render.

        Status (queued, running, succeeded, failed, cancelled), poll_after_seconds, artifacts, and on failure the
        cause's code, kind and data source.
        """
        ...

    def cancel_render(self, job_id: str, expected_version: int, request_id: str) -> RenderJob:
        """Best effort."""
        ...
