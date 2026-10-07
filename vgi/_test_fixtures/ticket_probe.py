# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""``ticket_probe``: the cross-SDK fixture catalog for attach tickets.

Every SDK's fixture worker serves this catalog identically, and the
``attach_ticket/*.test`` sqllogictests run against each of them. The contract
(see ``docs/protocol/vgi-attach-tickets.md``, "Fixture catalog"):

- Catalog ``ticket_probe``, default schema ``main``.
- Attach options, in this order:

  ``region``
      ``VARCHAR``, not required, not secret, default ``'us-east-1'``.
  ``api_key``
      ``VARCHAR``, **required**, **secret**, no default.

- Table ``main.probe`` (backed by the table function ``main.ticket_probe``, no
  arguments): exactly one row, columns ``region VARCHAR`` (the attached value,
  or the default) and ``api_key_sha256 VARCHAR`` (the first 12 lowercase hex
  characters of ``SHA-256(UTF-8(api_key))``). The secret itself is never
  returned.

So a reattach with nothing but ``vgi_attach_ticket`` reading the same row
proves the secret option took effect without travelling again, and a reattach
without the ticket fails for want of the required ``api_key``.
"""

from __future__ import annotations

import dataclasses
import hashlib
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import CallContext, OutputCollector

from vgi.catalog import AttachOpaqueData, Catalog, CatalogAttachResult, ReadOnlyCatalogInterface, Schema, Table
from vgi.catalog.attach_option import AttachOption
from vgi.invocation import BindResponse
from vgi.table_function import BindParams, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi.worker import Worker

__all__ = ["CATALOG_NAME", "TicketProbeFunction", "TicketProbeWorker", "api_key_digest"]

CATALOG_NAME = "ticket_probe"
DEFAULT_REGION = "us-east-1"

_SCHEMA = pa.schema([pa.field("region", pa.string()), pa.field("api_key_sha256", pa.string())])
_SEP = b"\x00"


def api_key_digest(api_key: str) -> str:
    """Return the first 12 lowercase hex characters of ``SHA-256(UTF-8(api_key))``."""
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:12]


class TicketProbeAttachOptions:
    """One plain and one secret attach option."""

    region: Annotated[str, AttachOption(desc="Region the probe reports back")] = DEFAULT_REGION
    api_key: Annotated[str, AttachOption(desc="API key; only its digest is ever returned", required=True, secret=True)]


@dataclass(slots=True, frozen=True)
class _Args:
    """No arguments: the row comes from the attach."""


@dataclass(kw_only=True)
class _State(ArrowSerializableDataclass):
    emitted: bool = False


@init_single_worker
class TicketProbeFunction(TableFunctionGenerator[_Args, _State]):
    """One row: the attached ``region`` and a digest of the attached ``api_key``."""

    FunctionArguments = _Args

    class Meta:
        """Function metadata."""

        name = "ticket_probe"
        description = "Report the attach options of this ticket_probe attach (the api_key only as a digest)"
        categories = ["generator", "testing"]

    FIXED_SCHEMA: ClassVar[pa.Schema] = _SCHEMA

    @classmethod
    def on_bind(cls, params: BindParams[_Args]) -> BindResponse:
        """Fixed two-column schema."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[_Args]) -> _State:
        """Start unemitted."""
        return _State()

    @classmethod
    def process(cls, params: ProcessParams[_Args], state: _State, out: OutputCollector) -> None:
        """Emit the one row decoded from ``attach_opaque_data``."""
        if state.emitted:
            out.finish()
            return
        raw = params.attach_opaque_data
        if raw is None or _SEP not in bytes(raw):
            raise ValueError("ticket_probe must be read through an attach of the ticket_probe catalog")
        region, digest = bytes(raw).split(_SEP, 1)
        out.emit(pa.record_batch([[region.decode()], [digest.decode()]], schema=_SCHEMA))
        state.emitted = True


_CATALOG = Catalog(
    name=CATALOG_NAME,
    default_schema="main",
    comment="Attach-ticket probe: one plain and one secret attach option",
    schemas=[
        Schema(
            path=["main"],
            tables=[Table(name="probe", function=TicketProbeFunction, comment="The options this attach was made with")],
            functions=[TicketProbeFunction],
        ),
    ],
)


class TicketProbeCatalog(ReadOnlyCatalogInterface):
    """Validates the options, then carries ``region`` and the key digest in the attach."""

    catalog = _CATALOG
    catalog_name = CATALOG_NAME

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        ctx: CallContext | None = None,
    ) -> CatalogAttachResult:
        """Attach, recording ``region`` and ``sha256(api_key)[:12]`` (never the key)."""
        result = super().catalog_attach(
            name=name,
            options=options,
            data_version_spec=data_version_spec,
            implementation_version=implementation_version,
            ctx=ctx,
        )
        lowered = {key.lower(): value for key, value in options.items()}
        region = str(lowered.get("region") or DEFAULT_REGION)
        digest = api_key_digest(str(lowered["api_key"]))
        return dataclasses.replace(
            result, attach_opaque_data=AttachOpaqueData(region.encode() + _SEP + digest.encode())
        )


class TicketProbeWorker(Worker):
    """Serves the ``ticket_probe`` catalog."""

    AttachOptions = TicketProbeAttachOptions
    catalog_interface = TicketProbeCatalog
    catalog_name = CATALOG_NAME
    catalog = _CATALOG
    route_unattached_calls = False
