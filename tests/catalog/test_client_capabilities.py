# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Client capability offers reach catalog hooks and remain scoped to each attach."""

from __future__ import annotations

import json
import time
from dataclasses import asdict, replace
from typing import Any, cast

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient
from vgi_rpc.rpc import AuthContext, CallContext
from vgi_rpc.utils import serialize_record_batch_bytes

from vgi.attach_ticket import mint_attach_ticket
from vgi.catalog import (
    AttachOpaqueData,
    CatalogAttachResult,
    ReadOnlyCatalogInterface,
    SchemaInfo,
    TransactionOpaqueData,
)
from vgi.client import Client
from vgi.meta_worker import MetaWorker
from vgi.protocol import CatalogAttachRequest, ClientCapabilities, VgiProtocol
from vgi.serve import create_app
from vgi.worker import Worker

from .typing_helpers import present

_SIGNING_KEY = b"client-capabilities-test-key"
_NATIVE = ClientCapabilities(
    engine="duckdb",
    native_formats=["parquet"],
    catalogs=["ducklake"],
    can_stream=False,
    filter_encodings=["vgi.filters.v2"],
)
_LIMITED = ClientCapabilities(
    engine="test-engine", native_formats=[], catalogs=[], can_stream=False, filter_encodings=[]
)


class _CapabilityCatalog(ReadOnlyCatalogInterface):
    """Expose the offer as metadata, carrying it in the sealed attachment."""

    catalog_name = "capabilities"
    catalog_contents_attach_independent = False

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        client_capabilities: ClientCapabilities | None = None,
        ctx: CallContext | None = None,
    ) -> CatalogAttachResult:
        """Persist the offer without storing client state on the catalog instance."""
        result = super().catalog_attach(
            name=name,
            options=options,
            data_version_spec=data_version_spec,
            implementation_version=implementation_version,
            client_capabilities=client_capabilities,
            ctx=ctx,
        )
        payload = json.dumps(asdict(client_capabilities) if client_capabilities is not None else None).encode()
        return replace(result, attach_opaque_data=AttachOpaqueData(payload), attach_opaque_data_required=True)

    def schemas(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None
    ) -> list[SchemaInfo]:
        """Read the original offer on any worker serving a later catalog call."""
        return [
            SchemaInfo(
                attach_opaque_data=attach_opaque_data, path=["main"], comment=attach_opaque_data.decode(), tags={}
            )
        ]


class _CapabilityWorker(Worker):
    """Serve a capability-aware catalog through the normal worker dispatcher."""

    catalog_interface = _CapabilityCatalog


@pytest.mark.parametrize("capabilities", [None, _NATIVE, _LIMITED])
def test_python_client_forwards_capabilities_over_http(capabilities: ClientCapabilities | None) -> None:
    """The public Python client and Arrow RPC preserve absent and explicit offers."""
    app = create_app(_CapabilityWorker, describe=False, signing_key=_SIGNING_KEY)
    with Client.from_http("http://vgi.test", httpx_client=_SyncTestClient(app)) as client:
        attach = client.catalog_attach(
            name="capabilities",
            data_version_spec=None,
            implementation_version=None,
            client_capabilities=capabilities,
        )
        schemas = client.schemas(attach_opaque_data=attach.attach_opaque_data)
        assert json.loads(present(schemas[0].comment)) == (asdict(capabilities) if capabilities is not None else None)


def test_capabilities_survive_reconnect_without_cross_attachment_leakage() -> None:
    """Different offers survive another client attaching and a new worker instance."""
    app = create_app(_CapabilityWorker, describe=False, signing_key=_SIGNING_KEY)
    with Client.from_http("http://vgi.test", httpx_client=_SyncTestClient(app)) as client:
        attachments = [
            client.catalog_attach(
                name="capabilities",
                data_version_spec=None,
                implementation_version=None,
                client_capabilities=offer,
            )
            for offer in (_NATIVE, _LIMITED, None)
        ]
        for attach, offer in zip(attachments, (_NATIVE, _LIMITED, None), strict=True):
            snapshot = client.load_catalog(attach=attach)
            assert json.loads(present(snapshot.schemas[0].schema.comment)) == (
                asdict(offer) if offer is not None else None
            )

    restarted_app = create_app(_CapabilityWorker, describe=False, signing_key=_SIGNING_KEY)
    with Client.from_http("http://vgi.test", httpx_client=_SyncTestClient(restarted_app)) as client:
        for attach, offer in zip(attachments, (_NATIVE, _LIMITED, None), strict=True):
            snapshot = client.load_catalog(attach=attach)
            assert json.loads(present(snapshot.schemas[0].schema.comment)) == (
                asdict(offer) if offer is not None else None
            )


@pytest.mark.parametrize("use_meta_worker", [False, True])
@pytest.mark.parametrize("capabilities", [None, _NATIVE, _LIMITED])
def test_ticket_redemption_preserves_redeeming_client_capabilities(
    monkeypatch: pytest.MonkeyPatch, use_meta_worker: bool, capabilities: ClientCapabilities | None
) -> None:
    """A ticket restores catalog/options while retaining the redeeming engine's offer."""
    auth = AuthContext(domain="test", authenticated=True, principal="alice")
    monkeypatch.setattr("vgi.worker.current_auth", lambda: auth)
    child = _CapabilityWorker(quiet=True)
    child._signing_key = _SIGNING_KEY
    worker = MetaWorker([child]) if use_meta_worker else child
    now = int(time.time())
    ticket, _ = mint_attach_ticket(
        _SIGNING_KEY,
        principal="alice",
        catalog_name="capabilities",
        options_ipc=serialize_record_batch_bytes(pa.RecordBatch.from_pylist([{"probe": "ticket"}])),
        issued_at=now,
        expires_at=now + 600,
    )
    request = CatalogAttachRequest(
        name="ticket-alias",
        options=pa.RecordBatch.from_pylist([{"vgi_attach_ticket": ticket}]),
        data_version_spec=None,
        implementation_version=None,
        client_capabilities=capabilities,
    )
    attach = worker.catalog_attach(CatalogAttachRequest.deserialize_from_bytes(request.serialize_to_bytes()))
    schemas = cast(VgiProtocol, worker).catalog_schemas(attach_opaque_data=attach.attach_opaque_data).to_infos()
    assert json.loads(present(schemas[0].comment)) == (asdict(capabilities) if capabilities is not None else None)
