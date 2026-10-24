# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Exercise restricted discovery, collection bounds and the revised wire shapes."""

from __future__ import annotations

import dataclasses
from collections.abc import Hashable
from dataclasses import dataclass
from typing import Any, cast

import pytest
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.http import http_connect
from vgi_rpc.http._testing import make_sync_client
from vgi_rpc.rpc import (
    CallContext,
    MethodNotImplementedError,
    MethodType,
    OutputCollector,
    ProducerState,
    RpcError,
    RpcServer,
    Stream,
    rpc_methods,
)

from vgi.reporting import PROTOCOLS
from vgi.reporting._metadata import read_annotation
from vgi.reporting.alerts import AlertsProtocol, InstanceSummary, RuleSummary, SnoozeRecord
from vgi.reporting.common import Destination, Limit, Notification, OwnerKey, OwnershipCapabilities, Run, RunStep
from vgi.reporting.delegations import DelegationsProtocol
from vgi.reporting.discovery import (
    DiscoveryCapabilities,
    OwnerDiscoveryCapabilities,
    batch_limit,
    validate_batch_size,
    validate_list_request,
    validate_search,
)
from vgi.reporting.notify import (
    ChannelInfo,
    DestinationCheck,
    DestinationRecord,
    NotificationReceipt,
    NotifyInfo,
    NotifyProtocol,
)
from vgi.reporting.objects import ObjectSummary
from vgi.reporting.ownership import ReportOwnershipProtocol
from vgi.reporting.render import ReportRenderProtocol
from vgi.reporting.resource_identity import (
    Identifier,
    IdentityCapabilities,
    ResourceContext,
    ResourceIdentityProtocol,
    validate_identity_request,
    validate_resource_context,
)
from vgi.reporting.resource_ownership import OwnershipProtocol
from vgi.reporting.schedules import SchedulesProtocol, ScheduleSummary
from vgi.reporting.schedules_v2 import SchedulesProtocolV2, ScheduleSummaryV2
from vgi.reporting.sql_tasks import SqlTasksProtocol, TaskSummary


def test_exact_only_identity_resolution_needs_no_search() -> None:
    """A restricted directory can validate supplied identities for creation and transfer."""
    exact = DiscoveryCapabilities(identifier_schemes=["email", "principal_id"])
    caps = IdentityCapabilities(
        ownership=OwnerDiscoveryCapabilities(
            identifier_schemes=["email", "principal_id"],
            policy=OwnershipCapabilities(
                owner_kinds=["principal", "group"],
                parent_owner_kinds=["group"],
                scopes=["all", "self"],
                membership_owner_kinds=[],
            ),
        ),
        execution=exact,
    )
    context = ResourceContext(protocol="vgi.objects.v1", resource_kind="object", operation="create")
    validate_identity_request(
        context,
        purpose="ownership",
        identifier=Identifier(scheme="email", value="owner@example.test"),
        capabilities=caps,
        supports_folders=True,
    )
    change = dataclasses.replace(context, operation="change_execution", resource_id="schedule-1")
    validate_identity_request(
        change,
        purpose="execution",
        identifier=Identifier(scheme="principal_id", value="service-1"),
        capabilities=caps,
        supports_folders=False,
    )
    for query in ("", "alice"):
        with pytest.raises(NotImplementedError):
            validate_search(query, 20, exact)


@pytest.mark.parametrize(
    "change",
    [
        {"resource_id": "existing"},
        {"operation": "transfer_ownership"},
        {"parent_folder_id": ""},
        {"operation": "change_execution", "resource_id": "r", "parent_folder_id": "folder"},
    ],
)
def test_ambiguous_identity_context_is_rejected(change: dict[str, Any]) -> None:
    """A malformed context cannot turn a creation lookup into a cross-resource lookup."""
    context = ResourceContext(protocol="vgi.objects.v1", resource_kind="object", operation="create")
    with pytest.raises(ValueError):
        validate_resource_context(dataclasses.replace(context, **change), supports_folders=True)


def test_identity_change_requires_explicit_matching_purpose() -> None:
    """Default-to-caller is creation-only, and ownership eligibility cannot authorize execution."""
    caps = IdentityCapabilities(
        ownership=OwnerDiscoveryCapabilities(
            identifier_schemes=["email"],
            policy=OwnershipCapabilities(
                owner_kinds=["principal", "group"],
                parent_owner_kinds=["group"],
                scopes=["all", "self"],
                membership_owner_kinds=[],
            ),
        ),
        execution=None,
    )
    context = ResourceContext(
        protocol="vgi.objects.v1", resource_kind="object", operation="transfer_ownership", resource_id="object-1"
    )
    with pytest.raises(ValueError, match="explicit"):
        validate_identity_request(
            context, purpose="ownership", identifier=None, capabilities=caps, supports_folders=True
        )
    with pytest.raises(NotImplementedError):
        validate_identity_request(
            context, purpose="execution", identifier=None, capabilities=caps, supports_folders=True
        )
    with pytest.raises(ValueError, match="advertised"):
        validate_identity_request(
            context,
            purpose="ownership",
            identifier=Identifier(scheme="label", value="Alice"),
            capabilities=caps,
            supports_folders=True,
        )


def test_search_and_suggestions_have_independent_capabilities() -> None:
    """Suggestions do not accidentally enable empty-query enumeration or arbitrary directory searches."""
    suggestions = DiscoveryCapabilities(suggestions_supported=True, max_results=3)
    validate_search("", 3, suggestions)
    with pytest.raises(NotImplementedError):
        validate_search("alice", 3, suggestions)
    search = DiscoveryCapabilities(search_supported=True, min_query_length=3, max_results=5)
    validate_search("alice", 5, search)
    with pytest.raises(NotImplementedError):
        validate_search("", 5, search)
    for query, limit in [("al", 5), ("alice", 6), ("alice", 0), ("alice", -1), ("alice", True)]:
        with pytest.raises(ValueError):
            validate_search(query, limit, search)


def test_collection_admission_never_defaults_to_unlimited() -> None:
    """Batch limits are explicit finite counts; invalid sizes fail before partial processing."""
    cap = Limit(name="max_check_destinations", value=4, unit="count")
    assert batch_limit([cap], cap.name) == 4
    validate_batch_size(4, maximum=4)
    for size in (-1, 5, True):
        with pytest.raises(ValueError):
            validate_batch_size(size, maximum=4)
    for limits in (
        [],
        [cap, cap],
        [dataclasses.replace(cap, value=None)],
        [dataclasses.replace(cap, value=True)],
        [dataclasses.replace(cap, value=cast(Any, float("inf")))],
        [dataclasses.replace(cap, value=cast(Any, 1.5))],
        [dataclasses.replace(cap, unit="bytes")],
    ):
        with pytest.raises(ValueError):
            batch_limit(limits, cap.name)


@pytest.mark.parametrize("maximum", [True, None, float("inf"), 1.5, -1])
def test_batch_ceiling_must_be_a_finite_nonnegative_integer(maximum: Any) -> None:
    """Misconfigured worker limits cannot turn a bounded request into an unbounded one."""
    with pytest.raises(ValueError):
        validate_batch_size(1, maximum=maximum)


def test_list_filters_reject_conflicting_owners_and_invalid_ordering() -> None:
    """Admin filters remain exact and explicit, with a total limit rather than a per-page cap."""
    options: dict[str, Any] = {"limit": None, "order_by": "id", "supported_orders": ("id", "title")}
    validate_list_request(**options)
    for change in (
        {"limit": 0},
        {"limit": -1},
        {"limit": True},
        {"order_by": "arbitrary_sql"},
        {"ownership_scope": "self", "owner": OwnerKey(kind="principal", id="other")},
    ):
        with pytest.raises(ValueError):
            validate_list_request(**(options | change))


@pytest.mark.parametrize(
    ("protocol", "name"),
    [
        (NotifyProtocol, "find_destinations"),
        (NotifyProtocol, "check_destinations"),
        (ReportRenderProtocol, "check_sources"),
        (ResourceIdentityProtocol, "find_owners"),
        (ResourceIdentityProtocol, "find_execution_principals"),
        (OwnershipProtocol, "find_owners"),
        (ReportOwnershipProtocol, "find_owners"),
        (AlertsProtocol, "list_snoozes"),
    ],
)
def test_collection_results_have_fixed_producer_rows(protocol: type, name: str) -> None:
    """Searches/checks expose real producer schemas rather than nested unary list wrappers."""
    method = rpc_methods(cast(Hashable, protocol))[name]
    assert method.method_type == MethodType.STREAM and not method.is_exchange
    annotation = read_annotation(getattr(protocol, name))
    assert annotation is not None and annotation.row_type is not None
    assert annotation.table is None
    if name.startswith("find_"):
        assert method.param_defaults["limit"] == 20


@pytest.mark.parametrize(
    "summary",
    [
        ObjectSummary,
        ScheduleSummary,
        ScheduleSummaryV2,
        TaskSummary,
        RuleSummary,
        InstanceSummary,
    ],
)
def test_browsing_never_embeds_definitions_or_history(summary: type[ArrowSerializableDataclass]) -> None:
    """Lightweight rows cannot regress into shipping SQL, recipients or large nested histories."""
    forbidden = {
        "definition",
        "body",
        "envelope",
        "steps",
        "attempts",
        "recovery_events",
        "latest_details",
        "fired_details",
    }
    assert not forbidden.intersection(summary.ARROW_SCHEMA.names)
    if "last_run" in summary.ARROW_SCHEMA.names:
        assert not {"schedule", "task", "steps", "recovery_events"}.intersection(
            field.name for field in summary.ARROW_SCHEMA.field("last_run").type
        )
    assert "attempts" not in RunStep.ARROW_SCHEMA.names
    assert not {"steps", "recovery_events", "destination_results"}.intersection(Run.ARROW_SCHEMA.names)


@pytest.mark.parametrize("protocol", [SchedulesProtocol, SchedulesProtocolV2, SqlTasksProtocol])
def test_admin_filters_and_history_streams_are_available(protocol: type) -> None:
    """Offboarding queries can filter on the executor; run details are independently streamed."""
    methods = rpc_methods(cast(Hashable, protocol))
    listing = "list_tasks" if protocol is SqlTasksProtocol else "list_schedules"
    assert {"owner", "parent_owner", "execution_principal_id", "query", "enabled", "limit"} <= set(
        methods[listing].param_types
    )
    assert {"before", "since", "limit"} <= set(methods["list_runs"].param_types)
    for name in ("list_run_steps", "list_step_attempts", "list_recovery_events", "list_run_destinations"):
        assert methods[name].method_type == MethodType.STREAM
    for name in ("start_test_run", "get_test_run", "cancel_test_run"):
        assert methods[name].method_type == MethodType.UNARY
    assert "request_id" in methods["start_test_run"].param_types
    assert "expected_version" in methods["cancel_test_run"].param_types


def test_snooze_reads_preserve_cancellation_preconditions() -> None:
    """A second client can obtain the ID/version and effective silence scope required to cancel."""
    methods = rpc_methods(AlertsProtocol)
    assert methods["get_snooze"].result_type is SnoozeRecord
    assert {"snooze_id", "version", "rule_id", "instance_id", "state", "until"} <= set(SnoozeRecord.ARROW_SCHEMA.names)
    assert methods["list_snoozes"].param_defaults["active_only"] is True
    assert {"snooze_id", "expected_version"} <= set(methods["unsnooze"].param_types)


def test_receipts_and_delegation_reads_are_compact_and_secret_free() -> None:
    """Notification progress has a current read; known credential keys need no inventory scan."""
    methods = rpc_methods(NotifyProtocol)
    assert methods["get_notification"].result_type is NotificationReceipt
    assert methods["list_notification_results"].method_type == MethodType.STREAM
    assert "results" not in NotificationReceipt.ARROW_SCHEMA.names
    assert {"retained_until", "last_provider_accepted_at", "unknown_count"} <= set(
        NotificationReceipt.ARROW_SCHEMA.names
    )
    delegation = rpc_methods(DelegationsProtocol)["get_delegation"]
    result_type = delegation.result_type
    assert isinstance(result_type, type) and issubclass(result_type, ArrowSerializableDataclass)
    assert not {"grant", "ticket"}.intersection(result_type.ARROW_SCHEMA.names)
    for protocol in PROTOCOLS:
        for name in rpc_methods(cast(Hashable, protocol)):
            if name.startswith(("start_", "cancel_", "validate_")):
                assert read_annotation(getattr(protocol, name)) is None


@dataclass
class _DestinationState(ProducerState):
    """Small HTTP fixture emitting a bounded search across response continuations."""

    limit: int
    index: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit one result at a time, retaining the total limit in the cursor."""
        if self.index >= self.limit:
            out.finish()
            return
        out.emit_pydict({"kind": ["email"], "address": [f"user-{self.index}@example.test"], "display_name": ["User"]})
        self.index += 1
        if self.index == self.limit:
            out.finish()


@dataclass
class _CheckState(ProducerState):
    """Checks complete in reverse input order, including an unavailable provider."""

    count: int
    index: int = 0
    fail_after: int | None = None

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Correlate every result explicitly instead of relying on arrival order."""
        if self.index >= self.count:
            out.finish()
            return
        if self.fail_after == self.index:
            raise RuntimeError("Directory failed after partial results")
        original = self.count - self.index - 1
        out.emit_pydict(
            {
                "kind": ["email"],
                "address": [f"user-{original}@example.test"],
                "display_name": [None],
                "input_index": [original],
                "state": ["unknown" if original == 1 else "allowed"],
                "reason_code": ["directory_unavailable" if original == 1 else None],
                "reason": [None],
            }
        )
        self.index += 1
        if self.index == self.count:
            out.finish()


class _NotifyWorker(NotifyProtocol):
    """Fixture implementing only the read operations exercised here."""

    def __init__(self, *, search_supported: bool = True, fail_checks: bool = False) -> None:
        """Configure restricted-directory scenarios."""
        self.discovery = DiscoveryCapabilities(search_supported=search_supported, max_results=5)
        self.fail_checks = fail_checks

    def find_destinations(self, kind: str, query: str, limit: int = 20) -> Stream[_DestinationState]:
        """Admit bounded search under an explicit policy."""
        try:
            validate_search(query, limit, self.discovery)
        except NotImplementedError as exc:
            raise MethodNotImplementedError(str(exc)) from exc
        return Stream(output_schema=DestinationRecord.ARROW_SCHEMA, state=_DestinationState(limit=limit))

    def check_destinations(self, destinations: list[Destination]) -> Stream[_CheckState]:
        """Validate supplied destinations independently of directory enumeration."""
        validate_batch_size(len(destinations), maximum=5)
        return Stream(
            output_schema=DestinationCheck.ARROW_SCHEMA,
            state=_CheckState(count=len(destinations), fail_after=1 if self.fail_checks else None),
        )

    def get_notify_info(self) -> NotifyInfo:
        """Publish discovery policy independently of exact checks."""
        return NotifyInfo(
            protocol_version="1.0.0",
            display_name="Test notifications",
            writable=True,
            limits=[Limit(name="max_check_destinations", value=5, unit="count")],
            channels=[
                ChannelInfo(
                    kind="email", enabled=True, images=False, threads=False, limits=[], discovery=self.discovery
                )
            ],
            destination_policy_summary="Exact checks do not require enumeration",
        )

    def list_destinations(self, kind: str, limit: int | None = None) -> Stream[_DestinationState]:
        """Directory enumeration is unavailable in this fixture."""
        raise NotImplementedError

    def send(self, request_id: str, destinations: list[Destination], notification: Notification) -> NotificationReceipt:
        """Unused fixture mutation."""
        raise NotImplementedError

    def get_notification(self, notification_id: str) -> NotificationReceipt:
        """Unused fixture method."""
        raise NotImplementedError

    def list_notification_results(self, notification_id: str, limit: int | None = None) -> Stream[_DestinationState]:
        """Unused fixture method."""
        raise NotImplementedError


def test_real_http_search_continuations_and_streamed_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    """The actual protocol crosses HTTP with bounded results and independent check rows."""
    client = make_sync_client(
        RpcServer(NotifyProtocol, _NotifyWorker(), grant_keys=None),
        token_key=b"reporting-test-key",
        preferred_response_bytes=65536,
        call_state_cache_entries=0,
    )
    posts: list[str] = []
    original_post = type(client).post

    def post(test_client: Any, *args: Any, **kwargs: Any) -> Any:
        posts.append(str(args[0] if args else kwargs.get("url", "")))
        return original_post(test_client, *args, **kwargs)

    monkeypatch.setattr(type(client), "post", post)
    with http_connect(NotifyProtocol, client=client) as proxy:  # type: ignore[type-abstract]
        rows = [
            row
            for batch in proxy.find_destinations(kind="email", query="user", limit=3)
            for row in batch.batch.to_pylist()
        ]
        assert len(rows) == 3
        assert [row["address"] for row in rows] == [f"user-{i}@example.test" for i in range(3)]
        assert any("exchange" in url for url in posts)
        inputs = [Destination(kind="email", address=f"user-{i}@example.test") for i in range(3)]
        checks = [row for batch in proxy.check_destinations(destinations=inputs) for row in batch.batch.to_pylist()]
        assert [row["input_index"] for row in checks] == [2, 1, 0]
        assert {row["input_index"]: row["state"] for row in checks} == {0: "allowed", 1: "unknown", 2: "allowed"}
        assert list(proxy.check_destinations(destinations=[])) == []


def test_http_exact_checks_work_when_search_is_unsupported() -> None:
    """An exact-only provider stays useful; unsupported search is an explicit RPC failure."""
    client = make_sync_client(
        RpcServer(NotifyProtocol, _NotifyWorker(search_supported=False), grant_keys=None),
        token_key=b"reporting-test-key",
    )
    with http_connect(NotifyProtocol, client=client) as proxy:  # type: ignore[type-abstract]
        assert proxy.get_notify_info().channels[0].discovery.search_supported is False
        with pytest.raises(RpcError) as error:
            list(proxy.find_destinations(kind="email", query="someone", limit=3))
        assert error.value.error_code == "UNIMPLEMENTED"
        checks = list(proxy.check_destinations(destinations=[Destination(kind="email", address="user-0@example.test")]))
        assert checks[0].batch.to_pylist()[0]["state"] == "allowed"


def test_http_partial_check_failure_is_not_successful_completion() -> None:
    """Receiving one allowed result never makes unfinished checks a complete successful batch."""
    client = make_sync_client(
        RpcServer(NotifyProtocol, _NotifyWorker(fail_checks=True), grant_keys=None), token_key=b"reporting-test-key"
    )
    with http_connect(NotifyProtocol, client=client) as proxy:  # type: ignore[type-abstract]
        inputs = [Destination(kind="email", address=f"user-{i}@example.test") for i in range(3)]
        stream = iter(proxy.check_destinations(destinations=inputs))
        assert next(stream).batch.to_pylist()[0]["input_index"] == 2
        with pytest.raises(RpcError):
            list(stream)
