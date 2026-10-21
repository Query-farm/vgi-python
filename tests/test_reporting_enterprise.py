# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Enterprise ownership selection, canonical identity and advertised scope contracts."""

from __future__ import annotations

from collections.abc import Hashable
from dataclasses import replace
from typing import Any, cast

import pytest
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.errors import Code, error_code_of
from vgi_rpc.rpc import rpc_methods

from vgi.reporting import PROTOCOLS
from vgi.reporting.common import OwnerKey, OwnerRef, Ownership, OwnershipCapabilities, PrincipalRef
from vgi.reporting.discovery import (
    MembershipUnavailableError,
    OwnerDiscoveryCapabilities,
    matches_ownership_scope,
    validate_list_request,
    validate_owner_key,
    validate_ownership_assignment,
    validate_ownership_capabilities,
    validate_search,
)
from vgi.reporting.resource_identity import Identifier, IdentityCapabilities, ResourceContext, validate_identity_request


def _policy(*, membership: bool = True) -> OwnershipCapabilities:
    """A worker supports teams and organizations independently of directory discovery."""
    return OwnershipCapabilities(
        owner_kinds=["principal", "group", "organization", "workspace"],
        parent_owner_kinds=["group", "organization", "workspace"],
        scopes=["all", "self", "member", "self_or_member"] if membership else ["all", "self"],
        membership_owner_kinds=["group", "organization"] if membership else [],
    )


def _ownership(kind: str, identifier: str) -> Ownership:
    """Team and personal resources share a durable organizational management parent."""
    return Ownership(
        owner_ref=OwnerRef(kind=kind, id=identifier, display_name="Display only"),
        parent_owner_ref=OwnerRef(kind="organization", id="tenant-a/org", display_name="Organization"),
    )


def test_member_scope_uses_only_direct_owner_and_never_enumerates_groups() -> None:
    """Effective membership checks one canonical owner, without expanding parent ancestry."""
    checked: list[OwnerKey] = []

    def membership(key: OwnerKey) -> bool:
        checked.append(key)
        return key.id == "tenant-a/groups/finance"

    args: dict[str, Any] = dict(
        principal_id="tenant-a/users/alice", scope="member", capabilities=_policy(), membership_lookup=membership
    )
    assert matches_ownership_scope(_ownership("group", "tenant-a/groups/finance"), **args)
    assert not matches_ownership_scope(_ownership("group", "tenant-a/other-issuer/groups/finance"), **args)
    assert not matches_ownership_scope(_ownership("principal", "tenant-a/users/bob"), **args)
    assert not matches_ownership_scope(_ownership("workspace", "tenant-a/workspace"), **args)
    assert checked == [
        OwnerKey(kind="group", id="tenant-a/groups/finance"),
        OwnerKey(kind="group", id="tenant-a/other-issuer/groups/finance"),
    ]


def test_self_and_union_scopes_use_authenticated_user_or_service_identity() -> None:
    """Self matching needs no membership lookup and never treats group labels as caller IDs."""
    for kind in ("user", "service"):
        actor = PrincipalRef(id=f"tenant-a/{kind}/actor", kind=cast(Any, kind), display_name="Actor", email=None)
        own = _ownership("principal", actor.id)
        assert matches_ownership_scope(own, principal_id=actor.id, scope="self", capabilities=_policy())
        assert matches_ownership_scope(own, principal_id=actor.id, scope="self_or_member", capabilities=_policy())
        assert not matches_ownership_scope(own, principal_id=actor.id, scope="member", capabilities=_policy())
        assert not matches_ownership_scope(
            _ownership("group", actor.id), principal_id=actor.id, scope="self", capabilities=_policy()
        )


@pytest.mark.parametrize("answer", [None, "yes", 1])
def test_indeterminate_membership_cannot_become_allowed_or_empty_success(answer: Any) -> None:
    """Only authoritative booleans are membership answers; incomplete decisions remain unavailable."""
    with pytest.raises(MembershipUnavailableError):
        matches_ownership_scope(
            _ownership("group", "tenant-a/groups/finance"),
            principal_id="tenant-a/users/alice",
            scope="member",
            capabilities=_policy(),
            membership_lookup=lambda key: answer,
        )


def test_membership_revocation_and_provider_failure_are_not_cached_as_permission() -> None:
    """Each invocation rechecks effective membership; outage propagates separately from a negative decision."""
    current: bool | None = True

    def membership(key: OwnerKey) -> bool | None:
        return current

    own = _ownership("group", "tenant-a/groups/finance")
    args: dict[str, Any] = dict(
        principal_id="tenant-a/users/alice", scope="member", capabilities=_policy(), membership_lookup=membership
    )
    assert matches_ownership_scope(own, **args)
    current = False
    assert not matches_ownership_scope(own, **args)
    current = None
    with pytest.raises(MembershipUnavailableError) as error:
        matches_ownership_scope(own, **args)
    assert error_code_of(error.value) is Code.UNAVAILABLE

    def failed_provider(key: OwnerKey) -> bool:
        raise TimeoutError("Identity authority did not answer")

    with pytest.raises(TimeoutError):
        matches_ownership_scope(own, **(args | {"membership_lookup": failed_provider}))


def test_exact_group_resolution_and_filtering_work_without_membership_or_search() -> None:
    """A restricted provider can resolve supplied group IDs and filter their resources."""
    policy = _policy(membership=False)
    discovery = OwnerDiscoveryCapabilities(policy=policy, identifier_schemes=["group_id"])
    context = ResourceContext(protocol="vgi.objects.v1", resource_kind="object", operation="create")
    validate_identity_request(
        context,
        purpose="ownership",
        identifier=Identifier(scheme="group_id", value="finance"),
        capabilities=IdentityCapabilities(ownership=discovery, execution=None),
        supports_folders=True,
    )
    validate_list_request(
        limit=10,
        order_by="id",
        supported_orders=("id",),
        owner=OwnerKey(kind="group", id="tenant-a/finance"),
        ownership_capabilities=policy,
    )
    with pytest.raises(NotImplementedError):
        validate_search("finance", 10, discovery)
    for scope in ("member", "self_or_member"):
        with pytest.raises(NotImplementedError):
            validate_list_request(
                limit=10,
                order_by="id",
                supported_orders=("id",),
                ownership_scope=cast(Any, scope),
                ownership_capabilities=policy,
            )


def test_membership_filter_support_does_not_require_search_support() -> None:
    """Group resource views can work even when the directory forbids user-facing searches."""
    policy = _policy()
    discovery = OwnerDiscoveryCapabilities(policy=policy, identifier_schemes=["group_id"])
    validate_list_request(
        limit=10,
        order_by="id",
        supported_orders=("id",),
        ownership_scope="member",
        ownership_capabilities=policy,
        parent_owner=OwnerKey(kind="organization", id="tenant-a/org"),
    )
    with pytest.raises(NotImplementedError):
        validate_search("finance", 10, discovery)


@pytest.mark.parametrize("scope", ["self", "member", "self_or_member"])
def test_explicit_owner_conflicts_with_non_all_scope(scope: Any) -> None:
    """Clients must choose exact ownership or relative caller scope, without ambiguous precedence."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        validate_list_request(
            limit=None,
            order_by="id",
            supported_orders=("id",),
            ownership_scope=scope,
            owner=OwnerKey(kind="group", id="tenant-a/finance"),
            ownership_capabilities=_policy(),
        )


@pytest.mark.parametrize(
    "change",
    [
        {"scopes": ["all"]},
        {"scopes": ["all", "self", "member"]},
        {"scopes": ["all", "self"]},
        {"scopes": ["all", "self", "everyone"]},
        {"membership_owner_kinds": []},
        {"membership_owner_kinds": ["principal"]},
        {"membership_owner_kinds": ["vendor.absent"]},
        {"parent_owner_kinds": ["principal"]},
        {"parent_owner_kinds": []},
        {"owner_kinds": ["team"]},
        {"owner_kinds": ["group", "group"]},
    ],
)
def test_contradictory_capabilities_are_rejected(change: dict[str, Any]) -> None:
    """Workers cannot advertise incomplete membership scopes or ambiguous owner vocabulary."""
    with pytest.raises(ValueError):
        validate_ownership_capabilities(replace(_policy(), **change))


def test_canonical_owner_vocabulary_and_durable_parent() -> None:
    """Teams use group keys; personal resources need a supported durable management parent."""
    validate_owner_key(OwnerKey(kind="vendor.cost_center", id="tenant-a/center-1"))
    for key in (
        OwnerKey(kind="team", id="finance"),
        OwnerKey(kind="group", id=" "),
        OwnerKey(kind="vendor.", id="x"),
        OwnerKey(kind="vendor..name", id="x"),
    ):
        with pytest.raises(ValueError):
            validate_owner_key(key)
    own = _ownership("principal", "tenant-a/users/alice")
    validate_ownership_assignment(own, _policy())
    with pytest.raises(ValueError, match="durable parent"):
        validate_ownership_assignment(replace(own, parent_owner_ref=None), _policy())
    with pytest.raises(ValueError, match="distinct"):
        group = _ownership("group", "tenant-a/finance")
        validate_ownership_assignment(replace(group, parent_owner_ref=group.owner_ref), _policy())
    validate_ownership_assignment(
        Ownership(
            owner_ref=OwnerRef(kind="organization", id="tenant-a/org", display_name="Org"), parent_owner_ref=None
        ),
        _policy(),
    )


def test_every_resource_and_folder_collection_advertises_consistent_scopes() -> None:
    """Normal service info advertises team filtering even without the optional identity companion."""
    count = 0
    for protocol in PROTOCOLS:
        methods = rpc_methods(cast(Hashable, protocol))
        for name, method in methods.items():
            if name.startswith(("list_", "scan_")) and "owner" in method.param_types:
                count += 1
                assert method.param_defaults["ownership_scope"] == "all"
                assert "owned_by_me" not in method.param_types
                assert "parent_owner" in method.param_types
                info = next(m for n, m in methods.items() if n.startswith("get_") and n.endswith("_info"))
                result_type = info.result_type
                assert isinstance(result_type, type) and issubclass(result_type, ArrowSerializableDataclass)
                assert result_type.ARROW_SCHEMA.field("ownership").type.num_fields == 4
    assert count == 16
