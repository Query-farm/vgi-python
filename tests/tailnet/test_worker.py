"""Fail-closed tests for the live Tailnet gate's authorization assertions."""

from __future__ import annotations

import pytest
from vgi_rpc import (
    IdentityAssurance,
    PeerEvidenceSet,
    PeerIdentity,
    PeerIdentityResult,
    SubjectKind,
    SubjectStability,
)
from vgi_rpc.rpc import AuthContext

from tests.tailnet.worker import _localapi_policy, _serve_evidence_policy


def _evidence(*, capabilities: tuple[str, ...], tags: tuple[str, ...]) -> PeerEvidenceSet:
    identity = PeerIdentity(
        provider="tailscale",
        evidence_source="localapi",
        assurance=IdentityAssurance.LOCAL_DAEMON,
        issuer="tailnet:test",
        transport="tcp",
        subject_kind=SubjectKind.TAGGED_NODE,
        subject_key="node:123",
        subject_stability=SubjectStability.STABLE,
        subject_verified=True,
        attributes={"tags": tags},
        capabilities={name: ({},) for name in capabilities},
        capabilities_verified=True,
    )
    return PeerEvidenceSet.from_results((PeerIdentityResult.available(identity),))


@pytest.mark.parametrize(
    ("capabilities", "tags"),
    [
        ((), ("tag:vgi-ci-client",)),
        (("query.farm/cap/vgi-test",), ("tag:unexpected",)),
    ],
)
def test_localapi_gate_rejects_missing_grant_inputs(
    capabilities: tuple[str, ...],
    tags: tuple[str, ...],
) -> None:
    """A valid node identity is insufficient without the configured grant and tag."""
    policy = _localapi_policy(
        capability="query.farm/cap/vgi-test",
        tag="tag:vgi-ci-client",
    )
    with pytest.raises(PermissionError):
        policy(_evidence(capabilities=capabilities, tags=tags), AuthContext.anonymous())


def test_serve_gate_rejects_missing_application_capability() -> None:
    """Serve evidence cannot pass merely because it came from the trusted proxy."""
    policy = _serve_evidence_policy(capability="query.farm/cap/vgi-test")
    with pytest.raises(PermissionError, match="application capability"):
        policy(
            _evidence(capabilities=(), tags=()),
            AuthContext.anonymous(),
        )
