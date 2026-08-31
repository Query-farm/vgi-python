"""Run an identity-aware high-level VGI Worker in the Tailnet topology."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Sequence
from typing import Annotated, Any

import pyarrow as pa
import waitress  # type: ignore[import-untyped]
from vgi_rpc import PeerEvidenceSet
from vgi_rpc.http import tailscale_localapi_provider, tailscale_serve_header_provider
from vgi_rpc.rpc import AuthContext, PeerAuthenticationPolicy, peer_identity_primary

from vgi.arguments import Auth, Param, Returns
from vgi.scalar_function import ScalarFunction
from vgi.serve import create_app
from vgi.worker import Worker


def _fingerprint(value: str | None) -> str | None:
    """Return a comparison-safe representation without logging an identity."""
    return hashlib.sha256(value.encode()).hexdigest() if value else None


class TailnetAuthSnapshot(ScalarFunction):
    """Return the redacted authentication visible to VGI function code."""

    class Meta:
        """Function metadata."""

        name = "tailnet_auth_snapshot"

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param(doc="one row per requested snapshot")],
        auth: Annotated[AuthContext, Auth()],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Serialize a redacted view of the injected authentication."""
        del cls
        safe_claims: dict[str, Any] = {
            "issuer": auth.claims.get("issuer"),
            "subject_kind": auth.claims.get("subject_kind"),
            "assurance": auth.claims.get("assurance"),
            "evidence_source": auth.claims.get("evidence_source"),
            "subject_fingerprint": _fingerprint(auth.claims.get("subject")),
            "peer_evidence_binding_present": bool(auth.claims.get("peer_evidence_binding")),
            "tags": sorted(str(value) for value in auth.claims.get("test_tags", ())),
            "capability_names": sorted(str(value) for value in auth.claims.get("test_capability_names", ())),
        }
        payload = json.dumps(
            {
                "authenticated": auth.authenticated,
                "domain": auth.domain,
                "principal_fingerprint": _fingerprint(auth.principal),
                "claims": safe_claims,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return pa.array([payload] * len(value))


class TailnetWorker(Worker):
    """High-level worker used by the live Tailnet release gate."""

    functions = [TailnetAuthSnapshot]


def _require_test_attributes(identity: Any, *, capability: str, tag: str | None) -> tuple[list[str], list[str]]:
    """Fail closed unless the live identity carries the configured grant inputs."""
    capability_names = sorted(str(name) for name in identity.capabilities)
    if not identity.capabilities_verified or capability not in capability_names:
        raise PermissionError("Tailnet identity lacks the required application capability")
    tags = sorted(str(value) for value in identity.attributes.get("tags", ()))
    if tag is not None and tag not in tags:
        raise PermissionError("Tailnet identity lacks the required node tag")
    return tags, capability_names


def _localapi_policy(*, capability: str, tag: str) -> PeerAuthenticationPolicy:
    """Build stable-primary authentication with live grant assertions."""

    def evaluate(evidence: PeerEvidenceSet, existing: AuthContext) -> AuthContext:
        auth = peer_identity_primary("tailscale")(evidence, existing)
        identity = evidence.require_usable_provider("tailscale")
        tags, capabilities = _require_test_attributes(identity, capability=capability, tag=tag)
        claims = dict(auth.claims)
        claims.update({"test_tags": tags, "test_capability_names": capabilities})
        return AuthContext(
            authenticated=auth.authenticated,
            domain=auth.domain,
            principal=auth.principal,
            claims=claims,
        )

    return evaluate


def _serve_evidence_policy(*, capability: str) -> PeerAuthenticationPolicy:
    """Build subjectless Serve evidence validation for the live probe."""

    def evaluate(evidence: PeerEvidenceSet, existing: AuthContext) -> AuthContext:
        identities = evidence.require_available_provider("tailscale")
        if len(identities) != 1:
            raise PermissionError("Tailnet integration expected exactly one Serve identity")
        identity = identities[0]
        tags, capabilities = _require_test_attributes(identity, capability=capability, tag=None)
        claims = dict(existing.claims)
        claims.update(
            {
                "issuer": identity.issuer,
                "subject_kind": identity.subject_kind.value,
                "assurance": identity.assurance.value,
                "evidence_source": identity.evidence_source,
                "subject": identity.subject_key,
                "peer_evidence_binding": evidence.binding_digest(("tailscale",)),
                "test_tags": tags,
                "test_capability_names": capabilities,
            }
        )
        return AuthContext(
            authenticated=existing.authenticated,
            domain=existing.domain,
            principal=existing.principal,
            claims=claims,
        )

    return evaluate


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issuer", required=True)
    parser.add_argument("--required-capability", required=True)
    parser.add_argument("--required-tag")
    subparsers = parser.add_subparsers(dest="transport", required=True)

    tcp = subparsers.add_parser("tcp")
    tcp.add_argument("--host", default="127.0.0.1")
    tcp.add_argument("--port", type=int, required=True)
    tcp.add_argument("--localapi-socket", required=True)

    http = subparsers.add_parser("http")
    http.add_argument("--host", default="127.0.0.1")
    http.add_argument("--port", type=int, required=True)
    http.add_argument("--localapi-socket")
    http.add_argument("--trusted-proxy-address", action="append", default=[])
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Start the configured high-level TCP or HTTP worker."""
    args = _parser().parse_args(argv)
    if args.transport == "tcp":
        if not args.required_tag:
            raise SystemExit("TCP LocalAPI mode requires --required-tag")
        provider = tailscale_localapi_provider(issuer=args.issuer, unix_socket=args.localapi_socket)
        TailnetWorker(quiet=True).serve_tcp(
            args.host,
            args.port,
            threaded=True,
            max_connections=16,
            peer_identity_providers=(provider,),
            peer_authentication_policy=_localapi_policy(
                capability=args.required_capability,
                tag=args.required_tag,
            ),
        )
        return

    if args.localapi_socket:
        if not args.required_tag:
            raise SystemExit("HTTP LocalAPI mode requires --required-tag")
        provider = tailscale_localapi_provider(issuer=args.issuer, unix_socket=args.localapi_socket)
        policy = _localapi_policy(capability=args.required_capability, tag=args.required_tag)
    else:
        if not args.trusted_proxy_address:
            raise SystemExit("HTTP Serve mode requires --trusted-proxy-address")
        provider = tailscale_serve_header_provider(
            issuer=args.issuer,
            trusted_proxy_addresses=args.trusted_proxy_address,
        )
        policy = _serve_evidence_policy(capability=args.required_capability)
    app = create_app(
        TailnetWorker,
        describe=False,
        peer_identity_providers=(provider,),
        peer_authentication_policy=policy,
    )
    waitress.serve(app, host=args.host, port=args.port, threads=8, _quiet=True)


if __name__ == "__main__":
    main()
