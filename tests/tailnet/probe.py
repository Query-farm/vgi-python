"""Assert high-level VGI authentication over a real Tailnet connection."""

from __future__ import annotations

import argparse
import json
import socket
from collections.abc import Sequence
from typing import Any, cast

import httpx2
import pyarrow as pa

from vgi.arguments import Arguments
from vgi.client import Client


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="transport", required=True)
    tcp = subparsers.add_parser("tcp")
    tcp.add_argument("--host", required=True)
    tcp.add_argument("--port", type=int, required=True)
    tcp.add_argument("--proxy")
    tcp.add_argument("--require-local-dns-failure", action="store_true")
    http = subparsers.add_parser("http")
    http.add_argument("--url", required=True)
    http.add_argument("--spoof-login")

    for child in (tcp, http):
        child.add_argument("--expected-issuer", required=True)
        child.add_argument("--expected-evidence-source", required=True)
        child.add_argument("--expected-assurance", required=True)
        child.add_argument("--expected-subject-kind", required=True)
        child.add_argument("--expected-capability", required=True)
        child.add_argument("--expected-tag")
        child.add_argument("--expect-authenticated", action="store_true")
    return parser


def _assert_no_local_dns(host: str, port: int) -> None:
    try:
        socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return
    raise AssertionError(f"{host!r} unexpectedly resolved outside the Tailnet SOCKS proxy")


def _invoke(client: Client) -> dict[str, Any]:
    batch = pa.record_batch({"value": pa.array([1], type=pa.int64())})
    output = list(
        client.scalar_function(
            function_name="tailnet_auth_snapshot",
            schema_name="main",
            arguments=Arguments(positional=(pa.scalar("value"),)),
            input=iter((batch,)),
        )
    )
    assert len(output) == 1 and output[0].num_rows == 1, output
    return cast("dict[str, Any]", json.loads(output[0].column("result")[0].as_py()))


def _assert_snapshot(payload: dict[str, Any], args: argparse.Namespace) -> None:
    assert payload["authenticated"] is args.expect_authenticated, payload
    assert (payload["principal_fingerprint"] is not None) is args.expect_authenticated, payload
    claims = payload["claims"]
    assert claims["issuer"] == args.expected_issuer, payload
    assert claims["evidence_source"] == args.expected_evidence_source, payload
    assert claims["assurance"] == args.expected_assurance, payload
    assert claims["subject_kind"] == args.expected_subject_kind, payload
    assert claims["peer_evidence_binding_present"] is True, payload
    assert args.expected_capability in claims["capability_names"], payload
    if args.expected_tag:
        assert args.expected_tag in claims["tags"], payload
    if args.expect_authenticated:
        assert payload["domain"] == "tailscale", payload
        assert claims["subject_fingerprint"] is not None, payload
    else:
        assert payload["domain"] is None, payload
        assert claims["subject_fingerprint"] is None, payload


def main(argv: Sequence[str] | None = None) -> None:
    """Run one live high-level function call and assert its authentication."""
    args = _parser().parse_args(argv)
    if args.transport == "tcp":
        if args.require_local_dns_failure:
            _assert_no_local_dns(args.host, args.port)
        with Client.from_tcp(args.host, args.port, proxy=args.proxy) as client:
            first = _invoke(client)
            second = _invoke(client)
    else:
        headers = {"Tailscale-User-Login": args.spoof_login} if args.spoof_login else None
        with (
            httpx2.Client(
                base_url=args.url,
                headers=headers,
                follow_redirects=True,
                timeout=15,
                trust_env=False,
            ) as http_client,
            Client.from_http(args.url, httpx_client=http_client) as client,
        ):
            first = _invoke(client)
            second = _invoke(client)
    _assert_snapshot(first, args)
    _assert_snapshot(second, args)
    assert first == second, "authentication changed between calls from one test peer"
    print(json.dumps(first, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
