# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The one place a worker's ``RpcServer`` is built.

Every transport a worker serves — stdin/stdout, AF_UNIX (or a Windows named
pipe), raw TCP, the Iroh raw upstream and HTTP — builds its server through
[`build_rpc_server`][vgi.rpc_server.build_rpc_server]. Before this existed each
transport built its own, and they had drifted: only HTTP and Iroh hosted
reflection, only HTTP hosted identity. One builder makes "what is hosted on
which transport" a single decision rather than five.

What a server hosts, in reflection order:

1. The worker's own protocol (``vgi.v2``, or the worker's ``protocol_class``).
2. The worker's [`hosted_protocols`][vgi.worker.Worker.hosted_protocols], in
   the order returned — on **every** transport.
3. ``vgi_rpc.Reflection.v1`` — on every transport unless ``describe=False``
   (an HTTP operator choice, ``--no-describe``).
4. ``vgi_rpc.Identity.v1`` — HTTP only, and only when the worker overrides
   ``resolve_token`` and/or ``mint_grant``.

Extra protocols cannot change ``vgi.v2`` behaviour: vgi-rpc routes every
request on its ``vgi_rpc.protocol`` key, with no fallback to the primary, so a
client that only ever names ``vgi.v2`` (the DuckDB extension) dispatches
exactly as it would against a single-protocol server.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from vgi_rpc.external import ExternalLocationConfig
    from vgi_rpc.rpc import RpcServer
    from vgi_rpc.rpc._token_identity import IdentityImpl

    from vgi.meta_worker import MetaWorker
    from vgi.worker import Worker

__all__ = ["Transport", "build_rpc_server"]

Transport = Literal["pipe", "unix", "tcp", "iroh", "http"]
"""The transport a server is being built for.

``"unix"`` also covers the Windows named-pipe launcher path. Only ``"http"``
changes what is hosted (identity); the rest are recorded so the decision lives
here rather than at each call site.
"""

_TRANSPORTS: frozenset[str] = frozenset({"pipe", "unix", "tcp", "iroh", "http"})

#: Transports that authenticate their callers, and so may host
#: ``vgi_rpc.Identity.v1``. Identity answers questions about *other*
#: callers' credentials; on a transport with no caller identity there is no
#: allowlist to enforce, so it is not hosted there.
_IDENTITY_TRANSPORTS: frozenset[str] = frozenset({"http"})


def build_rpc_server(
    worker: Worker | MetaWorker,
    *,
    transport: Transport,
    describe: bool = True,
    introspect_principals: Iterable[str] | None = None,
    external_location: ExternalLocationConfig | None = None,
    otel_config: Any = None,
) -> RpcServer:
    """Build the ``RpcServer`` for *worker* on *transport*.

    Calls ``worker.hosted_protocols()`` exactly once and hosts the result
    alongside the worker's own protocol. The same list is hosted on every
    transport; see the module docstring for the full hosted set.

    Args:
        worker: The worker instance (or a ``MetaWorker`` composing several)
            that implements the VGI protocol.
        transport: Which transport this server will serve.
        describe: Host ``vgi_rpc.Reflection.v1``. Defaults on everywhere;
            HTTP passes the operator's ``--describe/--no-describe`` choice.
        introspect_principals: HTTP only. Principals permitted to call
            ``introspect_token``; ``None`` reads ``VGI_INTROSPECT_PRINCIPALS``.
            Only consulted when the worker overrides ``resolve_token``.
        external_location: Optional external-storage configuration for large
            batches (HTTP).
        otel_config: Optional ``OtelConfig``. When given, the server is
            instrumented and the worker's tracer is created from it. HTTP
            callers leave it ``None``: ``make_wsgi_app`` instruments there.

    Returns:
        The configured server.

    Raises:
        TypeError: ``hosted_protocols()`` returned something other than a
            sequence of ``(protocol_class, implementation)`` pairs.
        ValueError: A hosted protocol's name is invalid, reserved, the
            worker's own, or repeated; or *transport* is unknown.
        SystemExit: The worker overrides ``resolve_token`` on HTTP but no
            introspector allowlist is configured.

    """
    from vgi_rpc.rpc import RpcServer

    from vgi.protocol import VgiProtocol
    from vgi.worker import Worker, _get_vgi_version

    if transport not in _TRANSPORTS:
        raise ValueError(f"Unknown transport {transport!r}; expected one of {sorted(_TRANSPORTS)}.")

    primary: type = getattr(worker, "protocol_class", VgiProtocol)
    extra = _validated_hosted_protocols(worker, primary)

    identity: IdentityImpl | None = None
    if transport in _IDENTITY_TRANSPORTS:
        # A Worker's hooks are classmethods; a MetaWorker answers for the one
        # child that implements each hook.
        identity = _build_identity(type(worker) if isinstance(worker, Worker) else worker, introspect_principals)

    server = RpcServer(
        primary,
        worker,
        extra_protocols=extra,
        identity=identity,
        external_location=external_location,
        enable_describe=describe,
        server_version=_get_vgi_version(),
    )

    if otel_config is not None:
        from vgi_rpc.otel import instrument_server

        from vgi.otel import VgiTracer

        instrument_server(server, otel_config)
        worker._vgi_tracer = VgiTracer.create(otel_config)

    return server


def _validated_hosted_protocols(worker: Worker | MetaWorker, primary: type) -> tuple[tuple[type, object], ...]:
    """Call ``worker.hosted_protocols()`` once and check its shape and names.

    vgi-rpc checks these too, but its messages name a protocol class and not
    the worker hook that supplied it; checking here first lets the error say
    which method to fix.
    """
    from vgi_rpc.rpc._types import RESERVED_PROTOCOL_PREFIX, _protocol_wire_name, validate_protocol_name

    owner = f"{type(worker).__name__}.hosted_protocols()"
    raw = worker.hosted_protocols()
    if isinstance(raw, (str, bytes)) or not isinstance(raw, Sequence):
        raise TypeError(f"{owner} must return a sequence of (protocol_class, implementation) pairs, got {raw!r}.")

    primary_name = _protocol_wire_name(primary)
    seen: dict[str, type] = {}
    pairs: list[tuple[type, object]] = []
    for index, item in enumerate(raw):
        if not isinstance(item, tuple) or len(item) != 2 or not isinstance(item[0], type):
            raise TypeError(f"{owner} entry {index} must be a (protocol_class, implementation) tuple, got {item!r}.")
        proto, impl = item
        name = _protocol_wire_name(proto)
        if name.startswith(RESERVED_PROTOCOL_PREFIX):
            raise ValueError(
                f"{owner} entry {index} ({proto.__name__}) is named {name!r}, which claims the reserved "
                f"{RESERVED_PROTOCOL_PREFIX!r} prefix. Framework protocols are not supplied through this "
                f"hook: reflection is hosted automatically, and vgi_rpc.Identity.v1 is enabled by "
                f"overriding resolve_token() and/or mint_grant()."
            )
        try:
            validate_protocol_name(name)
        except ValueError as exc:
            raise ValueError(f"{owner} entry {index} ({proto.__name__}): {exc}") from exc
        if name == primary_name:
            raise ValueError(
                f"{owner} entry {index} ({proto.__name__}) is named {name!r}, the worker's own protocol. "
                f"Give it a distinct `protocol_name: ClassVar[str]`."
            )
        previous = seen.get(name)
        if previous is not None:
            raise ValueError(
                f"{owner} lists protocol name {name!r} twice ({previous.__name__} and {proto.__name__}). "
                f"The name is the routing key, so each hosted protocol needs a distinct `protocol_name`."
            )
        seen[name] = proto
        pairs.append((proto, impl))
    return tuple(pairs)


def _build_identity(
    worker_cls: type[Worker] | MetaWorker,
    introspect_principals: Iterable[str] | None,
) -> IdentityImpl | None:
    """Build the ``vgi_rpc.Identity.v1`` implementation, or ``None``.

    ``None`` unless the worker class implements at least one of the two
    methods, and that is the point: ``RpcServer`` then does not host the
    protocol at all, rather than hosting it and refusing every call. The two
    are independent — a worker may resolve credentials without minting them,
    mint without resolving, or do both, and ``offered_methods()`` reports
    exactly what it wrote. Absent beats routed-and-refusing —
    it is what keeps a dependency upgrade from growing a
    credential-to-identity oracle on every existing worker.

    As of vgi-rpc 0.46.0 identity is an RPC-layer protocol hosted on the
    server, not the HTTP JSON route (``POST {prefix}/__introspect_token__``) it
    was through 0.45.x, so a client discovers it through ordinary reflection
    rather than by calling and reading an error. [`build_rpc_server`][] hosts
    it only on transports that authenticate callers (HTTP): its allowlist is a
    list of *principals*, which a transport without caller identity cannot
    check.

    Args:
        worker_cls: The worker class, consulted for ``resolve_token`` and
            ``mint_grant`` overrides, or a ``MetaWorker``, which answers for
            the one child class that overrides each.
        introspect_principals: Principals permitted to introspect, or ``None``
            to read the environment.

    Returns:
        The implementation, or ``None`` when this worker does not resolve
        credentials.

    """
    resolver = worker_cls._introspect_resolver()
    minter = worker_cls._grant_minter()
    if resolver is None and minter is None:
        return None

    # Not re-exported by ``vgi_rpc.rpc``, so the private module is the only
    # import path for the ``vgi_rpc.Identity.v1`` implementation helper.
    from vgi_rpc.rpc._token_identity import IdentityImpl

    return IdentityImpl(
        resolve_token=resolver,
        mint_grant=minter,
        # Only meaningful for ``introspect_token``, and ``IdentityImpl``
        # validates it only when a resolver is supplied — a worker that mints
        # grants but resolves nothing is not an oracle and needs no allowlist.
        introspect_principals=(_resolve_introspect_principals(introspect_principals) if resolver is not None else None),
    )


def _resolve_introspect_principals(explicit: Iterable[str] | None) -> list[str]:
    """Resolve the introspector allowlist, or exit with an actionable message.

    Env var: ``VGI_INTROSPECT_PRINCIPALS``, comma-separated.

    Fail-closed and loud rather than defaulting to "any authenticated caller":
    authenticating and introspecting are different capabilities, and a
    permissive default lets any user resolve any other user's credential to
    its owner. A worker that implements ``resolve_token`` and forgets the
    allowlist must not start.

    Args:
        explicit: Principals passed to :func:`create_app`, or None to read
            the environment.

    Returns:
        The allowlist, guaranteed non-empty.

    Raises:
        SystemExit: When neither source names a principal.

    """
    if explicit is not None:
        principals = [p.strip() for p in explicit if p.strip()]
    else:
        raw = os.environ.get("VGI_INTROSPECT_PRINCIPALS") or ""
        principals = [p.strip() for p in raw.split(",") if p.strip()]

    if not principals:
        sys.stderr.write(
            "Error: this worker implements resolve_token(), which hosts the\n"
            "  vgi_rpc.Identity.v1 protocol, but no introspector allowlist was\n"
            "  configured. Set VGI_INTROSPECT_PRINCIPALS (comma-separated) or\n"
            "  pass --introspect-principals.\n"
            "\n"
            "  There is no permissive default on purpose: introspection is a\n"
            "  separate capability from authentication, and allowing every\n"
            "  authenticated caller lets any user resolve any other user's\n"
            "  credential to its owner. Remove resolve_token() to leave the\n"
            "  protocol unhosted entirely.\n"
        )
        sys.exit(1)
    return principals
