# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Argument references (protocol 3.4): resolving ``BindRequest.arguments_location``.

A client binding a function with a large argument over HTTP uploads the
arguments once, through this worker's ``__upload_url__`` grant, and sends a
``BindRequest`` whose ``arguments`` is null and whose
``arguments_location`` carries the grant's sealed ``ref`` plus the upload's
size and SHA-256 (vgi ``docs/large-values-3.4.md`` §2, §5a). Every request that
embeds that bind call (cardinality, statistics, plan, projection,
dynamic-to-string and init) carries the small location, never the value.

The framework resolves the location **once per turn, at the turn's entry**
(where a ``CallContext`` exists) and hands user code a bind call whose
``arguments`` holds the resolved ``Arguments`` in
memory, so worker code that reads ``bind_call.arguments`` directly keeps
working. That resolved bind call still serializes as the bytes the client sent
(the reference form), so the location, never the value, goes back on the wire
or into stream state. vgi-rpc does the opening, the object cache (one ``get``
per object id per process, single-flight) and the size/SHA-256 checks.

A worker advertises support with the ``VGI-Arguments-Ref-Bytes`` capability
header (``install_arguments_ref_header``);
without it the extension never externalizes arguments.
"""

from __future__ import annotations

import dataclasses
import logging
import os
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from vgi_rpc.errors import Code, StatusError

from vgi.arguments import Arguments

if TYPE_CHECKING:
    from vgi_rpc.rpc import CallContext

    from vgi.protocol import BindRequest

__all__ = [
    "ARGUMENTS_REF_BYTES_ENV",
    "ARGUMENTS_REF_BYTES_HEADER",
    "ARGUMENTS_REF_MISSING_KIND",
    "ARGUMENTS_REF_MISSING_MESSAGE",
    "DEFAULT_ARGUMENTS_REF_BYTES",
    "DISABLE_ARGUMENTS_REF_ENV",
    "arguments_ref_bytes_from_env",
    "install_arguments_ref_header",
    "resolve_bind_call",
    "resolve_request_arguments",
    "resolved_arguments",
]

_logger = logging.getLogger(__name__)

#: The capability header that tells the client this worker accepts argument
#: references, and above how many bytes of arguments IPC to use one.
ARGUMENTS_REF_BYTES_HEADER = "VGI-Arguments-Ref-Bytes"
#: Operator override of the advertised value.
ARGUMENTS_REF_BYTES_ENV = "VGI_ARGUMENTS_REF_BYTES"
#: The default advertised value: 4 MiB.
DEFAULT_ARGUMENTS_REF_BYTES = 4 * 1024 * 1024
#: Test switch: the HTTP fixture server stops sending the header (the extension's
#: ``arguments_ref_fallback.test``), behaving like a worker without argument references.
DISABLE_ARGUMENTS_REF_ENV = "VGI_TEST_DISABLE_ARGUMENTS_REF"

#: Kind of the refusal for an argument object storage reports gone.
ARGUMENTS_REF_MISSING_KIND = "arguments_ref_missing"
#: The exact message for it (large-values spec §5a, "Prepared statements"):
#: argument objects live about a day, so a prepared statement's large argument
#: can expire, and there is no transparent rebind.
ARGUMENTS_REF_MISSING_MESSAGE = "this prepared statement's large argument has expired; prepare it again"

_INVALID_ARGUMENTS_REF_KIND = "invalid_arguments_ref"
_MAX_HEADER_DIGITS = 19


def arguments_ref_bytes_from_env(env: Mapping[str, str] | None = None) -> int:
    """The ``VGI-Arguments-Ref-Bytes`` value to advertise.

    Args:
        env: Environment to read; defaults to ``os.environ``.

    Returns:
        ``VGI_ARGUMENTS_REF_BYTES`` when set, else 4 MiB.

    Raises:
        ValueError: The variable is not a positive decimal integer of at most
            19 digits (the header's grammar), so a typo stops startup instead
            of silently advertising nothing.

    """
    source = os.environ if env is None else env
    raw = (source.get(ARGUMENTS_REF_BYTES_ENV) or "").strip()
    if not raw:
        return DEFAULT_ARGUMENTS_REF_BYTES
    if not raw.isdigit() or raw.startswith("0") or len(raw) > _MAX_HEADER_DIGITS:
        raise ValueError(
            f"{ARGUMENTS_REF_BYTES_ENV} must be a positive integer number of bytes "
            f"(no sign, no leading zero, at most {_MAX_HEADER_DIGITS} digits), got {raw!r}"
        )
    return int(raw)


class _ArgumentsRefHeaderMiddleware:
    """Falcon middleware adding ``VGI-Arguments-Ref-Bytes`` to every response.

    Sent alongside vgi-rpc's own capability headers (which its capabilities
    middleware sets on every response, including ``OPTIONS /health``
    discovery), so the client reads it wherever it reads those.
    """

    __slots__ = ("_value",)

    def __init__(self, value: int) -> None:
        self._value = str(value)

    def process_response(self, req: Any, resp: Any, resource: object, req_succeeded: bool) -> None:
        resp.set_header(ARGUMENTS_REF_BYTES_HEADER, self._value)


def install_arguments_ref_header(
    app: Any,
    *,
    upload_url_provider: object | None,
    external_location: object | None,
    env: Mapping[str, str] | None = None,
) -> int | None:
    """Advertise argument-reference support on a vgi-rpc WSGI app, when the worker has it.

    The header is the mandatory capability signal (large-values spec §5): a
    worker sends it whenever it advertises ``VGI-Upload-URL-Support: 2`` and
    can resolve what the grant mints, and never otherwise. That needs an
    upload-URL provider (the grant) and an object store that can fetch the
    object back by id (vgi-rpc ``ObjectStore``).

    Args:
        app: The Falcon app ``make_wsgi_app`` returned, before it serves.
        upload_url_provider: The provider passed to ``make_wsgi_app``.
        external_location: The server's ``ExternalLocationConfig`` (its
            ``storage`` resolves argument references).
        env: Environment for ``VGI_ARGUMENTS_REF_BYTES`` and the test switch
            ``VGI_TEST_DISABLE_ARGUMENTS_REF``; defaults to ``os.environ``.

    Returns:
        The advertised value, or ``None`` when nothing is advertised.

    """
    from vgi_rpc.external import ObjectStore

    source = os.environ if env is None else env
    if upload_url_provider is None:
        return None
    storage = getattr(external_location, "storage", None)
    if not isinstance(storage, ObjectStore):
        _logger.warning(
            "argument references disabled: the configured storage cannot fetch objects by id (no ObjectStore)"
        )
        return None
    if (source.get(DISABLE_ARGUMENTS_REF_ENV) or "").strip().lower() in ("1", "true", "yes"):
        return None
    value = arguments_ref_bytes_from_env(source)
    app.add_middleware(_ArgumentsRefHeaderMiddleware(value))
    return value


def _invalid(message: str) -> StatusError:
    return StatusError(
        f"{_INVALID_ARGUMENTS_REF_KIND}: {message}", code=Code.INVALID_ARGUMENT, kind=_INVALID_ARGUMENTS_REF_KIND
    )


def resolve_bind_call(bind_call: BindRequest, ctx: CallContext | None) -> BindRequest:
    """Resolve a bind call's ``arguments_location``, at the turn's entry.

    Steps (large-values spec §5a): open the sealed reference under this
    request's principal and check its size and SHA-256 equal the location's;
    look the object up in vgi-rpc's object cache by ``object_id``; on a miss,
    fetch it once (single-flight); verify its length and SHA-256; decode it as
    the ``Arguments`` IPC stream.

    Args:
        bind_call: The bind call as received.
        ctx: The turn's call context (it carries the principal, the server's
            key and storage).

    Returns:
        *bind_call* itself when its arguments came inline (or were already
        resolved); else a copy whose ``arguments`` holds the resolved value
        and which still serializes as the received bytes.

    Raises:
        StatusError: ``invalid_arguments_ref`` (``INVALID_ARGUMENT``) for a
            reference that does not open or match, no storage, or no context;
            ``arguments_ref_integrity`` (``DATA_LOSS``) for fetched bytes that
            differ; ``arguments_ref_missing`` (``NOT_FOUND``) with the
            prepared-statement message when storage reports the object gone.
            vgi-rpc's ``storage_unavailable`` passes through for transient
            storage failures.

    """
    if bind_call.arguments is not None:
        return bind_call
    location = bind_call.arguments_location
    assert location is not None  # BindRequest.__post_init__: exactly one is set
    if ctx is None:
        raise _invalid("argument references need the request's context to resolve")
    from vgi_rpc.state_ref import ArgumentsRefIntegrityError, ArgumentsRefMissingError, InvalidArgumentsRefError

    try:
        data = ctx.resolve_argument_ref(location.ref, size=location.size, sha256=location.sha256)
    except ArgumentsRefMissingError:
        raise StatusError(ARGUMENTS_REF_MISSING_MESSAGE, code=Code.NOT_FOUND, kind=ARGUMENTS_REF_MISSING_KIND) from None
    except (InvalidArgumentsRefError, ArgumentsRefIntegrityError) as exc:
        # Same kind and code; the message also names the kind, because a client
        # that shows only "<type>: <message>" (the DuckDB extension) would
        # otherwise never show the reason a caller can act on.
        raise StatusError(f"{exc.error_kind}: {exc}", code=exc.error_code, kind=exc.error_kind) from None
    try:
        arguments = Arguments.deserialize_from_bytes(data)
    except Exception as exc:
        # The bytes are exactly what the client uploaded and claimed (size and
        # SHA-256 checked), so a bad stream is the client's.
        raise _invalid(f"the uploaded arguments are not a valid arguments IPC stream: {exc}") from None
    return bind_call.with_resolved_arguments(arguments)


def resolve_request_arguments[R](request: R, ctx: CallContext | None) -> R:
    """Resolve the arguments of a ``BindRequest``, or of a request embedding one as ``bind_call``.

    Called at the entry of every worker method that receives a bind call
    (bind, cardinality, statistics, plan, projection, dynamic-to-string,
    init), so everything after sees resolved arguments.

    Args:
        request: A ``BindRequest`` or a request with a ``bind_call`` field.
        ctx: The turn's call context.

    Returns:
        *request* unchanged when nothing needed resolving, else a copy with
        the resolved bind call.

    """
    from vgi.protocol import BindRequest

    if isinstance(request, BindRequest):
        return resolve_bind_call(request, ctx)  # type: ignore[return-value]
    bind_call = getattr(request, "bind_call", None)
    if not isinstance(bind_call, BindRequest) or bind_call.arguments is not None:
        return request
    return dataclasses.replace(request, bind_call=resolve_bind_call(bind_call, ctx))  # type: ignore[type-var]


def resolved_arguments(bind_call: BindRequest, ctx: CallContext | None = None) -> Arguments:
    """The bind call's arguments: inline, already resolved, or resolved now through *ctx*.

    Framework code reads arguments only through this helper. A bind call that
    reached a reader unresolved and without a context is a framework bug
    (resolution happens at the turn's entry), reported as
    ``arguments_not_resolved`` (``FAILED_PRECONDITION``).

    Args:
        bind_call: The bind call.
        ctx: The turn's call context, when the caller has one.

    Returns:
        The bound ``Arguments``.

    Raises:
        StatusError: See ``resolve_bind_call``,
            plus ``arguments_not_resolved`` as above.

    """
    if bind_call.arguments is not None:
        return bind_call.arguments
    if ctx is None:
        raise StatusError(
            "BindRequest arguments are externalized (arguments_location) and were not resolved",
            code=Code.FAILED_PRECONDITION,
            kind="arguments_not_resolved",
        )
    arguments = resolve_bind_call(bind_call, ctx).arguments
    assert arguments is not None
    return arguments
