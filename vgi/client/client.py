# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""VGI reference client — canonical implementation for other-language ports.

``vgi-python`` is the authoritative VGI implementation. Real users invoke
VGI from DuckDB via the C++ extension; this module exists for two other
audiences:

1. **Non-DuckDB callers.** A TypeScript port that wants to browse catalog
   contents, invoke a scalar, or feed an HTTP worker from outside DuckDB.
   The HTTP transport below is the canonical path for those callers.
2. **Porters.** TS/Go/Rust teams reading this file to understand what
   their client must do. Every HTTP-relevant code path aims to be
   plain enough to translate.

Protocol sequence (HTTP)::

    capabilities   → GET /capabilities              — upload-URL caps
    connect        → http_connect(base_url, auth)   — typed proxy
    catalogs       → proxy.catalog_catalogs()       — discover
    attach         → proxy.catalog_attach(req)      — open a catalog
    bind           → proxy.bind(BindRequest)        — resolve schema
    init           → proxy.init(InitRequest)        — open a stream
    exchange loop  → stream.exchange(AnnotatedBatch)
                     • oversize input  → request_upload_urls + PUT + pointer batch
                     • pointer output  → auto-resolve via external_location config
    detach         → proxy.catalog_detach(attach_opaque_data)

The subprocess transport (``_spawn_subprocess_connection``, ``WorkerPool``,
``shell=True``) is a Python-only convenience for running tests against a
local worker. Other-language ports do not need to mirror it — implement
the HTTP flow and skip the subprocess branch.

Parallel processing
-------------------
When a bind returns ``max_workers > 1`` the client spawns additional
worker connections and distributes input batches round-robin. Output
order is non-deterministic in parallel mode. This is optimization; a
minimal port can ignore it and always use one connection.

Key classes
-----------
    [`Client`][]             — main entry point; ``Client.from_http(...)`` for HTTP
    [`ClientError`][]    — raised on communication errors
    [`WorkerConnection`][] — internal; one per transport-level connection

Key methods
-----------
    client.catalogs()             — discover catalogs
    client.catalog_attach(...)    — open a catalog
    client.schemas(...)           — list schemas
    client.schema_contents(...)   — list tables/views/functions/macros
    client.scalar_function(...)   — invoke a scalar
    client.table_function(...)    — invoke a table function
    client.table_function_plan(...) — plan a table function into named, redeemable splits
    client.table_in_out_function(...) — invoke a table-in-out function
    client.aggregate_function(...) — run a grouped/global aggregate
    client.aggregate_session(...) — raw aggregate RPCs (update/combine/window)
    client.aggregate_streaming(...) — streaming-partitioned aggregates
    client.copy_formats(...)      — discover custom COPY formats
    client.copy_from(...)         — read a custom ``COPY ... FROM`` format
    client.copy_to(...)           — write a custom ``COPY ... TO`` format
    client.server_capabilities()  — HTTP only; upload-URL caps

See Also:
--------
    vgi.protocol.VgiProtocol      — the RPC interface this client exercises
    vgi.protocol.BindRequest      — request types
    vgi.arguments.Arguments       — positional/named argument container
    vgi_rpc.http.http_connect     — transport primitive this client wraps

"""

from __future__ import annotations

import contextlib
import io
import itertools
import logging
import os
import subprocess
import threading
from collections.abc import Callable, Generator, Iterator, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field, replace
from queue import Queue
from typing import IO, Any, Literal, cast

import pyarrow as pa
from vgi_rpc import WorkerPool
from vgi_rpc.log import Message
from vgi_rpc.rpc import (
    AnnotatedBatch,
    PipeTransport,
    RpcConnection,
    RpcError,
    StreamSession,
)

from vgi.arguments import Arguments
from vgi.client.aggregate import AggregateClientMixin
from vgi.client.catalog_mixin import CatalogClientMixin
from vgi.client.errors import ClientError as ClientError  # re-exported for callers
from vgi.invocation import (
    BindResponse,
    FunctionType,
    GlobalInitResponse,
)
from vgi.protocol import (
    BindRequest,
    CopyFromContext,
    CopyToContext,
    InitRequest,
    PlanResponse,
    ScanSplit,
    TableBufferingCombineRequest,
    TableBufferingDestructorRequest,
    TableBufferingProcessRequest,
    TableFunctionPlanRequest,
    VgiProtocol,
    _decode_parent_rows,
)
from vgi.table_function import TableInOutFunctionInitPhase

_logger = logging.getLogger("vgi.client")
_worker_logger = logging.getLogger("vgi.client.worker")


class ResumeUnsupported(ClientError):
    """Raised when a resumable scan is requested on a non-resumable transport.

    Only the HTTP transport round-trips producer state in continuation tokens,
    so only HTTP clients can drive :meth:`Client.table_scan_resumable`. On the
    pipe/subprocess transport the stream is a live connection with no
    serializable resume point; the caller must keep the live stream in-process
    instead.
    """


class ResumableTableScan:
    """A resumable, one-batch-at-a-time handle on an upstream table-function scan.

    Unlike :meth:`Client.table_function` (a live generator that hides the
    server's continuation token), each :meth:`next` returns ``(batch, token)``
    where ``token`` is the worker's serialized producer state AFTER ``batch``.
    A stateless client (e.g. a load-balanced proxy) can persist ``token``, drop
    the connection, and resume on another node via
    ``Client.table_scan_resumable(resume_token=token, ...)``.

    Single-worker: reads the primary stream only (parallel ``max_workers>1``
    reads are unordered and not resumable from a single token).
    """

    def __init__(self, client: Client, stream: StreamSession) -> None:
        """Wrap a started single-worker stream as a resumable cursor."""
        self._client = client
        self._stream = stream

    def next(self) -> tuple[pa.RecordBatch | None, bytes | None]:
        """Return ``(batch, resume_token)``; ``(None, None)`` at end-of-stream.

        ``resume_token`` resumes the scan AFTER ``batch`` on any node.
        """
        try:
            ab, token = self._stream.next_with_token()  # type: ignore[attr-defined]
        except RpcError as e:
            raise ClientError.from_rpc_error(e) from e
        return (ab.batch if ab is not None else None), token

    def close(self) -> None:
        """Release the underlying stream (no-op over HTTP — stateless)."""
        self._stream.close()


# Module-level worker pool shared across all Client instances.
# Reuses idle worker subprocesses between Client sessions, avoiding
# repeated spawn/teardown overhead (especially valuable in tests).
_default_pool = WorkerPool(max_idle=8, idle_timeout=30.0)

# True once the HTTP transport is wired end-to-end. Used by the
# parametrized ``client_transport`` fixture in tests/conftest.py to decide
# whether to skip the HTTP leg of the matrix.
_HTTP_TRANSPORT_READY = True

_DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES = 256 * 1024 * 1024
_MIN_ACCEPTED_MAX_RESPONSE_BYTES = 65536
_MAX_SAFE_HTTP_BYTES = (1 << 53) - 1


def _validate_accepted_max_response_bytes(value: int | None) -> int | None:
    """Validate the cross-SDK HTTP response-budget range."""
    if value is not None and (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not _MIN_ACCEPTED_MAX_RESPONSE_BYTES <= value <= _MAX_SAFE_HTTP_BYTES
    ):
        raise ValueError(
            "accepted_max_response_bytes must be an integer from "
            f"{_MIN_ACCEPTED_MAX_RESPONSE_BYTES} through {_MAX_SAFE_HTTP_BYTES}, or None"
        )
    return value


@dataclass
class WorkerConnection:
    """Holds state for a single worker connection (subprocess, HTTP, TCP, or launch).

    Exactly one of {proc+connection, _pool_ctx, _http_ctx, _tcp_ctx,
    _iroh_ctx, _launch_ctx}
    is active per connection — transport-specific teardown inspects these fields.

    Attributes:
        proxy: The typed `[`VgiProtocol`][]` proxy used to invoke the worker.
        worker_index: Index of this worker within a parallel pool.
        stream: The active streaming session, if any.
        proc: The worker subprocess for direct (non-pooled) subprocess transport.
        connection: The RPC connection for direct subprocess transport.
    """

    proxy: VgiProtocol
    worker_index: int = 0
    stream: StreamSession | None = None
    # Subprocess transport, direct (non-pooled).
    proc: subprocess.Popen[bytes] | None = None
    connection: RpcConnection[VgiProtocol] | None = None
    # Subprocess transport, pooled.
    _pool_ctx: AbstractContextManager[Any] | None = field(default=None, repr=False)
    # HTTP transport: context manager from vgi_rpc.http.http_connect.
    _http_ctx: AbstractContextManager[Any] | None = field(default=None, repr=False)
    # TCP transport: context manager from vgi_rpc.rpc.tcp_connect.
    _tcp_ctx: AbstractContextManager[Any] | None = field(default=None, repr=False)
    # Native raw Iroh transport: context manager from vgi_rpc.iroh.iroh_connect.
    _iroh_ctx: AbstractContextManager[Any] | None = field(default=None, repr=False)
    # Launch transport: context manager from vgi_rpc.launcher.resolve_and_connect.
    _launch_ctx: AbstractContextManager[Any] | None = field(default=None, repr=False)


class Client(CatalogClientMixin, AggregateClientMixin):
    """Canonical VGI client — HTTP is the path other-language ports mirror.

    Two transports:

    * **HTTP** (``Client.from_http(base_url, bearer_token=...)``). The
      canonical non-DuckDB path. Uses ``vgi_rpc.http.http_connect`` under
      the hood; transparently resolves pointer batches returned by workers
      that externalize large outputs (demo storage, S3). Transparently
      externalizes large input batches when the server advertises upload-URL
      support.
    * **Subprocess** (``Client(server_path)``). Python-only convenience for
      local workers. Uses shell subprocesses + a ``WorkerPool`` for reuse.
      Ports don't need to mirror this.

    Catalog operations (``catalogs()``, ``schema_contents()``, etc.) are
    provided by `[`CatalogClientMixin`][]` and don't require ``start()``. They
    open a short-lived connection per call (HTTP) or borrow a pooled
    subprocess worker.

    Aggregate invocation (``aggregate_function``, ``aggregate_session``,
    ``aggregate_streaming``) comes from `[`AggregateClientMixin`][]`. Custom
    ``COPY`` formats reuse the table-function and buffered drivers with a COPY
    context attached — see ``copy_from`` / ``copy_to``, and ``copy_formats()``
    for discovery.

    Function invocation (``scalar_function``, ``table_function``,
    ``table_in_out_function``, ``aggregate_function``) requires ``start()`` —
    typically via the context-manager protocol::

        with Client.from_http("http://host:port", bearer_token="...") as c:
            for batch in c.table_function(function_name="sequence", ...):
                ...

    Attributes:
        THREAD_JOIN_TIMEOUT: Seconds to wait for a worker thread to join during
            shutdown.
        PROCESS_WAIT_TIMEOUT: Seconds to wait for a worker process to exit during
            shutdown before killing it. A hang guard — teardown never blocks past
            this, and never raises ``TimeoutExpired`` at the caller.
        STDERR_DRAIN_TIMEOUT: Seconds to wait for a worker's stderr to finish
            draining before an error message is built from it.
    """

    THREAD_JOIN_TIMEOUT: float = 5.0
    # A hang guard, not a latency budget: a healthy worker exits in milliseconds,
    # but a saturated machine (the whole test suite under `pytest -n auto`, each
    # fixture worker booting its own sub-workers) can push a clean exit past
    # several seconds. Overrunning this kills the worker, so the value has to be
    # far enough above normal shutdown that only a genuinely wedged process
    # reaches it.
    PROCESS_WAIT_TIMEOUT: float = 30.0
    # Bound on waiting for a worker's stderr to finish draining before an error
    # message is built from it. A dead worker's drainer returns almost at once;
    # a live worker's never does, so this is what that (rare) path costs.
    STDERR_DRAIN_TIMEOUT: float = 1.0

    @staticmethod
    def _combine_batches(batches: list[pa.RecordBatch]) -> pa.RecordBatch | None:
        """Combine multiple `RecordBatch`es into a single `RecordBatch`.

        Converts the batches to a PyArrow Table, combines chunks, and converts
        back to a single batch. When all input batches have zero rows, PyArrow's
        combine_chunks returns an empty list; in that case, the first original
        batch is returned to preserve the schema.

        Args:
            batches: List of `RecordBatch`es to combine. All batches must have
                compatible schemas.

        Returns:
            A single combined `RecordBatch`, or None if the input list is empty.

        """
        if not batches:
            return None

        combined = list(pa.Table.from_batches(batches).combine_chunks().to_batches())
        # If all batches were empty, combine_chunks returns empty list
        if len(combined) == 0:
            return batches[0]
        return combined[0]

    @staticmethod
    def _combine_parent_rows(parent_rows_batches: list[list[int]]) -> list[int]:
        """Concatenate per-output-batch parent-row lists in emission order.

        Mirrors `_combine_batches` for blended row-transform provenance: when
        a table-in-out call produces multiple output batches for one input
        batch (a worker replying `HAVE_MORE_OUTPUT` more than once), each
        sub-batch's decoded parent-row list already indexes into that same
        shipped input batch (see `_decode_parent_rows`'s contract), so
        concatenating the lists in the order the sub-batches were emitted is
        sound — no index renumbering is needed, because `_combine_batches`
        also concatenates the sub-batches themselves in that same order.

        Args:
            parent_rows_batches: One decoded parent-row list per output
                sub-batch, in emission order.

        Returns:
            The concatenated parent-row list, aligned row-for-row with what
            `_combine_batches` produces from the corresponding output batches.

        """
        combined: list[int] = []
        for batch_rows in parent_rows_batches:
            combined.extend(batch_rows)
        return combined

    @staticmethod
    def _on_worker_log(msg: Message) -> None:
        """Forward log messages from vgi_rpc to the worker logger."""
        level = getattr(logging, msg.level.name.upper(), logging.INFO)
        _worker_logger.log(level, "%s", msg.message)

    def _determine_max_workers(self, requested: int) -> int:
        """Apply system and user limits to the function's requested max_workers.

        Clamps the requested parallelism to the lower of:
        1. The system's CPU count (from os.cpu_count(), defaulting to 1)
        2. The user-specified worker_limit (if set via Client constructor)

        Args:
            requested: The max_workers value requested by the function,
                typically from the init response header.

        Returns:
            The effective max_workers after applying all limits. Always >= 1.

        """
        max_workers = requested

        # Limit to CPU count
        cpu_count = os.cpu_count() or 1
        if max_workers > cpu_count:
            _logger.debug("limiting_max_workers_to_cpu_count requested=%s cpu_count=%s", max_workers, cpu_count)
            max_workers = cpu_count

        # Limit to user-specified worker_limit
        if self._worker_limit is not None and max_workers > self._worker_limit:
            _logger.debug(
                "limiting_max_workers_to_worker_limit requested=%s worker_limit=%s",
                max_workers,
                self._worker_limit,
            )
            max_workers = self._worker_limit

        return max_workers

    @staticmethod
    def _settings_to_batch(settings: dict[str, Any] | None) -> pa.RecordBatch | None:
        """Convert settings dict to `RecordBatch` for protocol.

        Args:
            settings: Dictionary of setting name to value pairs.

        Returns:
            A single-row `RecordBatch` with one column per setting, or None.

        """
        if settings is None:
            return None
        return pa.RecordBatch.from_pydict({k: [v] for k, v in settings.items()})

    @staticmethod
    def _secrets_to_batch(secrets: dict[str, Any] | None) -> pa.RecordBatch | None:
        """Convert secrets dict to `RecordBatch` for protocol.

        Args:
            secrets: Dictionary of secret name to value pairs. Values can be
                simple scalars or dicts (for struct-typed secrets).

        Returns:
            A single-row `RecordBatch` with one column per secret, or None.

        """
        if secrets is None:
            return None
        return pa.RecordBatch.from_pydict({k: [v] for k, v in secrets.items()})

    @staticmethod
    def _deserialize_pushdown_filters(filters_bytes: bytes | None) -> pa.RecordBatch | None:
        """Deserialize pushdown filter bytes to `RecordBatch`.

        Args:
            filters_bytes: IPC-serialized `RecordBatch` bytes, or None.

        Returns:
            Deserialized `RecordBatch`, or None.

        """
        if filters_bytes is None:
            return None
        reader = pa.ipc.open_stream(pa.BufferReader(filters_bytes))
        return reader.read_next_batch()

    def __init__(
        self,
        server_path: str | Sequence[str] | None = None,
        passthrough_stderr: bool = False,
        worker_limit: int | None = None,
        attach_opaque_data: bytes | None = None,
        pool: WorkerPool | None = _default_pool,
        *,
        transport: Literal["subprocess", "http", "tcp", "iroh", "httpi", "launch"] = "subprocess",
        base_url: str | None = None,
        tcp_host: str | None = None,
        tcp_port: int | None = None,
        tcp_proxy: str | None = None,
        iroh_endpoint: str | None = None,
        iroh_secret_key: bytes | str | None = None,
        iroh_relay_urls: Sequence[str] | None = None,
        iroh_no_relay: bool = False,
        iroh_direct_addresses: Sequence[str] = (),
        iroh_remote_relay_url: str | None = None,
        iroh_connect_timeout: float | None = 30.0,
        iroh_io_timeout: float | None = 300.0,
        bearer_token: str | None = None,
        oauth: bool = False,
        oauth_refresh_token: str | None = None,
        oauth_flow: Literal["auto", "device_code", "pkce"] = "auto",
        oauth_timeout_seconds: float = 120.0,
        oauth_prompt: Literal["none", "login", "select_account", "consent"] = "none",
        httpx_client: Any | None = None,
        external_location: Any | None = None,
        accepted_max_response_bytes: int | None = _DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES,
        launch_argv: Sequence[str] | None = None,
        launch_idle_timeout: float = 300.0,
        launch_state_dir: str | None = None,
        launch_socket_path: str | None = None,
    ):
        """Initialize the VGI client.

        Creates a client configured to communicate with a VGI worker. The
        worker is not contacted until start() is called or the client is used
        as a context manager.

        Transport selection: pass ``server_path`` (default) to spawn a local
        subprocess worker; pass ``transport="http"`` + ``base_url=...`` (or
        use the ``Client.from_http(...)`` factory) to talk to a remote HTTP
        worker. Subprocess is Python-specific; HTTP is the canonical path
        other-language clients mirror.

        Args:
            server_path: Subprocess-only. The VGI worker command. A string is
                split with ``shlex.split``; pass a sequence to give
                the argv exactly, which is what you want for arguments carrying
                spaces or quotes (``[sys.executable, "-c", script]``). No shell
                is involved either way, so shell syntax — pipes, redirection,
                ``VAR=value`` prefixes, ``~`` expansion — is not interpreted.
            passthrough_stderr: Subprocess-only. If True, worker stderr is
                passed through to the parent process's stderr in real-time.
            worker_limit: Maximum number of parallel worker processes.
            attach_opaque_data: Optional unique identifier for the DuckDB database
                attachment. When VGI is used from an attached database, this
                allows tracing calls back to that specific attachment.
            pool: Subprocess-only. Optional `WorkerPool` for subprocess reuse.
                Pass None to disable pooling and use direct subprocess
                management.
            transport: Which transport to use. ``"subprocess"`` (default)
                spawns a local subprocess per worker; ``"http"`` connects to
                a running worker via ``vgi_rpc.http.http_connect``; ``"tcp"``
                connects to a running worker via ``vgi_rpc.rpc.tcp_connect``
                (raw Arrow-IPC framing, no auth/encryption — loopback /
                trusted networks only; use ``Client.from_tcp(...)``); ``"launch"``
                spawns-or-reuses a warm worker shared across every client
                pointing at the same command, coordinated via
                ``vgi_rpc.launcher`` (use ``Client.from_launch(...)``).
            base_url: HTTP-only. Base URL of the running worker, e.g.
                ``"http://127.0.0.1:8765"``.
            tcp_host: TCP-only. Hostname or IP of the running worker.
            tcp_port: TCP-only. Port of the running worker.
            tcp_proxy: TCP-only explicit SOCKS5h proxy URI, for example
                ``"socks5h://127.0.0.1:1055"``. Hostname resolution happens
                at the proxy. Proxy failure never falls back to direct TCP.
            iroh_endpoint: Iroh-only canonical ``iroh://`` or ``httpi://`` URI.
            iroh_secret_key: Optional persistent client identity as bytes,
                lowercase hexadecimal text, or z-base-32 text.
            iroh_relay_urls: Optional replacement relay set.
            iroh_no_relay: Disable relays and require a direct path.
            iroh_direct_addresses: Already-discovered remote socket addresses.
            iroh_remote_relay_url: Already-discovered relay hint for the remote.
            iroh_connect_timeout: Total endpoint bind/connect deadline.
            iroh_io_timeout: Per-I/O deadline after connection establishment.
            bearer_token: HTTP-only. When set, every request carries a
                static ``Authorization: Bearer <token>`` header. Mutually
                exclusive with ``oauth``/``oauth_refresh_token`` — for a
                token that expires or needs a login flow, use those instead.
            oauth: HTTP-only. When True, obtain and refresh bearer tokens
                automatically via OAuth (device-code today; PKCE is not yet
                implemented — see ``oauth_flow``) whenever the worker
                answers 401 with an RFC 9728 challenge. Implied by passing
                ``oauth_refresh_token``. The first call blocks and prints a
                "Visit: ... Enter code: ..." prompt until login completes;
                later calls reuse the cached token and refresh it silently.
                See ``vgi_rpc.http.VgiOAuthAuth`` for the full mechanism
                (RFC 8628 device-code polling, RFC 8414/9728 discovery,
                silent refresh, the secret-less-proxy ``token_endpoint``
                override).
            oauth_refresh_token: HTTP-only. Pre-obtained refresh token,
                seeded so the first request can silently refresh instead of
                running an interactive login. Implies ``oauth=True``.
                Mutually exclusive with ``bearer_token``.
            oauth_flow: HTTP-only, OAuth-only. ``"auto"`` (default) picks
                device-code when the server offers it; ``"device_code"``
                forces it; ``"pkce"`` is not yet implemented and raises
                ``NotImplementedError`` when actually needed.
            oauth_timeout_seconds: HTTP-only, OAuth-only. How long an
                interactive login may take before giving up (further capped
                by the authorization server's own device-code expiry).
            oauth_prompt: HTTP-only, OAuth-only. Reserved for the PKCE flow
                (not yet implemented); has no effect on device-code logins.
            httpx_client: HTTP-only escape hatch. When provided, overrides
                ``bearer_token``/``oauth``/``oauth_refresh_token`` and is
                used verbatim; supply this when you need mTLS or a custom
                auth scheme. Not the canonical path.
            external_location: HTTP-only. ``ExternalLocationConfig`` that
                controls how the client fetches pointer batches (workers
                that externalize large outputs via demo storage / S3 return
                empty batches carrying ``vgi_rpc.location`` metadata).
                Defaults to a vanilla ``ExternalLocationConfig()`` for HTTP
                transport so pointer batches are resolved automatically.
                Subprocess transport ignores this — subprocess workers
                don't return pointer batches.
            accepted_max_response_bytes: HTTP-only hard limit for decoded
                response bodies, advertised to the worker on every RPC request.
                Defaults to 256 MiB. Pass ``None`` to omit the negotiation
                header for legacy unbounded behavior.
            launch_argv: Launch-only. The worker command and arguments (an
                argv sequence, not a shell string — matches
                ``vgi_rpc.launcher.LaunchConfig.worker_argv``). Requires the
                ``vgi-python[launch]`` extra (pulls in ``vgi-rpc[cli]`` for
                ``filelock``, which ``vgi_rpc.launcher`` imports unconditionally).
            launch_idle_timeout: Launch-only. The shared worker self-terminates
                after this many idle seconds with zero connected clients.
                Forwarded to ``LaunchConfig.idle_timeout``.
            launch_state_dir: Launch-only. Override the launcher's default
                per-user state directory (lockfiles + sockets). Forwarded to
                ``LaunchConfig.state_dir``.
            launch_socket_path: Launch-only. Explicit socket path, skipping
                the hash-derived default. Forwarded to
                ``LaunchConfig.socket_path``.

        Raises:
            ValueError: If the transport / server_path / base_url
                combination is inconsistent.

        """
        if transport == "subprocess":
            if server_path is None:
                raise ValueError("subprocess transport requires server_path")
            if base_url is not None:
                raise ValueError("base_url is only meaningful for transport='http'")
        elif transport == "http":
            if base_url is None:
                raise ValueError("transport='http' requires base_url")
            if server_path is not None:
                raise ValueError("server_path is only meaningful for transport='subprocess'")
        elif transport == "tcp":
            if tcp_host is None or tcp_port is None:
                raise ValueError("transport='tcp' requires tcp_host and tcp_port")
            if server_path is not None:
                raise ValueError("server_path is only meaningful for transport='subprocess'")
            if base_url is not None:
                raise ValueError("base_url is only meaningful for transport='http'")
        elif transport in ("iroh", "httpi"):
            if iroh_endpoint is None:
                raise ValueError(f"transport={transport!r} requires iroh_endpoint")
            expected_scheme = f"{transport}://"
            if not iroh_endpoint.startswith(expected_scheme):
                raise ValueError(f"transport={transport!r} requires a {expected_scheme} endpoint")
            if server_path is not None:
                raise ValueError("server_path is only meaningful for transport='subprocess'")
            if base_url is not None:
                raise ValueError("base_url is only meaningful for transport='http'")
            if tcp_host is not None or tcp_port is not None:
                raise ValueError("tcp_host/tcp_port are only meaningful for transport='tcp'")
        elif transport == "launch":
            if not launch_argv:
                raise ValueError("transport='launch' requires a non-empty launch_argv")
            if server_path is not None:
                raise ValueError("server_path is only meaningful for transport='subprocess'")
            if base_url is not None:
                raise ValueError("base_url is only meaningful for transport='http'")
            if tcp_host is not None or tcp_port is not None:
                raise ValueError("tcp_host/tcp_port are only meaningful for transport='tcp'")
        else:
            raise ValueError(f"unknown transport {transport!r}")

        if transport != "tcp" and tcp_proxy is not None:
            raise ValueError("tcp_proxy is only meaningful for transport='tcp'")

        use_oauth = oauth or oauth_refresh_token is not None
        if use_oauth and bearer_token is not None:
            raise ValueError("bearer_token is mutually exclusive with oauth/oauth_refresh_token")
        if use_oauth and httpx_client is not None:
            raise ValueError(
                "oauth/oauth_refresh_token is mutually exclusive with httpx_client "
                "(the escape hatch is verbatim-or-nothing — build your own VgiOAuthAuth "
                "and pass it as httpx_client=httpx2.Client(auth=...) instead)"
            )
        if oauth_flow not in ("auto", "device_code", "pkce"):
            raise ValueError(f"oauth_flow must be 'auto', 'device_code', or 'pkce', got {oauth_flow!r}")
        if oauth_prompt not in ("none", "login", "select_account", "consent"):
            raise ValueError(
                f"oauth_prompt must be 'none', 'login', 'select_account', or 'consent', got {oauth_prompt!r}"
            )

        self.server_path: str | Sequence[str] = server_path if server_path is not None else ""
        self._transport = transport
        self._base_url = base_url
        self._http_prefix = ""
        self._iroh_endpoint = iroh_endpoint
        if transport == "httpi" and iroh_endpoint is not None:
            from vgi_rpc.iroh import parse_iroh_uri

            parsed_iroh = parse_iroh_uri(iroh_endpoint)
            self._base_url = f"http://{parsed_iroh.endpoint_hex}"
            self._http_prefix = parsed_iroh.base_path
        self._iroh_options: dict[str, Any] = {
            "secret_key": iroh_secret_key,
            "relay_urls": tuple(iroh_relay_urls) if iroh_relay_urls is not None else None,
            "no_relay": iroh_no_relay,
            "direct_addresses": tuple(iroh_direct_addresses),
            "remote_relay_url": iroh_remote_relay_url,
            "connect_timeout": iroh_connect_timeout,
            "io_timeout": iroh_io_timeout,
        }
        self._tcp_host = tcp_host
        self._tcp_port = tcp_port
        self._tcp_proxy = tcp_proxy
        self._bearer_token = bearer_token
        self._use_oauth = use_oauth
        self._oauth_refresh_token = oauth_refresh_token
        self._oauth_flow = oauth_flow
        self._oauth_timeout_seconds = oauth_timeout_seconds
        self._oauth_prompt = oauth_prompt
        # Built lazily by _get_or_create_httpx_client when self._use_oauth; None otherwise
        # (including when the caller supplied httpx_client= directly, which owns its own auth).
        self._oauth_auth: Any | None = None
        self._httpx_client = httpx_client
        self._accepted_max_response_bytes = _validate_accepted_max_response_bytes(accepted_max_response_bytes)
        self._launch_argv = tuple(launch_argv) if launch_argv is not None else None
        self._launch_idle_timeout = launch_idle_timeout
        self._launch_state_dir = launch_state_dir
        self._launch_socket_path = launch_socket_path
        # True when ``_get_or_create_httpx_client`` constructed the client and
        # is therefore responsible for closing it on ``stop()``. False when
        # the caller passed ``httpx_client=`` — ownership stays with them.
        self._httpx_client_owned = False
        # Auto-enable pointer-batch resolution for HTTP/TCP/launch unless the
        # caller asked for something different — all three are ordinary RPC
        # transports a worker can externalize batches over, unlike subprocess
        # (which never returns pointer batches). See ``external_location`` docs above.
        if transport in ("http", "tcp", "iroh", "httpi", "launch") and external_location is None:
            from vgi_rpc.external import ExternalLocationConfig

            external_location = ExternalLocationConfig()
        self._external_location = external_location
        # HTTP server capabilities cache. Populated lazily by
        # ``_get_http_capabilities`` — a single round-trip per Client that
        # drives upload-URL externalization decisions.
        self._http_capabilities: Any | None = None
        _worker_debug = os.environ.get("VGI_WORKER_DEBUG", "").lower() in ("1", "true", "yes")
        self.passthrough_stderr = passthrough_stderr or _worker_debug
        self._worker_limit = worker_limit
        self._attach_opaque_data = attach_opaque_data
        self._pool = pool
        self._primary: WorkerConnection | None = None
        # For multi-worker support
        self._additional_workers: list[WorkerConnection] = []
        self._stderr_buffer: list[bytes] = []
        self._stderr_lock = threading.Lock()
        self._stderr_threads: list[threading.Thread] = []

    @classmethod
    def from_http(
        cls,
        base_url: str,
        *,
        bearer_token: str | None = None,
        oauth: bool = False,
        oauth_refresh_token: str | None = None,
        oauth_flow: Literal["auto", "device_code", "pkce"] = "auto",
        oauth_timeout_seconds: float = 120.0,
        oauth_prompt: Literal["none", "login", "select_account", "consent"] = "none",
        httpx_client: Any | None = None,
        external_location: Any | None = None,
        accepted_max_response_bytes: int | None = _DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES,
        worker_limit: int | None = None,
        attach_opaque_data: bytes | None = None,
    ) -> Client:
        """Create a `[`Client`][]` bound to a remote HTTP VGI worker.

        Canonical entry point for non-DuckDB callers (e.g. a TypeScript port
        browsing catalog contents). Subprocess-specific kwargs are not
        accepted; pool/stderr semantics do not apply. See `[`Client.__init__`][]`
        for what ``oauth``/``oauth_refresh_token``/``oauth_flow`` do.
        """
        return cls(
            transport="http",
            base_url=base_url,
            bearer_token=bearer_token,
            oauth=oauth,
            oauth_refresh_token=oauth_refresh_token,
            oauth_flow=oauth_flow,
            oauth_timeout_seconds=oauth_timeout_seconds,
            oauth_prompt=oauth_prompt,
            httpx_client=httpx_client,
            external_location=external_location,
            accepted_max_response_bytes=accepted_max_response_bytes,
            worker_limit=worker_limit,
            attach_opaque_data=attach_opaque_data,
            pool=None,
        )

    @classmethod
    def from_tcp(
        cls,
        host: str,
        port: int,
        *,
        proxy: str | None = None,
        external_location: Any | None = None,
        worker_limit: int | None = None,
        attach_opaque_data: bytes | None = None,
    ) -> Client:
        """Create a `[`Client`][]` bound to a running TCP VGI worker.

        Connects via ``vgi_rpc.rpc.tcp_connect`` (raw Arrow-IPC framing). The
        framing carries **no authentication or encryption** — only connect to
        trusted endpoints on loopback or a trusted network; use
        ``Client.from_http(...)`` for untrusted networks. Spin up a matching
        worker with ``vgi-fixture-worker --tcp [HOST:]PORT``.

        Set ``proxy`` to an explicit ``socks5h://HOST:PORT`` URI for
        userspace networking. Target hostnames are resolved by the proxy;
        proxy errors never fall back to a direct connection.
        """
        return cls(
            transport="tcp",
            tcp_host=host,
            tcp_port=port,
            tcp_proxy=proxy,
            external_location=external_location,
            worker_limit=worker_limit,
            attach_opaque_data=attach_opaque_data,
            pool=None,
        )

    @classmethod
    def from_iroh(
        cls,
        endpoint: str,
        *,
        secret_key: bytes | str | None = None,
        relay_urls: Sequence[str] | None = None,
        no_relay: bool = False,
        direct_addresses: Sequence[str] = (),
        remote_relay_url: str | None = None,
        connect_timeout: float | None = 30.0,
        io_timeout: float | None = 300.0,
        bearer_token: str | None = None,
        external_location: Any | None = None,
        accepted_max_response_bytes: int | None = _DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES,
        worker_limit: int | None = None,
        attach_opaque_data: bytes | None = None,
    ) -> Client:
        """Create a client for a native ``iroh://`` or ``httpi://`` endpoint.

        ``iroh://`` retains connection-local stream state. ``httpi://`` uses
        VGI's HTTP continuation, request/response budget, authentication-header,
        and externalized-batch behavior over an authenticated Iroh connection.
        """
        from vgi_rpc.iroh import parse_iroh_uri

        target = parse_iroh_uri(endpoint)
        return cls(
            transport=cast('Literal["iroh", "httpi"]', target.scheme),
            iroh_endpoint=endpoint,
            iroh_secret_key=secret_key,
            iroh_relay_urls=relay_urls,
            iroh_no_relay=no_relay,
            iroh_direct_addresses=direct_addresses,
            iroh_remote_relay_url=remote_relay_url,
            iroh_connect_timeout=connect_timeout,
            iroh_io_timeout=io_timeout,
            bearer_token=bearer_token,
            external_location=external_location,
            accepted_max_response_bytes=accepted_max_response_bytes,
            worker_limit=worker_limit,
            attach_opaque_data=attach_opaque_data,
            pool=None,
        )

    @classmethod
    def from_launch(
        cls,
        worker_argv: Sequence[str],
        *,
        idle_timeout: float = 300.0,
        state_dir: str | None = None,
        socket_path: str | None = None,
        external_location: Any | None = None,
        worker_limit: int | None = None,
        attach_opaque_data: bytes | None = None,
    ) -> Client:
        """Create a `[`Client`][]` bound to a launcher-managed warm worker.

        Spawns (or reuses) a worker process serving over an ``AF_UNIX``
        socket via ``vgi_rpc.launcher`` — every client across the machine
        pointing at the same ``worker_argv`` shares one warm worker,
        coordinated by a per-command-hash flock, and the worker
        self-terminates after ``idle_timeout`` idle seconds with zero
        connected clients. This is the Python client-side counterpart to the
        VGI DuckDB extension's ``launch:<argv>`` LOCATION scheme. Requires
        the ``vgi-python[launch]`` extra.

        Args:
            worker_argv: The worker command and arguments, as an argv
                sequence — not a shell string, and not split with
                ``shlex.split`` the way ``server_path`` is for
                ``transport="subprocess"``.
            idle_timeout: Shared worker self-shutdown after this many idle
                seconds.
            state_dir: Override the launcher's default per-user state
                directory (lockfiles + sockets).
            socket_path: Explicit socket path, skipping the hash-derived
                default — every caller passing the same explicit path
                shares that worker regardless of ``worker_argv`` differences.
            external_location: Optional ``ExternalLocationConfig`` — see the
                constructor's docstring.
            worker_limit: Maximum number of parallel worker connections.
            attach_opaque_data: Optional unique identifier for the DuckDB
                database attachment.

        Returns:
            A `[`Client`][]` bound to the launcher-managed worker.

        """
        return cls(
            transport="launch",
            launch_argv=worker_argv,
            launch_idle_timeout=idle_timeout,
            launch_state_dir=state_dir,
            launch_socket_path=socket_path,
            external_location=external_location,
            worker_limit=worker_limit,
            attach_opaque_data=attach_opaque_data,
            pool=None,
        )

    def _drain_stderr(self, stderr: IO[bytes]) -> None:
        """Background thread that continuously reads stderr.

        This is necessary when using pipes because if stderr
        fills up the entire process will be blocked even writing
        to stdout.
        """
        while True:
            line = stderr.readline()
            if not line:
                break
            with self._stderr_lock:
                self._stderr_buffer.append(line)

    def get_worker_stderr(self) -> str:
        """Return all captured stderr from the worker processes.

        Returns stderr output from the primary worker and all additional workers
        spawned for parallel processing. The output is accumulated in a shared
        buffer throughout the client's lifetime.

        This method is thread-safe and can be called while processing is ongoing,
        though the buffer may not yet contain all output until the workers have
        completed.

        Returns:
            All captured stderr output as a UTF-8 decoded string. Invalid UTF-8
            sequences are replaced with the Unicode replacement character.

        Note:
            This method only returns data when passthrough_stderr=False was set
            in the constructor. When passthrough_stderr=True, stderr goes directly
            to the parent process's stderr and this method returns an empty string.

        """
        with self._stderr_lock:
            return b"".join(self._stderr_buffer).decode("utf-8", errors="replace")

    def _await_stderr_drain(self) -> None:
        """Give the stderr drainers a bounded chance to finish before we read them.

        ``_stderr_buffer`` is filled by daemon threads that are otherwise only
        joined in ``stop()``, so an error raised the instant a worker dies can be
        enriched *before* that worker's dying words have been read — the traceback
        the enrichment exists to surface is exactly the one most likely to be
        missing. It is a genuine race, not a theoretical one: delaying the drainer
        by 50ms is enough to lose the output entirely, and a loaded machine
        delays a daemon thread by far more than that.

        A dead worker's pipe is at EOF, so its drainer returns almost at once and
        this costs nothing. A live worker holds its pipe open and its drainer never
        returns, so that path pays the full timeout — measured across the whole
        test suite, that case arises 9 times, which is why this does not try to
        detect it. The obvious detector (skip the wait when ``proc.poll()`` says
        the worker is alive) is what shipped first and was wrong: ``poll()`` is
        only trustworthy in the *dead* direction, and on Windows it still reports
        None for a process whose pipes are already at EOF — precisely the moment
        this runs.
        """
        for stderr_thread in self._stderr_threads:
            stderr_thread.join(timeout=self.STDERR_DRAIN_TIMEOUT)

    def _client_error_with_stderr(self, error: ClientError) -> ClientError:
        """Enrich a [`ClientError`][] with captured worker stderr, if available.

        When passthrough_stderr is enabled, stderr already went to the terminal
        so we return the error unchanged. Otherwise we append the last 50 lines
        of captured stderr *after* the existing message — so the user's actual
        exception (first line of ``str(error)``) stays at the top of the
        rendered traceback and operational log noise trails.
        """
        if self.passthrough_stderr:
            return error
        self._await_stderr_drain()
        stderr = self.get_worker_stderr()
        if not stderr.strip():
            return error
        lines = stderr.strip().splitlines()
        excerpt = "\n".join(lines[-50:]) if len(lines) > 50 else "\n".join(lines)
        new_error = ClientError(f"{error}\n\nWorker stderr (last {len(excerpt.splitlines())} lines):\n{excerpt}")
        new_error.__cause__ = error.__cause__
        return new_error

    def _spawn_worker(self, worker_index: int) -> WorkerConnection:
        """Create a ``WorkerConnection`` for the configured transport.

        Dispatches to ``_spawn_subprocess_connection`` (Python-specific) or
        ``_spawn_http_connection`` (the canonical path other-language ports
        mirror). Keeping the two bodies separate makes the HTTP path easy
        to read in isolation.
        """
        if self._transport == "http":
            return self._spawn_http_connection(worker_index)
        if self._transport == "httpi":
            return self._spawn_http_connection(worker_index)
        if self._transport == "tcp":
            return self._spawn_tcp_connection(worker_index)
        if self._transport == "iroh":
            return self._spawn_iroh_connection(worker_index)
        if self._transport == "launch":
            return self._spawn_launch_connection(worker_index)
        return self._spawn_subprocess_connection(worker_index)

    def _spawn_tcp_connection(self, worker_index: int) -> WorkerConnection:
        """Connect to a running TCP worker via ``vgi_rpc.rpc.tcp_connect``.

        Raw Arrow-IPC framing with no auth/encryption — see ``from_tcp``.
        Multiple ``worker_index`` values open independent TCP connections to
        the same ``host:port``.
        """
        from vgi_rpc.rpc import tcp_connect

        assert self._tcp_host is not None and self._tcp_port is not None  # enforced in __init__
        proxy_options: dict[str, Any] = {}
        if self._tcp_proxy is not None:
            proxy_options["proxy"] = self._tcp_proxy
        ctx: AbstractContextManager[VgiProtocol] = tcp_connect(
            VgiProtocol,  # type: ignore[type-abstract]
            self._tcp_host,
            self._tcp_port,
            on_log=self._on_worker_log,
            external_location=self._external_location,
            **proxy_options,
        )
        proxy = ctx.__enter__()
        _logger.debug(
            "tcp_connection_opened worker_index=%s host=%s port=%s",
            worker_index,
            self._tcp_host,
            self._tcp_port,
        )
        return WorkerConnection(
            proxy=proxy,
            worker_index=worker_index,
            _tcp_ctx=ctx,
        )

    def _spawn_iroh_connection(self, worker_index: int) -> WorkerConnection:
        """Open one stateful native Iroh VGI stream."""
        from vgi_rpc.iroh import iroh_connect

        assert self._iroh_endpoint is not None
        ctx: AbstractContextManager[VgiProtocol] = iroh_connect(
            VgiProtocol,  # type: ignore[type-abstract]
            self._iroh_endpoint,
            on_log=self._on_worker_log,
            external_location=self._external_location,
            **self._iroh_options,
        )
        proxy = ctx.__enter__()
        _logger.debug("iroh_connection_opened worker_index=%s endpoint=%s", worker_index, self._iroh_endpoint)
        return WorkerConnection(proxy=proxy, worker_index=worker_index, _iroh_ctx=ctx)

    def _spawn_launch_connection(self, worker_index: int) -> WorkerConnection:
        """Connect to a launcher-managed worker via ``vgi_rpc.launcher.resolve_and_connect``.

        Every ``Client`` (in this process or any other on the machine) built
        with the same ``launch_argv`` shares one warm worker — see
        ``from_launch``. Multiple ``worker_index`` values open independent
        connections to that same shared worker.
        """
        from vgi_rpc.launcher import LaunchConfig, resolve_and_connect

        assert self._launch_argv is not None  # enforced in __init__
        config = LaunchConfig(
            worker_argv=self._launch_argv,
            socket_path=self._launch_socket_path,
            idle_timeout=self._launch_idle_timeout,
            state_dir=self._launch_state_dir,
        )
        ctx: AbstractContextManager[VgiProtocol] = resolve_and_connect(
            VgiProtocol,  # type: ignore[type-abstract]
            config,
            on_log=self._on_worker_log,
            external_location=self._external_location,
        )
        proxy = ctx.__enter__()
        _logger.debug(
            "launch_connection_opened worker_index=%s argv=%s",
            worker_index,
            self._launch_argv,
        )
        return WorkerConnection(
            proxy=proxy,
            worker_index=worker_index,
            _launch_ctx=ctx,
        )

    def _spawn_http_connection(self, worker_index: int) -> WorkerConnection:
        """Connect to a remote HTTP worker via ``vgi_rpc.http.http_connect``.

        This is the canonical path non-DuckDB clients implement; subprocess
        is a Python convenience. Multiple ``worker_index`` values map to
        independent RPC proxies against the same shared ``httpx2.Client``
        (and therefore the same base URL + auth config).
        """
        from vgi_rpc.http import http_connect

        httpx_client = self._get_or_create_httpx_client()
        if self._transport == "httpi":
            ctx = http_connect(
                VgiProtocol,  # type: ignore[type-abstract]
                base_url=self._base_url,
                client=httpx_client,
                prefix=self._http_prefix,
                on_log=self._on_worker_log,
                external_location=self._external_location,
                accepted_max_response_bytes=self._accepted_max_response_bytes,
            )
        else:
            ctx = http_connect(
                VgiProtocol,  # type: ignore[type-abstract]
                base_url=self._base_url,
                client=httpx_client,
                on_log=self._on_worker_log,
                external_location=self._external_location,
                accepted_max_response_bytes=self._accepted_max_response_bytes,
            )
        proxy = ctx.__enter__()
        _logger.debug("http_connection_opened worker_index=%s base_url=%s", worker_index, self._base_url)
        return WorkerConnection(
            proxy=proxy,
            worker_index=worker_index,
            _http_ctx=ctx,
        )

    def _get_or_create_httpx_client(self) -> Any:
        """Return the shared httpx2.Client for this Client's HTTP transport.

        Lazily constructs one bound to ``self._base_url`` (so RPC requests
        resolve against the remote worker) with either a static
        ``Authorization: Bearer <token>`` header (``bearer_token``) or a
        ``VgiOAuthAuth`` (``oauth``/``oauth_refresh_token``) that obtains and
        refreshes tokens on demand — never both, enforced at construction.
        When the caller passes ``httpx_client=`` directly, they're
        responsible for configuring ``base_url`` and auth on it — we use it
        verbatim.
        """
        if self._httpx_client is not None:
            return self._httpx_client

        import httpx2

        if self._transport == "httpi":
            from vgi_rpc.iroh import IrohHttpTransport, parse_iroh_uri

            assert self._iroh_endpoint is not None
            target = parse_iroh_uri(self._iroh_endpoint)
            native_transport = IrohHttpTransport(self._iroh_endpoint, **self._iroh_options)
            iroh_headers = (
                {"Authorization": f"Bearer {self._bearer_token}"}
                if self._bearer_token is not None
                else None
            )
            self._httpx_client = httpx2.Client(
                base_url=f"http://{target.endpoint_hex}",
                transport=cast("Any", native_transport),
                headers=iroh_headers,
            )
            self._httpx_client_owned = True
            return self._httpx_client

        auth = None
        headers: dict[str, str] = {}
        if self._use_oauth:
            from vgi_rpc.http import VgiOAuthAuth

            self._oauth_auth = VgiOAuthAuth(
                base_url=self._base_url or "",
                flow=self._oauth_flow,
                refresh_token=self._oauth_refresh_token,
                timeout_seconds=self._oauth_timeout_seconds,
                prompt=self._oauth_prompt,
            )
            auth = self._oauth_auth
        elif self._bearer_token is not None:
            headers["Authorization"] = f"Bearer {self._bearer_token}"
        self._httpx_client = httpx2.Client(
            base_url=self._base_url or "",
            follow_redirects=True,
            headers=headers,
            auth=auth,
            # httpx2's 5s default read timeout is too aggressive for RPC
            # calls that do real server-side work (scans, cold workers).
            timeout=httpx2.Timeout(60.0, connect=10.0),
        )
        self._httpx_client_owned = True
        return self._httpx_client

    def oauth_identity(self) -> Any | None:
        """Return the signed-in OAuth identity's parsed id_token claims.

        Returns ``None`` when this client isn't using OAuth, or hasn't
        completed a login yet (the identity is only known once a real
        exchange has happened — calling this before the first request
        never triggers one). See ``vgi_rpc.http.OAuthIdentity`` for the
        fields (``sub``/``email``/``name``/``issuer``/``claims``).
        """
        if self._oauth_auth is None:
            return None
        return self._oauth_auth.identity()

    def _spawn_subprocess_connection(self, worker_index: int) -> WorkerConnection:
        """Spawn or borrow a subprocess worker and wrap it in an RPC proxy.

        When a pool is configured, borrows an idle worker (or spawns a new
        one) from the pool. Otherwise creates a subprocess directly.

        Python-specific: subprocess management relies on the ``WorkerPool``
        abstraction that other languages don't need to mirror.
        """
        if self._pool is not None:
            _logger.debug("borrowing_worker worker_index=%s", worker_index)
            cmd = self._worker_argv()
            ctx = self._pool.connect(
                VgiProtocol,  # type: ignore[type-abstract]
                cmd,
                on_log=self._on_worker_log,
            )
            proxy = ctx.__enter__()
            _logger.debug("worker_borrowed worker_index=%s", worker_index)
            return WorkerConnection(
                proxy=proxy,
                worker_index=worker_index,
                _pool_ctx=ctx,
            )

        _logger.debug("spawning_worker worker_index=%s", worker_index)
        # Not shell=True: under a shell, proc is the *shell*, so proc.kill()
        # (stop(force=True)) kills the shell and leaves the worker holding the
        # pipe -- on Windows it kills cmd.exe and reports exit code 1 while the
        # worker runs on. Splitting the command ourselves makes proc the worker
        # on every platform, and matches how the pooled and catalog paths have
        # always spawned it.
        proc = subprocess.Popen(
            self._worker_argv(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None if self.passthrough_stderr else subprocess.PIPE,
            text=False,
            bufsize=0,
        )
        _logger.debug("worker_spawned worker_index=%s pid=%s", worker_index, proc.pid)

        if proc.stdout is None:
            raise ClientError("Failed to create stdout pipe for worker subprocess")

        if not self.passthrough_stderr:
            if proc.stderr is None:
                raise ClientError("Failed to create stderr pipe for worker subprocess")
            stderr_thread = threading.Thread(target=self._drain_stderr, args=(proc.stderr,), daemon=True)
            stderr_thread.start()
            self._stderr_threads.append(stderr_thread)

        assert proc.stdin is not None, "stdin pipe not created for worker"
        stdout_buffered = io.BufferedReader(cast(io.RawIOBase, proc.stdout))
        transport = PipeTransport(reader=stdout_buffered, writer=cast(io.IOBase, proc.stdin))
        connection: RpcConnection[VgiProtocol] = RpcConnection(
            VgiProtocol,  # type: ignore[type-abstract]
            transport,
            on_log=self._on_worker_log,
        )
        proxy = connection.__enter__()

        return WorkerConnection(
            proxy=proxy,
            worker_index=worker_index,
            proc=proc,
            connection=connection,
        )

    def _stop_worker(self, worker: WorkerConnection, *, force: bool = False) -> int:
        """Stop a worker subprocess or return it to the pool.

        Closes the worker's stream session (if open), then either returns the
        worker to the pool (pooled) or exits the RPC connection and waits for
        the subprocess to terminate (direct).

        Args:
            worker: The worker connection to stop.
            force: SIGKILL a direct subprocess before the graceful teardown, so a
                worker blocked inside a handler cannot stall the caller. Only direct
                (non-pooled) subprocess workers own a ``proc``; every other transport
                is already prompt to close and ignores this.

        Returns:
            The subprocess exit code. Returns 0 for pooled workers (returned
            to pool) or normal termination, non-zero for errors. A killed worker
            reports its signal exit code (``-9`` on POSIX).

        """
        if force and worker.proc is not None:
            # Kill BEFORE closing the stream/connection: both block on a worker that is
            # stuck in a handler, which is the case force exists to escape.
            _logger.debug("killing_worker worker_index=%s pid=%s", worker.worker_index, worker.proc.pid)
            worker.proc.kill()

        if worker.stream is not None:
            if force:
                # The pipe is already dead; a close that raises must not mask the kill.
                with contextlib.suppress(Exception):
                    worker.stream.close()
            else:
                worker.stream.close()
            worker.stream = None

        if worker._http_ctx is not None:
            # HTTP transport — close the RPC proxy. The underlying httpx2
            # client is shared across workers and closed in Client.stop().
            worker._http_ctx.__exit__(None, None, None)
            _logger.debug("http_connection_closed worker_index=%s", worker.worker_index)
            return 0

        if worker._tcp_ctx is not None:
            # TCP transport — close the RPC proxy (and its socket).
            worker._tcp_ctx.__exit__(None, None, None)
            _logger.debug("tcp_connection_closed worker_index=%s", worker.worker_index)
            return 0

        if worker._iroh_ctx is not None:
            worker._iroh_ctx.__exit__(None, None, None)
            _logger.debug("iroh_connection_closed worker_index=%s", worker.worker_index)
            return 0

        if worker._launch_ctx is not None:
            # Launch transport — close the RPC proxy (and its socket). The
            # shared worker process itself is untouched: it lives on for other
            # clients and self-terminates via its own idle timer.
            worker._launch_ctx.__exit__(None, None, None)
            _logger.debug("launch_connection_closed worker_index=%s", worker.worker_index)
            return 0

        if worker._pool_ctx is not None:
            # Return to pool — pool handles subprocess lifecycle
            worker._pool_ctx.__exit__(None, None, None)
            _logger.debug("worker_returned_to_pool worker_index=%s", worker.worker_index)
            return 0

        # Direct subprocess management
        assert worker.connection is not None
        assert worker.proc is not None
        if force:
            # Shutting down the RPC connection writes to the worker we just killed.
            with contextlib.suppress(Exception):
                worker.connection.__exit__(None, None, None)
        else:
            worker.connection.__exit__(None, None, None)
        try:
            worker.proc.wait(timeout=self.PROCESS_WAIT_TIMEOUT)
        except subprocess.TimeoutExpired:
            # A worker that outlives the graceful close gets killed rather than
            # propagating out of ``stop()`` / ``__exit__``, where it would both
            # leak the process and mask whatever exception was already unwinding.
            # The signal returncode still travels back to the caller, and the
            # warning keeps a genuinely wedged worker visible.
            _logger.warning(
                "worker_exit_timed_out worker_index=%s pid=%s timeout=%s killing",
                worker.worker_index,
                worker.proc.pid,
                self.PROCESS_WAIT_TIMEOUT,
            )
            worker.proc.kill()
            worker.proc.wait()
        returncode = worker.proc.returncode
        if force:
            # A signal exit code is the expected outcome here, not an error.
            _logger.debug(
                "worker_killed worker_index=%s pid=%s returncode=%s",
                worker.worker_index,
                worker.proc.pid,
                returncode,
            )
        elif returncode != 0:
            _logger.error(
                "worker_exited_with_error worker_index=%s pid=%s returncode=%s",
                worker.worker_index,
                worker.proc.pid,
                returncode,
            )
        else:
            _logger.debug(
                "worker_exited worker_index=%s pid=%s returncode=%s",
                worker.worker_index,
                worker.proc.pid,
                returncode,
            )
        return returncode

    def _close_secondary_workers(self, *, force: bool = False) -> None:
        """Close and stop all secondary (additional) workers.

        Args:
            force: Kill direct subprocess workers rather than closing them gracefully
                (see ``_stop_worker``).
        """
        for worker in self._additional_workers:
            self._stop_worker(worker, force=force)
        self._additional_workers = []

    def _join_threads(self, threads: list[threading.Thread]) -> None:
        """Wait for all threads to complete with timeout.

        Joins each thread with a timeout of THREAD_JOIN_TIMEOUT seconds.
        Logs a warning for any thread that does not terminate within the
        timeout period but does not raise an exception.

        Args:
            threads: List of Thread objects to wait for. Threads that have
                already completed will return immediately from join().

        """
        for thread in threads:
            thread.join(timeout=self.THREAD_JOIN_TIMEOUT)
            if thread.is_alive():
                _logger.warning("worker_thread_did_not_terminate")

    def start(self) -> None:
        """Start the primary worker subprocess.

        Spawns the worker process using the server_path configured in __init__,
        sets up RPC transport, and creates a typed [`VgiProtocol`][] proxy for
        method calls.

        After this method returns, the client is ready to invoke functions via
        table_in_out_function(), table_function(), or scalar_function(). When
        using the context manager protocol (with statement), this method is
        called automatically.

        The stderr buffer is cleared when start() is called, so any stderr from
        previous runs is discarded.

        Raises:
            [`ClientError`][]: If the client is already started (call stop() first),
                or if stdout/stderr pipes fail to be created.

        """
        if self._primary is not None:
            raise ClientError("Client already started")

        self._stderr_buffer = []
        _logger.debug("starting_server server_path=%s", self.server_path)
        self._primary = self._spawn_worker(0)
        if self._primary.proc is not None:
            id_repr: Any = self._primary.proc.pid
        elif self._primary._http_ctx is not None:
            id_repr = f"http({self._base_url})"
        elif self._primary._tcp_ctx is not None:
            id_repr = f"tcp({self._tcp_host}:{self._tcp_port})"
        elif self._primary._launch_ctx is not None:
            id_repr = f"launch({' '.join(self._launch_argv or ())})"
        else:
            id_repr = "pooled"
        _logger.debug("server_started id=%s", id_repr)

    def stop(self, *, force: bool = False) -> int:
        """Stop all worker subprocesses and clean up resources.

        Terminates all workers in the following order:
        1. Stops all additional workers (spawned for parallel processing)
        2. Stops the primary worker
        3. Waits for all stderr drain threads to complete (with timeout)
        4. Resets all internal state

        After this method returns, the client can be started again with start().
        When using the context manager protocol (with statement), this method
        is called automatically on exit.

        Cancelling an in-flight call
        ---------------------------
        A graceful stop waits for a worker that is blocked inside a handler, so it
        cannot be used to abandon a scan that has overrun its budget. Pass
        ``force=True`` to SIGKILL direct subprocess workers first, which unblocks any
        thread waiting on their output immediately. Note this only applies to *direct*
        subprocess workers: a pooled worker is returned to its pool rather than owned
        by this client, so construct the client with ``pool=None`` when you need to be
        able to cancel it. HTTP and TCP workers are already prompt to close.

        Args:
            force: Kill direct subprocess workers instead of shutting them down
                gracefully — see "Cancelling an in-flight call" above.

        Returns:
            The exit code of the primary worker process. Returns 0 for normal
            termination, non-zero values indicate errors (a forced stop reports the
            signal exit code, ``-9`` on POSIX). Exit codes from additional workers
            are logged but not returned.

        Raises:
            [`ClientError`][]: If the client was not started (call start() first).

        """
        if self._primary is None:
            raise ClientError("Client not started")

        # Stop additional workers first
        self._close_secondary_workers(force=force)

        # Stop primary worker
        returncode = self._stop_worker(self._primary, force=force)
        self._primary = None

        # Wait for stderr threads to finish draining
        for stderr_thread in self._stderr_threads:
            stderr_thread.join(timeout=self.THREAD_JOIN_TIMEOUT)
            if stderr_thread.is_alive():
                _logger.warning("stderr_thread_did_not_terminate")
        self._stderr_threads = []

        # Close the shared httpx2.Client if we created it ourselves.
        if self._httpx_client_owned and self._httpx_client is not None:
            try:
                self._httpx_client.close()
            finally:
                self._httpx_client = None
                self._httpx_client_owned = False
        # Close VgiOAuthAuth's own internal (unauthenticated) flow client, if one was built.
        if self._oauth_auth is not None:
            try:
                self._oauth_auth.close()
            finally:
                self._oauth_auth = None

        return returncode

    def server_capabilities(self) -> Any:
        """Return the HTTP server's advertised capabilities.

        Only valid for HTTP-mode clients. The returned
        ``HttpServerCapabilities`` includes the effective request/response
        limits, upload/externalization support, and whether the server honors
        ``VGI-Accept-Max-Response-Bytes``. The accepted-response setting is an
        RPC response-body limit, so it is not applied to this bodyless OPTIONS
        discovery request.
        """
        if self._transport not in ("http", "httpi"):
            raise ClientError("server_capabilities() is only available for HTTP or HTTP-over-Iroh transport")
        from vgi_rpc.http import http_capabilities

        httpx_client = self._get_or_create_httpx_client()
        if self._transport == "httpi":
            return http_capabilities(base_url=self._base_url, prefix=self._http_prefix, client=httpx_client)
        return http_capabilities(base_url=self._base_url, client=httpx_client)

    def __enter__(self) -> Client:
        """Enter the context manager by starting the worker subprocess."""
        self.start()
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        """Exit the context manager by stopping all worker subprocesses."""
        self.stop()

    # -----------------------------------------------------------------------
    # RPC helpers
    # -----------------------------------------------------------------------

    def _make_bind_request(
        self,
        *,
        function_name: str,
        schema_name: str,
        arguments: Arguments,
        function_type: FunctionType,
        input_schema: pa.Schema | None = None,
        settings: dict[str, Any] | None = None,
        secrets: dict[str, Any] | None = None,
        transaction_opaque_data: bytes | None = None,
        copy_from: CopyFromContext | None = None,
        copy_to: CopyToContext | None = None,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> BindRequest:
        """Create a BindRequest for the given function parameters."""
        return BindRequest(
            function_name=function_name,
            schema_name=schema_name,
            arguments=arguments,
            function_type=function_type,
            input_schema=input_schema,
            settings=self._settings_to_batch(settings),
            secrets=self._secrets_to_batch(secrets),
            attach_opaque_data=self._attach_opaque_data,
            transaction_opaque_data=transaction_opaque_data,
            copy_from=copy_from,
            copy_to=copy_to,
            at_unit=at_unit,
            at_value=at_value,
        )

    @staticmethod
    def _do_bind(
        proxy: VgiProtocol,
        bind_request: BindRequest,
        bind_result_callback: Callable[[BindResponse], None] | None = None,
    ) -> BindResponse:
        """Call bind on a worker proxy and return [`BindResponse`][].

        Args:
            proxy: [`VgiProtocol`][] proxy from `RpcConnection`.
            bind_request: The bind request to send.
            bind_result_callback: Optional callback invoked with the
                `BindResponse` before returning.

        Returns:
            `BindResponse` containing output_schema and opaque_data.

        Raises:
            [`ClientError`][]: If the RPC call fails.

        """
        try:
            bind_response: BindResponse = proxy.bind(request=bind_request)
        except RpcError as e:
            raise ClientError.from_rpc_error(e) from e

        if bind_result_callback is not None:
            bind_result_callback(bind_response)

        return bind_response

    @staticmethod
    def _do_init(
        proxy: VgiProtocol,
        bind_request: BindRequest,
        bind_response: BindResponse,
        *,
        projection_ids: list[int] | None = None,
        pushdown_filters_batch: pa.RecordBatch | None = None,
        phase: TableInOutFunctionInitPhase | None = None,
        execution_id: bytes | None = None,
        init_opaque_data: bytes | None = None,
        finalize_state_id: bytes | None = None,
        split_tokens: list[bytes] | None = None,
        join_keys: list[pa.RecordBatch] | None = None,
    ) -> StreamSession:
        """Call init on a worker proxy and return a `StreamSession`.

        Args:
            proxy: [`VgiProtocol`][] proxy from `RpcConnection`.
            bind_request: The original bind request.
            bind_response: The bind response containing output_schema.
            projection_ids: Optional column indices for projection.
            pushdown_filters_batch: Optional deserialized filter predicates.
            phase: Table-in-out function phase (INPUT or FINALIZE).
            execution_id: For secondary init, the execution ID from
                the primary worker's init response.
            init_opaque_data: For secondary init, the opaque data from
                the primary worker's init response.
            finalize_state_id: For ``TABLE_BUFFERING_FINALIZE`` init, the
                opaque finalize partition key this producer stream serves.
            split_tokens: Split tokens (from a prior `table_function_plan()`
                call) to redeem in this init, or `None` for an ordinary
                whole-scan init.
            join_keys: Serialized join-key batches pushed down for join
                filtering (one single-column `RecordBatch` per key column,
                looked up by column name — see
                ``PushdownFilters.get_join_keys_column``), or `None` when not
                applicable.

        Returns:
            `StreamSession` for data exchange or production.

        Raises:
            [`ClientError`][]: If the RPC call fails.

        """
        init_request = InitRequest(
            bind_call=bind_request,
            output_schema=bind_response.output_schema,
            bind_opaque_data=bind_response.opaque_data,
            projection_ids=projection_ids,
            pushdown_filters=pushdown_filters_batch,
            join_keys=join_keys,
            phase=phase,
            execution_id=execution_id,
            init_opaque_data=init_opaque_data,
            finalize_state_id=finalize_state_id,
            split_tokens=split_tokens,
        )
        try:
            stream: StreamSession = proxy.init(request=init_request)  # type: ignore[assignment]
            return stream
        except RpcError as e:
            raise ClientError.from_rpc_error(e) from e

    def _initialize_stream_common(
        self,
        *,
        function_name: str,
        schema_name: str,
        arguments: Arguments,
        function_type: FunctionType,
        input_schema: pa.Schema | None,
        settings: dict[str, Any] | None,
        secrets: dict[str, Any] | None,
        transaction_opaque_data: bytes | None,
        projection_ids: list[int] | None,
        pushdown_filters_batch: pa.RecordBatch | None,
        phase: TableInOutFunctionInitPhase | None,
        bind_result_callback: Callable[[BindResponse], None] | None,
        copy_from: CopyFromContext | None = None,
        split_tokens: list[bytes] | None = None,
        split_execution_id: bytes | None = None,
        split_init_opaque_data: bytes | None = None,
        join_keys: list[pa.RecordBatch] | None = None,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> tuple[BindRequest, BindResponse, GlobalInitResponse]:
        """Run the canonical bind → init → fan-out-workers sequence.

        All three function entry points (``scalar_function``,
        ``table_function``, ``table_in_out_function``) share this shape:

        1. Build a `[`BindRequest`][]` from the user's call.
        2. ``bind`` against the primary worker proxy.
        3. ``init`` against the primary — stores ``StreamSession`` on the
           primary worker connection.
        4. Read the `[`GlobalInitResponse`][]` header (carries ``max_workers``
           + ``execution_id`` for secondary workers).
        5. Spawn any additional workers and drive their ``init`` with the
           primary's execution identity.

        Centralizing this keeps HTTP/subprocess differences and protocol
        changes (e.g. future scoped-secret re-bind, init hints) in one
        place.

        ``split_tokens``, when given, redeems those specific named units of
        work from a prior ``table_function_plan()`` call instead of an
        ordinary whole-scan init — and forces single-worker mode (skips
        ``_spawn_additional_workers`` by clamping ``max_workers`` to 1): the
        server-advertised ``max_workers`` header describes fan-out for
        reading the *whole* table, which doesn't apply to redeeming one
        already-named unit of work. A caller wanting split-level parallelism
        drives multiple splits through multiple `Client`/thread instances
        itself (mirroring how `VgiCatalog._exchange_client()` in vgi-polars
        gives each thread its own `Client`), not via this method's own
        worker fan-out. ``split_execution_id``/``split_init_opaque_data``
        (from that same ``PlanResponse``) are echoed on this init exactly
        like a secondary worker's would be — ``PlanResponse.execution_id``'s
        docstring: "echoed on every split init. Scopes cross-process
        BoundStorage exactly as it does elsewhere" — so a worker whose splits
        share cross-process state via ``BoundStorage`` can find it, even
        though this is a *first* (not secondary) init for this connection.

        ``join_keys``, when given, is echoed verbatim on both the primary and
        every secondary worker's init — the same way ``pushdown_filters_batch``
        already is — so a semi-join pushdown applies consistently regardless
        of which worker ends up producing which rows.
        """
        assert self._primary is not None, "primary worker not started"

        bind_request = self._make_bind_request(
            function_name=function_name,
            schema_name=schema_name,
            arguments=arguments,
            function_type=function_type,
            input_schema=input_schema,
            settings=settings,
            secrets=secrets,
            transaction_opaque_data=transaction_opaque_data,
            copy_from=copy_from,
            at_unit=at_unit,
            at_value=at_value,
        )
        bind_response = self._do_bind(self._primary.proxy, bind_request, bind_result_callback)

        stream = self._do_init(
            self._primary.proxy,
            bind_request,
            bind_response,
            projection_ids=projection_ids,
            pushdown_filters_batch=pushdown_filters_batch,
            phase=phase,
            split_tokens=split_tokens,
            execution_id=split_execution_id,
            init_opaque_data=split_init_opaque_data,
            join_keys=join_keys,
        )
        self._primary.stream = stream

        init_response = stream.typed_header(GlobalInitResponse)
        max_workers = 1 if split_tokens is not None else self._determine_max_workers(init_response.max_workers)

        self._spawn_additional_workers(
            max_workers,
            bind_request,
            bind_response,
            init_response,
            projection_ids=projection_ids,
            pushdown_filters_batch=pushdown_filters_batch,
            phase=phase,
            join_keys=join_keys,
        )

        return bind_request, bind_response, init_response

    def _spawn_additional_workers(
        self,
        max_workers: int,
        bind_request: BindRequest,
        bind_response: BindResponse,
        global_init_response: GlobalInitResponse,
        *,
        projection_ids: list[int] | None = None,
        pushdown_filters_batch: pa.RecordBatch | None = None,
        phase: TableInOutFunctionInitPhase | None = None,
        join_keys: list[pa.RecordBatch] | None = None,
    ) -> None:
        """Spawn and initialize additional worker subprocesses in parallel.

        First spawns all worker subprocesses sequentially (fast operation), then
        initializes all workers in parallel using threads. Each additional worker
        receives a secondary init with the execution_id from the primary worker.

        The spawned workers are appended to self._additional_workers list.

        If max_workers is 1 or less, this method returns immediately without
        spawning any workers.

        Args:
            max_workers: Total number of workers desired (including the primary
                worker). For example, if max_workers=4, this method spawns
                3 additional workers (indices 1, 2, 3).
            bind_request: The original bind request to embed in init.
            bind_response: The bind response with output schema.
            global_init_response: The primary worker's init response containing
                execution_id and opaque_data for secondary init.
            projection_ids: Optional column indices for projection.
            pushdown_filters_batch: Optional deserialized filter predicates.
            phase: Table-in-out function phase (INPUT or FINALIZE).
            join_keys: Optional serialized join-key batches pushed down for
                join filtering, echoed to every secondary worker exactly like
                ``pushdown_filters_batch``.

        Raises:
            [`ClientError`][]: If any worker fails to initialize. The exception wraps
                the first initialization error encountered.

        """
        if max_workers <= 1:
            return

        # Spawn all worker subprocesses first (fast)
        new_workers: list[WorkerConnection] = []
        for worker_index in range(1, max_workers):
            worker = self._spawn_worker(worker_index)
            new_workers.append(worker)
            self._additional_workers.append(worker)

        # Initialize all workers in parallel (overlaps Python startup time)
        init_errors: list[Exception] = []

        def do_init(worker: WorkerConnection) -> None:
            try:
                stream = self._do_init(
                    worker.proxy,
                    bind_request,
                    bind_response,
                    projection_ids=projection_ids,
                    pushdown_filters_batch=pushdown_filters_batch,
                    phase=phase,
                    join_keys=join_keys,
                    execution_id=global_init_response.execution_id,
                    init_opaque_data=global_init_response.opaque_data,
                )
                worker.stream = stream
            except Exception as e:
                init_errors.append(e)

        init_threads: list[threading.Thread] = []
        for worker in new_workers:
            t = threading.Thread(target=do_init, args=(worker,))
            t.start()
            init_threads.append(t)

        for t in init_threads:
            t.join()

        if init_errors:
            error_msgs = [str(e) for e in init_errors]
            raise ClientError(
                f"Failed to initialize {len(init_errors)} worker(s):\n" + "\n".join(f"  - {msg}" for msg in error_msgs)
            ) from init_errors[0]

        _logger.debug("additional_workers_spawned count=%s", len(new_workers))

    # -----------------------------------------------------------------------
    # Batch processing helpers
    # -----------------------------------------------------------------------

    # -----------------------------------------------------------------------
    # HTTP upload-URL externalization (Phase 4)
    #
    # Non-DuckDB clients send IPC bytes inline on each exchange() call.
    # Servers can advertise a maximum request size via VGI-Max-Request-Bytes
    # (surfaced as HttpServerCapabilities.max_request_bytes). When an input
    # batch would exceed it AND the server supports upload URLs, we:
    #   1. request_upload_urls(count=1) → {upload_url, download_url}
    #   2. PUT the IPC bytes to upload_url
    #   3. replace the batch with an empty one + vgi_rpc.location metadata
    #      pointing at download_url
    # The worker resolves the pointer batch on its end (mirror of the
    # client's own external-location resolution on outputs).
    # -----------------------------------------------------------------------

    def _get_http_capabilities(self) -> Any:
        """Return cached ``HttpServerCapabilities`` (HTTP transport only)."""
        if self._http_capabilities is not None:
            return self._http_capabilities
        from vgi_rpc.http import http_capabilities

        httpx_client = self._get_or_create_httpx_client()
        if self._transport == "httpi":
            self._http_capabilities = http_capabilities(
                base_url=self._base_url,
                prefix=self._http_prefix,
                client=httpx_client,
            )
        else:
            self._http_capabilities = http_capabilities(base_url=self._base_url, client=httpx_client)
        return self._http_capabilities

    @staticmethod
    def _serialize_batch_ipc(batch: pa.RecordBatch) -> bytes:
        """Return Arrow IPC stream bytes for a single ``RecordBatch``."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        return sink.getvalue().to_pybytes()

    def _maybe_externalize_input_batch(self, batch: pa.RecordBatch) -> AnnotatedBatch:
        """If the batch would exceed ``max_request_bytes``, externalize via upload URL.

        No-op for subprocess transport or when the server doesn't advertise
        upload-URL support. Returns an ``AnnotatedBatch`` either wrapping
        the original batch (no externalization needed) or a pointer batch
        carrying ``vgi_rpc.location`` metadata.
        """
        if self._transport not in ("http", "httpi"):
            return AnnotatedBatch(batch=batch)

        caps = self._get_http_capabilities()
        if not getattr(caps, "upload_url_support", False):
            return AnnotatedBatch(batch=batch)
        threshold = getattr(caps, "max_request_bytes", None)
        if threshold is None or threshold <= 0:
            return AnnotatedBatch(batch=batch)

        ipc_bytes = self._serialize_batch_ipc(batch)
        if len(ipc_bytes) <= threshold:
            return AnnotatedBatch(batch=batch)

        from vgi_rpc.http import request_upload_urls
        from vgi_rpc.metadata import LOCATION_KEY

        httpx_client = self._get_or_create_httpx_client()
        upload_options: dict[str, Any] = {}
        if self._transport == "httpi":
            upload_options["prefix"] = self._http_prefix
        urls = request_upload_urls(base_url=self._base_url, count=1, client=httpx_client, **upload_options)
        if not urls:
            # Server claimed support but vended no URLs — surface the raw
            # request rather than silently sending too-large bytes.
            return AnnotatedBatch(batch=batch)
        upload = urls[0]

        put_resp = httpx_client.put(upload.upload_url, content=ipc_bytes, timeout=30.0)
        put_resp.raise_for_status()

        pointer = pa.RecordBatch.from_pydict(
            {field.name: [] for field in batch.schema},
            schema=batch.schema,
        )
        cm = pa.KeyValueMetadata({LOCATION_KEY: upload.download_url.encode()})
        _logger.debug(
            "externalized_input_batch size_bytes=%s download_url=%s",
            len(ipc_bytes),
            upload.download_url,
        )
        return AnnotatedBatch(batch=pointer, custom_metadata=cm)

    def _process_batch_on_worker(
        self,
        worker: WorkerConnection,
        input_batch: pa.RecordBatch,
        batch_index: int,
        *,
        decode_parent_rows: bool = False,
    ) -> tuple[list[pa.RecordBatch], list[list[int]] | None]:
        """Send a batch to a worker and collect all output batches.

        Sends the input batch via stream.exchange(), then checks the vgi.status
        metadata. If the worker returns HAVE_MORE_OUTPUT, sends the same input
        again. Continues until NEED_MORE_INPUT or no status (scalar functions).

        Args:
            worker: The worker connection to use. Must have stream initialized.
            input_batch: The input `RecordBatch` to send to the worker.
            batch_index: Index of this batch in the input sequence (for logging).
            decode_parent_rows: When True, additionally decode each output
                batch's `vgi_rpc.parent_row#b64` metadata (see
                `vgi.protocol._decode_parent_rows`) — used by blended
                row-transform table functions, where a worker's output row
                count need not match its input row count. False for every
                other table-in-out / scalar caller, which has no provenance
                concept and must not have the row-count-mismatch check inside
                `_decode_parent_rows` start firing for it.

        Returns:
            A tuple of (output batches, per-batch decoded parent-row lists).
            The second element is `None` when `decode_parent_rows` is False —
            keeps the common path from building a list it never uses.

        Raises:
            [`ClientError`][]: If worker.stream is None, if the worker returns
                an unexpected status, if the RPC call fails, or (when
                `decode_parent_rows` is True) if `vgi_rpc.parent_row#b64` is
                malformed or absent-with-mismatched-row-counts.

        """
        if worker.stream is None:
            raise ClientError(f"Worker {worker.worker_index} stream not initialized")

        output_batches: list[pa.RecordBatch] = []
        parent_rows_batches: list[list[int]] | None = [] if decode_parent_rows else None

        while True:
            _logger.debug(
                "sending_batch_to_worker worker_index=%s batch_index=%s num_rows=%s",
                worker.worker_index,
                batch_index,
                input_batch.num_rows,
            )

            try:
                annotated = self._maybe_externalize_input_batch(input_batch)
                output = worker.stream.exchange(annotated)
            except RpcError as e:
                raise ClientError.from_rpc_error(e) from e

            _logger.debug(
                "received_output_from_worker worker_index=%s num_rows=%s",
                worker.worker_index,
                output.batch.num_rows,
            )

            output_batches.append(output.batch)

            if parent_rows_batches is not None:
                try:
                    parent_rows_batches.append(
                        _decode_parent_rows(
                            output.custom_metadata,
                            output_rows=output.batch.num_rows,
                            input_rows=input_batch.num_rows,
                        )
                    )
                except RuntimeError as e:
                    raise ClientError(str(e)) from e

            # Check vgi.status for table-in-out status
            status = None
            if output.custom_metadata:
                status = output.custom_metadata.get(b"vgi.status")

            # status is None for scalar functions (no status metadata)
            if status == b"HAVE_MORE_OUTPUT":
                continue
            elif status == b"NEED_MORE_INPUT" or status is None:
                break
            else:
                raise ClientError(f"Unexpected status from worker {worker.worker_index}: {status!r}")

        return output_batches, parent_rows_batches

    def _worker_thread_loop(
        self,
        worker: WorkerConnection,
        input_queue: Queue[tuple[int, pa.RecordBatch] | None],
        output_queue: Queue[tuple[int, list[pa.RecordBatch], list[list[int]] | None] | BaseException],
        *,
        decode_parent_rows: bool = False,
    ) -> None:
        """Thread function that processes batches for a single worker.

        Runs in a dedicated thread, pulling (batch_index, batch) tuples from
        the input queue, processing them via _process_batch_on_worker, and
        pushing (batch_index, output_batches, parent_rows_batches) tuples to
        the output queue.

        When None is received from input_queue, signals thread completion by
        pushing (-1, [], None) to output_queue and exits.

        If an exception occurs during processing, it is caught, logged, and
        pushed to output_queue as the exception object itself.

        Args:
            worker: The worker connection to use for processing batches.
            input_queue: Thread-safe queue providing (batch_index, `RecordBatch`)
                tuples for processing. A None value signals end of input.
            output_queue: Thread-safe queue for results.
            decode_parent_rows: Forwarded to `_process_batch_on_worker` — see
                its docstring.

        """
        try:
            while True:
                item = input_queue.get()
                if item is None:
                    # End of input - signal thread completion
                    output_queue.put((-1, [], None))
                    break

                batch_index, input_batch = item
                outputs, parent_rows_batches = self._process_batch_on_worker(
                    worker, input_batch, batch_index, decode_parent_rows=decode_parent_rows
                )
                output_queue.put((batch_index, outputs, parent_rows_batches))
        except Exception as e:
            _logger.exception("worker_thread_error worker_index=%s", worker.worker_index)
            output_queue.put(e)

    def _distribute_and_collect(
        self,
        *,
        all_workers: list[WorkerConnection],
        first_batch: pa.RecordBatch,
        remaining_input: Iterator[pa.RecordBatch],
        decode_parent_rows: bool = False,
        parent_row_callback: Callable[[list[int]], None] | None = None,
    ) -> Generator[pa.RecordBatch]:
        """Distribute input batches round-robin across workers and collect output.

        Handles both single-worker and multi-worker cases uniformly. For each
        worker, spawns a dedicated thread that pulls batches from an input queue,
        sends them to the worker, and pushes results to a shared output queue.

        Args:
            all_workers: List of all workers (primary + additional).
            first_batch: The first input batch, already consumed from the
                iterator by the calling method.
            remaining_input: Iterator for remaining input batches.
            decode_parent_rows: See `_process_batch_on_worker`. Forwarded to
                every worker thread; False (the default) preserves today's
                behavior exactly for every existing caller.
            parent_row_callback: Optional callback invoked once per yielded
                output batch, immediately before the yield, with that batch's
                combined parent-row list (see `_combine_parent_rows`) — the
                same "callback then yield" contract `table_function`'s
                `batch_metadata_callback` uses. Only meaningful when
                `decode_parent_rows` is True; ignored otherwise.

        Yields:
            Output `RecordBatch`es from processing. When multiple batches are
            returned for a single input (HAVE_MORE_OUTPUT), they are combined
            into one batch. Order is non-deterministic for multi-worker mode.

        Raises:
            [`ClientError`][]: If a worker thread fails with an exception.

        """
        num_workers = len(all_workers)

        _logger.debug("starting_parallel_processing num_workers=%s", num_workers)

        # Create queues for each worker
        input_queues: list[Queue[tuple[int, pa.RecordBatch] | None]] = [Queue() for _ in range(num_workers)]
        output_queue: Queue[tuple[int, list[pa.RecordBatch], list[list[int]] | None] | BaseException] = Queue()

        # Start worker threads
        threads: list[threading.Thread] = []
        for i, worker in enumerate(all_workers):
            thread = threading.Thread(
                target=self._worker_thread_loop,
                args=(worker, input_queues[i], output_queue),
                kwargs={"decode_parent_rows": decode_parent_rows},
                daemon=True,
            )
            thread.start()
            threads.append(thread)

        # Distribute batches round-robin across workers
        batch_index = 0
        batches_sent = 0

        # Send first batch
        worker_idx = batch_index % num_workers
        input_queues[worker_idx].put((batch_index, first_batch))
        batches_sent += 1
        batch_index += 1

        # Send remaining batches
        for input_batch in remaining_input:
            worker_idx = batch_index % num_workers
            input_queues[worker_idx].put((batch_index, input_batch))
            batches_sent += 1
            batch_index += 1

        # Signal end of input to all workers
        for q in input_queues:
            q.put(None)

        _logger.debug("all_batches_distributed total_batches=%s", batches_sent)

        # Collect outputs from all workers
        # We expect batches_sent regular outputs + num_workers thread completion signals
        outputs_expected = batches_sent + num_workers
        outputs_received = 0

        while outputs_received < outputs_expected:
            result = output_queue.get()

            # Check for exceptions from worker threads
            if isinstance(result, BaseException):
                if isinstance(result, RpcError):
                    raise ClientError.from_rpc_error(result) from result
                raise ClientError(f"Worker thread failed: {result}") from result

            batch_idx, output_batches, parent_rows_batches = result
            outputs_received += 1

            # Combine output batches if needed
            combined = self._combine_batches(output_batches)
            if combined is not None:
                if decode_parent_rows and parent_row_callback is not None and parent_rows_batches is not None:
                    parent_row_callback(self._combine_parent_rows(parent_rows_batches))
                yield combined

            _logger.debug(
                "output_received batch_index=%s outputs_received=%s outputs_expected=%s",
                batch_idx,
                outputs_received,
                outputs_expected,
            )

        self._join_threads(threads)
        _logger.debug("all_worker_threads_complete")

    # -----------------------------------------------------------------------
    # Function methods
    # -----------------------------------------------------------------------

    def table_in_out_function(
        self,
        *,
        function_name: str,
        schema_name: str,
        input: Iterator[pa.RecordBatch],
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[BindResponse], None] | None = None,
        projection_ids: list[int] | None = None,
        pushdown_filters: bytes | None = None,
        join_keys: list[pa.RecordBatch] | None = None,
        settings: dict[str, Any] | None = None,
        secrets: dict[str, Any] | None = None,
        transaction_opaque_data: bytes | None = None,
        parent_row_callback: Callable[[list[int]], None] | None = None,
        has_finalize: bool = True,
    ) -> Generator[pa.RecordBatch]:
        """Invoke a table-in-out function on the worker and stream results.

        For parallel processing (max_workers > 1), input batches are distributed
        round-robin across workers using dedicated threads. Output order may not
        match input order in parallel mode. Only the primary worker receives the
        FINALIZE phase and produces final aggregated output.

        Args:
            function_name: Name of the function to invoke. Must exist in the
                worker's registry.
            schema_name: Name of the catalog schema that declares the function.
                Required — a worker may register one name in several schemas, so
                the (schema, name) pair is what identifies the implementation.
            input: Iterator yielding input `RecordBatch`es. Must yield at least one
                batch. The first batch's schema is used to initialize the IPC
                stream. Raises [`ClientError`][] if the iterator is empty.
            arguments: Optional [`Arguments`][] container with positional and named
                arguments to pass to the function. Defaults to empty `Arguments()`.
            bind_result_callback: Optional callback invoked with the [`BindResponse`][]
                before processing begins.
            projection_ids: Optional list of column indices for column projection.
            pushdown_filters: Optional byte string containing filter predicates
                to push down to the function.
            join_keys: Optional serialized join-key batches for semi-join
                pushdown — one single-column `RecordBatch` per join-key
                column, matched worker-side by column name. Same mechanism as
                :meth:`table_function`'s `join_keys`.
            settings: Optional dictionary of settings/pragmas to
                pass to the function.
            secrets: Optional dictionary of secret name to value pairs.
                Values can be simple scalars or dicts (for struct-typed secrets).
            transaction_opaque_data: Optional unique identifier for the DuckDB transaction.
            parent_row_callback: Optional callback invoked once per yielded
                output batch (before finalize), immediately before the yield,
                with that batch's decoded `vgi_rpc.parent_row` provenance —
                `parent_rows[i]` is the 0-based index into the input batch
                that produced output row `i`. Passing this switches on
                provenance decoding: a batch with no `vgi_rpc.parent_row`
                metadata is only accepted when its row count matches the
                input batch's (raising `ClientError` otherwise), since a
                worker changing row count without provenance is a worker bug
                for a function that opted into this contract. Intended for
                blended row-transform functions (`RowTransformFunction`,
                `FunctionInfo.input_from_args`); leave unset for ordinary
                table-in-out functions, which have no provenance concept and
                may legitimately change row count.
            has_finalize: Whether this function declares a FINALIZE stage
                (`FunctionInfo.has_finalize`). Defaults to `True` — every
                caller before this parameter existed got a FINALIZE-phase
                `init()` unconditionally, so this preserves that exactly.
                Pass `False` for a function known to have no finalize (every
                blended `RowTransformFunction` — `has_finalize` is always
                false for those, enforced at `resolve_metadata()`) to skip
                the FINALIZE `init()` entirely, not just send one expecting
                an empty reply. Confirmed load-bearing, not cosmetic: some
                worker SDKs (e.g. the TypeScript one) actively reject an
                unexpected FINALIZE `init()` for a function that never
                advertised `has_finalize`, rather than silently no-op'ing
                it — the C++ DuckDB extension avoids this the same way, by
                conditionally registering `in_out_function_final` at all.

        Yields:
            Output `RecordBatch`es from the function. In single-worker mode, output
            order corresponds to input order. In parallel mode (max_workers > 1),
            output order is non-deterministic due to round-robin distribution.
            Final output from finalize is always yielded last.

        Raises:
            `ClientError`: If the client is not started, input iterator is empty,
                input iterator yields non-`RecordBatch` objects, communication
                with the worker fails, or the worker returns an unexpected
                status or exception.

        """
        if arguments is None:
            arguments = Arguments()

        if self._primary is None:
            raise ClientError("Client not started. Call start() or use context manager.")

        try:
            # Get the first batch to determine schema and initialize
            for first_batch in input:
                if not isinstance(first_batch, pa.RecordBatch):
                    raise ClientError("Input iterator must yield RecordBatches")

                input_schema = first_batch.schema
                pushdown_filters_batch = self._deserialize_pushdown_filters(pushdown_filters)

                bind_request, bind_response, init_response = self._initialize_stream_common(
                    function_name=function_name,
                    schema_name=schema_name,
                    arguments=arguments,
                    function_type=FunctionType.TABLE,
                    input_schema=input_schema,
                    settings=settings,
                    secrets=secrets,
                    transaction_opaque_data=transaction_opaque_data,
                    projection_ids=projection_ids,
                    pushdown_filters_batch=pushdown_filters_batch,
                    phase=TableInOutFunctionInitPhase.INPUT,
                    bind_result_callback=bind_result_callback,
                    join_keys=join_keys,
                )

                # Process input batches across all workers
                all_workers = [self._primary] + self._additional_workers
                yield from self._distribute_and_collect(
                    all_workers=all_workers,
                    first_batch=first_batch,
                    remaining_input=input,
                    decode_parent_rows=parent_row_callback is not None,
                    parent_row_callback=parent_row_callback,
                )

                # Close all input streams
                for worker in all_workers:
                    if worker.stream is not None:
                        worker.stream.close()
                        worker.stream = None

                # Close secondary workers
                self._close_secondary_workers()

                # Finalize on primary worker — skipped entirely (not just
                # sent expecting an empty reply) when the function has no
                # finalize stage. See has_finalize's docstring: some worker
                # SDKs reject an unadvertised FINALIZE init() outright.
                if not has_finalize:
                    _logger.debug("skipping_finalize_no_finalize_stage")
                    return

                _logger.debug("finalizing_primary_worker")
                yield from self._finalize_primary_worker(
                    bind_request,
                    bind_response,
                    input_schema,
                    init_response,
                )
                _logger.debug("parallel_processing_complete")
                return

            # Input iterator was empty - table-in-out functions require input
            raise ClientError(
                f"table_in_out_function requires at least one input batch. "
                f"The input iterator for function '{function_name}' was empty. "
                f"Use table_function() for functions that generate data without input."
            )
        except ClientError as e:
            raise self._client_error_with_stderr(e) from e.__cause__

    def table_buffering_function(
        self,
        *,
        function_name: str,
        schema_name: str,
        input: Iterator[pa.RecordBatch],
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[BindResponse], None] | None = None,
        projection_ids: list[int] | None = None,
        pushdown_filters: bytes | None = None,
        settings: dict[str, Any] | None = None,
        secrets: dict[str, Any] | None = None,
        transaction_opaque_data: bytes | None = None,
        copy_to: CopyToContext | None = None,
        input_schema: pa.Schema | None = None,
    ) -> Generator[pa.RecordBatch]:
        """Invoke a ``TableBufferingFunction`` (Sink+Source) and stream results.

        This mirrors the C++ ``PhysicalVgiTableBufferingFunction`` operator
        rather than the streaming INPUT/FINALIZE path used by
        :meth:`table_in_out_function`. The sequence is:

        1. ``bind`` → ``init(phase=TABLE_BUFFERING)`` on the primary worker.
           The sink init persists init metadata to cold storage so any pool
           worker can serve subsequent process/combine RPCs; its stream
           carries no data, so it is closed immediately after the header.
        2. ``table_buffering_process`` (unary) per input batch — the worker
           sinks the batch and returns an opaque ``state_id``.
        3. ``table_buffering_combine`` (unary) once at end-of-input — the
           worker hands all ``state_id``s to user ``combine()`` and returns
           opaque ``finalize_state_id``s (the source-side partition keys).
        4. ``init(phase=TABLE_BUFFERING_FINALIZE, finalize_state_id=...)`` per
           finalize key — a producer stream driving user ``finalize()`` per
           tick. Output batches are yielded in finalize-key order.
        5. ``table_buffering_destructor`` (unary, best-effort) for cleanup.

        Unlike :meth:`table_in_out_function` this driver runs entirely on the
        primary worker connection (process/combine are unary RPCs); the
        worker buffers all input regardless, so the aggregate result is
        identical to the distributed C++ path.

        Args:
            function_name: Name of the ``TableBufferingFunction`` to invoke.
            schema_name: Name of the catalog schema that declares the function.
                Required — a worker may register one name in several schemas, so
                the (schema, name) pair is what identifies the implementation.
            input: Iterator yielding input `RecordBatch`es. May be empty —
                buffering aggregations still produce a result for zero rows.
            arguments: Optional [`Arguments`][] container. Defaults to empty.
            bind_result_callback: Optional callback invoked with the
                [`BindResponse`][] before processing begins.
            projection_ids: Optional column indices for projection.
            pushdown_filters: Optional serialized filter predicates.
            settings: Optional settings/pragmas to pass to the function.
            secrets: Optional dictionary of secret name to value pairs.
            transaction_opaque_data: Optional DuckDB transaction identifier.
            copy_to: Optional [`CopyToContext`][] marking this sink as a
                ``COPY ... TO`` write. A ``CopyToFunction`` returns no finalize
                keys, so the generator yields nothing. Prefer :meth:`copy_to`,
                which builds the context and drains for you.
            input_schema: Schema to bind with instead of the first input
                batch's. Needed when ``input`` may be empty and the function
                still depends on the source schema (a ``CopyToFunction``
                writing a header row for an empty COPY).

        Yields:
            Output `RecordBatch`es produced by the finalize (source) phase.

        Raises:
            [`ClientError`][]: If the client is not started or any RPC fails.

        """
        if arguments is None:
            arguments = Arguments()

        if self._primary is None:
            raise ClientError("Client not started. Call start() or use context manager.")

        proxy = self._primary.proxy
        attach = self._attach_opaque_data
        pushdown_filters_batch = self._deserialize_pushdown_filters(pushdown_filters)

        try:
            # Peek the first batch to learn the input schema for bind/init.
            first_batch: pa.RecordBatch | None = None
            for batch in input:
                if not isinstance(batch, pa.RecordBatch):
                    raise ClientError("Input iterator must yield RecordBatches")
                first_batch = batch
                break

            if input_schema is None and first_batch is not None:
                input_schema = first_batch.schema

            bind_request = self._make_bind_request(
                function_name=function_name,
                schema_name=schema_name,
                arguments=arguments,
                function_type=FunctionType.TABLE_BUFFERING,
                input_schema=input_schema,
                settings=settings,
                secrets=secrets,
                transaction_opaque_data=transaction_opaque_data,
                copy_to=copy_to,
            )
            bind_response = self._do_bind(proxy, bind_request, bind_result_callback)

            # Sink init: persists init metadata; the stream carries no data.
            sink_stream = self._do_init(
                proxy,
                bind_request,
                bind_response,
                projection_ids=projection_ids,
                pushdown_filters_batch=pushdown_filters_batch,
                phase=TableInOutFunctionInitPhase.TABLE_BUFFERING,
            )
            init_response = sink_stream.typed_header(GlobalInitResponse)
            sink_stream.close()
            execution_id = init_response.execution_id

            try:
                # Sink each input batch via the unary process RPC.
                state_ids: list[bytes] = []
                remaining: Iterator[pa.RecordBatch] = (
                    itertools.chain([first_batch], input) if first_batch is not None else iter(())
                )
                for batch_index, batch in enumerate(remaining):
                    if not isinstance(batch, pa.RecordBatch):
                        raise ClientError("Input iterator must yield RecordBatches")
                    try:
                        process_response = proxy.table_buffering_process(
                            request=TableBufferingProcessRequest(
                                function_name=function_name,
                                execution_id=execution_id,
                                input_batch=self._serialize_batch_ipc(batch),
                                attach_opaque_data=attach,
                                transaction_id=transaction_opaque_data,
                                batch_index=batch_index,
                            )
                        )
                    except RpcError as e:
                        raise ClientError.from_rpc_error(e) from e
                    state_ids.append(process_response.state_id)

                # End-of-input: combine → finalize partition keys.
                try:
                    combine_response = proxy.table_buffering_combine(
                        request=TableBufferingCombineRequest(
                            function_name=function_name,
                            execution_id=execution_id,
                            state_ids=state_ids,
                            attach_opaque_data=attach,
                            transaction_id=transaction_opaque_data,
                        )
                    )
                except RpcError as e:
                    raise ClientError.from_rpc_error(e) from e

                # Source: one producer stream per finalize partition key.
                for finalize_state_id in combine_response.finalize_state_ids:
                    finalize_stream = self._do_init(
                        proxy,
                        bind_request,
                        bind_response,
                        projection_ids=projection_ids,
                        pushdown_filters_batch=pushdown_filters_batch,
                        phase=TableInOutFunctionInitPhase.TABLE_BUFFERING_FINALIZE,
                        execution_id=execution_id,
                        finalize_state_id=finalize_state_id,
                    )
                    try:
                        while True:
                            try:
                                output = finalize_stream.tick()
                            except StopIteration:
                                break
                            except RpcError as e:
                                raise ClientError.from_rpc_error(e) from e
                            if output.batch.num_rows > 0:
                                yield output.batch
                    finally:
                        finalize_stream.close()
            finally:
                # Best-effort cleanup, mirroring the C++ destructor.
                try:
                    proxy.table_buffering_destructor(
                        request=TableBufferingDestructorRequest(
                            function_name=function_name,
                            execution_id=execution_id,
                            attach_opaque_data=attach,
                            transaction_id=transaction_opaque_data,
                        )
                    )
                except RpcError:
                    _logger.debug("table_buffering_destructor failed (ignored)", exc_info=True)
        except ClientError as e:
            raise self._client_error_with_stderr(e) from e.__cause__

    def _finalize_primary_worker(
        self,
        bind_request: BindRequest,
        bind_response: BindResponse,
        input_schema: pa.Schema,
        init_response: GlobalInitResponse,
    ) -> Generator[pa.RecordBatch]:
        """Send FINALIZE init to the primary worker and collect final output.

        Creates a new init(phase=FINALIZE) stream on the primary worker and
        iterates the producer stream until it finishes.

        Args:
            bind_request: The original bind request.
            bind_response: The bind response with output schema.
            input_schema: Schema of input batches (unused, kept for API compat).
            init_response: The init response from the INPUT phase, providing
                the execution_id needed to access stored worker state.

        Yields:
            Final output `RecordBatch`es from the worker's finalize phase.

        Raises:
            [`ClientError`][]: If the RPC call fails.

        """
        assert self._primary is not None

        # Start FINALIZE stream (producer — uses tick(), not exchange())
        # Pass execution_id from INPUT phase so finalize can access stored state
        finalize_stream = self._do_init(
            self._primary.proxy,
            bind_request,
            bind_response,
            phase=TableInOutFunctionInitPhase.FINALIZE,
            execution_id=init_response.execution_id,
            init_opaque_data=init_response.opaque_data,
        )

        try:
            while True:
                try:
                    output = finalize_stream.tick()
                except StopIteration:
                    break
                except RpcError as e:
                    raise ClientError.from_rpc_error(e) from e

                _logger.debug("received_finalize_from_worker num_rows=%s", output.batch.num_rows)

                if output.batch.num_rows > 0:
                    yield output.batch
        finally:
            finalize_stream.close()

    def bind(
        self,
        *,
        function_name: str,
        schema_name: str,
        arguments: Arguments | None = None,
        function_type: FunctionType = FunctionType.TABLE,
        settings: dict[str, Any] | None = None,
        transaction_opaque_data: bytes | None = None,
    ) -> BindResponse:
        """Resolve a function's bind response without running init()/process().

        Runs only the `bind()` RPC — no `init()`, no worker execution, no
        data produced. This is the schema-discovery primitive `table_function()`/
        `table_in_out_function()`/`scalar_function()` lack a standalone version
        of: their own `bind_result_callback` only fires as a side effect of a
        generator's first `next()`, which has already started `init()` and
        real execution by the time it runs. Prefer the catalog RPCs
        (`Client.table_get`, `Client.schema_contents(type=TABLE_FUNCTION)`)
        when a catalog attach is available — those are equally zero-execution
        and additionally expose pushdown-capability flags this method does
        not. Use this method for a bare (non-catalog) function name, where no
        attach exists to ask instead.

        Args:
            function_name: Name of the function to bind. Must exist in the
                worker's registry.
            schema_name: Name of the catalog schema that declares the
                function. Required — a worker may register one name in
                several schemas, so the (schema, name) pair is what
                identifies the implementation.
            arguments: Optional [`Arguments`][] container with positional and
                named arguments to pass to the function. Defaults to empty
                `Arguments()`.
            function_type: Which kind of function to bind
                (`FunctionType.TABLE`, `.SCALAR`, or `.TABLE_IN_OUT`).
                Defaults to `TABLE`, the common case for schema discovery.
            settings: Optional dictionary of settings/pragmas — some
                functions' output schema depends on setting values (see
                `Meta.required_settings`).
            transaction_opaque_data: Optional unique identifier for the
                DuckDB transaction.

        Returns:
            [`BindResponse`][] with `output_schema` and any opaque bind data —
            no batches, no worker state beyond the bind itself.

        Raises:
            [`ClientError`][]: If the client is not started, communication
                with the worker fails, or the worker returns an exception.

        """
        if arguments is None:
            arguments = Arguments()

        if self._primary is None:
            raise ClientError("Client not started. Call start() or use context manager.")

        try:
            bind_request = self._make_bind_request(
                function_name=function_name,
                schema_name=schema_name,
                arguments=arguments,
                function_type=function_type,
                settings=settings,
                secrets=None,
                transaction_opaque_data=transaction_opaque_data,
            )
            return self._do_bind(self._primary.proxy, bind_request, None)
        except ClientError as e:
            raise self._client_error_with_stderr(e) from e.__cause__

    def table_function_plan(
        self,
        *,
        function_name: str,
        schema_name: str,
        arguments: Arguments | None = None,
        projection_ids: list[int] | None = None,
        pushdown_filters: bytes | None = None,
        join_keys: list[pa.RecordBatch] | None = None,
        settings: dict[str, Any] | None = None,
        secrets: dict[str, Any] | None = None,
        transaction_opaque_data: bytes | None = None,
        target_split_bytes: int | None = None,
        min_splits: int | None = None,
        max_splits_per_response: int | None = None,
        cursor: bytes | None = None,
    ) -> PlanResponse:
        """Plan a table-function scan into named, independently redeemable splits.

        Runs `bind()` then `on_plan()` and returns the resulting
        [`PlanResponse`][], whose `splits` are each individually redeemable by
        :meth:`table_function` via its `split_tokens` argument — from this
        process or, since a split names work rather than describing it ("these
        three files at version 47", not "rows 0-999 of whatever this returns
        now"), any other. Workers that don't opt in via `supports_splits`
        (`FunctionInfo.supports_splits`) inherit a framework default (commonly
        one split for the whole scan) — check that flag first if the caller
        cares whether real parallelism/checkpointing is available versus a
        single degenerate split.

        A response's `next_cursors` is normally empty or one entry; more than
        one means the plan is paginated across parallel, disjoint enumeration
        branches — the *caller* is responsible for that disjointness (VGI
        itself does not verify it; see the "Split disjointness is a worker
        contract" note in the VGI extension's own docs). For a single
        sequential caller, following `next_cursors` one at a time (via
        `cursor=`) and concatenating each response's `splits` is always
        correct, whether the plan doled out one cursor or several.

        Args:
            function_name: Name of the table function to plan.
            schema_name: Name of the catalog schema that declares the function.
            arguments: Optional [`Arguments`][] container. Defaults to empty
                `Arguments()`.
            projection_ids: Optional list of column indices for projection —
                threaded into the plan so split sizing can account for it.
            pushdown_filters: Optional byte string of filter predicates,
                same wire format as :meth:`table_function`'s.
            join_keys: Optional serialized join-key batches for semi-join
                pushdown, threaded into split sizing/pruning the same way
                `pushdown_filters` is — same wire mechanism as
                :meth:`table_function`'s `join_keys`.
            settings: Optional dictionary of settings/pragmas.
            secrets: Optional dictionary of secret name to value pairs.
            transaction_opaque_data: Optional transaction identifier.
            target_split_bytes: Requested split size — the primary sizing
                lever; the client can't see per-split cost and will treat
                returned splits as interchangeable units.
            min_splits: Parallelism floor — ask for at least this many splits
                even for a small table, so a caller with idle readers has
                enough units to hand them.
            max_splits_per_response: Pagination cap on this one response
                (distinct from `min_splits`, which is a sizing hint, not a
                pagination control).
            cursor: Resume point from a previous response's `next_cursors`,
                or `None` to start a fresh plan.

        Returns:
            [`PlanResponse`][] with `splits` (each an individually redeemable
            [`ScanSplit`][], carrying the `token` to pass back into
            `table_function(split_tokens=...)`) and `next_cursors` for
            pagination.

        Raises:
            [`ClientError`][]: If the client is not started, communication with
                the worker fails, or the worker returns an exception.

        """
        if arguments is None:
            arguments = Arguments()

        if self._primary is None:
            raise ClientError("Client not started. Call start() or use context manager.")

        try:
            pushdown_filters_batch = self._deserialize_pushdown_filters(pushdown_filters)

            bind_request = self._make_bind_request(
                function_name=function_name,
                schema_name=schema_name,
                arguments=arguments,
                function_type=FunctionType.TABLE,
                settings=settings,
                secrets=secrets,
                transaction_opaque_data=transaction_opaque_data,
            )
            bind_response = self._do_bind(self._primary.proxy, bind_request, None)

            plan_request = TableFunctionPlanRequest(
                bind_call=bind_request,
                bind_opaque_data=bind_response.opaque_data,
                projection_ids=projection_ids,
                pushdown_filters=pushdown_filters_batch,
                join_keys=join_keys,
                target_split_bytes=target_split_bytes,
                min_splits=min_splits,
                max_splits_per_response=max_splits_per_response,
                cursor=cursor,
            )
            try:
                response: PlanResponse = self._primary.proxy.table_function_plan(request=plan_request)
            except RpcError as e:
                raise ClientError.from_rpc_error(e) from e

            # The wire carries each split as a serialized blob in a
            # list<binary> column (see PlanResponse.splits' docstring: "Both
            # [ScanSplit objects and serialized blobs] inhabit this field
            # across its lifetime") — deserialize them here so callers get
            # typed ScanSplit objects (with .token ready to redeem) rather
            # than raw bytes they'd all have to know to decode themselves.
            deserialized_splits = [
                s if isinstance(s, ScanSplit) else ScanSplit.deserialize_from_bytes(s) for s in response.splits
            ]
            return replace(response, splits=deserialized_splits)
        except ClientError as e:
            raise self._client_error_with_stderr(e) from e.__cause__

    def table_function(
        self,
        *,
        function_name: str,
        schema_name: str,
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[BindResponse], None] | None = None,
        projection_ids: list[int] | None = None,
        pushdown_filters: bytes | None = None,
        join_keys: list[pa.RecordBatch] | None = None,
        settings: dict[str, Any] | None = None,
        secrets: dict[str, Any] | None = None,
        transaction_opaque_data: bytes | None = None,
        copy_from: CopyFromContext | None = None,
        split_tokens: list[bytes] | None = None,
        split_execution_id: bytes | None = None,
        split_init_opaque_data: bytes | None = None,
        batch_metadata_callback: Callable[[pa.KeyValueMetadata | None], None] | None = None,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> Generator[pa.RecordBatch]:
        """Invoke a table function (source function) and stream output batches.

        Table functions generate output batches without receiving input data.
        They are useful for data sources, generators, or functions that produce
        results based solely on their arguments.

        For parallel processing (max_workers > 1), output is read from all
        workers concurrently using threads. Output order is non-deterministic.
        This is unrelated to (and mutually exclusive in effect with) redeeming
        splits — see ``split_tokens`` below.

        Args:
            function_name: Name of the function to invoke. Must exist in the
                worker's registry and be a table function (not table-in-out).
            schema_name: Name of the catalog schema that declares the function.
                Required — a worker may register one name in several schemas, so
                the (schema, name) pair is what identifies the implementation.
            arguments: Optional [`Arguments`][] container with positional and named
                arguments to pass to the function. Defaults to empty `Arguments()`.
            bind_result_callback: Optional callback invoked with the [`BindResponse`][]
                before processing begins.
            projection_ids: Optional list of column indices for column projection.
            pushdown_filters: Optional byte string containing filter predicates
                to push down to the function.
            join_keys: Optional serialized join-key batches for semi-join
                pushdown — one single-column `RecordBatch` per join-key
                column, matched worker-side by column name (see
                `PushdownFilters.get_join_keys_column`). Same wire mechanism
                DuckDB's own join pushdown into VGI already exercises;
                `Client` simply had no public way to set it before this.
            settings: Optional dictionary of settings/pragmas to
                pass to the function.
            secrets: Optional dictionary of secret name to value pairs.
            transaction_opaque_data: Optional unique identifier for the DuckDB transaction.
            copy_from: Optional [`CopyFromContext`][] marking this scan as a
                ``COPY ... FROM`` read. Prefer :meth:`copy_from`, which builds
                the context for you.
            split_tokens: Redeem these specific split tokens (from a prior
                :meth:`table_function_plan` call) instead of an ordinary
                whole-scan init. Forces single-worker mode for this call — the
                server's advertised ``max_workers`` describes fan-out for
                reading the whole table, not for one already-named unit of
                work. To read multiple splits in parallel, drive them through
                multiple `Client`/thread instances yourself; to read them
                sequentially (still sound and replayable, just without
                concurrency), call this once per split token in a loop.
            split_execution_id: When redeeming a split, the originating
                ``PlanResponse.execution_id`` — echoed on this init so a
                worker whose splits share cross-process state via
                ``BoundStorage`` can find it. `None` for an ordinary
                whole-scan init.
            split_init_opaque_data: When redeeming a split, the originating
                ``PlanResponse.init_opaque_data``, echoed the same way.
            batch_metadata_callback: Optional callback invoked once per yielded
                batch, before it's yielded, with that batch's ``custom_metadata``
                (``None`` if it carried none) — e.g. a worker's ``vgi.cache.*``
                cacheability advertisement (``vgi/cache_control.py``), which
                rides ``AnnotatedBatch.custom_metadata`` and is otherwise
                unreachable through this generator's plain ``pa.RecordBatch``
                yields. Invoked serially from the generator's own consumption
                loop (even in parallel mode, where multiple worker threads
                feed one shared queue) — never concurrently with itself.
            at_unit: Optional time travel unit (e.g. 'timestamp', 'version') —
                scan the table as of a past point rather than live. `None`
                for a live scan. Threaded straight into `BindRequest.at_unit`
                (the wire protocol has always carried this field; a worker
                that doesn't support time travel on this function rejects it
                at bind, the same as any other unsupported bind option).
            at_value: Optional time travel value, paired with `at_unit`.

        Yields:
            Output `RecordBatch`es from the function. In parallel mode
            (max_workers > 1, only possible when ``split_tokens`` is `None`),
            output order is non-deterministic.

        Raises:
            [`ClientError`][]: If the client is not started, communication with the
                worker fails, or the worker returns an exception.

        """
        if arguments is None:
            arguments = Arguments()

        if self._primary is None:
            raise ClientError("Client not started. Call start() or use context manager.")

        try:
            pushdown_filters_batch = self._deserialize_pushdown_filters(pushdown_filters)

            self._initialize_stream_common(
                function_name=function_name,
                schema_name=schema_name,
                arguments=arguments,
                function_type=FunctionType.TABLE,
                input_schema=None,
                settings=settings,
                secrets=secrets,
                transaction_opaque_data=transaction_opaque_data,
                projection_ids=projection_ids,
                pushdown_filters_batch=pushdown_filters_batch,
                phase=None,
                bind_result_callback=bind_result_callback,
                copy_from=copy_from,
                split_tokens=split_tokens,
                split_execution_id=split_execution_id,
                split_init_opaque_data=split_init_opaque_data,
                join_keys=join_keys,
                at_unit=at_unit,
                at_value=at_value,
            )

            # Read output from all workers in parallel
            yield from self._table_function_parallel(batch_metadata_callback=batch_metadata_callback)
        except ClientError as e:
            raise self._client_error_with_stderr(e) from e.__cause__

    @property
    def supports_resumable_scan(self) -> bool:
        """Whether this client's transport can drive :meth:`table_scan_resumable`.

        True only for HTTP, whose producer streams round-trip state in
        continuation tokens. The pipe/subprocess transport holds a live stream
        with no serializable resume point.
        """
        return self._transport in ("http", "httpi")

    def table_scan_resumable(
        self,
        *,
        function_name: str,
        schema_name: str,
        arguments: Arguments | None = None,
        projection_ids: list[int] | None = None,
        pushdown_filters: bytes | None = None,
        settings: dict[str, Any] | None = None,
        secrets: dict[str, Any] | None = None,
        transaction_opaque_data: bytes | None = None,
        resume_token: bytes | None = None,
    ) -> ResumableTableScan:
        """Open (or resume) a resumable table-function scan.

        Resumable variant of :meth:`table_function`: the returned
        :class:[`ResumableTableScan`][] yields ``(batch, token)`` one batch at a
        time, surfacing the worker's continuation token so a stateless caller
        can persist it and resume on another process/node.

        When ``resume_token`` is given, the scan continues from that token
        (the bind/init is still issued — the upstream's first turn is produced
        and discarded — so the same ``function_name``/projection/filters must
        be supplied). When ``None``, a fresh scan starts.

        Args:
            function_name: Name of the table function to scan.
            schema_name: Name of the catalog schema that declares the function.
                Required — a worker may register one name in several schemas, so
                the (schema, name) pair is what identifies the implementation.
            arguments: Positional/named arguments for the function's bind.
            projection_ids: Optional column indices to project (projection
                pushdown). ``None`` selects all columns.
            pushdown_filters: Optional serialized filter-pushdown payload.
            settings: Optional DuckDB settings to apply for the scan.
            secrets: Optional dictionary of secret name to value pairs.
            transaction_opaque_data: Optional catalog transaction handle.
            resume_token: Continuation token from a prior batch to resume from;
                must be paired with the same ``function_name``/projection/
                filters. ``None`` starts a fresh scan.

        Returns:
            A [`ResumableTableScan`][] yielding ``(batch, token)`` pairs.

        Raises:
            [`ResumeUnsupported`][]: If the transport is not HTTP.
            [`ClientError`][]: If the client is not started or the worker errors.

        """
        if not self.supports_resumable_scan:
            raise ResumeUnsupported(
                f"table_scan_resumable requires the HTTP transport; this client uses {self._transport!r}. "
                "Use table_function() and keep the live stream in-process."
            )
        if arguments is None:
            arguments = Arguments()
        if self._primary is None:
            raise ClientError("Client not started. Call start() or use context manager.")

        try:
            pushdown_filters_batch = self._deserialize_pushdown_filters(pushdown_filters)
            # Bind + init against the PRIMARY only — a resumable scan is single-worker
            # (parallel max_workers>1 reads are unordered and not token-resumable).
            bind_request = self._make_bind_request(
                function_name=function_name,
                schema_name=schema_name,
                arguments=arguments,
                function_type=FunctionType.TABLE,
                input_schema=None,
                settings=settings,
                secrets=secrets,
                transaction_opaque_data=transaction_opaque_data,
            )
            bind_response = self._do_bind(self._primary.proxy, bind_request, None)
            stream = self._do_init(
                self._primary.proxy,
                bind_request,
                bind_response,
                projection_ids=projection_ids,
                pushdown_filters_batch=pushdown_filters_batch,
                phase=None,
            )
            self._primary.stream = stream
            stream.typed_header(GlobalInitResponse)  # consume the init header
            if resume_token is not None:
                # Discard the freshly-produced first turn and continue from the token.
                stream.seek_to_token(resume_token)  # type: ignore[attr-defined]
            return ResumableTableScan(self, stream)
        except ClientError as e:
            raise self._client_error_with_stderr(e) from e.__cause__

    def table_scan_continue(
        self,
        *,
        resume_token: bytes,
        output_schema: pa.Schema | None = None,
    ) -> ResumableTableScan:
        """Resume a producer table scan from a continuation token WITHOUT re-binding.

        The cheap counterpart to ``table_scan_resumable(resume_token=...)``: a continuation
        token is a signed, self-describing snapshot of the worker's producer state, so the
        server recovers state + schemas + function identity from the token alone. This skips
        the ``bind``/``init`` round-trip (and the discarded first turn) that
        ``table_scan_resumable`` pays — the right primitive for a stateless relay that holds
        a per-batch token and resumes on any node every batch.

        The client must be started and connected to a worker that honours the token (the
        token is verified against the caller's auth identity, and routed by the same
        ``init`` stream method that minted it). HTTP transport only.

        Args:
            resume_token: A token previously returned by ``ResumableTableScan.next()``.
            output_schema: Unused on the producer-continuation path (each response carries
                its own schema); accepted for symmetry with ``table_scan_resumable``.

        Returns:
            A `[`ResumableTableScan`][]` positioned AFTER the token; ``next()`` continues the
            stream, yielding ``(batch, token)`` per call.

        Raises:
            [`ResumeUnsupported`][]: If the transport is not HTTP.
            [`ClientError`][]: If the client is not started.

        """
        if not self.supports_resumable_scan:
            raise ResumeUnsupported(
                f"table_scan_continue requires the HTTP transport; this client uses {self._transport!r}. "
                "Use table_function() and keep the live stream in-process."
            )
        if self._primary is None:
            raise ClientError("Client not started. Call start() or use context manager.")
        try:
            # ``init`` is the VgiProtocol stream method that mints producer continuation
            # tokens; continuations resume at ``POST /init/exchange``. The server is
            # stateless per token, so no prior bind/init on this connection is required.
            stream = self._primary.proxy.resume_stream(  # type: ignore[attr-defined]
                "init", resume_token, output_schema=output_schema
            )
            self._primary.stream = stream
            return ResumableTableScan(self, stream)
        except ClientError as e:
            raise self._client_error_with_stderr(e) from e.__cause__

    def _table_function_parallel(
        self,
        *,
        batch_metadata_callback: Callable[[pa.KeyValueMetadata | None], None] | None = None,
    ) -> Generator[pa.RecordBatch]:
        """Read output from table function workers using parallel threads.

        Handles both single-worker and multi-worker cases uniformly. For each
        worker, spawns a dedicated thread that reads output batches and pushes
        them to a shared output queue.

        Args:
            batch_metadata_callback: Optional callback invoked once per yielded
                batch, before it's yielded, with that batch's `custom_metadata`
                (`None` if it carried none) — e.g. `vgi.cache.*` cacheability
                metadata (`vgi/cache_control.py`), otherwise unreachable through
                this generator's plain `pa.RecordBatch` yields.

        Yields:
            Output `RecordBatch`es from all workers in non-deterministic order.

        Raises:
            [`ClientError`][]: If a worker thread fails with an exception.

        """
        assert self._primary is not None
        all_workers = [self._primary] + self._additional_workers
        num_workers = len(all_workers)

        _logger.debug("starting_parallel_table_function num_workers=%s", num_workers)

        # Queue for collecting output from all workers. Carries the full
        # AnnotatedBatch (not just .batch) so custom_metadata survives the
        # cross-thread handoff for batch_metadata_callback below.
        output_queue: Queue[AnnotatedBatch | BaseException | None] = Queue()

        def read_worker_output(worker: WorkerConnection) -> None:
            """Thread function that reads all output from a single worker."""
            try:
                if worker.stream is None:
                    output_queue.put(None)
                    return

                for output in worker.stream:
                    _logger.debug(
                        "received_output_from_worker worker_index=%s num_rows=%s",
                        worker.worker_index,
                        output.batch.num_rows,
                    )
                    if output.batch.num_rows > 0:
                        output_queue.put(output)

                output_queue.put(None)  # Signal completion
            except StopIteration:
                output_queue.put(None)
            except Exception as e:
                _logger.exception("table_function_worker_thread_error worker_index=%s", worker.worker_index)
                output_queue.put(e)

        # Start reader threads for all workers
        threads: list[threading.Thread] = []
        for worker in all_workers:
            thread = threading.Thread(
                target=read_worker_output,
                args=(worker,),
                daemon=True,
            )
            thread.start()
            threads.append(thread)

        # Collect outputs from all workers until all are done
        workers_finished = 0
        while workers_finished < num_workers:
            result = output_queue.get()

            # Check for exceptions from worker threads
            if isinstance(result, BaseException):
                if isinstance(result, RpcError):
                    raise ClientError.from_rpc_error(result) from result
                raise ClientError(f"Worker thread failed: {result}") from result

            # None signals a worker finished
            if result is None:
                workers_finished += 1
                _logger.debug(
                    "worker_finished workers_finished=%s total_workers=%s",
                    workers_finished,
                    num_workers,
                )
                continue

            if batch_metadata_callback is not None:
                batch_metadata_callback(result.custom_metadata)
            yield result.batch

        self._join_threads(threads)
        _logger.debug("all_table_function_workers_complete")

        # Close streams and secondary workers
        for worker in all_workers:
            if worker.stream is not None:
                worker.stream.close()
                worker.stream = None
        self._close_secondary_workers()
        _logger.debug("parallel_table_function_complete")

    # ==================================================================
    # Custom COPY formats
    # ==================================================================

    def copy_from(
        self,
        *,
        function_name: str,
        schema_name: str,
        format: str,
        file_path: str,
        expected_schema: pa.Schema,
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[BindResponse], None] | None = None,
        projection_ids: list[int] | None = None,
        pushdown_filters: bytes | None = None,
        settings: dict[str, Any] | None = None,
        transaction_opaque_data: bytes | None = None,
    ) -> Generator[pa.RecordBatch]:
        """Read a custom ``COPY ... FROM`` format and stream the parsed rows.

        A ``CopyFromFunction`` is an ordinary producer-mode table function that
        additionally receives a [`CopyFromContext`][], so this is
        :meth:`table_function` with that context attached — the same shape the
        C++ extension's ``copy_from_bind`` produces.

        Discover the ``(format, handler)`` pairs a catalog advertises with
        ``client.copy_formats(attach_opaque_data=...)``; ``handler`` is the
        ``function_name`` to pass here.

        Args:
            function_name: The reader function (a ``CopyFromFunction``) — the
                ``handler`` field of the advertised format.
            schema_name: Catalog schema that declares the function.
            format: The SQL ``FORMAT`` identifier the read is running under.
            file_path: Source path from the ``COPY ... FROM 'path'`` statement.
            expected_schema: Schema of the COPY target's columns, in target
                order. The reader must emit batches matching it exactly —
                DuckDB inserts no cast between the scan and the INSERT.
            arguments: The COPY options, as named [`Arguments`][]. Defaults to
                empty, which is valid only when every option has a default.
            bind_result_callback: Optional callback invoked with the
                [`BindResponse`][] before reading begins.
            projection_ids: Optional column indices for projection.
            pushdown_filters: Optional serialized filter predicates.
            settings: Optional settings/pragmas to pass to the function.
            transaction_opaque_data: Optional DuckDB transaction identifier.

        Yields:
            Parsed `RecordBatch`es, each matching ``expected_schema``.

        Raises:
            [`ClientError`][]: If the client is not started or an RPC fails.

        """
        yield from self.table_function(
            function_name=function_name,
            schema_name=schema_name,
            arguments=arguments,
            bind_result_callback=bind_result_callback,
            projection_ids=projection_ids,
            pushdown_filters=pushdown_filters,
            settings=settings,
            transaction_opaque_data=transaction_opaque_data,
            copy_from=CopyFromContext(
                format=format,
                file_path=file_path,
                expected_schema=expected_schema,
            ),
        )

    def copy_to(
        self,
        *,
        function_name: str,
        schema_name: str,
        format: str,
        file_path: str,
        input: Iterator[pa.RecordBatch],
        input_schema: pa.Schema | None = None,
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[BindResponse], None] | None = None,
        settings: dict[str, Any] | None = None,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Write ``input`` to a custom ``COPY ... TO`` format and close it.

        A ``CopyToFunction`` is a buffered Sink+Combine function with no Source
        phase, so this is :meth:`table_buffering_function` with a
        [`CopyToContext`][] attached: every batch is sunk via
        ``table_buffering_process`` and the terminal write happens once inside
        ``table_buffering_combine``. Returns when the destination is closed.

        Unlike the C++ path this drives a single worker connection, so ordered
        writers (``Meta.sink_order_dependent``) see source order for free.

        Args:
            function_name: The writer function (a ``CopyToFunction``) — the
                ``handler`` field of the advertised format.
            schema_name: Catalog schema that declares the function.
            format: The SQL ``FORMAT`` identifier the write is running under.
            file_path: Destination path from the ``COPY ... TO 'path'``
                statement.
            input: Iterator of source batches. May be empty — the writer's
                ``close()`` still runs and must produce an empty destination.
            input_schema: Source schema to bind with. Required when ``input``
                may be empty and the writer needs the source column names.
            arguments: The COPY options, as named [`Arguments`][]. Defaults to
                empty, which is valid only when every option has a default.
            bind_result_callback: Optional callback invoked with the
                [`BindResponse`][] before writing begins.
            settings: Optional settings/pragmas to pass to the function.
            transaction_opaque_data: Optional DuckDB transaction identifier.

        Raises:
            [`ClientError`][]: If the client is not started or an RPC fails.

        """
        for batch in self.table_buffering_function(
            function_name=function_name,
            schema_name=schema_name,
            input=input,
            arguments=arguments,
            bind_result_callback=bind_result_callback,
            settings=settings,
            transaction_opaque_data=transaction_opaque_data,
            input_schema=input_schema,
            copy_to=CopyToContext(format=format, file_path=file_path),
        ):
            # A CopyToFunction's combine() returns no finalize keys, so the
            # buffered driver has nothing to drain. Anything arriving here means
            # the named function is not a COPY-TO writer.
            raise ClientError(
                f"COPY TO handler '{function_name}' produced {batch.num_rows} output rows; "
                f"a CopyToFunction has no Source phase."
            )

    def scalar_function(
        self,
        *,
        function_name: str,
        schema_name: str,
        input: Iterator[pa.RecordBatch],
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[BindResponse], None] | None = None,
        settings: dict[str, Any] | None = None,
        secrets: dict[str, Any] | None = None,
        transaction_opaque_data: bytes | None = None,
    ) -> Generator[pa.RecordBatch]:
        """Invoke a scalar function on the worker and stream results.

        Scalar functions transform input batches to single-column output with
        1:1 row mapping. Processing ends when input is exhausted.

        For parallel processing (max_workers > 1), input batches are distributed
        round-robin across workers using dedicated threads. Output order may not
        match input order in parallel mode.

        Args:
            function_name: Name of the function to invoke. Must exist in the
                worker's registry.
            schema_name: Name of the catalog schema that declares the function.
                Required — a worker may register one name in several schemas, so
                the (schema, name) pair is what identifies the implementation.
            input: Iterator yielding input `RecordBatch`es. Must yield at least one
                batch. The first batch's schema is used to initialize the IPC
                stream. Raises [`ClientError`][] if the iterator is empty.
            arguments: Optional [`Arguments`][] container with positional and named
                arguments to pass to the function. Defaults to empty `Arguments()`.
            bind_result_callback: Optional callback invoked with the [`BindResponse`][]
                before processing begins.
            settings: Optional dictionary of settings/pragmas to
                pass to the function.
            secrets: Optional dictionary of secret name to value pairs.
                Values can be simple scalars or dicts (for struct-typed secrets).
            transaction_opaque_data: Optional unique identifier for the DuckDB transaction.

        Yields:
            Output `RecordBatch`es from the function. Each output batch has a single
            column and the same number of rows as its corresponding input batch.
            In single-worker mode, output order corresponds to input order.
            In parallel mode (max_workers > 1), output order is non-deterministic.

        Raises:
            `ClientError`: If the client is not started, input iterator is empty,
                input iterator yields non-`RecordBatch` objects, communication
                with the worker fails, or the worker returns an unexpected
                status or exception.

        """
        if arguments is None:
            arguments = Arguments()

        if self._primary is None:
            raise ClientError("Client not started. Call start() or use context manager.")

        try:
            # Get the first batch to determine schema and initialize
            for first_batch in input:
                if not isinstance(first_batch, pa.RecordBatch):
                    raise ClientError("Input iterator must yield RecordBatches")

                input_schema = first_batch.schema

                self._initialize_stream_common(
                    function_name=function_name,
                    schema_name=schema_name,
                    arguments=arguments,
                    function_type=FunctionType.SCALAR,
                    input_schema=input_schema,
                    settings=settings,
                    secrets=secrets,
                    transaction_opaque_data=transaction_opaque_data,
                    projection_ids=None,
                    pushdown_filters_batch=None,
                    phase=None,
                    bind_result_callback=bind_result_callback,
                )

                # Process batches across all workers
                all_workers = [self._primary] + self._additional_workers
                yield from self._distribute_and_collect(
                    all_workers=all_workers,
                    first_batch=first_batch,
                    remaining_input=input,
                )

                # Close streams and secondary workers
                for worker in all_workers:
                    if worker.stream is not None:
                        worker.stream.close()
                        worker.stream = None
                self._close_secondary_workers()
                return

            # Input iterator was empty - scalar functions require input
            raise ClientError(
                f"scalar_function requires at least one input batch. "
                f"The input iterator for function '{function_name}' was empty. "
                f"Use table_function() for functions that generate data without input."
            )
        except ClientError as e:
            raise self._client_error_with_stderr(e) from e.__cause__
