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
    detach         → proxy.catalog_detach(attach_id)

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
    Client             — main entry point; ``Client.from_http(...)`` for HTTP
    ClientError        — raised on communication errors
    WorkerConnection   — internal; one per transport-level connection

Key methods
-----------
    client.catalogs()             — discover catalogs
    client.catalog_attach(...)    — open a catalog
    client.schemas(...)           — list schemas
    client.schema_contents(...)   — list tables/views/functions/macros
    client.scalar_function(...)   — invoke a scalar
    client.table_function(...)    — invoke a table function
    client.table_in_out_function(...) — invoke a table-in-out function
    client.server_capabilities()  — HTTP only; upload-URL caps

See Also
--------
    vgi.protocol.VgiProtocol      — the RPC interface this client exercises
    vgi.protocol.BindRequest      — request types
    vgi.arguments.Arguments       — positional/named argument container
    vgi_rpc.http.http_connect     — transport primitive this client wraps

"""

from __future__ import annotations

import io
import logging
import os
import shlex
import subprocess
import threading
from collections.abc import Callable, Generator, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
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
from vgi.client.catalog_mixin import CatalogClientMixin
from vgi.invocation import (
    BindResponse,
    FunctionType,
    GlobalInitResponse,
)
from vgi.protocol import (
    BindRequest,
    InitRequest,
    VgiProtocol,
)
from vgi.table_function import TableInOutFunctionInitPhase

_logger = logging.getLogger("vgi.client")
_worker_logger = logging.getLogger("vgi.client.worker")


class ClientError(Exception):
    """Error raised by Client operations.

    The first line of ``str(ClientError)`` is the remote exception as the
    worker raised it (``{error_type}: {error_message}``), so that whatever
    a user typed into their `raise ValueError(...)` shows up at the top of
    their traceback instead of being buried under VGI framing. Remote
    traceback and worker-stderr excerpts, when present, follow after an
    empty line.
    """

    @classmethod
    def from_rpc_error(cls, e: RpcError) -> ClientError:
        """Create a ClientError from an RpcError, including remote traceback.

        Lead with the user's exception (``error_type: error_message``) so
        the most actionable line is first. The ``Remote traceback`` section
        trails and is only included when the worker produced one.
        """
        # str(e) is already "error_type: error_message" from RpcError.__init__.
        parts: list[str] = [str(e)]
        if getattr(e, "remote_traceback", ""):
            parts.append(f"Remote traceback:\n{e.remote_traceback}")
        return cls("\n\n".join(parts))


# Module-level worker pool shared across all Client instances.
# Reuses idle worker subprocesses between Client sessions, avoiding
# repeated spawn/teardown overhead (especially valuable in tests).
_default_pool = WorkerPool(max_idle=8, idle_timeout=30.0)

# True once the HTTP transport is wired end-to-end. Used by the
# parametrized ``client_transport`` fixture in tests/conftest.py to decide
# whether to skip the HTTP leg of the matrix.
_HTTP_TRANSPORT_READY = True


@dataclass
class WorkerConnection:
    """Holds state for a single worker connection (subprocess or HTTP).

    Exactly one of {proc+connection, _pool_ctx, _http_ctx} is active per
    connection — transport-specific teardown inspects these fields.
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


class Client(CatalogClientMixin):
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
    provided by ``CatalogClientMixin`` and don't require ``start()``. They
    open a short-lived connection per call (HTTP) or borrow a pooled
    subprocess worker.

    Function invocation (``scalar_function``, ``table_function``,
    ``table_in_out_function``) requires ``start()`` — typically via the
    context-manager protocol::

        with Client.from_http("http://host:port", bearer_token="...") as c:
            for batch in c.table_function(function_name="sequence", ...):
                ...
    """

    # Timeout for thread join operations (seconds)
    THREAD_JOIN_TIMEOUT: float = 5.0

    # Timeout for worker process wait operations (seconds)
    PROCESS_WAIT_TIMEOUT: float = 5.0

    @staticmethod
    def _combine_batches(batches: list[pa.RecordBatch]) -> pa.RecordBatch | None:
        """Combine multiple RecordBatches into a single RecordBatch.

        Converts the batches to a PyArrow Table, combines chunks, and converts
        back to a single batch. When all input batches have zero rows, PyArrow's
        combine_chunks returns an empty list; in that case, the first original
        batch is returned to preserve the schema.

        Args:
            batches: List of RecordBatches to combine. All batches must have
                compatible schemas.

        Returns:
            A single combined RecordBatch, or None if the input list is empty.

        """
        if not batches:
            return None

        combined = list(pa.Table.from_batches(batches).combine_chunks().to_batches())
        # If all batches were empty, combine_chunks returns empty list
        if len(combined) == 0:
            return batches[0]
        return combined[0]

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
        """Convert settings dict to RecordBatch for protocol.

        Args:
            settings: Dictionary of setting name to value pairs.

        Returns:
            A single-row RecordBatch with one column per setting, or None.

        """
        if settings is None:
            return None
        return pa.RecordBatch.from_pydict({k: [v] for k, v in settings.items()})

    @staticmethod
    def _secrets_to_batch(secrets: dict[str, Any] | None) -> pa.RecordBatch | None:
        """Convert secrets dict to RecordBatch for protocol.

        Args:
            secrets: Dictionary of secret name to value pairs. Values can be
                simple scalars or dicts (for struct-typed secrets).

        Returns:
            A single-row RecordBatch with one column per secret, or None.

        """
        if secrets is None:
            return None
        return pa.RecordBatch.from_pydict({k: [v] for k, v in secrets.items()})

    @staticmethod
    def _deserialize_pushdown_filters(filters_bytes: bytes | None) -> pa.RecordBatch | None:
        """Deserialize pushdown filter bytes to RecordBatch.

        Args:
            filters_bytes: IPC-serialized RecordBatch bytes, or None.

        Returns:
            Deserialized RecordBatch, or None.

        """
        if filters_bytes is None:
            return None
        reader = pa.ipc.open_stream(pa.BufferReader(filters_bytes))
        return reader.read_next_batch()

    def __init__(
        self,
        server_path: str | None = None,
        passthrough_stderr: bool = False,
        worker_limit: int | None = None,
        attach_id: bytes | None = None,
        pool: WorkerPool | None = _default_pool,
        *,
        transport: Literal["subprocess", "http"] = "subprocess",
        base_url: str | None = None,
        bearer_token: str | None = None,
        httpx_client: Any | None = None,
        external_location: Any | None = None,
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
            server_path: Subprocess-only. Shell command or path to the VGI
                worker executable. Executed via shell=True.
            passthrough_stderr: Subprocess-only. If True, worker stderr is
                passed through to the parent process's stderr in real-time.
            worker_limit: Maximum number of parallel worker processes.
            attach_id: Optional unique identifier for the DuckDB database
                attachment. When VGI is used from an attached database, this
                allows tracing calls back to that specific attachment.
            pool: Subprocess-only. Optional WorkerPool for subprocess reuse.
                Pass None to disable pooling and use direct subprocess
                management.
            transport: Which transport to use. ``"subprocess"`` (default)
                spawns a local subprocess per worker; ``"http"`` connects to
                a running worker via ``vgi_rpc.http.http_connect``.
            base_url: HTTP-only. Base URL of the running worker, e.g.
                ``"http://127.0.0.1:8765"``.
            bearer_token: HTTP-only. When set, every request carries an
                ``Authorization: Bearer <token>`` header. Static token
                support only — no JWT / OAuth flows.
            httpx_client: HTTP-only escape hatch. When provided, overrides
                ``bearer_token`` and is used verbatim; supply this when you
                need mTLS or a custom auth scheme. Not the canonical path.
            external_location: HTTP-only. ``ExternalLocationConfig`` that
                controls how the client fetches pointer batches (workers
                that externalize large outputs via demo storage / S3 return
                empty batches carrying ``vgi_rpc.location`` metadata).
                Defaults to a vanilla ``ExternalLocationConfig()`` for HTTP
                transport so pointer batches are resolved automatically.
                Subprocess transport ignores this — subprocess workers
                don't return pointer batches.

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
        else:
            raise ValueError(f"unknown transport {transport!r}")

        self.server_path = server_path or ""
        self._transport = transport
        self._base_url = base_url
        self._bearer_token = bearer_token
        self._httpx_client = httpx_client
        # True when ``_get_or_create_httpx_client`` constructed the client and
        # is therefore responsible for closing it on ``stop()``. False when
        # the caller passed ``httpx_client=`` — ownership stays with them.
        self._httpx_client_owned = False
        # Auto-enable pointer-batch resolution for HTTP unless the caller
        # asked for something different. See ``external_location`` docs above.
        if transport == "http" and external_location is None:
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
        self._attach_id = attach_id
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
        httpx_client: Any | None = None,
        external_location: Any | None = None,
        worker_limit: int | None = None,
        attach_id: bytes | None = None,
    ) -> Client:
        """Create a ``Client`` bound to a remote HTTP VGI worker.

        Canonical entry point for non-DuckDB callers (e.g. a TypeScript port
        browsing catalog contents). Subprocess-specific kwargs are not
        accepted; pool/stderr semantics do not apply.
        """
        return cls(
            transport="http",
            base_url=base_url,
            bearer_token=bearer_token,
            httpx_client=httpx_client,
            external_location=external_location,
            worker_limit=worker_limit,
            attach_id=attach_id,
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

    def _client_error_with_stderr(self, error: ClientError) -> ClientError:
        """Enrich a ClientError with captured worker stderr, if available.

        When passthrough_stderr is enabled, stderr already went to the terminal
        so we return the error unchanged. Otherwise we append the last 50 lines
        of captured stderr *after* the existing message — so the user's actual
        exception (first line of ``str(error)``) stays at the top of the
        rendered traceback and operational log noise trails.
        """
        if self.passthrough_stderr:
            return error
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
        return self._spawn_subprocess_connection(worker_index)

    def _spawn_http_connection(self, worker_index: int) -> WorkerConnection:
        """Connect to a remote HTTP worker via ``vgi_rpc.http.http_connect``.

        This is the canonical path non-DuckDB clients implement; subprocess
        is a Python convenience. Multiple ``worker_index`` values map to
        independent RPC proxies against the same shared ``httpx.Client``
        (and therefore the same base URL + auth config).
        """
        from vgi_rpc.http import http_connect

        httpx_client = self._get_or_create_httpx_client()
        ctx: AbstractContextManager[VgiProtocol] = http_connect(
            VgiProtocol,  # type: ignore[type-abstract]
            base_url=self._base_url,
            client=httpx_client,
            on_log=self._on_worker_log,
            external_location=self._external_location,
        )
        proxy = ctx.__enter__()
        _logger.debug("http_connection_opened worker_index=%s base_url=%s", worker_index, self._base_url)
        return WorkerConnection(
            proxy=proxy,
            worker_index=worker_index,
            _http_ctx=ctx,
        )

    def _get_or_create_httpx_client(self) -> Any:
        """Return the shared httpx.Client for this Client's HTTP transport.

        Lazily constructs one bound to ``self._base_url`` (so RPC requests
        resolve against the remote worker) with an ``Authorization: Bearer
        <token>`` header when ``bearer_token`` was supplied. When the
        caller passes ``httpx_client=`` directly, they're responsible for
        configuring ``base_url`` and auth on it — we use it verbatim.
        """
        if self._httpx_client is not None:
            return self._httpx_client

        import httpx

        headers: dict[str, str] = {}
        if self._bearer_token is not None:
            headers["Authorization"] = f"Bearer {self._bearer_token}"
        self._httpx_client = httpx.Client(
            base_url=self._base_url or "",
            follow_redirects=True,
            headers=headers,
        )
        self._httpx_client_owned = True
        return self._httpx_client

    def _spawn_subprocess_connection(self, worker_index: int) -> WorkerConnection:
        """Spawn or borrow a subprocess worker and wrap it in an RPC proxy.

        When a pool is configured, borrows an idle worker (or spawns a new
        one) from the pool. Otherwise creates a subprocess directly.

        Python-specific: subprocess management relies on ``shell=True``
        semantics and the ``WorkerPool`` abstraction that other languages
        don't need to mirror.
        """
        if self._pool is not None:
            _logger.debug("borrowing_worker worker_index=%s", worker_index)
            cmd = shlex.split(self.server_path)
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
        proc = subprocess.Popen(
            self.server_path,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None if self.passthrough_stderr else subprocess.PIPE,
            text=False,
            bufsize=0,
            shell=True,
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

    def _stop_worker(self, worker: WorkerConnection) -> int:
        """Stop a worker subprocess or return it to the pool.

        Closes the worker's stream session (if open), then either returns the
        worker to the pool (pooled) or exits the RPC connection and waits for
        the subprocess to terminate (direct).

        Args:
            worker: The worker connection to stop.

        Returns:
            The subprocess exit code. Returns 0 for pooled workers (returned
            to pool) or normal termination, non-zero for errors.

        """
        if worker.stream is not None:
            worker.stream.close()
            worker.stream = None

        if worker._http_ctx is not None:
            # HTTP transport — close the RPC proxy. The underlying httpx
            # client is shared across workers and closed in Client.stop().
            worker._http_ctx.__exit__(None, None, None)
            _logger.debug("http_connection_closed worker_index=%s", worker.worker_index)
            return 0

        if worker._pool_ctx is not None:
            # Return to pool — pool handles subprocess lifecycle
            worker._pool_ctx.__exit__(None, None, None)
            _logger.debug("worker_returned_to_pool worker_index=%s", worker.worker_index)
            return 0

        # Direct subprocess management
        assert worker.connection is not None
        assert worker.proc is not None
        worker.connection.__exit__(None, None, None)
        worker.proc.wait(timeout=self.PROCESS_WAIT_TIMEOUT)
        returncode = worker.proc.returncode
        if returncode != 0:
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

    def _close_secondary_workers(self) -> None:
        """Close and stop all secondary (additional) workers."""
        for worker in self._additional_workers:
            self._stop_worker(worker)
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
        sets up RPC transport, and creates a typed VgiProtocol proxy for
        method calls.

        After this method returns, the client is ready to invoke functions via
        table_in_out_function(), table_function(), or scalar_function(). When
        using the context manager protocol (with statement), this method is
        called automatically.

        The stderr buffer is cleared when start() is called, so any stderr from
        previous runs is discarded.

        Raises:
            ClientError: If the client is already started (call stop() first),
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
        else:
            id_repr = "pooled"
        _logger.debug("server_started id=%s", id_repr)

    def stop(self) -> int:
        """Stop all worker subprocesses and clean up resources.

        Terminates all workers in the following order:
        1. Stops all additional workers (spawned for parallel processing)
        2. Stops the primary worker
        3. Waits for all stderr drain threads to complete (with timeout)
        4. Resets all internal state

        After this method returns, the client can be started again with start().
        When using the context manager protocol (with statement), this method
        is called automatically on exit.

        Returns:
            The exit code of the primary worker process. Returns 0 for normal
            termination, non-zero values indicate errors. Exit codes from
            additional workers are logged but not returned.

        Raises:
            ClientError: If the client was not started (call start() first).

        """
        if self._primary is None:
            raise ClientError("Client not started")

        # Stop additional workers first
        self._close_secondary_workers()

        # Stop primary worker
        returncode = self._stop_worker(self._primary)
        self._primary = None

        # Wait for stderr threads to finish draining
        for stderr_thread in self._stderr_threads:
            stderr_thread.join(timeout=self.THREAD_JOIN_TIMEOUT)
            if stderr_thread.is_alive():
                _logger.warning("stderr_thread_did_not_terminate")
        self._stderr_threads = []

        # Close the shared httpx.Client if we created it ourselves.
        if self._httpx_client_owned and self._httpx_client is not None:
            try:
                self._httpx_client.close()
            finally:
                self._httpx_client = None
                self._httpx_client_owned = False

        return returncode

    def server_capabilities(self) -> Any:
        """Return the HTTP server's advertised capabilities.

        Only valid for HTTP-mode clients. The returned
        ``HttpServerCapabilities`` carries ``max_request_bytes``,
        ``upload_url_support``, and ``max_upload_bytes`` — the fields the
        client consults before deciding to externalize large input batches
        via upload URLs (see Phase 4 of the whimsical-mccarthy plan).
        """
        if self._transport != "http":
            raise ClientError("server_capabilities() is only available for HTTP transport")
        from vgi_rpc.http import http_capabilities

        httpx_client = self._get_or_create_httpx_client()
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
        arguments: Arguments,
        function_type: FunctionType,
        input_schema: pa.Schema | None = None,
        settings: dict[str, Any] | None = None,
        secrets: dict[str, Any] | None = None,
        transaction_id: bytes | None = None,
    ) -> BindRequest:
        """Create a BindRequest for the given function parameters."""
        return BindRequest(
            function_name=function_name,
            arguments=arguments,
            function_type=function_type,
            input_schema=input_schema,
            settings=self._settings_to_batch(settings),
            secrets=self._secrets_to_batch(secrets),
            attach_id=self._attach_id,
            transaction_id=transaction_id,
        )

    @staticmethod
    def _do_bind(
        proxy: VgiProtocol,
        bind_request: BindRequest,
        bind_result_callback: Callable[[BindResponse], None] | None = None,
    ) -> BindResponse:
        """Call bind on a worker proxy and return BindResponse.

        Args:
            proxy: VgiProtocol proxy from RpcConnection.
            bind_request: The bind request to send.
            bind_result_callback: Optional callback invoked with the
                BindResponse before returning.

        Returns:
            BindResponse containing output_schema and opaque_data.

        Raises:
            ClientError: If the RPC call fails.

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
    ) -> StreamSession:
        """Call init on a worker proxy and return a StreamSession.

        Args:
            proxy: VgiProtocol proxy from RpcConnection.
            bind_request: The original bind request.
            bind_response: The bind response containing output_schema.
            projection_ids: Optional column indices for projection.
            pushdown_filters_batch: Optional deserialized filter predicates.
            phase: Table-in-out function phase (INPUT or FINALIZE).
            execution_id: For secondary init, the execution ID from
                the primary worker's init response.
            init_opaque_data: For secondary init, the opaque data from
                the primary worker's init response.

        Returns:
            StreamSession for data exchange or production.

        Raises:
            ClientError: If the RPC call fails.

        """
        init_request = InitRequest(
            bind_call=bind_request,
            output_schema=bind_response.output_schema,
            bind_opaque_data=bind_response.opaque_data,
            projection_ids=projection_ids,
            pushdown_filters=pushdown_filters_batch,
            phase=phase,
            execution_id=execution_id,
            init_opaque_data=init_opaque_data,
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
        arguments: Arguments,
        function_type: FunctionType,
        input_schema: pa.Schema | None,
        settings: dict[str, Any] | None,
        secrets: dict[str, Any] | None,
        transaction_id: bytes | None,
        projection_ids: list[int] | None,
        pushdown_filters_batch: pa.RecordBatch | None,
        phase: TableInOutFunctionInitPhase | None,
        bind_result_callback: Callable[[BindResponse], None] | None,
    ) -> tuple[BindRequest, BindResponse, GlobalInitResponse]:
        """Run the canonical bind → init → fan-out-workers sequence.

        All three function entry points (``scalar_function``,
        ``table_function``, ``table_in_out_function``) share this shape:

        1. Build a ``BindRequest`` from the user's call.
        2. ``bind`` against the primary worker proxy.
        3. ``init`` against the primary — stores ``StreamSession`` on the
           primary worker connection.
        4. Read the ``GlobalInitResponse`` header (carries ``max_workers``
           + ``execution_id`` for secondary workers).
        5. Spawn any additional workers and drive their ``init`` with the
           primary's execution identity.

        Centralizing this keeps HTTP/subprocess differences and protocol
        changes (e.g. future scoped-secret re-bind, init hints) in one
        place.
        """
        assert self._primary is not None, "primary worker not started"

        bind_request = self._make_bind_request(
            function_name=function_name,
            arguments=arguments,
            function_type=function_type,
            input_schema=input_schema,
            settings=settings,
            secrets=secrets,
            transaction_id=transaction_id,
        )
        bind_response = self._do_bind(self._primary.proxy, bind_request, bind_result_callback)

        stream = self._do_init(
            self._primary.proxy,
            bind_request,
            bind_response,
            projection_ids=projection_ids,
            pushdown_filters_batch=pushdown_filters_batch,
            phase=phase,
        )
        self._primary.stream = stream

        init_response = stream.typed_header(GlobalInitResponse)
        max_workers = self._determine_max_workers(init_response.max_workers)

        self._spawn_additional_workers(
            max_workers,
            bind_request,
            bind_response,
            init_response,
            projection_ids=projection_ids,
            pushdown_filters_batch=pushdown_filters_batch,
            phase=phase,
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

        Raises:
            ClientError: If any worker fails to initialize. The exception wraps
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
        if self._transport != "http":
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
        urls = request_upload_urls(base_url=self._base_url, count=1, client=httpx_client)
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
    ) -> list[pa.RecordBatch]:
        """Send a batch to a worker and collect all output batches.

        Sends the input batch via stream.exchange(), then checks the vgi.status
        metadata. If the worker returns HAVE_MORE_OUTPUT, sends the same input
        again. Continues until NEED_MORE_INPUT or no status (scalar functions).

        Args:
            worker: The worker connection to use. Must have stream initialized.
            input_batch: The input RecordBatch to send to the worker.
            batch_index: Index of this batch in the input sequence (for logging).

        Returns:
            List of output RecordBatches produced by processing this input batch.

        Raises:
            ClientError: If worker.stream is None, or if the worker returns
                an unexpected status, or if the RPC call fails.

        """
        if worker.stream is None:
            raise ClientError(f"Worker {worker.worker_index} stream not initialized")

        output_batches: list[pa.RecordBatch] = []

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

        return output_batches

    def _worker_thread_loop(
        self,
        worker: WorkerConnection,
        input_queue: Queue[tuple[int, pa.RecordBatch] | None],
        output_queue: Queue[tuple[int, list[pa.RecordBatch]] | BaseException],
    ) -> None:
        """Thread function that processes batches for a single worker.

        Runs in a dedicated thread, pulling (batch_index, batch) tuples from
        the input queue, processing them via _process_batch_on_worker, and
        pushing (batch_index, output_batches) tuples to the output queue.

        When None is received from input_queue, signals thread completion by
        pushing (-1, []) to output_queue and exits.

        If an exception occurs during processing, it is caught, logged, and
        pushed to output_queue as the exception object itself.

        Args:
            worker: The worker connection to use for processing batches.
            input_queue: Thread-safe queue providing (batch_index, RecordBatch)
                tuples for processing. A None value signals end of input.
            output_queue: Thread-safe queue for results.

        """
        try:
            while True:
                item = input_queue.get()
                if item is None:
                    # End of input - signal thread completion
                    output_queue.put((-1, []))
                    break

                batch_index, input_batch = item
                outputs = self._process_batch_on_worker(worker, input_batch, batch_index)
                output_queue.put((batch_index, outputs))
        except Exception as e:
            _logger.exception("worker_thread_error worker_index=%s", worker.worker_index)
            output_queue.put(e)

    def _distribute_and_collect(
        self,
        *,
        all_workers: list[WorkerConnection],
        first_batch: pa.RecordBatch,
        remaining_input: Iterator[pa.RecordBatch],
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

        Yields:
            Output RecordBatches from processing. When multiple batches are
            returned for a single input (HAVE_MORE_OUTPUT), they are combined
            into one batch. Order is non-deterministic for multi-worker mode.

        Raises:
            ClientError: If a worker thread fails with an exception.

        """
        num_workers = len(all_workers)

        _logger.debug("starting_parallel_processing num_workers=%s", num_workers)

        # Create queues for each worker
        input_queues: list[Queue[tuple[int, pa.RecordBatch] | None]] = [Queue() for _ in range(num_workers)]
        output_queue: Queue[tuple[int, list[pa.RecordBatch]] | BaseException] = Queue()

        # Start worker threads
        threads: list[threading.Thread] = []
        for i, worker in enumerate(all_workers):
            thread = threading.Thread(
                target=self._worker_thread_loop,
                args=(worker, input_queues[i], output_queue),
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

            batch_idx, output_batches = result
            outputs_received += 1

            # Combine output batches if needed
            combined = self._combine_batches(output_batches)
            if combined is not None:
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
        input: Iterator[pa.RecordBatch],
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[BindResponse], None] | None = None,
        projection_ids: list[int] | None = None,
        pushdown_filters: bytes | None = None,
        settings: dict[str, Any] | None = None,
        transaction_id: bytes | None = None,
    ) -> Generator[pa.RecordBatch]:
        """Invoke a table-in-out function on the worker and stream results.

        For parallel processing (max_workers > 1), input batches are distributed
        round-robin across workers using dedicated threads. Output order may not
        match input order in parallel mode. Only the primary worker receives the
        FINALIZE phase and produces final aggregated output.

        Args:
            function_name: Name of the function to invoke. Must exist in the
                worker's registry.
            input: Iterator yielding input RecordBatches. Must yield at least one
                batch. The first batch's schema is used to initialize the IPC
                stream. Raises ClientError if the iterator is empty.
            arguments: Optional Arguments container with positional and named
                arguments to pass to the function. Defaults to empty Arguments().
            bind_result_callback: Optional callback invoked with the BindResponse
                before processing begins.
            projection_ids: Optional list of column indices for column projection.
            pushdown_filters: Optional byte string containing filter predicates
                to push down to the function.
            settings: Optional dictionary of settings/pragmas to
                pass to the function.
            transaction_id: Optional unique identifier for the DuckDB transaction.

        Yields:
            Output RecordBatches from the function. In single-worker mode, output
            order corresponds to input order. In parallel mode (max_workers > 1),
            output order is non-deterministic due to round-robin distribution.
            Final output from finalize is always yielded last.

        Raises:
            ClientError: If the client is not started, input iterator is empty,
                input iterator yields non-RecordBatch objects, communication
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
                    arguments=arguments,
                    function_type=FunctionType.TABLE,
                    input_schema=input_schema,
                    settings=settings,
                    secrets=None,
                    transaction_id=transaction_id,
                    projection_ids=projection_ids,
                    pushdown_filters_batch=pushdown_filters_batch,
                    phase=TableInOutFunctionInitPhase.INPUT,
                    bind_result_callback=bind_result_callback,
                )

                # Process input batches across all workers
                all_workers = [self._primary] + self._additional_workers
                yield from self._distribute_and_collect(
                    all_workers=all_workers,
                    first_batch=first_batch,
                    remaining_input=input,
                )

                # Close all input streams
                for worker in all_workers:
                    if worker.stream is not None:
                        worker.stream.close()
                        worker.stream = None

                # Close secondary workers
                self._close_secondary_workers()

                # Finalize on primary worker
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
            Final output RecordBatches from the worker's finalize phase.

        Raises:
            ClientError: If the RPC call fails.

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

    def table_function(
        self,
        *,
        function_name: str,
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[BindResponse], None] | None = None,
        projection_ids: list[int] | None = None,
        pushdown_filters: bytes | None = None,
        settings: dict[str, Any] | None = None,
        transaction_id: bytes | None = None,
    ) -> Generator[pa.RecordBatch]:
        """Invoke a table function (source function) and stream output batches.

        Table functions generate output batches without receiving input data.
        They are useful for data sources, generators, or functions that produce
        results based solely on their arguments.

        For parallel processing (max_workers > 1), output is read from all
        workers concurrently using threads. Output order is non-deterministic.

        Args:
            function_name: Name of the function to invoke. Must exist in the
                worker's registry and be a table function (not table-in-out).
            arguments: Optional Arguments container with positional and named
                arguments to pass to the function. Defaults to empty Arguments().
            bind_result_callback: Optional callback invoked with the BindResponse
                before processing begins.
            projection_ids: Optional list of column indices for column projection.
            pushdown_filters: Optional byte string containing filter predicates
                to push down to the function.
            settings: Optional dictionary of settings/pragmas to
                pass to the function.
            transaction_id: Optional unique identifier for the DuckDB transaction.

        Yields:
            Output RecordBatches from the function. In parallel mode
            (max_workers > 1), output order is non-deterministic.

        Raises:
            ClientError: If the client is not started, communication with the
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
                arguments=arguments,
                function_type=FunctionType.TABLE,
                input_schema=None,
                settings=settings,
                secrets=None,
                transaction_id=transaction_id,
                projection_ids=projection_ids,
                pushdown_filters_batch=pushdown_filters_batch,
                phase=None,
                bind_result_callback=bind_result_callback,
            )

            # Read output from all workers in parallel
            yield from self._table_function_parallel()
        except ClientError as e:
            raise self._client_error_with_stderr(e) from e.__cause__

    def _table_function_parallel(self) -> Generator[pa.RecordBatch]:
        """Read output from table function workers using parallel threads.

        Handles both single-worker and multi-worker cases uniformly. For each
        worker, spawns a dedicated thread that reads output batches and pushes
        them to a shared output queue.

        Yields:
            Output RecordBatches from all workers in non-deterministic order.

        Raises:
            ClientError: If a worker thread fails with an exception.

        """
        assert self._primary is not None
        all_workers = [self._primary] + self._additional_workers
        num_workers = len(all_workers)

        _logger.debug("starting_parallel_table_function num_workers=%s", num_workers)

        # Queue for collecting output from all workers
        output_queue: Queue[pa.RecordBatch | BaseException | None] = Queue()

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
                        output_queue.put(output.batch)

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

            yield result

        self._join_threads(threads)
        _logger.debug("all_table_function_workers_complete")

        # Close streams and secondary workers
        for worker in all_workers:
            if worker.stream is not None:
                worker.stream.close()
                worker.stream = None
        self._close_secondary_workers()
        _logger.debug("parallel_table_function_complete")

    def scalar_function(
        self,
        *,
        function_name: str,
        input: Iterator[pa.RecordBatch],
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[BindResponse], None] | None = None,
        settings: dict[str, Any] | None = None,
        secrets: dict[str, Any] | None = None,
        transaction_id: bytes | None = None,
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
            input: Iterator yielding input RecordBatches. Must yield at least one
                batch. The first batch's schema is used to initialize the IPC
                stream. Raises ClientError if the iterator is empty.
            arguments: Optional Arguments container with positional and named
                arguments to pass to the function. Defaults to empty Arguments().
            bind_result_callback: Optional callback invoked with the BindResponse
                before processing begins.
            settings: Optional dictionary of settings/pragmas to
                pass to the function.
            secrets: Optional dictionary of secret name to value pairs.
                Values can be simple scalars or dicts (for struct-typed secrets).
            transaction_id: Optional unique identifier for the DuckDB transaction.

        Yields:
            Output RecordBatches from the function. Each output batch has a single
            column and the same number of rows as its corresponding input batch.
            In single-worker mode, output order corresponds to input order.
            In parallel mode (max_workers > 1), output order is non-deterministic.

        Raises:
            ClientError: If the client is not started, input iterator is empty,
                input iterator yields non-RecordBatch objects, communication
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
                    arguments=arguments,
                    function_type=FunctionType.SCALAR,
                    input_schema=input_schema,
                    settings=settings,
                    secrets=secrets,
                    transaction_id=transaction_id,
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
