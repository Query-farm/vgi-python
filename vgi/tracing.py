"""OpenTelemetry tracing support for VGI workers.

This module provides optional OpenTelemetry integration for distributed tracing.
When opentelemetry-api is installed and a tracer provider is configured,
worker processes will emit spans for function invocations.

The module gracefully degrades when opentelemetry-api is not installed,
providing no-op implementations that have minimal overhead.

OpenTelemetry imports are lazy-loaded to avoid ~15ms startup cost when tracing
is not used.

Example:
    # In worker code
    from vgi.tracing import get_tracer, restore_trace_context

    tracer = get_tracer("vgi.worker")
    with tracer.start_as_current_span("worker.invocation") as span:
        span.set_attribute(VGI_FUNCTION_NAME, "my_function")
        # ... do work ...

"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

__all__ = [
    "TRACING_AVAILABLE",  # noqa: F822 - provided by __getattr__
    "get_tracer",
    "restore_trace_context",
    "extract_trace_context",
    "detach_trace_context",
    "get_span_kind_server",
    "get_span_kind_client",
    "get_span_kind_internal",
    "set_span_in_context",
    "attach_context",
    "detach_context",
    "set_span_error",
    "add_batch_write_event",
    "maybe_configure_tracing",
    "shutdown_tracing",
    # Attribute constants
    "VGI_FUNCTION_NAME",
    "VGI_FUNCTION_TYPE",
    "VGI_INVOCATION_ID",
    "VGI_EXECUTION_ID",
    "VGI_CORRELATION_ID",
    "VGI_WORKER_NAME",
    "VGI_WORKER_PID",
    "VGI_WORKER_IS_PRIMARY",
    "VGI_PHASE",
    "VGI_MAX_WORKERS",
    "VGI_INPUT_SCHEMA_COLUMNS",
    "VGI_OUTPUT_SCHEMA_COLUMNS",
    "VGI_BATCH_INDEX",
    "VGI_BATCH_INPUT_ROWS",
    "VGI_BATCH_OUTPUT_ROWS",
    "VGI_TOTAL_BATCHES",
    "VGI_TOTAL_INPUT_ROWS",
    "VGI_TOTAL_OUTPUT_ROWS",
    "VGI_TOTAL_INPUT_BYTES",
    "VGI_TOTAL_OUTPUT_BYTES",
    "VGI_IPC_READER_MESSAGES",
    "VGI_IPC_WRITER_MESSAGES",
]

# Span attribute constants following OpenTelemetry semantic conventions
VGI_FUNCTION_NAME = "vgi.function.name"
VGI_FUNCTION_TYPE = "vgi.function.type"
VGI_INVOCATION_ID = "vgi.invocation.id"
VGI_EXECUTION_ID = "vgi.execution.id"
VGI_CORRELATION_ID = "vgi.correlation.id"
VGI_WORKER_NAME = "vgi.worker.name"
VGI_WORKER_PID = "vgi.worker.pid"
VGI_WORKER_IS_PRIMARY = "vgi.worker.is_primary"
VGI_PHASE = "vgi.phase"
VGI_MAX_WORKERS = "vgi.max_workers"
VGI_INPUT_SCHEMA_COLUMNS = "vgi.input_schema.columns"
VGI_OUTPUT_SCHEMA_COLUMNS = "vgi.output_schema.columns"
VGI_BATCH_INDEX = "vgi.batch.index"
VGI_BATCH_INPUT_ROWS = "vgi.batch.input_rows"
VGI_BATCH_OUTPUT_ROWS = "vgi.batch.output_rows"
VGI_TOTAL_BATCHES = "vgi.total.batches"
VGI_TOTAL_INPUT_ROWS = "vgi.total.input_rows"
VGI_TOTAL_OUTPUT_ROWS = "vgi.total.output_rows"
VGI_TOTAL_INPUT_BYTES = "vgi.total.input_bytes"
VGI_TOTAL_OUTPUT_BYTES = "vgi.total.output_bytes"
VGI_IPC_READER_MESSAGES = "vgi.ipc.reader_messages"
VGI_IPC_WRITER_MESSAGES = "vgi.ipc.writer_messages"

# Lazy-loaded OpenTelemetry modules (cached after first access)
_otel_modules: dict[str, Any] | None = None


def _get_otel() -> dict[str, Any] | None:
    """Lazily import and cache OpenTelemetry modules.

    Returns a dict with keys: trace, context, extract, inject, SpanKind,
    Status, StatusCode. Returns None if opentelemetry-api is not installed.
    """
    global _otel_modules

    if _otel_modules is not None:
        return _otel_modules if _otel_modules else None

    try:
        from opentelemetry import context as otel_context
        from opentelemetry import trace
        from opentelemetry.propagate import extract, inject
        from opentelemetry.trace import SpanKind, Status, StatusCode

        _otel_modules = {
            "trace": trace,
            "context": otel_context,
            "extract": extract,
            "inject": inject,
            "SpanKind": SpanKind,
            "Status": Status,
            "StatusCode": StatusCode,
        }
        return _otel_modules
    except ImportError:
        _otel_modules = {}  # Empty dict signals "not available"
        return None


def _tracing_available() -> bool:
    """Check if OpenTelemetry is available (lazy check)."""
    return _get_otel() is not None


# TRACING_AVAILABLE is lazily evaluated via __getattr__ for backwards compatibility
# Users can also call _tracing_available() directly
_TRACING_AVAILABLE_CACHED: bool | None = None


def __getattr__(name: str) -> Any:
    """Module-level __getattr__ for lazy evaluation of TRACING_AVAILABLE."""
    global _TRACING_AVAILABLE_CACHED
    if name == "TRACING_AVAILABLE":
        if _TRACING_AVAILABLE_CACHED is None:
            _TRACING_AVAILABLE_CACHED = _tracing_available()
        return _TRACING_AVAILABLE_CACHED
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class _NoOpSpan:
    """No-op span implementation for when tracing is disabled."""

    def set_attribute(self, key: str, value: Any) -> None:
        """No-op: does nothing."""
        pass

    def set_attributes(self, attributes: dict[str, Any]) -> None:
        """No-op: does nothing."""
        pass

    def set_status(self, status: Any, description: str | None = None) -> None:
        """No-op: does nothing."""
        pass

    def record_exception(
        self,
        exception: BaseException,
        attributes: dict[str, Any] | None = None,
        timestamp: int | None = None,
        escaped: bool = False,
    ) -> None:
        """No-op: does nothing."""
        pass

    def add_event(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
        timestamp: int | None = None,
    ) -> None:
        """No-op: does nothing."""
        pass

    def is_recording(self) -> bool:
        """Return False since this is a no-op span."""
        return False

    def __enter__(self) -> _NoOpSpan:
        """Enter context manager."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit context manager."""
        pass


class _NoOpTracer:
    """No-op tracer implementation for when tracing is disabled."""

    @contextmanager
    def start_as_current_span(
        self,
        name: str,
        context: Any = None,
        kind: Any = None,
        attributes: dict[str, Any] | None = None,
        links: Any = None,
        start_time: int | None = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
        end_on_exit: bool = True,
    ) -> Iterator[_NoOpSpan]:
        """Return a no-op span context manager."""
        yield _NoOpSpan()

    def start_span(
        self,
        name: str,
        context: Any = None,
        kind: Any = None,
        attributes: dict[str, Any] | None = None,
        links: Any = None,
        start_time: int | None = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
    ) -> _NoOpSpan:
        """Return a no-op span."""
        return _NoOpSpan()


_noop_tracer = _NoOpTracer()


def _get_vgi_version() -> str:
    """Get the VGI package version for tracer instrumentation."""
    try:
        from importlib.metadata import version

        return version("vgi")
    except Exception:
        return "unknown"


def get_tracer(name: str = "vgi.worker") -> Any:
    """Get a tracer instance for creating spans.

    When opentelemetry-api is installed and a tracer provider is configured,
    returns a real tracer. Otherwise returns a no-op tracer that has minimal
    overhead.

    Args:
        name: The name of the tracer, typically the module or component name.
            Defaults to "vgi.worker".

    Returns:
        A tracer instance (real or no-op) that can be used to create spans.

    Example:
        tracer = get_tracer("vgi.worker")
        with tracer.start_as_current_span("my_operation") as span:
            span.set_attribute("key", "value")
            # ... do work ...

    """
    otel = _get_otel()
    if otel is None:
        return _noop_tracer
    return otel["trace"].get_tracer(name, _get_vgi_version())


def restore_trace_context(
    traceparent: str | None, tracestate: str | None = None
) -> Any:
    """Restore trace context from W3C Trace Context headers.

    This function should be called at the start of worker processing to
    link worker spans to the parent trace from the client.

    Args:
        traceparent: W3C traceparent header value, e.g.,
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        tracestate: Optional W3C tracestate header value for vendor-specific data.

    Returns:
        A context token that should be passed to detach_trace_context() when done,
        or None if tracing is not available or traceparent is None.

    Example:
        token = restore_trace_context(invocation.traceparent, invocation.tracestate)
        try:
            # ... do work with trace context active ...
        finally:
            detach_trace_context(token)

    """
    if traceparent is None:
        return None

    otel = _get_otel()
    if otel is None:
        return None

    carrier: dict[str, str] = {"traceparent": traceparent}
    if tracestate:
        carrier["tracestate"] = tracestate

    ctx = otel["extract"](carrier)
    return otel["context"].attach(ctx)


def detach_trace_context(token: Any) -> None:
    """Detach a previously restored trace context.

    Args:
        token: The token returned by restore_trace_context(), or None.

    """
    if token is None:
        return

    otel = _get_otel()
    if otel is not None:
        otel["context"].detach(token)


def extract_trace_context() -> tuple[str | None, str | None]:
    """Extract W3C Trace Context from the current span.

    This function should be called by the client to get trace context
    for propagation to worker processes.

    Returns:
        A tuple of (traceparent, tracestate) strings, or (None, None) if
        tracing is not available or there is no active span.

    Example:
        traceparent, tracestate = extract_trace_context()
        invocation = Invocation(
            ...,
            traceparent=traceparent,
            tracestate=tracestate,
        )

    """
    otel = _get_otel()
    if otel is None:
        return None, None

    carrier: dict[str, str] = {}
    otel["inject"](carrier)

    return carrier.get("traceparent"), carrier.get("tracestate")


def get_span_kind_server() -> Any:
    """Get SpanKind.SERVER or None if tracing unavailable."""
    otel = _get_otel()
    if otel is not None:
        return otel["SpanKind"].SERVER
    return None


def get_span_kind_client() -> Any:
    """Get SpanKind.CLIENT or None if tracing unavailable."""
    otel = _get_otel()
    if otel is not None:
        return otel["SpanKind"].CLIENT
    return None


def get_span_kind_internal() -> Any:
    """Get SpanKind.INTERNAL or None if tracing unavailable."""
    otel = _get_otel()
    if otel is not None:
        return otel["SpanKind"].INTERNAL
    return None


def set_span_in_context(span: Any) -> Any:
    """Set a span in a new context, returning the context.

    Args:
        span: The span to set in context.

    Returns:
        A context with the span set, or None if tracing unavailable.

    """
    otel = _get_otel()
    if otel is None:
        return None
    return otel["trace"].set_span_in_context(span)


def attach_context(ctx: Any) -> Any:
    """Attach a context as the current context.

    Args:
        ctx: The context to attach.

    Returns:
        A token to pass to detach_context(), or None if tracing unavailable.

    """
    if ctx is None:
        return None

    otel = _get_otel()
    if otel is None:
        return None
    return otel["context"].attach(ctx)


def detach_context(token: Any) -> None:
    """Detach a previously attached context.

    Args:
        token: The token returned by attach_context().

    """
    if token is None:
        return

    otel = _get_otel()
    if otel is not None:
        otel["context"].detach(token)


def set_span_error(span: Any, exception: BaseException) -> None:
    """Set error status on a span and record the exception.

    Args:
        span: The span to set error status on.
        exception: The exception that occurred.

    """
    otel = _get_otel()
    if otel is None:
        return

    if hasattr(span, "set_status") and hasattr(span, "record_exception"):
        span.record_exception(exception)
        span.set_status(otel["Status"](otel["StatusCode"].ERROR, str(exception)))


def add_batch_write_event(
    function_name: str,
    function_type: str,
    byte_size: int,
    row_count: int,
) -> None:
    """Add span event for IPC record batch write.

    Emits a span event on the current span with batch write details.
    This is a no-op if tracing is not available or no span is active.

    Args:
        function_name: Name of the function being executed.
        function_type: Type of the function (e.g., "scalar", "table_in_out").
        byte_size: Serialized byte size of the record batch.
        row_count: Number of rows in the batch.

    """
    otel = _get_otel()
    if otel is None:
        return

    span = otel["trace"].get_current_span()
    if span is not None and span.is_recording():
        span.add_event(
            "vgi.ipc.write_batch",
            attributes={
                VGI_FUNCTION_NAME: function_name,
                VGI_FUNCTION_TYPE: function_type,
                VGI_PHASE: "data",
                "ipc.batch.byte_size": int(byte_size),
                "ipc.batch.row_count": int(row_count),
            },
        )


def maybe_configure_tracing(
    default_service_name: str = "vgi",
    log_func: Any | None = None,
) -> Any:
    """Auto-configure OpenTelemetry SDK if OTEL env vars are set.

    Checks for OTEL_EXPORTER_OTLP_ENDPOINT and configures the appropriate
    exporter based on OTEL_EXPORTER_OTLP_PROTOCOL (grpc or http/protobuf).

    This is a no-op if:
    - OTEL_EXPORTER_OTLP_ENDPOINT is not set
    - A TracerProvider is already configured
    - Required packages are not installed

    Environment variables used:
    - OTEL_EXPORTER_OTLP_ENDPOINT: Collector endpoint (required to enable)
    - OTEL_EXPORTER_OTLP_PROTOCOL: "grpc" (default) or "http/protobuf"
    - OTEL_SERVICE_NAME: Service name (default: default_service_name param)
    - OTEL_EXPORTER_OTLP_HEADERS: Auth headers (handled by exporter)
    - OTEL_EXPORTER_OTLP_COMPRESSION: Compression (handled by exporter)

    Args:
        default_service_name: Service name to use if OTEL_SERVICE_NAME not set.
        log_func: Optional logging function that accepts keyword arguments.
            Called with (event, endpoint, protocol, service_name) on success.

    Returns:
        The configured TracerProvider, or None if not configured.
        Caller should call provider.shutdown() when done to flush spans.

    Example:
        provider = maybe_configure_tracing("vgi-worker", log.info)
        try:
            # ... do work ...
        finally:
            if provider:
                provider.shutdown()

    """
    import os

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        return None

    try:
        from opentelemetry import trace as otel_trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        return None

    # Don't override if already configured
    current_provider = otel_trace.get_tracer_provider()
    if isinstance(current_provider, TracerProvider):
        return None

    # Determine protocol and get appropriate exporter
    protocol = os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")

    try:
        if protocol == "http/protobuf":
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter as HttpExporter,
            )

            exporter_cls: Any = HttpExporter
        else:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter as GrpcExporter,
            )

            exporter_cls = GrpcExporter
    except ImportError:
        return None

    # Configure provider
    service_name = os.environ.get("OTEL_SERVICE_NAME", default_service_name)
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(exporter_cls()))
    otel_trace.set_tracer_provider(provider)

    if log_func is not None:
        log_func(
            "otel_tracing_configured",
            endpoint=endpoint,
            protocol=protocol,
            service_name=service_name,
        )

    return provider


def shutdown_tracing(provider: Any, log_func: Any | None = None) -> None:
    """Shutdown the tracer provider to flush pending spans.

    Args:
        provider: The TracerProvider returned by maybe_configure_tracing().
        log_func: Optional logging function for debug messages.

    """
    if provider is not None:
        try:
            provider.shutdown()
            if log_func is not None:
                log_func("otel_tracing_shutdown")
        except Exception as e:
            if log_func is not None:
                log_func("otel_shutdown_error", error=str(e))
