"""VGI application-level OpenTelemetry and Sentry instrumentation.

Provides ``VgiTracer`` — a thin wrapper that enriches both OTel spans and
Sentry scopes with VGI-level attributes (function name, attach_id, etc.)
and creates ``vgi.execute.*`` per-batch records (OTel spans + Sentry
breadcrumbs).

All OTel and Sentry imports are deferred to ``VgiTracer.create()`` so that
``import vgi.otel`` works even when neither dependency is installed.  When
both backends are disabled, all operations are zero-cost no-ops.

Despite the module name, this is the central instrumentation hook for both
backends.  vgi-rpc's own Sentry auto-attach handles RPC-layer fields
(method name, server id, auth principal); the helpers here add VGI-layer
fields (function name, function type, attach id, transaction id, per-batch
row counts) on top.
"""

from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vgi_rpc.otel import OtelConfig

__all__ = [
    "VgiTracer",
    "get_noop_tracer",
]


def _sentry_active() -> bool:
    """Return True when ``sentry_sdk`` is imported and initialised in this process.

    The ``sys.modules`` check ensures we never force the optional dependency
    on workers that have not opted into Sentry.
    """
    if "sentry_sdk" not in sys.modules:
        return False
    import sentry_sdk

    return bool(sentry_sdk.is_initialized())


class _NoopSpan:
    """No-op context manager + span. Zero imports."""

    def __enter__(self) -> _NoopSpan:
        return self

    def __exit__(self, *a: object) -> None:
        pass

    def set_attribute(self, k: str, v: Any) -> None:
        pass


_NOOP_SPAN = _NoopSpan()

_VGI_SCOPE = "vgi"


class VgiTracer:
    """Wraps OTel tracer + meter or acts as no-op.

    Use ``VgiTracer.create(otel_config)`` to build. When *otel_config* is
    ``None``, returns the module-level ``_NOOP_TRACER`` singleton — all
    methods become zero-cost no-ops.
    """

    __slots__ = (
        "_enabled",
        "_sentry_enabled",
        "_tracer",
        "_meter",
        "_duration_histogram",
        "_input_rows_counter",
        "_output_rows_counter",
        "_input_bytes_counter",
        "_output_bytes_counter",
    )

    def __init__(self, *, enabled: bool = False, sentry_enabled: bool = False) -> None:  # noqa: D107
        self._enabled = enabled
        self._sentry_enabled = sentry_enabled
        self._tracer: Any = None
        self._meter: Any = None
        self._duration_histogram: Any = None
        self._input_rows_counter: Any = None
        self._output_rows_counter: Any = None
        self._input_bytes_counter: Any = None
        self._output_bytes_counter: Any = None

    @staticmethod
    def create(otel_config: OtelConfig | None) -> VgiTracer:
        """Create a VgiTracer from an OtelConfig.

        When *otel_config* is ``None`` and Sentry is not initialised, returns
        the module-level noop tracer.  When Sentry is initialised, returns a
        tracer with Sentry enrichment active even if OTel is disabled, so VGI
        scope context still flows into Sentry events.
        """
        sentry_enabled = _sentry_active()
        if otel_config is None and not sentry_enabled:
            return _NOOP_TRACER

        if otel_config is None:
            # Sentry-only tracer: no OTel state needed.
            return VgiTracer(enabled=False, sentry_enabled=True)

        from opentelemetry import metrics, trace

        vt = VgiTracer(enabled=True, sentry_enabled=sentry_enabled)
        vt._tracer = trace.get_tracer(_VGI_SCOPE)
        vt._meter = metrics.get_meter(_VGI_SCOPE)

        vt._duration_histogram = vt._meter.create_histogram(
            name="vgi.function.duration",
            description="User code processing time per batch",
            unit="s",
        )
        vt._input_rows_counter = vt._meter.create_counter(
            name="vgi.function.input_rows",
            description="Total rows consumed",
        )
        vt._output_rows_counter = vt._meter.create_counter(
            name="vgi.function.output_rows",
            description="Total rows produced",
        )
        vt._input_bytes_counter = vt._meter.create_counter(
            name="vgi.function.input_bytes",
            description="Total logical bytes received",
        )
        vt._output_bytes_counter = vt._meter.create_counter(
            name="vgi.function.output_bytes",
            description="Total logical bytes sent",
        )
        return vt

    @property
    def enabled(self) -> bool:
        """Return whether OTel instrumentation is active."""
        return self._enabled

    @property
    def sentry_enabled(self) -> bool:
        """Return whether Sentry enrichment is active."""
        return self._sentry_enabled

    def start_span(self, name: str, attributes: dict[str, Any] | None = None) -> Any:
        """Start a child span. Returns ``_NOOP_SPAN`` when disabled."""
        if not self._enabled:
            return _NOOP_SPAN
        return self._tracer.start_as_current_span(name, attributes=attributes)

    def set_current_span_attributes(self, attributes: dict[str, Any]) -> None:
        """Enrich the active OTel span and Sentry scope with VGI attributes.

        Each non-``None`` value is set as both an OTel span attribute and
        (when Sentry is initialised) a Sentry tag.  Tags are merged into the
        current scope, so calling this multiple times during a dispatch
        accumulates context rather than overwriting it.
        """
        if not self._enabled and not self._sentry_enabled:
            return
        if self._enabled:
            from opentelemetry import trace

            span = trace.get_current_span()
            for k, v in attributes.items():
                if v is not None:
                    span.set_attribute(k, v)
        if self._sentry_enabled:
            import sentry_sdk

            scope = sentry_sdk.get_current_scope()
            for k, v in attributes.items():
                if v is None:
                    continue
                # Sentry tag values must be strings; bools render as
                # ``"True"``/``"False"`` which is fine for searchable filters.
                scope.set_tag(k, str(v))

    def record_execute_metrics(
        self,
        *,
        function_name: str,
        function_type: str,
        duration_s: float,
        input_rows: int | None = None,
        output_rows: int | None = None,
        input_bytes: int | None = None,
        output_bytes: int | None = None,
    ) -> None:
        """Record per-batch execution metrics."""
        if not self._enabled:
            return
        labels = {"vgi.function.name": function_name, "vgi.function.type": function_type}
        self._duration_histogram.record(duration_s, labels)
        if input_rows is not None:
            self._input_rows_counter.add(input_rows, labels)
        if output_rows is not None:
            self._output_rows_counter.add(output_rows, labels)
        if input_bytes is not None:
            self._input_bytes_counter.add(input_bytes, labels)
        if output_bytes is not None:
            self._output_bytes_counter.add(output_bytes, labels)


_NOOP_TRACER = VgiTracer(enabled=False)


def get_noop_tracer() -> VgiTracer:
    """Return the module-level noop tracer singleton."""
    return _NOOP_TRACER


def _batch_bytes(batch: Any) -> int:
    """Return total buffer size of a RecordBatch, or 0 on failure."""
    try:
        return int(batch.get_total_buffer_size())
    except Exception:
        return 0


def _timed_exchange(
    vgi_tracer: VgiTracer,
    span_name: str,
    function_name: str,
    function_type: str,
    execution_id: bytes | None,
) -> _ExchangeTimer:
    """Create an exchange timer for tracking per-batch metrics."""
    return _ExchangeTimer(vgi_tracer, span_name, function_name, function_type, execution_id)


class _ExchangeTimer:
    """Context manager that creates a span and records metrics for one exchange."""

    __slots__ = (
        "_vgi_tracer",
        "_span_name",
        "_function_name",
        "_function_type",
        "_execution_id",
        "_span_ctx",
        "_span",
        "_start",
    )

    def __init__(
        self,
        vgi_tracer: VgiTracer,
        span_name: str,
        function_name: str,
        function_type: str,
        execution_id: bytes | None,
    ) -> None:
        self._vgi_tracer = vgi_tracer
        self._span_name = span_name
        self._function_name = function_name
        self._function_type = function_type
        self._execution_id = execution_id
        self._span_ctx: Any = None
        self._span: Any = None
        self._start = 0.0

    def __enter__(self) -> _ExchangeTimer:
        if not self._vgi_tracer.enabled and not self._vgi_tracer.sentry_enabled:
            return self
        # Always start the wall clock so Sentry breadcrumbs report duration
        # even in Sentry-only deployments without OTel.
        self._start = time.monotonic()
        if not self._vgi_tracer.enabled:
            return self
        attrs: dict[str, Any] = {
            "vgi.function.name": self._function_name,
            "vgi.function.type": self._function_type,
        }
        if self._execution_id is not None:
            attrs["vgi.execute.execution_id"] = self._execution_id.hex()
        self._span_ctx = self._vgi_tracer.start_span(self._span_name, attributes=attrs)
        self._span = self._span_ctx.__enter__()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, *a: object) -> None:
        if self._span is not None:
            if exc_val is not None:
                from opentelemetry.trace import StatusCode

                self._span.set_status(StatusCode.ERROR, str(exc_val))
                self._span.record_exception(exc_val)
            else:
                from opentelemetry.trace import StatusCode

                self._span.set_status(StatusCode.OK)
        if self._span_ctx is not None:
            self._span_ctx.__exit__(exc_type, exc_val, *a)

    def record(
        self,
        *,
        input_rows: int | None = None,
        output_rows: int | None = None,
        input_bytes: int | None = None,
        output_bytes: int | None = None,
    ) -> None:
        """Set span attributes and record metrics for this exchange."""
        if not self._vgi_tracer.enabled and not self._vgi_tracer.sentry_enabled:
            return
        duration = time.monotonic() - self._start
        if self._vgi_tracer.enabled:
            if self._span is not None:
                if input_rows is not None:
                    self._span.set_attribute("vgi.execute.input_rows", input_rows)
                if output_rows is not None:
                    self._span.set_attribute("vgi.execute.output_rows", output_rows)
                if input_bytes is not None:
                    self._span.set_attribute("vgi.execute.input_bytes", input_bytes)
                if output_bytes is not None:
                    self._span.set_attribute("vgi.execute.output_bytes", output_bytes)
            self._vgi_tracer.record_execute_metrics(
                function_name=self._function_name,
                function_type=self._function_type,
                duration_s=duration,
                input_rows=input_rows,
                output_rows=output_rows,
                input_bytes=input_bytes,
                output_bytes=output_bytes,
            )
        if self._vgi_tracer.sentry_enabled:
            import sentry_sdk

            data: dict[str, Any] = {
                "function_name": self._function_name,
                "function_type": self._function_type,
                "duration_ms": round(duration * 1000.0, 3),
            }
            if self._execution_id is not None:
                data["execution_id"] = self._execution_id.hex()
            if input_rows is not None:
                data["input_rows"] = input_rows
            if output_rows is not None:
                data["output_rows"] = output_rows
            if input_bytes is not None:
                data["input_bytes"] = input_bytes
            if output_bytes is not None:
                data["output_bytes"] = output_bytes
            sentry_sdk.add_breadcrumb(
                category="vgi.execute",
                message=f"{self._function_name} batch",
                level="info",
                data=data,
            )
