"""VGI application-level OpenTelemetry instrumentation.

Provides ``VgiTracer`` — a thin wrapper around OTel tracer and meter that
enriches vgi_rpc spans with VGI-level attributes and creates ``vgi.execute.*``
child spans for per-batch processing visibility.

All OTel imports are deferred to ``VgiTracer.create()`` so that
``import vgi.otel`` works even when opentelemetry is not installed.
When OTel is disabled, all operations are zero-cost no-ops.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vgi_rpc.otel import OtelConfig

__all__ = [
    "VgiTracer",
    "get_noop_tracer",
]


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
        "_tracer",
        "_meter",
        "_duration_histogram",
        "_input_rows_counter",
        "_output_rows_counter",
        "_input_bytes_counter",
        "_output_bytes_counter",
    )

    def __init__(self, *, enabled: bool = False) -> None:  # noqa: D107
        self._enabled = enabled
        self._tracer: Any = None
        self._meter: Any = None
        self._duration_histogram: Any = None
        self._input_rows_counter: Any = None
        self._output_rows_counter: Any = None
        self._input_bytes_counter: Any = None
        self._output_bytes_counter: Any = None

    @staticmethod
    def create(otel_config: OtelConfig | None) -> VgiTracer:
        """Create a VgiTracer from an OtelConfig, or return noop when None."""
        if otel_config is None:
            return _NOOP_TRACER

        from opentelemetry import metrics, trace

        vt = VgiTracer(enabled=True)
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

    def start_span(self, name: str, attributes: dict[str, Any] | None = None) -> Any:
        """Start a child span. Returns ``_NOOP_SPAN`` when disabled."""
        if not self._enabled:
            return _NOOP_SPAN
        return self._tracer.start_as_current_span(name, attributes=attributes)

    def set_current_span_attributes(self, attributes: dict[str, Any]) -> None:
        """Set attributes on the current (parent vgi_rpc) span."""
        if not self._enabled:
            return
        from opentelemetry import trace

        span = trace.get_current_span()
        for k, v in attributes.items():
            if v is not None:
                span.set_attribute(k, v)

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
        self._start = time.monotonic()
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
        if not self._vgi_tracer.enabled:
            return
        duration = time.monotonic() - self._start
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
