"""Unit tests for VGI OpenTelemetry tracing support.

Tests cover:
- Graceful degradation when opentelemetry-api is not available
- NoOp implementations
- Trace context extraction and restoration
- Span attribute constants
"""

from __future__ import annotations


class TestTracingAvailable:
    """Tests when opentelemetry-api is available."""

    def test_tracing_available_flag(self) -> None:
        """TRACING_AVAILABLE should be True when opentelemetry is installed."""
        from vgi import tracing

        assert tracing.TRACING_AVAILABLE is True

    def test_get_tracer_returns_real_tracer(self) -> None:
        """get_tracer should return a real tracer when OTel is available."""
        from vgi import tracing

        tracer = tracing.get_tracer("test.tracer")
        # Real tracer has start_as_current_span method
        assert hasattr(tracer, "start_as_current_span")
        assert hasattr(tracer, "start_span")

    def test_get_tracer_with_default_name(self) -> None:
        """get_tracer should use default name 'vgi.worker'."""
        from vgi import tracing

        tracer = tracing.get_tracer()
        assert tracer is not None

    def test_span_kind_functions(self) -> None:
        """get_span_kind_server and get_span_kind_internal should return SpanKind."""
        from opentelemetry.trace import SpanKind

        from vgi import tracing

        assert tracing.get_span_kind_server() == SpanKind.SERVER
        assert tracing.get_span_kind_internal() == SpanKind.INTERNAL

    def test_extract_trace_context_no_active_span(self) -> None:
        """extract_trace_context should return (None, None) without active span."""
        from vgi import tracing

        traceparent, tracestate = tracing.extract_trace_context()
        # Without an active span, should return None
        assert traceparent is None
        assert tracestate is None

    def test_restore_trace_context_none(self) -> None:
        """restore_trace_context should return None when traceparent is None."""
        from vgi import tracing

        token = tracing.restore_trace_context(None)
        assert token is None

    def test_restore_and_detach_trace_context(self) -> None:
        """restore_trace_context and detach_trace_context should work together."""
        from vgi import tracing

        # Valid W3C traceparent format
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        tracestate = "vendor=value"

        token = tracing.restore_trace_context(traceparent, tracestate)
        assert token is not None

        # Detach should not raise
        tracing.detach_trace_context(token)

    def test_detach_trace_context_none(self) -> None:
        """detach_trace_context should handle None token gracefully."""
        from vgi import tracing

        # Should not raise
        tracing.detach_trace_context(None)

    def test_set_span_error(self) -> None:
        """set_span_error should set error status on span."""
        from vgi import tracing

        tracer = tracing.get_tracer("test")
        with tracer.start_as_current_span("test_span") as span:
            exception = ValueError("test error")
            tracing.set_span_error(span, exception)
            # Span should have error status (we can't easily verify this
            # without an in-memory exporter, but at least it shouldn't raise)


class TestSpanAttributeConstants:
    """Tests for span attribute constant values."""

    def test_function_attributes(self) -> None:
        """Function-related attributes should have correct values."""
        from vgi import tracing

        assert tracing.VGI_FUNCTION_NAME == "vgi.function.name"
        assert tracing.VGI_FUNCTION_TYPE == "vgi.function.type"

    def test_invocation_attributes(self) -> None:
        """Invocation-related attributes should have correct values."""
        from vgi import tracing

        assert tracing.VGI_INVOCATION_ID == "vgi.invocation.id"
        assert tracing.VGI_EXECUTION_ID == "vgi.execution.id"
        assert tracing.VGI_CORRELATION_ID == "vgi.correlation.id"

    def test_worker_attributes(self) -> None:
        """Worker-related attributes should have correct values."""
        from vgi import tracing

        assert tracing.VGI_WORKER_PID == "vgi.worker.pid"
        assert tracing.VGI_WORKER_IS_PRIMARY == "vgi.worker.is_primary"
        assert tracing.VGI_MAX_WORKERS == "vgi.max_workers"

    def test_schema_attributes(self) -> None:
        """Schema-related attributes should have correct values."""
        from vgi import tracing

        assert tracing.VGI_INPUT_SCHEMA_COLUMNS == "vgi.input_schema.columns"
        assert tracing.VGI_OUTPUT_SCHEMA_COLUMNS == "vgi.output_schema.columns"

    def test_batch_attributes(self) -> None:
        """Batch-related attributes should have correct values."""
        from vgi import tracing

        assert tracing.VGI_BATCH_INDEX == "vgi.batch.index"
        assert tracing.VGI_BATCH_INPUT_ROWS == "vgi.batch.input_rows"
        assert tracing.VGI_BATCH_OUTPUT_ROWS == "vgi.batch.output_rows"

    def test_total_attributes(self) -> None:
        """Total statistics attributes should have correct values."""
        from vgi import tracing

        assert tracing.VGI_TOTAL_BATCHES == "vgi.total.batches"
        assert tracing.VGI_TOTAL_INPUT_ROWS == "vgi.total.input_rows"
        assert tracing.VGI_TOTAL_OUTPUT_ROWS == "vgi.total.output_rows"
        assert tracing.VGI_TOTAL_INPUT_BYTES == "vgi.total.input_bytes"
        assert tracing.VGI_TOTAL_OUTPUT_BYTES == "vgi.total.output_bytes"

    def test_ipc_attributes(self) -> None:
        """IPC statistics attributes should have correct values."""
        from vgi import tracing

        assert tracing.VGI_IPC_READER_MESSAGES == "vgi.ipc.reader_messages"
        assert tracing.VGI_IPC_WRITER_MESSAGES == "vgi.ipc.writer_messages"


class TestNoOpImplementations:
    """Tests for NoOp span and tracer implementations."""

    def test_noop_span_methods(self) -> None:
        """NoOp span should have all required methods that do nothing."""
        from vgi.tracing import _NoOpSpan

        span = _NoOpSpan()

        # All these should not raise
        span.set_attribute("key", "value")
        span.set_attributes({"key1": "value1", "key2": "value2"})
        span.set_status(None, "description")
        span.record_exception(ValueError("test"))
        span.add_event("event_name", {"attr": "value"})

        # is_recording should return False
        assert span.is_recording() is False

    def test_noop_span_context_manager(self) -> None:
        """NoOp span should work as context manager."""
        from vgi.tracing import _NoOpSpan

        span = _NoOpSpan()

        with span as s:
            assert s is span

    def test_noop_tracer_start_as_current_span(self) -> None:
        """NoOp tracer start_as_current_span should return NoOp span."""
        from vgi.tracing import _NoOpSpan, _NoOpTracer

        tracer = _NoOpTracer()

        with tracer.start_as_current_span("test_span") as span:
            assert isinstance(span, _NoOpSpan)
            span.set_attribute("key", "value")

    def test_noop_tracer_start_span(self) -> None:
        """NoOp tracer start_span should return NoOp span."""
        from vgi.tracing import _NoOpSpan, _NoOpTracer

        tracer = _NoOpTracer()

        span = tracer.start_span("test_span")
        assert isinstance(span, _NoOpSpan)


class TestInvocationTraceContext:
    """Tests for trace context in Invocation."""

    def test_invocation_with_trace_context(self) -> None:
        """Invocation should serialize/deserialize trace context fields."""
        import pyarrow as pa

        from vgi.invocation import Invocation, InvocationType
        from vgi.ipc_utils import deserialize_record_batch

        invocation = Invocation(
            function_name="test_func",
            input_schema=pa.schema([pa.field("x", pa.int64())]),
            function_type=InvocationType.TABLE,
            correlation_id="test-123",
            invocation_id=None,
            traceparent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            tracestate="vendor=value",
        )

        # Serialize and deserialize
        serialized = invocation.serialize()
        batch = deserialize_record_batch(serialized)
        restored = Invocation.deserialize(batch)

        assert restored.traceparent == invocation.traceparent
        assert restored.tracestate == invocation.tracestate

    def test_invocation_without_trace_context(self) -> None:
        """Invocation should handle missing trace context."""
        import pyarrow as pa

        from vgi.invocation import Invocation, InvocationType
        from vgi.ipc_utils import deserialize_record_batch

        invocation = Invocation(
            function_name="test_func",
            input_schema=pa.schema([pa.field("x", pa.int64())]),
            function_type=InvocationType.TABLE,
            correlation_id="test-123",
            invocation_id=None,
        )

        # Serialize and deserialize
        serialized = invocation.serialize()
        batch = deserialize_record_batch(serialized)
        restored = Invocation.deserialize(batch)

        assert restored.traceparent is None
        assert restored.tracestate is None


class TestClientTraceContextExtraction:
    """Tests for trace context extraction in client."""

    def test_client_extracts_trace_context(self) -> None:
        """Client should extract trace context from current span."""
        import pytest

        try:
            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
        except ImportError:
            pytest.skip("opentelemetry-sdk not installed")

        from vgi import tracing

        # Set up a tracer provider
        provider = TracerProvider()
        trace.set_tracer_provider(provider)

        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("parent_span"):
            traceparent, tracestate = tracing.extract_trace_context()
            # With an active span, traceparent should be set
            assert traceparent is not None
            # Should be in W3C format: version-traceid-spanid-flags
            parts = traceparent.split("-")
            assert len(parts) == 4
            assert parts[0] == "00"  # version
            assert len(parts[1]) == 32  # trace_id (hex)
            assert len(parts[2]) == 16  # span_id (hex)


class TestTracingGracefulDegradation:
    """Tests for graceful degradation when OpenTelemetry is not available.

    These tests mock the import to simulate missing opentelemetry-api.
    """

    def test_noop_when_tracing_unavailable(self) -> None:
        """Functions should return no-op values when tracing unavailable."""
        # We can't easily mock the import in the already-loaded module,
        # but we can test the no-op implementations directly
        from vgi.tracing import _noop_tracer, _NoOpTracer

        # The global noop_tracer should be available
        assert _noop_tracer is not None
        assert isinstance(_noop_tracer, _NoOpTracer)

    def test_set_span_error_with_noop_span(self) -> None:
        """set_span_error should handle no-op spans gracefully."""
        from vgi import tracing
        from vgi.tracing import _NoOpSpan

        span = _NoOpSpan()
        exception = ValueError("test error")

        # Should not raise, even with no-op span
        tracing.set_span_error(span, exception)
