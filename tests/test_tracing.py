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
        batch, metadata = deserialize_record_batch(serialized)
        restored = Invocation.deserialize(batch, metadata)

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
        batch, metadata = deserialize_record_batch(serialized)
        restored = Invocation.deserialize(batch, metadata)

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


class TestTraceContextPropagationToSecondaryWorkers:
    """Tests for trace context propagation from primary to secondary workers."""

    def test_secondary_workers_receive_primary_trace_context(self) -> None:
        """Secondary workers should receive the primary worker's trace context.

        This test verifies the full protocol flow:
        1. Client runs under an active trace span
        2. Primary worker extracts its trace context and injects it into init data
        3. Secondary workers load init data and receive the traceparent
        4. All workers report the same traceparent value
        5. Multiple workers actually participated (different PIDs)
        """
        import pyarrow as pa
        import pytest

        try:
            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
        except ImportError:
            pytest.skip("opentelemetry-sdk not installed")

        from vgi import tracing
        from vgi.arguments import Arguments
        from vgi.client import Client

        # Set up tracer provider
        provider = TracerProvider()
        trace.set_tracer_provider(provider)

        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("test_span"):
            traceparent, _ = tracing.extract_trace_context()
            assert traceparent is not None, "Should have active trace context"

            # Run trace_context_reporter with 2 workers and enough work items
            # to ensure both workers participate. Each work item produces 10,000 rows.
            num_work_items = 4
            rows_per_work_item = 10_000
            with Client("vgi-example-worker", max_workers=2) as client:
                outputs = list(
                    client.table_function(
                        function_name="trace_context_reporter",
                        arguments=Arguments(positional=(pa.scalar(num_work_items),)),
                    )
                )

            table = pa.Table.from_batches(outputs)
            assert table.num_rows == num_work_items * rows_per_work_item

            # Verify multiple workers participated (different PIDs)
            worker_pids = set(table.column("worker_pid").to_pylist())
            assert len(worker_pids) >= 2, (
                f"Expected at least 2 workers to participate, got PIDs: {worker_pids}"
            )

            # Get unique traceparents reported by all workers
            traceparents = set(table.column("traceparent").to_pylist())

            # All workers should report the same non-null traceparent
            assert len(traceparents) == 1, (
                f"All workers should see same traceparent, got: {traceparents}"
            )
            reported_traceparent = traceparents.pop()
            assert reported_traceparent is not None, (
                "Workers should have received traceparent from init data"
            )

    def test_multiple_workers_same_traceparent(self) -> None:
        """Multiple workers processing work items should all see same traceparent."""
        import pyarrow as pa
        import pytest

        try:
            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
        except ImportError:
            pytest.skip("opentelemetry-sdk not installed")

        from vgi.arguments import Arguments
        from vgi.client import Client

        # Set up tracer provider
        provider = TracerProvider()
        trace.set_tracer_provider(provider)

        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("parent_span"):
            # Run with 3 workers and enough work items to ensure all participate.
            # Each work item produces 10,000 rows.
            num_work_items = 6
            rows_per_work_item = 10_000
            with Client("vgi-example-worker", max_workers=3) as client:
                outputs = list(
                    client.table_function(
                        function_name="trace_context_reporter",
                        arguments=Arguments(positional=(pa.scalar(num_work_items),)),
                    )
                )

            table = pa.Table.from_batches(outputs)
            assert table.num_rows == num_work_items * rows_per_work_item

            # Verify multiple workers participated (different PIDs)
            worker_pids = set(table.column("worker_pid").to_pylist())
            assert len(worker_pids) >= 2, (
                f"Expected at least 2 workers to participate, got PIDs: {worker_pids}"
            )

            # All rows should have the same traceparent
            traceparents = set(table.column("traceparent").to_pylist())
            assert len(traceparents) == 1, (
                f"All workers should report same traceparent, got {len(traceparents)}"
            )

            # The traceparent should not be None
            assert None not in traceparents, "Traceparent should not be None"
