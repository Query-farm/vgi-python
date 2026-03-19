"""Tests for vgi.otel — VGI application-level OpenTelemetry instrumentation."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pyarrow as pa
import pytest
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from vgi.otel import (
    VgiTracer,
    _batch_bytes,
    _NoopSpan,
    _timed_exchange,
    get_noop_tracer,
)


class _CollectingExporter(SpanExporter):
    """In-memory span exporter for tests."""

    def __init__(self) -> None:  # noqa: D107
        self.spans: list[Any] = []

    def export(self, spans: Any) -> SpanExportResult:
        """Collect spans into memory."""
        self.spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        """No-op shutdown."""


class TestNoopSpan:
    """Tests for _NoopSpan — zero-import no-op context manager."""

    def test_context_manager(self) -> None:
        """_NoopSpan works as context manager, returning self."""
        span = _NoopSpan()
        with span as s:
            assert s is span

    def test_set_attribute_noop(self) -> None:
        """set_attribute is a no-op, does not raise."""
        span = _NoopSpan()
        span.set_attribute("key", "value")


class TestGetNoopTracer:
    """Tests for get_noop_tracer() singleton."""

    def test_returns_singleton(self) -> None:
        """Same instance returned on every call."""
        t1 = get_noop_tracer()
        t2 = get_noop_tracer()
        assert t1 is t2

    def test_not_enabled(self) -> None:
        """Noop tracer reports enabled=False."""
        t = get_noop_tracer()
        assert not t.enabled


class TestVgiTracerNoop:
    """Tests for VgiTracer in noop mode (no OTel)."""

    def test_create_none_returns_noop(self) -> None:
        """VgiTracer.create(None) returns the noop singleton."""
        tracer = VgiTracer.create(None)
        assert not tracer.enabled
        assert tracer is get_noop_tracer()

    def test_start_span_returns_noop(self) -> None:
        """start_span returns _NoopSpan when disabled."""
        tracer = get_noop_tracer()
        ctx = tracer.start_span("test.span")
        assert isinstance(ctx, _NoopSpan)

    def test_set_current_span_attributes_noop(self) -> None:
        """set_current_span_attributes is a no-op when disabled."""
        tracer = get_noop_tracer()
        tracer.set_current_span_attributes({"key": "value"})

    def test_record_execute_metrics_noop(self) -> None:
        """record_execute_metrics is a no-op when disabled."""
        tracer = get_noop_tracer()
        tracer.record_execute_metrics(
            function_name="test",
            function_type="scalar",
            duration_s=0.1,
            input_rows=100,
            output_rows=100,
        )

    def test_noop_does_not_import_opentelemetry(self) -> None:
        """Noop tracer creation should not trigger opentelemetry imports."""
        tracer = VgiTracer.create(None)
        assert not tracer.enabled


class TestVgiTracerReal:
    """Tests for VgiTracer with real OTel (requires opentelemetry)."""

    @pytest.fixture()
    def otel_config(self) -> Any:
        """Provide an OtelConfig instance for tests."""
        from vgi_rpc.otel import OtelConfig

        return OtelConfig()

    def test_create_with_config_is_enabled(self, otel_config: Any) -> None:
        """VgiTracer.create(config) returns an enabled tracer."""
        tracer = VgiTracer.create(otel_config)
        assert tracer.enabled
        assert tracer is not get_noop_tracer()

    def test_scope_name_is_vgi(self, otel_config: Any) -> None:
        """Created tracer has a real underlying tracer."""
        tracer = VgiTracer.create(otel_config)
        assert tracer._tracer is not None

    def test_start_span_creates_real_span(self, otel_config: Any) -> None:
        """start_span creates an actual span with attributes."""
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor

        exporter = _CollectingExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        with patch("opentelemetry.trace.get_tracer") as mock_get_tracer:
            mock_get_tracer.return_value = provider.get_tracer("vgi")
            tracer = VgiTracer.create(otel_config)

        with tracer.start_span("vgi.test", attributes={"key": "val"}):
            pass

        spans = exporter.spans
        assert len(spans) == 1
        assert spans[0].name == "vgi.test"
        assert spans[0].attributes["key"] == "val"

    def test_set_current_span_attributes(self, otel_config: Any) -> None:
        """set_current_span_attributes enriches the active parent span."""
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor

        exporter = _CollectingExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        real_tracer = provider.get_tracer("vgi")

        with patch("opentelemetry.trace.get_tracer") as mock_get_tracer:
            mock_get_tracer.return_value = real_tracer
            tracer = VgiTracer.create(otel_config)

        with real_tracer.start_as_current_span("parent"):
            tracer.set_current_span_attributes(
                {
                    "vgi.function.name": "test_func",
                    "vgi.function.type": "scalar",
                }
            )

        spans = exporter.spans
        parent = [s for s in spans if s.name == "parent"][0]
        assert parent.attributes["vgi.function.name"] == "test_func"
        assert parent.attributes["vgi.function.type"] == "scalar"

    def test_skips_none_values(self, otel_config: Any) -> None:
        """None values in attributes dict are skipped."""
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor

        exporter = _CollectingExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        real_tracer = provider.get_tracer("vgi")

        with patch("opentelemetry.trace.get_tracer") as mock_get_tracer:
            mock_get_tracer.return_value = real_tracer
            tracer = VgiTracer.create(otel_config)

        with real_tracer.start_as_current_span("parent"):
            tracer.set_current_span_attributes(
                {
                    "vgi.present": "yes",
                    "vgi.absent": None,
                }
            )

        spans = exporter.spans
        parent = [s for s in spans if s.name == "parent"][0]
        assert parent.attributes["vgi.present"] == "yes"
        assert "vgi.absent" not in parent.attributes

    def test_metrics_recorded(self, otel_config: Any) -> None:
        """record_execute_metrics does not raise with valid inputs."""
        tracer = VgiTracer.create(otel_config)
        tracer.record_execute_metrics(
            function_name="echo",
            function_type="scalar",
            duration_s=0.05,
            input_rows=100,
            output_rows=100,
            input_bytes=4096,
            output_bytes=4096,
        )


class TestExchangeTimer:
    """Tests for _ExchangeTimer and _timed_exchange."""

    def test_noop_timer(self) -> None:
        """Noop timer does not raise on record()."""
        tracer = get_noop_tracer()
        timer = _timed_exchange(tracer, "vgi.execute.scalar", "echo", "scalar", b"\x01\x02")
        with timer:
            timer.record(input_rows=10, output_rows=10)

    def test_real_timer_creates_span(self) -> None:
        """Real timer creates a span with execute attributes."""
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from vgi_rpc.otel import OtelConfig

        exporter = _CollectingExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        with patch("opentelemetry.trace.get_tracer") as mock_get_tracer:
            mock_get_tracer.return_value = provider.get_tracer("vgi")
            tracer = VgiTracer.create(OtelConfig())

        exec_id = b"\xab\xcd"
        timer = _timed_exchange(tracer, "vgi.execute.scalar", "my_func", "scalar", exec_id)
        with timer:
            timer.record(input_rows=50, output_rows=50, input_bytes=2048, output_bytes=2048)

        spans = exporter.spans
        assert len(spans) == 1
        span = spans[0]
        assert span.name == "vgi.execute.scalar"
        assert span.attributes["vgi.function.name"] == "my_func"
        assert span.attributes["vgi.function.type"] == "scalar"
        assert span.attributes["vgi.execute.execution_id"] == exec_id.hex()
        assert span.attributes["vgi.execute.input_rows"] == 50
        assert span.attributes["vgi.execute.output_rows"] == 50
        assert span.attributes["vgi.execute.input_bytes"] == 2048
        assert span.attributes["vgi.execute.output_bytes"] == 2048


class TestBatchBytes:
    """Tests for _batch_bytes helper."""

    def test_normal_batch(self) -> None:
        """Returns positive byte count for a valid batch."""
        batch = pa.record_batch({"x": pa.array([1, 2, 3])})
        assert _batch_bytes(batch) > 0

    def test_handles_error(self) -> None:
        """Returns 0 for non-batch input."""
        assert _batch_bytes("not a batch") == 0


class TestWorkerOtelIntegration:
    """Tests that Worker properly threads VgiTracer."""

    def test_worker_init_has_noop_tracer(self) -> None:
        """Worker.__init__ sets _vgi_tracer to the noop singleton."""
        from vgi.worker import Worker

        class TestWorker(Worker):
            functions = []

        w = TestWorker(quiet=True)
        assert w._vgi_tracer is get_noop_tracer()

    def test_run_with_otel_config_creates_real_tracer(self) -> None:
        """Worker.run(otel_config=...) instruments server and creates real VgiTracer."""
        from vgi_rpc.otel import OtelConfig

        from vgi.worker import Worker

        class TestWorker(Worker):
            functions = []

        w = TestWorker(quiet=True)
        config = OtelConfig()

        with (
            patch("vgi.worker.serve_stdio"),
            patch("vgi_rpc.otel.instrument_server") as mock_instrument,
        ):
            w.run(otel_config=config)
            mock_instrument.assert_called_once()
            assert w._vgi_tracer.enabled

    def test_run_without_otel_config_keeps_noop(self) -> None:
        """Worker.run() without otel_config keeps the noop tracer."""
        from vgi.worker import Worker

        class TestWorker(Worker):
            functions = []

        w = TestWorker(quiet=True)
        with patch("vgi.worker.serve_stdio"):
            w.run()
            assert w._vgi_tracer is get_noop_tracer()
