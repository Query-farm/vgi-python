# OpenTelemetry Tracing

VGI supports distributed tracing via OpenTelemetry. When enabled, worker processes emit spans for function invocations, enabling observability across your data processing pipeline.

## Installation

VGI uses the **library instrumentation pattern**: it depends only on `opentelemetry-api`, and you provide the SDK and exporters based on your observability stack.

```bash
# Install VGI with tracing support
uv add vgi[tracing]

# Install OpenTelemetry SDK and your preferred exporter
uv add opentelemetry-sdk opentelemetry-exporter-otlp
```

Alternative exporters:
- `opentelemetry-exporter-jaeger` - Jaeger
- `opentelemetry-exporter-zipkin` - Zipkin
- `opentelemetry-exporter-prometheus` - Prometheus (metrics only)

## Configuration

VGI auto-configures the OpenTelemetry SDK when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Just set environment variables and run your worker:

```bash
# Basic configuration - tracing auto-enabled when endpoint is set
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_SERVICE_NAME=vgi-worker

# Run your worker - tracing is automatically configured
vgi-example-worker
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint (required to enable tracing) | - |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | Protocol: `grpc` or `http/protobuf` | `grpc` |
| `OTEL_SERVICE_NAME` | Service name in traces | `vgi-worker` |
| `OTEL_EXPORTER_OTLP_HEADERS` | Auth headers (e.g., `Authorization=Bearer token`) | - |
| `OTEL_EXPORTER_OTLP_COMPRESSION` | Compression: `gzip` or `none` | `none` |
| `OTEL_TRACES_SAMPLER` | Sampling strategy | `parentbased_always_on` |
| `OTEL_TRACES_SAMPLER_ARG` | Sampler argument (e.g., `0.1` for 10%) | - |

### Protocol Selection

```bash
# gRPC (default) - typically port 4317
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc

# HTTP/protobuf - typically port 4318
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
```

### Authentication

```bash
# Bearer token auth
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer your-token-here"

# Multiple headers
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer token,X-Custom=value"
```

**Important:** The console exporter will not work since VGI workers use stdout for Arrow IPC data. Use network-based exporters (OTLP, Jaeger, Zipkin).

## Span Hierarchy

VGI creates the following span hierarchy for each function invocation:

```
[Client span - if client has tracing configured]
└── worker.invocation (SERVER)
    ├── worker.bind (INTERNAL)
    ├── worker.init (INTERNAL)
    └── worker.process (INTERNAL)
```

### Span Descriptions

| Span | Kind | Description |
|------|------|-------------|
| `worker.invocation` | SERVER | Root span for the worker, covers the entire invocation |
| `worker.bind` | INTERNAL | Function binding phase (schema resolution, argument validation) |
| `worker.init` | INTERNAL | Initialization phase (global state setup) |
| `worker.process` | INTERNAL | Data processing phase (batch streaming) |

## Span Attributes

### worker.invocation

| Attribute | Type | Description |
|-----------|------|-------------|
| `vgi.function.name` | string | Name of the function being invoked |
| `vgi.function.type` | string | Type: `scalar`, `table`, or `catalog` |
| `vgi.invocation.id` | string | Unique ID for this invocation (hex) |
| `vgi.execution.id` | string | Global execution identifier (hex) |
| `vgi.correlation.id` | string | Client-provided correlation ID |
| `vgi.worker.pid` | int | Worker process ID |
| `vgi.worker.is_primary` | bool | True if this is the primary worker |
| `vgi.input_schema.columns` | int | Number of input columns |
| `vgi.total.batches` | int | Total batches processed |
| `vgi.total.input_rows` | int | Total input rows processed |
| `vgi.total.output_rows` | int | Total output rows produced |
| `vgi.ipc.reader_messages` | int | IPC messages read |
| `vgi.ipc.writer_messages` | int | IPC messages written |

### worker.bind

| Attribute | Type | Description |
|-----------|------|-------------|
| `vgi.max_workers` | int | Maximum parallel workers allowed |
| `vgi.output_schema.columns` | int | Number of output columns |

## Parallel Worker Correlation

When `max_workers > 1`, multiple worker processes run in parallel. These are correlated via:

1. **Shared trace context** - All workers receive the same `traceparent` from the client, so their spans share the same trace ID and have a common parent
2. **Shared `invocation_id`** - All workers for the same logical call share this UUID, captured as `vgi.invocation.id`
3. **Shared `execution_id`** - Links workers to the same execution state, captured as `vgi.execution.id`
4. **`is_primary` flag** - Distinguishes the primary worker (runs init) from secondary workers

This allows trace visualization tools to:
- Group all parallel worker spans under the same parent
- Filter by `vgi.invocation.id` to see all workers for one call
- Distinguish primary from secondary workers

## Trace Context Propagation

VGI uses [W3C Trace Context](https://www.w3.org/TR/trace-context/) for propagation between client and worker processes. The client extracts trace context from the current span (if any) and passes it via the `traceparent` and `tracestate` fields in the invocation.

If your client code has an active span, worker spans will automatically be linked as children:

```python
from opentelemetry import trace

tracer = trace.get_tracer("my-app")

with tracer.start_as_current_span("data-processing"):
    with Client("vgi-example-worker") as client:
        # Worker spans will be children of "data-processing"
        for batch in client.table_function("my_function", Arguments()):
            process(batch)
```

## Graceful Degradation

When `opentelemetry-api` is not installed or no tracer provider is configured:

- No spans are emitted
- Tracing code paths use no-op implementations
- Minimal performance overhead (just boolean checks)

This means you can deploy the same code in environments with and without tracing configured.

## Example: Jaeger Setup

1. Start Jaeger with OTLP support:

```bash
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  jaegertracing/all-in-one:latest
```

2. Install dependencies:

```bash
uv add vgi[tracing] opentelemetry-sdk opentelemetry-exporter-otlp
```

3. Configure and run your worker:

```bash
export OTEL_SERVICE_NAME=vgi-worker
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317

vgi-example-worker
```

4. View traces at http://localhost:16686

## Programmatic Configuration

For advanced use cases (custom exporters, processors, or resource attributes), configure the tracer provider before calling `run()`. VGI will detect the existing provider and skip auto-configuration:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

# Configure resource with custom attributes
resource = Resource.create({
    "service.name": "my-vgi-worker",
    "service.version": "1.0.0",
    "deployment.environment": "production",
})

# Configure tracer provider
provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317"))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

# Now start your worker - it will use YOUR provider (auto-config is skipped)
from my_worker import MyWorker
MyWorker().run()
```

## Troubleshooting

### No spans appearing

1. Verify `OTEL_EXPORTER_OTLP_ENDPOINT` is set: `echo $OTEL_EXPORTER_OTLP_ENDPOINT`
2. Check worker logs for `otel_tracing_configured` message
3. Verify the exporter endpoint is reachable
4. Check sampling configuration (use `OTEL_TRACES_SAMPLER=always_on` for debugging)

### Spans not linked to parent

1. Ensure the client has an active span when calling VGI functions
2. Verify the tracer provider is configured before making VGI calls
3. Check that context propagation is working (same trace ID in client and worker)

### Console output corrupted

The console exporter writes to stdout, which conflicts with Arrow IPC data. Use network-based exporters instead:
- OTLP: `opentelemetry-exporter-otlp`
- Jaeger: `opentelemetry-exporter-jaeger`
- Zipkin: `opentelemetry-exporter-zipkin`

## Attribute Reference

All VGI span attributes use the `vgi.` prefix:

```python
from vgi.tracing import (
    VGI_FUNCTION_NAME,        # "vgi.function.name"
    VGI_FUNCTION_TYPE,        # "vgi.function.type"
    VGI_INVOCATION_ID,        # "vgi.invocation.id"
    VGI_EXECUTION_ID,         # "vgi.execution.id"
    VGI_CORRELATION_ID,       # "vgi.correlation.id"
    VGI_WORKER_PID,           # "vgi.worker.pid"
    VGI_WORKER_IS_PRIMARY,    # "vgi.worker.is_primary"
    VGI_MAX_WORKERS,          # "vgi.max_workers"
    VGI_INPUT_SCHEMA_COLUMNS, # "vgi.input_schema.columns"
    VGI_OUTPUT_SCHEMA_COLUMNS,# "vgi.output_schema.columns"
    VGI_BATCH_INDEX,          # "vgi.batch.index"
    VGI_BATCH_INPUT_ROWS,     # "vgi.batch.input_rows"
    VGI_BATCH_OUTPUT_ROWS,    # "vgi.batch.output_rows"
    VGI_TOTAL_BATCHES,        # "vgi.total.batches"
    VGI_TOTAL_INPUT_ROWS,     # "vgi.total.input_rows"
    VGI_TOTAL_OUTPUT_ROWS,    # "vgi.total.output_rows"
    VGI_TOTAL_INPUT_BYTES,    # "vgi.total.input_bytes"
    VGI_TOTAL_OUTPUT_BYTES,   # "vgi.total.output_bytes"
    VGI_IPC_READER_MESSAGES,  # "vgi.ipc.reader_messages"
    VGI_IPC_WRITER_MESSAGES,  # "vgi.ipc.writer_messages"
)
```
