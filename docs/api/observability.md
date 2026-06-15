# Observability

!!! note "Advanced — reference only"
    OpenTelemetry tracing and Sentry reporting are advanced, opt-in features without a dedicated
    how-to guide yet. This page is the API reference.

Workers can emit OpenTelemetry traces and structured logs. Tracing is a no-op unless the `[otel]`
extra is installed and a tracer provider is configured; logging configuration helpers shape worker
log output for the CLIs.

## OpenTelemetry

Requires `pip install vgi-python[otel]` for live tracing; otherwise a no-op tracer is used.

::: vgi.otel

## Logging configuration

::: vgi.logging_config
