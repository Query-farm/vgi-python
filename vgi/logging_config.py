# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Shared logging configuration for VGI worker CLIs.

Provides enums, known-logger registry, and a configure function that
mirrors the vgi_rpc CLI logging setup so that ``--debug``, ``--log-level``,
``--log-logger``, and ``--log-format`` behave identically across all
VGI workers.
"""

from __future__ import annotations

import logging
import os
import sys
from enum import StrEnum


class LogLevel(StrEnum):
    """Python logging level for ``--log-level``."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class LogFormat(StrEnum):
    """Stderr log format for ``--log-format``."""

    text = "text"
    json = "json"


# (name, description, typical-scenario)
_KNOWN_LOGGERS: list[tuple[str, str, str]] = [
    ("vgi", "VGI root logger", "all VGI messages"),
    ("vgi.worker", "Worker lifecycle", "startup, shutdown"),
    ("vgi.client", "Client operations", "spawn, bind, exchange"),
    ("vgi.client.cli", "CLI front-end", "argument parsing"),
    ("vgi.filter_pushdown", "Filter pushdown debug", "filter deserialization / evaluation"),
    ("vgi_rpc", "vgi_rpc root logger", "all vgi_rpc messages"),
    ("vgi_rpc.access", "RPC access log (enriched by VGI)", "per-request structured access log"),
    ("vgi_rpc.wire.request", "RPC wire request", "serialised request bytes"),
    ("vgi_rpc.wire.response", "RPC wire response", "serialised response bytes"),
    ("vgi_rpc.wire.transport", "Transport layer", "pipe / HTTP transport debug"),
]


def _env_overrides(
    log_level: LogLevel,
    log_loggers: list[str] | None,
    log_format: LogFormat,
) -> tuple[LogLevel, list[str] | None, LogFormat]:
    """Apply ``VGI_WORKER_LOG_*`` environment overrides to the CLI options.

    The access log (``vgi_rpc.access``) already records ``duration_ms``,
    ``method`` and row/byte counts for every RPC, but reaching it required
    passing ``--log-logger vgi_rpc.access --log-format json`` on the command
    line. Anything that launches a worker without owning its argv — a container
    image, a test harness spawning workers from a dozen call sites — could not
    turn it on. These variables make the same configuration reachable the way
    ``VGI_SIGNING_KEY`` and ``VGI_ENABLE_DESCRIBE`` already are.

    Applied here rather than in a single CLI so that *every* worker entry point
    honours them identically — ``vgi-serve`` and the fixture servers had
    byte-identical logging blocks, and an override wired into only one of them
    would be a trap.

    Each variable wins when set, matching ``VGI_ENABLE_DESCRIBE``: Typer cannot
    distinguish "user passed the default" from "user passed nothing", so
    deferring to an explicit environment setting is the only unambiguous rule.

    Args:
        log_level: The ``--log-level`` value.
        log_loggers: The ``--log-logger`` values, if any.
        log_format: The ``--log-format`` value.

    Returns:
        The effective ``(level, loggers, format)``.

    Raises:
        SystemExit: If a variable names a level or format that does not exist.
            A typo would otherwise silently leave logging as it was, which is
            indistinguishable from the feature not working.
    """
    raw_level = os.environ.get("VGI_WORKER_LOG_LEVEL")
    if raw_level:
        try:
            log_level = LogLevel(raw_level.upper())
        except ValueError:
            valid = ", ".join(level.value for level in LogLevel)
            sys.exit(f"VGI_WORKER_LOG_LEVEL={raw_level!r} is not a log level (expected one of: {valid})")

    raw_format = os.environ.get("VGI_WORKER_LOG_FORMAT")
    if raw_format:
        try:
            log_format = LogFormat(raw_format.lower())
        except ValueError:
            valid = ", ".join(fmt.value for fmt in LogFormat)
            sys.exit(f"VGI_WORKER_LOG_FORMAT={raw_format!r} is not a log format (expected one of: {valid})")

    raw_loggers = os.environ.get("VGI_WORKER_LOG_LOGGERS")
    if raw_loggers:
        names = [name.strip() for name in raw_loggers.split(",") if name.strip()]
        if names:
            log_loggers = names

    return log_level, log_loggers, log_format


ACCESS_LOGGER = "vgi_rpc.access"


def _access_log_overrides(
    sample: float | None,
    use_async: bool,
    queue_size: int | None,
) -> tuple[float, bool, int]:
    """Apply ``VGI_WORKER_ACCESS_LOG_*`` overrides and validate the result.

    Same rule as :func:`_env_overrides` — an explicit environment setting wins,
    because Typer cannot distinguish "user passed the default" from "user passed
    nothing".

    Args:
        sample: ``--access-log-sample``, or None if not passed.
        use_async: ``--access-log-async``.
        queue_size: ``--access-log-queue-size``, or None if not passed.

    Returns:
        The effective ``(sample_rate, use_async, queue_size)``.

    Raises:
        SystemExit: On a value that is not a number, or a sample rate outside
            ``[0.0, 1.0]``.  A typo here silently changes how much of the audit
            trail survives, which is not something to discover from a gap in a
            dashboard months later.

    """
    raw_sample = os.environ.get("VGI_WORKER_ACCESS_LOG_SAMPLE")
    if raw_sample:
        try:
            sample = float(raw_sample)
        except ValueError:
            sys.exit(f"VGI_WORKER_ACCESS_LOG_SAMPLE={raw_sample!r} is not a number")

    raw_async = os.environ.get("VGI_WORKER_ACCESS_LOG_ASYNC")
    if raw_async:
        use_async = raw_async.strip().lower() in ("1", "true", "yes")

    raw_queue = os.environ.get("VGI_WORKER_ACCESS_LOG_QUEUE_SIZE")
    if raw_queue:
        try:
            queue_size = int(raw_queue)
        except ValueError:
            sys.exit(f"VGI_WORKER_ACCESS_LOG_QUEUE_SIZE={raw_queue!r} is not an integer")

    effective_sample = 1.0 if sample is None else sample
    if not 0.0 <= effective_sample <= 1.0:
        sys.exit(f"access-log sample rate must be between 0.0 and 1.0, got {effective_sample}")

    effective_queue = 10000 if queue_size is None else queue_size
    if effective_queue <= 0:
        sys.exit(f"access-log queue size must be positive, got {effective_queue}")

    return effective_sample, use_async, effective_queue


def configure_worker_logging(
    *,
    debug: bool = False,
    log_level: LogLevel = LogLevel.INFO,
    log_loggers: list[str] | None = None,
    log_format: LogFormat = LogFormat.text,
    access_log_sample: float | None = None,
    access_log_async: bool = False,
    access_log_queue_size: int | None = None,
) -> int:
    """Configure stdlib logging for a VGI worker process.

    ``VGI_WORKER_LOG_LEVEL``, ``VGI_WORKER_LOG_FORMAT`` and
    ``VGI_WORKER_LOG_LOGGERS`` override the corresponding arguments when set —
    see :func:`_env_overrides`.  The access-log arguments have their own
    ``VGI_WORKER_ACCESS_LOG_*`` overrides; see :func:`_access_log_overrides`.

    Sampling and async emission apply only to ``vgi_rpc.access``, and only when
    that logger is among the configured targets.  They are deliberately not
    applied to the diagnostic loggers: dropping half of a traceback is not a
    saving.

    Trace correlation needs no configuration here — vgi_rpc reads whatever span
    is current when it emits a record, so ``trace_id`` / ``span_id`` appear on
    every record as soon as OpenTelemetry is active (``VGI_OTEL_ENABLED=1``).

    Args:
        debug: If True, force DEBUG on all default loggers (overrides *log_level*).
        log_level: Logging level when *debug* is False.
        log_loggers: Logger names to configure.  Defaults to ``["vgi", "vgi_rpc"]``.
        log_format: Stderr output format (``text`` or ``json``).
        access_log_sample: Fraction of *successful* calls to keep in the access
            log.  Errors are always kept, and the decision is per call so every
            record belonging to one stream shares a fate.  ``None`` keeps all.
        access_log_async: Hand access-log records to a listener thread rather
            than formatting and writing them on the request thread.  Trades
            durability for latency: the queue is bounded, full means drop, and
            a crash loses whatever is still queued.  The next record through
            carries ``dropped_records`` so a gap is never silent.
        access_log_queue_size: Bound on the async queue.  Ignored unless
            *access_log_async*.  ``None`` uses 10000.

    Returns:
        The effective numeric log level.

    """
    log_level, log_loggers, log_format = _env_overrides(log_level, log_loggers, log_format)
    sample_rate, use_async, queue_size = _access_log_overrides(
        access_log_sample, access_log_async, access_log_queue_size
    )
    effective_level = logging.DEBUG if debug else getattr(logging, log_level.value)

    handler = logging.StreamHandler(sys.stderr)

    if log_format == LogFormat.json:
        from vgi_rpc.logging_utils import VgiJsonFormatter

        handler.setFormatter(VgiJsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(name)-30s %(levelname)-5s %(message)s", datefmt="%H:%M:%S")
        )

    targets = log_loggers if log_loggers else ["vgi", "vgi_rpc"]

    known_names = {name for name, _, _ in _KNOWN_LOGGERS}
    for name in targets:
        if name not in known_names:
            # Still configure it — the user may know what they're doing
            sys.stderr.write(f"warning: unknown logger {name!r}\n")
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.setLevel(effective_level)
        logger.addHandler(handler)

    _configure_access_log_handler(
        formatter=handler.formatter,
        sample_rate=sample_rate,
        use_async=use_async,
        queue_size=queue_size,
    )

    return effective_level


def _configure_access_log_handler(
    *,
    formatter: logging.Formatter | None,
    sample_rate: float,
    use_async: bool,
    queue_size: int,
) -> None:
    """Give ``vgi_rpc.access`` its own handler with sampling and/or async emission.

    A *separate* handler rather than a filter on the shared stderr one: that
    handler also carries ``vgi`` and ``vgi_rpc``, so sampling it would drop
    diagnostics at the same rate — dropping half of a traceback is not a
    saving.  Same formatter, so the output format is unchanged.

    The access logger normally has no handler of its own; records reach stderr
    by propagating to ``vgi_rpc``.  Once it has one, propagation is turned off,
    or every sampled-in record would be written twice.

    Args:
        formatter: Formatter from the shared stderr handler, so text/json
            selection carries over.
        sample_rate: Fraction of successful calls to keep; ``1.0`` keeps all.
        use_async: Whether to emit from a listener thread.
        queue_size: Bound on the async queue.

    """
    if sample_rate >= 1.0 and not use_async:
        return

    access_handler: logging.Handler = logging.StreamHandler(sys.stderr)
    access_handler.setFormatter(formatter)

    if sample_rate < 1.0:
        from vgi_rpc.logging_utils import AccessLogSampler

        # On the handler rather than the logger: a logger-level filter would
        # also suppress records for any handler the application attached.
        access_handler.addFilter(AccessLogSampler(sample_rate))

    access_logger = logging.getLogger(ACCESS_LOGGER)
    access_logger.handlers.clear()

    if use_async:
        import atexit
        import queue as _queue
        from logging.handlers import QueueListener

        from vgi_rpc.logging_utils import DroppingQueueHandler

        record_queue: _queue.Queue[logging.LogRecord] = _queue.Queue(maxsize=queue_size)
        listener = QueueListener(record_queue, access_handler)
        # QueueListener.start() already marks its thread daemon, so a stuck
        # writer cannot hold up exit. Setting it again afterwards raises
        # "cannot set daemon status of active thread".
        listener.start()
        # Drains whatever is still queued at exit; without it a clean shutdown
        # loses the tail of the access log.
        atexit.register(listener.stop)
        access_logger.addHandler(DroppingQueueHandler(record_queue))
    else:
        access_logger.addHandler(access_handler)

    # Emitted by this logger's own handler now; propagating as well would
    # write every surviving record twice.
    access_logger.propagate = False
