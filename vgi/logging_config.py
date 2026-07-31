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


def configure_worker_logging(
    *,
    debug: bool = False,
    log_level: LogLevel = LogLevel.INFO,
    log_loggers: list[str] | None = None,
    log_format: LogFormat = LogFormat.text,
) -> int:
    """Configure stdlib logging for a VGI worker process.

    ``VGI_WORKER_LOG_LEVEL``, ``VGI_WORKER_LOG_FORMAT`` and
    ``VGI_WORKER_LOG_LOGGERS`` override the corresponding arguments when set —
    see :func:`_env_overrides`.

    Args:
        debug: If True, force DEBUG on all default loggers (overrides *log_level*).
        log_level: Logging level when *debug* is False.
        log_loggers: Logger names to configure.  Defaults to ``["vgi", "vgi_rpc"]``.
        log_format: Stderr output format (``text`` or ``json``).

    Returns:
        The effective numeric log level.

    """
    log_level, log_loggers, log_format = _env_overrides(log_level, log_loggers, log_format)
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

    return effective_level
