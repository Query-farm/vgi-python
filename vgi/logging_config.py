# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Shared logging configuration for VGI worker CLIs.

Provides enums, known-logger registry, and a configure function that
mirrors the vgi_rpc CLI logging setup so that ``--debug``, ``--log-level``,
``--log-logger``, and ``--log-format`` behave identically across all
VGI workers.
"""

from __future__ import annotations

import logging
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


def configure_worker_logging(
    *,
    debug: bool = False,
    log_level: LogLevel = LogLevel.INFO,
    log_loggers: list[str] | None = None,
    log_format: LogFormat = LogFormat.text,
) -> int:
    """Configure stdlib logging for a VGI worker process.

    Args:
        debug: If True, force DEBUG on all default loggers (overrides *log_level*).
        log_level: Logging level when *debug* is False.
        log_loggers: Logger names to configure.  Defaults to ``["vgi", "vgi_rpc"]``.
        log_format: Stderr output format (``text`` or ``json``).

    Returns:
        The effective numeric log level.

    """
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
