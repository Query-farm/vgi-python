"""Tests for Worker.main() CLI logging options."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest

from vgi.logging_config import (
    _KNOWN_LOGGERS,
    LogFormat,
    LogLevel,
    configure_worker_logging,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

# All known logger names from the registry
_ALL_LOGGER_NAMES = [name for name, _, _ in _KNOWN_LOGGERS]


@pytest.fixture(autouse=False)
def _reset_loggers() -> Iterator[None]:
    """Save and restore logger handlers and levels after each test."""
    saved: dict[str, tuple[int, list[logging.Handler]]] = {}
    for name in _ALL_LOGGER_NAMES:
        logger = logging.getLogger(name)
        saved[name] = (logger.level, list(logger.handlers))
    yield
    for name in _ALL_LOGGER_NAMES:
        logger = logging.getLogger(name)
        level, handlers = saved[name]
        logger.handlers[:] = handlers
        logger.setLevel(level)


class TestConfigureWorkerLogging:
    """Tests for configure_worker_logging() options."""

    def test_debug_flag(self, _reset_loggers: None) -> None:
        """``--debug`` sets vgi + vgi_rpc loggers to DEBUG."""
        configure_worker_logging(debug=True)
        for name in ("vgi", "vgi_rpc"):
            logger = logging.getLogger(name)
            assert logger.level == logging.DEBUG
            assert len(logger.handlers) == 1

    def test_log_level_option(self, _reset_loggers: None) -> None:
        """``--log-level WARNING`` sets correct level."""
        configure_worker_logging(log_level=LogLevel.WARNING)
        for name in ("vgi", "vgi_rpc"):
            logger = logging.getLogger(name)
            assert logger.level == logging.WARNING

    def test_log_logger_targeting(self, _reset_loggers: None) -> None:
        """``--log-logger vgi.worker`` targets only that logger."""
        configure_worker_logging(log_level=LogLevel.DEBUG, log_loggers=["vgi.worker"])
        target = logging.getLogger("vgi.worker")
        assert target.level == logging.DEBUG
        assert len(target.handlers) == 1
        # Root vgi logger should not have been modified
        root = logging.getLogger("vgi")
        assert root.handlers == [] or root.level != logging.DEBUG

    def test_log_format_json(self, _reset_loggers: None) -> None:
        """``--log-format json`` uses VgiJsonFormatter."""
        from vgi_rpc.logging_utils import VgiJsonFormatter

        configure_worker_logging(debug=True, log_format=LogFormat.json)
        logger = logging.getLogger("vgi")
        assert any(isinstance(h.formatter, VgiJsonFormatter) for h in logger.handlers)

    def test_debug_overrides_log_level(self, _reset_loggers: None) -> None:
        """``--debug --log-level INFO`` resolves to DEBUG."""
        configure_worker_logging(debug=True, log_level=LogLevel.INFO)
        logger = logging.getLogger("vgi")
        assert logger.level == logging.DEBUG

    def test_unknown_logger_warning(self, _reset_loggers: None, capsys: pytest.CaptureFixture[str]) -> None:
        """Unrecognized logger name warns on stderr."""
        configure_worker_logging(log_level=LogLevel.DEBUG, log_loggers=["not.a.real.logger"])
        captured = capsys.readouterr()
        assert "warning: unknown logger 'not.a.real.logger'" in captured.err

    def test_configure_idempotent(self, _reset_loggers: None) -> None:
        """Calling configure_worker_logging() twice doesn't duplicate handlers."""
        configure_worker_logging(debug=True)
        configure_worker_logging(debug=True)
        logger = logging.getLogger("vgi")
        assert len(logger.handlers) == 1

    def test_returns_effective_level(self, _reset_loggers: None) -> None:
        """Return value is the numeric log level."""
        level = configure_worker_logging(log_level=LogLevel.WARNING)
        assert level == logging.WARNING

        level = configure_worker_logging(debug=True, log_level=LogLevel.WARNING)
        assert level == logging.DEBUG

    def test_text_format_has_timestamp(self, _reset_loggers: None) -> None:
        """Text format includes asctime in the formatter."""
        configure_worker_logging(log_format=LogFormat.text)
        logger = logging.getLogger("vgi")
        assert len(logger.handlers) == 1
        fmt = logger.handlers[0].formatter
        assert fmt is not None
        assert fmt._fmt is not None
        assert "asctime" in fmt._fmt
