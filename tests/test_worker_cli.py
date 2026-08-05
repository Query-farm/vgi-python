# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for Worker.main() CLI logging options."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest

from vgi.logging_config import (
    _KNOWN_LOGGERS,
    ACCESS_LOGGER,
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
    """Save and restore logger handlers, levels and propagation after each test.

    ``propagate`` is restored too because the access-log options turn it off on
    ``vgi_rpc.access``; leaving that set would silently suppress access records
    for every later test in the session.
    """
    saved: dict[str, tuple[int, list[logging.Handler], bool]] = {}
    for name in _ALL_LOGGER_NAMES:
        logger = logging.getLogger(name)
        saved[name] = (logger.level, list(logger.handlers), logger.propagate)
    yield
    for name in _ALL_LOGGER_NAMES:
        logger = logging.getLogger(name)
        level, handlers, propagate = saved[name]
        logger.handlers[:] = handlers
        logger.setLevel(level)
        logger.propagate = propagate


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


class TestAccessLogOptions:
    """Sampling and async emission for ``vgi_rpc.access``."""

    def test_untouched_by_default(self, _reset_loggers: None) -> None:
        """No options means no dedicated handler — records still propagate."""
        configure_worker_logging(debug=True)
        access = logging.getLogger(ACCESS_LOGGER)
        assert access.handlers == []
        assert access.propagate is True

    def test_sampling_attaches_filter(self, _reset_loggers: None) -> None:
        """A sample rate below 1.0 installs the sampler on the access logger."""
        from vgi_rpc.logging_utils import AccessLogSampler

        configure_worker_logging(debug=True, access_log_sample=0.25)
        access = logging.getLogger(ACCESS_LOGGER)
        assert len(access.handlers) == 1
        assert any(isinstance(f, AccessLogSampler) for f in access.handlers[0].filters)

    def test_sampling_does_not_touch_diagnostic_loggers(self, _reset_loggers: None) -> None:
        """Dropping a fraction of tracebacks is not a saving.

        The access logger gets its own handler precisely so the sampler cannot
        reach ``vgi`` / ``vgi_rpc``, which share the stderr handler.
        """
        from vgi_rpc.logging_utils import AccessLogSampler

        configure_worker_logging(debug=True, access_log_sample=0.25)
        for name in ("vgi", "vgi_rpc"):
            for handler in logging.getLogger(name).handlers:
                assert not any(isinstance(f, AccessLogSampler) for f in handler.filters)

    def test_sampling_stops_propagation(self, _reset_loggers: None) -> None:
        """Otherwise every surviving record is written twice."""
        configure_worker_logging(debug=True, access_log_sample=0.5)
        assert logging.getLogger(ACCESS_LOGGER).propagate is False

    def test_async_uses_queue_handler(self, _reset_loggers: None) -> None:
        """Async emission puts a bounded queue on the request thread's path."""
        from vgi_rpc.logging_utils import DroppingQueueHandler

        configure_worker_logging(debug=True, access_log_async=True, access_log_queue_size=64)
        access = logging.getLogger(ACCESS_LOGGER)
        assert len(access.handlers) == 1
        assert isinstance(access.handlers[0], DroppingQueueHandler)

    def test_json_format_carries_to_access_handler(self, _reset_loggers: None) -> None:
        """The dedicated handler must not silently revert to text output."""
        from vgi_rpc.logging_utils import VgiJsonFormatter

        configure_worker_logging(debug=True, log_format=LogFormat.json, access_log_sample=0.5)
        access = logging.getLogger(ACCESS_LOGGER)
        assert isinstance(access.handlers[0].formatter, VgiJsonFormatter)

    @pytest.mark.parametrize("rate", [-0.1, 1.5])
    def test_out_of_range_sample_rate_exits(self, _reset_loggers: None, rate: float) -> None:
        """A typo here silently changes how much of the audit trail survives."""
        with pytest.raises(SystemExit):
            configure_worker_logging(debug=True, access_log_sample=rate)

    def test_non_positive_queue_size_exits(self, _reset_loggers: None) -> None:
        """A zero-length queue would drop every record."""
        with pytest.raises(SystemExit):
            configure_worker_logging(debug=True, access_log_async=True, access_log_queue_size=0)

    def test_env_overrides_win(self, _reset_loggers: None, monkeypatch: pytest.MonkeyPatch) -> None:
        """Reachable without owning argv, like the other VGI_WORKER_LOG_* vars."""
        from vgi_rpc.logging_utils import AccessLogSampler

        monkeypatch.setenv("VGI_WORKER_ACCESS_LOG_SAMPLE", "0.1")
        configure_worker_logging(debug=True)
        access = logging.getLogger(ACCESS_LOGGER)
        assert any(isinstance(f, AccessLogSampler) for f in access.handlers[0].filters)

    def test_malformed_env_exits(self, _reset_loggers: None, monkeypatch: pytest.MonkeyPatch) -> None:
        """A typo must not leave sampling silently off."""
        monkeypatch.setenv("VGI_WORKER_ACCESS_LOG_SAMPLE", "half")
        with pytest.raises(SystemExit):
            configure_worker_logging(debug=True)
