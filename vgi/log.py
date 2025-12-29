"""Logging utilities for VGI functions.

This module provides Level and Message for emitting diagnostic information
during function processing. Log messages are attached to output metadata and
transmitted to the client alongside output batches.

Classes:
    Level: Severity levels for log messages.
    Message: Log message that can be yielded from process() or finalize().

Example:
    from vgi.log import Level, Message

    # Yield directly during processing
    yield Message(Level.INFO, f"Processing {batch.num_rows} rows")

    # Or attach to Output
    yield Output(batch, log_message=Message.info("Processed batch"))

"""

import json
import traceback
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from vgi.function import Request

__all__ = [
    "Level",
    "Message",
]


class Level(Enum):
    """Severity levels for log messages emitted during function processing.

    Levels are ordered from most to least severe. Use the appropriate level
    to indicate the nature of the message:

    Attributes:
        EXCEPTION: Unrecoverable error that terminated processing.
        ERROR: Significant error that may affect results but didn't terminate.
        WARN: Potential issue that should be reviewed but isn't necessarily wrong.
        INFO: General informational message about processing status.
        DEBUG: Detailed information useful for debugging.
        TRACE: Fine-grained tracing information for detailed diagnostics.

    """

    EXCEPTION = "EXCEPTION"
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"
    DEBUG = "DEBUG"
    TRACE = "TRACE"


class Message:
    """Log message that can be yielded from process() directly or via Result.

    Message allows functions to emit diagnostic information during batch
    processing. Messages are attached to the output metadata and transmitted
    to the client alongside the output batch.

    Attributes:
        level: Severity level indicating the nature of the message.
        message: Human-readable log message text.
        extra: Additional arbitrary key-value pairs to include in the JSON output.

    Example (via Result):
        def process(self) -> ResultGenerator:
            _ = yield None
            while batch := (yield None):
                yield Result(
                    batch,
                    log_message=Message(Level.INFO, "Processed batch")
                )

    Example (yielded directly):
        def process(self) -> ResultGenerator:
            _ = yield None
            while batch := (yield None):
                yield Message(Level.INFO, f"Processing {batch.num_rows} rows")
                yield Result(batch)

    """

    __slots__ = ("level", "message", "extra")
    __hash__ = None  # type: ignore[assignment]  # Unhashable since we define __eq__

    _MAX_TRACEBACK_CHARS: ClassVar[int] = 16_000

    def __init__(self, level: Level, message: str, **kwargs: Any) -> None:
        """Create a log message with level, message text, and optional extras."""
        self.level = level
        self.message = message
        self.extra: dict[str, Any] | None = kwargs if kwargs else None

    def __eq__(self, other: object) -> bool:
        """Compare log messages by level, message, and extra fields."""
        if not isinstance(other, Message):
            return NotImplemented
        return (
            self.level == other.level
            and self.message == other.message
            and self.extra == other.extra
        )

    def __repr__(self) -> str:
        """Return a string representation suitable for debugging."""
        if self.extra:
            return f"Message({self.level!r}, {self.message!r}, **{self.extra!r})"
        return f"Message({self.level!r}, {self.message!r})"

    @classmethod
    def exception(cls, message: str, **kwargs: Any) -> "Message":
        """Create an EXCEPTION level log message."""
        return cls(Level.EXCEPTION, message, **kwargs)

    @classmethod
    def error(cls, message: str, **kwargs: Any) -> "Message":
        """Create an ERROR level log message."""
        return cls(Level.ERROR, message, **kwargs)

    @classmethod
    def info(cls, message: str, **kwargs: Any) -> "Message":
        """Create an INFO level log message."""
        return cls(Level.INFO, message, **kwargs)

    @classmethod
    def warn(cls, message: str, **kwargs: Any) -> "Message":
        """Create a WARN level log message."""
        return cls(Level.WARN, message, **kwargs)

    @classmethod
    def debug(cls, message: str, **kwargs: Any) -> "Message":
        """Create a DEBUG level log message."""
        return cls(Level.DEBUG, message, **kwargs)

    @classmethod
    def trace(cls, message: str, **kwargs: Any) -> "Message":
        """Create a TRACE level log message."""
        return cls(Level.TRACE, message, **kwargs)

    def add_to_metadata(
        self, invocation: "Request", metadata: dict[str, str] | None = None
    ) -> dict[str, str]:
        """Add log message fields to an existing metadata dictionary.

        Creates a new dictionary with log-related keys added. Does not mutate
        the input dictionary.

        Args:
            invocation: The Request for this function invocation, used
                to include the correlation_id and invocation_id for correlation.
            metadata: Existing metadata dict to augment, or None to create new.

        Returns:
            New dict containing original entries plus:
            - log_level: The Level value (e.g., "INFO", "EXCEPTION")
            - log_message: The human-readable message text
            - log_extra: JSON string with {correlation_id, invocation_id,
                pid, ...extra kwargs}

        """
        result = dict(metadata) if metadata else {}
        result["log_level"] = self.level.value
        log_data: dict[str, Any] = {
            "correlation_id": invocation.correlation_id,
            "invocation_id": invocation.invocation_id.hex()
            if invocation.invocation_id
            else None,
            "pid": invocation.pid(),
        }
        if self.extra:
            log_data.update(self.extra)
        result["log_message"] = self.message
        result["log_extra"] = json.dumps(log_data)
        return result

    @classmethod
    def from_exception(cls, exc: BaseException) -> "Message":
        """Produce a Message from an exception."""
        tb_exc = traceback.TracebackException.from_exception(
            exc,
            capture_locals=False,
        )

        formatted_tb = "".join(tb_exc.format())
        if len(formatted_tb) > cls._MAX_TRACEBACK_CHARS:
            formatted_tb = (
                formatted_tb[: cls._MAX_TRACEBACK_CHARS] + "\n… <traceback truncated>"
            )

        # Short, semantic summary (LLM anchor)
        summary = f"{type(exc).__name__}: {exc}"

        extra: dict[str, Any] = {
            "exception_type": type(exc).__name__,
            "exception_message": str(exc),
            "traceback": formatted_tb,
        }

        if tb_exc.__cause__:
            extra["cause"] = "".join(tb_exc.__cause__.format())

        if tb_exc.__context__ and not tb_exc.__suppress_context__:
            extra["context"] = "".join(tb_exc.__context__.format())

        extra["frames"] = [
            {
                "file": f.filename,
                "line": f.lineno,
                "function": f.name,
                "code": f.line,
            }
            for f in tb_exc.stack[-5:]  # last N frames only
        ]

        return cls(
            Level.EXCEPTION,
            summary,
            **extra,
        )
