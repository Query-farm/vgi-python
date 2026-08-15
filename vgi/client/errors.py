# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Error type shared by every client driver.

``ClientError`` lives here rather than in ``vgi.client.client`` so the
per-family driver modules (``vgi.client.aggregate`` and friends) can raise it
without importing the module that imports *them*. ``vgi.client.client``
re-exports it, so ``from vgi.client.client import ClientError`` keeps working.
"""

from __future__ import annotations

from vgi_rpc.rpc import RpcError

__all__ = ["ClientError"]


class ClientError(Exception):
    """Error raised by Client operations.

    The first line of ``str(ClientError)`` is the remote exception as the
    worker raised it (``{error_type}: {error_message}``), so that whatever
    a user typed into their `raise ValueError(...)` shows up at the top of
    their traceback instead of being buried under VGI framing. Remote
    traceback and worker-stderr excerpts, when present, follow after an
    empty line.
    """

    @classmethod
    def from_rpc_error(cls, e: RpcError) -> ClientError:
        """Create a [`ClientError`][] from an `RpcError`, including remote traceback.

        Lead with the user's exception (``error_type: error_message``) so
        the most actionable line is first. The ``Remote traceback`` section
        trails and is only included when the worker produced one.
        """
        # str(e) is already "error_type: error_message" from RpcError.__init__.
        parts: list[str] = [str(e)]
        if getattr(e, "remote_traceback", ""):
            parts.append(f"Remote traceback:\n{e.remote_traceback}")
        return cls("\n\n".join(parts))
