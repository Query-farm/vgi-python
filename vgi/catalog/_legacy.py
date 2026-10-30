# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Clear errors for constructor arguments removed by the vgi.v3 protocol.

A removed dataclass field otherwise fails with Python's generic "unexpected
keyword argument". These shims keep the generated ``__init__`` (and its
signature, via ``functools.wraps``) and only replace that error with one
naming the replacement.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Mapping
from typing import Any, TypeVar

T = TypeVar("T", bound=type)


def reject_removed_arguments(removed: Mapping[str, str], *, positional: str | None = None) -> Callable[[T], T]:
    """Wrap a dataclass ``__init__`` so removed arguments raise a guiding TypeError.

    Args:
        removed: Removed keyword name to the message naming its replacement.
        positional: When set, any positional argument raises this message
            (for keyword-only records that used to accept positional values).

    Returns:
        A class decorator.
    """

    def decorate(cls: T) -> T:
        generated: Callable[..., None] = cls.__init__  # type: ignore[misc]

        @functools.wraps(generated)
        def __init__(self: Any, *args: Any, **kwargs: Any) -> None:
            if args and positional is not None:
                raise TypeError(f"{cls.__name__}: {positional}")
            for name in kwargs.keys() & removed.keys():
                raise TypeError(f"{cls.__name__}({name}=...) was removed in vgi.v3: {removed[name]}")
            generated(self, *args, **kwargs)

        cls.__init__ = __init__  # type: ignore[misc]
        return cls

    return decorate
