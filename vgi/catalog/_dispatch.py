# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Call catalog overrides written against the pre-v3 ``CatalogInterface`` signatures.

vgi.v3 added keyword arguments to several ``CatalogInterface`` methods
(``catalog_attach(client_capabilities=)``, ``schema_create(options=)``,
``view_create(aliases=, security=, ...)``, ...) and renamed ``column_name`` to
``column_path``. The worker always passes them, so an override with explicit
parameters and no ``**kwargs`` would raise TypeError on every call.

[`call_catalog`][] inspects the override's signature and passes only what it
accepts. A new argument is dropped only when its value carries no meaning the
old override could have honoured (``None``, empty, a top-level path); then a
DeprecationWarning (and one log line) asks for the new signature. A meaningful
value (a barrier view, a nested column path, schema options) is never silently
dropped: the call fails with an error naming the parameter.
"""

from __future__ import annotations

import inspect
import logging
import threading
import warnings
from collections.abc import Callable, Mapping
from typing import Any

_logger = logging.getLogger("vgi.catalog")


def _always(value: object) -> bool:
    del value
    return True


def _empty(value: object) -> bool:
    return value is None or (isinstance(value, (list, tuple, dict, set)) and not value)


def _top_level(value: object) -> bool:
    return isinstance(value, list) and len(value) == 1


#: method -> added keyword -> predicate: may this value be dropped for an old override?
_ADDED: dict[str, dict[str, Callable[[Any], bool]]] = {
    # The worker selects profiles against the client's offer after attach, so
    # an override that ignores the offer loses nothing.
    "catalog_attach": {"client_capabilities": _always},
    "schema_create": {"options": _empty},
    "view_create": {
        "aliases": _empty,
        "security": lambda value: value == "regular",
        "comment": _empty,
        "tags": _empty,
        "column_comments": _empty,
    },
    "table_create": {
        "constraint_definitions": _empty,
        "comment": _empty,
        "tags": _empty,
        "options": _empty,
        "layout": _empty,
    },
    "table_column_add": {"column_path": _top_level},
    "table_column_type_change": {"column_path": _top_level},
}

#: Methods whose ``column_name`` became ``column_path``.
_RENAMED_COLUMN = frozenset(
    {
        "table_column_comment_set",
        "table_column_drop",
        "table_column_rename",
        "table_column_default_set",
        "table_column_default_drop",
        "table_not_null_drop",
        "table_not_null_set",
    }
)

#: Removed parameters an old override may still require; passed empty when the
#: replacement is empty.
_REMOVED_EMPTY: dict[str, dict[str, Callable[[], Any]]] = {
    "table_create": {
        name: list
        for name in (
            "not_null_constraints",
            "unique_constraints",
            "check_constraints",
            "primary_key_constraints",
            "foreign_key_constraints",
        )
    },
}

_signatures: dict[tuple[type, str], frozenset[str] | None] = {}
_warned: set[tuple[type, str, str]] = set()
_lock = threading.Lock()


def _accepted(catalog: object, name: str) -> frozenset[str] | None:
    """Keyword names the override accepts, or ``None`` when it takes ``**kwargs``."""
    key = (type(catalog), name)
    with _lock:
        if key in _signatures:
            return _signatures[key]
    try:
        parameters = inspect.signature(getattr(catalog, name)).parameters.values()
    except (TypeError, ValueError):
        result = None
    else:
        result = (
            None
            if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in parameters)
            else frozenset(p.name for p in parameters if p.kind != inspect.Parameter.VAR_POSITIONAL)
        )
    with _lock:
        _signatures[key] = result
    return result


def _deprecated(catalog: object, name: str, message: str) -> None:
    owner = type(catalog)
    text = f"{owner.__qualname__}.{name}(): {message}; update the override to the vgi.v3 CatalogInterface signature"
    warnings.warn(text, DeprecationWarning, stacklevel=3)
    key = (owner, name, message)
    with _lock:
        first = key not in _warned
        _warned.add(key)
    if first:
        _logger.warning(text)


def call_catalog(catalog: object, name: str, /, **kwargs: Any) -> Any:
    """Call ``catalog.<name>(**kwargs)``, tolerating a pre-v3 override signature.

    Args:
        catalog: The catalog interface instance.
        name: The ``CatalogInterface`` method name.
        **kwargs: The vgi.v3 keyword arguments.

    Returns:
        Whatever the catalog method returns.

    Raises:
        NotImplementedError: A new argument carries a value the override
            cannot receive (it would otherwise be silently ignored).
    """
    method = getattr(catalog, name)
    accepted = _accepted(catalog, name)
    if accepted is None:
        return method(**kwargs)
    call: dict[str, Any] = {}
    added: Mapping[str, Callable[[Any], bool]] = _ADDED.get(name, {})
    for key, value in kwargs.items():
        if key in accepted:
            call[key] = value
        elif key in added:
            if not added[key](value):
                raise NotImplementedError(
                    f"{type(catalog).__qualname__}.{name}() has no {key!r} parameter, so it cannot honour "
                    f"{key}={value!r}; update the override to the vgi.v3 CatalogInterface signature"
                )
            _deprecated(catalog, name, f"does not accept {key}=, which was dropped")
        elif key == "column_path" and name in _RENAMED_COLUMN and "column_name" in accepted:
            if not _top_level(value):
                raise NotImplementedError(
                    f"{type(catalog).__qualname__}.{name}() takes column_name, so it cannot address nested "
                    f"column path {value!r}; update the override to take column_path"
                )
            call["column_name"] = value[0]
            _deprecated(catalog, name, "takes column_name; pass column_path instead")
        else:
            call[key] = value
    for legacy, empty in _REMOVED_EMPTY.get(name, {}).items():
        if legacy in accepted and legacy not in call:
            call[legacy] = empty()
            _deprecated(catalog, name, f"still takes the removed {legacy} parameter")
    return method(**call)
