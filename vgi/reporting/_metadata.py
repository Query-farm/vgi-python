# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Read annotations for documentation and the future reporting SQL adapter.

These annotations describe contracts only. They register no SQL functions and
do not change vgi-rpc dispatch or implement a service.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from vgi_rpc import ArrowSerializableDataclass

_F = TypeVar("_F", bound=Callable[..., Any])


@dataclass(frozen=True)
class FetchedColumn:
    """One SQL column fetched from a get method using fields of the list row."""

    column: str
    method: str
    arguments: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class SqlRead:
    """Explicit permission to expose a protocol read through SQL.

    Producer return annotations describe state, not output rows, in vgi-rpc.
    ``row_type`` supplies that missing contract; unary rows come from the return
    dataclass. ``table`` opts a read into a table in addition to its function.
    """

    row_type: type[ArrowSerializableDataclass] | None = None
    table: str | None = None
    fetched_columns: tuple[FetchedColumn, ...] = ()


def sql_read(
    *,
    row_type: type[ArrowSerializableDataclass] | None = None,
    table: str | None = None,
    fetched_columns: tuple[FetchedColumn, ...] = (),
) -> Callable[[_F], _F]:
    """Annotate a safe read without wrapping the method or changing its signature."""
    annotation = SqlRead(row_type=row_type, table=table, fetched_columns=fetched_columns)

    def decorate(method: _F) -> _F:
        setattr(method, "__reporting_sql_read__", annotation)  # noqa: B010 - preserve the callable's static type
        return method

    return decorate


def read_annotation(method: object) -> SqlRead | None:
    """Return a method's explicit read annotation, if any."""
    annotation = getattr(method, "__reporting_sql_read__", None)
    return annotation if isinstance(annotation, SqlRead) else None
