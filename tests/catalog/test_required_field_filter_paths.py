"""Unit tests for the ``Table.required_field_filter_paths`` field.

The field is purely declarative on the Python side — enforcement runs in the C++
optimizer extension. These tests cover:

1. Default-empty behaviour (no field set → ``TableInfo`` ships an empty list).
2. Populated round-trip (top-level + struct subfield + nested paths preserved).
3. Validation: each path's leading dotted segment must be a real column.
4. Empty-string entries are rejected loudly.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi.catalog.descriptors import Table


class TestRequiredFieldFilterPathsField:
    """Behaviour of the declarative required_field_filter_paths field."""

    def _bbox_columns(self) -> pa.Schema:
        """Return a small schema with a ``bbox`` STRUCT(xmin,ymin,xmax,ymax)."""
        return pa.schema(
            [
                ("id", pa.int64()),
                (
                    "bbox",
                    pa.struct(
                        [
                            ("xmin", pa.float64()),
                            ("ymin", pa.float64()),
                            ("xmax", pa.float64()),
                            ("ymax", pa.float64()),
                        ]
                    ),
                ),
            ]
        )

    def test_default_is_empty_tuple(self) -> None:
        """Default is an empty tuple; TableInfo wire field is an empty list."""
        t = Table(name="t", columns=self._bbox_columns())
        assert t.required_field_filter_paths == ()
        info = t.to_table_info("main")
        assert info.required_field_filter_paths == []

    def test_populated_round_trip(self) -> None:
        """Populated paths survive through to_table_info() as a list."""
        paths = ("bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax")
        t = Table(
            name="place",
            columns=self._bbox_columns(),
            required_field_filter_paths=paths,
        )
        assert t.required_field_filter_paths == paths
        info = t.to_table_info("main")
        assert info.required_field_filter_paths == list(paths)

    def test_top_level_path(self) -> None:
        """A top-level column name (no dots) is a valid path."""
        t = Table(
            name="t",
            columns=self._bbox_columns(),
            required_field_filter_paths=("id",),
        )
        assert t.to_table_info("main").required_field_filter_paths == ["id"]

    def test_nested_path_is_not_unpacked_for_validation(self) -> None:
        """Only the leading dotted segment is validated; deeper segments pass through."""
        # Subfield validity isn't checked here (descriptor doesn't unpack
        # struct schemas) — only the leading segment matters. Typos like
        # ``bbox.nope`` go through and DuckDB catches them at scan time.
        t = Table(
            name="t",
            columns=self._bbox_columns(),
            required_field_filter_paths=("bbox.nope",),
        )
        assert t.required_field_filter_paths == ("bbox.nope",)

    def test_unknown_leading_segment_raises(self) -> None:
        """A path whose leading segment is not a real column is rejected."""
        with pytest.raises(ValueError, match=r"unknown column 'nope'"):
            Table(
                name="t",
                columns=self._bbox_columns(),
                required_field_filter_paths=("nope.xmin",),
            )

    def test_empty_string_path_raises(self) -> None:
        """An empty string path is rejected loudly rather than silently accepted."""
        with pytest.raises(ValueError, match="must not contain empty strings"):
            Table(
                name="t",
                columns=self._bbox_columns(),
                required_field_filter_paths=("",),
            )
