"""Bounding-box intersection of WKB geometries, as DuckDB 2.0 core `&&` defines it.

This is the reference evaluator for ``duckdb.spatial/intersects_extent@1``
(vgi-filter-encoding-v2-spec.md section 5.11.1). The embedded evaluation engine
cannot be used for it: DuckDB 1.5 treats NaN coordinates differently from 2.0.

Rules (DuckDB core ``Geometry::GetExtent`` plus ``GeometryExtent::IntersectsXY``):

- A geometry with no vertices (any empty geometry) intersects nothing.
- A POINT whose coordinates are all NaN, Z and M included, has no vertex. That
  holds for a standalone point, a MULTIPOINT member and a point in a collection.
- Any other NaN coordinate makes that axis of the box unbounded.
- Infinities are ordinary values; boxes are closed (touching boxes intersect);
  comparisons are exact doubles; Z and M are ignored.

Input is ISO WKB or EWKB (Z/M/SRID flags), in either byte order per
sub-geometry. Malformed input raises ``ValueError``.
"""

from __future__ import annotations

import math
import struct

_EWKB_Z = 0x80000000
_EWKB_M = 0x40000000
_EWKB_SRID = 0x20000000
_MAX_DEPTH = 128
_INF = math.inf


class _Extent:
    __slots__ = ("vertices", "x_max", "x_min", "y_max", "y_min")

    def __init__(self) -> None:
        self.vertices = 0
        self.x_min = self.y_min = _INF
        self.x_max = self.y_max = -_INF

    def add(self, x: float, y: float) -> None:
        self.vertices += 1
        if math.isnan(x):
            self.x_min, self.x_max = -_INF, _INF
        else:
            self.x_min = min(self.x_min, x)
            self.x_max = max(self.x_max, x)
        if math.isnan(y):
            self.y_min, self.y_max = -_INF, _INF
        else:
            self.y_min = min(self.y_min, y)
            self.y_max = max(self.y_max, y)


def _read(wkb: bytes, pos: int, extent: _Extent, depth: int) -> int:
    if depth > _MAX_DEPTH:
        raise ValueError("WKB nesting is too deep")
    if pos + 5 > len(wkb):
        raise ValueError("truncated WKB")
    order = wkb[pos]
    if order not in (0, 1):
        raise ValueError(f"invalid WKB byte order {order}")
    prefix = "<" if order == 1 else ">"
    (raw_type,) = struct.unpack_from(prefix + "I", wkb, pos + 1)
    pos += 5
    ewkb_flags = raw_type & (_EWKB_Z | _EWKB_M | _EWKB_SRID)
    code = raw_type & 0x0FFFFFFF
    if code > 0xFFFF:
        raise ValueError(f"unsupported WKB geometry type {raw_type:#x}")
    base, iso_dims = code % 1000, code // 1000
    if base < 1 or base > 7 or iso_dims > 3:
        raise ValueError(f"unsupported WKB geometry type {raw_type:#x}")
    if iso_dims and ewkb_flags & (_EWKB_Z | _EWKB_M):
        raise ValueError(f"WKB geometry type {raw_type:#x} mixes ISO and EWKB dimensions")
    has_z = bool(ewkb_flags & _EWKB_Z) or iso_dims in (1, 3)
    has_m = bool(ewkb_flags & _EWKB_M) or iso_dims in (2, 3)
    if ewkb_flags & _EWKB_SRID:
        if pos + 4 > len(wkb):
            raise ValueError("truncated WKB")
        pos += 4
    dims = 2 + has_z + has_m
    vertex = struct.Struct(prefix + "d" * dims)

    def count(at: int) -> tuple[int, int]:
        if at + 4 > len(wkb):
            raise ValueError("truncated WKB")
        (n,) = struct.unpack_from(prefix + "I", wkb, at)
        return n, at + 4

    def vertices(at: int, n: int, is_point: bool) -> int:
        end = at + n * vertex.size
        if n < 0 or end > len(wkb):
            raise ValueError("truncated WKB")
        for coords in vertex.iter_unpack(wkb[at:end]):
            if is_point and all(math.isnan(c) for c in coords):
                continue
            extent.add(coords[0], coords[1])
        return end

    if base == 1:
        return vertices(pos, 1, True)
    if base == 2:
        n, pos = count(pos)
        return vertices(pos, n, False)
    if base == 3:
        rings, pos = count(pos)
        if rings > (len(wkb) - pos) // 4:
            raise ValueError("truncated WKB")
        for _ in range(rings):
            n, pos = count(pos)
            pos = vertices(pos, n, False)
        return pos
    parts, pos = count(pos)
    if parts > (len(wkb) - pos) // 5:
        raise ValueError("truncated WKB")
    for _ in range(parts):
        pos = _read(wkb, pos, extent, depth + 1)
    return pos


def wkb_extent(wkb: bytes) -> tuple[int, float, float, float, float]:
    """``(vertex_count, x_min, y_min, x_max, y_max)`` of a WKB geometry."""
    extent = _Extent()
    end = _read(wkb, 0, extent, 0)
    if end != len(wkb):
        raise ValueError("trailing bytes after WKB geometry")
    return extent.vertices, extent.x_min, extent.y_min, extent.x_max, extent.y_max


def intersects_extent(left: bytes, right: bytes) -> bool:
    """DuckDB core ``left && right`` for two WKB geometries."""
    ln, lx0, ly0, lx1, ly1 = wkb_extent(left)
    rn, rx0, ry0, rx1, ry1 = wkb_extent(right)
    if ln == 0 or rn == 0:
        return False
    return not (lx0 > rx1 or lx1 < rx0 or ly0 > ry1 or ly1 < ry0)
