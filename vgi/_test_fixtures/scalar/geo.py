"""Geospatial scalar fixtures (geo_distance_*, geo_centroid_*)."""

from __future__ import annotations

from typing import Annotated, Any

import pyarrow as pa
import pyarrow.compute as pc

from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

_POINT_STRUCT_TYPE = pa.struct([("lat", pa.float64()), ("lon", pa.float64())])


def _euclidean_distance(
    lat1: pa.Array[Any], lon1: pa.Array[Any], lat2: pa.Array[Any], lon2: pa.Array[Any]
) -> pa.DoubleArray:
    """Compute Euclidean distance: sqrt((lat2-lat1)^2 + (lon2-lon1)^2)."""
    dlat = pc.subtract(lat2, lat1)
    dlon = pc.subtract(lon2, lon1)
    return pc.sqrt(pc.add(pc.multiply(dlat, dlat), pc.multiply(dlon, dlon)))  # type: ignore[return-value]


def _compute_centroid(lat_arrays: list[pa.Array[Any]], lon_arrays: list[pa.Array[Any]]) -> pa.StructArray:
    """Compute centroid (average lat, average lon) from parallel lat/lon arrays."""
    n = len(lat_arrays)
    lat_sum: pa.Array[Any] = lat_arrays[0]
    lon_sum: pa.Array[Any] = lon_arrays[0]
    for i in range(1, n):
        lat_sum = pc.add(lat_sum, lat_arrays[i])
        lon_sum = pc.add(lon_sum, lon_arrays[i])
    divisor = pa.scalar(n, type=pa.float64())
    avg_lat = pc.divide(lat_sum, divisor)
    avg_lon = pc.divide(lon_sum, divisor)
    return pa.StructArray.from_arrays([avg_lat, avg_lon], names=["lat", "lon"])


class GeoDistanceStructFunction(ScalarFunction):
    """Euclidean distance between two struct points.

    Each point is a struct with lat and lon fields.

    Example:
        SQL:    SELECT geo_distance_struct(p1, p2) FROM points
        Input:  p1={lat: 0.0, lon: 0.0}, p2={lat: 3.0, lon: 4.0}
        Output: result=5.0

    """

    class Meta:
        """Function metadata."""

        name = "geo_distance_struct"
        description = "Euclidean distance between two struct points"
        examples = [
            FunctionExample(
                sql="SELECT geo_distance_struct({lat: 0, lon: 0}, {lat: 3, lon: 4})",
                description="Distance between origin and (3, 4)",
            ),
        ]

    @classmethod
    def compute(
        cls,
        p1: Annotated[
            pa.StructArray,
            Param(doc="First point {lat, lon}", arrow_type=_POINT_STRUCT_TYPE),
        ],
        p2: Annotated[
            pa.StructArray,
            Param(doc="Second point {lat, lon}", arrow_type=_POINT_STRUCT_TYPE),
        ],
    ) -> Annotated[pa.DoubleArray, Returns()]:
        """Compute Euclidean distance between two points."""
        return _euclidean_distance(p1.field("lat"), p1.field("lon"), p2.field("lat"), p2.field("lon"))


class GeoDistanceListFunction(ScalarFunction):
    """Euclidean distance between two list points.

    Each point is a list of two float64 values [lat, lon].

    Example:
        SQL:    SELECT geo_distance_list(p1, p2) FROM points
        Input:  p1=[0.0, 0.0], p2=[3.0, 4.0]
        Output: result=5.0

    """

    class Meta:
        """Function metadata."""

        name = "geo_distance_list"
        description = "Euclidean distance between two list points"
        examples = [
            FunctionExample(
                sql="SELECT geo_distance_list([0, 0], [3, 4])",
                description="Distance between origin and (3, 4)",
            ),
        ]

    @classmethod
    def compute(
        cls,
        p1: Annotated[  # type: ignore[type-arg]
            pa.ListArray,
            Param(doc="First point [lat, lon]", arrow_type=pa.list_(pa.float64())),
        ],
        p2: Annotated[  # type: ignore[type-arg]
            pa.ListArray,
            Param(doc="Second point [lat, lon]", arrow_type=pa.list_(pa.float64())),
        ],
    ) -> Annotated[pa.DoubleArray, Returns()]:
        """Compute Euclidean distance between two points."""
        return _euclidean_distance(
            pc.list_element(p1, 0),
            pc.list_element(p1, 1),
            pc.list_element(p2, 0),
            pc.list_element(p2, 1),
        )


class GeoDistanceFixedFunction(ScalarFunction):
    """Euclidean distance between two fixed-size list points.

    Each point is a fixed-size list of two float64 values [lat, lon].

    Example:
        SQL:    SELECT geo_distance_fixed(p1, p2) FROM points
        Input:  p1=[0.0, 0.0], p2=[3.0, 4.0]
        Output: result=5.0

    """

    class Meta:
        """Function metadata."""

        name = "geo_distance_fixed"
        description = "Euclidean distance between two fixed-size list points"
        examples = [
            FunctionExample(
                sql="SELECT geo_distance_fixed([0, 0], [3, 4])",
                description="Distance between origin and (3, 4)",
            ),
        ]

    @classmethod
    def compute(
        cls,
        p1: Annotated[  # type: ignore[type-arg]
            pa.FixedSizeListArray,
            Param(doc="First point [lat, lon]", arrow_type=pa.list_(pa.float64(), 2)),
        ],
        p2: Annotated[  # type: ignore[type-arg]
            pa.FixedSizeListArray,
            Param(doc="Second point [lat, lon]", arrow_type=pa.list_(pa.float64(), 2)),
        ],
    ) -> Annotated[pa.DoubleArray, Returns()]:
        """Compute Euclidean distance between two points."""
        return _euclidean_distance(
            pc.list_element(p1, 0),
            pc.list_element(p1, 1),
            pc.list_element(p2, 0),
            pc.list_element(p2, 1),
        )


class GeoCentroidStructFunction(ScalarFunction):
    """Centroid of N struct points (varargs).

    Computes the average lat and average lon across all input point columns.

    Example:
        SQL:    SELECT geo_centroid_struct(p1, p2) FROM points
        Input:  p1={lat: 0.0, lon: 0.0}, p2={lat: 4.0, lon: 6.0}
        Output: result={lat: 2.0, lon: 3.0}

    """

    class Meta:
        """Function metadata."""

        name = "geo_centroid_struct"
        description = "Centroid of N struct points"
        examples = [
            FunctionExample(
                sql="SELECT geo_centroid_struct(p1, p2) FROM points",
                description="Compute centroid of two struct points",
            ),
        ]

    @classmethod
    def compute(
        cls,
        points: Annotated[
            list[pa.StructArray],
            Param(
                doc="Point columns {lat, lon}",
                arrow_type=_POINT_STRUCT_TYPE,
                varargs=True,
            ),
        ],
    ) -> Annotated[pa.StructArray, Returns(arrow_type=_POINT_STRUCT_TYPE)]:
        """Compute centroid of all points."""
        return _compute_centroid(
            [p.field("lat") for p in points],
            [p.field("lon") for p in points],
        )


class GeoCentroidListFunction(ScalarFunction):
    """Centroid of N list points (varargs).

    Computes the average lat and average lon across all input point columns,
    where each point is a list of [lat, lon].

    Example:
        SQL:    SELECT geo_centroid_list(p1, p2) FROM points
        Input:  p1=[0.0, 0.0], p2=[4.0, 6.0]
        Output: result={lat: 2.0, lon: 3.0}

    """

    class Meta:
        """Function metadata."""

        name = "geo_centroid_list"
        description = "Centroid of N list points"
        examples = [
            FunctionExample(
                sql="SELECT geo_centroid_list(p1, p2) FROM points",
                description="Compute centroid of two list points",
            ),
        ]

    @classmethod
    def compute(
        cls,
        points: Annotated[  # type: ignore[type-arg]
            list[pa.ListArray],
            Param(
                doc="Point columns [lat, lon]",
                arrow_type=pa.list_(pa.float64()),
                varargs=True,
            ),
        ],
    ) -> Annotated[pa.StructArray, Returns(arrow_type=_POINT_STRUCT_TYPE)]:
        """Compute centroid of all points."""
        return _compute_centroid(
            [pc.list_element(p, 0) for p in points],
            [pc.list_element(p, 1) for p in points],
        )


class GeoCentroidFixedFunction(ScalarFunction):
    """Centroid of N fixed-size list points (varargs).

    Computes the average lat and average lon across all input point columns,
    where each point is a fixed-size list of [lat, lon].

    Example:
        SQL:    SELECT geo_centroid_fixed(p1, p2) FROM points
        Input:  p1=[0.0, 0.0], p2=[4.0, 6.0]
        Output: result={lat: 2.0, lon: 3.0}

    """

    class Meta:
        """Function metadata."""

        name = "geo_centroid_fixed"
        description = "Centroid of N fixed-size list points"
        examples = [
            FunctionExample(
                sql="SELECT geo_centroid_fixed(p1, p2) FROM points",
                description="Compute centroid of two fixed-size list points",
            ),
        ]

    @classmethod
    def compute(
        cls,
        points: Annotated[  # type: ignore[type-arg]
            list[pa.FixedSizeListArray],
            Param(
                doc="Point columns [lat, lon]",
                arrow_type=pa.list_(pa.float64(), 2),
                varargs=True,
            ),
        ],
    ) -> Annotated[pa.StructArray, Returns(arrow_type=_POINT_STRUCT_TYPE)]:
        """Compute centroid of all points."""
        return _compute_centroid(
            [pc.list_element(p, 0) for p in points],
            [pc.list_element(p, 1) for p in points],
        )
