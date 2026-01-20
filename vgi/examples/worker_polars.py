"""Example Polars worker with Polars-based scalar functions.

This worker contains functions that use Polars for computation.
It's separate from the main example worker to avoid the ~32ms Polars
import cost for users who don't need Polars functions.

Usage:
    vgi-example-polars-worker
"""

from vgi.examples.scalar_polars import (
    PolarsAddValuesFunction,
    PolarsDoubleFunction,
    PolarsMultiplyFunction,
    PolarsStringLengthFunction,
    PolarsSumValuesFunction,
    PolarsUpperCaseFunction,
)
from vgi.worker import Worker


class ExamplePolarsWorker(Worker):
    """Example worker with Polars-based scalar functions.

    This worker exposes Polars-based functions for users who need
    high-performance data processing with Polars expressions.
    """

    catalog_name = "example_polars"

    functions = [
        PolarsAddValuesFunction,
        PolarsDoubleFunction,
        PolarsMultiplyFunction,
        PolarsStringLengthFunction,
        PolarsSumValuesFunction,
        PolarsUpperCaseFunction,
    ]


def main() -> None:
    """Run the example Polars worker process."""
    parser = ExamplePolarsWorker.create_argument_parser()
    args = parser.parse_args()
    ExamplePolarsWorker(quiet=args.quiet).run()


if __name__ == "__main__":
    main()
