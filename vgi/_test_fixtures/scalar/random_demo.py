"""Random/seeded scalar fixtures (random_int, random_bytes, bernoulli, hash_seed)."""

from __future__ import annotations

from typing import Annotated

import pyarrow as pa

from vgi.arguments import ConstParam, OutputLength, Param, Returns
from vgi.metadata import FunctionExample, FunctionStability
from vgi.scalar_function import ScalarFunction


class RandomIntFunction(ScalarFunction):
    """Generates random integers for each row (demonstrates VOLATILE stability).

    This function demonstrates FunctionStability.VOLATILE - calling it twice
    with the same input will produce different results. The database optimizer
    cannot cache or reuse results from volatile functions.

    This example uses type inference with pa.Int64Array and Meta.stability.

    Other stability options:
    - CONSISTENT: Same input always produces same output (deterministic)
    - CONSISTENT_WITHIN_QUERY: Same within a query, may vary across queries

    Example:
        SQL:    SELECT random_int(min_col, max_col) FROM data
        Input:  min_col=[1, 10, 100], max_col=[10, 100, 1000]
        Output: result=[7, 55, 823]  (random values per row, different each time)

    """

    class Meta:
        """Function metadata."""

        name = "random_int"
        description = "Generate random integers (demonstrates VOLATILE stability)"
        stability = FunctionStability.VOLATILE
        examples = [
            FunctionExample(
                sql="SELECT random_int(min_col, max_col) FROM data",
                description="Generate random integers between min and max values",
            ),
        ]

    @classmethod
    def compute(
        cls,
        min_val: Annotated[pa.Int64Array, Param(doc="Minimum value (inclusive)")],
        max_val: Annotated[pa.Int64Array, Param(doc="Maximum value (inclusive)")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        """Generate random integers for each row."""
        import numpy as np

        result = np.random.randint(min_val.to_numpy(), max_val.to_numpy() + 1)
        return pa.array(result, type=pa.int64())


class BernoulliFunction(ScalarFunction):
    """Generates random booleans for each row (demonstrates VOLATILE stability).

    This function demonstrates how to generate output without any input parameters.
    It will produce a random 0 or 1 for each row in the output.

    Example:
        SQL:    SELECT bernoulli() FROM data

    """

    class Meta:
        """Function metadata."""

        name = "bernoulli"
        description = "Generate random booleans (demonstrates VOLATILE stability)"
        stability = FunctionStability.VOLATILE
        examples = [
            FunctionExample(
                sql="SELECT bernoulli() FROM data",
                description="Generate samples from the bernoulli distribution",
            ),
        ]

    @classmethod
    def compute(
        cls,
        _length: Annotated[int, OutputLength()],
    ) -> Annotated[pa.BooleanArray, Returns()]:
        """Generate random booleans for each row."""
        import random

        values = [bool(random.randint(0, 1)) for _ in range(_length)]
        return pa.array(values, type=pa.bool_())


class HashSeedFunction(ScalarFunction):
    """Generates deterministic integers from a constant seed.

    Demonstrates the single-ConstParam pattern: one constant argument
    folded at plan time, no column parameters.

    Example:
        SQL:    SELECT hash_seed(42) FROM data
        Input:  (no column input)
        Args:   seed=42
        Output: result=[42, 43, 44, ...]  (seed + row_index)

    """

    class Meta:
        """Function metadata."""

        name = "hash_seed"
        description = "Generate deterministic integers from a constant seed"
        stability = FunctionStability.CONSISTENT
        examples = [
            FunctionExample(
                sql="SELECT hash_seed(42) FROM data",
                description="Generate deterministic integers seeded at 42",
            ),
        ]

    @classmethod
    def compute(
        cls,
        seed: Annotated[int, ConstParam("Seed value")],
        _length: Annotated[int, OutputLength()],
    ) -> Annotated[pa.Int64Array, Returns()]:
        """Generate deterministic integers: seed + row_index for each row."""
        return pa.array([seed + i for i in range(_length)], type=pa.int64())


class RandomBytesFunction(ScalarFunction):
    """Generates deterministic pseudo-random binary blobs from a seed."""

    class Meta:
        """Function metadata."""

        name = "random_bytes"
        description = "Generate pseudo-random binary blobs from seed and length"
        stability = FunctionStability.CONSISTENT
        examples = [
            FunctionExample(
                sql="SELECT random_bytes(42, 16) FROM data",
                description="Generate a deterministic 16-byte blob per input row",
            ),
        ]

    @classmethod
    def compute(
        cls,
        seed: Annotated[int, ConstParam("Seed for pseudo-random byte generation")],
        byte_length: Annotated[int, ConstParam("Output blob length in bytes")],
        _length: Annotated[int, OutputLength()],
    ) -> Annotated[pa.BinaryArray, Returns()]:
        """Generate pseudo-random binary blobs for each row."""
        import random

        if byte_length < 0:
            raise ValueError("byte_length must be >= 0")
        rng = random.Random(seed)
        return pa.array(
            [bytes(rng.getrandbits(8) for _ in range(byte_length)) for _ in range(_length)],
            type=pa.binary(),
        )
