# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Compute-ladder scalar fixtures for the boundary-amortization benchmark.

These span the per-row *compute* axis (near-zero for multiply/upper_case up to
heavy for iterated hashing) at controlled payload sizes, so a benchmark sweep can
trace framework overhead as a function of (per-row compute, per-row bytes).

- ``collatz_steps``  Int64 -> Int64, data-dependent CPU work (a Python UDF classic).
- ``sha256_hex``     Utf8  -> Utf8(64), fixed moderate compute per byte.
- ``hash_rounds``    Utf8  -> Utf8(64), K rounds of SHA-256 (key-stretching): K is a
                     clean compute *knob* at a fixed payload — sweep it to walk the
                     compute axis without changing bytes on the wire.
"""

from __future__ import annotations

import hashlib
from typing import Annotated

import pyarrow as pa

from vgi.arguments import ConstParam, Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction


class PassthruFunction(ScalarFunction):
    """Identity: returns the input string unchanged.

    Zero compute — its whole cost is the VGI round-trip (convert in/out + wire),
    so a payload sweep over it measures the *pure wire cost per byte* with no
    compute contamination (unlike ``upper_case``, whose work grows with length).
    """

    class Meta:
        name = "passthru"
        description = "Returns the input string unchanged (zero-compute wire probe)"

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.StringArray, Param(doc="String value")],
    ) -> Annotated[pa.StringArray, Returns()]:
        return value


class CollatzStepsFunction(ScalarFunction):
    """Collatz sequence length for each integer.

    A textbook Python-UDF workload: an unvectorizable per-row loop whose cost is
    data-dependent (larger / odd-heavy trajectories take more steps).
    """

    class Meta:
        name = "collatz_steps"
        description = "Number of Collatz (3n+1) steps for each integer to reach 1"
        examples = [
            FunctionExample(sql="SELECT collatz_steps(n) FROM range(1000) t(n)",
                            description="Step count per integer"),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param(doc="Positive integer")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        out: list[int | None] = []
        for n in value.to_pylist():
            if n is None:
                out.append(None)
                continue
            if n <= 0:
                out.append(0)
                continue
            steps = 0
            while n != 1:
                n = n // 2 if (n & 1) == 0 else 3 * n + 1
                steps += 1
            out.append(steps)
        return pa.array(out, type=pa.int64())


class Sha256HexFunction(ScalarFunction):
    """Lowercase hex SHA-256 of each UTF-8 string. Fixed compute per byte."""

    class Meta:
        name = "sha256_hex"
        description = "Lowercase hex SHA-256 digest of the UTF-8 string"
        examples = [
            FunctionExample(sql="SELECT sha256_hex(s) FROM docs",
                            description="Hash each string"),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.StringArray, Param(doc="String to hash")],
    ) -> Annotated[pa.StringArray, Returns()]:
        out: list[str | None] = []
        for s in value.to_pylist():
            out.append(None if s is None else hashlib.sha256(s.encode("utf-8")).hexdigest())
        return pa.array(out, type=pa.string())


class HashRoundsFunction(ScalarFunction):
    """Iterated SHA-256 (key-stretching): apply the digest ``rounds`` times.

    ``rounds`` is a constant, so it is the compute knob: at a fixed input payload,
    sweeping it scales per-row compute linearly while bytes on the wire stay put.
    """

    class Meta:
        name = "hash_rounds"
        description = "Apply SHA-256 `rounds` times (key-stretching); rounds is a const compute knob"
        examples = [
            FunctionExample(sql="SELECT hash_rounds(s, 256) FROM docs",
                            description="256-round key stretch"),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.StringArray, Param(doc="String to stretch")],
        rounds: Annotated[pa.Int64Scalar, ConstParam("Number of SHA-256 rounds")],
    ) -> Annotated[pa.StringArray, Returns()]:
        k = int(rounds.as_py())
        out: list[str | None] = []
        for s in value.to_pylist():
            if s is None:
                out.append(None)
                continue
            b = s.encode("utf-8")
            for _ in range(k):
                b = hashlib.sha256(b).digest()
            out.append(b.hex())
        return pa.array(out, type=pa.string())
