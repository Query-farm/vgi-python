# Copyright 2026 Query Farm LLC - https://query.farm

"""The result-cache nonce fixtures mint values unique across worker processes.

The client's subprocess pool may serve two cache MISSes from two different
worker processes (``cache/transaction_scope.test`` asserts their nonces differ),
so a bare per-process counter is not enough: the pid lives in the high 32 bits.
"""

from __future__ import annotations

import multiprocessing
import os

from vgi._test_fixtures.table.cache import _next_nonce


def _child_nonce(_: int) -> int:
    return _next_nonce()


def test_nonce_monotonic_and_carries_pid() -> None:
    """Within a process nonces advance by one and carry this process's pid."""
    a = _next_nonce()
    b = _next_nonce()
    assert b == a + 1
    for n in (a, b):
        assert n >> 32 == os.getpid()
        assert -(2**63) <= n < 2**63  # fits the declared int64 column


def test_nonces_disjoint_across_processes() -> None:
    """Fresh processes (each counter at 0) never mint the parent's or each other's values."""
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(2) as pool:
        children = pool.map(_child_nonce, range(2), chunksize=1)
    parent = _next_nonce()
    values = [*children, parent]
    assert len(set(values)) == len(values)
