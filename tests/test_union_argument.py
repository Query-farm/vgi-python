# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Union-typed argument decoding.

A DuckDB ``UNION`` / Arrow union argument is tagged: the discriminator lives in
``UnionScalar.type_code``, which plain ``Scalar.as_py()`` discards. ``Arguments``
decodes such scalars to ``TaggedUnion`` so the active member name is preserved.
"""

from __future__ import annotations

import pyarrow as pa

from vgi.arguments import Arguments, TaggedUnion

_RF = pa.struct([pa.field("n_estimators", pa.list_(pa.int64())), pa.field("max_depth", pa.list_(pa.int64()))])
_SVC = pa.struct([pa.field("C", pa.list_(pa.float64())), pa.field("kernel", pa.list_(pa.string()))])


def _union_scalar(code: int, members: dict) -> pa.UnionScalar:
    """Build a one-element sparse-union scalar with named members, active = ``code``."""
    arr = pa.UnionArray.from_sparse(
        pa.array([code], type=pa.int8()),
        [
            pa.array([members.get("random_forest_classifier")], type=_RF),
            pa.array([members.get("svc")], type=_SVC),
        ],
        field_names=["random_forest_classifier", "svc"],
        type_codes=[0, 1],
    )
    return arr[0]


def test_union_arg_preserves_tag() -> None:
    """A union argument decodes to a TaggedUnion carrying the active member name."""
    scalar = _union_scalar(0, {"random_forest_classifier": {"n_estimators": [100, 300], "max_depth": [3, 5]}})
    got = Arguments(named={"config": scalar}).get("config")
    assert isinstance(got, TaggedUnion)
    assert got.tag == "random_forest_classifier"
    assert got.value == {"n_estimators": [100, 300], "max_depth": [3, 5]}


def test_union_arg_other_member() -> None:
    """The tag reflects whichever union member is set."""
    scalar = _union_scalar(1, {"svc": {"C": [1.0, 10.0], "kernel": ["rbf", "linear"]}})
    got = Arguments(named={"config": scalar}).get("config")
    assert got.tag == "svc"
    assert got.value == {"C": [1.0, 10.0], "kernel": ["rbf", "linear"]}


def test_union_arg_null_payload() -> None:
    """A null union slot decodes its value to None (the ``inner is None`` branch).

    Note: pyarrow reports ``type_code == 0`` for any null union slot regardless of
    the codes buffer, so the active member's tag is not recoverable at the scalar
    level for a null payload — only the value is. (Tag preservation for a
    non-null payload of a non-zero-code member is covered above and through the
    batch-level round-trip in the echo integration suite.)
    """
    scalar = _union_scalar(1, {"svc": None})
    got = Arguments(named={"config": scalar}).get("config")
    assert isinstance(got, TaggedUnion)
    assert got.value is None


def test_union_arg_non_contiguous_type_codes() -> None:
    """Tag resolution maps via type_codes, not member position.

    Type codes need not be 0,1,2,...; ``_scalar_to_py`` must locate the active
    member by ``type_codes.index(code)`` rather than treating the code as a
    positional index.
    """
    arr = pa.UnionArray.from_sparse(
        pa.array([9], type=pa.int8()),
        [
            pa.array([{"n_estimators": [1], "max_depth": [2]}], type=_RF),
            pa.array([{"C": [1.0], "kernel": ["rbf"]}], type=_SVC),
        ],
        field_names=["random_forest_classifier", "svc"],
        type_codes=[5, 9],
    )
    got = Arguments(named={"config": arr[0]}).get("config")
    assert isinstance(got, TaggedUnion)
    assert got.tag == "svc"
    assert got.value == {"C": [1.0], "kernel": ["rbf"]}


def test_union_arg_dense_union() -> None:
    """Dense unions (what DuckDB emits) decode the same as sparse."""
    arr = pa.UnionArray.from_dense(
        pa.array([1], type=pa.int8()),
        pa.array([0], type=pa.int32()),
        [
            pa.array([], type=_RF),
            pa.array([{"C": [2.0], "kernel": ["linear"]}], type=_SVC),
        ],
        field_names=["random_forest_classifier", "svc"],
        type_codes=[0, 1],
    )
    got = Arguments(named={"config": arr[0]}).get("config")
    assert isinstance(got, TaggedUnion)
    assert got.tag == "svc"
    assert got.value == {"C": [2.0], "kernel": ["linear"]}


def test_non_union_args_unchanged() -> None:
    """Non-union arguments still decode via plain as_py()."""
    args = Arguments(named={"n": pa.scalar(5), "s": pa.scalar("hi")}, positional=(pa.scalar(1.5),))
    assert args.get("n") == 5
    assert args.get("s") == "hi"
    assert args.get(0) == 1.5
