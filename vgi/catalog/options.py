# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Typed catalog options with distinct SET, SET NULL, omission, and RESET.

The provider supplies the accepted Arrow schema and a consistent snapshot of
its defaults. Helpers validate and construct a candidate state without effects;
the provider commits that state under its advertised transaction guarantees.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass, field
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.typed_values import TypedValueBatch


def _validate_names(names: list[str], label: str) -> None:
    if not isinstance(names, list) or any(not isinstance(name, str) for name in names):
        raise TypeError(f"{label} must be a list of option names")
    if any(not name or "\x00" in name for name in names):
        raise ValueError(f"{label} names must be nonempty and contain no NUL")
    if len(set(names)) != len(names):
        raise ValueError(f"{label} names must be unique")


@dataclass(frozen=True, kw_only=True)
class OptionState(ArrowSerializableDataclass):
    """Readback of effective option values and which values were explicitly set.

    Attributes:
        values: One typed value for every option exposed by this provider.
        explicit_names: Names with a stored override, including explicit NULLs
            and values equal to the default. Names absent here use defaults.
    """

    values: Annotated[TypedValueBatch, ArrowType(pa.binary())]
    explicit_names: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Require explicit names to reference the effective value schema."""
        _validate_names(self.explicit_names, "explicit option")
        unknown = set(self.explicit_names) - set(self.values.schema.names)
        if unknown:
            raise ValueError(f"explicit option names are absent from values: {sorted(unknown)}")


@dataclass(frozen=True, kw_only=True)
class OptionChanges(ArrowSerializableDataclass):
    """An atomic candidate change to literal catalog options.

    All identifiers are canonical provider option names. Every supplied field
    is a SET, including a typed NULL. Missing fields are unchanged. RESET drops
    an override and restores the provider default from the mutation's snapshot.
    Expressions are not encoded as strings masquerading as literal values.

    Attributes:
        set_values: Optional one-row batch containing only options to set.
        reset_names: Options whose stored overrides should be removed.
    """

    set_values: Annotated[TypedValueBatch | None, ArrowType(pa.binary())] = None
    reset_names: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Reject contradictory or malformed operations before applying values."""
        _validate_names(self.reset_names, "reset option")
        if self.set_values is not None:
            overlap = set(self.set_values.schema.names) & set(self.reset_names)
            if overlap:
                raise ValueError(f"options cannot be both SET and RESET: {sorted(overlap)}")

    def apply_to(
        self,
        current: OptionState,
        *,
        defaults: TypedValueBatch,
        mutable_names: Collection[str] | None = None,
    ) -> OptionState:
        """Validate the whole change and return a new state without mutation.

        ``defaults`` declares all accepted fields, their exact Arrow types,
        nullability and metadata, and current reset values. It must be resolved
        in the same transaction as this mutation. SET fields must match their
        declared fields exactly; no cast, case conversion or metadata loss is
        allowed. ``mutable_names`` may further restrict which options change.

        A provider validates any additional value/dependency constraints before
        persisting the returned state. Returning a candidate does not advertise
        durable atomicity, rollback, replay safety, or engine DDL support.
        """
        # Frozen dataclasses contain mutable lists: validate again at use time.
        self.__post_init__()
        current.__post_init__()
        current.values.validate_schema(defaults.schema)
        names = defaults.schema.names
        sets = set(self.set_values.schema.names) if self.set_values is not None else set()
        resets = set(self.reset_names)
        changed = sets | resets
        unknown = changed - set(names)
        if unknown:
            raise ValueError(f"unknown option names: {sorted(unknown)}")
        if mutable_names is not None:
            forbidden = changed - set(mutable_names)
            if forbidden:
                raise ValueError(f"options are not mutable: {sorted(forbidden)}")
        if self.set_values is not None:
            expected = pa.schema(
                [defaults.schema.field(name) for name in self.set_values.schema.names],
            ).with_metadata(defaults.schema.metadata)
            self.set_values.validate_schema(expected)
        arrays = []
        for name in names:
            if name in sets:
                assert self.set_values is not None
                arrays.append(self.set_values.batch.column(name))
            elif name in resets:
                arrays.append(defaults.batch.column(name))
            else:
                arrays.append(current.values.batch.column(name))
        values = (
            TypedValueBatch(pa.RecordBatch.from_arrays(arrays, schema=defaults.schema))
            if names
            else TypedValueBatch.empty(metadata=defaults.schema.metadata)
        )
        explicit = (set(current.explicit_names) | sets) - resets
        return OptionState(values=values, explicit_names=[name for name in names if name in explicit])
