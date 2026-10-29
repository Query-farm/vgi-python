# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Atomic changes to catalog comments and literal string tags."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Self

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.utils import IpcValidation

from vgi.typed_values import TypedValueBatch


@dataclass(frozen=True, kw_only=True)
class CatalogMetadataChanges(ArrowSerializableDataclass):
    """A reusable patch with explicit comment presence and per-key tag changes.

    ``set_comment=False`` leaves the comment unchanged and requires ``comment``
    to be NULL. With ``set_comment=True``, NULL clears the comment; an empty
    string stores an empty comment. Tag keys are literal, case-sensitive strings.
    Setting an empty value differs from removing a key. SET/REMOVE overlap and
    duplicate removals are invalid, including when the target does not exist.
    """

    set_comment: bool = False
    comment: str | None = None
    set_tags: dict[str, str] = field(default_factory=dict)
    remove_tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate the entire patch before constructing a candidate state."""
        if type(self.set_comment) is not bool:
            raise ValueError("set_comment must be a non-null boolean")
        if self.comment is not None and not isinstance(self.comment, str):
            raise ValueError("comment must be a string or NULL")
        if not self.set_comment and self.comment is not None:
            raise ValueError("comment requires set_comment=true")
        if not isinstance(self.set_tags, dict) or any(
            not isinstance(k, str) or not isinstance(v, str) for k, v in self.set_tags.items()
        ):
            raise ValueError("set_tags must map literal strings to strings")
        if not isinstance(self.remove_tags, list) or any(not isinstance(k, str) for k in self.remove_tags):
            raise ValueError("remove_tags must be a list of literal strings")
        if len(set(self.remove_tags)) != len(self.remove_tags):
            raise ValueError("remove_tags must contain unique keys")
        if self.set_tags.keys() & set(self.remove_tags):
            raise ValueError("tags cannot be both SET and REMOVE")

    @classmethod
    def deserialize_from_bytes(cls, data: bytes, ipc_validation: IpcValidation = IpcValidation.FULL) -> Self:
        """Reject extra batches/trailing bytes and fully validate the Arrow payload."""
        batch = TypedValueBatch.deserialize_from_bytes(data).batch
        return cls.deserialize_from_batch(batch, ipc_validation=ipc_validation)

    @classmethod
    def deserialize_from_batch(
        cls,
        batch: pa.RecordBatch,
        custom_metadata: pa.KeyValueMetadata | None = None,
        *,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> Self:
        """Require complete patch fields and reject duplicate map keys on the wire."""
        if batch.num_rows != 1 or not batch.schema.equals(cls.ARROW_SCHEMA, check_metadata=False):
            raise ValueError("CatalogMetadataChanges requires exactly one row with the complete patch schema")
        if any(batch.column(name).null_count for name in ("set_comment", "set_tags", "remove_tags")):
            raise ValueError("CatalogMetadataChanges patch fields must be non-null")
        pairs = batch.column("set_tags")[0].as_py()
        keys = [key for key, _ in pairs]
        if len(set(keys)) != len(keys):
            raise ValueError("set_tags must contain unique keys")
        return super().deserialize_from_batch(batch, custom_metadata, ipc_validation=ipc_validation)

    def apply_to(self, comment: str | None, tags: dict[str, str]) -> tuple[str | None, dict[str, str]]:
        """Return a candidate without modifying the supplied state.

        The provider validates authorization and target existence, applies this
        candidate in the same transaction snapshot, preserves other object
        fields, and advances its catalog version only when metadata changes.
        Missing removal keys and an empty patch are no-ops on an existing target.
        """
        self.__post_init__()
        result = {key: value for key, value in tags.items() if key not in self.remove_tags}
        result.update(self.set_tags)
        return (self.comment if self.set_comment else comment), result
