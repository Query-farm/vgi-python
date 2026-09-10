# Copyright 2026 Query Farm LLC - https://query.farm

"""Typed model and strict consumer for VGI Filter Encoding v2."""

from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from typing import Literal as TypingLiteral

import pyarrow as pa

FILTER_ENCODING = "vgi.filters.v2"
FILTER_VERSION = "2"
DUCKDB_STANDARD_V1 = "vgi.duckdb.standard.v1"
NO_EVALUATION_CONTEXT = "vgi.none.v1"
DUCKDB_SESSION_CONTEXT = "vgi.duckdb.session.v1"

MAX_JSON_BYTES = 1 << 20
MAX_DEPTH = 64
MAX_NODES = 10_000
MAX_PREDICATES = 1_024
MAX_PREDICATE_IDS = 4_096
MAX_ARGUMENTS = 256
MAX_ID_BYTES = 128
MAX_PROVIDER_FINGERPRINT_BYTES = 256
MAX_PAYLOAD_BYTES = 16 << 20
_UINT64_MAX = (1 << 64) - 1
_PAYLOAD_NAME = re.compile(r"(?:value|type|artifact)_(?:0|[1-9][0-9]*)\Z")
_IDENTITY_NAMESPACE = re.compile(r"[a-z][a-z0-9]*(?:\.[a-z][a-z0-9_]*)*\Z")
_IDENTITY_NAME = re.compile(r"[a-z][a-z0-9_]*\Z")
_KNOWN_ARROW_EXTENSIONS = {
    "arrow.bool8",
    "arrow.json",
    "arrow.uuid",
    "geoarrow.linestring",
    "geoarrow.multilinestring",
    "geoarrow.multipoint",
    "geoarrow.multipolygon",
    "geoarrow.point",
    "geoarrow.polygon",
    "geoarrow.wkb",
}


class FilterV2Error(ValueError):
    """A Filter Encoding v2 document is malformed or cannot be evaluated."""


class PredicateMode(StrEnum):
    """Whether a predicate must be applied exactly or is only a pruning hint."""

    REQUIRED = "required"
    ADVISORY = "advisory"


class PredicateSource(StrEnum):
    """Diagnostic origin of a predicate."""

    QUERY = "query"
    JOIN = "join"
    TOP_N = "top_n"
    SPLIT_REFINEMENT = "split_refinement"
    OTHER = "other"


class ComparisonOperator(StrEnum):
    """V2 comparison operators."""

    EQ = "eq"
    NE = "ne"
    LT = "lt"
    LE = "le"
    GT = "gt"
    GE = "ge"
    DISTINCT_FROM = "distinct_from"
    NOT_DISTINCT_FROM = "not_distinct_from"


class ArithmeticOperator(StrEnum):
    """V2 binary arithmetic operators."""

    ADD = "add"
    SUBTRACT = "subtract"
    MULTIPLY = "multiply"
    DIVIDE = "divide"
    MODULO = "modulo"


class StandardFilterFunction(StrEnum):
    """Functions fixed by ``vgi.duckdb.standard.v1``."""

    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    CONTAINS = "contains"
    LIST_CONTAINS = "list_contains"


@dataclass(frozen=True, slots=True)
class FunctionIdentity:
    """Stable identity for an extension function or runtime algorithm."""

    namespace: str
    name: str
    version: int


@dataclass(frozen=True, slots=True)
class EvaluationContext:
    """Immutable evaluation-context metadata attached to every filter batch."""

    profile: str
    time_zone: str | None = None
    calendar: str | None = None
    default_collation: str | None = None
    ieee_floating_point_ops: bool | None = None
    integer_division: bool | None = None
    provider_fingerprint: str | None = None


@dataclass(frozen=True, slots=True)
class ColumnRef:
    """Reference to an unprojected bind-output column."""

    column_index: int
    column_name: str
    data_type: pa.DataType | None


@dataclass(frozen=True, slots=True)
class FieldRef:
    """Reference to a field of a struct expression; nesting is recursive."""

    expression: FilterExpression
    field_index: int
    field_name: str
    data_type: pa.DataType


@dataclass(frozen=True, slots=True)
class Literal:
    """Typed Arrow scalar literal."""

    value_ref: int
    field: pa.Field[Any]
    value: pa.Scalar[Any]


@dataclass(frozen=True, slots=True)
class Comparison:
    """Binary comparison expression."""

    op: ComparisonOperator
    left: FilterExpression
    right: FilterExpression


@dataclass(frozen=True, slots=True)
class BooleanExpression:
    """Two-or-more-child SQL AND or OR expression."""

    node: TypingLiteral["and", "or"]
    children: tuple[FilterExpression, ...]


@dataclass(frozen=True, slots=True)
class Not:
    """SQL three-valued negation."""

    expression: FilterExpression


@dataclass(frozen=True, slots=True)
class IsNull:
    """IS NULL or IS NOT NULL expression."""

    expression: FilterExpression
    negated: bool


@dataclass(frozen=True, slots=True)
class LiteralSet:
    """Inline typed list used by an IN expression."""

    value_ref: int
    field: pa.Field[Any]
    values: pa.Array[Any]


@dataclass(frozen=True, slots=True)
class ExternalSet:
    """Exact typed set stored in an InitRequest join-key batch."""

    batch_index: int
    column_index: int
    column_name: str
    values: pa.Array[Any]
    batch: pa.RecordBatch


type FilterSet = LiteralSet | ExternalSet


@dataclass(frozen=True, slots=True)
class In:
    """SQL IN or NOT IN expression."""

    expression: FilterExpression
    set: FilterSet
    negated: bool


@dataclass(frozen=True, slots=True)
class Cast:
    """Ordinary throwing cast with an Arrow-declared target type."""

    expression: FilterExpression
    type_ref: int
    field: pa.Field[Any]


@dataclass(frozen=True, slots=True)
class Arithmetic:
    """Binary arithmetic expression."""

    op: ArithmeticOperator
    left: FilterExpression
    right: FilterExpression


@dataclass(frozen=True, slots=True)
class Negate:
    """Unary numeric negation."""

    expression: FilterExpression


@dataclass(frozen=True, slots=True)
class Call:
    """Standard or capability-gated extension function call."""

    function: StandardFilterFunction | FunctionIdentity
    arguments: tuple[FilterExpression, ...]
    options: dict[str, object] | None = None


@dataclass(frozen=True, slots=True)
class RuntimeFilter:
    """Advisory runtime-filter artifact reference."""

    algorithm: FunctionIdentity
    input: FilterExpression
    artifact_ref: int
    field: pa.Field[Any]
    artifact: pa.Scalar[Any]
    null_handling: TypingLiteral["pass", "reject"]
    supported: bool = False


type FilterExpression = (
    ColumnRef
    | FieldRef
    | Literal
    | Comparison
    | BooleanExpression
    | Not
    | IsNull
    | In
    | Cast
    | Arithmetic
    | Negate
    | Call
    | RuntimeFilter
)


@dataclass(frozen=True, slots=True)
class FilterPredicate:
    """One independently revisioned v2 predicate."""

    id: str
    revision: int
    mode: PredicateMode
    source: PredicateSource
    expression: FilterExpression


@dataclass(frozen=True, slots=True)
class FilterState:
    """Snapshot state, including revision tombstones needed by future deltas."""

    semantics: str
    evaluation_context: EvaluationContext
    predicates: tuple[FilterPredicate, ...]
    revisions: tuple[tuple[str, int], ...]
    required_ids: frozenset[str]
    output_schema: pa.Schema | None
    join_keys: tuple[pa.RecordBatch, ...]
    extension_functions: frozenset[tuple[str, str, int]]
    runtime_algorithms: frozenset[tuple[str, str, int]]
    evaluation_capabilities: tuple[tuple[str, str | None], ...]

    def apply_delta(self, batch: pa.RecordBatch) -> FilterState:
        """Validate and atomically apply one v2 delta batch."""
        return _Parser(
            batch,
            self.output_schema,
            list(self.join_keys),
            prior=self,
            extension_functions=self.extension_functions,
            runtime_algorithms=self.runtime_algorithms,
            evaluation_capabilities=self.evaluation_capabilities,
        ).parse_delta()

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Evaluate live predicates, retaining only SQL TRUE rows."""
        import pyarrow.compute as pc

        mask: pa.BooleanArray = pa.array([True] * batch.num_rows, type=pa.bool_())
        for predicate in self.predicates:
            if isinstance(predicate.expression, RuntimeFilter) and not predicate.expression.supported:
                continue
            try:
                current = _evaluate_expression(predicate.expression, batch, self.evaluation_context)
            except Exception:
                if predicate.mode is PredicateMode.ADVISORY:
                    continue
                raise
            mask = pc.and_kleene(mask, current)
        return mask

    def apply(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Apply live predicates to a record batch."""
        import pyarrow.compute as pc

        result: Any = pc.filter(batch, self.evaluate(batch))  # type: ignore[call-overload]
        return result  # type: ignore[no-any-return]


class _DuplicateKey(ValueError):
    pass


def _object_no_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKey(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def _object(value: object, where: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(k, str) for k in value):
        raise FilterV2Error(f"{where} must be a JSON object")
    return value


def _array(value: object, where: str) -> list[object]:
    if not isinstance(value, list):
        raise FilterV2Error(f"{where} must be a JSON array")
    return value


def _string(value: object, where: str, *, nonempty: bool = True) -> str:
    if not isinstance(value, str) or (nonempty and not value):
        raise FilterV2Error(f"{where} must be {'a nonempty' if nonempty else 'a'} UTF-8 string")
    return value


def _bool(value: object, where: str) -> bool:
    if not isinstance(value, bool):
        raise FilterV2Error(f"{where} must be a Boolean")
    return value


def _uint(value: object, where: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < (1 if positive else 0) or value > _UINT64_MAX:
        raise FilterV2Error(f"{where} must be an unsigned 64-bit integer")
    return value


def _keys(obj: dict[str, object], required: set[str], optional: set[str], where: str) -> None:
    missing = required - obj.keys()
    unknown = obj.keys() - required - optional
    if missing:
        raise FilterV2Error(f"{where} is missing {sorted(missing)!r}")
    if unknown:
        raise FilterV2Error(f"{where} has unknown properties {sorted(unknown)!r}")


def _metadata_text(metadata: dict[bytes, bytes], key: bytes) -> str:
    try:
        return metadata[key].decode("utf-8")
    except KeyError as exc:
        raise FilterV2Error(f"missing schema metadata {key.decode()!r}") from exc
    except UnicodeDecodeError as exc:
        raise FilterV2Error(f"schema metadata {key.decode()!r} is not UTF-8") from exc


def _validate_arrow_extensions(field: pa.Field[Any]) -> None:
    """Reject extension identities outside the v2 reference evaluator registry."""
    names: list[str] = []
    if field.metadata and b"ARROW:extension:name" in field.metadata:
        try:
            names.append(field.metadata[b"ARROW:extension:name"].decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise FilterV2Error(f"Arrow extension name on {field.name!r} is not UTF-8") from exc
    if isinstance(field.type, pa.BaseExtensionType):
        names.append(field.type.extension_name)
    for name in names:
        if name not in _KNOWN_ARROW_EXTENSIONS:
            raise FilterV2Error(f"unknown Arrow extension type {name!r}")
    if pa.types.is_struct(field.type):
        for child in field.type:
            _validate_arrow_extensions(child)
    elif pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        _validate_arrow_extensions(field.type.value_field)


def parse_evaluation_context(schema: pa.Schema) -> EvaluationContext:
    """Parse and strictly validate v2 evaluation-context schema metadata."""
    metadata = schema.metadata or {}
    encoding = _metadata_text(metadata, b"vgi_filter_encoding")
    version = _metadata_text(metadata, b"vgi_filter_version")
    if encoding != FILTER_ENCODING:
        raise FilterV2Error(f"unsupported filter encoding {encoding!r}")
    if version != FILTER_VERSION:
        raise FilterV2Error(f"unsupported filter version {version!r}")
    profile = _metadata_text(metadata, b"vgi_evaluation_context")
    context_keys = {
        b"vgi_time_zone",
        b"vgi_calendar",
        b"vgi_default_collation",
        b"vgi_ieee_floating_point_ops",
        b"vgi_integer_division",
        b"vgi_context_provider_fingerprint",
    }
    present = context_keys & metadata.keys()
    if profile == NO_EVALUATION_CONTEXT:
        if present:
            raise FilterV2Error("vgi.none.v1 forbids DuckDB session-context metadata")
        return EvaluationContext(profile=profile)
    if profile != DUCKDB_SESSION_CONTEXT:
        raise FilterV2Error(f"unknown evaluation context {profile!r}")
    required = context_keys - {b"vgi_context_provider_fingerprint"}
    if missing := required - metadata.keys():
        raise FilterV2Error(f"incomplete DuckDB evaluation context; missing {sorted(k.decode() for k in missing)!r}")

    def context_bool(key: bytes) -> bool:
        value = _metadata_text(metadata, key)
        if value not in {"true", "false"}:
            raise FilterV2Error(f"{key.decode()} must be canonical 'true' or 'false'")
        return value == "true"

    fingerprint = None
    if b"vgi_context_provider_fingerprint" in metadata:
        fingerprint = _metadata_text(metadata, b"vgi_context_provider_fingerprint")
        if not fingerprint or len(fingerprint.encode()) > MAX_PROVIDER_FINGERPRINT_BYTES:
            raise FilterV2Error("context provider fingerprint must be nonempty and at most 256 UTF-8 bytes")
    return EvaluationContext(
        profile=profile,
        time_zone=_metadata_text(metadata, b"vgi_time_zone"),
        calendar=_metadata_text(metadata, b"vgi_calendar"),
        default_collation=_metadata_text(metadata, b"vgi_default_collation"),
        ieee_floating_point_ops=context_bool(b"vgi_ieee_floating_point_ops"),
        integer_division=context_bool(b"vgi_integer_division"),
        provider_fingerprint=fingerprint,
    )


class _Parser:
    def __init__(
        self,
        batch: pa.RecordBatch,
        output_schema: pa.Schema | None,
        join_keys: list[pa.RecordBatch] | None,
        prior: FilterState | None = None,
        extension_functions: frozenset[tuple[str, str, int]] = frozenset(),
        runtime_algorithms: frozenset[tuple[str, str, int]] = frozenset(),
        evaluation_capabilities: tuple[tuple[str, str | None], ...] = (),
    ) -> None:
        self.batch = batch
        self.output_schema = output_schema
        self.join_keys = join_keys or []
        self.prior = prior
        self.extension_functions = extension_functions
        self.runtime_algorithms = runtime_algorithms
        self.evaluation_capabilities = evaluation_capabilities
        self.nodes = 0
        self.context = self._validate_batch()
        self._validate_context_capability()
        self.document = self._parse_document()

    def _validate_context_capability(self) -> None:
        if self.context.profile == NO_EVALUATION_CONTEXT:
            return
        matching = [
            fingerprint for profile, fingerprint in self.evaluation_capabilities if profile == self.context.profile
        ]
        if not matching:
            raise FilterV2Error(f"evaluation context {self.context.profile!r} was not advertised")
        requested = self.context.provider_fingerprint
        if requested is not None and requested not in matching:
            raise FilterV2Error("evaluation-context provider fingerprint does not match an advertised capability")

    def _validate_batch(self) -> EvaluationContext:
        if self.batch.num_rows != 1:
            raise FilterV2Error("filter RecordBatch must contain exactly one row")
        if self.batch.num_columns == 0:
            raise FilterV2Error("filter RecordBatch has no filter_spec field")
        first = self.batch.schema.field(0)
        if first.name != "filter_spec" or first.type != pa.string() or first.nullable:
            raise FilterV2Error("first field must be filter_spec: utf8 not null")
        if not self.batch.column(0)[0].is_valid:
            raise FilterV2Error("filter_spec value must not be NULL")
        names = self.batch.schema.names
        if len(names) != len(set(names)):
            raise FilterV2Error("filter payload field names must be unique")
        for i, name in enumerate(names[1:], 1):
            if not _PAYLOAD_NAME.fullmatch(name):
                raise FilterV2Error(f"noncanonical payload field name {name!r}")
            if name.startswith("type_") and self.batch.column(i)[0].is_valid:
                raise FilterV2Error(f"{name} must contain a NULL value")
        if self.batch.nbytes > MAX_PAYLOAD_BYTES:
            raise FilterV2Error("filter payload exceeds 16 MiB")
        return parse_evaluation_context(self.batch.schema)

    def _parse_document(self) -> dict[str, object]:
        raw = self.batch.column(0)[0].as_py()
        if not isinstance(raw, str) or len(raw.encode()) > MAX_JSON_BYTES:
            raise FilterV2Error("filter JSON must be UTF-8 and at most 1 MiB")
        try:
            value = json.loads(raw, object_pairs_hook=_object_no_duplicates)
        except (json.JSONDecodeError, UnicodeError, _DuplicateKey) as exc:
            raise FilterV2Error(f"invalid filter JSON: {exc}") from exc
        return _object(value, "filter document")

    def _header(self, kind: str, member: str) -> list[object]:
        _keys(self.document, {"encoding", "semantics", "kind", member}, set(), "filter document")
        if self.document["encoding"] != FILTER_ENCODING:
            raise FilterV2Error("document encoding must be vgi.filters.v2")
        if self.document["semantics"] != DUCKDB_STANDARD_V1:
            raise FilterV2Error(f"unsupported filter semantics {self.document['semantics']!r}")
        if self.document["kind"] != kind:
            raise FilterV2Error(f"expected a {kind} filter document")
        return _array(self.document[member], member)

    def parse_snapshot(self) -> FilterState:
        """Parse an initial snapshot into immutable scan state."""
        entries = self._header("snapshot", "predicates")
        if len(entries) > MAX_PREDICATES:
            raise FilterV2Error("snapshot exceeds predicate limit")
        predicates: list[FilterPredicate] = []
        ids: set[str] = set()
        for index, raw in enumerate(entries):
            predicate = self._predicate(_object(raw, f"predicates[{index}]"), f"predicates[{index}]")
            if predicate.revision != 0:
                raise FilterV2Error("snapshot predicate revisions must be zero")
            if predicate.id in ids:
                raise FilterV2Error(f"duplicate predicate ID {predicate.id!r}")
            ids.add(predicate.id)
            predicates.append(predicate)
        return FilterState(
            semantics=DUCKDB_STANDARD_V1,
            evaluation_context=self.context,
            predicates=tuple(predicates),
            revisions=tuple((predicate.id, 0) for predicate in predicates),
            required_ids=frozenset(p.id for p in predicates if p.mode is PredicateMode.REQUIRED),
            output_schema=self.output_schema,
            join_keys=tuple(self.join_keys),
            extension_functions=self.extension_functions,
            runtime_algorithms=self.runtime_algorithms,
            evaluation_capabilities=self.evaluation_capabilities,
        )

    def parse_delta(self) -> FilterState:
        """Parse and atomically apply a delta to prior state."""
        if self.prior is None:
            raise FilterV2Error("a delta requires prior snapshot state")
        if self.context != self.prior.evaluation_context:
            raise FilterV2Error("evaluation context changed within one scan")
        updates = self._header("delta", "updates")
        seen: set[str] = set()
        current = {p.id: p for p in self.prior.predicates}
        revisions = dict(self.prior.revisions)
        parsed: list[tuple[str, int, FilterPredicate | None]] = []
        for index, raw in enumerate(updates):
            where = f"updates[{index}]"
            obj = _object(raw, where)
            operation = obj.get("operation")
            if operation == "remove":
                _keys(obj, {"operation", "id", "revision"}, set(), where)
                predicate_id = self._predicate_id(obj["id"], where)
                revision = _uint(obj["revision"], f"{where}.revision")
                predicate = None
            elif operation == "upsert":
                _keys(obj, {"operation", "id", "revision", "mode", "source", "expression"}, set(), where)
                predicate_id = self._predicate_id(obj["id"], where)
                revision = _uint(obj["revision"], f"{where}.revision")
                try:
                    mode = PredicateMode(_string(obj["mode"], f"{where}.mode"))
                    PredicateSource(_string(obj["source"], f"{where}.source"))
                except ValueError as exc:
                    raise FilterV2Error(f"{where} has an unknown mode or source") from exc
                _object(obj["expression"], f"{where}.expression")
                if mode is not PredicateMode.ADVISORY:
                    raise FilterV2Error("delta upserts must be advisory")
                predicate = None
            else:
                raise FilterV2Error(f"{where}.operation must be 'upsert' or 'remove'")
            if predicate_id in seen:
                raise FilterV2Error(f"duplicate delta predicate ID {predicate_id!r}")
            seen.add(predicate_id)
            if predicate_id in self.prior.required_ids:
                raise FilterV2Error(f"delta targets required predicate {predicate_id!r}")
            if revision > revisions.get(predicate_id, -1):
                if operation == "upsert":
                    predicate = self._predicate(obj, where, update=True)
                parsed.append((predicate_id, revision, predicate))
        if len(set(revisions) | {entry[0] for entry in parsed}) > MAX_PREDICATE_IDS:
            raise FilterV2Error("delta exceeds per-scan predicate-ID limit")
        # Commit only after every applicable update was parsed and validated.
        for predicate_id, revision, predicate in parsed:
            revisions[predicate_id] = revision
            if predicate is None:
                current.pop(predicate_id, None)
            else:
                current[predicate_id] = predicate
        return FilterState(
            semantics=self.prior.semantics,
            evaluation_context=self.context,
            predicates=tuple(current.values()),
            revisions=tuple(revisions.items()),
            required_ids=self.prior.required_ids,
            output_schema=self.prior.output_schema,
            join_keys=self.prior.join_keys,
            extension_functions=self.prior.extension_functions,
            runtime_algorithms=self.prior.runtime_algorithms,
            evaluation_capabilities=self.prior.evaluation_capabilities,
        )

    def _predicate_id(self, value: object, where: str) -> str:
        result = _string(value, f"{where}.id")
        if len(result.encode()) > MAX_ID_BYTES:
            raise FilterV2Error(f"{where}.id exceeds 128 UTF-8 bytes")
        return result

    def _predicate(self, obj: dict[str, object], where: str, *, update: bool = False) -> FilterPredicate:
        required = {"id", "revision", "mode", "source", "expression"}
        _keys(obj, required | ({"operation"} if update else set()), set(), where)
        try:
            mode = PredicateMode(_string(obj["mode"], f"{where}.mode"))
            source = PredicateSource(_string(obj["source"], f"{where}.source"))
        except ValueError as exc:
            raise FilterV2Error(f"{where} has an unknown mode or source") from exc
        expression = self._expression(_object(obj["expression"], f"{where}.expression"), 1, root=True)
        if self.context.profile == NO_EVALUATION_CONTEXT and _requires_session_context(expression):
            raise FilterV2Error("context-dependent expression requires vgi.duckdb.session.v1")
        if isinstance(expression, RuntimeFilter):
            if mode is not PredicateMode.ADVISORY:
                raise FilterV2Error("runtime_filter predicates must be advisory")
        elif not self._is_boolean(expression):
            raise FilterV2Error("predicate root must resolve to BOOLEAN")
        if self.output_schema is not None and not isinstance(expression, RuntimeFilter):
            empty = pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in self.output_schema],
                schema=self.output_schema,
            )
            try:
                _evaluate_expression(expression, empty, self.context)
            except Exception as exc:
                raise FilterV2Error(f"predicate does not bind under {DUCKDB_STANDARD_V1}: {exc}") from exc
        return FilterPredicate(
            id=self._predicate_id(obj["id"], where),
            revision=_uint(obj["revision"], f"{where}.revision"),
            mode=mode,
            source=source,
            expression=expression,
        )

    @staticmethod
    def _is_boolean(expression: FilterExpression) -> bool:
        if isinstance(expression, Literal):
            return pa.types.is_boolean(expression.field.type)
        if isinstance(expression, (Comparison, BooleanExpression, Not, IsNull, In, RuntimeFilter)):
            return True
        return isinstance(expression, Call)

    def _payload(self, prefix: str, ref: object) -> tuple[pa.Field[Any], pa.Scalar[Any]]:
        index = _uint(ref, f"{prefix}_ref")
        name = f"{prefix}_{index}"
        indexes = self.batch.schema.get_all_field_indices(name)
        if len(indexes) != 1:
            raise FilterV2Error(f"missing or duplicate payload field {name!r}")
        position = indexes[0]
        field = self.batch.schema.field(position)
        if prefix != "artifact":
            _validate_arrow_extensions(field)
        return field, self.batch.column(position)[0]

    def _identity(self, raw: object, where: str) -> FunctionIdentity:
        obj = _object(raw, where)
        _keys(obj, {"namespace", "name", "version"}, set(), where)
        namespace = _string(obj["namespace"], f"{where}.namespace")
        name = _string(obj["name"], f"{where}.name")
        if not _IDENTITY_NAMESPACE.fullmatch(namespace) or not _IDENTITY_NAME.fullmatch(name):
            raise FilterV2Error(f"{where} has a noncanonical identity")
        return FunctionIdentity(namespace, name, _uint(obj["version"], f"{where}.version", positive=True))

    def _expression(self, obj: dict[str, object], depth: int, *, root: bool = False) -> FilterExpression:
        if depth > MAX_DEPTH:
            raise FilterV2Error("expression exceeds nesting-depth limit")
        self.nodes += 1
        if self.nodes > MAX_NODES:
            raise FilterV2Error("document exceeds expression-node limit")
        node = obj.get("node")

        def child(value: object) -> FilterExpression:
            return self._expression(_object(value, "child expression"), depth + 1)

        if node == "column_ref":
            _keys(obj, {"node", "column_index", "column_name"}, set(), "column_ref")
            index = _uint(obj["column_index"], "column_ref.column_index")
            name = _string(obj["column_name"], "column_ref.column_name")
            data_type: pa.DataType | None = None
            if self.output_schema is not None:
                if index >= len(self.output_schema):
                    raise FilterV2Error(f"column_ref index {index} is out of range")
                field = self.output_schema.field(index)
                _validate_arrow_extensions(field)
                if field.name != name:
                    raise FilterV2Error(f"column_ref name {name!r} does not match index {index} ({field.name!r})")
                data_type = field.type
            return ColumnRef(index, name, data_type)
        if node == "field_ref":
            _keys(obj, {"node", "expression", "field_index", "field_name"}, set(), "field_ref")
            expression = child(obj["expression"])
            parent_type = expression_type(expression)
            if parent_type is None or not pa.types.is_struct(parent_type):
                raise FilterV2Error("field_ref input must resolve to a struct type")
            index = _uint(obj["field_index"], "field_ref.field_index")
            if index >= parent_type.num_fields:
                raise FilterV2Error(f"field_ref index {index} is out of range")
            field = parent_type.field(index)
            name = _string(obj["field_name"], "field_ref.field_name")
            if field.name != name:
                raise FilterV2Error(f"field_ref name {name!r} does not match index {index} ({field.name!r})")
            return FieldRef(expression, index, name, field.type)
        if node == "literal":
            _keys(obj, {"node", "value_ref"}, set(), "literal")
            ref = _uint(obj["value_ref"], "literal.value_ref")
            field, scalar = self._payload("value", ref)
            return Literal(ref, field, scalar)
        if node == "comparison":
            _keys(obj, {"node", "op", "left", "right"}, set(), "comparison")
            try:
                comparison_op = ComparisonOperator(_string(obj["op"], "comparison.op"))
            except ValueError as exc:
                raise FilterV2Error(f"unknown comparison operator {obj['op']!r}") from exc
            return Comparison(comparison_op, child(obj["left"]), child(obj["right"]))
        if node in {"and", "or"}:
            _keys(obj, {"node", "children"}, set(), str(node))
            children = _array(obj["children"], f"{node}.children")
            if len(children) < 2:
                raise FilterV2Error(f"{node} requires at least two children")
            boolean_node: TypingLiteral["and", "or"] = "and" if node == "and" else "or"
            parsed_children = tuple(child(value) for value in children)
            if not all(self._is_boolean(value) for value in parsed_children):
                raise FilterV2Error(f"{node} children must resolve to BOOLEAN")
            return BooleanExpression(boolean_node, parsed_children)
        if node == "not":
            _keys(obj, {"node", "expression"}, set(), "not")
            expression = child(obj["expression"])
            if not self._is_boolean(expression):
                raise FilterV2Error("not input must resolve to BOOLEAN")
            return Not(expression)
        if node == "is_null":
            _keys(obj, {"node", "expression", "negated"}, set(), "is_null")
            return IsNull(child(obj["expression"]), _bool(obj["negated"], "is_null.negated"))
        if node == "in":
            _keys(obj, {"node", "expression", "set", "negated"}, set(), "in")
            expression = child(obj["expression"])
            set_obj = _object(obj["set"], "in.set")
            if set_obj.get("kind") == "literal":
                _keys(set_obj, {"kind", "value_ref"}, set(), "in.set")
                ref = _uint(set_obj["value_ref"], "in.set.value_ref")
                field, scalar = self._payload("value", ref)
                if not (pa.types.is_list(field.type) or pa.types.is_large_list(field.type)):
                    raise FilterV2Error("literal IN payload must be a list scalar")
                if not scalar.is_valid:
                    raise FilterV2Error("literal IN list must not be NULL")
                values: pa.Array[Any] = scalar.values  # type: ignore[attr-defined]
                value_set: FilterSet = LiteralSet(ref, field, values)
            elif set_obj.get("kind") == "external":
                _keys(set_obj, {"kind", "batch_index", "column_index", "column_name"}, set(), "in.set")
                batch_index = _uint(set_obj["batch_index"], "in.set.batch_index")
                column_index = _uint(set_obj["column_index"], "in.set.column_index")
                name = _string(set_obj["column_name"], "in.set.column_name")
                if batch_index >= len(self.join_keys):
                    raise FilterV2Error(f"external IN batch index {batch_index} is unavailable")
                keys = self.join_keys[batch_index]
                if column_index >= keys.num_columns:
                    raise FilterV2Error(f"external IN column index {column_index} is out of range")
                if keys.schema.field(column_index).name != name:
                    raise FilterV2Error("external IN column name does not match its authoritative index")
                _validate_arrow_extensions(keys.schema.field(column_index))
                value_set = ExternalSet(batch_index, column_index, name, keys.column(column_index), keys)
            else:
                raise FilterV2Error("in.set has an unknown kind")
            return In(expression, value_set, _bool(obj["negated"], "in.negated"))
        if node == "cast":
            _keys(obj, {"node", "expression", "type_ref"}, set(), "cast")
            ref = _uint(obj["type_ref"], "cast.type_ref")
            field, scalar = self._payload("type", ref)
            if scalar.is_valid:
                raise FilterV2Error("cast type payload must contain NULL")
            return Cast(child(obj["expression"]), ref, field)
        if node == "arithmetic":
            _keys(obj, {"node", "op", "left", "right"}, set(), "arithmetic")
            try:
                arithmetic_op = ArithmeticOperator(_string(obj["op"], "arithmetic.op"))
            except ValueError as exc:
                raise FilterV2Error(f"unknown arithmetic operator {obj['op']!r}") from exc
            return Arithmetic(arithmetic_op, child(obj["left"]), child(obj["right"]))
        if node == "negate":
            _keys(obj, {"node", "expression"}, set(), "negate")
            return Negate(child(obj["expression"]))
        if node == "call":
            _keys(obj, {"node", "function", "arguments"}, {"options"}, "call")
            function_raw = obj["function"]
            if isinstance(function_raw, str):
                try:
                    function: StandardFilterFunction | FunctionIdentity = StandardFilterFunction(function_raw)
                except ValueError as exc:
                    raise FilterV2Error(f"unknown standard filter function {function_raw!r}") from exc
            else:
                function = self._identity(function_raw, "call.function")
                identity_key = (function.namespace, function.name, function.version)
                if identity_key not in {("duckdb.spatial", "intersects_extent", 1)}:
                    raise FilterV2Error(
                        f"unknown extension filter function {function.namespace}/{function.name}@{function.version}"
                    )
                if identity_key not in self.extension_functions:
                    raise FilterV2Error(
                        f"extension filter function {function.namespace}/{function.name}@{function.version} "
                        "was not advertised"
                    )
            arguments = _array(obj["arguments"], "call.arguments")
            if len(arguments) > MAX_ARGUMENTS:
                raise FilterV2Error("call exceeds argument-count limit")
            options = obj.get("options")
            if options is not None:
                if isinstance(function, StandardFilterFunction):
                    raise FilterV2Error("standard filter functions do not accept options")
                options = _object(options, "call.options")
                if options:
                    raise FilterV2Error(
                        f"{function.namespace}/{function.name}@{function.version} does not accept options"
                    )
            return Call(function, tuple(child(value) for value in arguments), options)
        if node == "runtime_filter":
            _keys(obj, {"node", "algorithm", "input", "artifact_ref", "null_handling"}, set(), "runtime_filter")
            if not root:
                raise FilterV2Error("runtime_filter may appear only as a predicate root")
            algorithm = self._identity(obj["algorithm"], "runtime_filter.algorithm")
            known = (algorithm.namespace, algorithm.name, algorithm.version) in {
                ("duckdb.runtime_filter", "bloom", 1),
                ("duckdb.runtime_filter", "prefix_range", 1),
            }
            if not known:
                raise FilterV2Error("unknown runtime-filter algorithm")
            ref = _uint(obj["artifact_ref"], "runtime_filter.artifact_ref")
            field, artifact = self._payload("artifact", ref)
            null_handling = obj["null_handling"]
            if null_handling not in {"pass", "reject"}:
                raise FilterV2Error("runtime_filter.null_handling must be 'pass' or 'reject'")
            typed_null_handling: TypingLiteral["pass", "reject"] = "pass" if null_handling == "pass" else "reject"
            supported = (algorithm.namespace, algorithm.name, algorithm.version) in self.runtime_algorithms
            if supported:
                raise FilterV2Error("advertised runtime-filter algorithm has no registered artifact evaluator")
            return RuntimeFilter(algorithm, child(obj["input"]), ref, field, artifact, typed_null_handling, supported)
        raise FilterV2Error(f"unknown expression node {node!r}")


def expression_type(expression: FilterExpression) -> pa.DataType | None:
    """Return the statically known Arrow type of an expression, if available."""
    if isinstance(expression, ColumnRef):
        return expression.data_type
    if isinstance(expression, FieldRef):
        return expression.data_type
    if isinstance(expression, Literal):
        return expression.field.type  # type: ignore[no-any-return]
    if isinstance(expression, Cast):
        return expression.field.type  # type: ignore[no-any-return]
    if isinstance(expression, (Comparison, BooleanExpression, Not, IsNull, In, RuntimeFilter, Call)):
        return pa.bool_()
    return None


def _requires_session_context(expression: FilterExpression) -> bool:
    """Conservatively detect core expressions governed by DuckDB session state."""
    if isinstance(expression, Arithmetic):
        return (
            expression.op in {ArithmeticOperator.DIVIDE, ArithmeticOperator.MODULO}
            or _requires_session_context(expression.left)
            or _requires_session_context(expression.right)
        )
    if isinstance(expression, Cast):
        source = expression_type(expression.expression)
        target = expression.field.type

        def contextual(value: pa.DataType | None) -> bool:
            return value is not None and (
                pa.types.is_string(value)
                or pa.types.is_large_string(value)
                or pa.types.is_date(value)
                or pa.types.is_time(value)
                or pa.types.is_timestamp(value)
            )

        return contextual(source) and contextual(target) or _requires_session_context(expression.expression)
    if isinstance(expression, FieldRef):
        return _requires_session_context(expression.expression)
    if isinstance(expression, Comparison):
        return _requires_session_context(expression.left) or _requires_session_context(expression.right)
    if isinstance(expression, BooleanExpression):
        return any(_requires_session_context(value) for value in expression.children)
    if isinstance(expression, In):
        return _requires_session_context(expression.expression)
    if isinstance(expression, (Not, IsNull, Negate)):
        return _requires_session_context(expression.expression)
    if isinstance(expression, Call):
        return any(_requires_session_context(value) for value in expression.arguments)
    if isinstance(expression, RuntimeFilter):
        return _requires_session_context(expression.input)
    return False


def deserialize_snapshot(
    batch: pa.RecordBatch,
    *,
    output_schema: pa.Schema | None = None,
    join_keys: list[pa.RecordBatch] | None = None,
    extension_functions: frozenset[tuple[str, str, int]] = frozenset(),
    runtime_algorithms: frozenset[tuple[str, str, int]] = frozenset(),
    evaluation_capabilities: tuple[tuple[str, str | None], ...] = (),
) -> FilterState:
    """Strictly decode an initial v2 snapshot batch."""
    return _Parser(
        batch,
        output_schema,
        join_keys,
        extension_functions=extension_functions,
        runtime_algorithms=runtime_algorithms,
        evaluation_capabilities=evaluation_capabilities,
    ).parse_snapshot()


_evaluation_local = threading.local()


def _quote_identifier(value: str) -> str:
    return f'"{value.replace(chr(34), chr(34) * 2)}"'


def expression_columns(expression: FilterExpression) -> tuple[ColumnRef, ...]:
    """Return column references in stable first-occurrence order."""
    result: list[ColumnRef] = []
    seen: set[tuple[int, str]] = set()

    def visit(item: FilterExpression) -> None:
        if isinstance(item, ColumnRef):
            key = (item.column_index, item.column_name)
            if key not in seen:
                seen.add(key)
                result.append(item)
        elif isinstance(item, FieldRef):
            visit(item.expression)
        elif isinstance(item, (Comparison, Arithmetic)):
            visit(item.left)
            visit(item.right)
        elif isinstance(item, BooleanExpression):
            for child in item.children:
                visit(child)
        elif isinstance(item, (Not, IsNull, Cast, Negate, In)):
            visit(item.expression)
        elif isinstance(item, Call):
            for argument in item.arguments:
                visit(argument)
        elif isinstance(item, RuntimeFilter):
            visit(item.input)

    visit(expression)
    return tuple(result)


def _expression_sql(expression: FilterExpression) -> str:
    if isinstance(expression, ColumnRef):
        return f"input.{_quote_identifier(f'__vgi_col_{expression.column_index}')}"
    if isinstance(expression, FieldRef):
        return f"struct_extract({_expression_sql(expression.expression)}, {_quote_string(expression.field_name)})"
    if isinstance(expression, Literal):
        return f"payload.{_quote_identifier(f'value_{expression.value_ref}')}"
    if isinstance(expression, Comparison):
        comparison_symbols: dict[ComparisonOperator, str] = {
            ComparisonOperator.EQ: "=",
            ComparisonOperator.NE: "!=",
            ComparisonOperator.LT: "<",
            ComparisonOperator.LE: "<=",
            ComparisonOperator.GT: ">",
            ComparisonOperator.GE: ">=",
            ComparisonOperator.DISTINCT_FROM: "IS DISTINCT FROM",
            ComparisonOperator.NOT_DISTINCT_FROM: "IS NOT DISTINCT FROM",
        }
        return (
            f"({_expression_sql(expression.left)} {comparison_symbols[expression.op]} "
            f"{_expression_sql(expression.right)})"
        )
    if isinstance(expression, BooleanExpression):
        joiner = f" {expression.node.upper()} "
        return f"({joiner.join(_expression_sql(child) for child in expression.children)})"
    if isinstance(expression, Not):
        return f"(NOT {_expression_sql(expression.expression)})"
    if isinstance(expression, IsNull):
        suffix = "IS NOT NULL" if expression.negated else "IS NULL"
        return f"({_expression_sql(expression.expression)} {suffix})"
    if isinstance(expression, In):
        if isinstance(expression.set, LiteralSet):
            source = f"SELECT * FROM unnest(payload.{_quote_identifier(f'value_{expression.set.value_ref}')})"
        else:
            source = (
                f"SELECT {_quote_identifier(expression.set.column_name)} FROM vgi_keys_{expression.set.batch_index}"
            )
        operator = "NOT IN" if expression.negated else "IN"
        return f"({_expression_sql(expression.expression)} {operator} ({source}))"
    if isinstance(expression, Cast):
        target = f"payload.{_quote_identifier(f'type_{expression.type_ref}')}"
        return f"cast_to_type({_expression_sql(expression.expression)}, {target})"
    if isinstance(expression, Arithmetic):
        arithmetic_symbols: dict[ArithmeticOperator, str] = {
            ArithmeticOperator.ADD: "+",
            ArithmeticOperator.SUBTRACT: "-",
            ArithmeticOperator.MULTIPLY: "*",
            ArithmeticOperator.DIVIDE: "/",
            ArithmeticOperator.MODULO: "%",
        }
        return (
            f"({_expression_sql(expression.left)} {arithmetic_symbols[expression.op]} "
            f"{_expression_sql(expression.right)})"
        )
    if isinstance(expression, Negate):
        return f"(-{_expression_sql(expression.expression)})"
    if isinstance(expression, Call):
        arguments = ", ".join(_expression_sql(argument) for argument in expression.arguments)
        if isinstance(expression.function, StandardFilterFunction):
            return f"{expression.function.value}({arguments})"
        identity = expression.function
        if (identity.namespace, identity.name, identity.version) == ("duckdb.spatial", "intersects_extent", 1):
            if len(expression.arguments) != 2 or expression.options:
                raise FilterV2Error("duckdb.spatial/intersects_extent@1 requires exactly two arguments and no options")
            return f"({_expression_sql(expression.arguments[0])} && {_expression_sql(expression.arguments[1])})"
        raise FilterV2Error(
            f"no evaluator for extension function {identity.namespace}/{identity.name}@{identity.version}"
        )
    raise FilterV2Error("runtime-filter artifacts require a negotiated algorithm evaluator")


def expression_sql(expression: FilterExpression) -> str:
    """Render an expression for the embedded DuckDB reference evaluator."""
    return _expression_sql(expression)


def _quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _get_evaluation_connection(context: EvaluationContext) -> Any:
    connections = getattr(_evaluation_local, "connections", None)
    if connections is None:
        connections = {}
        _evaluation_local.connections = connections
    key = (
        context.profile,
        context.time_zone,
        context.calendar,
        context.default_collation,
        context.ieee_floating_point_ops,
        context.integer_division,
        context.provider_fingerprint,
    )
    connection = connections.get(key)
    if connection is None:
        from vgi._duckdb import connect, engine_module

        engine = engine_module()
        engine_name = engine.__name__.split(".", 1)[0]
        engine_version = str(getattr(engine, "__version__", ""))
        accepted_versions = {
            "duckdb": {"1.5.5"},
            "haybarn": {"1.5.5rc1", "1.5.5-rc1"},
        }
        if engine_version not in accepted_versions.get(engine_name, set()):
            raise FilterV2Error(
                f"{DUCKDB_STANDARD_V1} requires DuckDB 1.5.5 or Haybarn 1.5.5-rc1; "
                f"found {engine_name} {engine_version or '<unknown>'}"
            )

        connection = connect()
        _apply_context(connection, context)
        connections[key] = connection
    return connection


def _apply_context(connection: Any, context: EvaluationContext) -> None:
    if context.profile == NO_EVALUATION_CONTEXT:
        # Context-free string operations use the standard-v1 binary baseline,
        # never a mutable default inherited from another evaluator session.
        connection.execute("SET default_collation = 'binary'")
        return
    try:
        connection.load_extension("icu")
    except Exception as exc:
        raise FilterV2Error("vgi.duckdb.session.v1 requires DuckDB's ICU extension") from exc
    settings = {
        "TimeZone": context.time_zone,
        "Calendar": context.calendar,
        "default_collation": context.default_collation,
        "ieee_floating_point_ops": context.ieee_floating_point_ops,
        "integer_division": context.integer_division,
    }
    for name, value in settings.items():
        connection.execute(f"SET {_quote_identifier(name)} = ?", [value])
    try:
        value = connection.sql("SELECT current_setting('null_on_division_by_zero')").fetchone()[0]
    except Exception:
        value = False
    if value not in {False, "false", "False"}:
        raise FilterV2Error("DuckDB evaluator must have null_on_division_by_zero=false")


def _evaluate_expression(
    expression: FilterExpression,
    batch: pa.RecordBatch,
    context: EvaluationContext,
) -> pa.BooleanArray:
    connection = _get_evaluation_connection(context)
    referenced = expression_columns(expression)
    input_fields: list[pa.Field[Any]] = []
    input_arrays: list[pa.Array[Any]] = []
    for column in referenced:
        if (
            column.column_index < batch.num_columns
            and batch.schema.field(column.column_index).name == column.column_name
        ):
            position = column.column_index
        else:
            positions = batch.schema.get_all_field_indices(column.column_name)
            if len(positions) != 1:
                raise FilterV2Error(
                    f"referenced column {column.column_name!r} is unavailable or ambiguous for evaluation"
                )
            position = positions[0]
        actual = batch.schema.field(position)
        if column.data_type is not None and actual.type != column.data_type:
            raise FilterV2Error(f"referenced column {column.column_name!r} changed type before evaluation")
        input_fields.append(pa.field(f"__vgi_col_{column.column_index}", actual.type, metadata=actual.metadata))
        input_arrays.append(batch.column(position))
    if not input_fields:
        input_fields.append(pa.field("__vgi_row", pa.bool_(), nullable=False))
        input_arrays.append(pa.array([True] * batch.num_rows))
    # A one-row payload relation preserves Arrow literal and cast-target types.
    payload_fields: list[pa.Field[Any]] = []
    payload_arrays: list[pa.Array[Any]] = []
    seen_payloads: set[str] = set()

    def add_payload(item: FilterExpression) -> None:
        field: pa.Field[Any] | None = None
        scalar: pa.Scalar[Any] | None = None
        if isinstance(item, Literal):
            field, scalar = item.field, item.value
        elif isinstance(item, Cast):
            field = item.field
            scalar = pa.scalar(None, type=field.type)
            add_payload(item.expression)
        elif isinstance(item, FieldRef):
            add_payload(item.expression)
        elif isinstance(item, (Comparison, Arithmetic)):
            add_payload(item.left)
            add_payload(item.right)
        elif isinstance(item, BooleanExpression):
            for nested in item.children:
                add_payload(nested)
        elif isinstance(item, (Not, IsNull, Negate)):
            add_payload(item.expression)
        elif isinstance(item, In):
            add_payload(item.expression)
            if isinstance(item.set, LiteralSet):
                field = item.set.field
                scalar = pa.scalar(item.set.values.to_pylist(), type=field.type)
        elif isinstance(item, Call):
            for argument in item.arguments:
                add_payload(argument)
        if field is not None and scalar is not None and field.name not in seen_payloads:
            seen_payloads.add(field.name)
            payload_fields.append(field)
            payload_arrays.append(pa.array([scalar], type=field.type))

    add_payload(expression)
    if not payload_fields:
        payload_fields.append(pa.field("__unit", pa.bool_(), nullable=False))
        payload_arrays.append(pa.array([True]))
    payload = pa.RecordBatch.from_arrays(payload_arrays, schema=pa.schema(payload_fields))
    evaluation_batch = pa.RecordBatch.from_arrays(input_arrays, schema=pa.schema(input_fields))
    connection.register("vgi_filter_input", evaluation_batch)
    connection.register("vgi_filter_payload", payload)
    for index, keys in _external_batches(expression):
        connection.register(f"vgi_keys_{index}", keys)
    sql = _expression_sql(expression)
    result = connection.sql(
        f"SELECT ({sql})::BOOLEAN AS result FROM vgi_filter_input AS input CROSS JOIN vgi_filter_payload AS payload"
    )
    fetch = getattr(result, "to_arrow_table", None) or result.fetch_arrow_table
    return fetch().column("result").combine_chunks()  # type: ignore[no-any-return]


def evaluate_expression(
    expression: FilterExpression,
    batch: pa.RecordBatch,
    context: EvaluationContext,
) -> pa.BooleanArray:
    """Evaluate one v2 Boolean expression with DuckDB reference semantics."""
    return _evaluate_expression(expression, batch, context)


def _external_batches(expression: FilterExpression) -> tuple[tuple[int, pa.RecordBatch], ...]:
    found: dict[int, pa.RecordBatch] = {}

    def visit(item: FilterExpression) -> None:
        if isinstance(item, In) and isinstance(item.set, ExternalSet):
            found[item.set.batch_index] = item.set.batch
        elif isinstance(item, FieldRef):
            visit(item.expression)
        elif isinstance(item, (Comparison, Arithmetic)):
            visit(item.left)
            visit(item.right)
        elif isinstance(item, BooleanExpression):
            for nested in item.children:
                visit(nested)
        elif isinstance(item, (Not, IsNull, Cast, Negate, In)):
            visit(item.expression)
        elif isinstance(item, Call):
            for argument in item.arguments:
                visit(argument)

    visit(expression)
    return tuple(sorted(found.items()))
