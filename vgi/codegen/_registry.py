# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The language-neutral model behind every SDK's generated ``vgi.v2`` registry.

Why the registry is generated
-----------------------------

``vgi.v2`` is the unit of optionality: every SDK hosts *every* method, with
exactly the reference's method type and params / result / header schemas, so
``vgi_rpc.Reflection.v1`` reports one protocol hash everywhere. A method an SDK
does not implement is still registered and answers ``UNIMPLEMENTED`` /
``method_not_implemented`` / ``"<method> is not implemented by this worker"``.

Each SDK used to write that registration by hand, and it drifted: vgi-java was
missing 28 methods, vgi-csharp 19, with wrong init headers and result
nullability, all hidden until the hash was compared. The request / response
*types* were already generated; the method table that binds them to the server
was the last hand-maintained copy of the protocol. Now it is generated too.

One generator, one backend per language
---------------------------------------

There is one registry generator. It has three layers:

1. **The model** (this module). :func:`registry_methods` reads
   :class:`vgi.protocol.VgiProtocol` into :class:`RegistryMethod` values, and
   everything a language would otherwise re-derive is a property or helper
   here: the :class:`MethodKind` (``Unary`` / ``Void`` / ``Stream``), whether a
   method is in the routable catalog family (:attr:`RegistryMethod.is_catalog`),
   whether a stream needs a state decoder for HTTP continuation, the
   params / header fields ready to emit and the stems of their schema
   factories (:class:`SchemaNames`), which annotations are packed records
   (:func:`is_record`, :func:`records_used`), wire-name -> identifier mapping
   with a collision check (:func:`identifiers`), the Arrow -> native type hook
   that rejects an unmapped type (:func:`map_arrow_type`), and validation of a
   backend's hand-kept name tables (:func:`check_record_table`,
   :func:`check_raw_results`). :func:`preimage_hash` hashes a method list
   exactly as vgi-rpc hashes a protocol.

2. **The backend contract** (:mod:`vgi.codegen._registry_backend`). A
   :class:`~vgi.codegen._registry_backend.RegistryBackend` supplies two things:
   ``render_body(methods)`` -- the language's file, from the model -- and
   ``derive(text)`` -- that file read back the way the language's vgi-rpc port
   reads it, as :class:`~vgi.codegen._registry_backend.DerivedMethod` values.
   The base class does everything else: provenance banner, ``render`` /
   ``emit`` / ``main``, the drift target, the registration rows an
   explicit-registration port walks (with the port's "no result" convention
   applied), rebuilding derived methods into :class:`RegistryMethod` values for
   the hash, and a shared Arrow-expression evaluator
   (:class:`~vgi.codegen._registry_backend.ArrowDialect`), so a port that
   registers schemas as values supplies only a rewrite table from its spelling
   to call syntax.

3. **Six backends** (``java_registry``, ``csharp_registry``, ``ts_registry``,
   ``go_registry``, ``rust_registry``, ``cpp_registry``). Each decides only
   what is idiomatic in its language: type names, signatures, the default body
   that raises the SDK's ``UNIMPLEMENTED``, and, for an explicit-registration
   port, how the registration table is spelled. Nothing protocol-specific lives
   in a backend: the method set, the parameter order, wire names and Arrow
   types, nullability, result presence and the stream header all come from
   here.

Every backend's output is three things: **a complete declaration**
(interface, trait, abstract class) of every ``vgi.v2`` method with its exact
types; **a default implementation** in which every method answers the SDK's
``UNIMPLEMENTED``, which SDK code inherits and overrides; and **the
registration** that hands the declaration to the vgi-rpc server. In Java and
C# the server reflects over the interface, so the declaration *is* the table;
Go, Rust, TypeScript and C++ register explicitly, so the table is emitted too.

Checking a generator
--------------------

``tests/test_generated_registry.py`` runs one parametrization over every
backend: render, ``derive`` back, rebuild, and assert :func:`preimage_hash`
equals the live ``VgiProtocol`` hash (computed, never pinned); apply the
backend's declared ``tamper`` (one rendered nullability or envelope flipped)
and assert the hash moves, so the check is not vacuous; render twice for
determinism; and compare with the sibling SDK checkout for drift. A mapping
bug fails in vgi-python rather than as a hash mismatch (or an Arrow schema
rejection) in the SDK. The SDK keeps its own pinned-hash test end to end.

Adding a seventh language
-------------------------

1. Write ``vgi/codegen/<lang>_registry.py`` with a ``RegistryBackend``
   subclass: ``key``, ``language``, ``module``, ``target``, ``repo``,
   ``root_env``, ``void_result`` (how the port's vgi-rpc spells a unary with
   no return), a ``tamper``, ``render_body`` and ``derive``. Bind the module
   API with ``BACKEND = <Lang>Registry()`` and
   ``emit, render, main = BACKEND.emit, BACKEND.render, BACKEND.main``.
2. Reuse the language's ``*_types`` / ``*_schemas`` generator for type names
   and field expressions; map anything else through :func:`map_arrow_type`.
3. Derive back by the port's own rules: a reflection port parses signatures
   (see Java, C#); a schema-as-value port declares an ``ArrowDialect`` and
   evaluates the rendered schema expressions (see TypeScript, Rust, C++).
4. Append the module to ``REGISTRY_BACKENDS`` in ``_registry_backend``. The
   regen script, the drift check and every parametrized test pick it up.
5. Keep any hand-kept table small and wire-identical (a typed record vs raw
   IPC bytes for one ``binary`` column), validated by
   :func:`check_record_table` / :func:`check_raw_results`.
"""

from __future__ import annotations

import dataclasses
import enum
import functools
import hashlib
import inspect
import types
import typing
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from vgi_rpc.rpc._protocol_hash import HASH_DOMAIN, canonical_json
from vgi_rpc.rpc._type_tokens import field_token, schema_tokens
from vgi_rpc.rpc._types import MethodType, rpc_methods  # type: ignore[attr-defined]

from vgi.codegen._common import GeneratorError
from vgi.protocol import VgiProtocol

#: Marker for a parameter with no default.
NO_DEFAULT: Any = inspect.Parameter.empty


class MethodKind(enum.Enum):
    """How a port registers a method: a unary with a return, one without, or a stream."""

    UNARY = "Unary"
    VOID = "Void"
    STREAM = "Stream"


def is_record(annotation: object) -> bool:
    """Whether *annotation* is a dataclass carrying an ``ARROW_SCHEMA`` (a packed request/response)."""
    return (
        isinstance(annotation, type)
        and dataclasses.is_dataclass(annotation)
        and isinstance(getattr(annotation, "ARROW_SCHEMA", None), pa.Schema)
    )


def record_name(annotation: object) -> str | None:
    """The Python class name of a packed record annotation, or ``None`` when it is not one."""
    return annotation.__name__ if is_record(annotation) else None  # type: ignore[attr-defined]


@dataclass(frozen=True)
class RegistryParam:
    """One wire parameter of a ``vgi.v2`` method, in params-schema order.

    Attributes:
        name: The wire name (snake_case), which is also the params-schema field name.
        field: The params-schema field: Arrow type and nullability.
        annotation: The Python annotation, used only to choose among target types
            that derive the same Arrow type (a request record vs raw ``bytes``, an
            enum vs a dictionary-encoded string).
        default: The Python default, or :data:`NO_DEFAULT`. Not part of the wire.
    """

    name: str
    field: pa.Field[Any]
    annotation: object
    default: object = NO_DEFAULT

    @property
    def record(self) -> str | None:
        """The packed record's Python class name, or ``None`` for a plain column."""
        return record_name(self.annotation)


@dataclass(frozen=True)
class SchemaNames:
    """The stems a port's schema factories / constants are named by.

    ``params`` and ``result`` are ``<Pascal>Params`` / ``<Pascal>Result`` (the
    names :mod:`vgi.codegen.cpp_schemas` and friends emit); ``header`` is the
    header record's class name, or ``None``.
    """

    params: str
    result: str
    header: str | None


@dataclass(frozen=True)
class RegistryMethod:
    """One ``vgi.v2`` method, everything a registry needs to declare it.

    Attributes:
        name: The wire method name.
        method_type: ``UNARY`` or ``STREAM``.
        params: The wire parameters, in order.
        result_field: The unary result column (always named ``result``), or
            ``None`` when the method returns nothing (and for streams).
        result_annotation: The Python return annotation (unwrapped from
            ``Optional``); for a stream, the ``Stream[...]`` annotation.
        header_type: The stream header dataclass, or ``None``.
        doc: The Python docstring, cleaned, or ``None``.
    """

    name: str
    method_type: MethodType
    params: tuple[RegistryParam, ...]
    result_field: pa.Field[Any] | None
    result_annotation: object
    header_type: type | None
    doc: str | None

    @property
    def is_stream(self) -> bool:
        """Whether this is a streaming method."""
        return bool(self.method_type == MethodType.STREAM)

    @property
    def kind(self) -> MethodKind:
        """``Stream``, ``Unary`` (a unary with a ``result`` column) or ``Void``."""
        if self.is_stream:
            return MethodKind.STREAM
        return MethodKind.UNARY if self.result_field is not None else MethodKind.VOID

    @property
    def is_catalog(self) -> bool:
        """Whether this is a routable catalog method (the ``catalog_*`` family).

        A port that unseals opaque attach / transaction values and routes a call
        to the catalog that owns it (vgi-go's ``unaryCatalog``) does it for
        exactly these. Defined once, here, rather than by each backend's prefix test.
        """
        return self.name.startswith("catalog_")

    @property
    def needs_state_decoder(self) -> bool:
        """Whether the method needs a state decoder: every stream, for HTTP continuation."""
        return self.is_stream

    @property
    def result_record(self) -> str | None:
        """The result's packed-record class name, or ``None`` (raw bytes, void, stream)."""
        return record_name(self.result_annotation) if self.result_field is not None else None

    @property
    def header_schema(self) -> pa.Schema | None:
        """The stream header's Arrow schema, or ``None``."""
        if self.header_type is None:
            return None
        schema = getattr(self.header_type, "ARROW_SCHEMA", None)
        if not isinstance(schema, pa.Schema):
            raise GeneratorError(f"{self.name}: header type {self.header_type!r} has no ARROW_SCHEMA")
        return schema

    @property
    def params_fields(self) -> list[pa.Field[Any]]:
        """The params-schema fields, in order, ready to emit."""
        return [p.field for p in self.params]

    @property
    def header_fields(self) -> list[pa.Field[Any]] | None:
        """The stream header's fields, or ``None`` for a unary or a headerless stream."""
        header = self.header_schema
        return None if header is None else list(header)

    @property
    def pascal(self) -> str:
        """``catalog_schema_get`` -> ``CatalogSchemaGet``."""
        return pascal(self.name)

    @property
    def schema_names(self) -> SchemaNames:
        """The stems of this method's params / result / header schema factories."""
        header = None if self.header_type is None else self.header_type.__name__
        return SchemaNames(f"{self.pascal}Params", f"{self.pascal}Result", header)

    @property
    def summary(self) -> str | None:
        """The docstring's first paragraph, whitespace-normalized."""
        return summary(self.doc)


def protocol_name(protocol_cls: type = VgiProtocol) -> str:
    """The protocol's wire name (its ``vgi_rpc.protocol`` routing key)."""
    value = vars(protocol_cls).get("protocol_name")
    if not isinstance(value, str):
        raise GeneratorError(f"{protocol_cls.__name__}.protocol_name must be declared as a str")
    return value


def protocol_version(protocol_cls: type = VgiProtocol) -> str:
    """The protocol's semver, which clients stamp on every request."""
    value = vars(protocol_cls).get("protocol_version")
    if not isinstance(value, str):
        raise GeneratorError(f"{protocol_cls.__name__}.protocol_version must be declared as a str")
    return value


def _strip_optional(annotation: object) -> object:
    """``T`` for ``T | None`` (or ``Optional[T]``); anything else unchanged."""
    if typing.get_origin(annotation) in (typing.Union, types.UnionType):
        rest = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(rest) == 1:
            return rest[0]
    return annotation


def registry_methods(protocol_cls: type = VgiProtocol) -> list[RegistryMethod]:
    """Every method of *protocol_cls*, sorted by wire name, as a registry sees it."""
    return list(_registry_methods(protocol_cls))


@functools.cache
def _registry_methods(protocol_cls: type) -> tuple[RegistryMethod, ...]:
    out: list[RegistryMethod] = []
    methods = rpc_methods(protocol_cls)
    for name in sorted(methods):
        info = methods[name]
        func = getattr(protocol_cls, name)
        hints = typing.get_type_hints(func, include_extras=True)
        signature = inspect.signature(func)
        wire_params = [p for p in signature.parameters if p != "self"]
        schema: pa.Schema = info.params_schema
        if list(schema.names) != wire_params:
            raise GeneratorError(
                f"{name}: params schema {schema.names} does not match the Python signature {wire_params}"
            )
        params = tuple(
            RegistryParam(
                name=p,
                field=schema.field(p),
                annotation=_strip_optional(hints.get(p)),
                default=signature.parameters[p].default,
            )
            for p in wire_params
        )

        result_field: pa.Field[Any] | None = None
        if info.method_type == MethodType.UNARY and info.has_return:
            result_schema: pa.Schema = info.result_schema
            if len(result_schema) != 1 or result_schema.field(0).name != "result":
                raise GeneratorError(f"{name}: expected a single 'result' column, got {result_schema}")
            result_field = result_schema.field(0)
        if info.method_type == MethodType.UNARY and info.header_type is not None:
            raise GeneratorError(f"{name}: a unary method has no stream header")

        out.append(
            RegistryMethod(
                name=name,
                method_type=info.method_type,
                params=params,
                result_field=result_field,
                result_annotation=_strip_optional(hints.get("return")),
                header_type=info.header_type,
                doc=inspect.getdoc(func),
            )
        )
    return tuple(out)


# ---------------------------------------------------------------------------
# The protocol hash
# ---------------------------------------------------------------------------


def preimage(name: str, methods: Sequence[RegistryMethod]) -> dict[str, Any]:
    """The ``vgi_rpc.protocol_hash.v1`` description of a method table.

    Mirrors ``vgi_rpc.rpc._protocol_hash.protocol_description`` field for field,
    but over :class:`RegistryMethod` values, so a backend's derive-back can
    build the table from what it *rendered* and compare digests.
    """
    entries: list[dict[str, Any]] = []
    for m in sorted(methods, key=lambda m: m.name):
        header = m.header_schema
        entry: dict[str, Any] = {
            "name": m.name,
            "type": m.method_type.value,
            "has_return": m.result_field is not None,
            "has_header": header is not None,
            "params": [field_token(p.field) for p in m.params],
        }
        if m.result_field is not None:
            entry["result"] = [field_token(m.result_field)]
        if header is not None:
            entry["header"] = schema_tokens(header)
        entries.append(entry)
    return {"protocol": name, "methods": entries}


def preimage_hash(name: str, methods: Sequence[RegistryMethod]) -> str:
    """SHA-256 of :func:`preimage`, exactly as vgi-rpc computes a protocol hash."""
    return hashlib.sha256(HASH_DOMAIN + canonical_json(preimage(name, methods))).hexdigest()


# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------


def camel(name: str) -> str:
    """``catalog_schema_get`` -> ``catalogSchemaGet``."""
    head, *rest = name.split("_")
    return head + "".join(p.capitalize() for p in rest)


def pascal(name: str) -> str:
    """``catalog_schema_get`` -> ``CatalogSchemaGet``."""
    return "".join(p.capitalize() for p in name.split("_"))


def identifiers(names: Iterable[str], convert: Callable[[str], str], *, what: str) -> dict[str, str]:
    """Map each wire name to its identifier, refusing two that collide.

    *what* names the identifier space in the error (``"TypeScript VgiService key"``).
    """
    out: dict[str, str] = {}
    owner: dict[str, str] = {}
    for name in names:
        ident = convert(name)
        if ident in owner:
            raise GeneratorError(f"{name} and {owner[ident]} both map to the {what} {ident}")
        owner[ident] = name
        out[name] = ident
    return out


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

#: A backend's Arrow -> native mapping: ``(dtype, recurse) -> native or None``.
#: ``recurse(child_dtype, origin_suffix)`` maps a nested type through the same rules.
TypeMapping = Callable[[pa.DataType, Callable[[pa.DataType, str], str]], str | None]


def map_arrow_type(dtype: pa.DataType, mapping: TypeMapping, *, origin: str, language: str, hint: str) -> str:
    """Map an Arrow type to a native one through *mapping*, rejecting a type it does not cover.

    A mapping returns ``None`` for a type it has no rule for; the rejection is
    here, so no backend can fall back to ``any`` / ``Object`` by omission.
    *hint* names the function to extend.
    """

    def recurse(child: pa.DataType, suffix: str) -> str:
        return map_arrow_type(child, mapping, origin=f"{origin}{suffix}", language=language, hint=hint)

    native = mapping(dtype, recurse)
    if native is None:
        raise GeneratorError(f"{origin}: no {language} mapping for Arrow type {dtype}; extend {hint}")
    return native


def records_used(methods: Iterable[RegistryMethod], *, headers: bool = True) -> set[str]:
    """Every packed-record class name a param, a result (or, with *headers*, a stream header) carries."""
    used: set[str] = set()
    for m in methods:
        used.update(p.record for p in m.params if p.record is not None)
        if (r := record_name(m.result_annotation)) is not None:
            used.add(r)
        if headers and m.header_type is not None:
            used.add(m.header_type.__name__)
    return used


def check_record_table(
    table: Mapping[str, str], methods: Sequence[RegistryMethod], *, what: str, headers: bool
) -> None:
    """Refuse a backend name-table entry for a record no ``vgi.v2`` method carries (a typo binds nothing)."""
    stale = sorted(set(table) - records_used(methods, headers=headers))
    if stale:
        raise GeneratorError(f"{what} names records no vgi.v2 method carries: {stale}")


def check_raw_results(
    table: Mapping[str, str], methods: Sequence[RegistryMethod], *, what: str, non_null: bool = False
) -> None:
    """Validate a "raw ``bytes`` result returned as a typed record" table.

    Every key must be a method whose result is raw IPC ``bytes`` in one
    ``binary`` column (a typed record is the same column). With *non_null*, the
    column must also be non-nullable -- for a port whose record type always
    derives a non-null result.
    """
    by_name = {m.name: m for m in methods}
    unknown = sorted(set(table) - set(by_name))
    if unknown:
        raise GeneratorError(f"{what} names unknown methods: {unknown}")
    for name in table:
        m = by_name[name]
        f = m.result_field
        if m.result_annotation is not bytes or f is None or not f.type.equals(pa.binary()):
            raise GeneratorError(f"{what}[{name!r}] applies only to a raw-bytes result")
        if non_null and f.nullable:
            raise GeneratorError(f"{what}[{name!r}] applies only to a non-null raw-bytes result")


def require_binary(field: pa.Field[Any], origin: str, what: str = "result") -> None:
    """Refuse a non-``binary`` column where a backend binds a record or raw bytes."""
    if not field.type.equals(pa.binary()):
        raise GeneratorError(f"{origin}: {what} is {field.type}, expected binary")


# ---------------------------------------------------------------------------
# Docs
# ---------------------------------------------------------------------------


def summary(doc: str | None) -> str | None:
    """The first paragraph of a docstring, whitespace-normalized."""
    if not doc:
        return None
    first = doc.strip().split("\n\n", 1)[0]
    return " ".join(first.split()) or None


def paragraphs(doc: str | None) -> list[str]:
    """A docstring's paragraphs, whitespace-normalized."""
    if not doc:
        return []
    return [" ".join(p.split()) for p in doc.strip().split("\n\n") if p.strip()]
