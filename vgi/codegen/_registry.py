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

The shape of a registry generator
---------------------------------

One module per language (``java_registry``, ``csharp_registry``; Phase 2 adds
Go, Rust, TypeScript and C++) turns :func:`registry_methods` into three things:

1. **A complete declaration** -- interface, trait, abstract class -- of every
   ``vgi.v2`` method with its exact method type and params / result / header
   types, chosen so the SDK's vgi-rpc port *derives* the reference schemas
   from it (or, for a port that registers schemas explicitly, emitting them
   from :attr:`RegistryParam.field` and :attr:`RegistryMethod.result_field`).
2. **A default implementation** in which every method answers the SDK's
   ``UNIMPLEMENTED`` error. SDK code inherits it and overrides only what it
   implements; a method added to :class:`vgi.protocol.VgiProtocol` appears in
   every SDK as a stub on the next regeneration.
3. **The registration** that hands the declaration to the vgi-rpc server. In
   Java and C# the server reflects over the interface, so (1) *is* the table;
   a port with explicit registration emits the table as well.

Everything language-specific -- type names, which packed requests a port
decodes into a typed record rather than raw bytes, how the error is
constructed -- lives in that language's module. Nothing protocol-specific does:
the method set, the parameter order, wire names and Arrow types, nullability,
result presence and the stream header all come from here.

Checking a generator
--------------------

:func:`preimage_hash` computes the ``vgi_rpc.protocol_hash.v1`` digest of a
list of :class:`RegistryMethod`. ``tests/test_generated_registry.py`` parses
each language's *rendered* file, rebuilds the methods with every field derived
back from the rendered type by that port's ``SchemaDerivation`` rules
(``dataclasses.replace``), and asserts the digest equals the live
``VgiProtocol`` hash -- so a mapping bug in a generator fails in vgi-python
rather than as a hash mismatch (or an Arrow schema rejection) in the SDK. The
SDK keeps its own pinned-hash test as the end-to-end check.

Adding a language (Phase 2)
---------------------------

- Reuse the language's ``*_types`` generator for type names and the Arrow ->
  native mapping; the registry module only decides signatures and bodies.
- Add a ``TARGET`` constant and list the module in
  ``scripts/regen_generated.py`` (the ``--check`` drift mechanism and the drift
  test in ``tests/test_generated_registry.py`` key off it), plus a derive-back
  parser and hash test there.
- Make the default body construct the SDK's existing UNIMPLEMENTED error, and
  give every method the SDK's call-context parameter uniformly.
- Behaviour the SDK used to put in hand-written interface defaults (empty
  listings, read-only DDL refusals, a composed ``catalog_contents``) moves into
  the SDK's concrete service, which overrides the generated stub.
- Keep the language table small: only choices that are wire-identical (a typed
  record vs raw IPC bytes for one ``binary`` column) belong there, and the
  generator must reject an entry that would change a schema.
"""

from __future__ import annotations

import dataclasses
import hashlib
import inspect
import types
import typing
from collections.abc import Sequence
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
    def header_schema(self) -> pa.Schema | None:
        """The stream header's Arrow schema, or ``None``."""
        if self.header_type is None:
            return None
        schema = getattr(self.header_type, "ARROW_SCHEMA", None)
        if not isinstance(schema, pa.Schema):
            raise GeneratorError(f"{self.name}: header type {self.header_type!r} has no ARROW_SCHEMA")
        return schema


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
    return out


def preimage(name: str, methods: Sequence[RegistryMethod]) -> dict[str, Any]:
    """The ``vgi_rpc.protocol_hash.v1`` description of a method table.

    Mirrors ``vgi_rpc.rpc._protocol_hash.protocol_description`` field for field,
    but over :class:`RegistryMethod` values, so a language test can build the
    table from what its generator *rendered* (each field derived back by that
    port's rules) and compare digests.
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


def is_record(annotation: object) -> bool:
    """Whether *annotation* is a dataclass carrying an ``ARROW_SCHEMA`` (a packed request/response)."""
    return (
        isinstance(annotation, type)
        and dataclasses.is_dataclass(annotation)
        and isinstance(getattr(annotation, "ARROW_SCHEMA", None), pa.Schema)
    )


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
