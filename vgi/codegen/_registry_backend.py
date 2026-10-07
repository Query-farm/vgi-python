# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The registry generator's backend contract: what one language supplies, and what it gets.

See :mod:`vgi.codegen._registry` for the design. A backend is a
:class:`RegistryBackend` subclass with two methods -- ``render_body`` (model ->
the language's file) and ``derive`` (that file -> :class:`DerivedMethod`
values, by the port's own derivation rules) -- plus a few class attributes
naming its target and conventions. Everything else is here, once:

- rendering: the provenance banner, ``emit`` / ``render`` / ``main``;
- :meth:`RegistryBackend.rows` -- the registration rows an explicit-registration
  port walks (params / result / header fields), with the port's
  :class:`VoidResult` convention applied;
- derive-back: :meth:`RegistryBackend.derived_methods` rebuilds what a backend
  parsed into :class:`~vgi.codegen._registry.RegistryMethod` values (checking
  the method set and parameter names against the model) so the caller can hash
  them; :class:`ArrowDialect` evaluates a rendered Arrow schema expression;
  :class:`Tamper` is the one-edit mutation that proves the derivation reads the
  rendered text;
- :data:`REGISTRY_BACKENDS` / :func:`registry_backends` -- the list the regen
  script and the tests iterate.
"""

from __future__ import annotations

import abc
import dataclasses
import enum
import importlib
import io
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

import pyarrow as pa
from vgi_rpc.rpc._types import MethodType  # type: ignore[attr-defined]

from vgi.codegen._common import GeneratorError, provenance_comment
from vgi.codegen._registry import MethodKind, RegistryMethod, registry_methods

if TYPE_CHECKING:
    from typing import TextIO

#: Every registry backend module, in regen order. Each binds ``BACKEND``.
REGISTRY_BACKENDS: tuple[str, ...] = (
    "vgi.codegen.csharp_registry",
    "vgi.codegen.java_registry",
    "vgi.codegen.ts_registry",
    "vgi.codegen.go_registry",
    "vgi.codegen.rust_registry",
    "vgi.codegen.cpp_registry",
)

REGEN_LINE = "uv run --project ~/Development/vgi-python python scripts/regen_generated.py"


def registry_backends() -> list[RegistryBackend]:
    """Every registry backend, in :data:`REGISTRY_BACKENDS` order."""
    out = []
    for name in REGISTRY_BACKENDS:
        backend = getattr(importlib.import_module(name), "BACKEND", None)
        if not isinstance(backend, RegistryBackend):
            raise GeneratorError(f"{name} binds no RegistryBackend as BACKEND")
        out.append(backend)
    return out


def expect(condition: object, message: str) -> None:
    """Fail a derive-back with *message* unless *condition* holds."""
    if not condition:
        raise GeneratorError(message)


class VoidResult(enum.Enum):
    """How a port's vgi-rpc spells a unary that returns nothing.

    ``ABSENT``: there is no result slot at all (a ``void`` return type, Go's
    ``UnaryVoid``, a C++ ``Void`` row). ``EMPTY_SCHEMA``: the result schema is
    passed as a value and is empty; vgi-rpc reports ``has_return`` iff it has
    fields (TypeScript, Rust).
    """

    ABSENT = "absent"
    EMPTY_SCHEMA = "empty-schema"


@dataclass(frozen=True)
class RegistrationRow:
    """One method as an explicit-registration table carries it.

    Attributes:
        method: The model method.
        params: The params-schema fields.
        result: The result-schema fields: ``[result]``, ``[]`` for a void unary
            under :attr:`VoidResult.EMPTY_SCHEMA`, ``None`` when the port
            declares no result slot (a stream, or a void unary under ``ABSENT``).
        header: The stream header's fields, or ``None``.
    """

    method: RegistryMethod
    params: list[pa.Field[Any]]
    result: list[pa.Field[Any]] | None
    header: list[pa.Field[Any]] | None


@dataclass(frozen=True)
class DerivedMethod:
    """One method as a port derives it from the rendered file: what the hash needs.

    Attributes:
        name: The wire name the rendered file registers.
        kind: Unary, Void or Stream, as the rendered file declares it.
        params: The params-schema fields, as the port derives them.
        result: The ``result`` field (``Unary`` only).
        header: The stream header schema (``Stream`` only), or ``None``.
    """

    name: str
    kind: MethodKind
    params: Sequence[pa.Field[Any]]
    result: pa.Field[Any] | None = None
    header: pa.Schema | None = None


@dataclass(frozen=True)
class Tamper:
    """A one-edit mutation of a rendered file that must move the derived hash.

    Within the region from the first *start* to the next *end* after it,
    *old* (which must occur there) becomes *new*.
    """

    start: str
    end: str
    old: str
    new: str

    def apply(self, text: str) -> str:
        """The tampered text."""
        start = text.index(self.start)
        end = text.index(self.end, start + len(self.start))
        region = text[start:end]
        expect(self.old in region, f"tamper target {self.old!r} not in its region")
        return text[:start] + region.replace(self.old, self.new) + text[end:]


@dataclass(frozen=True)
class ArrowDialect:
    """How one language spells Arrow schema expressions, for derive-back.

    The shared evaluator turns a rendered expression into pyarrow values: apply
    *rewrites* (regex, replacement), in order, to reach Python call syntax, then
    evaluate with *names* (plus ``true`` / ``false`` / ``null``) and no
    builtins. A language supplies only those two tables; the rendered text is
    this generator's own output, never input from elsewhere.
    """

    rewrites: tuple[tuple[str, str], ...]
    names: Mapping[str, object] = field(default_factory=dict)

    def eval(self, expr: str) -> Any:
        """Evaluate one rendered expression."""
        py = expr
        for pattern, replacement in self.rewrites:
            py = re.sub(pattern, replacement, py)
        env: dict[str, object] = {"true": True, "false": False, "null": None, **self.names}
        try:
            return eval(py, {"__builtins__": {}}, env)  # noqa: S307 - the generator's own output
        except Exception as exc:  # noqa: BLE001 - report the expression that failed
            raise GeneratorError(f"cannot evaluate {expr!r} (as {py!r}): {exc}") from exc

    def schema(self, expr: str) -> pa.Schema:
        """Evaluate an expression that builds a schema, or a list of fields."""
        built = self.eval(expr)
        if isinstance(built, list):
            built = pa.schema(built)
        expect(isinstance(built, pa.Schema), f"{expr!r} is not a schema")
        return built  # type: ignore[no-any-return]


def call_args(text: str, func: str) -> list[str]:
    """The argument text of every ``func(...)`` call in *text*, parentheses balanced.

    *func* matches as a whole word (``schema`` does not match ``header_schema``).
    For the generator's own output, whose string literals hold no parentheses.
    """
    out = []
    for m in re.finditer(rf"(?<![\w.]){re.escape(func)}\(", text):
        depth, i = 1, m.end()
        while depth:
            expect(i < len(text), f"unbalanced {func}( call")
            depth += {"(": 1, ")": -1}.get(text[i], 0)
            i += 1
        out.append(text[m.end() : i - 1])
    return out


def header_holder(name: str, schema: pa.Schema) -> type:
    """A stand-in header type carrying a derived ``ARROW_SCHEMA``, for :func:`preimage`."""
    return type(f"{name}DerivedHeader", (), {"ARROW_SCHEMA": schema})


def rebuild(derived: Sequence[DerivedMethod]) -> list[RegistryMethod]:
    """Rebuild derived methods into model methods, every hashed field taken from the derivation.

    Checks each against the model: a known, not repeated wire name, the
    model's parameter names in order, and a result / header only where the
    kind allows one.
    """
    model = {m.name: m for m in registry_methods()}
    out: list[RegistryMethod] = []
    for d in derived:
        base = model.get(d.name)
        expect(base is not None, f"the rendered file registers {d.name}, which is not a vgi.v2 method")
        assert base is not None
        expect(d.name not in {m.name for m in out}, f"{d.name} is registered twice")
        names = [f.name for f in d.params]
        expect(names == [p.name for p in base.params], f"{d.name}: params derive as {names}")
        if d.kind is MethodKind.UNARY:
            expect(d.result is not None and d.result.name == "result", f"{d.name}: a Unary needs a `result` column")
        else:
            expect(d.result is None, f"{d.name}: a {d.kind.value} derives no result")
        expect(d.header is None or d.kind is MethodKind.STREAM, f"{d.name}: only a stream has a header")
        out.append(
            dataclasses.replace(
                base,
                method_type=MethodType.STREAM if d.kind is MethodKind.STREAM else MethodType.UNARY,
                params=tuple(dataclasses.replace(p, field=f) for p, f in zip(base.params, d.params, strict=True)),
                result_field=d.result,
                header_type=None if d.header is None else header_holder(d.name, d.header),
            )
        )
    return out


class RegistryBackend(abc.ABC):
    """One language's registry: how it renders the model, and how its port reads the rendering back.

    Class attributes:
        key: Short id (``"java"``), used for test ids.
        language: Display name, used in messages (``"Java"``).
        module: The backend's module, named in the provenance banner.
        target: The generated file, relative to the SDK checkout.
        repo: The SDK checkout's directory name.
        root_env: Environment variable that overrides the checkout location.
        void_result: How the port spells a unary with no return.
        tamper: A one-edit mutation that must move the derived hash.
        banner_head / banner_tail: Text around the provenance comment.
    """

    key: ClassVar[str]
    language: ClassVar[str]
    module: ClassVar[str]
    target: ClassVar[str]
    repo: ClassVar[str]
    root_env: ClassVar[str]
    void_result: ClassVar[VoidResult] = VoidResult.ABSENT
    tamper: ClassVar[Tamper]
    generator_version: ClassVar[str] = "1"
    banner_head: ClassVar[str] = ""
    banner_tail: ClassVar[str] = ""

    @abc.abstractmethod
    def render_body(self, methods: Sequence[RegistryMethod]) -> str:
        """The generated file's body (everything after the provenance banner)."""

    @abc.abstractmethod
    def derive(self, text: str) -> list[DerivedMethod]:
        """Read a rendered file back, as the language's vgi-rpc port would register it."""

    # -- rows ---------------------------------------------------------------

    def row(self, m: RegistryMethod) -> RegistrationRow:
        """One method's registration row, under this port's :attr:`void_result`."""
        result: list[pa.Field[Any]] | None = None
        if m.result_field is not None:
            result = [m.result_field]
        elif m.kind is MethodKind.VOID and self.void_result is VoidResult.EMPTY_SCHEMA:
            result = []
        return RegistrationRow(m, m.params_fields, result, m.header_fields)

    def unary_from_result(self, name: str, fields: Sequence[pa.Field[Any]]) -> tuple[MethodKind, pa.Field[Any] | None]:
        """Interpret a derived result schema under :attr:`void_result`: ``[]`` is Void, ``[result]`` Unary."""
        expect(self.void_result is VoidResult.EMPTY_SCHEMA or fields, f"{name}: an empty result schema")
        expect(len(fields) <= 1, f"{name}: a result schema with {len(fields)} fields")
        return (MethodKind.UNARY, fields[0]) if fields else (MethodKind.VOID, None)

    # -- rendering ----------------------------------------------------------

    def emit(self, out: TextIO) -> None:
        """Write the generated file to *out*."""
        body = self.render_body(registry_methods())
        out.write(self.banner_head)
        out.write(
            provenance_comment(
                generator_module=self.module,
                generator_command=f"python -m {self.module}",
                generator_version=self.generator_version,
                regen_command_lines=[REGEN_LINE],
                body=body,
            )
        )
        out.write(self.banner_tail)
        out.write("\n")
        out.write(body)

    def render(self) -> str:
        """The full text of the generated file."""
        buf = io.StringIO()
        self.emit(buf)
        return buf.getvalue()

    def main(self) -> None:
        """Console-script entrypoint: write the generated file to stdout."""
        try:
            self.emit(sys.stdout)
        except GeneratorError as e:
            print(f"\nerror: {e}\n", file=sys.stderr)
            sys.exit(2)

    # -- derive-back --------------------------------------------------------

    def derived_methods(self, text: str | None = None) -> list[RegistryMethod]:
        """*text* (default: a fresh rendering) derived back and rebuilt, ready to hash."""
        return rebuild(self.derive(self.render() if text is None else text))
